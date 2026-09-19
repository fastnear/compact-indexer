mod click;
mod common;
mod redis_db;

use crate::click::{extract_rows, ActionKind, ActionRow, EventRow, ReceiptStatus};
use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::BlockWithTxHashes;
use fastnear_primitives::near_primitives::account::id::AccountType;
use fastnear_primitives::near_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::types::ChainId;
use near_crypto::{PublicKey, PublicKeyHandle};
use redis_db::RedisDB;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

const PROJECT_ID: &str = "ft_red";

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PairUpdate {
    account_id: String,
    token_id: String,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PublicKeyPair {
    account_id: String,
    // The on-chain handle: ed25519/secp256k1 keys print unchanged, while ML-DSA-65
    // keys become `ml-dsa-65-hash:...`, the form access-key lists and the API use.
    public_key: PublicKeyHandle,
}

#[derive(Debug, Copy, Clone)]
pub enum PublicKeyUpdateType {
    AddedFullAccess,
    AddedLimitedAccessKey,
    RemovedKey,
}

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();
    tokio::spawn(async move {
        let mut signals = signal_hook::iterator::Signals::new(&[
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGINT,
        ])
        .unwrap();
        for sig in signals.forever() {
            match sig {
                signal_hook::consts::SIGTERM | signal_hook::consts::SIGINT => {
                    println!("Received signal {}, shutting down...", sig);
                    ctrl_c_running.store(false, Ordering::SeqCst);
                    break;
                }
                _ => unreachable!(),
            }
        }
    });

    common::setup_tracing("ft_red=info,redis=info,clickhouse=info,neardata-fetcher=info");

    tracing::log::info!(target: PROJECT_ID, "Starting FT Redis Indexer");

    let mut write_redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    let client = reqwest::Client::new();
    let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
        .expect("Invalid chain id");
    let first_block_height = fetcher::fetch_first_block(&client, chain_id)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;
    tracing::log::info!(target: PROJECT_ID, "First redis block {}", first_block_height);
    let auth_bearer_token = std::env::var("AUTH_BEARER_TOKEN").ok();

    let last_block_height: Option<BlockHeight> = write_redis_db
        .get("meta:latest_block")
        .await
        .expect("Failed to get the latest block")
        .map(|s| s.parse().expect("Failed to parse the latest block"));

    let start_block_height = last_block_height.map(|b| b + 1).unwrap_or_else(|| {
        std::env::var("START_BLOCK")
            .map(|s| s.parse().expect("Failed to parse START_BLOCK"))
            .expect("START_BLOCK is not set")
    });
    tracing::log::info!(target: PROJECT_ID, "Resuming from {}", start_block_height);

    let num_threads = std::env::var("NUM_THREADS")
        .map(|s| s.parse::<u64>().expect("Failed to parse NUM_THREADS"))
        .unwrap_or(4);
    let num_lookahead_threads = std::env::var("NUM_LOOKAHEAD_THREADS")
        .map(|s| {
            s.parse::<u64>()
                .expect("Failed to parse NUM_LOOKAHEAD_THREADS")
        })
        .unwrap_or(4);

    let (sender, receiver) = mpsc::channel(100);
    let mut builder = fetcher::FetcherConfigBuilder::new()
        .chain_id(chain_id)
        .num_threads(num_threads)
        .start_block_height(start_block_height);
    if let Some(auth_bearer_token) = auth_bearer_token {
        builder = builder.auth_bearer_token(auth_bearer_token);
    }
    let mut config = builder.build();
    // Temporary fix, since FetcherConfigBuilder doesn't support num_lookahead_threads
    config.num_lookahead_threads = num_lookahead_threads;
    tokio::spawn(fetcher::start_fetcher(config, sender, is_running));

    listen_blocks(receiver, write_redis_db, chain_id).await;
}

async fn listen_blocks(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    mut redis_db: RedisDB,
    chain_id: ChainId,
) {
    while let Some(streamer_message) = stream.recv().await {
        let block_height = streamer_message.block.header.height;
        let block_timestamp = streamer_message.block.header.timestamp_nanosec;
        let current_time_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let time_diff_ns = current_time_ns.saturating_sub(block_timestamp);
        tracing::log::info!(target: PROJECT_ID, "Processing block {}\tlatency {:.3} sec", block_height, time_diff_ns as f64 / 1e9f64);
        let (actions, events) = extract_rows(streamer_message);

        let mut to_update: HashMap<String, Vec<(String, String)>> = HashMap::new();

        let ft_pairs = extract_ft_pairs(&actions, &events, chain_id);
        let accounts = extract_all_accounts(&actions, chain_id);

        add_pairs_to_update("ft", ft_pairs.clone(), &mut to_update, block_height);
        add_pairs_to_update(
            "nf",
            extract_nft_pairs(&actions, &events, chain_id),
            &mut to_update,
            block_height,
        );
        add_pairs_to_update(
            "st",
            extract_staking_pairs(&actions, chain_id),
            &mut to_update,
            block_height,
        );
        let public_key_updates = extract_public_keys(&actions, chain_id);

        tracing::log::info!(target: PROJECT_ID, "Updating {} accounts, {} keys", to_update.len(), public_key_updates.len());
        // tracing::log::info!(target: PROJECT_ID, "Updating keys {:?}", public_key_updates);
        // tracing::log::info!(target: PROJECT_ID, "Updating accounts {:?}", to_update);

        let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
            let mut pipe = redis::pipe();
            for (key, fields_data) in &to_update {
                pipe.cmd("HSET").arg(key).arg(fields_data).ignore();
            }
            for (pair, update_type) in &public_key_updates {
                let key = format!("pk:{}", pair.public_key);
                match update_type {
                    PublicKeyUpdateType::AddedFullAccess => {
                        pipe.cmd("HSET")
                            .arg(key)
                            .arg(&pair.account_id)
                            .arg("f")
                            .ignore();
                    }
                    PublicKeyUpdateType::AddedLimitedAccessKey => {
                        pipe.cmd("HSET")
                            .arg(key)
                            .arg(&pair.account_id)
                            .arg("l")
                            .ignore();
                    }
                    PublicKeyUpdateType::RemovedKey => {
                        pipe.cmd("HDEL").arg(key).arg(&pair.account_id).ignore();
                    }
                }
            }

            pipe.cmd("SET")
                .arg("meta:latest_block")
                .arg(block_height)
                .ignore();

            pipe.cmd("SET")
                .arg("meta:latest_block_time")
                .arg(block_timestamp.to_string())
                .ignore();

            if !ft_pairs.is_empty() || !accounts.is_empty() {
                pipe.cmd("RPUSH")
                    .arg("ft_updates")
                    .arg(json!({
                        "block_height": block_height,
                        "pairs": ft_pairs.iter().map(|pair| format!("{}:{}", pair.token_id, pair.account_id)).collect::<Vec<_>>(),
                        "accounts": &accounts
                    }).to_string())
                    .ignore();
            }

            pipe.query_async(connection).await
        });
        res.expect("Failed to update");
    }
}

fn extract_staking_pairs(actions: &[ActionRow], _chain_id: ChainId) -> HashSet<PairUpdate> {
    // Extract matching (account_id, validator_id) for staking changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success || action.action != ActionKind::FunctionCall {
            continue;
        }
        if action.account_id.ends_with(".poolv1.near")
            || action.account_id.ends_with(".pool.near")
            || action.account_id.ends_with(".pool.f863973.m0")
        {
            pairs.insert(PairUpdate {
                account_id: action.predecessor_id.clone(),
                token_id: action.account_id.clone(),
            });
        }
    }

    pairs
}

fn extract_public_keys(
    actions: &[ActionRow],
    _chain_id: ChainId,
) -> HashMap<PublicKeyPair, PublicKeyUpdateType> {
    // Extract matching (account_id, validator_id) for staking changes
    let mut pairs = HashMap::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        match action.action {
            ActionKind::AddKey | ActionKind::DeleteKey => {
                let public_key: PublicKeyHandle = PublicKey::from_str(
                    &action
                        .public_key
                        .as_ref()
                        .expect("Missing PublicKey for AddKey action"),
                )
                .expect("Invalid public key")
                .into();
                if action.action == ActionKind::AddKey {
                    pairs.insert(
                        PublicKeyPair {
                            account_id: action.account_id.clone(),
                            public_key,
                        },
                        if action.access_key_contract_id.is_none() {
                            PublicKeyUpdateType::AddedFullAccess
                        } else {
                            PublicKeyUpdateType::AddedLimitedAccessKey
                        },
                    );
                } else {
                    pairs.insert(
                        PublicKeyPair {
                            account_id: action.account_id.clone(),
                            public_key,
                        },
                        PublicKeyUpdateType::RemovedKey,
                    );
                }
            }
            ActionKind::Transfer => {
                if action.predecessor_id == "system" {
                    continue;
                }
                let account_id =
                    AccountId::from_str(&action.account_id).expect("Invalid account_id");
                if account_id.get_account_type() == AccountType::NearImplicitAccount {
                    let bytes = hex::decode(&account_id.as_str()).expect("Invalid hex");
                    let public_key: PublicKeyHandle =
                        PublicKey::ED25519(bytes.as_slice().try_into().unwrap()).into();
                    pairs.insert(
                        PublicKeyPair {
                            account_id: account_id.to_string(),
                            public_key,
                        },
                        PublicKeyUpdateType::AddedFullAccess,
                    );
                }
            }
            _ => {}
        }
    }

    pairs
}

fn extract_ft_pairs(
    actions: &[ActionRow],
    events: &[EventRow],
    _chain_id: ChainId,
) -> HashSet<PairUpdate> {
    // Extract matching (account_id, token_id) for FT changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &action.account_id;
        if token_id.ends_with(".poolv1.near")
            || token_id.ends_with(".pool.near")
            || token_id.ends_with(".lockup.near")
            || token_id.ends_with(".pool.f863973.m0")
        {
            continue;
        }
        if action.method_name.is_none() {
            continue;
        }
        let method_name = action.method_name.as_ref().unwrap();
        // Special case for HERE staking contract
        if token_id == "storage.herewallet.near" {
            if ["deposit", "withdraw"].contains(&method_name.as_str()) {
                pairs.insert(PairUpdate {
                    account_id: action.predecessor_id.clone(),
                    token_id: token_id.clone(),
                });
            }
        }
        if token_id.ends_with(".factory.bridge.near")
            || token_id == "aurora"
            || token_id.ends_with(".factory.sepolia.testnet")
            || token_id.ends_with(".factory.goerli.testnet")
        {
            if ["mint", "burn"].contains(&method_name.as_str()) {
                if let Some(account_id) = action.args_account_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
            if method_name == "withdraw" {
                pairs.insert(PairUpdate {
                    account_id: action.predecessor_id.clone(),
                    token_id: token_id.clone(),
                });
            }
        }
        if [
            "ft_transfer_call",
            "ft_transfer",
            "ft_mint",
            "ft_burn",
            "near_withdraw",
            "near_deposit",
            "deposit_and_stake",
        ]
        .contains(&method_name.as_str())
        {
            pairs.insert(PairUpdate {
                account_id: action.predecessor_id.clone(),
                token_id: token_id.clone(),
            });
            if let Some(account_id) = action.args_receiver_id.as_ref() {
                pairs.insert(PairUpdate {
                    account_id: account_id.clone(),
                    token_id: token_id.clone(),
                });
            }
        }
        if method_name == "ft_resolve_transfer" {
            if let Some(account_id) = action.args_sender_id.as_ref() {
                pairs.insert(PairUpdate {
                    account_id: account_id.clone(),
                    token_id: token_id.clone(),
                });
            }
            if let Some(account_id) = action.args_receiver_id.as_ref() {
                pairs.insert(PairUpdate {
                    account_id: account_id.clone(),
                    token_id: token_id.clone(),
                });
            }
        }
    }
    for event in events {
        if event.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &event.account_id;
        if let Some(event_type) = event.event.as_ref() {
            if ["ft_mint", "ft_burn"].contains(&event_type.as_str()) {
                if let Some(account_id) = event.data_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            } else if event_type == "ft_transfer" {
                if let Some(account_id) = event.data_new_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
                if let Some(account_id) = event.data_old_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
        }
    }

    pairs
}

fn extract_nft_pairs(
    actions: &[ActionRow],
    events: &[EventRow],
    _chain_id: ChainId,
) -> HashSet<PairUpdate> {
    // Extract matching (account_id, token_id) for FT changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &action.account_id;
        if let Some(method_name) = action.method_name.as_ref() {
            if [
                "nft_transfer_call",
                "nft_transfer",
                "nft_approve",
                "nft_revoke",
                "nft_revoke_all",
                "nft_mint",
                "nft_burn",
            ]
            .contains(&method_name.as_str())
            {
                pairs.insert(PairUpdate {
                    account_id: action.predecessor_id.clone(),
                    token_id: token_id.clone(),
                });
                if let Some(account_id) = action.args_receiver_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
        }
    }
    for event in events {
        if event.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &event.account_id;
        if let Some(event_type) = event.event.as_ref() {
            if ["nft_mint", "nft_burn"].contains(&event_type.as_str()) {
                if let Some(account_id) = event.data_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            } else if event_type == "nft_transfer" {
                if let Some(account_id) = event.data_new_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
                if let Some(account_id) = event.data_old_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
        }
    }

    pairs
}

fn add_pairs_to_update(
    prefix: &str,
    pairs: HashSet<PairUpdate>,
    to_update: &mut HashMap<String, Vec<(String, String)>>,
    block_height: BlockHeight,
) {
    for PairUpdate {
        account_id,
        token_id,
    } in pairs
    {
        to_update
            .entry(format!("{}:{}", prefix, account_id))
            .or_insert_with(Vec::new)
            .push((token_id, block_height.to_string()));
    }
}

fn extract_all_accounts(actions: &[ActionRow], _chain_id: ChainId) -> HashSet<String> {
    actions
        .iter()
        .filter_map(|action| {
            if action.status == ReceiptStatus::Success {
                Some(action.account_id.clone())
            } else {
                None
            }
        })
        .collect()
}
