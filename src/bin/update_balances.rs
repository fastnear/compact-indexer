mod common;
mod redis_db;
mod rpc;

use redis_db::RedisDB;
use std::collections::HashSet;
use std::env;

use crate::rpc::*;
use dotenv::dotenv;
use fastnear_primitives::near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};

const PROJECT_ID: &str = "update_balances";

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PairUpdate {
    account_id: String,
    token_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BlockUpdate {
    pub block_height: BlockHeight,
    pub pairs: Vec<String>,
    pub accounts: Option<Vec<String>>,
}

pub struct Config {
    pub max_top_holders_count: u64,
    pub rpc_config: rpc::RpcConfig,
}

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("update_balances=info,redis=info,rpc=debug");

    tracing::log::info!(target: PROJECT_ID, "Starting Balance updater");

    let mut redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    let config = Config {
        max_top_holders_count: env::var("MAX_TOP_HOLDERS_COUNT")
            .unwrap_or("1000".to_string())
            .parse()
            .expect("Invalid MAX_TOP_HOLDERS_COUNT"),
        rpc_config: rpc::RpcConfig::from_env(),
    };
    assert!(config.max_top_holders_count <= i64::MAX as u64);

    let path = env::var("BACKFILL_FILE").expect("BACKFILL_FILE is not set");
    if std::path::Path::new(&path).exists() {
        let f = std::fs::File::open(path).expect("Failed to open backfill file");
        let ft_update: BlockUpdate =
            serde_json::from_reader(f).expect("Failed to read backfill file");
        update_balances(&mut redis_db, vec![ft_update], &config, true).await;
    }

    let balances_batch_size: Option<usize> = env::var("BALANCES_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok());

    if let Some(batch_size) = balances_batch_size {
        if batch_size > 1 {
            tracing::info!(target: PROJECT_ID, "Backfill using batch size: {}", batch_size);
            loop {
                let response: redis::RedisResult<(Vec<String>,)> =
                    with_retries!(redis_db, |connection| async {
                        redis::pipe()
                            .cmd("LRANGE")
                            .arg("ft_updates")
                            .arg("0")
                            .arg(batch_size - 1)
                            .query_async(connection)
                            .await
                    });
                let updates: (Vec<String>,) = response.expect("Failed to get ft_updates");
                let updates: Vec<BlockUpdate> = updates
                    .0
                    .into_iter()
                    .map(|s| serde_json::from_str(&s).expect("Invalid JSON"))
                    .collect();
                let count = updates.len();
                if !updates.is_empty() {
                    update_balances(&mut redis_db, updates, &config, false).await;
                }
                if count < batch_size {
                    tracing::info!(target: PROJECT_ID, "Backfill completed");
                    break;
                }
            }
        }
    }

    loop {
        let response: redis::RedisResult<(String,)> = with_retries!(redis_db, |connection| async {
            redis::pipe()
                .cmd("BLMOVE")
                .arg("ft_updates")
                .arg("ft_updates")
                .arg("LEFT")
                .arg("LEFT")
                .arg(0)
                .query_async(connection)
                .await
        });
        let (s,) = response.expect("Failed to get ft_updates");
        let ft_updates: BlockUpdate = serde_json::from_str(&s).expect("Invalid JSON");
        update_balances(&mut redis_db, vec![ft_updates], &config, false).await;
    }
}

async fn update_balances(
    redis_db: &mut RedisDB,
    block_updates: Vec<BlockUpdate>,
    config: &Config,
    backfill: bool,
) {
    if block_updates.is_empty() {
        tracing::info!(target: PROJECT_ID, "No block updates to process");
        return;
    }
    let count = block_updates.len();
    let block_height = block_updates.last().unwrap().block_height;
    let mut pairs = vec![];
    let mut accounts = vec![];
    for block_update in block_updates {
        pairs.extend(block_update.pairs);
        if let Some(accs) = block_update.accounts {
            accounts.extend(accs);
        }
    }

    let mut tasks = vec![];
    // Pair tasks
    tasks.extend(pairs.iter().map(|pair| {
        let (token_id, account_id) = pair.split_once(':').unwrap();
        let account_id = account_id.to_string();
        RpcTask::FtPair {
            block_height: Some(block_height),
            token_id: token_id.to_string(),
            account_id: account_id.to_string(),
        }
    }));
    tasks.extend(accounts.iter().map(|account_id| RpcTask::AccountState {
        block_height: Some(block_height),
        account_id: account_id.clone(),
    }));

    let results = fetch_from_rpc(&tasks, &config.rpc_config)
        .await
        .expect("Failed to fetch updates from the RPC");

    let all_tokens: HashSet<&String> = results
        .iter()
        .filter_map(|b| match &b.task {
            RpcTask::FtPair { token_id, .. } => Some(token_id),
            _ => None,
        })
        .collect();

    let limit_arg = -(config.max_top_holders_count as i64 + 1);

    let mut real_tokens = HashSet::new();
    for RpcResultPair { task, result } in &results {
        match task {
            RpcTask::FtPair { token_id, .. } => {
                let balance = result.as_ref().map(|r| r.unwrap_as_ft_pair().balance);
                if balance.is_some() {
                    real_tokens.insert(token_id);
                }
            }
            _ => {}
        }
    }

    // Save balances to redis
    let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();
        pipe.cmd("LPOP").arg("ft_updates").arg(count).ignore();
        for RpcResultPair { task, result } in &results {
            match task {
                RpcTask::FtPair {
                    token_id,
                    account_id,
                    ..
                } => {
                    let balance = result.as_ref().map(|r| r.unwrap_as_ft_pair().balance);
                    if balance.is_none() {
                        if real_tokens.contains(token_id) {
                            tracing::info!(target: PROJECT_ID, "No balance for token {} account {}", token_id, account_id);
                        }
                    }
                    pipe.cmd(if backfill { "HSETNX" } else { "HSET" })
                        .arg(format!("b:{}", token_id))
                        .arg(account_id)
                        .arg(balance.as_ref().map(|s| s.to_string()).unwrap_or_default())
                        .ignore();

                    if let Some(balance) = balance {
                        pipe.cmd("ZADD").arg(format!("tb:{}", token_id));
                        if backfill {
                            pipe.arg("NX");
                        }
                        pipe.arg(balance.to_string()).arg(account_id).ignore();
                    }
                }
                RpcTask::AccountState { account_id, .. } => {
                    let account_state = result.as_ref().map(|r| r.unwrap_as_account_state());
                    pipe.cmd(if backfill { "HSETNX" } else { "HSET" })
                        .arg("accounts")
                        .arg(account_id)
                        .arg(
                            account_state
                                .as_ref()
                                .map(|s| serde_json::to_string(s).unwrap())
                                .unwrap_or_default(),
                        )
                        .ignore();
                    if let Some(account_state) = account_state {
                        pipe.cmd("ZADD").arg("top_account_total");
                        if backfill {
                            pipe.arg("NX");
                        }
                        pipe.arg((account_state.balance + account_state.locked).to_string())
                            .arg(account_id)
                            .ignore();

                        pipe.cmd("ZADD").arg("top_account_liquid");
                        if backfill {
                            pipe.arg("NX");
                        }
                        pipe.arg(account_state.balance.to_string())
                            .arg(account_id)
                            .ignore();

                        pipe.cmd("ZADD").arg("top_account_staked");
                        if backfill {
                            pipe.arg("NX");
                        }
                        pipe.arg(account_state.locked.to_string())
                            .arg(account_id)
                            .ignore();

                        pipe.cmd("ZADD").arg("top_account_storage");
                        if backfill {
                            pipe.arg("NX");
                        }
                        pipe.arg(account_state.storage_bytes.to_string())
                            .arg(account_id)
                            .ignore();
                    }
                }
                _ => unreachable!(),
            };
        }

        // Keeping sorted sets lengths to a fixed size
        for token_id in &all_tokens {
            pipe.cmd("ZREMRANGEBYRANK")
                .arg(format!("tb:{}", token_id))
                .arg(0)
                .arg(limit_arg)
                .ignore();
        }

        if !accounts.is_empty() {
            pipe.cmd("ZREMRANGEBYRANK")
                .arg("top_account_total")
                .arg(0)
                .arg(limit_arg)
                .ignore();
            pipe.cmd("ZREMRANGEBYRANK")
                .arg("top_account_liquid")
                .arg(0)
                .arg(limit_arg)
                .ignore();
            pipe.cmd("ZREMRANGEBYRANK")
                .arg("top_account_staked")
                .arg(0)
                .arg(limit_arg)
                .ignore();
            pipe.cmd("ZREMRANGEBYRANK")
                .arg("top_account_storage")
                .arg(0)
                .arg(limit_arg)
                .ignore();
        }

        if !backfill {
            pipe.cmd("SET")
                .arg("meta:latest_balance_block")
                .arg(block_height)
                .ignore();
        }
        pipe.query_async(connection).await
    });
    res.expect("Failed to update");

    tracing::info!(target: PROJECT_ID, "Updated {} tasks for block {}{}", tasks.len(), block_height, if count > 1 || backfill { " (backfill)" } else { "" });

    // Delete pending update
    if backfill {
        let path = env::var("BACKFILL_FILE").expect("BACKFILL_FILE is not set");
        std::fs::remove_file(path).expect("Failed to remove backfill file");
    }
}
