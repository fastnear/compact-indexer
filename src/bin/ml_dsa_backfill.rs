//! One-off migration for ML-DSA-65 access keys indexed before ft-red stored them
//! by their on-chain handle.
//!
//!     pk:ml-dsa-65:<full key>  ->  pk:ml-dsa-65-hash:<handle>
//!
//! The handle comes from `PublicKeyHandle`, the same conversion ft-red now uses, so
//! the two cannot disagree. Every account/key pair is checked against the chain
//! before it is written, and the chain's permission wins over the stored flag.
//!
//! Dry run unless `--apply` is passed:
//!
//!     ml-dsa-backfill --chain testnet
//!     ml-dsa-backfill --chain testnet --apply
mod common;
mod redis_db;

use dotenv::dotenv;
use near_crypto::{PublicKey, PublicKeyHandle};
use redis_db::RedisDB;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

const PROJECT_ID: &str = "ml_dsa_backfill";
const OLD_PREFIX: &str = "pk:ml-dsa-65:";
const SCAN_COUNT: usize = 5000;
const FULL_ACCESS: &str = "f";
const LIMITED_ACCESS: &str = "l";
const GAS_KEY_FULL_ACCESS: &str = "gf";
const GAS_KEY_LIMITED_ACCESS: &str = "gl";

struct Args {
    chain: String,
    rpc_url: String,
    apply: bool,
    delete_stale: bool,
    limit: Option<usize>,
}

fn parse_args() -> Args {
    let (mut chain, mut rpc_url, mut apply, mut delete_stale, mut limit) =
        (None, None, false, false, None);
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--chain" => chain = args.next(),
            "--rpc-url" => rpc_url = args.next(),
            "--limit" => limit = args.next().map(|s| s.parse().expect("Invalid --limit")),
            "--apply" => apply = true,
            "--delete-stale" => delete_stale = true,
            "--help" | "-h" => {
                println!(
                    "Usage: ml-dsa-backfill --chain <testnet|mainnet> [--apply] [--delete-stale] \
                     [--limit N] [--rpc-url URL]\n\n\
                     Converts pre-fix `pk:ml-dsa-65:<full key>` entries to \
                     `pk:ml-dsa-65-hash:<handle>`.\n\
                     Without --apply nothing is written. Entries whose key is no longer on the \
                     account are reported\nand left alone unless --delete-stale is passed. \
                     Safe to re-run."
                );
                std::process::exit(0);
            }
            other => panic!("Unsupported argument: {other}"),
        }
    }
    let chain = chain.expect("--chain <testnet|mainnet> is required");
    assert!(
        chain == "testnet" || chain == "mainnet",
        "--chain must be testnet or mainnet"
    );
    let rpc_url = rpc_url.unwrap_or_else(|| format!("https://rpc.{chain}.fastnear.com"));
    Args {
        chain,
        rpc_url,
        apply,
        delete_stale,
        limit,
    }
}

/// Access keys currently on the account, keyed by their on-chain form.
/// `Ok(None)` means the account itself is gone.
async fn access_keys(
    client: &reqwest::Client,
    rpc_url: &str,
    account_id: &str,
) -> anyhow::Result<Option<HashMap<String, String>>> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": "ml-dsa-backfill",
        "method": "query",
        "params": {
            "request_type": "view_access_key_list",
            "finality": "final",
            "account_id": account_id,
        }
    });
    let response: Value = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .json()
        .await?;

    if let Some(error) = response.get("error") {
        let message = error.to_string();
        if message.contains("UNKNOWN_ACCOUNT") || message.contains("does not exist") {
            return Ok(None);
        }
        anyhow::bail!("RPC error for {}: {}", account_id, message);
    }

    let keys = response["result"]["keys"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Unexpected RPC response for {}", account_id))?;
    Ok(Some(
        keys.iter()
            .filter_map(|key| {
                let public_key = key["public_key"].as_str()?.to_string();
                // AccessKeyPermissionView has four variants over two axes: full vs
                // function-call, each either plain or a gas key. These are the same four
                // flags ft-red derives from the AddKey action. Name the full shapes
                // explicitly so a variant added later reads as limited: under-reporting
                // hides an account, over-reporting claims a key controls one.
                let permission = &key["access_key"]["permission"];
                let flag = if permission == "FullAccess" {
                    FULL_ACCESS
                } else if permission.get("GasKeyFullAccess").is_some() {
                    GAS_KEY_FULL_ACCESS
                } else if permission.get("GasKeyFunctionCall").is_some() {
                    GAS_KEY_LIMITED_ACCESS
                } else {
                    LIMITED_ACCESS
                };
                Some((public_key, flag.to_string()))
            })
            .collect(),
    ))
}

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();
    let args = parse_args();
    common::setup_tracing("ml_dsa_backfill=info,redis=info");

    // The Redis comes from the host's .env; --chain states which network that host
    // serves, so a mainnet run cannot be started against a testnet box by mistake.
    if let Ok(chain_id) = env::var("CHAIN_ID") {
        assert_eq!(
            chain_id, args.chain,
            "--chain {} does not match CHAIN_ID={} in the environment",
            args.chain, chain_id
        );
    }

    let mut db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;
    tracing::info!(target: PROJECT_ID, "{} on {}", if args.apply { "APPLY" } else { "DRY RUN" }, args.chain);

    let mut cursor = "0".to_string();
    let mut old_keys = Vec::new();
    loop {
        let cursor_arg = cursor.clone();
        let (next, keys): (String, Vec<String>) = with_retries!(db, |connection| async {
            redis::cmd("SCAN")
                .arg(&cursor_arg)
                .arg("MATCH")
                .arg(format!("{}*", OLD_PREFIX))
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(connection)
                .await
        })
        .expect("Failed to scan");
        old_keys.extend(keys);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    old_keys.sort();
    if let Some(limit) = args.limit {
        old_keys.truncate(limit);
    }
    tracing::info!(target: PROJECT_ID, "Found {} entries in the old format", old_keys.len());

    let client = reqwest::Client::new();
    let mut on_chain: HashMap<String, Option<HashMap<String, String>>> = HashMap::new();
    let (mut converted, mut stale, mut unverified, mut skipped) = (0, 0, 0, 0);

    for old_key in &old_keys {
        let public_key = match PublicKey::from_str(old_key.trim_start_matches("pk:")) {
            Ok(public_key) if matches!(public_key, PublicKey::MLDSA65(_)) => public_key,
            _ => {
                tracing::warn!(target: PROJECT_ID, "Skipping unparseable entry {}", old_key);
                skipped += 1;
                continue;
            }
        };
        let handle = PublicKeyHandle::from(&public_key).to_string();
        let new_key = format!("pk:{}", handle);

        let fields: Vec<(String, String)> = with_retries!(db, |connection| async {
            redis::cmd("HGETALL")
                .arg(old_key)
                .query_async(connection)
                .await
        })
        .expect("Failed to read entry");

        let mut live = Vec::new();
        let mut entry_unverified = false;
        for (account_id, stored_flag) in &fields {
            if !on_chain.contains_key(account_id) {
                match access_keys(&client, &args.rpc_url, account_id).await {
                    Ok(keys) => {
                        on_chain.insert(account_id.clone(), keys);
                    }
                    Err(err) => {
                        tracing::warn!(target: PROJECT_ID, "{} left alone: {}", account_id, err);
                        entry_unverified = true;
                        continue;
                    }
                }
            }
            match on_chain.get(account_id).and_then(|keys| keys.as_ref()) {
                None => {
                    tracing::info!(target: PROJECT_ID, "  stale: {} (account gone)", account_id)
                }
                Some(keys) => match keys.get(&handle) {
                    // The chain's permission wins: the stored flag may predate a change.
                    Some(chain_flag) => {
                        if chain_flag != stored_flag {
                            tracing::info!(target: PROJECT_ID, "  {} permission {} -> {}", account_id, stored_flag, chain_flag);
                        }
                        live.push((account_id.clone(), chain_flag.clone()));
                    }
                    None => {
                        tracing::info!(target: PROJECT_ID, "  stale: {} (key not on account)", account_id);
                        stale += 1;
                    }
                },
            }
        }

        if entry_unverified {
            unverified += 1;
            tracing::warn!(target: PROJECT_ID, "Leaving {} for a later run", old_key);
            continue;
        }

        if live.is_empty() {
            if args.apply && args.delete_stale {
                let _: () = with_retries!(db, |connection| async {
                    redis::cmd("DEL").arg(old_key).query_async(connection).await
                })
                .expect("Failed to delete stale entry");
                tracing::info!(target: PROJECT_ID, "Deleted stale {}", old_key);
            }
            continue;
        }

        tracing::info!(target: PROJECT_ID, "{} <- {} accounts", new_key, live.len());
        if args.apply {
            let _: () = with_retries!(db, |connection| async {
                let mut pipe = redis::pipe();
                pipe.cmd("HSET").arg(&new_key).arg(&live).ignore();
                pipe.cmd("DEL").arg(old_key).ignore();
                pipe.query_async(connection).await
            })
            .expect("Failed to convert entry");
        }
        converted += 1;
    }

    tracing::info!(
        target: PROJECT_ID,
        "{}: {} converted, {} stale pairs, {} entries left unverified, {} skipped",
        if args.apply { "Applied" } else { "Dry run" },
        converted,
        stale,
        unverified,
        skipped
    );
    if !args.apply {
        tracing::info!(target: PROJECT_ID, "Nothing was written. Re-run with --apply.");
    }
}
