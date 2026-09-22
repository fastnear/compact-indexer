mod common;
mod redis_db;
mod rpc;

use redis_db::RedisDB;
use std::collections::HashSet;
use std::env;

use crate::rpc::{fetch_from_rpc, RpcResultPair, RpcTask};
use dotenv::dotenv;
use itertools::Itertools;
use tokio::sync::mpsc;
use tracing_subscriber::fmt::format;

const PROJECT_ID: &str = "balances_backfill";

async fn export_fn_start(pairs_sync: mpsc::Sender<Vec<String>>) {
    let export_fn = env::var("EXPORT_FN").expect("Missing env EXPORT_FN");
    let f = std::fs::File::open(export_fn).expect("Failed to open export file");
    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(b' ')
        .has_headers(false)
        .from_reader(std::io::BufReader::new(f));

    let mut pairs = Vec::new();
    for (i, result) in csv_reader.records().enumerate() {
        let record = result.expect("Failed to read record");
        let token_id = record.get(1).expect("Missing token_id");
        let account_id = record.get(0).expect("Missing account_id");
        pairs.push(format!("{}:{}", token_id, account_id));
        if pairs.len() == 1000 {
            let mut new_pairs = vec![];
            std::mem::swap(&mut new_pairs, &mut pairs);
            pairs_sync.send(new_pairs).await.expect("Failed to send");
        }
        if i % 100000 == 0 {
            tracing::info!(target: PROJECT_ID, "Read {} records", i);
        }
    }
}

pub fn export_fn_streamer() -> mpsc::Receiver<Vec<String>> {
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(export_fn_start(sender));
    receiver
}

async fn redis_start(pairs_sync: mpsc::Sender<Vec<String>>) {
    let mut read_redis_db = RedisDB::new(Some(
        env::var("EXPORT_READ_REDIS_URL").expect("Missing env EXPORT_READ_REDIS_URL"),
    ))
    .await;

    let mut accounts = HashSet::new();
    let mut cursor = "0".to_string();
    let mut total_accounts = 0;
    let mut last_multiplier = 0;
    loop {
        let res = with_retries!(read_redis_db, |connection| async {
            let res: redis::RedisResult<(String, Vec<String>)> = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg("ft:*")
                .arg("COUNT")
                .arg(100000)
                .query_async(connection)
                .await;
            res
        })
        .expect("Failed to scan keys");
        let (next_cursor, keys) = res;
        cursor = next_cursor;
        total_accounts += keys.len();
        accounts.extend(keys);
        let mult = accounts.len() / 100000;
        if mult > last_multiplier {
            last_multiplier = mult;
            tracing::info!(target: PROJECT_ID, "Scanned {} accounts", total_accounts);
        }
        if cursor == "0" {
            break;
        }
    }
    tracing::info!(target: PROJECT_ID, "Total accounts: {}", total_accounts);

    let mut total_pairs = 0;
    let mut all_pairs = vec![];
    for (i, account_key) in accounts.into_iter().enumerate() {
        if i % 10000 == 0 {
            tracing::info!(target: PROJECT_ID, "Exported {} tokens out of {}. Total pairs: {}", i, total_accounts, total_pairs);
        }
        let res = with_retries!(read_redis_db, |connection| async {
            let res: redis::RedisResult<Vec<(String, String)>> = redis::cmd("HGETALL")
                .arg(&account_key)
                .query_async(connection)
                .await;
            res
        })
        .expect("Failed to get tokens for account");
        let account_id = account_key.split(':').last().unwrap();
        total_pairs += res.len();
        for (token_id, _) in res {
            all_pairs.push((token_id, account_id.to_string()));
        }
    }

    tracing::info!(target: PROJECT_ID, "Total pairs: {}", total_pairs);

    total_pairs = 0;
    let mut pairs = vec![];
    for chunk in all_pairs.chunks(10000) {
        let res: redis::RedisResult<Vec<Option<String>>> =
            with_retries!(read_redis_db, |connection| async {
                let mut pipe = redis::pipe();
                for (token_id, account_id) in chunk {
                    pipe.cmd("HGET")
                        .arg(format!("b:{}", token_id))
                        .arg(account_id);
                }
                pipe.query_async(connection).await
            });
        let res = res.expect("Failed to get balances for token");
        total_pairs += res.len();
        for (balance, (token_id, account_id)) in res.into_iter().zip(chunk) {
            if balance.is_none() || balance == Some("".to_string()) {
                pairs.push(format!("{}:{}", token_id, account_id));
                if pairs.len() == 1000 {
                    let mut new_pairs = vec![];
                    std::mem::swap(&mut new_pairs, &mut pairs);
                    pairs_sync.send(new_pairs).await.expect("Failed to send");
                }
            }
        }
    }
    if !pairs.is_empty() {
        pairs_sync.send(pairs).await.expect("Failed to send");
    }
    tracing::info!(target: PROJECT_ID, "Total pairs: {}", total_pairs);
}

pub fn redis_streamer() -> mpsc::Receiver<Vec<String>> {
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(redis_start(sender));
    receiver
}

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("balances_backfill=info,redis=info,rpc=debug");

    tracing::log::info!(target: PROJECT_ID, "Starting Balance backfill");

    let rpc_config = rpc::RpcConfig::from_env();

    let args: Vec<String> = std::env::args().collect();
    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("Command argument missing");

    let stream = match command {
        "export" => export_fn_streamer(),
        "empty_ft" => redis_streamer(),
        _ => panic!("Invalid command"),
    };

    let redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    process_balances(stream, redis_db, &rpc_config).await;
}

async fn process_balances(
    mut stream: mpsc::Receiver<Vec<String>>,
    mut redis_db: RedisDB,
    rpc_config: &rpc::RpcConfig,
) {
    let mut total_pairs = 0;
    while let Some(pairs) = stream.recv().await {
        total_pairs += pairs.len();
        tracing::info!(target: PROJECT_ID, "Pair example: {:?}", pairs[0]);
        update_balances(&mut redis_db, pairs, rpc_config).await;
        tracing::info!(target: PROJECT_ID, "Processed {} pairs", total_pairs);
    }
}

async fn update_balances(redis_db: &mut RedisDB, pairs: Vec<String>, rpc_config: &rpc::RpcConfig) {
    let mut tasks = vec![];
    // Pair tasks
    tasks.extend(pairs.iter().map(|pair| {
        let (token_id, account_id) = pair.split_once(':').unwrap();
        let account_id = account_id.to_string();
        RpcTask::FtPair {
            block_height: None,
            token_id: token_id.to_string(),
            account_id: account_id.to_string(),
        }
    }));
    // Fetching balances
    let results = fetch_from_rpc(&tasks, &rpc_config)
        .await
        .expect("Failed to fetch updates from the RPC");

    // Save balances to redis
    let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();
        for RpcResultPair { task, result } in &results {
            if result.is_none() {
                tracing::info!(target: PROJECT_ID, "No result for task {:?}", task);
                continue;
            }
            let (token_id, account_id) = match task {
                RpcTask::FtPair {
                    token_id,
                    account_id,
                    ..
                } => (token_id, account_id),
                _ => unreachable!(),
            };
            let balance = result.as_ref().unwrap().unwrap_as_ft_pair().balance;
            pipe.cmd("HSET")
                .arg(format!("b:{}", token_id))
                .arg(account_id)
                .arg(balance.to_string())
                .ignore();
        }

        pipe.query_async(connection).await
    });
    res.expect("Failed to update");
}
