use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(name = "extract-pool-supply")]
struct Args {
    #[arg(long, default_value = "mainnet")]
    network: String,
    #[arg(long)]
    cookie: Option<PathBuf>,
    #[arg(long)]
    rpc_url: Option<String>,
    #[arg(long, value_delimiter = ',', num_args = 0..)]
    pools: Vec<String>,
    #[arg(long, default_value = "interval")]
    mode: String,
    #[arg(long, default_value_t = 1)]
    block_interval: u32,
    #[arg(long)]
    start_height: Option<u32>,
    #[arg(long)]
    end_height: Option<u32>,
    #[arg(long)]
    last_blocks: Option<u32>,
    #[arg(long, default_value = ".")]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
}

#[derive(Serialize, Clone)]
struct SupplyPoint {
    supply: f64,
    close: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== extract-pool-supply (robust) ===");
    let args = Args::parse();
    let network = args.network.to_lowercase();
    let expected_chain = match network.as_str() {
        "mainnet" => "main",
        "testnet" | "tailnet" => "test",
        other => bail!("Unsupported network '{}'", other),
    };
    let mut selected: Vec<String> = if args.pools.iter().any(|p| p.eq_ignore_ascii_case("all")) {
        vec!["sprout".into(), "sapling".into(), "orchard".into(), "lockbox".into(), "ironwood".into()]
    } else if args.pools.is_empty() {
        vec!["orchard".into()]
    } else {
        args.pools.iter()
            .map(|p| p.to_lowercase())
            .filter(|p| ["sprout", "sapling", "orchard", "lockbox", "ironwood"].contains(&p.as_str()))
            .collect()
    };
    if selected.is_empty() {
        bail!("No valid pools selected");
    }
    selected.sort();
    selected.dedup();
    let rpc_url = args.rpc_url.clone().unwrap_or_else(|| {
        if network == "mainnet" {
            "http://127.0.0.1:8232".into()
        } else {
            "http://127.0.0.1:18232".into()
        }
    });
    let auth: Option<(String, String)> = args.cookie.clone()
        .or_else(|| dirs::cache_dir().map(|mut p| { p.push("zebra/.cookie"); p }))
        .and_then(|path| fs::read_to_string(path).ok())
        .and_then(|c| c.trim().split_once(':').map(|(u, p)| (u.to_string(), p.to_string())));
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let info = call_rpc(&client, &rpc_url, &auth, "getblockchaininfo", serde_json::json!([])).await?;
    let chain_str = info.get("chain").and_then(|v| v.as_str()).unwrap_or("");
    if chain_str != expected_chain {
        bail!("Node is on '{}' but expected '{}'", chain_str, expected_chain);
    }
    let tip_height = info.get("blocks").and_then(|v| v.as_u64()).context("missing blocks")? as u32;
    let (nu5_height, nu6_height, nu63_height, sapling_height) = parse_upgrades(&info);
    println!("Verified: {} chain | tip height {}", chain_str, tip_height);
    let mode_lc = args.mode.to_lowercase();
    let start_h = if let Some(n) = args.last_blocks {
        let calculated = tip_height.saturating_sub(n);
        println!("Using --last-blocks {} → starting from height {}", n, calculated);
        calculated
    } else if let Some(h) = args.start_height {
        h.min(tip_height)
    } else if mode_lc == "current" {
        tip_height
    } else {
        let auto_start = selected.iter()
            .map(|p| get_pool_start(p, nu5_height, nu6_height, nu63_height, sapling_height, &network))
            .min()
            .unwrap_or(0);
        if auto_start > 0 {
            println!("Auto-starting from height {} (earliest selected pool)", auto_start);
        }
        auto_start.min(tip_height)
    };
    let end_h = if args.last_blocks.is_some() {
        tip_height
    } else {
        args.end_height.unwrap_or(tip_height).min(tip_height)
    };
    let heights: Vec<u32> = if mode_lc == "current" && args.last_blocks.is_none() {
        vec![tip_height]
    } else {
        let step = args.block_interval.max(1);
        let mut hs = Vec::new();
        let mut h = start_h;
        while h <= end_h {
            hs.push(h);
            if h == end_h { break; }
            h = h.saturating_add(step).min(end_h);
        }
        hs
    };
    println!("Fetching {} blocks with concurrency {}...", heights.len(), args.concurrency);
    let pb = ProgressBar::new(heights.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let results: Vec<(u32, Result<Value>)> = stream::iter(heights.clone())
        .map(|height| {
            let client = client.clone();
            let rpc_url = rpc_url.clone();
            let auth = auth.clone();
            let pb = pb.clone();
            let sem = semaphore.clone();
            async move {
                let _permit = sem.acquire().await.unwrap();
                let block = fetch_block(&client, &rpc_url, &auth, height).await;
                pb.inc(1);
                (height, block)
            }
        })
        .buffer_unordered(args.concurrency)
        .collect()
        .await;
    pb.finish_and_clear();
    let mut block_map: BTreeMap<u32, Value> = BTreeMap::new();
    for (height, block_result) in results {
        if let Ok(block) = block_result {
            block_map.insert(height, block);
        }
    }
    println!("First processed height: {:?}", block_map.keys().next());
    println!("Last processed height: {:?}", block_map.keys().next_back());
    let mut pool_data: HashMap<String, Vec<SupplyPoint>> = HashMap::new();
    for (_height, block) in block_map {
        let time = block.get("time").and_then(|v| v.as_u64()).unwrap_or(0);
        let close = if time > 0 {
            DateTime::<Utc>::from_timestamp(time as i64, 0)
                .map(|dt| dt.format("%m/%d/%Y").to_string())
                .unwrap_or_default()
        } else {
            "01/01/1970".to_string()
        };
        if let Some(vpools) = block.get("valuePools").and_then(|v| v.as_array()) {
            for p_name in &selected {
                if let Some(pobj) = vpools.iter().find(|p| {
                    p.get("id").and_then(|id| id.as_str()) == Some(p_name.as_str())
                }) {
                    let zat = pobj.get("chainValueZat").and_then(|v| v.as_i64()).unwrap_or(0);
                    let supply = zat as f64 / 100_000_000.0;
                    pool_data
                        .entry(p_name.clone())
                        .or_default()
                        .push(SupplyPoint { supply, close: close.clone() });
                }
            }
        }
    }
    fs::create_dir_all(&args.output_dir)?;
    for (pool, entries) in pool_data {
        if entries.is_empty() {
            continue;
        }
        let path = args.output_dir.join(format!("{}_supply.json", pool));
        fs::write(&path, serde_json::to_string_pretty(&entries)?)?;
        println!("Wrote {} points → {}", entries.len(), path.display());
    }
    println!("Done.");
    Ok(())
}

async fn call_rpc(
    client: &Client,
    url: &str,
    auth: &Option<(String, String)>,
    method: &str,
    params: Value,
) -> Result<Value> {
    let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let mut req = client.post(url).json(&body);
    if let Some((u, p)) = auth {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    let j: Value = resp.json().await?;
    if let Some(err) = j.get("error") {
        if !err.is_null() {
            bail!("RPC error: {}", err);
        }
    }
    j.get("result").cloned().context("no result in response")
}

async fn fetch_block(
    client: &Client,
    url: &str,
    auth: &Option<(String, String)>,
    height: u32,
) -> Result<Value> {
    let params = serde_json::json!([height.to_string(), 1u64]);
    call_rpc(client, url, auth, "getblock", params).await
}

fn parse_upgrades(info: &Value) -> (Option<u32>, Option<u32>, Option<u32>, Option<u32>) {
    let mut nu5 = None;
    let mut nu6 = None;
    let mut nu63 = None;
    let mut sapling = None;
    if let Some(upgrades) = info.get("upgrades").and_then(|u| u.as_object()) {
        for (_id, upg) in upgrades {
            let name = upg.get("name").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
            let h = upg.get("activationheight").and_then(|v| v.as_u64()).map(|x| x as u32);
            if name.contains("nu5") || name.contains("orchard") { nu5 = h; }
            if name.contains("nu6.3") || name.contains("ironwood") { nu63 = h; }
            else if name.contains("nu6") { nu6 = h; }
            if name.contains("sapling") { sapling = h; }
        }
    }
    (nu5, nu6, nu63, sapling)
}

fn get_pool_start(
    pool: &str,
    nu5: Option<u32>,
    nu6: Option<u32>,
    nu63: Option<u32>,
    sapling: Option<u32>,
    network: &str,
) -> u32 {
    match pool {
        "orchard" => nu5.unwrap_or_else(|| if network == "mainnet" { 1_687_104 } else { 1_842_420 }),
        "lockbox" => nu6.unwrap_or_else(|| if network == "mainnet" { 3_364_600 } else { 4_052_000 }),
        "ironwood" => nu63.unwrap_or_else(|| if network == "mainnet" { 3_417_100 } else { 4_134_000 }),
        "sapling" => sapling.unwrap_or(280_000),
        "sprout" => 0,
        _ => 0,
    }
}