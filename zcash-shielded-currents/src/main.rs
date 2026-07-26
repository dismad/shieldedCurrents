use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::future::join_all;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;
#[derive(Parser, Debug)]
#[command(author, version, about, arg_required_else_help = true)]
struct Args {
    #[arg(long, default_value_t = 500)]
    last: u32,
    #[arg(long)]
    diagnose: bool,
    #[arg(long, alias = "test-block")]
    block: Option<u32>,
    #[arg(long)]
    from: Option<u32>,
    #[arg(long)]
    to: Option<u32>,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    rpc: String,
    #[arg(long)]
    cookie_file: Option<String>,
    #[arg(long, default_value = "./results")]
    out: String,
    #[arg(long, default_value_t = 32)]
    parallel: usize,
    #[arg(long, default_value_t = false)]
    skip_fees: bool,
    #[arg(long, default_value_t = 480)]
    batch_size: usize,
    #[arg(long, default_value_t = false)]
    verbose: bool,
}
#[derive(Deserialize, Debug, Clone, Default)]
struct Vin {
    coinbase: Option<String>,
    txid: Option<String>,
    vout: Option<u32>,
}
#[derive(Deserialize, Debug, Clone, Default)]
struct Vout {
    #[serde(rename = "valueZat", default)]
    value_zat: i64,
}
#[derive(Deserialize, Debug, Clone, Default)]
struct ShieldedPool {
    actions: Option<Vec<Value>>,
    #[serde(rename = "valueBalanceZat", default)]
    value_balance_zat: Option<i64>,
}
#[derive(Deserialize, Debug, Clone, Default)]
struct RawTx {
    #[serde(default)]
    txid: String,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    time: Option<u64>,
    #[serde(default)]
    vin: Vec<Vin>,
    #[serde(default)]
    vout: Vec<Vout>,
    #[serde(rename = "vjoinsplit", default)]
    v_joinsplit: Option<Vec<Value>>,
    #[serde(rename = "vShieldedSpend", default)]
    v_shielded_spend: Option<Vec<Value>>,
    #[serde(rename = "vShieldedOutput", default)]
    v_shielded_output: Option<Vec<Value>>,
    #[serde(default)]
    orchard: Option<ShieldedPool>,
    #[serde(default)]
    ironwood: Option<ShieldedPool>,
    #[serde(rename = "valueBalanceZat", default)]
    value_balance_zat: Option<i64>,
}
#[derive(Debug, Clone)]
struct TxMetrics {
    block: u32,
    time: u64,
    txid: String,
    transfers: u32,
    fee_zec: f64,
    value_out: f64,
    transparent: f64,
    sapling: f64,
    orchard: f64,
    ironwood: f64,
    pool_type: String,
    is_coinbase: bool,
    vout_count: u32,
    vshielded_count: u32,
    orchard_count: u32,
    ironwood_count: u32,
}
#[derive(Clone)]
struct Rpc {
    client: reqwest::Client,
    url: String,
    prevout_cache: Arc<Mutex<HashMap<String, Vec<i64>>>>,
    auth: Option<(String, String)>,
}
impl Rpc {
    fn new(url: String, cookie_file: Option<String>) -> Result<Self> {
        let auth = if let Some(path) = cookie_file {
            Self::read_cookie(&path)?
        } else {
            Self::auto_detect_cookie()?
        };
        Ok(Self {
            client: reqwest::Client::new(),
            url,
            prevout_cache: Arc::new(Mutex::new(HashMap::new())),
            auth,
        })
    }
    fn read_cookie(path: &str) -> Result<Option<(String, String)>> {
        let content =
            fs::read_to_string(path).context(format!("Cannot read cookie file: {}", path))?;
        let line = content.trim();
        if let Some((user, pass)) = line.split_once(':') {
            Ok(Some((user.to_string(), pass.to_string())))
        } else {
            anyhow::bail!("Invalid cookie format in {}", path)
        }
    }
    fn auto_detect_cookie() -> Result<Option<(String, String)>> {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
        let candidates = [
            format!("{}/.cache/zebra/.cookie", home),
            format!("{}/.zcash/.cookie", home),
            "/home/zebra/.cache/zebra/.cookie".to_string(),
            "/var/lib/zebrad-rpc/.cookie".to_string(),
        ];
        for path in candidates {
            if Path::new(&path).exists() {
                if let Ok(Some(auth)) = Self::read_cookie(&path) {
                    println!(".cookie found");
                    return Ok(Some(auth));
                }
            }
        }
        println!("No cookie file found - running without auth");
        Ok(None)
    }
    async fn rpc<T: for<'de> Deserialize<'de>>(&self, method: &str, params: Value) -> Result<T> {
        let mut req = self.client.post(&self.url).json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }));
        if let Some((user, pass)) = &self.auth {
            req = req.basic_auth(user, Some(pass));
        }
        let res: Value = req.send().await?.json().await?;
        if let Some(err) = res.get("error") {
            anyhow::bail!("Zebra RPC error: {:?}", err);
        }
        let result = res.get("result").context("no 'result' field")?;
        if result.is_null() {
            anyhow::bail!("Zebra returned null result");
        }
        serde_json::from_value(result.clone()).context("deserialize failed")
    }
    async fn batch_get_raw_transactions(&self, txids: &[String]) -> Result<Vec<RawTx>> {
        if txids.is_empty() {
            return Ok(vec![]);
        }
        let mut requests = vec![];
        for (i, txid) in txids.iter().enumerate() {
            requests.push(serde_json::json!({
                "jsonrpc": "2.0",
                "method": "getrawtransaction",
                "params": [txid, 1],
                "id": i
            }));
        }
        let mut req = self.client.post(&self.url).json(&requests);
        if let Some((user, pass)) = &self.auth {
            req = req.basic_auth(user, Some(pass));
        }
        let responses: Vec<Value> = req.send().await?.json().await?;
        let mut results = vec![];
        for resp in responses {
            if let Some(result) = resp.get("result") {
                if let Ok(tx) = serde_json::from_value::<RawTx>(result.clone()) {
                    results.push(tx);
                }
            }
        }
        Ok(results)
    }
    async fn get_block(&self, height: u32) -> Result<Value> {
        if let Ok(block) = self.rpc("getblock", serde_json::json!([height, 2])).await {
            return Ok(block);
        }
        let hash: String = self
            .rpc("getblockhash", serde_json::json!([height]))
            .await?;
        self.rpc("getblock", serde_json::json!([hash, 2])).await
    }
}
fn detect_pools_and_values(
    tx: &RawTx,
) -> (String, bool, i64, i64, i64, i64, u32, u32, u32, u32, u32) {
    let is_cb = tx.vin.first().and_then(|v| v.coinbase.as_deref()).is_some();
    let mut pools = Vec::new();
    if tx.vin.iter().any(|v| v.txid.is_some()) || tx.vout.iter().any(|v| v.value_zat > 0) {
        pools.push("Transparent");
    }
    if tx.v_joinsplit.as_ref().map_or(false, |v| !v.is_empty()) {
        pools.push("Sprout");
    }
    if tx
        .v_shielded_spend
        .as_ref()
        .map_or(false, |v| !v.is_empty())
        || tx
            .v_shielded_output
            .as_ref()
            .map_or(false, |v| !v.is_empty())
    {
        pools.push("Sapling");
    }
    if tx
        .orchard
        .as_ref()
        .and_then(|o| o.actions.as_ref())
        .map_or(false, |a| !a.is_empty())
    {
        pools.push("Orchard");
    }
    if tx
        .ironwood
        .as_ref()
        .and_then(|o| o.actions.as_ref())
        .map_or(false, |a| !a.is_empty())
    {
        pools.push("Ironwood");
    }
    let pool_type = if is_cb {
        "Coinbase".to_string()
    } else if pools.is_empty() {
        "Unknown".to_string()
    } else {
        pools.join(",")
    };
    let vout_count = tx.vout.len() as u32;
    let vshielded_count = tx.v_shielded_output.as_ref().map_or(0, |v| v.len() as u32);
    let orchard_count = tx
        .orchard
        .as_ref()
        .and_then(|o| o.actions.as_ref())
        .map_or(0, |a| a.len() as u32);
    let ironwood_count = tx
        .ironwood
        .as_ref()
        .and_then(|o| o.actions.as_ref())
        .map_or(0, |a| a.len() as u32);
    let transfers = vout_count + vshielded_count + orchard_count + ironwood_count;
    let t = tx.vout.iter().map(|v| v.value_zat).sum::<i64>();
    let s = tx.value_balance_zat.unwrap_or(0);
    let o = tx
        .orchard
        .as_ref()
        .and_then(|or| or.value_balance_zat)
        .unwrap_or(0);
    let i = tx
        .ironwood
        .as_ref()
        .and_then(|iw| iw.value_balance_zat)
        .unwrap_or(0);
    (
        pool_type,
        is_cb,
        t,
        s,
        o,
        i,
        transfers,
        vout_count,
        vshielded_count,
        orchard_count,
        ironwood_count,
    )
}
async fn process_block(
    rpc: Arc<Rpc>,
    height: u32,
    quiet: bool,
    calculate_fees: bool,
    batch_size: usize,
    skipped: Arc<AtomicU32>,
    rpc_errors: Arc<AtomicU32>,
    verbose: bool,
) -> Vec<TxMetrics> {
    let mut res = Vec::new();
    let block_data = match rpc.get_block(height).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Invalid params") {
                skipped.fetch_add(1, Ordering::Relaxed);
                if !quiet && verbose {
                    eprintln!(
                        "Skipping block {}: Invalid params (node not ready yet)",
                        height
                    );
                }
            } else {
                rpc_errors.fetch_add(1, Ordering::Relaxed);
                if !quiet {
                    eprintln!("Error fetching block {}: {}", height, e);
                }
            }
            return res;
        }
    };
    let block_height = block_data["height"].as_u64().unwrap_or(height as u64) as u32;
    let block_time = block_data["time"].as_u64().unwrap_or(0);
    if let Some(tx_field) = block_data.get("tx") {
        if tx_field.is_array() {
            let txs = tx_field.as_array().unwrap();
            if !quiet {
                println!("Block {}: {} transactions", height, txs.len());
            }
            // === Per-block batch pre-fetch for missing prevouts ===
            if calculate_fees {
                let mut needed = HashSet::new();
                for tx_json in txs {
                    if let Ok(tx) = serde_json::from_value::<RawTx>(tx_json.clone()) {
                        for vin in &tx.vin {
                            if let Some(txid) = &vin.txid {
                                let cache = rpc.prevout_cache.lock().unwrap();
                                if !cache.contains_key(txid) {
                                    needed.insert(txid.clone());
                                }
                            }
                        }
                    }
                }
                if !needed.is_empty() {
                    let txid_vec: Vec<String> = needed.into_iter().collect();
                    for chunk in txid_vec.chunks(batch_size) {
                        if let Ok(batch) = rpc.batch_get_raw_transactions(chunk).await {
                            let mut cache = rpc.prevout_cache.lock().unwrap();
                            for tx in batch {
                                let vals: Vec<i64> = tx.vout.iter().map(|v| v.value_zat).collect();
                                cache.insert(tx.txid.clone(), vals);
                            }
                        }
                    }
                }
            }
            // === Main transaction processing loop ===
            for tx_json in txs {
                match serde_json::from_value::<RawTx>(tx_json.clone()) {
                    Ok(mut tx) => {
                        if tx.height.is_none() {
                            tx.height = Some(block_height);
                        }
                        if tx.time.is_none() {
                            tx.time = Some(block_time);
                        }
                        // Store this tx's own vouts in cache
                        let vout_values: Vec<i64> = tx.vout.iter().map(|v| v.value_zat).collect();
                        {
                            let mut cache = rpc.prevout_cache.lock().unwrap();
                            cache.insert(tx.txid.clone(), vout_values);
                        }
                        let (
                            pool_type,
                            is_cb,
                            t_zat,
                            s_zat,
                            o_zat,
                            i_zat,
                            transfers,
                            vout_count,
                            vshielded_count,
                            orchard_count,
                            ironwood_count,
                        ) = detect_pools_and_values(&tx);
                        // ==================== CORRECT FEE CALCULATION (matches zcash-block-fees) ====================
                        let fee_zat = if !calculate_fees || is_cb {
                            0
                        } else {
                            let mut input_sum = 0i64;
                            for vin in &tx.vin {
                                if let (Some(txid), Some(vout_idx)) = (&vin.txid, vin.vout) {
                                    let cache = rpc.prevout_cache.lock().unwrap();
                                    if let Some(vals) = cache.get(txid) {
                                        if (vout_idx as usize) < vals.len() {
                                            input_sum += vals[vout_idx as usize];
                                        }
                                    }
                                }
                            }
                            let output_sum = tx.vout.iter().map(|v| v.value_zat).sum::<i64>();
                            // Sprout vjoinsplit handling (vpub_old / vpub_new) - this was the missing piece
                            let mut vpub_old = 0i64;
                            let mut vpub_new = 0i64;
                            if let Some(js) = &tx.v_joinsplit {
                                for j in js {
                                    vpub_old += j["vpub_oldZat"].as_i64().unwrap_or(0);
                                    vpub_new += j["vpub_newZat"].as_i64().unwrap_or(0);
                                }
                            }
                            // Official Zcash fee formula (value balances for Sapling/Orchard/Ironwood)
                            input_sum - output_sum - vpub_old + vpub_new + s_zat + o_zat + i_zat
                        };
                        // ============================================================================================
                        res.push(TxMetrics {
                            block: tx.height.unwrap_or(block_height),
                            time: tx.time.unwrap_or(block_time),
                            txid: tx.txid,
                            transfers,
                            fee_zec: fee_zat as f64 / 100_000_000.0,
                            value_out: (t_zat + s_zat + o_zat + i_zat) as f64 / 100_000_000.0,
                            transparent: t_zat as f64 / 100_000_000.0,
                            sapling: s_zat as f64 / 100_000_000.0,
                            orchard: o_zat as f64 / 100_000_000.0,
                            ironwood: i_zat as f64 / 100_000_000.0,
                            pool_type,
                            is_coinbase: is_cb,
                            vout_count,
                            vshielded_count,
                            orchard_count,
                            ironwood_count,
                        });
                    }
                    Err(e) => {
                        if !quiet {
                            eprintln!("Block {} tx deserial error: {}", height, e);
                        }
                    }
                }
            }
        }
    }
    res
}
#[tokio::main]
async fn main() -> Result<()> {
    print!("Connecting to Zebrad now | ");
    let args = Args::parse();
    let out_dir = Path::new(&args.out);
    fs::create_dir_all(out_dir)?;
    let rpc = Arc::new(Rpc::new(args.rpc.clone(), args.cookie_file)?);
    let start_time = Instant::now();
    if let Err(e) = rpc
        .rpc::<Value>("getblockchaininfo", serde_json::json!([]))
        .await
    {
        eprintln!("Cannot connect to Zebra at {}: {}", args.rpc, e);
        return Ok(());
    }
    let skipped = Arc::new(AtomicU32::new(0));
    let rpc_errors = Arc::new(AtomicU32::new(0));
    if args.diagnose {
        println!("Running full RPC diagnostics...");
        let info: Value = rpc.rpc("getblockchaininfo", serde_json::json!([])).await?;
        let tip: u32 = info["blocks"].as_u64().context("no blocks field")? as u32;
        println!("RPC connection successful!");
        println!("Tip height: {}", tip);
        println!("Chain: {}", info["chain"]);
        println!("Best block hash: {}", info["bestblockhash"]);
        let test_height = if tip > 1000 { tip - 1000 } else { tip / 2 };
        match rpc.get_block(test_height).await {
            Ok(block) => println!("getblock({}) succeeded", block["height"]),
            Err(e) => println!("getblock failed (normal for recent blocks): {}", e),
        }
        println!("Diagnostics complete.");
        let elapsed = start_time.elapsed();
        println!("Completed in {:.2} seconds", elapsed.as_secs_f64());
        return Ok(());
    }
    if let Some(height) = args.block {
        println!("Testing single block {}", height);
        let info: Value = rpc.rpc("getblockchaininfo", serde_json::json!([])).await?;
        println!("Current blockchain info:");
        println!(" Tip height : {}", info["blocks"]);
        println!(" Best block hash : {}", info["bestblockhash"]);
        println!(" Chain : {}", info["chain"]);
        println!(" Validated height : {}", info["blocks"]);
        println!(" Blocks : {}", info["blocks"]);
        println!(" Headers : {}", info["headers"]);
        println!(" Difficulty : {}", info["difficulty"]);
        let metrics = process_block(
            rpc.clone(),
            height,
            false,
            !args.skip_fees,
            args.batch_size,
            skipped.clone(),
            rpc_errors.clone(),
            args.verbose,
        )
        .await;
        println!("Block {} processed: {} transactions", height, metrics.len());
        write_myresults_md(&metrics, out_dir)?;
        write_summary_md(&metrics, height, height, out_dir, &rpc).await?;
        write_pool_currents(&metrics, out_dir)?;
        write_all_combo_files(&metrics, out_dir)?;
        println!("Single-block output written to {}/", out_dir.display());
        let elapsed = start_time.elapsed();
        println!("Completed in {:.2} seconds", elapsed.as_secs_f64());
        return Ok(());
    }
    let (start, end) = if let (Some(f), Some(t)) = (args.from, args.to) {
        if f > t {
            anyhow::bail!("--from must be <= --to");
        }
        (f, t)
    } else {
        let info: Value = rpc.rpc("getblockchaininfo", serde_json::json!([])).await?;
        let tip: u32 = info["blocks"].as_u64().context("no blocks field")? as u32;
        let end = tip;
        let start = end.saturating_sub(args.last.saturating_sub(1));
        (start, end)
    };
    println!(
        "Processing blocks {}–{} with max {} concurrent RPCs ",
        start, end, args.parallel
    );
    let semaphore = Arc::new(Semaphore::new(args.parallel));
    let tasks: Vec<_> = (start..=end)
        .map(|h| {
            let rpc_clone = rpc.clone();
            let sem_clone = semaphore.clone();
            let skipped_clone = skipped.clone();
            let rpc_errors_clone = rpc_errors.clone();
            let batch_size = args.batch_size;
            let verbose = args.verbose;
            tokio::spawn(async move {
                let _permit = sem_clone.acquire().await.unwrap();
                process_block(
                    rpc_clone,
                    h,
                    true,
                    !args.skip_fees,
                    batch_size,
                    skipped_clone,
                    rpc_errors_clone,
                    verbose,
                )
                .await
            })
        })
        .collect();
    let all_results = join_all(tasks).await;
    let all_metrics: Vec<TxMetrics> = all_results
        .into_iter()
        .filter_map(|r| r.ok())
        .flatten()
        .collect();
    let total_skipped = skipped.load(Ordering::Relaxed);
    let total_errors = rpc_errors.load(Ordering::Relaxed);
    println!("Processed {} transactions", all_metrics.len());
    println!(
        "Error summary: {} blocks skipped | {} RPC errors",
        total_skipped, total_errors
    );
    write_myresults_md(&all_metrics, out_dir)?;
    write_summary_md(&all_metrics, start, end, out_dir, &rpc).await?;
    write_pool_currents(&all_metrics, out_dir)?;
    write_all_combo_files(&all_metrics, out_dir)?;
    println!("All outputs written to {}/", out_dir.display());
    let elapsed = start_time.elapsed();
    println!("Completed in {:.2} seconds", elapsed.as_secs_f64());
    Ok(())
}
fn format_date(ts: u64) -> String {
    DateTime::<Utc>::from_timestamp(ts as i64, 0)
        .unwrap_or_default()
        .format("%c")
        .to_string()
}
fn write_myresults_md(metrics: &[TxMetrics], out: &Path) -> Result<()> {
    let mut f = File::create(out.join("myresults.md"))?;
    writeln!(
        f,
        "================================================================================"
    )?;
    writeln!(f, "Finding TXs ...")?;
    writeln!(f)?;
    for m in metrics {
        let date = format_date(m.time);
        let cb = if m.is_coinbase { "IsCoinbase" } else { "" };
        writeln!(
            f,
            "{} | {} | {} | {} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {} | {}",
            date,
            m.block,
            m.txid,
            m.transfers,
            m.fee_zec,
            m.value_out,
            m.transparent,
            m.sapling,
            m.orchard,
            m.ironwood,
            m.pool_type,
            cb
        )?;
    }
    Ok(())
}
async fn write_summary_md(
    metrics: &[TxMetrics],
    start: u32,
    end: u32,
    out: &Path,
    rpc: &Rpc,
) -> Result<()> {
    let total_txs = metrics.len();
    let pure_t = metrics
        .iter()
        .filter(|m| m.pool_type == "Transparent")
        .count();
    let pure_s = metrics.iter().filter(|m| m.pool_type == "Sapling").count();
    let pure_o = metrics.iter().filter(|m| m.pool_type == "Orchard").count();
    let pure_i = metrics.iter().filter(|m| m.pool_type == "Ironwood").count();
    let pure_sprout = metrics.iter().filter(|m| m.pool_type == "Sprout").count();
    let mixed_total = metrics.iter().filter(|m| m.pool_type.contains(',')).count();
    let cb = metrics.iter().filter(|m| m.is_coinbase).count();
    let unknown = metrics.iter().filter(|m| m.pool_type == "Unknown").count();
    let sum = pure_t + pure_s + pure_o + pure_i + pure_sprout + mixed_total + cb + unknown;
    println!("Pool verification: Pure T={} | S={} | O={} | I={} | Sprout={} | Mixed={} | CB={} | Unknown={} → Sum={} / Total={}",
        pure_t, pure_s, pure_o, pure_i, pure_sprout, mixed_total, cb, unknown, sum, total_txs);
    // ==================== COMPLETE MIXED BREAKDOWN ====================
    // Every possible multi-pool combination in detector order.
    // Residual must stay 0.
    let all_mixed_types: &[(&str, &str)] = &[
        // 2-pool
        ("Transparent,Sprout",                "Transparent + Sprout"),
        ("Transparent,Sapling",               "Transparent + Sapling"),
        ("Transparent,Orchard",               "Transparent + Orchard"),
        ("Transparent,Ironwood",              "Transparent + Ironwood"),
        ("Sprout,Sapling",                    "Sprout + Sapling"),
        ("Sprout,Orchard",                    "Sprout + Orchard"),
        ("Sprout,Ironwood",                   "Sprout + Ironwood"),
        ("Sapling,Orchard",                   "Sapling + Orchard"),
        ("Sapling,Ironwood",                  "Sapling + Ironwood"),
        ("Orchard,Ironwood",                  "Orchard + Ironwood"),
        // 3-pool
        ("Transparent,Sprout,Sapling",        "T + Sprout + Sapling"),
        ("Transparent,Sprout,Orchard",        "T + Sprout + Orchard"),
        ("Transparent,Sprout,Ironwood",       "T + Sprout + Ironwood"),
        ("Transparent,Sapling,Orchard",       "Transparent + Sapling + Orchard"),
        ("Transparent,Sapling,Ironwood",      "T + Sapling + Ironwood"),
        ("Transparent,Orchard,Ironwood",      "T + Orchard + Ironwood"),
        ("Sprout,Sapling,Orchard",            "Sprout + Sapling + Orchard"),
        ("Sprout,Sapling,Ironwood",           "Sprout + Sapling + Ironwood"),
        ("Sprout,Orchard,Ironwood",           "Sprout + Orchard + Ironwood"),
        ("Sapling,Orchard,Ironwood",          "Sapling + Orchard + Ironwood"),
        // 4-pool
        ("Transparent,Sprout,Sapling,Orchard","T + Sprout + S + O"),
        ("Transparent,Sprout,Sapling,Ironwood","T + Sprout + S + I"),
        ("Transparent,Sprout,Orchard,Ironwood","T + Sprout + O + I"),
        ("Transparent,Sapling,Orchard,Ironwood","T + S + O + Ironwood"),
        ("Sprout,Sapling,Orchard,Ironwood",   "Sprout + S + O + I"),
        // 5-pool
        ("Transparent,Sprout,Sapling,Orchard,Ironwood", "All five pools"),
    ];

    let mut mixed_counts: Vec<(&str, &str, usize)> = Vec::new();
    let mut accounted = 0usize;
    for &(key, label) in all_mixed_types {
        let c = metrics.iter().filter(|m| m.pool_type == key).count();
        mixed_counts.push((key, label, c));
        accounted += c;
    }
    let residual_mixed = mixed_total.saturating_sub(accounted);

    println!("\nComplete Mixed Transaction Breakdown:");
    for &(_, label, c) in &mixed_counts {
        println!(" {:<37} : {:>6}", label, c);
    }
    println!(" {:<37} : {:>6}", "Residual (must be 0)", residual_mixed);
    println!(" {:<37} : {:>6}", "Total Mixed", mixed_total);

    // ==================== PERCENTAGE MATRIX ====================
    let total = total_txs as f64;
    println!(
        "\nTransaction Type Percentages (of {} total transactions):",
        total_txs
    );
    println!(" {:<37} : {:>6} ({:.2}%)", "Pure Transparent", pure_t, (pure_t as f64 / total * 100.0));
    println!(" {:<37} : {:>6} ({:.2}%)", "Pure Sapling", pure_s, (pure_s as f64 / total * 100.0));
    println!(" {:<37} : {:>6} ({:.2}%)", "Pure Orchard", pure_o, (pure_o as f64 / total * 100.0));
    println!(" {:<37} : {:>6} ({:.2}%)", "Pure Ironwood", pure_i, (pure_i as f64 / total * 100.0));
    println!(" {:<37} : {:>6} ({:.2}%)", "Pure Sprout", pure_sprout, (pure_sprout as f64 / total * 100.0));
    for &(_, label, c) in &mixed_counts {
        if c > 0 {
            println!(" {:<37} : {:>6} ({:.2}%)", format!("Mixed {}", label), c, (c as f64 / total * 100.0));
        }
    }
    if residual_mixed > 0 {
        println!(" {:<37} : {:>6} ({:.2}%)", "Residual Mixed", residual_mixed, (residual_mixed as f64 / total * 100.0));
    }
    println!(" {:<37} : {:>6} ({:.2}%)", "Coinbase", cb, (cb as f64 / total * 100.0));
    println!(" {:<37} : {:>6} ({:.2}%)", "Unknown", unknown, (unknown as f64 / total * 100.0));
    println!(" {}", "─".repeat(55));
    println!(" {:<37} : {:>6} (100.00%)", "TOTAL", total_txs);
    // ============================================================
    // (rest of the function unchanged - only the content string gets the matrix added)
    let coinbase_count = cb;
    let coinbase_pct = if total_txs > 0 {
        (coinbase_count as f64 / total_txs as f64 * 100.0).round()
    } else {
        0.0
    };
    let t_transfer: u32 = metrics.iter().map(|m| m.vout_count).sum();
    let s_transfer: u32 = metrics.iter().map(|m| m.vshielded_count).sum();
    let o_transfer: u32 = metrics.iter().map(|m| m.orchard_count).sum();
    let i_transfer: u32 = metrics.iter().map(|m| m.ironwood_count).sum();
    let total_transfer = t_transfer + s_transfer + o_transfer + i_transfer;

    // Option B: any shielded involvement
    // A tx counts as shielded if it has any Sapling/Orchard/Ironwood/Sprout activity.
    // Pure transparent coinbases (no shielded value) do NOT count.
    let any_shielded = metrics
        .iter()
        .filter(|m| {
            m.sapling != 0.0
                || m.orchard != 0.0
                || m.ironwood != 0.0
                || m.pool_type.contains("Sprout")
                || m.pool_type.contains("Sapling")
                || m.pool_type.contains("Orchard")
                || m.pool_type.contains("Ironwood")
                || m.vshielded_count > 0
                || m.orchard_count > 0
                || m.ironwood_count > 0
        })
        .count();

    let shielded_pct = if total_txs > 0 {
        ((any_shielded as f64 / total_txs as f64) * 10000.0).round() / 100.0
    } else {
        0.0
    };

    // Keep a pure-transparent count for the "T txs" line (transparent-only or pure transparent coinbase)
    let t_tx_count = metrics
        .iter()
        .filter(|m| {
            m.pool_type == "Transparent"
                || (m.is_coinbase
                    && m.sapling == 0.0
                    && m.orchard == 0.0
                    && m.ironwood == 0.0
                    && m.vshielded_count == 0
                    && m.orchard_count == 0
                    && m.ironwood_count == 0)
        })
        .count();

    let s_in = metrics.iter().filter(|m| m.sapling < 0.0).count();
    let s_out = metrics.iter().filter(|m| m.sapling > 0.0).count();
    let s_total = s_in + s_out;
    let s_pct = if total_txs > 0 {
        (s_total as f64 / total_txs as f64 * 100.0).round()
    } else {
        0.0
    };
    let o_in = metrics.iter().filter(|m| m.orchard < 0.0).count();
    let o_out = metrics.iter().filter(|m| m.orchard > 0.0).count();
    let o_total = o_in + o_out;
    let o_pct = if total_txs > 0 {
        (o_total as f64 / total_txs as f64 * 100.0).round()
    } else {
        0.0
    };
    let i_in = metrics.iter().filter(|m| m.ironwood < 0.0).count();
    let i_out = metrics.iter().filter(|m| m.ironwood > 0.0).count();
    let i_total = i_in + i_out;
    let i_pct = if total_txs > 0 {
        (i_total as f64 / total_txs as f64 * 100.0).round()
    } else {
        0.0
    };
    let s_inflow: f64 = metrics
        .iter()
        .filter(|m| m.sapling < 0.0)
        .map(|m| m.sapling)
        .sum();
    let s_outflow: f64 = metrics
        .iter()
        .filter(|m| m.sapling > 0.0)
        .map(|m| m.sapling)
        .sum();
    let s_flow = s_outflow + s_inflow;
    let o_inflow: f64 = metrics
        .iter()
        .filter(|m| m.orchard < 0.0)
        .map(|m| m.orchard)
        .sum();
    let o_outflow: f64 = metrics
        .iter()
        .filter(|m| m.orchard > 0.0)
        .map(|m| m.orchard)
        .sum();
    let o_flow = o_outflow + o_inflow;
    let i_inflow: f64 = metrics
        .iter()
        .filter(|m| m.ironwood < 0.0)
        .map(|m| m.ironwood)
        .sum();
    let i_outflow: f64 = metrics
        .iter()
        .filter(|m| m.ironwood > 0.0)
        .map(|m| m.ironwood)
        .sum();
    let i_flow = i_outflow + i_inflow;
    let info = rpc
        .rpc::<Value>("getblockchaininfo", serde_json::json!([]))
        .await?;
    let chain_supply = info["chainSupply"]["chainValue"].as_f64().unwrap_or(0.0);
    let value_pools = if let Some(pools) = info["valuePools"].as_array() {
        pools
    } else {
        &vec![]
    };
    let mut transparent = 0.0;
    let mut sprout = 0.0;
    let mut sapling = 0.0;
    let mut orchard = 0.0;
    let mut ironwood = 0.0;
    let mut lockbox = 0.0;
    for pool in value_pools {
        if let (Some(id), Some(val)) = (pool["id"].as_str(), pool["chainValue"].as_f64()) {
            match id {
                "transparent" => transparent = val,
                "sprout" => sprout = val,
                "sapling" => sapling = val,
                "orchard" => orchard = val,
                "ironwood" => ironwood = val,
                "lockbox" => lockbox = val,
                _ => {}
            }
        }
    }
    let total_chain = chain_supply;
    let total_shielded = sprout + sapling + orchard + ironwood + lockbox;
    let mut mixed_breakdown = String::from("Complete Mixed Transaction Breakdown:\n");
    for &(_, label, c) in &mixed_counts {
        mixed_breakdown.push_str(&format!(" {:<37} : {:>6}\n", label, c));
    }
    mixed_breakdown.push_str(&format!(" {:<37} : {:>6}\n", "Residual (must be 0)", residual_mixed));
    mixed_breakdown.push_str(&format!(" {:<37} : {:>6}\n", "Total Mixed", mixed_total));
    let mut percentage_matrix = format!(
        "Transaction Type Percentages (of {} total transactions):\n",
        total_txs
    );
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Pure Transparent", pure_t, (pure_t as f64 / total * 100.0)));
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Pure Sapling", pure_s, (pure_s as f64 / total * 100.0)));
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Pure Orchard", pure_o, (pure_o as f64 / total * 100.0)));
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Pure Ironwood", pure_i, (pure_i as f64 / total * 100.0)));
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Pure Sprout", pure_sprout, (pure_sprout as f64 / total * 100.0)));
    for &(_, label, c) in &mixed_counts {
        if c > 0 {
            percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", format!("Mixed {}", label), c, (c as f64 / total * 100.0)));
        }
    }
    if residual_mixed > 0 {
        percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Residual Mixed", residual_mixed, (residual_mixed as f64 / total * 100.0)));
    }
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Coinbase", cb, (cb as f64 / total * 100.0)));
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} ({:.2}%)\n", "Unknown", unknown, (unknown as f64 / total * 100.0)));
    percentage_matrix.push_str(&format!(" {}\n", "─".repeat(55)));
    percentage_matrix.push_str(&format!(" {:<37} : {:>6} (100.00%)", "TOTAL", total_txs));
    // Fixed: use a raw string to avoid the "multiple lines skipped by escaped newline" warnings
    let content = format!(
        r#"Between [{start}],[{end}]
{total_txs} txs
{coinbase_count} coinbase txs (=> {coinbase_pct:.2}% Coinbase txs)
{t_transfer} t transfer txs
{s_transfer} s transfer txs
{o_transfer} o transfer txs
{i_transfer} i transfer txs
{total_transfer} total transfer txs
T txs : {t_tx_count} (=> {shielded_pct:.2}% Shielded)
S txs in : {s_in}
S txs out : {s_out}
S txs : {s_total} ( {s_pct:.2}% )
S Inflows : {s_inflow:.8} ZEC
S Outflows: {s_outflow:.8} ZEC
S flow => : {s_flow:.8} ZEC
O txs in : {o_in}
O txs out : {o_out}
O txs : {o_total} ( {o_pct:.2}% )
O Inflows : {o_inflow:.8} ZEC
O Outflows: {o_outflow:.8} ZEC
O flow => : {o_flow:.8} ZEC
I txs in : {i_in}
I txs out : {i_out}
I txs : {i_total} ( {i_pct:.2}% )
I Inflows : {i_inflow:.8} ZEC
I Outflows: {i_outflow:.8} ZEC
I flow => : {i_flow:.8} ZEC

{mixed_breakdown}
{percentage_matrix}

Total Chain supply       : {total_chain:.8}
Total Transparent supply : {transparent:.8}
Total Sprout supply      : {sprout:.8}
Total Sapling supply     : {sapling:.8}
Total Orchard supply     : {orchard:.8}
Total Ironwood supply    : {ironwood:.8}
Total Lockbox supply     : {lockbox:.8}
----------------------------------------
Total Shielded supply    : {total_shielded:.8}
\_-ZECHUB-_/
"#,
        start = start,
        end = end,
        total_txs = total_txs,
        coinbase_count = coinbase_count,
        coinbase_pct = coinbase_pct,
        t_transfer = t_transfer,
        s_transfer = s_transfer,
        o_transfer = o_transfer,
        i_transfer = i_transfer,
        total_transfer = total_transfer,
        t_tx_count = t_tx_count,
        shielded_pct = shielded_pct,
        s_in = s_in,
        s_out = s_out,
        s_total = s_total,
        s_pct = s_pct,
        s_inflow = s_inflow,
        s_outflow = s_outflow,
        s_flow = s_flow,
        o_in = o_in,
        o_out = o_out,
        o_total = o_total,
        o_pct = o_pct,
        o_inflow = o_inflow,
        o_outflow = o_outflow,
        o_flow = o_flow,
        i_in = i_in,
        i_out = i_out,
        i_total = i_total,
        i_pct = i_pct,
        i_inflow = i_inflow,
        i_outflow = i_outflow,
        i_flow = i_flow,
        total_chain = total_chain,
        transparent = transparent,
        sprout = sprout,
        sapling = sapling,
        orchard = orchard,
        ironwood = ironwood,
        lockbox = lockbox,
        total_shielded = total_shielded,
        mixed_breakdown = mixed_breakdown,
        percentage_matrix = percentage_matrix
    );
    fs::write(out.join("summaryOnly.md"), content)?;
    Ok(())
}

fn write_all_combo_files(metrics: &[TxMetrics], out: &Path) -> Result<()> {
    let combo_dir = out.join("combos");
    fs::create_dir_all(&combo_dir)?;

    let pures: &[(&str, &str)] = &[
        ("Transparent", "pure_Transparent.md"),
        ("Sprout",      "pure_Sprout.md"),
        ("Sapling",     "pure_Sapling.md"),
        ("Orchard",     "pure_Orchard.md"),
        ("Ironwood",    "pure_Ironwood.md"),
        ("Coinbase",    "pure_Coinbase.md"),
        ("Unknown",     "pure_Unknown.md"),
    ];

    let mixes: &[(&str, &str)] = &[
        ("Transparent,Sprout",                "mixed_Transparent_Sprout.md"),
        ("Transparent,Sapling",               "mixed_Transparent_Sapling.md"),
        ("Transparent,Orchard",               "mixed_Transparent_Orchard.md"),
        ("Transparent,Ironwood",              "mixed_Transparent_Ironwood.md"),
        ("Sprout,Sapling",                    "mixed_Sprout_Sapling.md"),
        ("Sprout,Orchard",                    "mixed_Sprout_Orchard.md"),
        ("Sprout,Ironwood",                   "mixed_Sprout_Ironwood.md"),
        ("Sapling,Orchard",                   "mixed_Sapling_Orchard.md"),
        ("Sapling,Ironwood",                  "mixed_Sapling_Ironwood.md"),
        ("Orchard,Ironwood",                  "mixed_Orchard_Ironwood.md"),
        ("Transparent,Sprout,Sapling",        "mixed_T_Sprout_Sapling.md"),
        ("Transparent,Sprout,Orchard",        "mixed_T_Sprout_Orchard.md"),
        ("Transparent,Sprout,Ironwood",       "mixed_T_Sprout_Ironwood.md"),
        ("Transparent,Sapling,Orchard",       "mixed_Transparent_Sapling_Orchard.md"),
        ("Transparent,Sapling,Ironwood",      "mixed_T_Sapling_Ironwood.md"),
        ("Transparent,Orchard,Ironwood",      "mixed_T_Orchard_Ironwood.md"),
        ("Sprout,Sapling,Orchard",            "mixed_Sprout_Sapling_Orchard.md"),
        ("Sprout,Sapling,Ironwood",           "mixed_Sprout_Sapling_Ironwood.md"),
        ("Sprout,Orchard,Ironwood",           "mixed_Sprout_Orchard_Ironwood.md"),
        ("Sapling,Orchard,Ironwood",          "mixed_Sapling_Orchard_Ironwood.md"),
        ("Transparent,Sprout,Sapling,Orchard","mixed_T_Sprout_S_O.md"),
        ("Transparent,Sprout,Sapling,Ironwood","mixed_T_Sprout_S_I.md"),
        ("Transparent,Sprout,Orchard,Ironwood","mixed_T_Sprout_O_I.md"),
        ("Transparent,Sapling,Orchard,Ironwood","mixed_T_S_O_Ironwood.md"),
        ("Sprout,Sapling,Orchard,Ironwood",   "mixed_Sprout_S_O_I.md"),
        ("Transparent,Sprout,Sapling,Orchard,Ironwood", "mixed_All_Five.md"),
    ];

    let header = "date | block | txid | transfers | fee | value_out | transparent | sapling | orchard | ironwood | pool_type | coinbase\n";

    for &(pool, filename) in pures {
        let mut f = File::create(combo_dir.join(filename))?;
        f.write_all(header.as_bytes())?;
        for m in metrics.iter().filter(|m| m.pool_type == pool) {
            let date = format_date(m.time);
            let line = format!(
                "{} | {} | {} | {} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {} | {}\n",
                date, m.block, m.txid, m.transfers, m.fee_zec, m.value_out,
                m.transparent, m.sapling, m.orchard, m.ironwood, m.pool_type,
                if m.is_coinbase { "IsCoinbase" } else { "" }
            );
            f.write_all(line.as_bytes())?;
        }
    }

    for &(pool, filename) in mixes {
        let mut f = File::create(combo_dir.join(filename))?;
        f.write_all(header.as_bytes())?;
        for m in metrics.iter().filter(|m| m.pool_type == pool) {
            let date = format_date(m.time);
            let line = format!(
                "{} | {} | {} | {} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {} | {}\n",
                date, m.block, m.txid, m.transfers, m.fee_zec, m.value_out,
                m.transparent, m.sapling, m.orchard, m.ironwood, m.pool_type,
                if m.is_coinbase { "IsCoinbase" } else { "" }
            );
            f.write_all(line.as_bytes())?;
        }
    }

    // Residual catch-all (should stay empty)
    let known: std::collections::HashSet<&str> = pures.iter().map(|x| x.0)
        .chain(mixes.iter().map(|x| x.0))
        .collect();
    let mut residual = File::create(combo_dir.join("residual.md"))?;
    residual.write_all(header.as_bytes())?;
    let mut residual_count = 0usize;
    for m in metrics {
        if !known.contains(m.pool_type.as_str()) {
            residual_count += 1;
            let date = format_date(m.time);
            let line = format!(
                "{} | {} | {} | {} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {} | {}\n",
                date, m.block, m.txid, m.transfers, m.fee_zec, m.value_out,
                m.transparent, m.sapling, m.orchard, m.ironwood, m.pool_type,
                if m.is_coinbase { "IsCoinbase" } else { "" }
            );
            residual.write_all(line.as_bytes())?;
        }
    }
    if residual_count > 0 {
        eprintln!("WARNING: {} transactions landed in residual.md – investigate new pool types", residual_count);
    }
    Ok(())
}

fn write_pool_currents(metrics: &[TxMetrics], out: &Path) -> Result<()> {
    let mut out_t = File::create(out.join("myResultsOutT.md"))?;
    let mut in_s = File::create(out.join("myResultsInS.md"))?;
    let mut out_s = File::create(out.join("myResultsOutS.md"))?;
    let mut in_o = File::create(out.join("myResultsInO.md"))?;
    let mut out_o = File::create(out.join("myResultsOutO.md"))?;
    let mut in_i = File::create(out.join("myResultsInI.md"))?;
    let mut out_i = File::create(out.join("myResultsOutI.md"))?;
    for m in metrics {
        let date = format_date(m.time);
        let line = format!(
            "{} | {} | {} | {} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {:.8} | {} | {}\n",
            date,
            m.block,
            m.txid,
            m.transfers,
            m.fee_zec,
            m.value_out,
            m.transparent,
            m.sapling,
            m.orchard,
            m.ironwood,
            m.pool_type,
            if m.is_coinbase { "IsCoinbase" } else { "" }
        );
        if m.pool_type.contains("Transparent") && m.transparent > 0.0 {
            let _ = out_t.write_all(line.as_bytes());
        }
        if m.pool_type.contains("Sapling") && m.sapling < 0.0 {
            let _ = in_s.write_all(line.as_bytes());
        }
        if m.pool_type.contains("Sapling") && m.sapling > 0.0 {
            let _ = out_s.write_all(line.as_bytes());
        }
        if m.pool_type.contains("Orchard") && m.orchard < 0.0 {
            let _ = in_o.write_all(line.as_bytes());
        }
        if m.pool_type.contains("Orchard") && m.orchard > 0.0 {
            let _ = out_o.write_all(line.as_bytes());
        }
        if m.pool_type.contains("Ironwood") && m.ironwood < 0.0 {
            let _ = in_i.write_all(line.as_bytes());
        }
        if m.pool_type.contains("Ironwood") && m.ironwood > 0.0 {
            let _ = out_i.write_all(line.as_bytes());
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn make_test_tx(
        has_transparent_vout: bool,
        has_transparent_vin: bool,
        has_sprout: bool,
        has_sapling_spend: bool,
        has_sapling_output: bool,
        has_orchard: bool,
        has_ironwood: bool,
        is_coinbase: bool,
    ) -> RawTx {
        RawTx {
            txid: "testtx".to_string(),
            height: Some(1_234_567),
            time: Some(1_700_000_000),
            vin: if is_coinbase {
                vec![Vin {
                    coinbase: Some("0100000000000000".to_string()),
                    txid: None,
                    vout: None,
                }]
            } else if has_transparent_vin {
                vec![Vin {
                    coinbase: None,
                    txid: Some("prevtx".to_string()),
                    vout: Some(0),
                }]
            } else {
                vec![]
            },
            vout: if has_transparent_vout {
                vec![Vout {
                    value_zat: 1_000_000_000,
                }]
            } else {
                vec![]
            },
            v_joinsplit: if has_sprout {
                Some(vec![serde_json::json!({})])
            } else {
                None
            },
            v_shielded_spend: if has_sapling_spend {
                Some(vec![serde_json::json!({})])
            } else {
                None
            },
            v_shielded_output: if has_sapling_output {
                Some(vec![serde_json::json!({})])
            } else {
                None
            },
            orchard: if has_orchard {
                Some(ShieldedPool {
                    actions: Some(vec![serde_json::json!({})]),
                    value_balance_zat: Some(-500_000_000),
                })
            } else {
                None
            },
            ironwood: if has_ironwood {
                Some(ShieldedPool {
                    actions: Some(vec![serde_json::json!({})]),
                    value_balance_zat: Some(-300_000_000),
                })
            } else {
                None
            },
            value_balance_zat: if has_sapling_spend || has_sapling_output {
                Some(-250_000_000)
            } else {
                None
            },
        }
    }
    #[test]
    fn test_detect_coinbase() {
        let tx = make_test_tx(false, false, false, false, false, false, false, true);
        let (pool, is_cb, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Coinbase");
        assert!(is_cb);
        assert_eq!(transfers, 0);
    }
    #[test]
    fn test_detect_transparent() {
        let tx = make_test_tx(true, true, false, false, false, false, false, false);
        let (pool, is_cb, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Transparent");
        assert!(!is_cb);
        assert_eq!(transfers, 1);
    }
    #[test]
    fn test_detect_sapling_only() {
        let tx = make_test_tx(false, false, false, true, true, false, false, false);
        let (pool, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Sapling");
        assert_eq!(transfers, 1);
    }
    #[test]
    fn test_detect_orchard_only() {
        let tx = make_test_tx(false, false, false, false, false, true, false, false);
        let (pool, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Orchard");
        assert_eq!(transfers, 1);
    }
    #[test]
    fn test_detect_ironwood_only() {
        let tx = make_test_tx(false, false, false, false, false, false, true, false);
        let (pool, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Ironwood");
        assert_eq!(transfers, 1);
    }
    #[test]
    fn test_detect_mixed_t_s_o() {
        let tx = make_test_tx(true, true, false, true, false, true, false, false);
        let (pool, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Transparent,Sapling,Orchard");
        assert_eq!(transfers, 2);
    }
    #[test]
    fn test_detect_mixed_with_ironwood() {
        let tx = make_test_tx(true, true, false, false, false, false, true, false);
        let (pool, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Transparent,Ironwood");
        assert_eq!(transfers, 2);
    }
    #[test]
    fn test_detect_sprout() {
        let tx = make_test_tx(false, false, true, false, false, false, false, false);
        let (pool, _, _, _, _, _, _, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Sprout");
    }
    #[test]
    fn test_transfers_count() {
        let tx = make_test_tx(true, false, false, false, true, true, false, false);
        let (_, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(transfers, 3);
    }
    #[test]
    fn test_empty_tx() {
        let tx = make_test_tx(false, false, false, false, false, false, false, false);
        let (pool, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Unknown");
        assert_eq!(transfers, 0);
    }
    #[test]
    fn test_shielded_with_zero_value_vout() {
        let mut tx = make_test_tx(false, false, false, true, true, false, false, false);
        tx.vout = vec![Vout { value_zat: 0 }];
        let (pool, _, _, _, _, _, _, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Sapling");
    }
    #[test]
    fn test_coinbase_with_orchard() {
        let tx = make_test_tx(true, false, false, false, false, true, false, true);
        let (pool, is_cb, _, _, _, _, _, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Coinbase");
        assert!(is_cb);
    }
    #[test]
    fn test_getblock_parsing() {
        let sample_json = r#"{
            "hash": "0000000000000000000000000000000000000000000000000000000000000000",
            "height": 3257342,
            "time": 1725123456,
            "tx": [
                {
                    "txid": "testtx1",
                    "vin": [{"txid": "prevtx", "vout": 0}],
                    "vout": [{"valueZat": 100000000}],
                    "vShieldedOutput": [{}],
                    "orchard": {"actions": [{}], "valueBalanceZat": -50000000}
                }
            ]
        }"#;
        let block_data: Value = serde_json::from_str(sample_json).unwrap();
        let block_height = block_data["height"].as_u64().unwrap() as u32;
        let block_time = block_data["time"].as_u64().unwrap();
        let txs = block_data["tx"].as_array().unwrap();
        assert_eq!(txs.len(), 1);
        let tx: RawTx = serde_json::from_value(txs[0].clone()).unwrap();
        let (pool_type, _, _, _, _, _, transfers, _, _, _, _) = detect_pools_and_values(&tx);
        assert_eq!(pool_type, "Transparent,Sapling,Orchard");
        assert_eq!(transfers, 3);
        assert_eq!(block_height, 3257342);
        assert_eq!(block_time, 1725123456);
    }
    #[test]
    fn test_ironwood_json_parsing() {
        let sample = r#"{
            "txid": "iwtx",
            "vin": [],
            "vout": [],
            "ironwood": {"actions": [{}, {}], "valueBalanceZat": -100000000}
        }"#;
        let tx: RawTx = serde_json::from_str(sample).unwrap();
        let (pool, _, _, _, _, i, transfers, _, _, _, iw_count) = detect_pools_and_values(&tx);
        assert_eq!(pool, "Ironwood");
        assert_eq!(i, -100_000_000);
        assert_eq!(transfers, 2);
        assert_eq!(iw_count, 2);
    }
}
