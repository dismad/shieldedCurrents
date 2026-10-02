use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use lru::LruCache;
use plotters::prelude::*;
use rayon::prelude::*;
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod fee;

#[derive(Parser, Debug)]
#[command(author, version, about = "zcash-block-fees")]
struct Args {
    #[arg(long)]
    block: Option<u32>,
    #[arg(long)]
    last: Option<u32>,
    #[arg(long)]
    from: Option<u32>,
    #[arg(long, help = "End height or 'tip' for current blockchain tip")]
    to: Option<String>,
    #[arg(long, default_value_t = 1)]
    step: u32,
    #[arg(short, long)]
    debug: bool,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    graph: Option<PathBuf>,
    #[arg(long, default_value_t = 1920)]
    graph_width: u32,
    #[arg(long, default_value_t = 1080)]
    graph_height: u32,
    #[arg(long, default_value = "%m/%d/%Y")]
    date_format: String,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    pass: Option<String>,
    #[arg(long)]
    cookie_file: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    rpc: String,
    #[arg(
        long,
        value_enum,
        default_value_t = FeeSource::Prevout,
        help = "prevout (default) reads getblock verbosity 3 and requires zebrad >= 7.0.0-rc.0. legacy fetches inputs with getrawtransaction"
    )]
    fee_source: FeeSource,
    #[arg(
        long,
        help = "Time the legacy getrawtransaction path and getblock verbosity 3 on the same heights"
    )]
    compare: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set,
          help = "Include transaction count (use --tx-count=false or --no-tx-count to disable)")]
    tx_count: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum FeeSource {
    Legacy,
    Prevout,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct BlockEntry {
    date: String,
    block: u32,
    fees: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_count: Option<u32>,
}

fn main() -> Result<()> {
    let mut args = Args::parse();
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(8)
        .build()?;
    let (user, pass) = get_credentials(&args)?;
    let start = Instant::now();
    let tip = get_block_count(&client, &args.rpc, &user, &pass)?;
    if let Some(to_str) = &args.to {
        if to_str.to_lowercase() == "tip" {
            args.to = Some(tip.to_string());
        }
    }
    let blocks = determine_blocks(&args, tip)?;
    if blocks.is_empty() {
        anyhow::bail!("No blocks specified");
    }
    if args.compare {
        run_compare(&client, &args.rpc, &user, &pass, &blocks, args.debug, args.quiet)?;
        return Ok(());
    }
    let (stats, failed_blocks) = process_blocks_safe(
        &client,
        &args.rpc,
        &user,
        &pass,
        &blocks,
        args.debug,
        args.quiet,
        args.fee_source,
    )?;
    let elapsed = start.elapsed().as_secs_f64();
    let total_zats: i64 = stats.iter().map(|s| s.total_fee_zats).sum();
    let total_txs: u32 = stats.iter().map(|s| s.tx_count).sum();
    print_summary(stats.len(), total_zats, total_txs, elapsed, args.tx_count);
    println!("Fee source: {:?} (prevout requires zebrad >= 7.0.0-rc.0)", args.fee_source);
    let total_fallbacks: usize = stats.iter().map(|s| s.fallback_misses).sum();
    let label = match args.fee_source {
        FeeSource::Legacy => "Unresolved inputs",
        FeeSource::Prevout => "Transactions with a missing prevout",
    };
    println!("{}: {}", label, total_fallbacks);
    if !failed_blocks.is_empty() {
        eprintln!("\nWARNING: {} blocks failed:", failed_blocks.len());
        for h in &failed_blocks {
            eprintln!(" - Block {}", h);
        }
    }
    if let Some(path) = args.output {
        write_pretty_json(&path, &blocks, &stats, &args.date_format, args.tx_count)?;
    }
    if let Some(path) = args.graph {
        generate_graph(&blocks, &stats, &path, args.graph_width, args.graph_height)?;
    }
    Ok(())
}

struct BlockStats {
    total_fee_zats: i64,
    timestamp: i64,
    fallback_misses: usize,
    tx_count: u32,
}

fn determine_blocks(args: &Args, tip: u32) -> Result<Vec<u32>> {
    if let Some(h) = args.block {
        return Ok(vec![h]);
    }
    if let Some(n) = args.last {
        let start = if tip >= n { tip - n + 1 } else { 0 };
        return Ok((start..=tip).collect());
    }
    let from = args.from.unwrap_or(0);
    let to = if let Some(to_str) = &args.to {
        if to_str.to_lowercase() == "tip" {
            tip
        } else {
            to_str
                .parse::<u32>()
                .context("Invalid --to (use number or 'tip')")?
        }
    } else {
        tip
    };
    if from > to {
        anyhow::bail!("--from must be <= --to");
    }
    let mut v = vec![];
    let mut h = from;
    while h <= to {
        v.push(h);
        h = h.saturating_add(args.step);
    }
    Ok(v)
}

fn get_block_count(client: &Client, url: &str, user: &str, pass: &str) -> Result<u32> {
    let res = rpc_call(client, url, user, pass, "getblockcount", vec![])?;
    Ok(res.as_u64().context("invalid getblockcount")? as u32)
}

fn run_compare(
    client: &Client,
    url: &str,
    user: &str,
    pass: &str,
    blocks: &[u32],
    debug: bool,
    quiet: bool,
) -> Result<()> {
    println!(
        "Compare {} blocks. Legacy runs first, so verbosity 3 may benefit from a warm page cache.",
        blocks.len()
    );
    let (legacy, legacy_failed, legacy_secs) =
        timed(client, url, user, pass, blocks, debug, quiet, FeeSource::Legacy)?;
    let (prevout, prevout_failed, prevout_secs) =
        timed(client, url, user, pass, blocks, debug, quiet, FeeSource::Prevout)?;
    let speedup = if prevout_secs > 0.0 {
        legacy_secs / prevout_secs
    } else {
        0.0
    };
    let legacy_fees: i64 = legacy.iter().map(|s| s.total_fee_zats).sum();
    let prevout_fees: i64 = prevout.iter().map(|s| s.total_fee_zats).sum();
    let legacy_misses: usize = legacy.iter().map(|s| s.fallback_misses).sum();
    let prevout_missing: usize = prevout.iter().map(|s| s.fallback_misses).sum();
    println!("\n=== FEE PATH COMPARE ===");
    println!(
        "legacy   {:>8.2}s  blocks {}  failed {}  fee {} zat  input misses {}",
        legacy_secs,
        legacy.len(),
        legacy_failed.len(),
        legacy_fees,
        legacy_misses
    );
    println!(
        "prevout  {:>8.2}s  blocks {}  failed {}  fee {} zat  missing prevout txs {}",
        prevout_secs,
        prevout.len(),
        prevout_failed.len(),
        prevout_fees,
        prevout_missing
    );
    println!("prevout / legacy time: {:.2}x", speedup);
    println!("fee delta (prevout - legacy): {} zat", prevout_fees - legacy_fees);
    if legacy_misses > 0 {
        println!("legacy undercounts when an input fetch misses; delta is expected in that case");
    }
    Ok(())
}

fn timed(
    client: &Client,
    url: &str,
    user: &str,
    pass: &str,
    blocks: &[u32],
    debug: bool,
    quiet: bool,
    source: FeeSource,
) -> Result<(Vec<BlockStats>, Vec<u32>, f64)> {
    let start = Instant::now();
    let (stats, failed) = process_blocks_safe(client, url, user, pass, blocks, debug, quiet, source)?;
    Ok((stats, failed, start.elapsed().as_secs_f64()))
}

fn process_blocks_safe(
    client: &Client,
    url: &str,
    user: &str,
    pass: &str,
    blocks: &[u32],
    debug: bool,
    quiet: bool,
    source: FeeSource,
) -> Result<(Vec<BlockStats>, Vec<u32>)> {
    let pb = if !quiet {
        let pb = ProgressBar::new(blocks.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} blocks ({eta})")?
            .progress_chars("#>-"));
        Some(pb)
    } else {
        None
    };
    let results: Vec<(Option<BlockStats>, u32)> = blocks
        .par_iter()
        .with_max_len(3)
        .map(|&height| {
            match calculate_block_fees(client, url, user, pass, height, debug, source) {
                Ok((total, timestamp, fallbacks, tx_count)) => {
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    (
                        Some(BlockStats {
                            total_fee_zats: total,
                            timestamp,
                            fallback_misses: fallbacks,
                            tx_count,
                        }),
                        height,
                    )
                }
                Err(e) => {
                    eprintln!("Failed to process block {}: {}", height, e);
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    (None, height)
                }
            }
        })
        .collect();
    if let Some(pb) = pb {
        pb.finish();
    }
    let mut stats = Vec::new();
    let mut failed = Vec::new();
    for (opt, h) in results {
        if let Some(s) = opt {
            stats.push(s);
        } else {
            failed.push(h);
        }
    }
    Ok((stats, failed))
}

fn calculate_block_fees(
    client: &Client,
    url: &str,
    user: &str,
    pass: &str,
    height: u32,
    debug: bool,
    source: FeeSource,
) -> Result<(i64, i64, usize, u32)> {
    let hash = rpc_call(client, url, user, pass, "getblockhash", vec![json!(height)])?;
    let hash_str = hash.as_str().context("no hash")?.to_string();
    let verbosity = match source {
        FeeSource::Legacy => 2,
        FeeSource::Prevout => 3,
    };
    let block = rpc_call(
        client,
        url,
        user,
        pass,
        "getblock",
        vec![json!(hash_str), json!(verbosity)],
    )?;
    let timestamp = block["time"].as_i64().unwrap_or(0);
    let txs = block["tx"].as_array().context("no tx array")?;
    let tx_count = txs.len() as u32;
    if source == FeeSource::Prevout {
        let mut total = 0i64;
        let mut missing = 0usize;
        for tx in txs {
            match fee::prevout_fee_zats(tx) {
                Some(fee) => total += fee,
                None => missing += 1,
            }
        }
        return Ok((total, timestamp, missing, tx_count));
    }
    let mut local_cache: LruCache<String, Value> = LruCache::new(NonZeroUsize::new(300).unwrap());
    let mut missing_txids: HashSet<String> = HashSet::new();
    for tx in txs {
        if fee::is_coinbase(tx) {
            continue;
        }
        if let Some(vins) = tx["vin"].as_array() {
            for vin in vins {
                if let Some(ptxid) = vin["txid"].as_str() {
                    let ptxid_str = ptxid.to_string();
                    if local_cache.get(&ptxid_str).is_none() {
                        missing_txids.insert(ptxid_str);
                    }
                }
            }
        }
    }
    if !missing_txids.is_empty() {
        if missing_txids.len() > 5000 {
            println!("Very dense block {} ({} transactions but {} unique prev-txids) — using safe fallback mode (on-demand fetching)", height, tx_count, missing_txids.len());
        } else {
            if missing_txids.len() > 400 {
                println!("Dense block {} ({} transactions but {} unique prev-txids) — fetching sequentially", height, tx_count, missing_txids.len());
            }
            let txids_vec: Vec<String> = missing_txids.into_iter().collect();
            for txid in txids_vec {
                if let Ok(value) = rpc_call(
                    client,
                    url,
                    user,
                    pass,
                    "getrawtransaction",
                    vec![json!(&txid), json!(1)],
                ) {
                    local_cache.put(txid, value);
                }
            }
        }
    }
    let mut total = 0i64;
    let mut total_fallbacks = 0usize;
    for (i, tx) in txs.iter().enumerate() {
        if fee::is_coinbase(tx) {
            continue;
        }
        let (fee, misses) =
            calculate_fee_from_tx(client, url, user, pass, tx, &mut local_cache, debug, i)?;
        total += fee;
        total_fallbacks += misses;
    }
    Ok((total, timestamp, total_fallbacks, tx_count))
}

fn calculate_fee_from_tx(
    client: &Client,
    url: &str,
    user: &str,
    pass: &str,
    tx: &Value,
    local_cache: &mut LruCache<String, Value>,
    debug: bool,
    tx_index: usize,
) -> Result<(i64, usize)> {
    let (fee, misses) = fee::legacy_fee_zats(tx, |ptxid, idx| {
        let ptxid_str = ptxid.to_string();
        let prev_tx = if let Some(entry) = local_cache.get(&ptxid_str) {
            entry.clone()
        } else {
            if debug {
                eprintln!("Cache miss on {} (tx index {})", ptxid_str, tx_index);
            }
            let fetched = rpc_call(
                client,
                url,
                user,
                pass,
                "getrawtransaction",
                vec![json!(ptxid), json!(1)],
            )
            .ok()?;
            local_cache.put(ptxid_str, fetched.clone());
            fetched
        };
        prev_tx["vout"]
            .as_array()
            .and_then(|vout_arr| vout_arr.get(idx as usize))
            .and_then(|vout| vout["valueZat"].as_i64())
    });
    Ok((fee, misses))
}

fn get_credentials(args: &Args) -> Result<(String, String)> {
    if let (Some(u), Some(p)) = (&args.user, &args.pass) {
        return Ok((u.clone(), p.clone()));
    }
    if let Some(ref p) = args.cookie_file {
        return read_cookie_file(p);
    }
    let home = dirs::home_dir().context("no home dir")?;
    let candidates = vec![
        home.join(".cache").join("zebra").join(".cookie"),
        home.join(".zcash").join(".cookie"),
        PathBuf::from(".cookie"),
        "/var/lib/zebrad-rpc/.cookie".to_string().into(),
    ];
    for p in candidates {
        if p.exists() {
            if let Ok(creds) = read_cookie_file(&p) {
                if !args.quiet {
                    eprintln!("Using cookie file: {}", p.display());
                }
                return Ok(creds);
            }
        }
    }
    Ok((String::new(), String::new()))
}

fn read_cookie_file(p: &PathBuf) -> Result<(String, String)> {
    let s = fs::read_to_string(p)?;
    let line = s.lines().next().context("empty cookie")?.trim();
    let (u, pw) = line.split_once(':').context("bad cookie format")?;
    Ok((u.trim().to_string(), pw.trim().to_string()))
}

fn rpc_call(
    client: &Client,
    url: &str,
    user: &str,
    pass: &str,
    method: &str,
    params: Vec<Value>,
) -> Result<Value> {
    let req =
        json!({ "jsonrpc": "1.0", "id": "zcash-block-fees", "method": method, "params": params });
    let mut builder = client.post(url).json(&req);
    if !user.is_empty() {
        builder = builder.basic_auth(user, (!pass.is_empty()).then_some(pass));
    }
    let resp: Value = builder
        .send()
        .context("RPC send failed")?
        .json()
        .context("RPC JSON parse failed")?;
    if let Some(err) = resp.get("error") {
        if !err.is_null() {
            anyhow::bail!("RPC error: {}", err);
        }
    }
    Ok(resp["result"].clone())
}

fn write_pretty_json(
    path: &PathBuf,
    blocks: &[u32],
    stats: &[BlockStats],
    date_format: &str,
    include_tx_count: bool,
) -> Result<()> {
    let mut data = vec![];
    for (i, &b) in blocks.iter().enumerate() {
        let s = &stats[i];
        let date_str = if s.timestamp != 0 {
            DateTime::<Utc>::from_timestamp(s.timestamp, 0)
                .map(|dt| dt.format(date_format).to_string())
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            "unknown".to_string()
        };
        data.push(BlockEntry {
            date: date_str,
            block: b,
            fees: s.total_fee_zats as f64 / 1e8,
            tx_count: if include_tx_count {
                Some(s.tx_count)
            } else {
                None
            },
        });
    }
    let pretty = serde_json::to_string_pretty(&data)?;
    fs::write(path, pretty)?;
    println!(
        "JSON output written ({} blocks): {}",
        data.len(),
        path.display()
    );
    Ok(())
}

fn generate_graph(
    blocks: &[u32],
    stats: &[BlockStats],
    path: &PathBuf,
    width: u32,
    height: u32,
) -> Result<()> {
    let max_points = 2000;
    let step = if blocks.len() > max_points {
        blocks.len() / max_points
    } else {
        1
    };
    let points: Vec<(u32, f64)> = blocks
        .iter()
        .step_by(step)
        .zip(stats.iter().step_by(step))
        .map(|(&b, s)| (b, s.total_fee_zats as f64 / 1e8))
        .collect();
    let root = SVGBackend::new(path.to_str().unwrap(), (width, height)).into_drawing_area();
    root.fill(&WHITE)?;
    let max_fee = points
        .iter()
        .map(|(_, f)| *f)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(0.0);
    let mut chart = ChartBuilder::on(&root)
        .caption("Zcash Block Fees Over Time", ("sans-serif", 30))
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(
            *blocks.first().unwrap_or(&0)..*blocks.last().unwrap_or(&0),
            0.0..(max_fee * 1.1),
        )?;
    chart
        .configure_mesh()
        .x_desc("Block Height")
        .y_desc("Fees (ZEC)")
        .draw()?;
    chart.draw_series(LineSeries::new(points, &BLUE))?;
    root.present()?;
    println!("Graph saved to {}", path.display());
    Ok(())
}

fn print_summary(
    processed: usize,
    total_zats: i64,
    total_txs: u32,
    elapsed: f64,
    include_tx_count: bool,
) {
    println!("\n=== BLOCK FEES SUMMARY ===");
    println!("Blocks processed : {}", processed);
    if include_tx_count {
        println!("Total transactions : {}", total_txs);
        println!(
            "Avg txs per block : {:.1}",
            if processed > 0 {
                total_txs as f64 / processed as f64
            } else {
                0.0
            }
        );
    }
    println!(
        "Total fees : {} Zats ({:.4} ZEC)",
        total_zats,
        total_zats as f64 / 1e8
    );
    println!("Time : {:.2}s", elapsed);
    println!(
        "Avg fee per block : {:.0} Zats",
        if processed > 0 {
            total_zats as f64 / processed as f64
        } else {
            0.0
        }
    );
}
