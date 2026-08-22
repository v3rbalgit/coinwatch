//! `coinwatch` — orchestration entry point.
//!
//! Phase-1 (in progress): the live seams end-to-end across a *universe* —
//! discover top liquid pools ([`UniverseSource`]), backfill each into the DuckDB
//! [`DuckStore`] (paced + idempotent), compute the [`TechnicalFeatures`] matrix,
//! and rank instruments cross-sectionally by momentum. Strategy/backtest/eval land next.

use std::collections::HashMap;
use std::fs;

use cw_core::{CoreError, Decimal, Instrument, Interval, Network, Result};
use cw_features::{FeatureContext, FeatureModule, TechnicalFeatures};
use cw_providers::{BarRequest, DataSource, GeckoTerminal, UniverseRequest, UniverseSource};
use cw_storage::{DuckStore, Store, TimeRange};

const DB_PATH: &str = "research/data/coinwatch.duckdb";

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let provider = GeckoTerminal::default();
    let network = Network::Base;

    let uni_req = UniverseRequest {
        min_reserve_usd: Decimal::from(1_000_000u64),
        limit: 6,
        pages: 1,
    };
    println!(
        "universe: top {} pools on {} (reserve >= ${})",
        uni_req.limit,
        network.slug(),
        uni_req.min_reserve_usd
    );
    let pools = provider.top_pools(&network, &uni_req).await?;
    if pools.is_empty() {
        println!("no pools matched; nothing to do");
        return Ok(());
    }

    fs::create_dir_all("research/data")
        .map_err(|e| CoreError::Storage(format!("mkdir research/data: {e}")))?;
    let store = DuckStore::open(DB_PATH)?;

    // Backfill each pool's daily bars into the store, then read back.
    let mut dataset: Vec<(Instrument, Vec<cw_core::Bar>)> = Vec::new();
    let mut total_rows = 0usize;
    for pool in &pools {
        let req = BarRequest {
            instrument: pool.instrument.clone(),
            interval: Interval::Day,
            limit: 120,
        };
        // Resilient backfill: a throttled/failed pool is skipped, not fatal.
        let bars = match provider.fetch_bars(&req).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  {:<28} fetch failed: {e}", pool.instrument.name);
                continue;
            }
        };
        let (Some(first), Some(last)) = (bars.first(), bars.last()) else {
            println!("  {:<28} no bars", pool.instrument.name);
            continue;
        };
        total_rows += store.upsert_bars(&pool.instrument, Interval::Day, &bars)?;
        let loaded = store.load_bars(
            &pool.instrument,
            Interval::Day,
            TimeRange { from: first.ts, to: last.ts },
        )?;
        println!(
            "  {:<28} reserve=${:>12.0}  bars={}",
            pool.instrument.name, pool.reserve_usd, loaded.len()
        );
        dataset.push((pool.instrument.clone(), loaded));
    }
    println!("store: {} pools, {} bars total; db: {DB_PATH}", dataset.len(), total_rows);

    // Cross-sectional momentum leaderboard from the latest feature bar per pool.
    let module = TechnicalFeatures::default();
    let frame = module.compute(&FeatureContext { bars: &dataset })?;
    let mut latest: HashMap<&str, &cw_features::FeatureRow> = HashMap::new();
    for row in &frame.rows {
        let key = row.instrument.pool.0.as_str();
        if latest.get(key).is_none_or(|cur| row.ts > cur.ts) {
            latest.insert(key, row);
        }
    }
    let mut board: Vec<&cw_features::FeatureRow> = latest.into_values().collect();
    board.sort_by(|a, b| b.values["mom"].cmp(&a.values["mom"]));

    println!("\nmomentum leaderboard (latest bar, {} lookback):", module.momentum_lookback);
    for (rank, row) in board.iter().enumerate() {
        println!(
            "  {:>2}. {:<28} mom={:>8.4} rsi={:>6.2} vol={:>7.4}",
            rank + 1,
            row.instrument.name,
            row.values["mom"],
            row.values["rsi"],
            row.values["vol"],
        );
    }
    Ok(())
}
