//! `coinwatch` — orchestration entry point.
//!
//! Phase-1 (in progress): exercises the live seams end-to-end — fetch bars from
//! [`GeckoTerminal`], upsert them idempotently into the DuckDB [`DuckStore`],
//! read them back point-in-time, and compute the [`TechnicalFeatures`] matrix.
//! Strategy, backtest and eval get wired in as their seams land.

use std::fs;

use cw_core::{CoreError, Instrument, Interval, Network, PoolId, Result};
use cw_features::{FeatureContext, FeatureModule, TechnicalFeatures};
use cw_providers::{BarRequest, DataSource, GeckoTerminal};
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

    // Known-liquid pool validated in the Phase-0 spike (WETH/USDC 0.01% on Base).
    let instrument = Instrument {
        network: Network::Base,
        pool: PoolId("0x72ab388e2e2f6facef59e3c3fa2c4e29011c2d38".to_string()),
        name: "WETH / USDC 0.01%".to_string(),
    };
    let req = BarRequest {
        instrument: instrument.clone(),
        interval: Interval::Day,
        limit: 120,
    };

    println!("fetch: {} daily bars for {} via {}", req.limit, instrument.name, provider.name());
    let bars = provider.fetch_bars(&req).await?;
    let (Some(first), Some(last)) = (bars.first(), bars.last()) else {
        println!("no bars returned; nothing to store");
        return Ok(());
    };
    let (from, to) = (first.ts, last.ts);
    println!("  -> {} bars ({} .. {})", bars.len(), from.date_naive(), to.date_naive());

    fs::create_dir_all("research/data")
        .map_err(|e| CoreError::Storage(format!("mkdir research/data: {e}")))?;
    let store = DuckStore::open(DB_PATH)?;
    let n = store.upsert_bars(&instrument, Interval::Day, &bars)?;
    store.upsert_bars(&instrument, Interval::Day, &bars)?; // re-run: idempotent
    println!("store: upserted {n} (idempotent re-run -> no duplicates); db: {DB_PATH}");

    // Read back point-in-time, then compute the technical feature matrix.
    let loaded = store.load_bars(&instrument, Interval::Day, TimeRange { from, to })?;
    let module = TechnicalFeatures::default();
    let dataset = vec![(instrument.clone(), loaded)];
    let frame = module.compute(&FeatureContext { bars: &dataset })?;

    println!(
        "features[{}]: {} rows (lookbacks mom={} vol={} rsi={})",
        module.name(),
        frame.rows.len(),
        module.momentum_lookback,
        module.vol_lookback,
        module.rsi_lookback,
    );
    for row in frame.rows.iter().rev().take(5).rev() {
        println!(
            "  {}  mom={:>8.4} vol={:>7.4} rsi={:>6.2}",
            row.ts.date_naive(),
            row.values["mom"],
            row.values["vol"],
            row.values["rsi"],
        );
    }
    Ok(())
}
