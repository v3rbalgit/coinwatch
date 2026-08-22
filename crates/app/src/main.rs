//! `coinwatch` — orchestration entry point.
//!
//! Phase-1 (in progress): exercises the first two real seams end-to-end — fetch
//! live bars from the [`GeckoTerminal`] provider, upsert them into the DuckDB
//! [`DuckStore`] (idempotently), then read a point-in-time window back out.
//! Features, strategy, backtest and eval get wired in as their seams land.

use std::fs;

use chrono::Duration;
use cw_core::{CoreError, Instrument, Interval, Network, PoolId, Result};
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
        limit: 30,
    };

    println!(
        "fetch: {} daily bars for {} via {}",
        req.limit,
        instrument.name,
        provider.name()
    );
    let bars = provider.fetch_bars(&req).await?;
    let (Some(first), Some(last)) = (bars.first(), bars.last()) else {
        println!("no bars returned; nothing to store");
        return Ok(());
    };
    println!(
        "  -> {} bars ({} .. {})",
        bars.len(),
        first.ts.date_naive(),
        last.ts.date_naive()
    );

    fs::create_dir_all("research/data")
        .map_err(|e| CoreError::Storage(format!("mkdir research/data: {e}")))?;
    let store = DuckStore::open(DB_PATH)?;

    let n1 = store.upsert_bars(&instrument, Interval::Day, &bars)?;
    let n2 = store.upsert_bars(&instrument, Interval::Day, &bars)?;
    println!("store: upserted {n1}, re-upserted {n2} (idempotent -> no duplicates)");

    // Point-in-time read: only the last 7 days, proving range-bounded loads.
    let to = last.ts;
    let from = to - Duration::days(6);
    let window = store.load_bars(&instrument, Interval::Day, TimeRange { from, to })?;
    println!(
        "load [{} .. {}]: {} bars",
        from.date_naive(),
        to.date_naive(),
        window.len()
    );
    for b in &window {
        println!("  {}  C={} V={}", b.ts.date_naive(), b.close, b.volume);
    }
    println!("db: {DB_PATH}");
    Ok(())
}
