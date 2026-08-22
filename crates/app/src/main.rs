//! `coinwatch` — orchestration entry point.
//!
//! Phase-2: modularity confirmation. Two independent feature modules
//! ([`TechnicalFeatures`] + [`LiquidityFeatures`]) are combined into one frame and
//! fed to the *unchanged* engine (`strategy` + `backtest` + `eval`). The same
//! [`CrossSectional`] strategy is then ranked by either module's feature via
//! config only — proving new edge hypotheses slot in without engine changes.

use std::collections::HashMap;
use std::fs;

use chrono::{Duration, Utc};
use cw_backtest::{Backtester, CostModel, WalkForward};
use cw_core::{Bar, CoreError, Decimal, Instrument, Interval, Network, Result};
use cw_features::{combine, FeatureContext, FeatureFrame, FeatureModule, LiquidityFeatures, TechnicalFeatures};
use cw_providers::{BarRequest, DataSource, GeckoTerminal, UniverseRequest, UniverseSource};
use cw_storage::{DuckStore, Store, TimeRange};
use cw_strategy::CrossSectional;

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

    // Ingest each pool: load from the store when we already have history,
    // otherwise fetch from the provider and persist. Reruns are offline + instant.
    let wide = TimeRange {
        from: Utc::now() - Duration::days(400),
        to: Utc::now() + Duration::days(1),
    };
    let mut dataset: Vec<(Instrument, Vec<Bar>)> = Vec::new();
    for pool in &pools {
        let stored = store.load_bars(&pool.instrument, Interval::Day, wide).unwrap_or_default();
        let (bars, src) = if stored.len() >= 60 {
            (stored, "store")
        } else {
            let req = BarRequest {
                instrument: pool.instrument.clone(),
                interval: Interval::Day,
                limit: 120,
            };
            match provider.fetch_bars(&req).await {
                Ok(fetched) if !fetched.is_empty() => {
                    store.upsert_bars(&pool.instrument, Interval::Day, &fetched)?;
                    (fetched, "fetch")
                }
                Ok(_) => {
                    println!("  {:<28} no bars", pool.instrument.name);
                    continue;
                }
                Err(e) if !stored.is_empty() => {
                    eprintln!("  {:<28} fetch failed ({e}); using stored", pool.instrument.name);
                    (stored, "store")
                }
                Err(e) => {
                    eprintln!("  {:<28} fetch failed: {e}", pool.instrument.name);
                    continue;
                }
            }
        };
        println!(
            "  {:<28} reserve=${:>12.0}  bars={:>3} [{}]",
            pool.instrument.name, pool.reserve_usd, bars.len(), src
        );
        dataset.push((pool.instrument.clone(), bars));
    }
    let total_bars: usize = dataset.iter().map(|(_, b)| b.len()).sum();
    println!("dataset: {} pools, {} bars; db: {DB_PATH}", dataset.len(), total_bars);

    // Two independent feature modules → one combined frame for the same engine.
    let technical = TechnicalFeatures::default();
    let liquidity = LiquidityFeatures::default();
    let ctx = FeatureContext { bars: &dataset };
    let frame = combine(&[technical.compute(&ctx)?, liquidity.compute(&ctx)?]);
    println!(
        "features: modules [{}, {}] combined -> {} rows",
        technical.name(),
        liquidity.name(),
        frame.rows.len()
    );

    // Cross-sectional momentum leaderboard from the latest feature bar per pool.
    let mut latest: HashMap<&str, &cw_features::FeatureRow> = HashMap::new();
    for row in &frame.rows {
        let key = row.instrument.pool.0.as_str();
        if latest.get(key).is_none_or(|cur| row.ts > cur.ts) {
            latest.insert(key, row);
        }
    }
    let mut board: Vec<&cw_features::FeatureRow> = latest.into_values().collect();
    board.sort_by(|a, b| b.values["mom"].cmp(&a.values["mom"]));
    println!("\nleaderboard (latest bar): momentum | dollar-vol momentum | illiquidity");
    for (rank, row) in board.iter().enumerate() {
        println!(
            "  {:>2}. {:<28} mom={:>8.4}  dvol_mom={:>8.4}  illiq={:>8.4}",
            rank + 1,
            row.instrument.name,
            row.values["mom"],
            row.values["dvol_mom"],
            row.values["illiq"],
        );
    }

    // Same engine, two edge hypotheses — selected by config (rank feature) only.
    run_backtest("technical momentum", "mom", &dataset, &frame)?;
    run_backtest("liquidity: dollar-vol momentum", "dvol_mom", &dataset, &frame)?;
    Ok(())
}

/// Backtest [`CrossSectional`] ranked by `rank_feature` and print metrics.
/// Nothing here is feature-module-specific — the same engine consumes any column.
fn run_backtest(label: &str, rank_feature: &str, dataset: &[(Instrument, Vec<Bar>)], frame: &FeatureFrame) -> Result<()> {
    let strategy = CrossSectional {
        rank_feature: rank_feature.to_string(),
        top_quantile: 0.4,
        allow_short: false,
    };
    let costs = CostModel {
        fee_frac: Decimal::new(5, 4),        // 5 bps swap fee
        slippage_coeff: Decimal::new(10, 4), // 10 bps slippage haircut
        gas_usd: Decimal::new(50, 2),        // $0.50 per position change
    };
    let backtester = Backtester::new(costs, WalkForward { train_bars: 60, test_bars: 30, embargo_bars: 3 });
    let result = backtester.run(&strategy, dataset, frame)?;
    let m = cw_eval::evaluate(&result)?;

    if let (Some((_, e0)), Some((_, e1))) = (result.equity_curve.first(), result.equity_curve.last()) {
        let total = ((e1 / e0 - Decimal::ONE) * Decimal::from(100)).round_dp(2);
        println!(
            "\nbacktest [{label}] (long-only top 40% by {rank_feature}, {} rebalances):",
            result.equity_curve.len().saturating_sub(1)
        );
        println!("  equity: ${} -> ${}  ({}% total)", e0.round_dp(2), e1.round_dp(2), total);
        println!(
            "  sharpe={:.2}  sortino={:.2}  maxDD={:.1}%  hit={:.1}%  turnover={:.2}  PSR={:.1}%  trades={}",
            m.sharpe,
            m.sortino,
            m.max_drawdown * 100.0,
            m.hit_rate * 100.0,
            m.turnover,
            m.psr * 100.0,
            result.trades.len(),
        );
    }
    Ok(())
}
