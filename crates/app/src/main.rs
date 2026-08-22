//! `coinwatch` — orchestration entry point (Hyperliquid perps).
//!
//! Two subcommands separate the slow, networked ingest from fast, offline research:
//!
//! - `coinwatch ingest`  — pull the Hyperliquid perp universe: ~3y of daily candles
//!   and hourly funding into the DuckDB store (resumable, store-first).
//! - `coinwatch analyze` — reconstruct the universe from the store (no network),
//!   compute features, screen them by IC, and run cost-aware walk-forward backtests
//!   + a deflated strategy sweep.

use std::collections::HashMap;
use std::fs;

use chrono::{Duration, Utc};
use cw_backtest::{Backtester, CostModel, WalkForward};
use cw_core::{Bar, CoreError, Decimal, FundingPoint, Instrument, Interval, Result};
use cw_features::{combine, FeatureContext, FeatureFrame, FeatureModule, FeatureRow, LiquidityFeatures, TechnicalFeatures};
use cw_harness::{feature_ic, sweep, WfConfig};
use cw_providers::{BarRequest, DataSource, Hyperliquid};
use cw_storage::{DuckStore, Store, TimeRange};
use cw_strategy::CrossSectional;

const DB_PATH: &str = "research/data/coinwatch.duckdb";
/// Minimum bars an instrument needs before it can carry features / be analyzed.
const MIN_BARS: usize = 60;
/// Funding-history window pulled per perp on ingest (paginated, resumable).
const FUNDING_DAYS: i64 = 120;

#[tokio::main]
async fn main() {
    let cmd = std::env::args().nth(1).unwrap_or_default();
    let result = match cmd.as_str() {
        "ingest" => ingest().await,
        "" | "analyze" => analyze(),
        other => {
            eprintln!("unknown command '{other}'. usage: coinwatch [ingest|analyze]");
            std::process::exit(2);
        }
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn open_store() -> Result<DuckStore> {
    fs::create_dir_all("research/data").map_err(|e| CoreError::Storage(format!("mkdir: {e}")))?;
    DuckStore::open(DB_PATH)
}

/// A generous read window covering all retained history.
fn wide_range() -> TimeRange {
    TimeRange {
        from: Utc::now() - Duration::days(1500),
        to: Utc::now() + Duration::days(1),
    }
}

fn feat(row: &FeatureRow, key: &str) -> Decimal {
    row.values.get(key).copied().unwrap_or_default()
}

/// Ingest the Hyperliquid perp universe: ~3y daily candles + hourly funding.
/// Resumable/store-first, so re-runs skip current coins and fill the rest.
async fn ingest() -> Result<()> {
    let hl = Hyperliquid::new();
    let store = open_store()?;
    let perps = hl.top_perps(40, Decimal::from(1_000_000u64)).await?;
    println!("hyperliquid: {} perps (min 24h vol $1M)", perps.len());

    let (mut total, mut funding_rows) = (0usize, 0usize);
    for (inst, day_vol) in &perps {
        store.register_instrument(inst)?;

        // Funding (perp carry signal) — resumable: pull a bounded window unless current.
        let funding_current = store
            .load_funding(&inst.network, &inst.pool.0, wide_range())
            .unwrap_or_default()
            .last()
            .is_some_and(|p| p.time > Utc::now() - Duration::days(2));
        if !funding_current {
            match hl.fetch_funding(&inst.pool.0, Utc::now() - Duration::days(FUNDING_DAYS)).await {
                Ok(f) if !f.is_empty() => {
                    funding_rows += store.upsert_funding(&inst.network, &inst.pool.0, &f)?;
                }
                Ok(_) => {}
                Err(e) => eprintln!("  {:<12} funding: {e}", inst.name),
            }
        }

        // Resumable: skip coins already backfilled and current.
        let stored = store.load_bars(inst, Interval::Day, wide_range()).unwrap_or_default();
        if stored.len() >= 300 && stored.last().is_some_and(|b| b.ts > Utc::now() - Duration::days(2)) {
            println!("  {:<12} up-to-date", inst.name);
            continue;
        }
        let req = BarRequest {
            instrument: inst.clone(),
            interval: Interval::Day,
            limit: 1200, // ~3.3y of daily candles
        };
        match hl.fetch_bars(&req).await {
            Ok(bars) if !bars.is_empty() => {
                let n = store.upsert_bars(inst, Interval::Day, &bars)?;
                total += n;
                println!("  {:<12} {:>5} bars  24h_vol=${:>14.0}", inst.name, n, day_vol);
            }
            Ok(_) => println!("  {:<12} no bars", inst.name),
            Err(e) => eprintln!("  {:<12} fetch failed: {e}", inst.name),
        }
    }
    println!("ingest complete: {total} bars, {funding_rows} funding rows across {} perps; db: {DB_PATH}", perps.len());
    Ok(())
}

/// Reconstruct the universe from the store and run the research pipeline offline.
fn analyze() -> Result<()> {
    let store = open_store()?;
    let instruments = store.list_instruments()?;
    if instruments.is_empty() {
        println!("store empty — run `coinwatch ingest` first");
        return Ok(());
    }

    let mut dataset: Vec<(Instrument, Vec<Bar>)> = Vec::new();
    for inst in &instruments {
        let bars = store.load_bars(inst, Interval::Day, wide_range()).unwrap_or_default();
        if bars.len() >= MIN_BARS {
            dataset.push((inst.clone(), bars));
        }
    }
    let total_bars: usize = dataset.iter().map(|(_, b)| b.len()).sum();
    println!(
        "analyze: {}/{} instruments with >={} bars, {} bars total",
        dataset.len(), instruments.len(), MIN_BARS, total_bars
    );
    if dataset.len() < 2 {
        println!("need >=2 instruments for a cross-sectional backtest — ingest more");
        return Ok(());
    }

    // Funding series per instrument (perp funding P&L in the backtest).
    let mut funding_dataset: Vec<(Instrument, Vec<FundingPoint>)> = Vec::new();
    for (inst, _) in &dataset {
        let f = store.load_funding(&inst.network, &inst.pool.0, wide_range()).unwrap_or_default();
        if !f.is_empty() {
            funding_dataset.push((inst.clone(), f));
        }
    }
    println!(
        "funding: {} points across {} perps",
        funding_dataset.iter().map(|(_, f)| f.len()).sum::<usize>(),
        funding_dataset.len()
    );

    // Feature modules → one combined frame for the same engine.
    let technical = TechnicalFeatures::default();
    let liquidity = LiquidityFeatures::default();
    let ctx = FeatureContext { bars: &dataset };
    let frame = combine(&[technical.compute(&ctx)?, liquidity.compute(&ctx)?]);
    println!("features: modules [{}, {}] -> {} rows", technical.name(), liquidity.name(), frame.rows.len());

    // Cross-sectional leaderboard from the latest feature bar per instrument.
    let mut latest: HashMap<&str, &FeatureRow> = HashMap::new();
    for row in &frame.rows {
        let key = row.instrument.pool.0.as_str();
        if latest.get(key).is_none_or(|cur| row.ts > cur.ts) {
            latest.insert(key, row);
        }
    }
    let mut board: Vec<&FeatureRow> = latest.into_values().collect();
    board.sort_by_key(|b| std::cmp::Reverse(feat(b, "mom")));
    println!("\nleaderboard (latest bar) top 12 by momentum:");
    for (rank, row) in board.iter().take(12).enumerate() {
        println!(
            "  {:>2}. {:<10} mom={:>7.3}  vol={:>7.4}  rsi={:>6.2}  dvol_mom={:>7.3}",
            rank + 1,
            row.instrument.name,
            feat(row, "mom"),
            feat(row, "vol"),
            feat(row, "rsi"),
            feat(row, "dvol_mom"),
        );
    }

    // Which features actually predict next-bar returns? (rank-IC screen — the
    // first thing to look at: run this before trusting any backtest.)
    let ics = feature_ic(&dataset, &frame);
    if !ics.is_empty() {
        println!("\nfeature IC (cross-sectional rank corr vs next-bar return; |IC-IR| desc):");
        for f in &ics {
            println!(
                "  {:<10} IC={:>7.4}  IC-IR={:>6.2}  t={:>6.2}  hit={:>5.1}%  n={}",
                f.feature, f.mean_ic, f.icir, f.t_stat, f.hit_rate * 100.0, f.n_periods
            );
        }
    }

    // In-sample, full-period backtests (the flattering, overfit-prone view).
    run_backtest("in-sample: technical momentum", "mom", &dataset, &frame, &funding_dataset)?;
    run_backtest("in-sample: liquidity dvol-momentum", "dvol_mom", &dataset, &frame, &funding_dataset)?;

    // Strategy sweep: evaluate MANY configs out-of-sample (each its own
    // walk-forward), deflated for the number of trials → an honest strategy
    // leaderboard. Contrast the DSR here with the flattering in-sample runs above.
    let costs = CostModel {
        fee_frac: Decimal::new(5, 4),
        slippage_coeff: Decimal::new(5, 1),
        gas_usd: Decimal::new(50, 2),
    };
    // Market-neutral (long/short) tests of the significant signals + baselines.
    // (label, feature, top_quantile, invert); all allow_short = true.
    let specs: &[(&str, &str, f64, bool)] = &[
        ("vol L/S (long low-vol)", "vol", 0.20, true),
        ("vol L/S 33%", "vol", 0.33, true),
        ("illiq L/S", "illiq", 0.20, false),
        ("illiq L/S 33%", "illiq", 0.33, false),
        ("mom L/S", "mom", 0.20, false),
        ("dvol_mom L/S", "dvol_mom", 0.20, false),
        ("rsi L/S", "rsi", 0.20, false),
    ];
    let candidates: Vec<(String, CrossSectional)> = specs
        .iter()
        .map(|(label, feature, q, invert)| {
            (
                label.to_string(),
                CrossSectional {
                    rank_feature: feature.to_string(),
                    top_quantile: *q,
                    allow_short: true,
                    invert: *invert,
                },
            )
        })
        .collect();
    let rows = sweep(&candidates, &dataset, &frame, WfConfig { train: 90, test: 30, embargo: 3 }, &costs, &funding_dataset)?;
    if rows.iter().all(|r| r.n_folds == 0) {
        println!("\nstrategy sweep: not enough history for the chosen windows");
        return Ok(());
    }
    println!("\nstrategy sweep ({} configs, walk-forward OOS, ranked by Deflated Sharpe):", candidates.len());
    println!("  {:<14} {:>8} {:>9} {:>8} {:>7} {:>6}", "config", "OOS-Shp", "OOS-ret%", "maxDD%", "DSR%", "folds");
    for r in &rows {
        println!(
            "  {:<14} {:>8.2} {:>9.1} {:>8.1} {:>7.1} {:>6}",
            r.label, r.oos_sharpe, r.oos_return_pct, r.max_drawdown * 100.0, r.dsr * 100.0, r.n_folds
        );
    }
    Ok(())
}

/// Backtest [`CrossSectional`] ranked by `rank_feature` and print metrics.
/// Nothing here is feature-module-specific — the same engine consumes any column.
fn run_backtest(
    label: &str,
    rank_feature: &str,
    dataset: &[(Instrument, Vec<Bar>)],
    frame: &FeatureFrame,
    funding: &[(Instrument, Vec<FundingPoint>)],
) -> Result<()> {
    let strategy = CrossSectional {
        rank_feature: rank_feature.to_string(),
        top_quantile: 0.4,
        allow_short: false,
        invert: false,
    };
    let costs = CostModel {
        fee_frac: Decimal::new(5, 4),        // 5 bps swap fee
        slippage_coeff: Decimal::new(5, 1),  // linear price-impact coefficient
        gas_usd: Decimal::new(50, 2),        // $0.50 per position change
    };
    let backtester = Backtester::new(costs, WalkForward { train_bars: 90, test_bars: 30, embargo_bars: 3 });
    let result = backtester.run(&strategy, dataset, frame, funding)?;
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
