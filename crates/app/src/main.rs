//! `coinwatch` — orchestration entry point.
//!
//! Two subcommands cleanly separate the slow, networked ingest from fast, offline
//! research (the shape a scheduled deployment needs):
//!
//! - `coinwatch ingest`  — discover the universe on Base + Arbitrum, register
//!   instruments, and incrementally backfill daily bars into the DuckDB store
//!   (skips pools that are already up to date).
//! - `coinwatch analyze` — reconstruct the universe from the store (no network),
//!   compute combined technical + liquidity features, and backtest the same
//!   `CrossSectional` strategy ranked by either module's feature.

use std::collections::HashMap;
use std::fs;

use chrono::{Duration, Utc};
use cw_backtest::{Backtester, CostModel, WalkForward};
use cw_core::{Bar, CoreError, Decimal, Instrument, Interval, Network, OnchainDaily, Result};
use cw_features::{combine, FeatureContext, FeatureFrame, FeatureModule, FeatureRow, LiquidityFeatures, OnchainFeatures, TechnicalFeatures};
use cw_harness::{feature_ic, walk_forward, WfConfig};
use cw_providers::{BarRequest, Bitquery, DataSource, GeckoTerminal, OnchainSource, UniverseRequest, UniverseSource};
use cw_storage::{DuckStore, Store, TimeRange};
use cw_strategy::CrossSectional;

const DB_PATH: &str = "research/data/coinwatch.duckdb";
/// Minimum bars an instrument needs before it can carry features / be analyzed.
const MIN_BARS: usize = 60;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok(); // load .env (BITQUERY_ACCESS_TOKEN, …) if present
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
        from: Utc::now() - Duration::days(400),
        to: Utc::now() + Duration::days(1),
    }
}

fn feat(row: &FeatureRow, key: &str) -> Decimal {
    row.values.get(key).copied().unwrap_or_default()
}

/// Discover + incrementally backfill the universe. Networked and resilient.
async fn ingest() -> Result<()> {
    let provider = GeckoTerminal::default();
    let store = open_store()?;
    let networks = [Network::Base, Network::Arbitrum];
    let uni_req = UniverseRequest {
        min_reserve_usd: Decimal::from(1_000_000u64),
        limit: 12,
        pages: 1,
    };

    // On-chain flow is optional: only runs when a Bitquery token is configured.
    let bitquery = std::env::var("BITQUERY_ACCESS_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
        .map(Bitquery::new);
    if bitquery.is_none() {
        println!("(no BITQUERY_ACCESS_TOKEN set — skipping on-chain flow)");
    }

    let (mut fetched, mut fresh, mut onchain_rows) = (0usize, 0usize, 0usize);
    for network in &networks {
        let pools = provider.top_pools(network, &uni_req).await?;
        println!("network {}: {} pools (reserve >= ${})", network.slug(), pools.len(), uni_req.min_reserve_usd);
        for pool in &pools {
            store.register_instrument(&pool.instrument)?;

            // On-chain daily flow — always run (forward-accumulate the ~4 recent
            // days the free realtime tier exposes; idempotent upsert).
            if let (Some(bq), Some(token)) = (bitquery.as_ref(), pool.instrument.token.as_deref()) {
                let since = Utc::now() - Duration::days(5);
                match bq.daily_flow(&pool.instrument.network, token, since).await {
                    Ok(flow) if !flow.is_empty() => {
                        onchain_rows += store.upsert_onchain(&pool.instrument.network, token, &flow)?;
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("  {:<28} on-chain: {e}", pool.instrument.name),
                }
            }

            let stored = store.load_bars(&pool.instrument, Interval::Day, wide_range()).unwrap_or_default();
            let up_to_date = stored.len() >= MIN_BARS
                && stored.last().is_some_and(|b| b.ts > Utc::now() - Duration::days(2));
            if up_to_date {
                fresh += 1;
                println!("  {:<28} up-to-date ({} bars)", pool.instrument.name, stored.len());
                continue;
            }
            let req = BarRequest {
                instrument: pool.instrument.clone(),
                interval: Interval::Day,
                limit: 120,
            };
            match provider.fetch_bars(&req).await {
                Ok(bars) if !bars.is_empty() => {
                    let n = store.upsert_bars(&pool.instrument, Interval::Day, &bars)?;
                    fetched += 1;
                    println!("  {:<28} +{} bars", pool.instrument.name, n);
                }
                Ok(_) => println!("  {:<28} no bars", pool.instrument.name),
                Err(e) => eprintln!("  {:<28} fetch failed: {e}", pool.instrument.name),
            }
        }
    }
    println!("ingest complete: {fetched} fetched, {fresh} up-to-date, {onchain_rows} on-chain rows; db: {DB_PATH}");
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

    // On-chain daily aggregates for the same universe (sparse until history grows).
    let mut onchain_dataset: Vec<(Instrument, Vec<OnchainDaily>)> = Vec::new();
    for (inst, _) in &dataset {
        if let Some(token) = inst.token.as_deref() {
            let oc = store.load_onchain(&inst.network, token, wide_range()).unwrap_or_default();
            if !oc.is_empty() {
                onchain_dataset.push((inst.clone(), oc));
            }
        }
    }

    // Three independent feature modules → one combined frame for the same engine.
    let technical = TechnicalFeatures::default();
    let liquidity = LiquidityFeatures::default();
    let onchain = OnchainFeatures;
    let ctx = FeatureContext { bars: &dataset, onchain: &onchain_dataset };
    let frame = combine(&[technical.compute(&ctx)?, liquidity.compute(&ctx)?, onchain.compute(&ctx)?]);
    let oc_days: usize = onchain_dataset.iter().map(|(_, v)| v.len()).sum();
    println!(
        "features: modules [{}, {}, {}] -> {} rows ({} on-chain days / {} tokens)",
        technical.name(), liquidity.name(), onchain.name(), frame.rows.len(), oc_days, onchain_dataset.len()
    );

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
    println!("\nleaderboard (latest bar) top 10 by momentum  [oc_* present only for accumulated days]:");
    for (rank, row) in board.iter().take(10).enumerate() {
        println!(
            "  {:>2}. {:<9} {:<22} mom={:>7.3}  dvol_mom={:>7.3}  oc_buyers={:>6}  oc_tpb={:>6.1}",
            rank + 1,
            row.instrument.network.slug(),
            row.instrument.name,
            feat(row, "mom"),
            feat(row, "dvol_mom"),
            feat(row, "oc_buyers"),
            feat(row, "oc_tpb"),
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
    run_backtest("in-sample: technical momentum", "mom", &dataset, &frame)?;
    run_backtest("in-sample: liquidity dvol-momentum", "dvol_mom", &dataset, &frame)?;

    // Walk-forward CV: sweep configs on each train window, evaluate the winner
    // out-of-sample, and deflate the result for the number of trials. This is the
    // honest number — expect it to be far less flattering than the in-sample runs.
    let costs = CostModel {
        fee_frac: Decimal::new(5, 4),
        slippage_coeff: Decimal::new(5, 1),
        gas_usd: Decimal::new(50, 2),
    };
    let candidates = vec![
        ("mom @40%".to_string(), cross("mom", 0.4)),
        ("mom @60%".to_string(), cross("mom", 0.6)),
        ("dvol_mom @40%".to_string(), cross("dvol_mom", 0.4)),
        ("dvol_mom @60%".to_string(), cross("dvol_mom", 0.6)),
    ];
    let wf = walk_forward(&candidates, &dataset, &frame, WfConfig { train: 40, test: 12, embargo: 2 }, &costs)?;
    if wf.folds.is_empty() {
        println!("\nwalk-forward: not enough history for the chosen windows");
        return Ok(());
    }
    println!("\nwalk-forward ({} folds, {} candidates each):", wf.folds.len(), candidates.len());
    for f in &wf.folds {
        println!(
            "  test {}..{}  chose {:<16} (train SR/step {:+.3})",
            f.test_from.date_naive(), f.test_to.date_naive(), f.chosen, f.train_sharpe
        );
    }
    let oos = cw_eval::evaluate(&wf.oos)?;
    let dsr = cw_eval::deflated_sharpe(&cw_eval::returns(&wf.oos), &wf.trial_sharpes);
    if let (Some((_, s)), Some((_, e))) = (wf.oos.equity_curve.first(), wf.oos.equity_curve.last()) {
        let total = ((e / s - Decimal::ONE) * Decimal::from(100)).round_dp(2);
        println!("  OOS equity: ${} -> ${}  ({}% total)", s.round_dp(2), e.round_dp(2), total);
    }
    println!(
        "  OOS: sharpe={:.2}  maxDD={:.1}%  hit={:.1}%  PSR={:.1}%  DSR={:.1}%  ({} trials)",
        oos.sharpe, oos.max_drawdown * 100.0, oos.hit_rate * 100.0, oos.psr * 100.0, dsr * 100.0, wf.trial_sharpes.len()
    );
    Ok(())
}

/// Shorthand for a long-only cross-sectional candidate.
fn cross(rank_feature: &str, top_quantile: f64) -> CrossSectional {
    CrossSectional {
        rank_feature: rank_feature.to_string(),
        top_quantile,
        allow_short: false,
    }
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
        slippage_coeff: Decimal::new(5, 1), // linear price-impact coefficient
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
