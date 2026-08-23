//! `cw-harness` — research workflows that compose the engine primitives.
//!
//! Lives above `backtest` + `eval` (which cannot depend on each other without a
//! cycle) so it can *select* and *score*. Its headline workflow is
//! [`walk_forward`]: roll train/test windows across the feature timeline, sweep
//! candidate configs on each train window, pick the best by in-train Sharpe, and
//! evaluate the winner out-of-sample — the honest antidote to picking the best
//! full-sample backtest. The stitched OOS returns plus the full set of trial
//! Sharpes feed the Deflated Sharpe Ratio.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use cw_backtest::{BacktestResult, Backtester, CostModel, Prepared, WalkForward};
use cw_core::{Bar, Decimal, FundingPoint, Instrument, Result};
use cw_features::{FeatureFrame, FeatureRow};
use cw_strategy::Strategy;
use rust_decimal::prelude::ToPrimitive;

/// Notional starting capital for the stitched OOS curve.
const OOS_START: f64 = 10_000.0;

/// Walk-forward CV + per-fold execution configuration. Train/test/embargo are in
/// feature timestamps (bars); the two knobs are applied to every fold's backtest.
#[derive(Debug, Clone, Copy)]
pub struct WfConfig {
    /// Training window length (bars).
    pub train: usize,
    /// Test window length (bars).
    pub test: usize,
    /// Embargo gap (bars) between train and test.
    pub embargo: usize,
    /// Portfolio vol target for each fold's backtest (None = off).
    pub vol_target: Option<Decimal>,
    /// Rebalance cadence (bars) for each fold's backtest (1 = every bar).
    pub rebalance_every: usize,
}

/// What one fold decided and how the chosen config scored in-sample.
#[derive(Debug, Clone)]
pub struct FoldSummary {
    /// First timestamp of the fold's test window.
    pub test_from: DateTime<Utc>,
    /// Last timestamp of the fold's test window.
    pub test_to: DateTime<Utc>,
    /// Label of the config selected on the train window.
    pub chosen: String,
    /// In-train per-period Sharpe of the selected config.
    pub train_sharpe: f64,
}

/// Result of a walk-forward run.
#[derive(Debug, Clone)]
pub struct WalkForwardResult {
    /// Per-fold summaries (selection + test window).
    pub folds: Vec<FoldSummary>,
    /// Stitched out-of-sample equity curve across all test windows.
    pub oos: BacktestResult,
    /// Per-period Sharpe of every (candidate × fold) train run — the multiple-
    /// testing trial distribution for the Deflated Sharpe Ratio.
    pub trial_sharpes: Vec<f64>,
}

fn step_return(e0: &Decimal, e1: &Decimal) -> f64 {
    let (a, b) = (e0.to_f64().unwrap_or(0.0), e1.to_f64().unwrap_or(0.0));
    if a > 0.0 { b / a - 1.0 } else { 0.0 }
}

/// Compound `(ts, per-step-return)` points into an equity curve from `OOS_START`.
fn stitch(points: &[(DateTime<Utc>, f64)]) -> BacktestResult {
    let mut equity = OOS_START;
    let mut curve = Vec::with_capacity(points.len() + 1);
    if let Some((first_ts, _)) = points.first() {
        curve.push((*first_ts, Decimal::from_f64_retain(equity).unwrap_or_default()));
    }
    for (ts, r) in points {
        equity *= 1.0 + r;
        curve.push((*ts, Decimal::from_f64_retain(equity).unwrap_or_default()));
    }
    BacktestResult {
        equity_curve: curve,
        trades: Vec::new(),
    }
}

/// Roll walk-forward folds, sweeping `candidates` on each train window and
/// evaluating the in-train winner out-of-sample. Test windows tile contiguously,
/// so the concatenated OOS returns form one honest equity curve.
pub fn walk_forward<S: Strategy>(
    candidates: &[(String, S)],
    bars: &[(Instrument, Vec<Bar>)],
    features: &FeatureFrame,
    cfg: WfConfig,
    costs: &CostModel,
    funding: &[(Instrument, Vec<FundingPoint>)],
) -> Result<WalkForwardResult> {
    walk_forward_prepared(candidates, &Prepared::new(bars, features, funding), cfg, costs)
}

/// Like [`walk_forward`] but over a shared, precomputed [`Prepared`] so a sweep can
/// build it once and reuse it across every candidate.
pub fn walk_forward_prepared<S: Strategy>(
    candidates: &[(String, S)],
    prep: &Prepared,
    cfg: WfConfig,
    costs: &CostModel,
) -> Result<WalkForwardResult> {
    let times = &prep.times;

    let mut result = WalkForwardResult {
        folds: Vec::new(),
        oos: BacktestResult::default(),
        trial_sharpes: Vec::new(),
    };
    if candidates.is_empty() || cfg.train == 0 || cfg.test == 0 {
        return Ok(result);
    }

    let backtester = Backtester::new(
        costs.clone(),
        WalkForward {
            train_bars: cfg.train,
            test_bars: cfg.test,
            embargo_bars: cfg.embargo,
        },
    )
    .with_vol_target(cfg.vol_target)
    .with_rebalance_every(cfg.rebalance_every);
    let window = cfg.train + cfg.embargo + cfg.test;
    let ms = |ms: i64| DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default();

    let mut oos_points: Vec<(DateTime<Utc>, f64)> = Vec::new();
    // Positions carried across folds so the OOS is a continuously-held path.
    let mut carry: HashMap<String, Decimal> = HashMap::new();
    let mut i = 0;
    while i + window <= times.len() {
        let Some(train_times) = times.get(i..i + cfg.train) else {
            break;
        };
        let test_start = i + cfg.train + cfg.embargo;
        let Some(test_times) = times.get(test_start..test_start + cfg.test) else {
            break;
        };

        // Sweep candidates on the train window; pick the best in-train Sharpe.
        let mut best: Option<(usize, f64)> = None;
        for (idx, (_, strat)) in candidates.iter().enumerate() {
            let (train_res, _) = backtester.run_prepared(strat, prep, train_times, &HashMap::new(), false)?;
            let sharpe = cw_eval::per_period_sharpe(&cw_eval::returns(&train_res));
            result.trial_sharpes.push(sharpe);
            if best.is_none_or(|(_, bs)| sharpe > bs) {
                best = Some((idx, sharpe));
            }
        }
        let Some((best_idx, best_sharpe)) = best else { break };
        let Some((chosen_label, chosen_strat)) = candidates.get(best_idx) else {
            break;
        };

        // Evaluate the winner out-of-sample, carrying positions across folds so the
        // OOS is a continuously-held path: each fold seeds from the prior fold's
        // ending positions instead of re-entering from cash (only the delta when the
        // chosen strategy changes is traded). Without this, 35 full cash re-entries
        // wildly deflate OOS returns — especially in HL's thin early-2023 market.
        let (test_res, end_held) = backtester.run_prepared(chosen_strat, prep, test_times, &carry, false)?;
        for w in test_res.equity_curve.windows(2) {
            let [a, b] = w else { continue };
            oos_points.push((b.0, step_return(&a.1, &b.1)));
        }
        carry = end_held;

        result.folds.push(FoldSummary {
            test_from: times.get(test_start).copied().map(&ms).unwrap_or_default(),
            test_to: times.get(test_start + cfg.test - 1).copied().map(&ms).unwrap_or_default(),
            chosen: chosen_label.clone(),
            train_sharpe: best_sharpe,
        });

        i += cfg.test; // contiguous, non-overlapping OOS windows
    }

    result.oos = stitch(&oos_points);
    Ok(result)
}

/// Minimum instruments in a cross-section to compute a rank IC for that bar.
const MIN_XS: usize = 4;

/// Predictive-power summary for one feature column: the cross-sectional rank
/// correlation (Spearman IC) between the feature and next-bar returns, averaged
/// over time. The IC-IR (`mean_ic / ic_std`) and its `t_stat` say whether that
/// signal is stable enough to bother backtesting.
#[derive(Debug, Clone)]
pub struct FeatureIC {
    /// Feature column name.
    pub feature: String,
    /// Time-averaged cross-sectional rank IC.
    pub mean_ic: f64,
    /// IC information ratio (mean_ic / ic_std).
    pub icir: f64,
    /// t-statistic of the mean IC.
    pub t_stat: f64,
    /// Fraction of periods with positive IC.
    pub hit_rate: f64,
    /// Number of cross-sections averaged.
    pub n_periods: usize,
}

/// Average ranks (1-based, ties share the mean rank).
fn ranks(v: &[f64]) -> Vec<f64> {
    let mut order: Vec<(usize, f64)> = v.iter().copied().enumerate().collect();
    order.sort_by(|a, b| a.1.total_cmp(&b.1));
    let mut r = vec![0.0; v.len()];
    let mut rank = 1.0;
    for group in order.chunk_by(|a, b| a.1.total_cmp(&b.1) == Ordering::Equal) {
        let size = group.len() as f64;
        let avg = rank + (size - 1.0) / 2.0;
        for &(orig, _) in group {
            if let Some(slot) = r.get_mut(orig) {
                *slot = avg;
            }
        }
        rank += size;
    }
    r
}

fn pearson(x: &[f64], y: &[f64]) -> Option<f64> {
    let n = x.len() as f64;
    if n < 2.0 {
        return None;
    }
    let (mx, my) = (x.iter().sum::<f64>() / n, y.iter().sum::<f64>() / n);
    let (mut cov, mut vx, mut vy) = (0.0, 0.0, 0.0);
    for (a, b) in x.iter().zip(y) {
        let (dx, dy) = (a - mx, b - my);
        cov += dx * dy;
        vx += dx * dx;
        vy += dy * dy;
    }
    if vx <= 0.0 || vy <= 0.0 {
        return None;
    }
    Some(cov / (vx.sqrt() * vy.sqrt()))
}

fn spearman(x: &[f64], y: &[f64]) -> Option<f64> {
    pearson(&ranks(x), &ranks(y))
}

/// Information Coefficient per feature: the time-averaged cross-sectional rank
/// correlation between each feature and next-bar returns. The first-pass screen
/// for "which features actually predict?" — sorted by |IC-IR|, strongest first.
pub fn feature_ic(bars: &[(Instrument, Vec<Bar>)], features: &FeatureFrame) -> Vec<FeatureIC> {
    // next-bar return per (pool, ts)
    let mut fwd: HashMap<String, HashMap<i64, f64>> = HashMap::new();
    for (inst, series) in bars {
        let mut s: Vec<&Bar> = series.iter().collect();
        s.sort_by_key(|b| b.ts);
        let m = fwd.entry(inst.pool.0.clone()).or_default();
        for w in s.windows(2) {
            let [b0, b1] = w else { continue };
            let c0 = b0.close.to_f64().unwrap_or(0.0);
            let c1 = b1.close.to_f64().unwrap_or(0.0);
            if c0 > 0.0 {
                m.insert(b0.ts.timestamp_millis(), c1 / c0 - 1.0);
            }
        }
    }

    let mut by_ts: BTreeMap<i64, Vec<&FeatureRow>> = BTreeMap::new();
    let mut cols: BTreeSet<String> = BTreeSet::new();
    for row in &features.rows {
        by_ts.entry(row.ts.timestamp_millis()).or_default().push(row);
        cols.extend(row.values.keys().cloned());
    }

    let mut out = Vec::new();
    for col in &cols {
        let mut ics: Vec<f64> = Vec::new();
        for (ts, rows) in &by_ts {
            let (mut fv, mut rv) = (Vec::new(), Vec::new());
            for row in rows {
                if let (Some(v), Some(r)) = (
                    row.values.get(col),
                    fwd.get(row.instrument.pool.0.as_str()).and_then(|m| m.get(ts)),
                ) {
                    fv.push(v.to_f64().unwrap_or(0.0));
                    rv.push(*r);
                }
            }
            if fv.len() >= MIN_XS
                && let Some(ic) = spearman(&fv, &rv).filter(|x| x.is_finite())
            {
                ics.push(ic);
            }
        }
        if ics.is_empty() {
            continue;
        }
        let n = ics.len() as f64;
        let mean = ics.iter().sum::<f64>() / n;
        let std = (ics.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n).sqrt();
        let icir = if std > 0.0 { mean / std } else { 0.0 };
        out.push(FeatureIC {
            feature: col.clone(),
            mean_ic: mean,
            icir,
            t_stat: icir * n.sqrt(),
            hit_rate: ics.iter().filter(|x| **x > 0.0).count() as f64 / n,
            n_periods: ics.len(),
        });
    }
    out.sort_by(|a, b| b.icir.abs().partial_cmp(&a.icir.abs()).unwrap_or(Ordering::Equal));
    out
}

/// One row of a strategy sweep: a config's out-of-sample performance, deflated
/// for the number of configs tried.
#[derive(Debug, Clone)]
pub struct SweepRow {
    /// Config label.
    pub label: String,
    /// Out-of-sample annualized Sharpe.
    pub oos_sharpe: f64,
    /// Out-of-sample total return (%).
    pub oos_return_pct: f64,
    /// Maximum out-of-sample drawdown (fraction).
    pub max_drawdown: f64,
    /// Deflated Sharpe Ratio (multiple-testing-aware).
    pub dsr: f64,
    /// Number of walk-forward folds.
    pub n_folds: usize,
}

/// Evaluate each candidate independently out-of-sample (its own walk-forward),
/// then deflate every result by the full set of trial Sharpes. This is the honest
/// "strategy leaderboard": ranked by Deflated Sharpe Ratio, strongest first.
pub fn sweep<S: Strategy>(
    candidates: &[(String, S)],
    bars: &[(Instrument, Vec<Bar>)],
    features: &FeatureFrame,
    cfg: WfConfig,
    costs: &CostModel,
    funding: &[(Instrument, Vec<FundingPoint>)],
) -> Result<Vec<SweepRow>> {
    let prep = Prepared::new(bars, features, funding);
    let mut runs = Vec::with_capacity(candidates.len());
    let mut trial_sharpes = Vec::with_capacity(candidates.len());
    for cand in candidates {
        let wf = walk_forward_prepared(std::slice::from_ref(cand), &prep, cfg, costs)?;
        let rets = cw_eval::returns(&wf.oos);
        trial_sharpes.push(cw_eval::per_period_sharpe(&rets));
        runs.push((cand.0.clone(), wf, rets));
    }

    let mut rows = Vec::with_capacity(runs.len());
    for (label, wf, rets) in &runs {
        let m = cw_eval::evaluate(&wf.oos)?;
        let ret_pct = match (wf.oos.equity_curve.first(), wf.oos.equity_curve.last()) {
            (Some((_, e0)), Some((_, e1))) => {
                let (a, b) = (e0.to_f64().unwrap_or(0.0), e1.to_f64().unwrap_or(0.0));
                if a > 0.0 { (b / a - 1.0) * 100.0 } else { 0.0 }
            }
            _ => 0.0,
        };
        rows.push(SweepRow {
            label: label.clone(),
            oos_sharpe: m.sharpe,
            oos_return_pct: ret_pct,
            max_drawdown: m.max_drawdown,
            dsr: cw_eval::deflated_sharpe(rets, &trial_sharpes),
            n_folds: wf.folds.len(),
        });
    }
    rows.sort_by(|a, b| b.dsr.partial_cmp(&a.dsr).unwrap_or(Ordering::Equal));
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use cw_backtest::CostModel;
    use cw_core::{Instrument, Network, PoolId};
    use cw_features::FeatureRow;
    use cw_strategy::CrossSectional;
    use std::collections::BTreeMap;

    fn inst(name: &str) -> Instrument {
        Instrument {
            network: Network::Base,
            pool: PoolId(name.to_string()),
            name: name.to_string(),
            token: None,
        }
    }

    // Two instruments over N days; A trends up, B trends down — momentum should
    // have something to rank. Build both bars and a matching feature frame.
    fn synthetic(days: usize) -> (Vec<(Instrument, Vec<Bar>)>, FeatureFrame) {
        let mk_bars = |up: bool| -> Vec<Bar> {
            (0..days)
                .map(|d| {
                    let p = if up { 100.0 + d as f64 } else { 200.0 - d as f64 };
                    let c = Decimal::from_f64_retain(p).unwrap();
                    Bar {
                        ts: Utc.timestamp_opt(1_700_000_000 + d as i64 * 86_400, 0).unwrap(),
                        open: c,
                        high: c,
                        low: c,
                        close: c,
                        volume: Decimal::from(1000),
                    }
                })
                .collect()
        };
        let a = inst("A");
        let b = inst("B");
        let bars = vec![(a.clone(), mk_bars(true)), (b.clone(), mk_bars(false))];

        let mut rows = Vec::new();
        for (instrument, series) in &bars {
            let up = instrument.name == "A";
            for (d, bar) in series.iter().enumerate() {
                rows.push(FeatureRow {
                    instrument: instrument.clone(),
                    ts: bar.ts,
                    values: BTreeMap::from([(
                        "mom".to_string(),
                        Decimal::from(if up { d as i64 } else { -(d as i64) }),
                    )]),
                });
            }
        }
        (bars, FeatureFrame { rows })
    }

    #[test]
    fn walk_forward_produces_folds_and_oos() {
        let (bars, frame) = synthetic(40);
        let candidates = vec![(
            "mom@50".to_string(),
            CrossSectional {
                rank_feature: "mom".to_string(),
                top_quantile: 0.5,
                allow_short: false,
                invert: false,
                weight_feature: None,
            },
        )];
        let costs = CostModel {
            fee_frac: Decimal::new(5, 4),
            slippage_coeff: Decimal::new(10, 4),
            gas_usd: Decimal::ZERO,
        };
        let wf = walk_forward(
            &candidates,
            &bars,
            &frame,
            WfConfig {
                train: 15,
                test: 5,
                embargo: 1,
                vol_target: None,
                rebalance_every: 1,
            },
            &costs,
            &[],
        )
        .unwrap();
        assert!(!wf.folds.is_empty());
        assert!(!wf.oos.equity_curve.is_empty());
        assert_eq!(wf.trial_sharpes.len(), wf.folds.len()); // 1 candidate per fold
    }

    #[test]
    fn feature_ic_detects_a_predictive_feature() {
        // 5 instruments, each growing at a fixed daily rate; a feature equal to that
        // rate perfectly ranks next-bar returns → cross-sectional IC ≈ 1.
        let rates: [f64; 5] = [0.01, 0.02, 0.03, 0.04, 0.05];
        let day = |d: i64| Utc.timestamp_opt(1_700_000_000 + d * 86_400, 0).unwrap();
        let (mut bars, mut rows) = (Vec::new(), Vec::new());
        for (i, &rate) in rates.iter().enumerate() {
            let instrument = inst(&format!("T{i}"));
            let series: Vec<Bar> = (0..6)
                .map(|d| {
                    let c = Decimal::from_f64_retain(100.0 * (1.0 + rate).powi(d as i32)).unwrap();
                    Bar {
                        ts: day(d),
                        open: c,
                        high: c,
                        low: c,
                        close: c,
                        volume: Decimal::from(1000),
                    }
                })
                .collect();
            for bar in &series {
                rows.push(FeatureRow {
                    instrument: instrument.clone(),
                    ts: bar.ts,
                    values: BTreeMap::from([("g".to_string(), Decimal::from_f64_retain(rate).unwrap())]),
                });
            }
            bars.push((instrument, series));
        }
        let ics = feature_ic(&bars, &FeatureFrame { rows });
        let g = ics.iter().find(|f| f.feature == "g").expect("feature g present");
        assert!(g.mean_ic > 0.99, "expected near-perfect IC, got {}", g.mean_ic);
        assert!(g.n_periods >= 3);
    }

    #[test]
    fn sweep_returns_a_row_per_candidate() {
        let (bars, frame) = synthetic(40);
        let candidates = vec![
            (
                "mom@50".to_string(),
                CrossSectional {
                    rank_feature: "mom".to_string(),
                    top_quantile: 0.5,
                    allow_short: false,
                    invert: false,
                    weight_feature: None,
                },
            ),
            (
                "mom@30".to_string(),
                CrossSectional {
                    rank_feature: "mom".to_string(),
                    top_quantile: 0.3,
                    allow_short: false,
                    invert: false,
                    weight_feature: None,
                },
            ),
        ];
        let costs = CostModel {
            fee_frac: Decimal::new(5, 4),
            slippage_coeff: Decimal::new(5, 1),
            gas_usd: Decimal::ZERO,
        };
        let rows = sweep(
            &candidates,
            &bars,
            &frame,
            WfConfig {
                train: 15,
                test: 5,
                embargo: 1,
                vol_target: None,
                rebalance_every: 1,
            },
            &costs,
            &[],
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }
}
