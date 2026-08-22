//! `cw-harness` — research workflows that compose the engine primitives.
//!
//! Lives above `backtest` + `eval` (which cannot depend on each other without a
//! cycle) so it can *select* and *score*. Its headline workflow is
//! [`walk_forward`]: roll train/test windows across the feature timeline, sweep
//! candidate configs on each train window, pick the best by in-train Sharpe, and
//! evaluate the winner out-of-sample — the honest antidote to picking the best
//! full-sample backtest. The stitched OOS returns plus the full set of trial
//! Sharpes feed the Deflated Sharpe Ratio.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use cw_backtest::{BacktestResult, Backtester, CostModel, WalkForward};
use cw_core::{Bar, Decimal, Instrument, Result};
use cw_features::{FeatureFrame, FeatureRow};
use cw_strategy::Strategy;
use rust_decimal::prelude::ToPrimitive;

/// Notional starting capital for the stitched OOS curve.
const OOS_START: f64 = 10_000.0;

/// Walk-forward CV configuration, in units of feature timestamps (bars).
#[derive(Debug, Clone, Copy)]
pub struct WfConfig {
    pub train: usize,
    pub test: usize,
    pub embargo: usize,
}

/// What one fold decided and how the chosen config scored in-sample.
#[derive(Debug, Clone)]
pub struct FoldSummary {
    pub test_from: DateTime<Utc>,
    pub test_to: DateTime<Utc>,
    pub chosen: String,
    pub train_sharpe: f64,
}

/// Result of a walk-forward run.
#[derive(Debug, Clone)]
pub struct WalkForwardResult {
    pub folds: Vec<FoldSummary>,
    /// Stitched out-of-sample equity curve across all test windows.
    pub oos: BacktestResult,
    /// Per-period Sharpe of every (candidate × fold) train run — the multiple-
    /// testing trial distribution for the Deflated Sharpe Ratio.
    pub trial_sharpes: Vec<f64>,
}

fn subframe(frame: &FeatureFrame, ts_set: &HashSet<i64>) -> FeatureFrame {
    FeatureFrame {
        rows: frame
            .rows
            .iter()
            .filter(|r: &&FeatureRow| ts_set.contains(&r.ts.timestamp_millis()))
            .cloned()
            .collect(),
    }
}

fn step_return(e0: &Decimal, e1: &Decimal) -> f64 {
    let (a, b) = (e0.to_f64().unwrap_or(0.0), e1.to_f64().unwrap_or(0.0));
    if a > 0.0 {
        b / a - 1.0
    } else {
        0.0
    }
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
) -> Result<WalkForwardResult> {
    let mut times: Vec<i64> = features.rows.iter().map(|r| r.ts.timestamp_millis()).collect();
    times.sort_unstable();
    times.dedup();

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
        WalkForward { train_bars: cfg.train, test_bars: cfg.test, embargo_bars: cfg.embargo },
    );
    let window = cfg.train + cfg.embargo + cfg.test;
    let ms = |ms: i64| DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default();

    let mut oos_points: Vec<(DateTime<Utc>, f64)> = Vec::new();
    let mut i = 0;
    while i + window <= times.len() {
        let train_set: HashSet<i64> = times[i..i + cfg.train].iter().copied().collect();
        let test_start = i + cfg.train + cfg.embargo;
        let test_set: HashSet<i64> = times[test_start..test_start + cfg.test].iter().copied().collect();

        // Sweep candidates on the train window; pick the best in-train Sharpe.
        let train_frame = subframe(features, &train_set);
        let mut best: Option<(usize, f64)> = None;
        for (idx, (_, strat)) in candidates.iter().enumerate() {
            let train_res = backtester.run(strat, bars, &train_frame)?;
            let sharpe = cw_eval::per_period_sharpe(&cw_eval::returns(&train_res));
            result.trial_sharpes.push(sharpe);
            if best.is_none_or(|(_, bs)| sharpe > bs) {
                best = Some((idx, sharpe));
            }
        }
        let (best_idx, best_sharpe) = best.expect("candidates non-empty");
        let (chosen_label, chosen_strat) = &candidates[best_idx];

        // Evaluate the winner out-of-sample and collect its step returns.
        let test_frame = subframe(features, &test_set);
        let test_res = backtester.run(chosen_strat, bars, &test_frame)?;
        for w in test_res.equity_curve.windows(2) {
            oos_points.push((w[1].0, step_return(&w[0].1, &w[1].1)));
        }

        result.folds.push(FoldSummary {
            test_from: ms(times[test_start]),
            test_to: ms(times[test_start + cfg.test - 1]),
            chosen: chosen_label.clone(),
            train_sharpe: best_sharpe,
        });

        i += cfg.test; // contiguous, non-overlapping OOS windows
    }

    result.oos = stitch(&oos_points);
    Ok(result)
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
                        open: c, high: c, low: c, close: c,
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
                    values: BTreeMap::from([("mom".to_string(), Decimal::from(if up { d as i64 } else { -(d as i64) }))]),
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
            CrossSectional { rank_feature: "mom".to_string(), top_quantile: 0.5, allow_short: false },
        )];
        let costs = CostModel {
            fee_frac: Decimal::new(5, 4),
            slippage_coeff: Decimal::new(10, 4),
            gas_usd: Decimal::ZERO,
        };
        let wf = walk_forward(&candidates, &bars, &frame, WfConfig { train: 15, test: 5, embargo: 1 }, &costs).unwrap();
        assert!(!wf.folds.is_empty());
        assert!(!wf.oos.equity_curve.is_empty());
        assert_eq!(wf.trial_sharpes.len(), wf.folds.len()); // 1 candidate per fold
    }
}
