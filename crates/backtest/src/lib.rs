//! `cw-backtest` — the point-in-time backtest engine.
//!
//! Two invariants define this crate's honesty:
//! 1. **No lookahead** — weights at bar `t` are decided from the feature
//!    cross-section at `t` (which uses data through `t`), and earn the return
//!    from `close[t]` to `close[t+1]`.
//! 2. **Honest costs** — every rebalance pays proportional fees, a slippage
//!    haircut, and flat per-trade gas. A cost-free backtest is a lie.
//!
//! Phase-1 scope: a single-pass daily backtest with equal-spacing rebalances at
//! every feature timestamp. `WalkForward` is carried in config for the CV that
//! activates once strategies gain fittable parameters. The slippage term is a
//! linear haircut on turnover; modeling slippage against per-block pool liquidity
//! is deferred (documented as a known simplification).

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, Instrument, Result};
use cw_features::{FeatureFrame, FeatureRow};
use cw_strategy::Strategy;

/// Notional starting capital (USD). Gas is converted to a return drag against
/// current equity, so this sets the scale at which flat costs bite.
const INITIAL_CAPITAL_USD: i64 = 10_000;

/// Transaction-cost parameters applied to every simulated fill.
#[derive(Debug, Clone)]
pub struct CostModel {
    /// DEX swap fee as a fraction (e.g. 0.0005 for a 5 bps pool).
    pub fee_frac: Decimal,
    /// Flat gas cost per changed position, in quote currency (USD).
    pub gas_usd: Decimal,
    /// Linear slippage haircut applied per unit of turnover.
    pub slippage_coeff: Decimal,
}

/// Walk-forward split: rolling train window, out-of-sample test window, with an
/// embargo gap to prevent leakage across the boundary. Carried for future
/// parametric CV; unused by the single-pass Phase-1 run.
#[derive(Debug, Clone, Copy)]
pub struct WalkForward {
    pub train_bars: usize,
    pub test_bars: usize,
    pub embargo_bars: usize,
}

/// One realized fill in the simulated ledger.
#[derive(Debug, Clone)]
pub struct Trade {
    pub ts: DateTime<Utc>,
    pub instrument_name: String,
    pub qty_usd: Decimal,
    pub cost_usd: Decimal,
}

/// Backtest output: the equity curve plus the trade ledger for attribution.
#[derive(Debug, Default, Clone)]
pub struct BacktestResult {
    pub equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    pub trades: Vec<Trade>,
}

/// Drives a [`Strategy`] over historical bars + features under a [`CostModel`].
pub struct Backtester {
    pub costs: CostModel,
    pub walk_forward: WalkForward,
}

fn ms_to_dt(ms: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| CoreError::InvalidData(format!("timestamp out of range: {ms}")))
}

impl Backtester {
    pub fn new(costs: CostModel, walk_forward: WalkForward) -> Self {
        Self { costs, walk_forward }
    }

    /// Run `strategy` over `bars` (per-instrument close series) and `features`
    /// (the rebalance cross-sections). Rebalances at each feature timestamp.
    pub fn run<S: Strategy>(
        &self,
        strategy: &S,
        bars: &[(Instrument, Vec<Bar>)],
        features: &FeatureFrame,
    ) -> Result<BacktestResult> {
        // close lookup: pool -> (ts_millis -> close)
        let mut closes: HashMap<String, HashMap<i64, Decimal>> = HashMap::new();
        for (inst, series) in bars {
            let m = closes.entry(inst.pool.0.clone()).or_default();
            for b in series {
                m.insert(b.ts.timestamp_millis(), b.close);
            }
        }

        // rebalance cross-sections, ordered by timestamp
        let mut sections: BTreeMap<i64, Vec<FeatureRow>> = BTreeMap::new();
        for row in &features.rows {
            sections.entry(row.ts.timestamp_millis()).or_default().push(row.clone());
        }
        let times: Vec<i64> = sections.keys().copied().collect();
        if times.len() < 2 {
            return Ok(BacktestResult::default());
        }

        let (fee, slip, gas) = (self.costs.fee_frac, self.costs.slippage_coeff, self.costs.gas_usd);

        let mut equity = Decimal::from(INITIAL_CAPITAL_USD);
        let mut prev_weights: HashMap<String, Decimal> = HashMap::new();
        let mut result = BacktestResult {
            equity_curve: vec![(ms_to_dt(times[0])?, equity)],
            trades: Vec::new(),
        };

        for pair in times.windows(2) {
            let (t0, t1) = (pair[0], pair[1]);
            let targets = strategy.target_weights(&sections[&t0])?;

            let mut new_weights: HashMap<String, Decimal> = HashMap::new();
            let mut name_by_pool: HashMap<String, String> = HashMap::new();
            for tw in &targets {
                new_weights.insert(tw.instrument.pool.0.clone(), tw.weight);
                name_by_pool.insert(tw.instrument.pool.0.clone(), tw.instrument.name.clone());
            }

            // gross return over [t0, t1]
            let mut gross = Decimal::ZERO;
            for (pool, w) in &new_weights {
                if *w == Decimal::ZERO {
                    continue;
                }
                if let Some(cmap) = closes.get(pool) {
                    if let (Some(c0), Some(c1)) = (cmap.get(&t0), cmap.get(&t1)) {
                        if *c0 > Decimal::ZERO {
                            gross += *w * (c1 / c0 - Decimal::ONE);
                        }
                    }
                }
            }

            // turnover, gas, and per-trade ledger over the union of positions
            let mut turnover = Decimal::ZERO;
            let mut changed = 0i64;
            let pools: HashSet<&String> = new_weights.keys().chain(prev_weights.keys()).collect();
            for pool in pools {
                let wn = new_weights.get(pool).copied().unwrap_or(Decimal::ZERO);
                let wp = prev_weights.get(pool).copied().unwrap_or(Decimal::ZERO);
                let delta = (wn - wp).abs();
                if delta > Decimal::ZERO {
                    turnover += delta;
                    changed += 1;
                    result.trades.push(Trade {
                        ts: ms_to_dt(t0)?,
                        instrument_name: name_by_pool.get(pool).cloned().unwrap_or_else(|| pool.clone()),
                        qty_usd: (wn - wp) * equity,
                        cost_usd: delta * (fee + slip) * equity + gas,
                    });
                }
            }

            let gas_drag = if equity > Decimal::ZERO {
                (Decimal::from(changed) * gas) / equity
            } else {
                Decimal::ZERO
            };
            let net = gross - ((fee + slip) * turnover + gas_drag);
            equity += equity * net;
            result.equity_curve.push((ms_to_dt(t1)?, equity));
            prev_weights = new_weights;
        }
        Ok(result)
    }
}
