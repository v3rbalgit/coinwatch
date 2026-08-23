//! `cw-backtest` — the point-in-time backtest engine.
//!
//! Two invariants define this crate's honesty:
//! 1. **No lookahead** — weights at bar `t` are decided from the feature
//!    cross-section at `t` (which uses data through `t`), and earn the return
//!    from `close[t]` to `close[t+1]`.
//! 2. **Honest costs** — every rebalance pays proportional fees, **depth-aware
//!    slippage** (linear price impact scaled by trade size vs. the pool's daily
//!    volume), and flat per-trade gas. A cost-free backtest is a lie.
//!
//! Phase-1 scope: a single-pass daily backtest with equal-spacing rebalances at
//! every feature timestamp. `WalkForward` is carried in config for the CV that
//! activates once strategies gain fittable parameters. Slippage uses the daily bar
//! `volume` as the liquidity proxy; per-block pool depth would be finer but needs
//! richer data.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, FundingPoint, Instrument, Result};
use cw_features::{FeatureFrame, FeatureRow};
use cw_strategy::Strategy;
use rust_decimal::prelude::ToPrimitive;

/// Notional starting capital (USD). Gas is converted to a return drag against
/// current equity, so this sets the scale at which flat costs bite.
const INITIAL_CAPITAL_USD: i64 = 10_000;
/// Trailing window (bars) for realized-vol estimation in vol targeting.
const VOL_WINDOW: usize = 20;
/// Cap on the vol-target leverage multiplier.
const MAX_LEVERAGE: f64 = 3.0;
/// Cap on a trade's participation in a pool's daily volume (slippage saturates here).
const MAX_PARTICIPATION: f64 = 0.5;

/// Transaction-cost parameters applied to every simulated fill.
#[derive(Debug, Clone)]
pub struct CostModel {
    /// DEX swap fee as a fraction (e.g. 0.0005 for a 5 bps pool).
    pub fee_frac: Decimal,
    /// Flat gas cost per changed position, in quote currency (USD).
    pub gas_usd: Decimal,
    /// Linear price-impact coefficient. Per-trade slippage = `coeff × notional ×
    /// participation`, where `participation = min(notional / pool_daily_volume, 0.5)`.
    /// So small trades in deep pools cost ~nothing; large trades in thin pools cost more.
    pub slippage_coeff: Decimal,
}

/// Walk-forward split: rolling train window, out-of-sample test window, with an
/// embargo gap to prevent leakage across the boundary. Carried for future
/// parametric CV; unused by the single-pass Phase-1 run.
#[derive(Debug, Clone, Copy)]
pub struct WalkForward {
    /// Training window length in bars.
    pub train_bars: usize,
    /// Out-of-sample test window length in bars.
    pub test_bars: usize,
    /// Embargo gap (bars) between train and test to prevent leakage.
    pub embargo_bars: usize,
}

/// One realized fill in the simulated ledger.
#[derive(Debug, Clone)]
pub struct Trade {
    /// Fill timestamp (UTC).
    pub ts: DateTime<Utc>,
    /// Human-readable instrument symbol.
    pub instrument_name: String,
    /// Signed traded notional (USD); sign = buy/sell.
    pub qty_usd: Decimal,
    /// Transaction cost charged for the fill (USD).
    pub cost_usd: Decimal,
}

/// Backtest output: the equity curve plus the trade ledger for attribution.
#[derive(Debug, Default, Clone)]
pub struct BacktestResult {
    /// Equity value at each rebalance timestamp.
    pub equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    /// Per-fill ledger for cost/turnover attribution.
    pub trades: Vec<Trade>,
}

/// Drives a [`Strategy`] over historical bars + features under a [`CostModel`].
pub struct Backtester {
    /// Transaction-cost model applied to every fill.
    pub costs: CostModel,
    /// Walk-forward split (carried for the harness; single runs ignore it).
    pub walk_forward: WalkForward,
    /// Optional target daily volatility; when set, gross exposure is scaled each
    /// step by target / trailing-realized-vol (capped) to control the geometric
    /// volatility drag that ruins high-variance crypto strategies.
    pub vol_target: Option<Decimal>,
    /// Rebalance the portfolio every N bars (default 1 = every bar). Higher values
    /// cut turnover: between rebalances weights are held and no costs are paid.
    pub rebalance_every: usize,
}

fn ms_to_dt(ms: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| CoreError::InvalidData(format!("timestamp out of range: {ms}")))
}

/// Exposure scale for vol targeting from trailing realized daily vol (past
/// returns only — no lookahead). Returns 1.0 when targeting is off or too few
/// observations exist.
fn vol_target_scale(target: Option<Decimal>, past: &[f64]) -> f64 {
    let Some(t) = target else { return 1.0 };
    let tf = t.to_f64().unwrap_or(0.0);
    if past.len() < 5 || tf <= 0.0 {
        return 1.0;
    }
    let Some(w) = past.get(past.len().saturating_sub(VOL_WINDOW)..) else {
        return 1.0;
    };
    let mean = w.iter().sum::<f64>() / w.len() as f64;
    let var = w.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / w.len() as f64;
    let vol = var.sqrt();
    if vol > 1e-9 {
        (tf / vol).clamp(0.0, MAX_LEVERAGE)
    } else {
        MAX_LEVERAGE
    }
}

/// Price/volume/funding lookups precomputed once and shared across many backtest
/// runs (e.g. every fold of a walk-forward) so the per-run loop never rebuilds them.
struct MarketData {
    closes: HashMap<String, HashMap<i64, f64>>,
    volumes: HashMap<String, HashMap<i64, f64>>,
    funding_daily: HashMap<String, HashMap<i64, f64>>,
}

/// Everything a run needs, computed once: market lookups (as `f64` for the hot
/// loop), instrument display names, the cross-section per timestamp, and the
/// ordered rebalance timestamps. Walk-forward slices [`Prepared::times`] per fold
/// and reuses the rest instead of rebuilding lookups and re-cloning a subframe
/// every fold.
pub struct Prepared {
    market: MarketData,
    names: HashMap<String, String>,
    sections: BTreeMap<i64, Vec<FeatureRow>>,
    /// Ordered rebalance timestamps (ms), ascending and deduplicated.
    pub times: Vec<i64>,
}

impl Prepared {
    /// Precompute market lookups + per-timestamp cross-sections from raw inputs.
    #[must_use]
    pub fn new(
        bars: &[(Instrument, Vec<Bar>)],
        features: &FeatureFrame,
        funding: &[(Instrument, Vec<FundingPoint>)],
    ) -> Self {
        let mut closes: HashMap<String, HashMap<i64, f64>> = HashMap::new();
        let mut volumes: HashMap<String, HashMap<i64, f64>> = HashMap::new();
        let mut names: HashMap<String, String> = HashMap::new();
        for (inst, series) in bars {
            names.insert(inst.pool.0.clone(), inst.name.clone());
            let c = closes.entry(inst.pool.0.clone()).or_default();
            let v = volumes.entry(inst.pool.0.clone()).or_default();
            for b in series {
                let ms = b.ts.timestamp_millis();
                c.insert(ms, b.close.to_f64().unwrap_or(0.0));
                v.insert(ms, b.volume.to_f64().unwrap_or(0.0));
            }
        }
        let mut funding_daily: HashMap<String, HashMap<i64, f64>> = HashMap::new();
        for (inst, points) in funding {
            let m = funding_daily.entry(inst.pool.0.clone()).or_default();
            for p in points {
                let day = (p.time.timestamp_millis() / 86_400_000) * 86_400_000;
                *m.entry(day).or_insert(0.0) += p.funding_rate.to_f64().unwrap_or(0.0);
            }
        }
        let mut sections: BTreeMap<i64, Vec<FeatureRow>> = BTreeMap::new();
        for row in &features.rows {
            sections.entry(row.ts.timestamp_millis()).or_default().push(row.clone());
        }
        let times: Vec<i64> = sections.keys().copied().collect();
        Self {
            market: MarketData {
                closes,
                volumes,
                funding_daily,
            },
            names,
            sections,
            times,
        }
    }
}

impl Backtester {
    /// Create a backtester from a cost model and a walk-forward split.
    #[must_use]
    pub const fn new(costs: CostModel, walk_forward: WalkForward) -> Self {
        Self {
            costs,
            walk_forward,
            vol_target: None,
            rebalance_every: 1,
        }
    }

    /// Enable portfolio volatility targeting at `target` daily vol.
    #[must_use]
    pub const fn with_vol_target(mut self, target: Option<Decimal>) -> Self {
        self.vol_target = target;
        self
    }

    /// Rebalance every `every` bars (turnover control); `every` < 1 is treated as 1.
    #[must_use]
    pub fn with_rebalance_every(mut self, every: usize) -> Self {
        self.rebalance_every = every.max(1);
        self
    }

    /// Run `strategy` over `bars`, `features`, and per-instrument `funding` from an
    /// all-cash start. Rebalances at each feature timestamp; each step nets the price
    /// return, funding P&L (longs pay positive funding, shorts receive), and costs.
    /// `funding` may be empty (spot-like: no funding P&L). See [`Backtester::run_stateful`]
    /// for the carried-position variant used by walk-forward.
    pub fn run<S: Strategy>(
        &self,
        strategy: &S,
        bars: &[(Instrument, Vec<Bar>)],
        features: &FeatureFrame,
        funding: &[(Instrument, Vec<FundingPoint>)],
    ) -> Result<BacktestResult> {
        let prep = Prepared::new(bars, features, funding);
        Ok(self.run_prepared(strategy, &prep, &prep.times, &HashMap::new(), true)?.0)
    }

    /// Like [`Backtester::run`] but seeds starting positions with `initial_held` and
    /// returns the ending positions, so a caller (walk-forward) can carry a portfolio
    /// across segment boundaries instead of re-entering from cash every segment.
    pub fn run_stateful<S: Strategy>(
        &self,
        strategy: &S,
        bars: &[(Instrument, Vec<Bar>)],
        features: &FeatureFrame,
        funding: &[(Instrument, Vec<FundingPoint>)],
        initial_held: &HashMap<String, Decimal>,
    ) -> Result<(BacktestResult, HashMap<String, Decimal>)> {
        let prep = Prepared::new(bars, features, funding);
        self.run_prepared(strategy, &prep, &prep.times, initial_held, true)
    }

    /// Shared core: run over precomputed [`Prepared`] data and an explicit `times`
    /// window (a contiguous slice of [`Prepared::times`]), seeded with `initial_held`.
    /// Walk-forward slices `times` per fold and reuses `prep` across all folds. When
    /// `ledger` is false the per-trade ledger is skipped (walk-forward keeps only the
    /// stitched returns). Arithmetic runs in `f64`; equity/trade amounts are stored
    /// back as [`Decimal`] at the boundary.
    pub fn run_prepared<S: Strategy>(
        &self,
        strategy: &S,
        prep: &Prepared,
        times: &[i64],
        initial_held: &HashMap<String, Decimal>,
        ledger: bool,
    ) -> Result<(BacktestResult, HashMap<String, Decimal>)> {
        if times.len() < 2 {
            return Ok((BacktestResult::default(), initial_held.clone()));
        }
        let to_dec = |x: f64| Decimal::from_f64_retain(x).unwrap_or_default();
        let fee = self.costs.fee_frac.to_f64().unwrap_or(0.0);
        let slip = self.costs.slippage_coeff.to_f64().unwrap_or(0.0);
        let gas = self.costs.gas_usd.to_f64().unwrap_or(0.0);

        let mut equity = INITIAL_CAPITAL_USD as f64;
        let mut held: HashMap<String, f64> = initial_held
            .iter()
            .map(|(k, v)| (k.clone(), v.to_f64().unwrap_or(0.0)))
            .collect();
        let mut net_returns: Vec<f64> = Vec::new();
        let mut result = BacktestResult {
            equity_curve: vec![(ms_to_dt(times.first().copied().unwrap_or_default())?, to_dec(equity))],
            trades: Vec::new(),
        };
        let cadence = self.rebalance_every.max(1);

        for (step, pair) in times.windows(2).enumerate() {
            let &[t0, t1] = pair else { continue };

            // Rebalance only every `cadence` bars; between, hold weights and pay no
            // costs — the turnover-control lever.
            let mut total_cost = 0.0_f64;
            if step % cadence == 0 {
                let Some(section) = prep.sections.get(&t0) else {
                    continue;
                };
                let targets = strategy.target_weights(section)?;
                // Vol targeting scales gross exposure from trailing realized vol.
                let scale = vol_target_scale(self.vol_target, &net_returns);
                let mut target: HashMap<String, f64> = HashMap::with_capacity(targets.len());
                for tw in &targets {
                    target.insert(tw.instrument.pool.0.clone(), tw.weight.to_f64().unwrap_or(0.0) * scale);
                }

                // depth-aware costs + (optional) per-trade ledger over the position union
                let pools: HashSet<&String> = target.keys().chain(held.keys()).collect();
                for pool in pools {
                    let wn = target.get(pool).copied().unwrap_or(0.0);
                    let wp = held.get(pool).copied().unwrap_or(0.0);
                    let delta = (wn - wp).abs();
                    if delta <= 0.0 {
                        continue;
                    }
                    let notional = delta * equity;
                    // Price impact scales with participation in the pool's daily volume.
                    let volume = prep.market.volumes.get(pool).and_then(|m| m.get(&t0)).copied().unwrap_or(0.0);
                    let participation = if volume > 0.0 {
                        (notional / volume).min(MAX_PARTICIPATION)
                    } else {
                        MAX_PARTICIPATION
                    };
                    let cost = fee * notional + slip * notional * participation + gas;
                    total_cost += cost;
                    if ledger {
                        result.trades.push(Trade {
                            ts: ms_to_dt(t0)?,
                            instrument_name: prep.names.get(pool).cloned().unwrap_or_else(|| pool.clone()),
                            qty_usd: to_dec((wn - wp) * equity),
                            cost_usd: to_dec(cost),
                        });
                    }
                }
                held = target;
            }

            // Price return + funding P&L over [t0, t1] on the currently-held weights.
            let mut gross = 0.0_f64;
            let mut funding_pnl = 0.0_f64;
            for (pool, w) in &held {
                if let Some(cmap) = prep.market.closes.get(pool)
                    && let (Some(&c0), Some(&c1)) = (cmap.get(&t0), cmap.get(&t1))
                    && c0 > 0.0
                {
                    gross += *w * (c1 / c0 - 1.0);
                }
                let f = prep
                    .market
                    .funding_daily
                    .get(pool)
                    .and_then(|m| m.get(&t0))
                    .copied()
                    .unwrap_or(0.0);
                funding_pnl -= *w * f;
            }

            let cost_drag = if equity > 0.0 { total_cost / equity } else { 0.0 };
            let net = gross + funding_pnl - cost_drag;
            // A real account can't go below zero — ruin floors equity at 0.
            equity = (equity + equity * net).max(0.0);
            net_returns.push(net);
            result.equity_curve.push((ms_to_dt(t1)?, to_dec(equity)));
        }
        let held = held.into_iter().map(|(k, v)| (k, to_dec(v))).collect();
        Ok((result, held))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use cw_core::{Instrument, Network, PoolId};
    use cw_features::FeatureRow;
    use cw_strategy::CrossSectional;
    use std::collections::BTreeMap;

    fn inst(name: &str) -> Instrument {
        Instrument {
            network: Network::Base,
            pool: PoolId(name.into()),
            name: name.into(),
            token: None,
        }
    }

    // Flat prices (gross return = 0) so final equity reflects *costs only*. Momentum
    // flips each day, so the strategy rotates A<->B and trades every step.
    fn scenario(volume: i64) -> (Vec<(Instrument, Vec<Bar>)>, FeatureFrame) {
        let (a, b) = (inst("A"), inst("B"));
        let day = |d: i64| Utc.timestamp_opt(1_700_000_000 + d * 86_400, 0).unwrap();
        let bar = |d: i64| Bar {
            ts: day(d),
            open: Decimal::from(100),
            high: Decimal::from(100),
            low: Decimal::from(100),
            close: Decimal::from(100),
            volume: Decimal::from(volume),
        };
        let bars = vec![
            (a.clone(), (0..4).map(bar).collect()),
            (b.clone(), (0..4).map(bar).collect()),
        ];
        let mut rows = Vec::new();
        for d in 0..4 {
            let (ma, mb) = if d % 2 == 0 { (1, 0) } else { (0, 1) };
            rows.push(FeatureRow {
                instrument: a.clone(),
                ts: day(d),
                values: BTreeMap::from([("mom".to_string(), Decimal::from(ma))]),
            });
            rows.push(FeatureRow {
                instrument: b.clone(),
                ts: day(d),
                values: BTreeMap::from([("mom".to_string(), Decimal::from(mb))]),
            });
        }
        (bars, FeatureFrame { rows })
    }

    fn final_equity(volume: i64) -> Decimal {
        let (bars, frame) = scenario(volume);
        let strat = CrossSectional {
            rank_feature: "mom".to_string(),
            top_quantile: 0.5,
            allow_short: false,
            invert: false,
            weight_feature: None,
        };
        let bt = Backtester::new(
            CostModel {
                fee_frac: Decimal::new(5, 4),
                slippage_coeff: Decimal::new(5, 1),
                gas_usd: Decimal::ZERO,
            },
            WalkForward {
                train_bars: 1,
                test_bars: 1,
                embargo_bars: 0,
            },
        );
        bt.run(&strat, &bars, &frame, &[]).unwrap().equity_curve.last().unwrap().1
    }

    #[test]
    fn thinner_pool_incurs_more_slippage() {
        // Identical scenario except pool volume: the thin pool must end poorer.
        let deep = final_equity(1_000_000);
        let thin = final_equity(1_000);
        assert!(thin < deep, "thin {thin} should trail deep {deep}");
        assert!(deep < Decimal::from(INITIAL_CAPITAL_USD), "costs must reduce equity");
    }

    #[test]
    fn higher_cadence_cuts_trades_and_cost() {
        // Momentum flips daily, so daily rebalancing rotates A<->B every step; a
        // long cadence rebalances once and holds, trading (and paying) far less.
        let (bars, frame) = scenario(1_000_000);
        let strat = CrossSectional {
            rank_feature: "mom".to_string(),
            top_quantile: 0.5,
            allow_short: false,
            invert: false,
            weight_feature: None,
        };
        let costs = CostModel {
            fee_frac: Decimal::new(5, 4),
            slippage_coeff: Decimal::new(5, 1),
            gas_usd: Decimal::ZERO,
        };
        let wf = WalkForward {
            train_bars: 1,
            test_bars: 1,
            embargo_bars: 0,
        };
        let daily = Backtester::new(costs.clone(), wf).run(&strat, &bars, &frame, &[]).unwrap();
        let rare = Backtester::new(costs, wf)
            .with_rebalance_every(10)
            .run(&strat, &bars, &frame, &[])
            .unwrap();
        assert!(
            rare.trades.len() < daily.trades.len(),
            "rare {} < daily {}",
            rare.trades.len(),
            daily.trades.len()
        );
        // Flat prices => equity reflects costs only; fewer trades ends richer.
        let (de, re) = (daily.equity_curve.last().unwrap().1, rare.equity_curve.last().unwrap().1);
        assert!(re > de, "rare {re} should beat daily {de} on cost savings");
    }

    #[test]
    fn carried_positions_avoid_reentry_cost() {
        use std::collections::HashMap;
        // Seeding a segment with its own opening positions skips the cash-entry cost
        // (the walk-forward carry that keeps OOS returns from being deflated).
        let (bars, frame) = scenario(1_000);
        let strat = CrossSectional {
            rank_feature: "mom".to_string(),
            top_quantile: 0.5,
            allow_short: false,
            invert: false,
            weight_feature: None,
        };
        let costs = CostModel {
            fee_frac: Decimal::new(5, 4),
            slippage_coeff: Decimal::new(5, 1),
            gas_usd: Decimal::ZERO,
        };
        let bt = Backtester::new(
            costs,
            WalkForward {
                train_bars: 1,
                test_bars: 1,
                embargo_bars: 0,
            },
        );
        let t0 = frame.rows.iter().map(|r| r.ts).min().unwrap();
        let cs: Vec<FeatureRow> = frame.rows.iter().filter(|r| r.ts == t0).cloned().collect();
        let opening: HashMap<String, Decimal> = strat
            .target_weights(&cs)
            .unwrap()
            .into_iter()
            .map(|tw| (tw.instrument.pool.0, tw.weight))
            .collect();
        let cash = bt.run(&strat, &bars, &frame, &[]).unwrap();
        let (seeded, _end) = bt.run_stateful(&strat, &bars, &frame, &[], &opening).unwrap();
        let (c, s) = (cash.equity_curve.last().unwrap().1, seeded.equity_curve.last().unwrap().1);
        assert!(s > c, "seeded {s} should beat cash-start {c} by the saved entry cost");
    }
}
