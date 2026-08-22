//! `cw-backtest` — the point-in-time backtest engine.
//!
//! Two invariants define this crate's honesty:
//! 1. **No lookahead** — a signal at bar `t` may only use data through `t`.
//! 2. **Honest costs** — every simulated fill pays fees, gas, and slippage
//!    modeled against the pool's liquidity. A cost-free backtest is a lie.
//!
//! Overfitting defense (walk-forward + purged/embargoed CV) is a first-class
//! input, not an afterthought.

use chrono::{DateTime, Utc};
use cw_core::{Decimal, Result};
use cw_features::FeatureFrame;
use cw_strategy::Strategy;

/// Transaction-cost parameters applied to every simulated fill.
#[derive(Debug, Clone)]
pub struct CostModel {
    /// DEX swap fee as a fraction (e.g. 0.0005 for a 5 bps pool).
    pub fee_frac: Decimal,
    /// Flat gas cost per trade, in quote currency (USD).
    pub gas_usd: Decimal,
    /// Slippage coefficient applied to trade-size / pool-liquidity ratio.
    pub slippage_coeff: Decimal,
}

/// Walk-forward split: expanding/rolling train window, out-of-sample test window,
/// with an embargo gap to prevent leakage across the boundary.
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

/// Drives a [`Strategy`] over historical [`FeatureFrame`]s under a [`CostModel`].
pub struct Backtester {
    pub costs: CostModel,
    pub walk_forward: WalkForward,
}

impl Backtester {
    pub fn new(costs: CostModel, walk_forward: WalkForward) -> Self {
        Self { costs, walk_forward }
    }

    /// Run the strategy over the supplied per-rebalance feature frames.
    pub fn run<S: Strategy>(&self, _strategy: &S, _frames: &[FeatureFrame]) -> Result<BacktestResult> {
        Err(cw_core::CoreError::Unimplemented("Backtester::run"))
    }
}
