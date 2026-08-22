//! `cw-eval` — turns a [`BacktestResult`] into performance and overfitting metrics.
//!
//! Beyond the usual risk/return stats, this crate is where anti-overfitting
//! diagnostics (deflated Sharpe, probability of backtest overfitting) live, so
//! "it backtests well" can be distinguished from "it found a real edge".

use cw_backtest::BacktestResult;
use cw_core::Result;

/// Headline metrics for a single backtest run.
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub sharpe: f64,
    pub sortino: f64,
    pub max_drawdown: f64,
    /// Average fraction of the book turned over per rebalance.
    pub turnover: f64,
    pub hit_rate: f64,
    /// Sharpe deflated for the number of trials — guards against selection bias.
    pub deflated_sharpe: f64,
}

/// Compute metrics from a completed backtest. `trials` is the number of
/// configurations tested (feeds the deflated-Sharpe correction).
pub fn evaluate(_result: &BacktestResult, _trials: usize) -> Result<Metrics> {
    Err(cw_core::CoreError::Unimplemented("eval::evaluate"))
}
