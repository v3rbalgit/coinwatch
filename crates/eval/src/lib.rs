//! `cw-eval` — turns a [`BacktestResult`] into performance and overfitting metrics.
//!
//! Beyond risk/return stats, this crate reports the **Probabilistic Sharpe Ratio**
//! (Bailey & López de Prado): the probability that the true Sharpe exceeds zero
//! given the sample's length, skew and kurtosis — so "it backtests well" can be
//! told apart from "it found a real edge". Multi-trial deflation (DSR) plugs in
//! here once the harness sweeps parameters.

use cw_backtest::BacktestResult;
use cw_core::Result;
use rust_decimal::prelude::ToPrimitive;

/// Crypto trades every day; annualize per-period stats by √365.
const PERIODS_PER_YEAR: f64 = 365.0;

/// Headline metrics for a single backtest run.
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub sharpe: f64,
    pub sortino: f64,
    pub max_drawdown: f64,
    /// Average gross turnover per rebalance (fraction of equity traded).
    pub turnover: f64,
    pub hit_rate: f64,
    /// Probabilistic Sharpe Ratio vs. a zero benchmark (probability true SR > 0).
    pub psr: f64,
}

/// Standard-normal CDF via an Abramowitz–Stegun erf approximation (7.1.26).
fn normal_cdf(x: f64) -> f64 {
    fn erf(x: f64) -> f64 {
        let sign = if x < 0.0 { -1.0 } else { 1.0 };
        let x = x.abs();
        let t = 1.0 / (1.0 + 0.327_591_1 * x);
        let y = 1.0
            - (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t
                + 0.254_829_592)
                * t
                * (-x * x).exp();
        sign * y
    }
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Probability that the true (per-period) Sharpe exceeds 0, adjusting for the
/// sample's non-normality (skew/kurtosis) and length.
fn probabilistic_sharpe(rets: &[f64], mean: f64, sd: f64) -> f64 {
    let n = rets.len() as f64;
    if n < 3.0 || sd <= 0.0 {
        return 0.0;
    }
    let sr = mean / sd;
    let m3 = rets.iter().map(|r| (r - mean).powi(3)).sum::<f64>() / n;
    let m4 = rets.iter().map(|r| (r - mean).powi(4)).sum::<f64>() / n;
    let skew = m3 / sd.powi(3);
    let kurt = m4 / sd.powi(4); // non-excess (normal = 3)
    let denom = (1.0 - skew * sr + ((kurt - 1.0) / 4.0) * sr * sr).max(1e-12).sqrt();
    normal_cdf(sr * (n - 1.0).sqrt() / denom)
}

/// Compute metrics from a completed backtest.
pub fn evaluate(result: &BacktestResult) -> Result<Metrics> {
    let eq: Vec<f64> = result
        .equity_curve
        .iter()
        .map(|(_, e)| e.to_f64().unwrap_or(0.0))
        .collect();
    if eq.len() < 2 {
        return Ok(Metrics::default());
    }

    let rets: Vec<f64> = eq
        .windows(2)
        .filter_map(|w| if w[0] > 0.0 { Some(w[1] / w[0] - 1.0) } else { None })
        .collect();
    if rets.is_empty() {
        return Ok(Metrics::default());
    }
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    let sd = var.sqrt();
    let ann = PERIODS_PER_YEAR.sqrt();

    let sharpe = if sd > 0.0 { mean / sd * ann } else { 0.0 };

    let dvar = rets.iter().map(|r| if *r < 0.0 { r * r } else { 0.0 }).sum::<f64>() / n;
    let dsd = dvar.sqrt();
    let sortino = if dsd > 0.0 { mean / dsd * ann } else { 0.0 };

    let mut peak = eq[0];
    let mut max_drawdown: f64 = 0.0;
    for &e in &eq {
        peak = peak.max(e);
        if peak > 0.0 {
            max_drawdown = max_drawdown.max((peak - e) / peak);
        }
    }

    let hit_rate = rets.iter().filter(|r| **r > 0.0).count() as f64 / n;

    // Average gross turnover: traded notional / (mean equity × rebalances).
    let gross_traded: f64 = result.trades.iter().map(|t| t.qty_usd.to_f64().unwrap_or(0.0).abs()).sum();
    let mean_eq = eq.iter().sum::<f64>() / eq.len() as f64;
    let turnover = if mean_eq > 0.0 { gross_traded / (mean_eq * n) } else { 0.0 };

    let psr = probabilistic_sharpe(&rets, mean, sd);

    Ok(Metrics {
        sharpe,
        sortino,
        max_drawdown,
        turnover,
        hit_rate,
        psr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use cw_core::Decimal;

    fn result_from(equity: &[f64]) -> BacktestResult {
        BacktestResult {
            equity_curve: equity
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    (
                        Utc.timestamp_opt(1_700_000_000 + i as i64 * 86_400, 0).unwrap(),
                        Decimal::from_f64_retain(*e).unwrap(),
                    )
                })
                .collect(),
            trades: Vec::new(),
        }
    }

    #[test]
    fn steady_growth_has_positive_sharpe_and_no_drawdown() {
        let eq: Vec<f64> = (0..40).map(|i| 10_000.0 * 1.01_f64.powi(i)).collect();
        let m = evaluate(&result_from(&eq)).unwrap();
        assert!(m.sharpe > 0.0);
        assert!(m.max_drawdown < 1e-9);
        assert!((m.hit_rate - 1.0).abs() < 1e-9);
        assert!(m.psr > 0.9);
    }

    #[test]
    fn drawdown_is_measured() {
        let m = evaluate(&result_from(&[10_000.0, 12_000.0, 6_000.0, 9_000.0])).unwrap();
        assert!((m.max_drawdown - 0.5).abs() < 1e-9); // 12k -> 6k = 50%
    }
}
