//! `cw-eval` — turns a [`BacktestResult`] into performance and overfitting metrics.
//!
//! Beyond risk/return stats it reports two multiple-testing-aware numbers:
//! - **PSR** (Probabilistic Sharpe Ratio): probability the true Sharpe > 0 given
//!   the sample's length, skew and kurtosis.
//! - **DSR** (Deflated Sharpe Ratio, Bailey & López de Prado): PSR against a
//!   benchmark equal to the *expected maximum* Sharpe of `N` zero-skill trials —
//!   the honest antidote to "we tried many configs and picked the best".

use cw_backtest::BacktestResult;
use cw_core::Result;
use rust_decimal::prelude::ToPrimitive;

/// Crypto trades every day; annualize per-period stats by √365.
const PERIODS_PER_YEAR: f64 = 365.0;
/// Euler–Mascheroni constant, used in the expected-maximum-Sharpe benchmark.
const EULER_MASCHERONI: f64 = 0.577_215_664_901_532_9;

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

/// Per-step simple returns implied by a backtest's equity curve.
pub fn returns(result: &BacktestResult) -> Vec<f64> {
    let eq: Vec<f64> = result.equity_curve.iter().map(|(_, e)| e.to_f64().unwrap_or(0.0)).collect();
    eq.windows(2)
        .filter_map(|w| if w[0] > 0.0 { Some(w[1] / w[0] - 1.0) } else { None })
        .collect()
}

/// Non-annualized (per-period) Sharpe of a return series.
pub fn per_period_sharpe(rets: &[f64]) -> f64 {
    if rets.len() < 2 {
        return 0.0;
    }
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let sd = (rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n).sqrt();
    if sd > 0.0 {
        mean / sd
    } else {
        0.0
    }
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

/// Inverse standard-normal CDF (probit) via Acklam's rational approximation.
fn probit(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1, 2.209_460_984_245_205e2, -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2, -3.066_479_806_614_716e1, 2.506_628_277_459_239e0,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1, 1.615_858_368_580_409e2, -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1, -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3, -3.223_964_580_411_365e-1, -2.400_758_277_161_838e0,
        -2.549_732_539_343_734e0, 4.374_664_141_464_968e0, 2.938_163_982_698_783e0,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3, 3.224_671_290_700_398e-1, 2.445_134_137_142_996e0,
        3.754_408_661_907_416e0,
    ];
    const P_LOW: f64 = 0.02425;
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    if p < P_LOW {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= 1.0 - P_LOW {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

/// Moments (mean, sd, skew, kurtosis) of a return series; kurtosis is non-excess.
fn moments(rets: &[f64]) -> (f64, f64, f64, f64) {
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let sd = (rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n).sqrt();
    if sd <= 0.0 {
        return (mean, 0.0, 0.0, 3.0);
    }
    let skew = rets.iter().map(|r| ((r - mean) / sd).powi(3)).sum::<f64>() / n;
    let kurt = rets.iter().map(|r| ((r - mean) / sd).powi(4)).sum::<f64>() / n;
    (mean, sd, skew, kurt)
}

/// Probabilistic Sharpe Ratio vs. a benchmark per-period Sharpe `sr_benchmark`.
fn psr_vs(rets: &[f64], sr_benchmark: f64) -> f64 {
    let n = rets.len();
    if n < 3 {
        return 0.0;
    }
    let (mean, sd, skew, kurt) = moments(rets);
    if sd <= 0.0 {
        return 0.0;
    }
    let sr = mean / sd;
    let denom = (1.0 - skew * sr + ((kurt - 1.0) / 4.0) * sr * sr).max(1e-12).sqrt();
    normal_cdf((sr - sr_benchmark) * (n as f64 - 1.0).sqrt() / denom)
}

/// Deflated Sharpe Ratio: PSR against the expected maximum Sharpe of `N` zero-skill
/// trials, where `N` and the trial-Sharpe variance come from `trial_sharpes`
/// (the per-period Sharpes observed while sweeping configurations).
pub fn deflated_sharpe(rets: &[f64], trial_sharpes: &[f64]) -> f64 {
    let big_n = trial_sharpes.len();
    if big_n < 2 {
        return psr_vs(rets, 0.0); // no multiplicity to deflate for
    }
    let tm = trial_sharpes.iter().sum::<f64>() / big_n as f64;
    let var_trials = trial_sharpes.iter().map(|s| (s - tm).powi(2)).sum::<f64>() / big_n as f64;
    if var_trials <= 0.0 {
        return psr_vs(rets, 0.0);
    }
    let n = big_n as f64;
    // Expected maximum of N i.i.d. standard-normal draws (Gumbel approximation).
    let expected_max = (1.0 - EULER_MASCHERONI) * probit(1.0 - 1.0 / n)
        + EULER_MASCHERONI * probit(1.0 - 1.0 / (n * std::f64::consts::E));
    let sr_star = var_trials.sqrt() * expected_max;
    psr_vs(rets, sr_star)
}

/// Compute metrics from a completed backtest.
pub fn evaluate(result: &BacktestResult) -> Result<Metrics> {
    let rets = returns(result);
    if rets.is_empty() {
        return Ok(Metrics::default());
    }
    let n = rets.len() as f64;
    let (mean, sd, _, _) = moments(&rets);
    let ann = PERIODS_PER_YEAR.sqrt();

    let sharpe = if sd > 0.0 { mean / sd * ann } else { 0.0 };

    let dsd = (rets.iter().map(|r| if *r < 0.0 { r * r } else { 0.0 }).sum::<f64>() / n).sqrt();
    let sortino = if dsd > 0.0 { mean / dsd * ann } else { 0.0 };

    let eq: Vec<f64> = result.equity_curve.iter().map(|(_, e)| e.to_f64().unwrap_or(0.0)).collect();
    let mut peak = eq.first().copied().unwrap_or(0.0);
    let mut max_drawdown: f64 = 0.0;
    for &e in &eq {
        peak = peak.max(e);
        if peak > 0.0 {
            max_drawdown = max_drawdown.max((peak - e) / peak);
        }
    }

    let hit_rate = rets.iter().filter(|r| **r > 0.0).count() as f64 / n;

    let gross_traded: f64 = result.trades.iter().map(|t| t.qty_usd.to_f64().unwrap_or(0.0).abs()).sum();
    let mean_eq = if eq.is_empty() { 0.0 } else { eq.iter().sum::<f64>() / eq.len() as f64 };
    let turnover = if mean_eq > 0.0 { gross_traded / (mean_eq * n) } else { 0.0 };

    let psr = psr_vs(&rets, 0.0);

    Ok(Metrics { sharpe, sortino, max_drawdown, turnover, hit_rate, psr })
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

    #[test]
    fn deflation_lowers_confidence_with_more_trials() {
        // Same modest edge; more (dispersed) trials => stricter benchmark => lower DSR.
        let eq: Vec<f64> = (0..60).map(|i| 10_000.0 * 1.003_f64.powi(i)).collect();
        let rets = returns(&result_from(&eq));
        let few = deflated_sharpe(&rets, &[0.05, 0.06]);
        let many = deflated_sharpe(&rets, &[0.02, 0.05, 0.1, 0.2, 0.3, 0.4]);
        assert!(many <= few);
    }

    #[test]
    fn probit_normal_cdf_roundtrip() {
        for p in [0.1, 0.25, 0.5, 0.75, 0.9] {
            assert!((normal_cdf(probit(p)) - p).abs() < 1e-3);
        }
    }
}
