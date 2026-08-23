//! Technical-analysis feature module (Phase-1 first module).
//!
//! Contributes per-instrument time-series features over OHLCV bars:
//! - `mom` — simple return over `momentum_lookback` bars,
//! - `vol` — population stdev of log returns over `vol_lookback` bars,
//! - `rsi` — Relative Strength Index over `rsi_lookback` bars.
//!
//! Cross-sectional ranking across instruments happens later, in the strategy
//! layer; this module only produces the raw per-instrument signal columns.
//!
//! Feature math runs in `f64` (these are ranking inputs, not money) and is stored
//! back as [`Decimal`] for a uniform frame. Rows are only emitted once every
//! feature has enough warmup history and is finite.

use std::collections::BTreeMap;

use cw_core::{Decimal, Result};
use rust_decimal::prelude::ToPrimitive;

use crate::{FeatureContext, FeatureFrame, FeatureModule, FeatureRow};

const FEATURES: [&str; 3] = ["mom", "vol", "rsi"];

/// Configurable TA feature set. Lookbacks are in bars of the source interval.
#[derive(Debug, Clone)]
pub struct TechnicalFeatures {
    /// Lookback (bars) for the momentum feature.
    pub momentum_lookback: usize,
    /// Lookback (bars) for the volatility feature.
    pub vol_lookback: usize,
    /// Lookback (bars) for RSI.
    pub rsi_lookback: usize,
}

impl Default for TechnicalFeatures {
    fn default() -> Self {
        Self {
            momentum_lookback: 30,
            vol_lookback: 14,
            rsi_lookback: 14,
        }
    }
}

impl TechnicalFeatures {
    /// Bars of history required before the first row can be emitted.
    fn warmup(&self) -> usize {
        self.momentum_lookback.max(self.vol_lookback).max(self.rsi_lookback)
    }
}

/// Simple return over `lb` bars ending at index `i`.
fn momentum(closes: &[f64], i: usize, lb: usize) -> Option<f64> {
    let prev = *closes.get(i - lb)?;
    if prev == 0.0 {
        return None;
    }
    Some(*closes.get(i)? / prev - 1.0)
}

/// Population stdev of log returns over the `lb` bars ending at index `i`.
fn volatility(closes: &[f64], i: usize, lb: usize) -> Option<f64> {
    let window = closes.get(i - lb..=i)?;
    let mut rets = Vec::with_capacity(lb);
    for (p0, p1) in window.iter().zip(window.iter().skip(1)) {
        if *p0 <= 0.0 || *p1 <= 0.0 {
            return None;
        }
        rets.push((p1 / p0).ln());
    }
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    Some(var.sqrt())
}

/// Simple (SMA-based) RSI over the `lb` deltas ending at index `i`.
fn rsi(closes: &[f64], i: usize, lb: usize) -> Option<f64> {
    let window = closes.get(i - lb..=i)?;
    let (mut gain, mut loss) = (0.0, 0.0);
    for (p0, p1) in window.iter().zip(window.iter().skip(1)) {
        let d = p1 - p0;
        if d >= 0.0 {
            gain += d;
        } else {
            loss -= d;
        }
    }
    let n = lb as f64;
    let (avg_gain, avg_loss) = (gain / n, loss / n);
    if avg_loss == 0.0 {
        return Some(if avg_gain == 0.0 { 50.0 } else { 100.0 });
    }
    let rs = avg_gain / avg_loss;
    Some(100.0 - 100.0 / (1.0 + rs))
}

fn dec(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).unwrap_or_default()
}

impl FeatureModule for TechnicalFeatures {
    fn name(&self) -> &'static str {
        "technical"
    }

    fn feature_names(&self) -> &[&str] {
        &FEATURES
    }

    fn compute(&self, ctx: &FeatureContext<'_>) -> Result<FeatureFrame> {
        let warm = self.warmup();
        let mut rows = Vec::new();

        for (instrument, bars) in ctx.bars {
            if bars.len() <= warm {
                continue; // not enough history for even one row
            }
            let closes: Vec<f64> = bars.iter().map(|b| b.close.to_f64().unwrap_or(0.0)).collect();

            for (i, bar) in bars.iter().enumerate().skip(warm) {
                let (Some(m), Some(v), Some(r)) = (
                    momentum(&closes, i, self.momentum_lookback),
                    volatility(&closes, i, self.vol_lookback),
                    rsi(&closes, i, self.rsi_lookback),
                ) else {
                    continue;
                };
                if !(m.is_finite() && v.is_finite() && r.is_finite()) {
                    continue;
                }
                let values = BTreeMap::from([
                    ("mom".to_string(), dec(m)),
                    ("vol".to_string(), dec(v)),
                    ("rsi".to_string(), dec(r)),
                ]);
                rows.push(FeatureRow {
                    instrument: (*instrument).clone(),
                    ts: bar.ts,
                    values,
                });
            }
        }
        Ok(FeatureFrame { rows })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use cw_core::{Bar, Instrument, Network, PoolId};
    use rust_decimal::prelude::FromPrimitive;

    fn ts(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, day, 0, 0, 0).unwrap()
    }

    fn bar(day: u32, close: f64) -> Bar {
        let c = Decimal::from_f64(close).unwrap();
        Bar {
            ts: ts(day),
            open: c,
            high: c,
            low: c,
            close: c,
            volume: Decimal::from(1),
        }
    }

    fn instrument() -> Instrument {
        Instrument {
            network: Network::Base,
            pool: PoolId("0xtest".to_string()),
            name: "TEST".to_string(),
            token: None,
        }
    }

    /// A strictly rising series: momentum > 0 and RSI pinned at 100 (no down days).
    #[test]
    fn rising_series_has_positive_momentum_and_max_rsi() {
        let bars: Vec<Bar> = (1..=20).map(|d| bar(d, 100.0 + d as f64)).collect();
        let module = TechnicalFeatures {
            momentum_lookback: 5,
            vol_lookback: 5,
            rsi_lookback: 5,
        };
        let dataset = vec![(instrument(), bars)];
        let frame = module
            .compute(&FeatureContext {
                bars: &dataset,
                funding: &[],
            })
            .unwrap();

        assert!(!frame.rows.is_empty());
        let last = frame.rows.last().unwrap();
        assert!(last.values["mom"] > Decimal::ZERO, "momentum should be positive");
        assert_eq!(last.values["rsi"], dec(100.0), "RSI should be 100 with no losses");
        assert!(last.values["vol"] > Decimal::ZERO, "volatility should be non-zero");
    }

    /// Warmup gating: too few bars → no rows.
    #[test]
    fn insufficient_history_emits_no_rows() {
        let bars: Vec<Bar> = (1..=5).map(|d| bar(d, 100.0 + d as f64)).collect();
        let module = TechnicalFeatures {
            momentum_lookback: 10,
            vol_lookback: 10,
            rsi_lookback: 10,
        };
        let dataset = vec![(instrument(), bars)];
        let frame = module
            .compute(&FeatureContext {
                bars: &dataset,
                funding: &[],
            })
            .unwrap();
        assert!(frame.rows.is_empty());
    }

    /// A falling series pins RSI at 0 and momentum negative.
    #[test]
    fn falling_series_has_negative_momentum_and_min_rsi() {
        let bars: Vec<Bar> = (1..=20).map(|d| bar(d, 200.0 - d as f64)).collect();
        let module = TechnicalFeatures {
            momentum_lookback: 5,
            vol_lookback: 5,
            rsi_lookback: 5,
        };
        let dataset = vec![(instrument(), bars)];
        let frame = module
            .compute(&FeatureContext {
                bars: &dataset,
                funding: &[],
            })
            .unwrap();
        let last = frame.rows.last().unwrap();
        assert!(last.values["mom"] < Decimal::ZERO);
        assert_eq!(last.values["rsi"], dec(0.0));
    }
}
