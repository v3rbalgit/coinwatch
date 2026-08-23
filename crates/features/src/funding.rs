//! Funding feature module — perp-native signals from hourly funding history.
//!
//! Funding is the periodic payment between longs and shorts on a perp. Positive
//! funding = longs pay shorts (crowded longs); it is both a *carry* (short
//! high-funding coins to collect it) and a *positioning* signal (extreme funding
//! often precedes mean-reversion). Per instrument, at each bar day:
//! - `fund_rate` — trailing `smooth`-day mean of daily funding (the carry level),
//! - `fund_z`    — z-score of the day's funding vs its `window`-day history.
//!
//! Emitted only where real funding covers the whole trailing window, so coins/days
//! without (deep) funding are simply absent from the cross-section rather than
//! being treated as zero funding.

use std::collections::{BTreeMap, HashMap};

use cw_core::{Decimal, FundingPoint, Result};
use rust_decimal::prelude::ToPrimitive;

use crate::{FeatureContext, FeatureFrame, FeatureModule, FeatureRow};

const FEATURES: [&str; 2] = ["fund_rate", "fund_z"];
const DAY_MS: i64 = 86_400_000;

/// Configurable funding feature set. Lookbacks are in days.
#[derive(Debug, Clone)]
pub struct FundingFeatures {
    /// Trailing window (days) for the smoothed carry level.
    pub smooth: usize,
    /// Trailing window (days) for the z-score.
    pub window: usize,
}

impl Default for FundingFeatures {
    fn default() -> Self {
        Self { smooth: 7, window: 30 }
    }
}

fn dec(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).unwrap_or_default()
}

impl FeatureModule for FundingFeatures {
    fn name(&self) -> &'static str {
        "funding"
    }

    fn feature_names(&self) -> &[&str] {
        &FEATURES
    }

    fn compute(&self, ctx: &FeatureContext<'_>) -> Result<FeatureFrame> {
        let warm = self.window.max(self.smooth);
        let by_pool: HashMap<&str, &Vec<FundingPoint>> =
            ctx.funding.iter().map(|(i, f)| (i.pool.0.as_str(), f)).collect();
        let mut rows = Vec::new();

        for (instrument, bars) in ctx.bars {
            let Some(points) = by_pool.get(instrument.pool.0.as_str()) else {
                continue;
            };
            // hourly -> daily funding (sum of the day's hourly rates)
            let mut daily: BTreeMap<i64, f64> = BTreeMap::new();
            for p in *points {
                let day = (p.time.timestamp_millis() / DAY_MS) * DAY_MS;
                *daily.entry(day).or_insert(0.0) += p.funding_rate.to_f64().unwrap_or(0.0);
            }
            if daily.is_empty() {
                continue;
            }
            // align to bar days (None where that day has no funding data)
            let series: Vec<Option<f64>> = bars
                .iter()
                .map(|b| daily.get(&((b.ts.timestamp_millis() / DAY_MS) * DAY_MS)).copied())
                .collect();

            for (i, bar) in bars.iter().enumerate().skip(warm) {
                // Require real funding across the whole trailing window; the Option
                // collect yields None if any day is missing.
                let Some(win) = series.get(i - self.window + 1..=i) else {
                    continue;
                };
                let Some(vals) = win.iter().copied().collect::<Option<Vec<f64>>>() else {
                    continue;
                };
                let (Some(&cur), Some(smooth_win)) = (vals.last(), vals.get(vals.len() - self.smooth..)) else {
                    continue;
                };
                let rate = smooth_win.iter().sum::<f64>() / smooth_win.len() as f64;
                let m = vals.iter().sum::<f64>() / vals.len() as f64;
                let sd = (vals.iter().map(|v| (v - m).powi(2)).sum::<f64>() / vals.len() as f64).sqrt();
                // Epsilon (not > 0.0): a constant series has sd ~ fp-noise, which would
                // otherwise yield z = 1/√n instead of 0. Real funding sd is >> 1e-12.
                let z = if sd > 1e-12 { (cur - m) / sd } else { 0.0 };
                if !(rate.is_finite() && z.is_finite()) {
                    continue;
                }
                rows.push(FeatureRow {
                    instrument: (*instrument).clone(),
                    ts: bar.ts,
                    values: BTreeMap::from([("fund_rate".to_string(), dec(rate)), ("fund_z".to_string(), dec(z))]),
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

    fn day(d: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(d * 86_400, 0).unwrap()
    }

    fn bar(d: i64) -> Bar {
        let c = Decimal::from(100);
        Bar {
            ts: day(d),
            open: c,
            high: c,
            low: c,
            close: c,
            volume: Decimal::from(1_000_000),
        }
    }

    fn instrument() -> Instrument {
        Instrument {
            network: Network::Other("hyperliquid".into()),
            pool: PoolId("X".into()),
            name: "X".into(),
            token: None,
        }
    }

    #[test]
    fn constant_funding_gives_flat_rate_and_zero_z() {
        // One funding point per day at a constant rate → fund_rate == that rate,
        // fund_z == 0 (no dispersion), and rows emit only after warmup.
        let bars: Vec<Bar> = (0..40).map(bar).collect();
        let funding: Vec<FundingPoint> = (0..40)
            .map(|d| FundingPoint {
                time: day(d),
                funding_rate: Decimal::from_f64(0.001).unwrap(),
                premium: Decimal::ZERO,
            })
            .collect();
        let bars_ds = vec![(instrument(), bars)];
        let fund_ds = vec![(instrument(), funding)];
        let module = FundingFeatures { smooth: 7, window: 30 };
        let frame = module
            .compute(&FeatureContext {
                bars: &bars_ds,
                funding: &fund_ds,
            })
            .unwrap();

        assert!(!frame.rows.is_empty(), "should emit rows past warmup");
        let last = frame.rows.last().unwrap();
        assert!((last.values["fund_rate"].to_f64().unwrap() - 0.001).abs() < 1e-9);
        assert!(last.values["fund_z"].to_f64().unwrap().abs() < 1e-9);
    }

    #[test]
    fn missing_funding_instrument_is_skipped() {
        let bars_ds = vec![(instrument(), (0..40).map(bar).collect())];
        let frame = FundingFeatures::default()
            .compute(&FeatureContext {
                bars: &bars_ds,
                funding: &[],
            })
            .unwrap();
        assert!(frame.rows.is_empty(), "no funding channel => no funding rows");
    }
}
