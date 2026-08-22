//! Liquidity / microstructure feature module (Phase-2 modularity proof).
//!
//! A *second, independent* [`FeatureModule`] that plugs into the unchanged engine.
//! It derives features from the `volume` (USD) already carried on each bar — no
//! new data source required:
//! - `dvol_mom` — dollar-volume momentum: recent avg volume vs. older avg,
//! - `illiq` — Amihud illiquidity: mean(|return| / dollar-volume), scaled for
//!   readability (a cited liquidity factor; higher = thinner market),
//! - `dvol_z` — z-score of the latest dollar volume vs. its recent window.
//!
//! Same contract as `technical`: f64 math, stored as [`Decimal`], warmup-gated.

use std::collections::BTreeMap;

use cw_core::{Decimal, Result};
use rust_decimal::prelude::ToPrimitive;

use crate::{FeatureContext, FeatureFrame, FeatureModule, FeatureRow};

const FEATURES: [&str; 3] = ["dvol_mom", "illiq", "dvol_z"];
/// Amihud values are tiny; scale so ranked/printed numbers are legible.
const ILLIQ_SCALE: f64 = 1e6;

/// Configurable liquidity feature set. Lookbacks are in bars.
#[derive(Debug, Clone)]
pub struct LiquidityFeatures {
    pub window: usize,
    pub mom_short: usize,
    pub mom_long: usize,
}

impl Default for LiquidityFeatures {
    fn default() -> Self {
        Self {
            window: 14,
            mom_short: 7,
            mom_long: 30,
        }
    }
}

impl LiquidityFeatures {
    fn warmup(&self) -> usize {
        self.window.max(self.mom_long)
    }
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn dec(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).unwrap_or_default()
}

/// Dollar-volume momentum: recent mean volume / older mean volume − 1.
fn dvol_momentum(dvol: &[f64], i: usize, short: usize, long: usize) -> Option<f64> {
    let recent = mean(&dvol[i - short + 1..=i]);
    let older = mean(&dvol[i - long + 1..=i]);
    if older <= 0.0 {
        return None;
    }
    Some(recent / older - 1.0)
}

/// Amihud illiquidity over `w` bars ending at `i` (scaled).
fn amihud(closes: &[f64], dvol: &[f64], i: usize, w: usize) -> Option<f64> {
    let mut acc = 0.0;
    let mut count = 0.0;
    for t in (i - w + 1)..=i {
        let (p0, p1, v) = (closes[t - 1], closes[t], dvol[t]);
        if p0 <= 0.0 || v <= 0.0 {
            continue;
        }
        acc += (p1 / p0 - 1.0).abs() / v;
        count += 1.0;
    }
    if count == 0.0 {
        return None;
    }
    Some(acc / count * ILLIQ_SCALE)
}

/// Z-score of the latest dollar volume vs. its `w`-bar window.
fn dvol_zscore(dvol: &[f64], i: usize, w: usize) -> Option<f64> {
    let window = &dvol[i - w + 1..=i];
    let m = mean(window);
    let var = window.iter().map(|v| (v - m).powi(2)).sum::<f64>() / window.len() as f64;
    let sd = var.sqrt();
    if sd <= 0.0 {
        return None;
    }
    Some((dvol[i] - m) / sd)
}

impl FeatureModule for LiquidityFeatures {
    fn name(&self) -> &str {
        "liquidity"
    }

    fn feature_names(&self) -> &[&str] {
        &FEATURES
    }

    fn compute(&self, ctx: &FeatureContext<'_>) -> Result<FeatureFrame> {
        let warm = self.warmup();
        let mut rows = Vec::new();

        for (instrument, bars) in ctx.bars {
            if bars.len() <= warm {
                continue;
            }
            let closes: Vec<f64> = bars.iter().map(|b| b.close.to_f64().unwrap_or(0.0)).collect();
            let dvol: Vec<f64> = bars.iter().map(|b| b.volume.to_f64().unwrap_or(0.0)).collect();

            for (i, bar) in bars.iter().enumerate().skip(warm) {
                let (Some(m), Some(il), Some(z)) = (
                    dvol_momentum(&dvol, i, self.mom_short, self.mom_long),
                    amihud(&closes, &dvol, i, self.window),
                    dvol_zscore(&dvol, i, self.window),
                ) else {
                    continue;
                };
                if !(m.is_finite() && il.is_finite() && z.is_finite()) {
                    continue;
                }
                rows.push(FeatureRow {
                    instrument: (*instrument).clone(),
                    ts: bar.ts,
                    values: BTreeMap::from([
                        ("dvol_mom".to_string(), dec(m)),
                        ("illiq".to_string(), dec(il)),
                        ("dvol_z".to_string(), dec(z)),
                    ]),
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

    fn bar(day: u32, close: f64, volume: f64) -> Bar {
        let c = Decimal::from_f64(close).unwrap();
        Bar {
            ts: ts(day),
            open: c,
            high: c,
            low: c,
            close: c,
            volume: Decimal::from_f64(volume).unwrap(),
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

    #[test]
    fn rising_volume_has_positive_dvol_momentum() {
        // Flat price, steadily rising volume → recent avg > older avg.
        let bars: Vec<Bar> = (1..=20).map(|d| bar(d, 100.0, 1000.0 + d as f64 * 100.0)).collect();
        let module = LiquidityFeatures {
            window: 5,
            mom_short: 3,
            mom_long: 10,
        };
        let dataset = vec![(instrument(), bars)];
        let frame = module.compute(&FeatureContext { bars: &dataset, funding: &[] }).unwrap();
        assert!(!frame.rows.is_empty());
        assert!(frame.rows.last().unwrap().values["dvol_mom"] > Decimal::ZERO);
    }

    #[test]
    fn produces_all_declared_features() {
        // Volume must vary or the z-score is undefined (σ=0) and rows are dropped.
        let bars: Vec<Bar> = (1..=25).map(|d| bar(d, 100.0 + d as f64, 5000.0 + d as f64 * 100.0)).collect();
        let module = LiquidityFeatures {
            window: 5,
            mom_short: 3,
            mom_long: 10,
        };
        let dataset = vec![(instrument(), bars)];
        let frame = module.compute(&FeatureContext { bars: &dataset, funding: &[] }).unwrap();
        let row = frame.rows.last().unwrap();
        for name in FEATURES {
            assert!(row.values.contains_key(name), "missing feature {name}");
        }
    }
}
