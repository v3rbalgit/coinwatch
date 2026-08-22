//! `cw-strategy` — the [`Strategy`] seam.
//!
//! A strategy maps the **cross-section of features at one rebalance timestamp**
//! (one [`FeatureRow`] per instrument) to target portfolio weights. The *same*
//! implementation is driven by the backtester and (later) the live runner — that
//! single source of truth is what lets a backtested edge be trusted in deployment.

use cw_core::{Decimal, Result, TargetWeight};
use cw_features::FeatureRow;

/// Maps a rebalance-point cross-section to desired positions.
pub trait Strategy {
    fn name(&self) -> &str;

    /// Produce target weights from the cross-section of features at one instant.
    /// Implementations must be pure (no hidden state, no lookahead).
    fn target_weights(&self, cross_section: &[FeatureRow]) -> Result<Vec<TargetWeight>>;
}

/// Long-top / (optionally) short-bottom cross-sectional strategy over a ranked
/// feature. This is the Phase-1 strategy that exercises portfolio construction,
/// costs, and survivorship in the backtester.
///
/// Long-only: the top leg is equal-weighted to a gross of 1.0. Long-short:
/// top and bottom legs are equal-weighted to +0.5 / -0.5 (gross 1.0, net 0.0).
#[derive(Debug, Clone)]
pub struct CrossSectional {
    /// Feature column to rank on (must be produced by an active module).
    pub rank_feature: String,
    /// Fraction of the universe taken as each leg (0.0–1.0).
    pub top_quantile: f64,
    /// If false, the bottom leg is held flat instead of shorted.
    pub allow_short: bool,
    /// If true, rank ascending — long the LOW end — for signals whose edge is
    /// negative (e.g. the low-volatility anomaly: long low vol, short high vol).
    pub invert: bool,
    /// Optional risk feature to inverse-weight positions within each leg (e.g.
    /// `"vol"`): weight ∝ 1/risk, normalized to the leg's gross. `None` = equal
    /// weight. Inverse-vol sizing caps the fat-tail blowup from volatile names.
    pub weight_feature: Option<String>,
}

impl Strategy for CrossSectional {
    fn name(&self) -> &str {
        "cross_sectional"
    }

    fn target_weights(&self, cross_section: &[FeatureRow]) -> Result<Vec<TargetWeight>> {
        // (row, rank value) pairs for instruments that have the ranked feature.
        let mut ranked: Vec<(&FeatureRow, Decimal)> = cross_section
            .iter()
            .filter_map(|r| r.values.get(&self.rank_feature).map(|v| (r, *v)))
            .collect();
        // No instrument carries the rank feature this period (e.g. a sparse signal
        // like funding before its history exists) → hold cash rather than error.
        if ranked.is_empty() {
            return Ok(Vec::new());
        }
        if self.invert {
            ranked.sort_by(|a, b| a.1.cmp(&b.1)); // ascending: long the low end
        } else {
            ranked.sort_by(|a, b| b.1.cmp(&a.1)); // descending: long the high end
        }

        let n = ranked.len();
        let leg = (((n as f64) * self.top_quantile).floor() as usize).clamp(1, n);
        let short = self.allow_short && n > leg;
        // Long-only → gross 1.0 on the top leg; long-short → +0.5 / -0.5.
        let long_total = if short { Decimal::new(5, 1) } else { Decimal::ONE };
        let wf = self.weight_feature.as_deref();

        let long_rows: Vec<&FeatureRow> = ranked.iter().take(leg).map(|(r, _)| *r).collect();
        let mut weights = allocate(&long_rows, long_total, wf);
        if short {
            let short_rows: Vec<&FeatureRow> = ranked.iter().rev().take(leg).map(|(r, _)| *r).collect();
            weights.extend(allocate(&short_rows, -Decimal::new(5, 1), wf));
        }
        Ok(weights)
    }
}

/// Distribute `gross` exposure across `rows`: equal-weight, or (when
/// `weight_feature` is set) inversely to that risk feature (∝ 1/risk), so
/// volatile names get smaller positions. The leg always sums to `gross`.
fn allocate(rows: &[&FeatureRow], gross: Decimal, weight_feature: Option<&str>) -> Vec<TargetWeight> {
    let eps = Decimal::new(1, 9);
    let raws: Vec<Decimal> = rows
        .iter()
        .map(|r| match weight_feature.and_then(|wf| r.values.get(wf)) {
            Some(risk) if risk.abs() > eps => Decimal::ONE / risk.abs(),
            _ => Decimal::ONE,
        })
        .collect();
    let total: Decimal = raws.iter().copied().sum();
    let n = Decimal::from(rows.len().max(1) as u64);
    rows.iter()
        .zip(&raws)
        .map(|(r, raw)| {
            let frac = if total > Decimal::ZERO { *raw / total } else { Decimal::ONE / n };
            TargetWeight { instrument: r.instrument.clone(), weight: gross * frac }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use cw_core::{Instrument, Network, PoolId};
    use std::collections::BTreeMap;

    fn row(name: &str, mom: i64) -> FeatureRow {
        FeatureRow {
            instrument: Instrument {
                network: Network::Base,
                pool: PoolId(name.to_string()),
                name: name.to_string(),
                token: None,
            },
            ts: Utc::now(),
            values: BTreeMap::from([("mom".to_string(), Decimal::from(mom))]),
        }
    }

    #[test]
    fn long_only_picks_top_and_sums_to_one() {
        let s = CrossSectional {
            rank_feature: "mom".to_string(),
            top_quantile: 0.5,
            allow_short: false,
            invert: false,
            weight_feature: None,
        };
        let cross = vec![row("a", 5), row("b", 1), row("c", 9), row("d", -3)];
        let w = s.target_weights(&cross).unwrap();
        assert_eq!(w.len(), 2); // floor(4 * 0.5) = 2 longs
        let total: Decimal = w.iter().map(|t| t.weight).sum();
        assert_eq!(total, Decimal::ONE);
        // Top-ranked "c" (mom 9) must be selected.
        assert!(w.iter().any(|t| t.instrument.name == "c"));
        assert!(w.iter().all(|t| t.weight > Decimal::ZERO));
    }

    #[test]
    fn long_short_is_dollar_neutral() {
        let s = CrossSectional {
            rank_feature: "mom".to_string(),
            top_quantile: 0.25,
            allow_short: true,
            invert: false,
            weight_feature: None,
        };
        let cross = vec![row("a", 5), row("b", 1), row("c", 9), row("d", -3)];
        let w = s.target_weights(&cross).unwrap();
        let net: Decimal = w.iter().map(|t| t.weight).sum();
        assert_eq!(net, Decimal::ZERO);
        let gross: Decimal = w.iter().map(|t| t.weight.abs()).sum();
        assert_eq!(gross, Decimal::ONE);
    }

    #[test]
    fn missing_feature_yields_no_positions() {
        let s = CrossSectional {
            rank_feature: "nope".to_string(),
            top_quantile: 0.5,
            allow_short: false,
            invert: false,
            weight_feature: None,
        };
        assert!(s.target_weights(&[row("a", 1)]).unwrap().is_empty());
    }

    #[test]
    fn inverse_vol_weighting_sizes_down_volatile_names() {
        let mut a = row("a", 2);
        a.values.insert("vol".to_string(), Decimal::new(2, 2)); // 0.02
        let mut b = row("b", 1);
        b.values.insert("vol".to_string(), Decimal::new(8, 2)); // 0.08
        let s = CrossSectional {
            rank_feature: "mom".to_string(),
            top_quantile: 1.0,
            allow_short: false,
            invert: false,
            weight_feature: Some("vol".to_string()),
        };
        let w = s.target_weights(&[a, b]).unwrap();
        let wa = w.iter().find(|t| t.instrument.name == "a").unwrap().weight;
        let wb = w.iter().find(|t| t.instrument.name == "b").unwrap().weight;
        assert!(wa > wb, "low-vol name should get more weight");
        assert_eq!((wa / wb).round_dp(0), Decimal::from(4)); // 1/0.02 : 1/0.08 = 4:1
        assert_eq!(wa + wb, Decimal::ONE); // long-only sums to gross 1
    }
}
