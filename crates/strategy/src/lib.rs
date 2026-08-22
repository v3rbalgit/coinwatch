//! `cw-strategy` — the [`Strategy`] seam.
//!
//! A strategy maps the **cross-section of features at one rebalance timestamp**
//! (one [`FeatureRow`] per instrument) to target portfolio weights. The *same*
//! implementation is driven by the backtester and (later) the live runner — that
//! single source of truth is what lets a backtested edge be trusted in deployment.

use cw_core::{CoreError, Decimal, Result, TargetWeight};
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
        if ranked.is_empty() {
            return Err(CoreError::Config(format!(
                "rank feature '{}' absent from cross-section",
                self.rank_feature
            )));
        }
        ranked.sort_by(|a, b| b.1.cmp(&a.1)); // descending

        let n = ranked.len();
        let leg = (((n as f64) * self.top_quantile).floor() as usize).clamp(1, n);
        let short = self.allow_short && n > leg;
        // Long-only → gross 1.0 on the top leg; long-short → +0.5 / -0.5.
        let long_total = if short { Decimal::new(5, 1) } else { Decimal::ONE };
        let long_w = long_total / Decimal::from(leg as u64);

        let mut weights = Vec::with_capacity(if short { leg * 2 } else { leg });
        for (row, _) in ranked.iter().take(leg) {
            weights.push(TargetWeight {
                instrument: row.instrument.clone(),
                weight: long_w,
            });
        }
        if short {
            let short_w = -(Decimal::new(5, 1) / Decimal::from(leg as u64));
            for (row, _) in ranked.iter().rev().take(leg) {
                weights.push(TargetWeight {
                    instrument: row.instrument.clone(),
                    weight: short_w,
                });
            }
        }
        Ok(weights)
    }
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
        };
        let cross = vec![row("a", 5), row("b", 1), row("c", 9), row("d", -3)];
        let w = s.target_weights(&cross).unwrap();
        let net: Decimal = w.iter().map(|t| t.weight).sum();
        assert_eq!(net, Decimal::ZERO);
        let gross: Decimal = w.iter().map(|t| t.weight.abs()).sum();
        assert_eq!(gross, Decimal::ONE);
    }

    #[test]
    fn missing_feature_errors() {
        let s = CrossSectional {
            rank_feature: "nope".to_string(),
            top_quantile: 0.5,
            allow_short: false,
        };
        assert!(s.target_weights(&[row("a", 1)]).is_err());
    }
}
