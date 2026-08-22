//! On-chain flow feature module.
//!
//! Consumes per-day on-chain DEX aggregates ([`OnchainDaily`]) and emits features
//! that OHLCV *cannot* express — the differentiated-signal direction:
//! - `oc_flow`   — real DEX USD volume from decoded swaps (not pool-reported),
//! - `oc_buyers` — unique buyer addresses per day (organic demand proxy),
//! - `oc_tpb`    — trades per unique buyer (high = bot-churned / wash-like, low = organic).
//!
//! These are per-day level features (no warmup), so they're available as soon as
//! any on-chain history exists. Free-tier accumulation is shallow (~4 days) today;
//! the same module densifies as history grows (forward-accumulation or a paid backfill).

use std::collections::BTreeMap;

use cw_core::{Decimal, Result};

use crate::{FeatureContext, FeatureFrame, FeatureModule, FeatureRow};

const FEATURES: [&str; 3] = ["oc_flow", "oc_buyers", "oc_tpb"];

/// On-chain flow features. Stateless — all signal comes from the daily aggregates.
#[derive(Debug, Clone, Default)]
pub struct OnchainFeatures;

fn dec(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).unwrap_or_default()
}

impl FeatureModule for OnchainFeatures {
    fn name(&self) -> &str {
        "onchain"
    }

    fn feature_names(&self) -> &[&str] {
        &FEATURES
    }

    fn compute(&self, ctx: &FeatureContext<'_>) -> Result<FeatureFrame> {
        let mut rows = Vec::new();
        for (instrument, series) in ctx.onchain {
            for day in series {
                if day.unique_buyers <= 0 {
                    continue; // no organic activity → skip (also avoids div-by-zero)
                }
                let tpb = day.trades as f64 / day.unique_buyers as f64;
                rows.push(FeatureRow {
                    instrument: (*instrument).clone(),
                    ts: day.date,
                    values: BTreeMap::from([
                        ("oc_flow".to_string(), day.volume_usd),
                        ("oc_buyers".to_string(), Decimal::from(day.unique_buyers)),
                        ("oc_tpb".to_string(), dec(tpb)),
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
    use chrono::{TimeZone, Utc};
    use cw_core::{Instrument, Network, OnchainDaily, PoolId};

    #[test]
    fn computes_flow_buyers_and_trades_per_buyer() {
        let inst = Instrument {
            network: Network::Base,
            pool: PoolId("0xpool".to_string()),
            name: "TEST".to_string(),
            token: Some("0xtoken".to_string()),
        };
        let series = vec![
            OnchainDaily {
                date: Utc.with_ymd_and_hms(2026, 8, 20, 0, 0, 0).unwrap(),
                trades: 1000,
                volume_usd: Decimal::from(500_000),
                unique_buyers: 250,
            },
            OnchainDaily {
                date: Utc.with_ymd_and_hms(2026, 8, 21, 0, 0, 0).unwrap(),
                trades: 0,
                volume_usd: Decimal::ZERO,
                unique_buyers: 0, // skipped
            },
        ];
        let onchain = vec![(inst, series)];
        let ctx = FeatureContext { bars: &[], onchain: &onchain };
        let frame = OnchainFeatures.compute(&ctx).unwrap();

        assert_eq!(frame.rows.len(), 1); // the zero-buyer day is skipped
        let row = &frame.rows[0];
        assert_eq!(row.values["oc_buyers"], Decimal::from(250));
        assert_eq!(row.values["oc_flow"], Decimal::from(500_000));
        assert_eq!(row.values["oc_tpb"], dec(4.0)); // 1000 / 250
    }
}
