//! `cw-features` — the [`FeatureModule`] seam where edge hypotheses live.
//!
//! Each module turns canonical bars for a universe into a standard
//! `(entity × time × feature)` matrix. Modules are independent and combinable:
//! adding a `tokenomics` or `onchain_activity` module must not touch the engine.
//! This is the seam Phase 2 uses to *confirm* the architecture.

pub mod liquidity;
pub mod onchain;
pub mod technical;

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use cw_core::{Bar, Decimal, Instrument, OnchainDaily, Result};

/// Inputs available to a feature computation. Modules read only the channels they
/// need: price/OHLCV modules use `bars`; on-chain modules use `onchain`. Both are
/// per-instrument and oldest-first. Widening this is a feature-layer concern only
/// — the engine (`strategy`/`backtest`/`eval`) never sees it.
pub struct FeatureContext<'a> {
    pub bars: &'a [(Instrument, Vec<Bar>)],
    pub onchain: &'a [(Instrument, Vec<OnchainDaily>)],
}

/// One row of computed features for one instrument at one timestamp.
#[derive(Debug, Clone)]
pub struct FeatureRow {
    pub instrument: Instrument,
    pub ts: DateTime<Utc>,
    /// Named feature values (ordered for stable serialization).
    pub values: BTreeMap<String, Decimal>,
}

/// The `(entity × time × feature)` matrix a module produces.
#[derive(Debug, Default, Clone)]
pub struct FeatureFrame {
    pub rows: Vec<FeatureRow>,
}

/// A pluggable edge hypothesis. `feature_names` declares the columns this
/// module contributes so the strategy layer can bind to them by name.
pub trait FeatureModule {
    fn name(&self) -> &str;
    fn feature_names(&self) -> &[&str];
    fn compute(&self, ctx: &FeatureContext<'_>) -> Result<FeatureFrame>;
}

/// Merge several modules' frames into one, unioning feature columns per
/// `(instrument, timestamp)`. This is how independent modules become combinable
/// inputs to a single strategy — without the engine knowing how many modules ran.
pub fn combine(frames: &[FeatureFrame]) -> FeatureFrame {
    let mut merged: BTreeMap<(String, i64), FeatureRow> = BTreeMap::new();
    for frame in frames {
        for row in &frame.rows {
            let key = (row.instrument.pool.0.clone(), row.ts.timestamp_millis());
            merged
                .entry(key)
                .and_modify(|existing| {
                    for (k, v) in &row.values {
                        existing.values.insert(k.clone(), *v);
                    }
                })
                .or_insert_with(|| row.clone());
        }
    }
    FeatureFrame {
        rows: merged.into_values().collect(),
    }
}

pub use liquidity::LiquidityFeatures;
pub use onchain::OnchainFeatures;
pub use technical::TechnicalFeatures;
