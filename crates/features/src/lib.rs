//! `cw-features` — the [`FeatureModule`] seam where edge hypotheses live.
//!
//! Each module turns canonical bars for a universe into a standard
//! `(entity × time × feature)` matrix. Modules are independent and combinable:
//! adding a `tokenomics` or `onchain_activity` module must not touch the engine.
//! This is the seam Phase 2 uses to *confirm* the architecture.

pub mod technical;

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use cw_core::{Bar, Decimal, Instrument, Result};

/// Per-instrument input bars, oldest-first, for a feature computation.
pub struct FeatureContext<'a> {
    pub bars: &'a [(Instrument, Vec<Bar>)],
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

pub use technical::TechnicalFeatures;
