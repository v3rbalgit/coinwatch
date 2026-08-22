//! `cw-strategy` — the [`Strategy`] seam.
//!
//! A strategy maps a [`FeatureFrame`] to target portfolio weights. The *same*
//! implementation is driven by the backtester and (later) the live runner —
//! that single source of truth is what lets a backtested edge be trusted in
//! deployment.

use cw_core::{Result, TargetWeight};
use cw_features::FeatureFrame;

/// Maps computed features to desired positions at a rebalance point.
pub trait Strategy {
    fn name(&self) -> &str;

    /// Produce target weights from the latest feature frame. Implementations
    /// must be pure w.r.t. their inputs (no hidden state, no lookahead).
    fn target_weights(&self, frame: &FeatureFrame) -> Result<Vec<TargetWeight>>;
}

/// Long-top / short-or-flat-bottom cross-sectional strategy over a ranked
/// feature. Phase-1 first strategy — the one that exercises portfolio
/// construction, costs, and survivorship in the backtester.
#[derive(Debug, Clone)]
pub struct CrossSectional {
    /// Feature column to rank on (must be produced by an active module).
    pub rank_feature: String,
    /// Fraction of the universe taken as the long leg (0.0–1.0).
    pub top_quantile: f64,
    /// If false, the bottom leg is held flat instead of shorted.
    pub allow_short: bool,
}

impl Strategy for CrossSectional {
    fn name(&self) -> &str {
        "cross_sectional"
    }

    fn target_weights(&self, _frame: &FeatureFrame) -> Result<Vec<TargetWeight>> {
        Err(cw_core::CoreError::Unimplemented("CrossSectional::target_weights"))
    }
}
