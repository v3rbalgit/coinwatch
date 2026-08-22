//! Technical-analysis feature module (Phase-1 first module).
//!
//! Contributes cross-sectional TA features (momentum, volatility, RSI, …) over
//! OHLCV bars. The concrete computations are implemented in Phase 1; this is the
//! trait wiring proving the seam compiles end-to-end.

use cw_core::Result;

use crate::{FeatureContext, FeatureFrame, FeatureModule};

/// Configurable TA feature set. Lookbacks are in bars of the source interval.
#[derive(Debug, Clone)]
pub struct TechnicalFeatures {
    pub momentum_lookback: usize,
    pub vol_lookback: usize,
}

impl Default for TechnicalFeatures {
    fn default() -> Self {
        Self {
            momentum_lookback: 30,
            vol_lookback: 14,
        }
    }
}

impl FeatureModule for TechnicalFeatures {
    fn name(&self) -> &str {
        "technical"
    }

    fn feature_names(&self) -> &[&str] {
        &["mom", "vol", "rsi"]
    }

    fn compute(&self, _ctx: &FeatureContext<'_>) -> Result<FeatureFrame> {
        Err(cw_core::CoreError::Unimplemented("TechnicalFeatures::compute"))
    }
}
