//! `cw-providers` — the ingestion seam and its adapters.
//!
//! [`DataSource`] fetches canonical bars for one instrument; adapters normalize
//! provider payloads into canonical [`cw_core`] types and own their own
//! rate-limiting/backoff. The active adapter is [`Hyperliquid`] (perps); its
//! non-bar capabilities (perp universe, funding) are inherent methods on it.

pub mod hyperliquid;

use async_trait::async_trait;
use cw_core::{Bar, Instrument, Interval, Result};

/// A request for a contiguous run of bars for one instrument.
#[derive(Debug, Clone)]
pub struct BarRequest {
    /// Instrument whose bars to fetch.
    pub instrument: Instrument,
    /// Bar aggregation interval.
    pub interval: Interval,
    /// Maximum bars to return (providers cap this; the ingester paginates).
    pub limit: usize,
}

/// The bar-ingestion seam. Adapters normalize provider payloads into canonical
/// [`Bar`]s.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Stable adapter name for logging/metrics.
    fn name(&self) -> &'static str;

    /// Fetch bars for `req`, oldest-first, OHLC-consistent.
    async fn fetch_bars(&self, req: &BarRequest) -> Result<Vec<Bar>>;
}

pub use hyperliquid::Hyperliquid;
