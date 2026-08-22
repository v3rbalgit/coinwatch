//! `cw-providers` — the [`DataSource`] seam and its adapters.
//!
//! Every external feed (GeckoTerminal, Bitquery, a self-hosted RPC node) is an
//! adapter behind one async trait. Swapping keyless → keyed → paid, or one
//! provider for another, is a wiring change — never a change to the rest of the
//! system, which only ever sees canonical [`cw_core`] types.

pub mod geckoterminal;

use async_trait::async_trait;
use cw_core::{Bar, Instrument, Interval, Result};

/// A request for a contiguous run of bars for one instrument.
#[derive(Debug, Clone)]
pub struct BarRequest {
    pub instrument: Instrument,
    pub interval: Interval,
    /// Maximum bars to return (providers cap this; the ingester paginates).
    pub limit: usize,
}

/// The ingestion seam. Adapters normalize provider payloads into canonical
/// [`Bar`]s and are responsible for their own rate-limiting/backoff.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Stable adapter name for logging/metrics (e.g. `"geckoterminal"`).
    fn name(&self) -> &str;

    /// Fetch bars for `req`, oldest-first, OHLC-consistent.
    async fn fetch_bars(&self, req: &BarRequest) -> Result<Vec<Bar>>;
}

pub use geckoterminal::GeckoTerminal;
