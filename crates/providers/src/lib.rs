//! `cw-providers` — the ingestion seams and their adapters.
//!
//! Two capabilities, deliberately split so a feed that only serves OHLCV need not
//! implement pool discovery:
//! - [`DataSource`] — fetch canonical bars for one instrument.
//! - [`UniverseSource`] — discover the tradable universe (top liquid pools).
//!
//! Every external feed (GeckoTerminal, Bitquery, a self-hosted RPC node) is an
//! adapter behind these traits. Swapping keyless → keyed → paid, or one provider
//! for another, is a wiring change — never a change to the rest of the system,
//! which only ever sees canonical [`cw_core`] types. Adapters own their
//! rate-limiting/backoff.

pub mod geckoterminal;

use async_trait::async_trait;
use cw_core::{Bar, Decimal, Instrument, Interval, Network, Result};

/// A request for a contiguous run of bars for one instrument.
#[derive(Debug, Clone)]
pub struct BarRequest {
    pub instrument: Instrument,
    pub interval: Interval,
    /// Maximum bars to return (providers cap this; the ingester paginates).
    pub limit: usize,
}

/// The bar-ingestion seam. Adapters normalize provider payloads into canonical
/// [`Bar`]s.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Stable adapter name for logging/metrics (e.g. `"geckoterminal"`).
    fn name(&self) -> &str;

    /// Fetch bars for `req`, oldest-first, OHLC-consistent.
    async fn fetch_bars(&self, req: &BarRequest) -> Result<Vec<Bar>>;
}

/// Parameters for universe discovery.
#[derive(Debug, Clone)]
pub struct UniverseRequest {
    /// Minimum pool reserve (USD) to be considered tradable.
    pub min_reserve_usd: Decimal,
    /// Max pools to return (after filtering/dedupe).
    pub limit: usize,
    /// How many listing pages to scan.
    pub pages: usize,
}

/// A discovered pool with the liquidity/volume that qualified it.
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub instrument: Instrument,
    pub reserve_usd: Decimal,
    pub volume_24h_usd: Decimal,
}

/// The universe-discovery seam: rank/list the top liquid pools on a network.
#[async_trait]
pub trait UniverseSource: Send + Sync {
    /// Top pools on `network` satisfying `req`, deduped by base token,
    /// highest-liquidity first.
    async fn top_pools(&self, network: &Network, req: &UniverseRequest) -> Result<Vec<PoolInfo>>;
}

pub use geckoterminal::GeckoTerminal;
