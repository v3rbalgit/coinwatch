//! Real-time Hyperliquid market-data feed.
//!
//! A [`HyperliquidAdapter`] runs one Hyperliquid WebSocket session, streaming
//! prices and candles as typed [`MarketEvent`]s onto a shared [`MarketFeed`] and
//! merging candles into the [`MarketStore`] hot cache. [`spawn_sync`] keeps the
//! cache warm over REST (universe metadata + candle history). Consumers read the
//! store or subscribe to the feed; the adapter reconnects on its own.
//!
//! The process must install a rustls crypto provider at startup (the binary's job).

mod error;
mod event;
mod feed;
mod history;
mod ratelimit;
mod rest;
mod store;
mod stream;
mod sync;

pub use error::FeedError;
pub use event::{CandleEvent, MarketEvent, MidEvent};
pub use feed::MarketFeed;
pub use history::{CandleHistory, CandleProvider, HistoryError};
pub use rest::{HyperliquidRest, UniverseCoin};
pub use store::{ConnectionState, MarketStatus, MarketStore, SyncPhase};
pub use stream::HyperliquidAdapter;
pub use sync::{SyncConfig, spawn_sync};

/// Candles retained per (coin, timeframe): the indicator hot working window,
/// shared by the DB seed, REST backfill depth, and in-memory cap.
///
/// The built-in divergence template reaches back 68 bars (freshness 3 + pivot-right
/// 5 + max-gap 60); the default MACD slow EMA needs about 90 more bars to reduce seed
/// influence below 1%. The resulting ~158-bar working set plus ~25% headroom gives
/// 200. Custom definitions calculate their exact completed-bar requirements and
/// hydrate older SQLite/REST history through `CandleHistory`, so this is a performance
/// boundary rather than a validity limit.
pub(crate) const CANDLE_WINDOW: usize = 200;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{HyperliquidAdapter, MarketEvent, MarketStore};

    /// Live smoke test: connect and confirm mid events flow through the feed.
    #[tokio::test]
    #[ignore = "live network"]
    async fn receives_live_mids() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut adapter = HyperliquidAdapter::new(MarketStore::new(), Vec::new());
        let mut rx = adapter.feed().subscribe_all();
        adapter.connect();

        let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("a mid event within 10s")
            .expect("feed channel stays open");
        assert!(matches!(&*event, MarketEvent::Mid(_)), "first event should be a mid");
    }
}
