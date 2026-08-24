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
mod ratelimit;
mod rest;
mod store;
mod stream;
mod sync;

pub use event::{CandleEvent, MarketEvent, MidEvent};
pub use feed::MarketFeed;
pub use rest::UniverseCoin;
pub use store::{ConnectionState, MarketStatus, MarketStore, SyncPhase};
pub use stream::HyperliquidAdapter;
pub use sync::{SyncConfig, spawn_sync};

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{HyperliquidAdapter, MarketEvent, MarketStore};

    /// Live smoke test: connect and confirm mid events flow through the feed.
    #[tokio::test]
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
