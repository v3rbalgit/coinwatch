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

pub use error::FeedError;
pub use event::{CandleEvent, MarketEvent, MidEvent};
pub use feed::MarketFeed;
pub use rest::{UniverseCoin, fetch_candle_range};
pub use store::{ConnectionState, MarketStatus, MarketStore, SyncPhase};
pub use stream::HyperliquidAdapter;
pub use sync::{SyncConfig, spawn_sync};

/// Candles retained per (coin, timeframe): the indicator hot working window,
/// shared by the DB seed, REST backfill depth, and in-memory cap.
///
/// Derived from the shipped scanner parameters (MACD 26/9, RSI 14, 5-bar pivots, max_gap 60,
/// recent_within 3): the freshest reported divergence reaches back
/// recent_within(3) + pivot_right(5) + max_gap(60) = 68 bars, and its oscillator
/// values need MACD's slow-EMA(26) warmup (~90 bars to under 1% seed influence),
/// so ~158 bars are load-bearing. 200 adds ~25% margin so the oldest usable bars
/// are comfortably converged. Deeper chart history lives in the DB, not memory;
/// user-defined signals needing more history require a separate validated contract.
pub(crate) const CANDLE_WINDOW: usize = 200;

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
