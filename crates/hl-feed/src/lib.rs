//! Real-time Hyperliquid market-data feed.
//!
//! A [`HyperliquidAdapter`] holds Hyperliquid's public WebSocket and publishes typed
//! [`MarketEvent`]s onto a shared [`MarketFeed`] (per-symbol and global `broadcast`
//! channels). Consumers subscribe to the feed; the adapter reconnects on its own.
//!
//! The process must install a rustls crypto provider at startup (the binary's job,
//! per convention) before the first TLS handshake.

mod event;
mod feed;
mod hyperliquid;

pub use event::{MarketEvent, MidEvent};
pub use feed::MarketFeed;
pub use hyperliquid::HyperliquidAdapter;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{HyperliquidAdapter, MarketEvent};

    /// Live smoke test: connect to Hyperliquid and confirm mid events flow through
    /// the feed's global channel. Requires network access.
    #[tokio::test]
    async fn receives_live_mids() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut adapter = HyperliquidAdapter::new();
        let mut rx = adapter.feed().subscribe_all();
        adapter.connect();

        let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("a mid event within 10s")
            .expect("feed channel stays open");
        assert!(matches!(&*event, MarketEvent::Mid(_)), "first event should be a mid");
    }
}
