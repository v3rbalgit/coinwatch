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
mod reporting_store;
mod rest;
mod store;
mod stream;
mod sync;

pub use error::FeedError;
pub use event::{CandleEvent, MarkPriceEvent, MarketEvent};
pub use feed::MarketFeed;
pub use history::{CandleHistory, CandleProvider, HistoryError};
pub use reporting_store::ReportingCandleStore;
pub use rest::{HyperliquidRest, UniverseCoin};
pub use store::{
    ActiveSeries, CandleHealth, CandleSnapshot, CandleUpdate, ConnectionState, MarketStatus, MarketStore, StorageMode,
    SyncPhase,
};
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

    use crate::{ActiveSeries, HyperliquidAdapter, MarketEvent, MarketStore, UniverseCoin};
    use domain::Timeframe;

    /// Live smoke test: connect and confirm mark-price events flow through the feed.
    #[tokio::test]
    #[ignore = "live network"]
    async fn receives_live_mark_prices() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut adapter = HyperliquidAdapter::new(MarketStore::new(), Vec::new());
        let mut rx = adapter.feed().subscribe_all();
        adapter.connect();

        let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("a mark-price event within 10s")
            .expect("feed channel stays open");
        assert!(matches!(&*event, MarketEvent::Mark(_)), "first event should be a mark price");
    }

    /// Live smoke test: a non-global active chart interval is subscribed dynamically.
    #[tokio::test]
    #[ignore = "live network"]
    async fn receives_active_non_global_candles() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = MarketStore::new();
        store.set_universe(vec![UniverseCoin {
            coin: "BTC".to_owned(),
            day_volume_usd: 0.0,
            mark_px: None,
            price_decimals: 0,
            prev_day_px: None,
            funding: None,
            open_interest: None,
        }]);
        store.set_active_series(Some(ActiveSeries {
            coin: "BTC".to_owned(),
            timeframe: Timeframe::Day1,
        }));
        let mut adapter = HyperliquidAdapter::new(store, vec![Timeframe::Hour1]);
        let mut rx = adapter.feed().subscribe_all();
        adapter.connect();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = rx.recv().await.expect("feed channel stays open");
                if matches!(
                    &*event,
                    MarketEvent::Candle(candle)
                        if candle.coin == "BTC" && candle.timeframe == Timeframe::Day1
                ) {
                    break;
                }
            }
        })
        .await
        .expect("an active 1d candle within 10s");
    }
}
