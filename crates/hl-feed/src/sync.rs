//! The sync engine: keeps the [`MarketStore`] live from Hyperliquid.
//!
//! Backfills the universe and candle buffers over REST, then refreshes them on an
//! interval so the cache stays warm. Consumers read the store instead of fetching,
//! which is what makes scans and screener reads instant. WebSocket candle streaming
//! can later replace the periodic refresh for lower latency.

use std::sync::Arc;
use std::time::Duration;

use domain::Timeframe;
use tokio::task::JoinHandle;

use crate::{MarketStatus, MarketStore, fetch_candles, fetch_universe};

/// How the sync engine seeds and refreshes the store.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Timeframes to keep cached per coin.
    pub timeframes: Vec<Timeframe>,
    /// Number of top coins (by day volume) to track.
    pub universe_limit: usize,
    /// Minimum day volume (USD) for a coin to be tracked.
    pub min_day_volume_usd: f64,
    /// Candles fetched per (coin, timeframe).
    pub lookback: usize,
    /// Delay between REST requests, to respect rate limits.
    pub request_delay: Duration,
    /// Pause between full refresh cycles.
    pub refresh_interval: Duration,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            timeframes: vec![Timeframe::Hour1, Timeframe::Hour4, Timeframe::Day1, Timeframe::Week1],
            universe_limit: 60,
            min_day_volume_usd: 1_000_000.0,
            lookback: 300,
            request_delay: Duration::from_millis(120),
            refresh_interval: Duration::from_secs(60),
        }
    }
}

/// Spawn the background sync: fetch the universe, backfill candles for every
/// (coin, timeframe), then refresh on an interval. The task runs until aborted.
#[must_use]
pub fn spawn_sync(store: Arc<MarketStore>, config: SyncConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut first = true;
        loop {
            refresh(&store, &client, &config, first).await;
            first = false;
            tokio::time::sleep(config.refresh_interval).await;
        }
    })
}

/// One full pass: refresh the universe, then backfill each coin/timeframe.
async fn refresh(store: &MarketStore, client: &reqwest::Client, config: &SyncConfig, first: bool) {
    if first {
        store.set_status(MarketStatus::Connecting);
    }
    let Ok(universe) = fetch_universe(client, config.universe_limit, config.min_day_volume_usd).await else {
        return;
    };
    let coins: Vec<String> = universe.iter().map(|coin| coin.coin.clone()).collect();
    let total = coins.len();
    store.set_universe(universe);

    for (index, coin) in coins.iter().enumerate() {
        if first {
            store.set_status(MarketStatus::Backfilling { done: index, total });
        }
        for &timeframe in &config.timeframes {
            if let Ok(candles) = fetch_candles(client, coin, timeframe, config.lookback).await {
                store.set_candles(coin.clone(), timeframe, candles.into());
            }
            tokio::time::sleep(config.request_delay).await;
        }
    }
    store.set_status(MarketStatus::Live);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use domain::Timeframe;

    use super::{SyncConfig, spawn_sync};
    use crate::MarketStore;

    /// Live smoke test: the sync backfills the store's universe and candles.
    #[tokio::test]
    async fn sync_fills_the_store() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = MarketStore::new();
        let config = SyncConfig {
            timeframes: vec![Timeframe::Day1],
            universe_limit: 3,
            lookback: 60,
            ..SyncConfig::default()
        };
        let handle = spawn_sync(Arc::clone(&store), config);
        tokio::time::sleep(Duration::from_secs(10)).await;
        handle.abort();

        let universe = store.universe();
        assert!(!universe.is_empty(), "universe should populate");
        let cached = universe
            .iter()
            .any(|coin| store.candles(&coin.coin, Timeframe::Day1).is_some_and(|candles| !candles.is_empty()));
        assert!(cached, "at least one coin should have cached daily candles");
    }
}
