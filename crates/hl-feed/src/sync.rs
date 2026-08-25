//! The sync engine: keeps the [`MarketStore`] live from Hyperliquid.
//!
//! Backfills the universe and candle buffers over REST, then refreshes them on an
//! interval so the cache stays warm. Consumers read the store instead of fetching,
//! which is what makes scans and screener reads instant.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use domain::Timeframe;
use storage::{CandleSeries, CandleStore};
use tokio::task::JoinHandle;

use crate::ratelimit::WeightLimiter;
use crate::rest::{fetch_candles, fetch_universe};
use crate::{CANDLE_WINDOW, MarketStore, SyncPhase};

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
    /// REST weight budget per rolling minute (HL's cap is 1200; keep margin).
    pub weight_budget_per_min: u32,
    /// Pause between full refresh cycles.
    pub refresh_interval: Duration,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            timeframes: vec![Timeframe::Hour1, Timeframe::Hour4, Timeframe::Day1, Timeframe::Week1],
            universe_limit: 60,
            min_day_volume_usd: 1_000_000.0,
            lookback: CANDLE_WINDOW,
            weight_budget_per_min: 1100,
            refresh_interval: Duration::from_secs(60),
        }
    }
}

/// Spawn the background sync: fetch the universe, backfill candles for every
/// (coin, timeframe), then refresh on an interval. The task runs until aborted.
#[must_use]
pub fn spawn_sync(store: Arc<MarketStore>, config: SyncConfig, persistence: Arc<dyn CandleStore>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = match reqwest::Client::builder().build() {
            Ok(client) => client,
            Err(error) => {
                tracing::error!(%error, "failed to build HTTP client; market sync disabled");
                return;
            }
        };
        let limiter = WeightLimiter::new(config.weight_budget_per_min);
        let mut backfilled: HashSet<(String, Timeframe)> = HashSet::new();

        match persistence.load_recent(CANDLE_WINDOW).await {
            Ok(series) => {
                let restored = series.len();
                for CandleSeries {
                    coin,
                    timeframe,
                    candles,
                } in series
                {
                    if !candles.is_empty() {
                        store.set_candles(coin.clone(), timeframe, candles);
                        backfilled.insert((coin, timeframe));
                    }
                }
                tracing::info!(restored, "restored persisted candle series");
            }
            Err(error) => tracing::error!(%error, "failed to load persisted candles"),
        }

        loop {
            refresh(&store, &client, &limiter, &config, &mut backfilled, persistence.as_ref()).await;
            tokio::time::sleep(config.refresh_interval).await;
        }
    })
}

/// One full pass: refresh the universe, then backfill each coin/timeframe.
async fn refresh(
    store: &MarketStore,
    client: &reqwest::Client,
    limiter: &WeightLimiter,
    config: &SyncConfig,
    backfilled: &mut HashSet<(String, Timeframe)>,
    persistence: &dyn CandleStore,
) {
    // Heal gaps: the WebSocket flags non-contiguous timeframes; re-backfill them.
    for stale in store.take_stale() {
        backfilled.remove(&stale);
    }

    let universe = match fetch_universe(client, limiter, config.universe_limit, config.min_day_volume_usd).await {
        Ok(universe) => universe,
        Err(error) => {
            tracing::warn!(%error, "universe fetch failed");
            return;
        }
    };
    let coins: Vec<String> = universe.iter().map(|coin| coin.coin.clone()).collect();
    store.set_universe_size(coins.len());
    store.set_universe(universe);

    // Fetch only not-yet-backfilled sets; the WebSocket keeps streamed timeframes live.
    let pending: Vec<(String, Timeframe)> = coins
        .iter()
        .flat_map(|coin| config.timeframes.iter().map(move |&timeframe| (coin.clone(), timeframe)))
        .filter(|key| !backfilled.contains(key))
        .collect();

    if pending.is_empty() {
        store.set_phase(SyncPhase::Live);
        return;
    }

    let total = pending.len();
    store.set_phase(SyncPhase::Backfilling);
    let started = Instant::now();
    let mut done = 0;
    let mut errors = 0;
    for (coin, timeframe) in pending {
        match fetch_candles(client, limiter, &coin, timeframe, config.lookback).await {
            Ok(candles) => {
                if let Err(error) = persistence.save(&coin, timeframe, &candles).await {
                    tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), %error, "failed to persist candles");
                }
                store.set_candles(coin.clone(), timeframe, candles);
                backfilled.insert((coin, timeframe));
            }
            Err(error) => {
                tracing::debug!(coin = %coin, timeframe = timeframe.as_hl(), %error, "candle fetch failed");
                errors += 1;
            }
        }
        done += 1;
        store.set_backfill(done, total, errors, fetch_rate(done, started));
    }
    tracing::info!(done, errors, "candle backfill pass complete");
    store.set_phase(SyncPhase::Live);
}

/// Average candle-fetch rate (fetches per minute) since the pass started.
fn fetch_rate(done: usize, since: Instant) -> u32 {
    let secs = since.elapsed().as_secs();
    if secs == 0 {
        return 0;
    }
    let done = u64::try_from(done).unwrap_or(u64::MAX);
    u32::try_from(done.saturating_mul(60) / secs).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use domain::{Candle, Timeframe};
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::{SyncConfig, spawn_sync};
    use crate::MarketStore;

    struct NoopStore;

    #[async_trait]
    impl CandleStore for NoopStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Ok(Vec::new())
        }
        async fn save(&self, _coin: &str, _timeframe: Timeframe, _candles: &[Candle]) -> Result<(), StoreError> {
            Ok(())
        }
    }

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
        let handle = spawn_sync(Arc::clone(&store), config, Arc::new(NoopStore));
        tokio::time::sleep(Duration::from_secs(10)).await;
        handle.abort();

        let universe = store.universe();
        assert!(!universe.is_empty(), "universe should populate");
        let cached = universe.iter().any(|coin| {
            store
                .candles(&coin.coin, Timeframe::Day1)
                .is_some_and(|candles| !candles.is_empty())
        });
        assert!(cached, "at least one coin should have cached daily candles");
    }
}
