//! The sync engine: keeps the [`MarketStore`] live from Hyperliquid.
//!
//! Backfills the universe and candle buffers over REST, then refreshes them on an
//! interval so the cache stays warm. Consumers read the store instead of fetching,
//! which is what makes scans and screener reads instant.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use domain::Timeframe;
use storage::{CandleSeries, CandleStore};
use tokio::task::JoinHandle;

use crate::ratelimit::WeightLimiter;
use crate::rest::{fetch_candles, fetch_universe};
use crate::store::ReconcileMode;
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
    /// Candles requested for an initial or full repair, capped by the in-memory window.
    pub lookback: usize,
    /// Timeframes maintained by WebSocket after REST backfill. REST remains the
    /// fallback when a streamed series has not advanced after the boundary grace.
    pub streamed_timeframes: Vec<Timeframe>,
    /// Delay after a new bar opens before REST reconciliation. This lets the
    /// provider finalize the closed bar and publish the new forming bar.
    pub boundary_grace: Duration,
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
            streamed_timeframes: vec![Timeframe::Hour1, Timeframe::Hour4],
            boundary_grace: Duration::from_secs(10),
            weight_budget_per_min: 1100,
            refresh_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CandleFetch {
    coin: String,
    timeframe: Timeframe,
    limit: usize,
    mode: ReconcileMode,
}

fn candle_fetch(
    store: &MarketStore,
    coin: String,
    timeframe: Timeframe,
    config: &SyncConfig,
    now_ms: i64,
) -> Option<CandleFetch> {
    let lookback = config.lookback.clamp(2, CANDLE_WINDOW);
    let health = store.series_health(&coin, timeframe);
    if health.is_current(timeframe, now_ms) {
        return None;
    }

    let current_open_ms = timeframe.bar_open_ms(now_ms);
    let latest_open_ms = match health.latest_open_ms {
        Some(open_ms) if !health.needs_repair && timeframe.is_aligned(open_ms) && open_ms < current_open_ms => open_ms,
        _ => {
            return Some(CandleFetch {
                coin,
                timeframe,
                limit: lookback,
                mode: ReconcileMode::Replace,
            });
        }
    };
    let grace_ms = i64::try_from(config.boundary_grace.as_millis()).unwrap_or(i64::MAX);
    let within_grace = now_ms.saturating_sub(current_open_ms) < grace_ms;
    if within_grace || latest_open_ms == current_open_ms {
        return None;
    }

    let missing_bars = current_open_ms.saturating_sub(latest_open_ms) / timeframe.duration_ms();
    let required = usize::try_from(missing_bars).unwrap_or(usize::MAX).saturating_add(1).max(2);
    Some(if required >= lookback {
        CandleFetch {
            coin,
            timeframe,
            limit: lookback,
            mode: ReconcileMode::Replace,
        }
    } else {
        CandleFetch {
            coin,
            timeframe,
            limit: required,
            mode: ReconcileMode::Merge,
        }
    })
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
                        store.set_candles(coin, timeframe, candles);
                    }
                }
                tracing::info!(restored, "restored persisted candle series");
            }
            Err(error) => tracing::error!(%error, "failed to load persisted candles"),
        }

        loop {
            refresh(&store, &client, &limiter, &config, persistence.as_ref()).await;
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
    persistence: &dyn CandleStore,
) {
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

    let now_ms = Utc::now().timestamp_millis();
    let pending: Vec<CandleFetch> = coins
        .iter()
        .flat_map(|coin| config.timeframes.iter().map(move |&timeframe| (coin.clone(), timeframe)))
        .filter_map(|(coin, timeframe)| candle_fetch(store, coin, timeframe, config, now_ms))
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
    for fetch in pending {
        let CandleFetch {
            coin,
            timeframe,
            limit,
            mode,
        } = fetch;
        match fetch_candles(client, limiter, &coin, timeframe, limit).await {
            Ok(candles) if candles.is_empty() => {
                tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "candle fetch returned no rows");
                errors += 1;
            }
            Ok(candles) => {
                // Reconcile before SQLite I/O so a newer WebSocket update cannot be
                // overwritten after a slow persistence transaction completes.
                store.reconcile_candles(coin.clone(), timeframe, candles.clone(), mode);
                let health = store.series_health(&coin, timeframe);
                if health.needs_repair {
                    tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "candle snapshot is not contiguous");
                    errors += 1;
                } else {
                    if let Err(error) = persistence.save(&coin, timeframe, &candles).await {
                        tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), %error, "failed to persist candles");
                    }
                    let reconciled_at = Utc::now().timestamp_millis();
                    if !store.series_health(&coin, timeframe).is_current(timeframe, reconciled_at) {
                        tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "candle snapshot is not current");
                        errors += 1;
                    }
                }
            }
            Err(error) => {
                tracing::debug!(coin = %coin, timeframe = timeframe.as_hl(), %error, "candle fetch failed");
                errors += 1;
            }
        }
        done += 1;
        store.set_backfill(done, total, errors, fetch_rate(done, started));
    }
    tracing::info!(done, errors, "candle reconciliation pass complete");
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
    use chrono::{DateTime, Utc};
    use domain::{Candle, Timeframe};
    use rust_decimal::Decimal;
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::{SyncConfig, candle_fetch, spawn_sync};
    use crate::store::ReconcileMode;
    use crate::{CANDLE_WINDOW, MarketStore};

    struct NoopStore;

    fn candle_at(open_ms: i64) -> Candle {
        Candle {
            open_time: DateTime::<Utc>::from_timestamp_millis(open_ms).expect("valid timestamp"),
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ZERO,
        }
    }

    #[async_trait]
    impl CandleStore for NoopStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Ok(Vec::new())
        }
        async fn save(&self, _coin: &str, _timeframe: Timeframe, _candles: &[Candle]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    #[test]
    fn missing_series_plan_a_capped_full_fetch() {
        let store = MarketStore::new();
        let config = SyncConfig {
            lookback: usize::MAX,
            ..SyncConfig::default()
        };

        let fetch = candle_fetch(&store, "BTC".to_owned(), Timeframe::Hour1, &config, 0)
            .expect("missing series needs an initial fetch");
        assert_eq!(fetch.limit, CANDLE_WINDOW);
        assert_eq!(fetch.mode, ReconcileMode::Replace);
    }

    #[test]
    fn rest_series_fetch_once_per_new_bar_after_the_grace_period() {
        let config = SyncConfig::default();
        let grace_ms = i64::try_from(config.boundary_grace.as_millis()).expect("grace fits i64");

        for (coin, timeframe) in [("BTC", Timeframe::Day1), ("ETH", Timeframe::Week1)] {
            let store = MarketStore::new();
            store.set_candles(coin.to_owned(), timeframe, vec![candle_at(0)]);
            let next_open = timeframe.duration_ms();

            assert!(store.current_candles(coin, timeframe, next_open).is_none());
            assert!(candle_fetch(&store, coin.to_owned(), timeframe, &config, next_open + grace_ms - 1).is_none());

            let fetch = candle_fetch(&store, coin.to_owned(), timeframe, &config, next_open + grace_ms)
                .expect("new REST bar is due");
            assert_eq!(fetch.limit, 2);
            assert_eq!(fetch.mode, ReconcileMode::Merge);
            assert_eq!(
                candle_fetch(&store, coin.to_owned(), timeframe, &config, next_open + grace_ms),
                Some(fetch),
                "an unsuccessful fetch remains due",
            );

            store.reconcile_candles(
                coin.to_owned(),
                timeframe,
                vec![candle_at(0), candle_at(next_open)],
                ReconcileMode::Merge,
            );
            assert!(candle_fetch(&store, coin.to_owned(), timeframe, &config, next_open + grace_ms + 1).is_none());
        }
    }

    #[test]
    fn streamed_timeframes_use_rest_only_when_websocket_is_behind() {
        let store = MarketStore::new();
        let config = SyncConfig::default();
        let current_open = 10 * Timeframe::Hour1.duration_ms();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle_at(current_open)]);
        assert!(candle_fetch(&store, "BTC".to_owned(), Timeframe::Hour1, &config, current_open).is_none());

        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle_at(0)]);
        let grace_ms = i64::try_from(config.boundary_grace.as_millis()).expect("grace fits i64");
        let fetch = candle_fetch(&store, "BTC".to_owned(), Timeframe::Hour1, &config, current_open + grace_ms)
            .expect("stale streamed series needs REST fallback");
        assert_eq!(fetch.limit, 11);
        assert_eq!(fetch.mode, ReconcileMode::Merge);
    }

    #[test]
    fn interrupted_streams_plan_a_full_rest_repair() {
        let store = MarketStore::new();
        let config = SyncConfig::default();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle_at(0)]);
        store.mark_streamed_for_reconciliation(&[Timeframe::Hour1]);

        let fetch = candle_fetch(&store, "BTC".to_owned(), Timeframe::Hour1, &config, 0)
            .expect("disconnected stream needs repair");
        assert_eq!(fetch.limit, config.lookback);
        assert_eq!(fetch.mode, ReconcileMode::Replace);
    }

    #[test]
    fn websocket_gaps_plan_a_full_rest_repair() {
        let store = MarketStore::new();
        let config = SyncConfig::default();
        store.merge_candle("BTC", Timeframe::Hour1, candle_at(0));
        store.merge_candle("BTC", Timeframe::Hour1, candle_at(2 * Timeframe::Hour1.duration_ms()));

        let fetch = candle_fetch(
            &store,
            "BTC".to_owned(),
            Timeframe::Hour1,
            &config,
            2 * Timeframe::Hour1.duration_ms(),
        )
        .expect("gapped stream needs repair");
        assert_eq!(fetch.limit, config.lookback);
        assert_eq!(fetch.mode, ReconcileMode::Replace);
    }

    /// Live smoke test: the sync backfills the store's universe and candles.
    #[tokio::test]
    async fn sync_fills_the_store() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = MarketStore::new();
        let config = SyncConfig {
            timeframes: vec![Timeframe::Day1, Timeframe::Week1],
            universe_limit: 3,
            lookback: 60,
            ..SyncConfig::default()
        };
        let handle = spawn_sync(Arc::clone(&store), config, Arc::new(NoopStore));
        tokio::time::sleep(Duration::from_secs(10)).await;
        handle.abort();

        let universe = store.universe();
        assert!(!universe.is_empty(), "universe should populate");
        let now_ms = Utc::now().timestamp_millis();
        for timeframe in [Timeframe::Day1, Timeframe::Week1] {
            let current = universe
                .iter()
                .any(|coin| store.current_candles(&coin.coin, timeframe, now_ms).is_some());
            assert!(
                current,
                "at least one coin should have current, contiguous {timeframe:?} candles"
            );
        }
    }
}
