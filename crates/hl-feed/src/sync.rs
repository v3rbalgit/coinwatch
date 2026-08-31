//! The sync engine: keeps the [`MarketStore`] live from Hyperliquid.
//!
//! Backfills the universe and candle buffers over REST, then refreshes them on an
//! interval so the cache stays warm. Consumers read the store instead of fetching,
//! which is what makes scans and screener reads instant.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use domain::{Candle, Timeframe, completed_prefix_len};
use storage::{CandleSeries, CandleStore};
use tokio::task::JoinHandle;

use crate::rest::HyperliquidRest;
use crate::store::{CandleIntegrity, ReconcileMode, STREAM_RECONCILE_WINDOW};
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
    epoch: u64,
    revision: u64,
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
    match health.integrity {
        CandleIntegrity::VerifyOverlap => {
            return Some(CandleFetch {
                coin,
                timeframe,
                limit: STREAM_RECONCILE_WINDOW,
                mode: ReconcileMode::VerifyOverlap,
                epoch: health.repair_epoch,
                revision: health.write_revision,
            });
        }
        CandleIntegrity::Repair => {
            return Some(CandleFetch {
                coin,
                timeframe,
                limit: lookback,
                mode: ReconcileMode::Replace,
                epoch: health.repair_epoch,
                revision: health.write_revision,
            });
        }
        CandleIntegrity::Verified => {}
    }
    if health.is_current(timeframe, now_ms) {
        return None;
    }

    let current_open_ms = timeframe.bar_open_ms(now_ms);
    let latest_open_ms = match health.latest_open_ms {
        Some(open_ms) if timeframe.is_aligned(open_ms) && open_ms < current_open_ms => open_ms,
        _ => {
            return Some(CandleFetch {
                coin,
                timeframe,
                limit: lookback,
                mode: ReconcileMode::Replace,
                epoch: health.repair_epoch,
                revision: health.write_revision,
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
            epoch: health.repair_epoch,
            revision: health.write_revision,
        }
    } else {
        CandleFetch {
            coin,
            timeframe,
            limit: required,
            mode: ReconcileMode::Merge,
            epoch: health.repair_epoch,
            revision: health.write_revision,
        }
    })
}

/// Spawn the background sync: fetch the universe, backfill candles for every
/// (coin, timeframe), then refresh on an interval. The task runs until aborted.
#[must_use]
pub fn spawn_sync(
    store: Arc<MarketStore>,
    config: SyncConfig,
    persistence: Arc<dyn CandleStore>,
    rest: Arc<HyperliquidRest>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
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
            refresh(&store, &rest, &config, persistence.as_ref()).await;
            tokio::time::sleep(config.refresh_interval).await;
        }
    })
}

/// Freshly completed streamed bars persisted per refresh cycle.
const STREAMED_PERSIST_TAIL: usize = 3;

/// One full pass: refresh the universe, then backfill each coin/timeframe.
async fn refresh(store: &MarketStore, rest: &HyperliquidRest, config: &SyncConfig, persistence: &dyn CandleStore) {
    let universe = match rest.universe(config.universe_limit, config.min_day_volume_usd).await {
        Ok(universe) => universe,
        Err(error) => {
            tracing::warn!(%error, "universe fetch failed");
            return;
        }
    };
    let coins: Vec<String> = universe.iter().map(|coin| coin.coin.clone()).collect();
    store.set_universe(universe);

    let now_ms = Utc::now().timestamp_millis();
    persist_streamed_heads(store, config, persistence, &coins, now_ms).await;
    let pending: Vec<(String, Timeframe)> = coins
        .iter()
        .flat_map(|coin| config.timeframes.iter().map(move |&timeframe| (coin.clone(), timeframe)))
        .filter(|(coin, timeframe)| candle_fetch(store, coin.clone(), *timeframe, config, now_ms).is_some())
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
        if !execute_fetch(store, rest, config, persistence, coin, timeframe).await {
            errors += 1;
        }
        done += 1;
        store.set_backfill(done, total, errors, fetch_rate(done, started));
    }
    tracing::info!(done, errors, "candle reconciliation pass complete");
    store.set_phase(SyncPhase::Live);
}

/// Write streamed series' completed bars through to SQLite; the WS path never persists.
async fn persist_streamed_heads(
    store: &MarketStore,
    config: &SyncConfig,
    persistence: &dyn CandleStore,
    coins: &[String],
    now_ms: i64,
) {
    for coin in coins {
        for &timeframe in &config.streamed_timeframes {
            if !store.series_health(coin, timeframe).is_current(timeframe, now_ms) {
                continue;
            }
            let Some(candles) = store.candles(coin, timeframe) else {
                continue;
            };
            let completed = completed_prefix_len(&candles, timeframe, now_ms);
            let Some(tail) = candles.get(completed.saturating_sub(STREAMED_PERSIST_TAIL)..completed) else {
                continue;
            };
            if tail.is_empty() {
                continue;
            }
            if let Err(error) = persistence.save(coin, timeframe, tail).await {
                tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), %error, "failed to persist streamed candles");
            }
        }
    }
}

/// A snapshot row fetched while its bar was forming holds pre-close values;
/// once that bar closes mid-flight the row must not overwrite fresher data.
fn drop_bars_closed_since_fetch(candles: &mut Vec<Candle>, timeframe: Timeframe, fetched_at_ms: i64, now_ms: i64) {
    let forming_open_ms = timeframe.bar_open_ms(fetched_at_ms);
    if timeframe.bar_open_ms(now_ms) > forming_open_ms {
        candles.retain(|candle| candle.open_time.timestamp_millis() < forming_open_ms);
    }
}

fn completed_rows_in_range(
    candles: &[Candle],
    timeframe: Timeframe,
    at_ms: i64,
    first_open_ms: i64,
    last_open_ms: i64,
) -> Option<&[Candle]> {
    let completed = candles.get(..completed_prefix_len(candles, timeframe, at_ms))?;
    let start = completed.partition_point(|candle| candle.open_time.timestamp_millis() < first_open_ms);
    let end = completed.partition_point(|candle| candle.open_time.timestamp_millis() <= last_open_ms);
    completed.get(start..end)
}

/// Re-plans at execution time so a moved bar boundary cannot stale the window.
/// Rows that close mid-flight are excluded, and persistence uses the reconciled winner.
async fn execute_fetch(
    store: &MarketStore,
    rest: &HyperliquidRest,
    config: &SyncConfig,
    persistence: &dyn CandleStore,
    coin: String,
    timeframe: Timeframe,
) -> bool {
    let fetched_at_ms = Utc::now().timestamp_millis();
    let Some(CandleFetch {
        coin,
        timeframe,
        limit,
        mode,
        epoch,
        revision,
    }) = candle_fetch(store, coin, timeframe, config, fetched_at_ms)
    else {
        return true;
    };
    match rest.recent_candles(&coin, timeframe, limit).await {
        Ok(candles) if candles.is_empty() => {
            tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "candle fetch returned no rows");
            false
        }
        Ok(mut candles) => {
            let reconciled_at_ms = Utc::now().timestamp_millis();
            drop_bars_closed_since_fetch(&mut candles, timeframe, fetched_at_ms, reconciled_at_ms);
            if candles.is_empty() {
                return true;
            }
            let (Some(first), Some(last)) = (candles.first(), candles.last()) else {
                return true;
            };
            let first_open_ms = first.open_time.timestamp_millis();
            let last_open_ms = last.open_time.timestamp_millis();
            // Reconcile before SQLite I/O so the persisted rows come from the
            // post-fetch winner and cannot regress a newer WebSocket tail.
            store.reconcile_candles(coin.clone(), timeframe, candles, mode, epoch, revision);
            match store.series_health(&coin, timeframe).integrity {
                CandleIntegrity::Repair => {
                    tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "candle snapshot requires structural repair");
                    return false;
                }
                CandleIntegrity::VerifyOverlap => {
                    tracing::debug!(coin = %coin, timeframe = timeframe.as_hl(), "candle overlap remains unverified");
                    return false;
                }
                CandleIntegrity::Verified => {}
            }
            store.provision_forming_candle_after_reconcile(&coin, timeframe, timeframe.bar_open_ms(reconciled_at_ms));
            let Some(reconciled) = store.candles(&coin, timeframe) else {
                tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "reconciled candle buffer is missing");
                return false;
            };
            let Some(rows) =
                completed_rows_in_range(&reconciled, timeframe, fetched_at_ms, first_open_ms, last_open_ms)
            else {
                tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "reconciled candle range is invalid");
                return false;
            };
            if !rows.is_empty()
                && let Err(error) = persistence.save(&coin, timeframe, rows).await
            {
                tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), %error, "failed to persist candles");
            }
            if store
                .series_health(&coin, timeframe)
                .is_current(timeframe, Utc::now().timestamp_millis())
            {
                true
            } else {
                tracing::warn!(coin = %coin, timeframe = timeframe.as_hl(), "candle snapshot is not current");
                false
            }
        }
        Err(error) => {
            tracing::debug!(coin = %coin, timeframe = timeframe.as_hl(), %error, "candle fetch failed");
            false
        }
    }
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
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use domain::{Candle, Timeframe};
    use rust_decimal::Decimal;
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::{
        SyncConfig, candle_fetch, completed_rows_in_range, drop_bars_closed_since_fetch, persist_streamed_heads,
        spawn_sync,
    };
    use crate::store::{ReconcileMode, STREAM_RECONCILE_WINDOW};
    use crate::{CANDLE_WINDOW, HyperliquidRest, MarketStore};

    struct NoopStore;

    #[derive(Default)]
    struct RecordingStore {
        saved: Mutex<Vec<(Timeframe, Vec<i64>)>>,
    }

    #[async_trait]
    impl CandleStore for RecordingStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Ok(Vec::new())
        }
        async fn save(&self, _coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError> {
            let opens = candles.iter().map(|candle| candle.open_time.timestamp_millis()).collect();
            self.saved
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((timeframe, opens));
            Ok(())
        }
        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            _before_ms: i64,
            _limit: usize,
        ) -> Result<Vec<Candle>, StoreError> {
            Ok(Vec::new())
        }
    }

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
        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            _before_ms: i64,
            _limit: usize,
        ) -> Result<Vec<Candle>, StoreError> {
            Ok(Vec::new())
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

        store.set_candles("ETH".to_owned(), Timeframe::Hour1, Vec::new());
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);
        let empty_fetch = candle_fetch(&store, "ETH".to_owned(), Timeframe::Hour1, &config, 0)
            .expect("an empty buffer still needs a full initial fetch");
        assert_eq!(empty_fetch.limit, CANDLE_WINDOW);
        assert_eq!(empty_fetch.mode, ReconcileMode::Replace);
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

            let health = store.series_health(coin, timeframe);
            store.reconcile_candles(
                coin.to_owned(),
                timeframe,
                vec![candle_at(0), candle_at(next_open)],
                ReconcileMode::Merge,
                health.repair_epoch,
                health.write_revision,
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
    fn interrupted_streams_plan_a_bounded_overlap_until_verified() {
        let store = MarketStore::new();
        let config = SyncConfig::default();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle_at(0)]);
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);

        let fetch = candle_fetch(&store, "BTC".to_owned(), Timeframe::Hour1, &config, 0)
            .expect("disconnected stream needs verification");
        assert_eq!(fetch.limit, STREAM_RECONCILE_WINDOW);
        assert_eq!(fetch.mode, ReconcileMode::VerifyOverlap);
        assert_eq!(
            candle_fetch(&store, "BTC".to_owned(), Timeframe::Hour1, &config, 0),
            Some(fetch),
            "a failed overlap remains pending",
        );
    }

    #[test]
    fn websocket_gaps_plan_a_full_rest_repair() {
        let store = MarketStore::new();
        let config = SyncConfig::default();
        store.merge_candle("BTC", Timeframe::Hour1, candle_at(0));
        store.merge_candle("BTC", Timeframe::Hour1, candle_at(2 * Timeframe::Hour1.duration_ms()));
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);

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

    #[test]
    fn snapshot_rows_that_closed_mid_flight_are_dropped() {
        let hour = Timeframe::Hour1.duration_ms();
        let mut crossed = vec![candle_at(0), candle_at(hour)];
        drop_bars_closed_since_fetch(&mut crossed, Timeframe::Hour1, hour + 1, 2 * hour + 1);
        assert_eq!(crossed, vec![candle_at(0)]);

        let mut same_bar = vec![candle_at(0), candle_at(hour)];
        drop_bars_closed_since_fetch(&mut same_bar, Timeframe::Hour1, hour + 1, hour + 2);
        assert_eq!(same_bar.len(), 2);
    }

    #[test]
    fn persistence_range_uses_the_reconciled_winner() {
        let hour = Timeframe::Hour1.duration_ms();
        let mut corrected = candle_at(hour);
        corrected.close = Decimal::TWO;
        let winner = vec![candle_at(0), corrected, candle_at(2 * hour)];

        let rows = completed_rows_in_range(&winner, Timeframe::Hour1, 3 * hour, hour, 2 * hour)
            .expect("valid persistence range");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows.first().expect("corrected row").close, Decimal::TWO);
    }

    #[tokio::test]
    async fn streamed_heads_persist_only_completed_bars() {
        let store = MarketStore::new();
        let hour = Timeframe::Hour1.duration_ms();
        store.set_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle_at(0), candle_at(hour), candle_at(2 * hour)],
        );
        let recorder = RecordingStore::default();
        let config = SyncConfig::default();

        persist_streamed_heads(&store, &config, &recorder, &["BTC".to_owned()], 2 * hour + 1).await;

        let saved = recorder.saved.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(saved.as_slice(), &[(Timeframe::Hour1, vec![0, hour])]);
    }

    /// Live smoke test: the sync backfills the store's universe and candles.
    #[tokio::test]
    #[ignore = "live network"]
    async fn sync_fills_the_store() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = MarketStore::new();
        let config = SyncConfig {
            timeframes: vec![Timeframe::Day1, Timeframe::Week1],
            universe_limit: 3,
            lookback: 60,
            ..SyncConfig::default()
        };
        let rest = Arc::new(HyperliquidRest::new(reqwest::Client::new(), config.weight_budget_per_min));
        let handle = spawn_sync(Arc::clone(&store), config, Arc::new(NoopStore), rest);
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
