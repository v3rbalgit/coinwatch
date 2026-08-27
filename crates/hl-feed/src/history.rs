//! Cache-first access to candle history deeper than the live working window.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use domain::{Candle, Timeframe};
use storage::{CandleStore, StoreError};
use thiserror::Error;

use crate::error::FeedError;
use crate::rest::MAX_CANDLE_PAGE;

/// Provider of candle pages ending immediately before an aligned boundary.
#[async_trait]
pub trait CandleProvider: Send + Sync {
    /// Up to `count` candles strictly before `before_ms`, oldest first.
    async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        count: usize,
    ) -> Result<Vec<Candle>, FeedError>;
}

/// A candle-history load failure.
#[derive(Debug, Error)]
pub enum HistoryError {
    /// The local candle cache could not be read or written.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Hyperliquid could not supply a missing range.
    #[error(transparent)]
    Feed(#[from] FeedError),
    /// The exclusive boundary is not a valid bar open for the timeframe.
    #[error("{before_ms} is not an aligned {timeframe:?} candle boundary")]
    UnalignedBoundary {
        /// Requested timeframe.
        timeframe: Timeframe,
        /// Exclusive boundary in epoch milliseconds.
        before_ms: i64,
    },
    /// Hyperliquid returned candles that do not form the requested suffix.
    #[error("Hyperliquid returned non-contiguous {timeframe:?} candles for '{coin}'")]
    NonContiguousResponse {
        /// Coin symbol.
        coin: String,
        /// Requested timeframe.
        timeframe: Timeframe,
    },
}

/// Shared cache-first loader for chart paging and signal hydration.
pub struct CandleHistory {
    store: Arc<dyn CandleStore>,
    rest: Arc<dyn CandleProvider>,
}

impl CandleHistory {
    /// Combine a durable candle cache with a page provider (Hyperliquid in production).
    #[must_use]
    pub fn new(store: Arc<dyn CandleStore>, rest: Arc<dyn CandleProvider>) -> Self {
        Self { store, rest }
    }

    /// Load up to `limit` contiguous candles immediately before `before_ms`.
    ///
    /// Results are oldest-first. The cache is consulted between REST pages, so
    /// only actual gaps are fetched (and persisted). A short result is a valid
    /// provider or listing boundary.
    pub async fn load_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, HistoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !timeframe.is_aligned(before_ms) {
            return Err(HistoryError::UnalignedBoundary { timeframe, before_ms });
        }
        // A future boundary would fetch and persist the forming bar as final.
        let before_ms = before_ms.min(timeframe.bar_open_ms(Utc::now().timestamp_millis()));

        let duration = timeframe.duration_ms();
        let mut segments: Vec<Vec<Candle>> = Vec::new();
        let mut boundary = before_ms;
        let mut missing = limit;
        while missing > 0 {
            let cached = self.store.candles_before(coin, timeframe, boundary, missing).await?;
            let suffix = contiguous_suffix(&cached, timeframe, boundary);
            if let Some(first) = suffix.first() {
                boundary = first.open_time.timestamp_millis();
                missing = missing.saturating_sub(suffix.len());
                segments.push(suffix.to_vec());
                continue;
            }
            let gap_bars = cached.last().map(|candle| {
                let span = boundary.saturating_sub(candle.open_time.timestamp_millis());
                usize::try_from(span / duration).unwrap_or(usize::MAX).saturating_sub(1).max(1)
            });
            let requested = gap_bars.unwrap_or(missing).min(missing).min(MAX_CANDLE_PAGE);
            let fetched = self.rest.candles_before(coin, timeframe, boundary, requested).await?;
            if fetched.is_empty() {
                break;
            }
            let contiguous = contiguous_suffix(&fetched, timeframe, boundary);
            if contiguous.is_empty() {
                self.store.save(coin, timeframe, &fetched).await?;
                break;
            }
            if contiguous.len() != fetched.len() {
                return Err(HistoryError::NonContiguousResponse {
                    coin: coin.to_owned(),
                    timeframe,
                });
            }
            self.store.save(coin, timeframe, contiguous).await?;
            let short_page = contiguous.len() < requested;
            boundary = contiguous
                .first()
                .map_or(boundary, |candle| candle.open_time.timestamp_millis());
            missing = missing.saturating_sub(contiguous.len());
            segments.push(contiguous.to_vec());
            if short_page {
                break;
            }
        }

        let mut candles = Vec::with_capacity(limit.saturating_sub(missing));
        for segment in segments.into_iter().rev() {
            candles.extend(segment);
        }
        Ok(candles)
    }
}

fn contiguous_suffix(candles: &[Candle], timeframe: Timeframe, before_ms: i64) -> &[Candle] {
    let duration = timeframe.duration_ms();
    let Some(mut expected) = before_ms.checked_sub(duration) else {
        return &[];
    };
    let mut start = candles.len();
    for (index, candle) in candles.iter().enumerate().rev() {
        if candle.open_time.timestamp_millis() != expected {
            break;
        }
        start = index;
        let Some(previous) = expected.checked_sub(duration) else {
            break;
        };
        expected = previous;
    }
    candles.get(start..).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use domain::{Candle, Timeframe};
    use rust_decimal::Decimal;
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::{CandleHistory, CandleProvider, HistoryError, contiguous_suffix};
    use crate::HyperliquidRest;
    use crate::error::FeedError;

    struct CachedStore {
        candles: Mutex<BTreeMap<i64, Candle>>,
        writes: AtomicUsize,
    }

    impl CachedStore {
        fn new(candles: impl IntoIterator<Item = Candle>) -> Self {
            Self {
                candles: Mutex::new(
                    candles
                        .into_iter()
                        .map(|candle| (candle.open_time.timestamp_millis(), candle))
                        .collect(),
                ),
                writes: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl CandleStore for CachedStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Ok(Vec::new())
        }

        async fn save(&self, _coin: &str, _timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            let mut stored = self.candles.lock().unwrap_or_else(PoisonError::into_inner);
            for candle in candles {
                stored.insert(candle.open_time.timestamp_millis(), candle.clone());
            }
            Ok(())
        }

        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            before_ms: i64,
            limit: usize,
        ) -> Result<Vec<Candle>, StoreError> {
            let stored = self.candles.lock().unwrap_or_else(PoisonError::into_inner);
            let mut candles: Vec<Candle> = stored
                .range(..before_ms)
                .rev()
                .take(limit)
                .map(|(_, candle)| candle.clone())
                .collect();
            candles.reverse();
            Ok(candles)
        }
    }

    fn rest() -> Arc<HyperliquidRest> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Arc::new(HyperliquidRest::new(reqwest::Client::new(), 1100))
    }

    struct UnusedProvider;

    #[async_trait]
    impl CandleProvider for UnusedProvider {
        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            _before_ms: i64,
            _count: usize,
        ) -> Result<Vec<Candle>, FeedError> {
            panic!("this test must be served entirely from the cache")
        }
    }

    struct ScriptedProvider {
        pages: Mutex<Vec<(i64, usize, Vec<Candle>)>>,
        requests: Mutex<Vec<(i64, usize)>>,
    }

    impl ScriptedProvider {
        fn new(pages: Vec<(i64, usize, Vec<Candle>)>) -> Self {
            Self {
                pages: Mutex::new(pages),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl CandleProvider for ScriptedProvider {
        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            before_ms: i64,
            count: usize,
        ) -> Result<Vec<Candle>, FeedError> {
            self.requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((before_ms, count));
            let mut pages = self.pages.lock().unwrap_or_else(PoisonError::into_inner);
            let position = pages
                .iter()
                .position(|(expected_before, expected_count, _)| {
                    *expected_before == before_ms && *expected_count == count
                })
                .unwrap_or_else(|| panic!("unscripted provider request ({before_ms}, {count})"));
            Ok(pages.remove(position).2)
        }
    }

    fn candle(hour: i64) -> Candle {
        Candle {
            open_time: DateTime::<Utc>::from_timestamp_millis(hour * Timeframe::Hour1.duration_ms())
                .expect("valid timestamp"),
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ZERO,
        }
    }

    #[test]
    fn contiguous_suffix_stops_at_the_nearest_gap() {
        let candles = vec![candle(0), candle(1), candle(3)];
        assert_eq!(
            contiguous_suffix(&candles, Timeframe::Hour1, 4 * Timeframe::Hour1.duration_ms()),
            &[candle(3)]
        );
    }

    #[tokio::test]
    async fn complete_cache_does_not_fetch_or_write() {
        let store = Arc::new(CachedStore::new((0..5).map(candle)));
        let history = CandleHistory::new(store.clone(), Arc::new(UnusedProvider));

        let candles = history
            .load_before("BTC", Timeframe::Hour1, 5 * Timeframe::Hour1.duration_ms(), 3)
            .await
            .expect("cached history");
        assert_eq!(candles, vec![candle(2), candle(3), candle(4)]);
        assert_eq!(store.writes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    #[ignore = "live network"]
    async fn cache_misses_page_persist_and_become_cache_hits() {
        let store = Arc::new(CachedStore::new(Vec::new()));
        let history = CandleHistory::new(store.clone(), rest());
        let timeframe = Timeframe::Hour1;
        let before_ms = timeframe.bar_open_ms(Utc::now().timestamp_millis());

        let candles = history
            .load_before("BTC", timeframe, before_ms, 600)
            .await
            .expect("paged history");
        assert_eq!(candles.len(), 600);
        assert_eq!(contiguous_suffix(&candles, timeframe, before_ms).len(), 600);
        assert_eq!(store.writes.load(Ordering::Relaxed), 2);

        let cached = history
            .load_before("BTC", timeframe, before_ms, 600)
            .await
            .expect("cached paged history");
        assert_eq!(cached, candles);
        assert_eq!(store.writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn head_gap_fetches_only_the_gap_then_reuses_the_cache() {
        let hour = Timeframe::Hour1.duration_ms();
        let store = Arc::new(CachedStore::new((0..8).map(candle)));
        let provider = Arc::new(ScriptedProvider::new(vec![(10 * hour, 2, vec![candle(8), candle(9)])]));
        let history = CandleHistory::new(store.clone(), provider.clone());

        let candles = history
            .load_before("BTC", Timeframe::Hour1, 10 * hour, 10)
            .await
            .expect("gap-filled history");

        assert_eq!(candles, (0..10).map(candle).collect::<Vec<_>>());
        assert_eq!(
            provider.requests.lock().unwrap_or_else(PoisonError::into_inner).as_slice(),
            &[(10 * hour, 2)]
        );
        assert_eq!(store.writes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn short_head_page_is_persisted_and_returned_short() {
        let hour = Timeframe::Hour1.duration_ms();
        let store = Arc::new(CachedStore::new(Vec::new()));
        let provider = Arc::new(ScriptedProvider::new(vec![(10 * hour, 5, vec![candle(6), candle(7)])]));
        let history = CandleHistory::new(store.clone(), provider);

        let candles = history
            .load_before("BTC", Timeframe::Hour1, 10 * hour, 5)
            .await
            .expect("short history at a provider hole");

        assert!(candles.is_empty());
        assert_eq!(store.writes.load(Ordering::Relaxed), 1);
        let stored = store.candles.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(stored.keys().copied().collect::<Vec<_>>(), vec![6 * hour, 7 * hour]);
    }

    #[tokio::test]
    async fn future_boundaries_are_clamped_to_the_last_completed_bar() {
        let week = Timeframe::Week1.duration_ms();
        let forming_open = Timeframe::Week1.bar_open_ms(Utc::now().timestamp_millis());
        let store = Arc::new(CachedStore::new(Vec::new()));
        let provider = Arc::new(ScriptedProvider::new(vec![(forming_open, 2, Vec::new())]));
        let history = CandleHistory::new(store, provider.clone());

        let candles = history
            .load_before("BTC", Timeframe::Week1, forming_open + week, 2)
            .await
            .expect("clamped history");

        assert!(candles.is_empty());
        assert_eq!(
            provider.requests.lock().unwrap_or_else(PoisonError::into_inner).as_slice(),
            &[(forming_open, 2)]
        );
    }

    #[tokio::test]
    async fn interior_gap_inside_a_page_remains_a_hard_error() {
        let hour = Timeframe::Hour1.duration_ms();
        let store = Arc::new(CachedStore::new(Vec::new()));
        let provider = Arc::new(ScriptedProvider::new(vec![(10 * hour, 5, vec![candle(7), candle(9)])]));
        let history = CandleHistory::new(store, provider);

        assert!(matches!(
            history.load_before("BTC", Timeframe::Hour1, 10 * hour, 5).await,
            Err(HistoryError::NonContiguousResponse { .. })
        ));
    }

    #[tokio::test]
    async fn unaligned_boundaries_are_rejected() {
        let store = Arc::new(CachedStore::new(Vec::new()));
        let history = CandleHistory::new(store, Arc::new(UnusedProvider));

        assert!(matches!(
            history.load_before("BTC", Timeframe::Hour1, 1, 3).await,
            Err(HistoryError::UnalignedBoundary { before_ms: 1, .. })
        ));
    }
}
