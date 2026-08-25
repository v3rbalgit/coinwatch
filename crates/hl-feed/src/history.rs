//! Cache-first access to candle history deeper than the live working window.

use std::sync::Arc;

use domain::{Candle, Timeframe};
use storage::{CandleStore, StoreError};
use thiserror::Error;

use crate::error::FeedError;
use crate::rest::{HyperliquidRest, MAX_CANDLE_PAGE};

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
    rest: Arc<HyperliquidRest>,
}

impl CandleHistory {
    /// Combine a durable candle cache with the process-wide Hyperliquid client.
    #[must_use]
    pub fn new(store: Arc<dyn CandleStore>, rest: Arc<HyperliquidRest>) -> Self {
        Self { store, rest }
    }

    /// Load up to `limit` contiguous candles immediately before `before_ms`.
    ///
    /// Results are oldest-first. SQLite is checked first; missing older prefixes
    /// are fetched in provider-sized pages, persisted, then read back. A short
    /// result is a valid provider or listing boundary.
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

        let cached = self.store.candles_before(coin, timeframe, before_ms, limit).await?;
        let cached = contiguous_suffix(&cached, timeframe, before_ms);
        if cached.len() >= limit {
            return Ok(cached.to_vec());
        }

        let mut boundary = cached.first().map_or(before_ms, |candle| candle.open_time.timestamp_millis());
        let mut missing = limit - cached.len();
        while missing > 0 {
            let requested = missing.min(MAX_CANDLE_PAGE);
            let fetched = self.rest.candles_before(coin, timeframe, boundary, requested).await?;
            if fetched.is_empty() {
                break;
            }
            let contiguous = contiguous_suffix(&fetched, timeframe, boundary);
            if contiguous.len() != fetched.len() {
                return Err(HistoryError::NonContiguousResponse {
                    coin: coin.to_owned(),
                    timeframe,
                });
            }

            let fetched = contiguous;
            let fetched_count = fetched.len();
            let Some(first) = fetched.first() else {
                break;
            };
            let next_boundary = first.open_time.timestamp_millis();
            self.store.save(coin, timeframe, fetched).await?;
            boundary = next_boundary;
            if fetched_count >= missing || fetched_count < requested {
                break;
            }
            missing -= fetched_count;
        }

        let cached = self.store.candles_before(coin, timeframe, before_ms, limit).await?;
        Ok(contiguous_suffix(&cached, timeframe, before_ms).to_vec())
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

    use super::{CandleHistory, HistoryError, contiguous_suffix};
    use crate::HyperliquidRest;

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
        let history = CandleHistory::new(store.clone(), rest());

        let candles = history
            .load_before("BTC", Timeframe::Hour1, 5 * Timeframe::Hour1.duration_ms(), 3)
            .await
            .expect("cached history");
        assert_eq!(candles, vec![candle(2), candle(3), candle(4)]);
        assert_eq!(store.writes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
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
    async fn unaligned_boundaries_are_rejected() {
        let store = Arc::new(CachedStore::new(Vec::new()));
        let history = CandleHistory::new(store, rest());

        assert!(matches!(
            history.load_before("BTC", Timeframe::Hour1, 1, 3).await,
            Err(HistoryError::UnalignedBoundary { before_ms: 1, .. })
        ));
    }
}
