//! Candle persistence decorated with shared engine-status reporting.

use std::sync::Arc;

use async_trait::async_trait;
use domain::{Candle, Timeframe};
use storage::{CandleSeries, CandleStore, StoreError};

use crate::market_store::MarketStore;

/// A candle store that records every failed operation in the shared market status.
pub struct ReportingCandleStore {
    inner: Arc<dyn CandleStore>,
    status: Arc<MarketStore>,
}

impl ReportingCandleStore {
    /// Wrap a candle store with storage-failure reporting.
    #[must_use]
    pub fn new(inner: Arc<dyn CandleStore>, status: Arc<MarketStore>) -> Self {
        Self { inner, status }
    }

    fn record_failure(&self, error: &StoreError) {
        self.status.record_storage_failure(error.to_string());
    }
}

#[async_trait]
impl CandleStore for ReportingCandleStore {
    async fn load_recent(&self, limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
        self.inner
            .load_recent(limit)
            .await
            .inspect_err(|error| self.record_failure(error))
    }

    async fn save(&self, coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError> {
        self.inner
            .save(coin, timeframe, candles)
            .await
            .inspect_err(|error| self.record_failure(error))
    }

    async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, StoreError> {
        self.inner
            .candles_before(coin, timeframe, before_ms, limit)
            .await
            .inspect_err(|error| self.record_failure(error))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use domain::{Candle, Timeframe};
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::ReportingCandleStore;
    use crate::{MarketStore, StorageMode};

    struct FailingStore;

    #[async_trait]
    impl CandleStore for FailingStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Err(StoreError::InvalidCandleRow(0))
        }

        async fn save(&self, _coin: &str, _timeframe: Timeframe, _candles: &[Candle]) -> Result<(), StoreError> {
            Err(StoreError::InvalidCandleRow(0))
        }

        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            _before_ms: i64,
            _limit: usize,
        ) -> Result<Vec<Candle>, StoreError> {
            Err(StoreError::InvalidCandleRow(0))
        }
    }

    #[tokio::test]
    async fn reports_each_failed_store_operation() {
        let status = MarketStore::new();
        status.set_storage_mode(StorageMode::Persistent, None);
        let inner: Arc<dyn CandleStore> = Arc::new(FailingStore);
        let store = ReportingCandleStore::new(inner, Arc::clone(&status));

        assert!(store.load_recent(1).await.is_err());
        assert!(store.save("BTC", Timeframe::Hour1, &[]).await.is_err());
        assert!(store.candles_before("BTC", Timeframe::Hour1, 0, 1).await.is_err());

        let status = status.status();
        assert_eq!(status.storage_failures, 3);
        assert!(status.last_storage_error.is_some());
    }
}
