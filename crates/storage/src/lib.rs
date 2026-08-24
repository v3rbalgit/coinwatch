//! SQLite-backed candle persistence: the [`CandleStore`] port and its local adapter.
//!
//! OHLCV values are stored as text for exact [`Decimal`] precision; open times as
//! epoch milliseconds. The same port can back onto Postgres later without touching
//! the sync.

use std::path::Path;

use async_trait::async_trait;
use chrono::DateTime;
use domain::{Candle, Timeframe};
use rust_decimal::Decimal;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteRow};
use sqlx::{Row, SqlitePool};

/// A persisted candle series for one (coin, timeframe), oldest candle first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandleSeries {
    /// Coin symbol.
    pub coin: String,
    /// Timeframe of the series.
    pub timeframe: Timeframe,
    /// The candles.
    pub candles: Vec<Candle>,
}

/// A candle-store backend failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database rejected the operation.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// A durable candle cache. The sync seeds the in-memory store from it on startup
/// and writes each backfilled series through, so restarts skip re-downloading.
#[async_trait]
pub trait CandleStore: Send + Sync {
    /// Load every persisted series.
    async fn load(&self) -> Result<Vec<CandleSeries>, StoreError>;

    /// Insert-or-replace one (coin, timeframe) series.
    async fn save(&self, coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError>;
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS candles (
    coin      TEXT    NOT NULL,
    timeframe TEXT    NOT NULL,
    open_time INTEGER NOT NULL,
    open      TEXT    NOT NULL,
    high      TEXT    NOT NULL,
    low       TEXT    NOT NULL,
    close     TEXT    NOT NULL,
    volume    TEXT    NOT NULL,
    PRIMARY KEY (coin, timeframe, open_time)
) WITHOUT ROWID";

/// A local SQLite candle cache.
pub struct SqliteCandleStore {
    pool: SqlitePool,
}

impl SqliteCandleStore {
    /// Open (creating if missing) the candle database at `path` and ensure the schema.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePool::connect_with(options).await?;
        sqlx::query(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl CandleStore for SqliteCandleStore {
    async fn load(&self) -> Result<Vec<CandleSeries>, StoreError> {
        let rows = sqlx::query(
            "SELECT coin, timeframe, open_time, open, high, low, close, volume \
             FROM candles ORDER BY coin, timeframe, open_time",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut series: Vec<CandleSeries> = Vec::new();
        for row in &rows {
            let coin: String = row.try_get("coin")?;
            let interval: String = row.try_get("timeframe")?;
            let (Some(timeframe), Some(candle)) = (Timeframe::from_hl(&interval), row_to_candle(row)?) else {
                continue;
            };
            match series.last_mut() {
                Some(last) if last.coin == coin && last.timeframe == timeframe => last.candles.push(candle),
                _ => series.push(CandleSeries {
                    coin,
                    timeframe,
                    candles: vec![candle],
                }),
            }
        }
        Ok(series)
    }

    async fn save(&self, coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        for candle in candles {
            sqlx::query(
                "INSERT OR REPLACE INTO candles \
                 (coin, timeframe, open_time, open, high, low, close, volume) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(coin)
            .bind(timeframe.as_hl())
            .bind(candle.open_time.timestamp_millis())
            .bind(candle.open.to_string())
            .bind(candle.high.to_string())
            .bind(candle.low.to_string())
            .bind(candle.close.to_string())
            .bind(candle.volume.to_string())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// Read one row into a candle; a malformed timestamp or decimal skips the row.
fn row_to_candle(row: &SqliteRow) -> Result<Option<Candle>, StoreError> {
    let open_ms: i64 = row.try_get("open_time")?;
    let open: String = row.try_get("open")?;
    let high: String = row.try_get("high")?;
    let low: String = row.try_get("low")?;
    let close: String = row.try_get("close")?;
    let volume: String = row.try_get("volume")?;

    let (Some(open_time), Ok(open), Ok(high), Ok(low), Ok(close), Ok(volume)) = (
        DateTime::from_timestamp_millis(open_ms),
        open.parse::<Decimal>(),
        high.parse::<Decimal>(),
        low.parse::<Decimal>(),
        close.parse::<Decimal>(),
        volume.parse::<Decimal>(),
    ) else {
        return Ok(None);
    };
    Ok(Some(Candle {
        open_time,
        open,
        high,
        low,
        close,
        volume,
    }))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};

    use super::{CandleSeries, CandleStore, SqliteCandleStore};

    fn candle(hour: i64, open: &str, high: &str, low: &str, close: &str, volume: &str) -> Candle {
        Candle {
            open_time: Utc.timestamp_opt(hour * 3600, 0).single().expect("timestamp"),
            open: open.parse().expect("open"),
            high: high.parse().expect("high"),
            low: low.parse().expect("low"),
            close: close.parse().expect("close"),
            volume: volume.parse().expect("volume"),
        }
    }

    #[tokio::test]
    async fn round_trips_a_series_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("candles.db");
        let candles = vec![
            candle(1, "100.5", "101", "99.5", "100.75", "12.3"),
            candle(2, "100.75", "102", "100", "101.5", "8.1"),
        ];

        let store = SqliteCandleStore::open(&path).await.expect("open");
        store.save("BTC", Timeframe::Hour1, &candles).await.expect("save");
        drop(store);

        let reopened = SqliteCandleStore::open(&path).await.expect("reopen");
        let loaded = reopened.load().await.expect("load");
        assert_eq!(
            loaded,
            vec![CandleSeries {
                coin: "BTC".to_owned(),
                timeframe: Timeframe::Hour1,
                candles
            }]
        );
    }
}
