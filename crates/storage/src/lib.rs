//! SQLite-backed local store: candle persistence (the [`CandleStore`] port) plus
//! user watchlists. OHLCV values are text for exact [`Decimal`] precision; open
//! times are epoch milliseconds. The same store can back onto Postgres later.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

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

/// A user watchlist with its coins.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ListView {
    /// List name.
    pub name: String,
    /// Coins on the list.
    pub coins: Vec<String>,
}

/// A store-backend failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database rejected the operation.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// A durable candle cache the sync seeds from on startup and writes through.
#[async_trait]
pub trait CandleStore: Send + Sync {
    /// Load the most recent `limit` candles per (coin, timeframe) — the sync's hot
    /// working set, not the whole accumulated cache.
    async fn load_recent(&self, limit: usize) -> Result<Vec<CandleSeries>, StoreError>;

    /// Insert-or-replace one (coin, timeframe) series.
    async fn save(&self, coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError>;
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS candles (
    coin      TEXT    NOT NULL,
    timeframe TEXT    NOT NULL,
    open_time INTEGER NOT NULL,
    open      TEXT    NOT NULL,
    high      TEXT    NOT NULL,
    low       TEXT    NOT NULL,
    close     TEXT    NOT NULL,
    volume    TEXT    NOT NULL,
    PRIMARY KEY (coin, timeframe, open_time)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS lists (name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS list_members (
    list TEXT NOT NULL,
    coin TEXT NOT NULL,
    PRIMARY KEY (list, coin)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS logos (coin TEXT PRIMARY KEY, svg TEXT NOT NULL) WITHOUT ROWID;
";

/// The local SQLite store.
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (creating if missing) the database at `path`. A corrupt file is
    /// discarded and recreated — candles are regenerable and lists are cheap.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        if let Ok(store) = Self::connect(path).await {
            if store.is_healthy().await {
                return Ok(store);
            }
            drop(store);
            tracing::warn!(path = %path.display(), "local database failed its integrity check; recreating");
        } else {
            tracing::warn!(path = %path.display(), "local database could not be opened; recreating");
        }
        remove_db_files(path);
        Self::connect(path).await
    }

    async fn connect(path: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePool::connect_with(options).await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }

    async fn is_healthy(&self) -> bool {
        matches!(
            sqlx::query_scalar::<_, String>("PRAGMA integrity_check").fetch_one(&self.pool).await,
            Ok(result) if result == "ok"
        )
    }

    /// Every watchlist with its coins, sorted by name then coin.
    pub async fn lists(&self) -> Result<Vec<ListView>, StoreError> {
        let names: Vec<String> = sqlx::query_scalar("SELECT name FROM lists ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        let members: Vec<(String, String)> = sqlx::query_as("SELECT list, coin FROM list_members ORDER BY coin")
            .fetch_all(&self.pool)
            .await?;

        let mut views: Vec<ListView> = names
            .into_iter()
            .map(|name| ListView {
                name,
                coins: Vec::new(),
            })
            .collect();
        for (list, coin) in members {
            if let Some(view) = views.iter_mut().find(|view| view.name == list) {
                view.coins.push(coin);
            }
        }
        Ok(views)
    }

    /// Create an empty list (a no-op if it already exists).
    pub async fn create_list(&self, name: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT OR IGNORE INTO lists (name) VALUES (?)")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete a list and its members.
    pub async fn delete_list(&self, name: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM list_members WHERE list = ?")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM lists WHERE name = ?")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Add a coin to a list, creating the list if it does not exist.
    pub async fn add_to_list(&self, list: &str, coin: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO lists (name) VALUES (?)")
            .bind(list)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO list_members (list, coin) VALUES (?, ?)")
            .bind(list)
            .bind(coin)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Remove a coin from a list.
    pub async fn remove_from_list(&self, list: &str, coin: &str) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM list_members WHERE list = ? AND coin = ?")
            .bind(list)
            .bind(coin)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every cached (coin, svg) logo.
    pub async fn logos(&self) -> Result<Vec<(String, String)>, StoreError> {
        Ok(sqlx::query_as("SELECT coin, svg FROM logos").fetch_all(&self.pool).await?)
    }

    /// Coins whose logo is already cached.
    pub async fn logo_coins(&self) -> Result<HashSet<String>, StoreError> {
        let coins: Vec<String> = sqlx::query_scalar("SELECT coin FROM logos").fetch_all(&self.pool).await?;
        Ok(coins.into_iter().collect())
    }

    /// Cache a coin's SVG logo.
    pub async fn cache_logo(&self, coin: &str, svg: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT OR REPLACE INTO logos (coin, svg) VALUES (?, ?)")
            .bind(coin)
            .bind(svg)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Up to `limit` cached candles for a (coin, timeframe) with open time before
    /// `before_ms`, oldest-first — for lazily paging chart history.
    pub async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT open_time, open, high, low, close, volume FROM candles \
             WHERE coin = ? AND timeframe = ? AND open_time < ? \
             ORDER BY open_time DESC LIMIT ?",
        )
        .bind(coin)
        .bind(timeframe.as_hl())
        .bind(before_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut candles = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Some(candle) = row_to_candle(row)? {
                candles.push(candle);
            }
        }
        candles.reverse();
        Ok(candles)
    }
}

/// Delete a SQLite database file and its WAL/SHM sidecars.
fn remove_db_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(file));
    }
}

#[async_trait]
impl CandleStore for SqliteStore {
    async fn load_recent(&self, limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT coin, timeframe, open_time, open, high, low, close, volume FROM ( \
                 SELECT coin, timeframe, open_time, open, high, low, close, volume, \
                        ROW_NUMBER() OVER (PARTITION BY coin, timeframe ORDER BY open_time DESC) AS rn \
                 FROM candles \
             ) WHERE rn <= ? ORDER BY coin, timeframe, open_time",
        )
        .bind(limit)
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

    use super::{CandleSeries, CandleStore, SqliteStore};

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
    async fn candles_round_trip_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cw.db");
        let candles = vec![
            candle(1, "100.5", "101", "99.5", "100.75", "12.3"),
            candle(2, "100.75", "102", "100", "101.5", "8.1"),
        ];

        let store = SqliteStore::open(&path).await.expect("open");
        store.save("BTC", Timeframe::Hour1, &candles).await.expect("save");
        drop(store);

        let reopened = SqliteStore::open(&path).await.expect("reopen");
        let loaded = reopened.load_recent(100).await.expect("load");
        assert_eq!(
            loaded,
            vec![CandleSeries {
                coin: "BTC".to_owned(),
                timeframe: Timeframe::Hour1,
                candles
            }]
        );
    }

    #[tokio::test]
    async fn recreates_a_corrupt_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cw.db");
        std::fs::write(&path, b"this is not a sqlite database").expect("write garbage");

        let store = SqliteStore::open(&path).await.expect("open recovers from corruption");
        store
            .save("BTC", Timeframe::Hour1, &[candle(1, "1", "1", "1", "1", "0")])
            .await
            .expect("save");
        assert_eq!(store.load_recent(100).await.expect("load").len(), 1);
    }

    #[tokio::test]
    async fn watchlists_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open");

        store.add_to_list("Favourites", "BTC").await.expect("add");
        store.add_to_list("Favourites", "ETH").await.expect("add");
        store.create_list("Empty").await.expect("create");

        let lists = store.lists().await.expect("lists");
        assert_eq!(lists.len(), 2);
        let favourites = lists.iter().find(|list| list.name == "Favourites").expect("favourites");
        assert_eq!(favourites.coins, vec!["BTC".to_owned(), "ETH".to_owned()]);

        store.remove_from_list("Favourites", "BTC").await.expect("remove");
        store.delete_list("Empty").await.expect("delete");

        let lists = store.lists().await.expect("lists");
        assert_eq!(lists.len(), 1);
        assert_eq!(lists[0].coins, vec!["ETH".to_owned()]);
    }

    #[tokio::test]
    async fn logos_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open");

        assert!(store.logo_coins().await.expect("coins").is_empty());
        store.cache_logo("BTC", "<svg/>").await.expect("cache");
        store.cache_logo("BTC", "<svg-updated/>").await.expect("recache");

        assert_eq!(store.logo_coins().await.expect("coins").len(), 1);
        assert_eq!(
            store.logos().await.expect("logos"),
            vec![("BTC".to_owned(), "<svg-updated/>".to_owned())]
        );
    }

    #[tokio::test]
    async fn candles_before_pages_oldest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open");
        let candles: Vec<Candle> = (1..=10).map(|hour| candle(hour, "1", "1", "1", "1", "0")).collect();
        store.save("BTC", Timeframe::Hour1, &candles).await.expect("save");

        let before = candle(6, "1", "1", "1", "1", "0").open_time.timestamp_millis();
        let page = store.candles_before("BTC", Timeframe::Hour1, before, 3).await.expect("page");

        assert_eq!(page.len(), 3);
        assert_eq!(page.first().map(|bar| bar.open_time), Some(candle(3, "1", "1", "1", "1", "0").open_time));
        assert_eq!(page.last().map(|bar| bar.open_time), Some(candle(5, "1", "1", "1", "1", "0").open_time));
    }
}
