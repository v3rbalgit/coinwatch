//! SQLite-backed local store: candle persistence (the [`CandleStore`] port),
//! watchlists, saved signals, indicator presets, logos, and UI settings.
//!
//! OHLCV values are stored as text to preserve exact decimal precision; open times
//! are epoch milliseconds. Signal definitions and preset params remain opaque JSON
//! here so storage stays independent of the product model that validates them.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::DateTime;
use domain::{Candle, Timeframe};
use sqlx::SqlitePool;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use thiserror::Error;

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

/// One saved signal catalog entry. `definition` is validated JSON owned by the
/// signals layer, kept opaque here to avoid reversing the crate dependency graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalRecord {
    /// Stable id used by future alerts and references.
    pub id: String,
    /// Unique user-facing name.
    pub name: String,
    /// Serialized signal definition.
    pub definition: String,
}

/// One named parameter preset for an indicator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndicatorPresetRecord {
    /// Stable preset id.
    pub id: String,
    /// Indicator descriptor id whose params this preset configures.
    pub indicator_id: String,
    /// Name unique within that indicator.
    pub name: String,
    /// Serialized parameter map.
    pub params: String,
}

struct CandleGroupRow {
    coin: String,
    timeframe: String,
}

struct CandleRow {
    open_time: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

/// A store-backend failure.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The database rejected the operation.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// A schema migration failed.
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// Another signal already owns the generated id.
    #[error("a signal with id '{0}' already exists")]
    DuplicateSignalId(String),
    /// Another signal already owns the requested catalog name.
    #[error("a signal named '{0}' already exists")]
    DuplicateSignalName(String),
    /// No signal owns the requested id.
    #[error("no signal with id '{0}' exists")]
    SignalNotFound(String),
    /// Another preset already owns the generated id.
    #[error("an indicator preset with id '{0}' already exists")]
    DuplicatePresetId(String),
    /// Another preset for the same indicator already owns the requested name.
    #[error("a preset named '{name}' already exists for indicator '{indicator_id}'")]
    DuplicatePresetName {
        /// Indicator descriptor id.
        indicator_id: String,
        /// Conflicting preset name.
        name: String,
    },
    /// No indicator preset owns the requested id.
    #[error("no indicator preset with id '{0}' exists")]
    PresetNotFound(String),
    /// SQLite opened the file but its integrity check failed. The file is left in
    /// place so user data can be recovered explicitly.
    #[error("database at '{}' failed its integrity check", .0.display())]
    Integrity(PathBuf),
    /// A cached candle row contains an invalid timestamp or decimal value.
    #[error("cached candle row at {0} contains invalid data")]
    InvalidCandleRow(i64),
}

impl TryFrom<CandleRow> for Candle {
    type Error = StoreError;

    fn try_from(row: CandleRow) -> Result<Self, Self::Error> {
        let open_time =
            DateTime::from_timestamp_millis(row.open_time).ok_or(StoreError::InvalidCandleRow(row.open_time))?;
        let open = row.open.parse().map_err(|_| StoreError::InvalidCandleRow(row.open_time))?;
        let high = row.high.parse().map_err(|_| StoreError::InvalidCandleRow(row.open_time))?;
        let low = row.low.parse().map_err(|_| StoreError::InvalidCandleRow(row.open_time))?;
        let close = row.close.parse().map_err(|_| StoreError::InvalidCandleRow(row.open_time))?;
        let volume = row.volume.parse().map_err(|_| StoreError::InvalidCandleRow(row.open_time))?;
        Ok(Self {
            open_time,
            open,
            high,
            low,
            close,
            volume,
        })
    }
}

fn candles_oldest_first(rows: Vec<CandleRow>) -> Result<Vec<Candle>, StoreError> {
    let mut candles = rows.into_iter().map(Candle::try_from).collect::<Result<Vec<_>, _>>()?;
    candles.reverse();
    Ok(candles)
}

/// A durable candle cache the sync seeds from on startup and writes through.
#[async_trait]
pub trait CandleStore: Send + Sync {
    /// Load the most recent `limit` candles per (coin, timeframe) — the sync's hot
    /// working set, not the whole accumulated cache.
    async fn load_recent(&self, limit: usize) -> Result<Vec<CandleSeries>, StoreError>;

    /// Insert-or-replace one (coin, timeframe) series.
    async fn save(&self, coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError>;

    /// Load up to `limit` candles strictly before `before_ms`, oldest first.
    async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, StoreError>;
}

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// The local SQLite store.
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (creating if missing) the database at `path`.
    ///
    /// A corrupt file is never deleted automatically: the database now contains
    /// durable user-authored state, so preserving it for recovery is safer than
    /// silently recreating an empty store.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let store = Self::connect(path).await?;
        if !store.is_healthy().await? {
            return Err(StoreError::Integrity(path.to_owned()));
        }
        MIGRATOR.run(&store.pool).await?;
        Ok(store)
    }

    async fn connect(path: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePool::connect_with(options).await?;
        Ok(Self { pool })
    }

    async fn is_healthy(&self) -> Result<bool, StoreError> {
        let result = sqlx::query_scalar!("PRAGMA integrity_check").fetch_one(&self.pool).await?;
        Ok(result.as_deref() == Some("ok"))
    }

    /// Every watchlist with its coins, sorted by name then coin.
    pub async fn lists(&self) -> Result<Vec<ListView>, StoreError> {
        let names = sqlx::query_scalar!("SELECT name FROM lists ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        let members = sqlx::query!("SELECT list, coin FROM list_members ORDER BY coin")
            .fetch_all(&self.pool)
            .await?;

        let mut views: Vec<ListView> = names
            .into_iter()
            .map(|name| ListView {
                name,
                coins: Vec::new(),
            })
            .collect();
        for member in members {
            if let Some(view) = views.iter_mut().find(|view| view.name == member.list) {
                view.coins.push(member.coin);
            }
        }
        Ok(views)
    }

    /// Create an empty list (a no-op if it already exists).
    pub async fn create_list(&self, name: &str) -> Result<(), StoreError> {
        sqlx::query!("INSERT OR IGNORE INTO lists (name) VALUES (?)", name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete a list and its members.
    pub async fn delete_list(&self, name: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query!("DELETE FROM list_members WHERE list = ?", name)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM lists WHERE name = ?", name).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Add a coin to a list, creating the list if it does not exist.
    pub async fn add_to_list(&self, list: &str, coin: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query!("INSERT OR IGNORE INTO lists (name) VALUES (?)", list)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("INSERT OR IGNORE INTO list_members (list, coin) VALUES (?, ?)", list, coin)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Remove a coin from a list.
    pub async fn remove_from_list(&self, list: &str, coin: &str) -> Result<(), StoreError> {
        sqlx::query!("DELETE FROM list_members WHERE list = ? AND coin = ?", list, coin)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every cached (coin, svg) logo.
    pub async fn logos(&self) -> Result<Vec<(String, String)>, StoreError> {
        let rows = sqlx::query!("SELECT coin, svg FROM logos").fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|row| (row.coin, row.svg)).collect())
    }

    /// Coins whose logo is already cached.
    pub async fn logo_coins(&self) -> Result<HashSet<String>, StoreError> {
        let coins = sqlx::query_scalar!("SELECT coin FROM logos").fetch_all(&self.pool).await?;
        Ok(coins.into_iter().collect())
    }

    /// Cache a coin's SVG logo.
    pub async fn cache_logo(&self, coin: &str, svg: &str) -> Result<(), StoreError> {
        sqlx::query!("INSERT OR REPLACE INTO logos (coin, svg) VALUES (?, ?)", coin, svg)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Read a persisted UI/app setting (an opaque string — JSON or scalar), if set.
    pub async fn get_setting(&self, key: &str) -> Result<Option<String>, StoreError> {
        Ok(sqlx::query_scalar!("SELECT value FROM settings WHERE key = ?", key)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// Insert-or-replace a persisted UI/app setting.
    pub async fn set_setting(&self, key: &str, value: &str) -> Result<(), StoreError> {
        sqlx::query!("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", key, value)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every saved signal, sorted by name (case-insensitive) then id.
    pub async fn signals(&self) -> Result<Vec<SignalRecord>, StoreError> {
        Ok(sqlx::query_as!(
            SignalRecord,
            "SELECT id, name, definition FROM signals ORDER BY name COLLATE NOCASE, id"
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// One saved signal by stable id.
    pub async fn signal(&self, id: &str) -> Result<Option<SignalRecord>, StoreError> {
        Ok(
            sqlx::query_as!(SignalRecord, "SELECT id, name, definition FROM signals WHERE id = ?", id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Insert a new signal. Ids and names must both be unique.
    pub async fn create_signal(&self, id: &str, name: &str, definition: &str) -> Result<(), StoreError> {
        let result = sqlx::query!(
            "INSERT INTO signals (id, name, definition) VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING",
            id,
            name,
            definition,
        )
        .execute(&self.pool)
        .await
        .map_err(|error| map_signal_write_error(error, name))?;
        if result.rows_affected() == 0 {
            return Err(StoreError::DuplicateSignalId(id.to_owned()));
        }
        Ok(())
    }

    /// Update an existing signal without creating a missing row.
    pub async fn update_signal(&self, id: &str, name: &str, definition: &str) -> Result<(), StoreError> {
        let result = sqlx::query!("UPDATE signals SET name = ?, definition = ? WHERE id = ?", name, definition, id,)
            .execute(&self.pool)
            .await
            .map_err(|error| map_signal_write_error(error, name))?;
        if result.rows_affected() == 0 {
            return Err(StoreError::SignalNotFound(id.to_owned()));
        }
        Ok(())
    }

    /// Delete a saved signal by id (a no-op if it does not exist).
    pub async fn delete_signal(&self, id: &str) -> Result<(), StoreError> {
        sqlx::query!("DELETE FROM signals WHERE id = ?", id).execute(&self.pool).await?;
        Ok(())
    }

    /// Every indicator preset, sorted by indicator then name (case-insensitive).
    pub async fn indicator_presets(&self) -> Result<Vec<IndicatorPresetRecord>, StoreError> {
        Ok(sqlx::query_as!(
            IndicatorPresetRecord,
            "SELECT id, indicator_id, name, params FROM indicator_presets \
             ORDER BY indicator_id, name COLLATE NOCASE, id"
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// One indicator preset by stable id.
    pub async fn indicator_preset(&self, id: &str) -> Result<Option<IndicatorPresetRecord>, StoreError> {
        Ok(sqlx::query_as!(
            IndicatorPresetRecord,
            "SELECT id, indicator_id, name, params FROM indicator_presets WHERE id = ?",
            id
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Insert a new preset. Ids are globally unique; names are unique within one
    /// indicator.
    pub async fn create_indicator_preset(
        &self,
        id: &str,
        indicator_id: &str,
        name: &str,
        params: &str,
    ) -> Result<(), StoreError> {
        let result = sqlx::query!(
            "INSERT INTO indicator_presets (id, indicator_id, name, params) VALUES (?, ?, ?, ?) \
             ON CONFLICT(id) DO NOTHING",
            id,
            indicator_id,
            name,
            params,
        )
        .execute(&self.pool)
        .await
        .map_err(|error| map_preset_write_error(error, indicator_id, name))?;
        if result.rows_affected() == 0 {
            return Err(StoreError::DuplicatePresetId(id.to_owned()));
        }
        Ok(())
    }

    /// Update an existing preset while preserving its indicator identity.
    pub async fn update_indicator_preset(
        &self,
        id: &str,
        indicator_id: &str,
        name: &str,
        params: &str,
    ) -> Result<(), StoreError> {
        let result = sqlx::query!(
            "UPDATE indicator_presets SET name = ?, params = ? WHERE id = ? AND indicator_id = ?",
            name,
            params,
            id,
            indicator_id,
        )
        .execute(&self.pool)
        .await
        .map_err(|error| map_preset_write_error(error, indicator_id, name))?;
        if result.rows_affected() == 0 {
            return Err(StoreError::PresetNotFound(id.to_owned()));
        }
        Ok(())
    }

    /// Delete an indicator preset by id (a no-op if it does not exist).
    pub async fn delete_indicator_preset(&self, id: &str) -> Result<(), StoreError> {
        sqlx::query!("DELETE FROM indicator_presets WHERE id = ?", id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn map_signal_write_error(error: sqlx::Error, name: &str) -> StoreError {
    if error.as_database_error().is_some_and(|database| database.is_unique_violation()) {
        StoreError::DuplicateSignalName(name.to_owned())
    } else {
        StoreError::Database(error)
    }
}

fn map_preset_write_error(error: sqlx::Error, indicator_id: &str, name: &str) -> StoreError {
    if error.as_database_error().is_some_and(|database| database.is_unique_violation()) {
        StoreError::DuplicatePresetName {
            indicator_id: indicator_id.to_owned(),
            name: name.to_owned(),
        }
    } else {
        StoreError::Database(error)
    }
}

#[async_trait]
impl CandleStore for SqliteStore {
    async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query_as!(
            CandleRow,
            "SELECT open_time, open, high, low, close, volume FROM candles \
             WHERE coin = ? AND timeframe = ? AND open_time < ? \
             ORDER BY open_time DESC LIMIT ?",
            coin,
            timeframe.as_hl(),
            before_ms,
            limit,
        )
        .fetch_all(&self.pool)
        .await?;

        candles_oldest_first(rows)
    }

    async fn load_recent(&self, limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let groups = sqlx::query_as!(
            CandleGroupRow,
            "SELECT DISTINCT coin, timeframe FROM candles ORDER BY coin, timeframe"
        )
        .fetch_all(&self.pool)
        .await?;
        let mut series = Vec::with_capacity(groups.len());

        // Small primary-key range reads avoid both ranking the accumulated cache and
        // materializing every series' SQLx rows at once during startup.
        for group in groups {
            let Some(timeframe) = Timeframe::from_hl(&group.timeframe) else {
                continue;
            };
            let rows = sqlx::query_as!(
                CandleRow,
                "SELECT open_time, open, high, low, close, volume FROM candles \
                 WHERE coin = ? AND timeframe = ? ORDER BY open_time DESC LIMIT ?",
                group.coin,
                group.timeframe,
                limit,
            )
            .fetch_all(&self.pool)
            .await?;
            let candles = candles_oldest_first(rows)?;
            if !candles.is_empty() {
                series.push(CandleSeries {
                    coin: group.coin,
                    timeframe,
                    candles,
                });
            }
        }
        Ok(series)
    }

    async fn save(&self, coin: &str, timeframe: Timeframe, candles: &[Candle]) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        for candle in candles {
            let open_time = candle.open_time.timestamp_millis();
            let open = candle.open.to_string();
            let high = candle.high.to_string();
            let low = candle.low.to_string();
            let close = candle.close.to_string();
            let volume = candle.volume.to_string();
            sqlx::query!(
                "INSERT OR REPLACE INTO candles \
                 (coin, timeframe, open_time, open, high, low, close, volume) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                coin,
                timeframe.as_hl(),
                open_time,
                open,
                high,
                low,
                close,
                volume,
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};

    use super::{CandleRow, CandleSeries, CandleStore, IndicatorPresetRecord, SignalRecord, SqliteStore, StoreError};

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

    #[test]
    fn malformed_candle_rows_are_explicit_errors() {
        let row = CandleRow {
            open_time: 0,
            open: "not-a-decimal".to_owned(),
            high: "1".to_owned(),
            low: "1".to_owned(),
            close: "1".to_owned(),
            volume: "0".to_owned(),
        };
        assert!(matches!(Candle::try_from(row), Err(StoreError::InvalidCandleRow(0))));
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
    async fn load_recent_limits_each_series_oldest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cw.db");
        let store = SqliteStore::open(&path).await.expect("open");
        let btc_hourly = (1..=4)
            .map(|hour| candle(hour, "100", "101", "99", "100", "10"))
            .collect::<Vec<_>>();
        let btc_four_hour = (1..=3)
            .map(|hour| candle(hour, "200", "201", "199", "200", "20"))
            .collect::<Vec<_>>();
        let eth_hourly = vec![candle(1, "300", "301", "299", "300", "30")];
        store.save("BTC", Timeframe::Hour1, &btc_hourly).await.expect("save hourly BTC");
        store
            .save("BTC", Timeframe::Hour4, &btc_four_hour)
            .await
            .expect("save four-hour BTC");
        store.save("ETH", Timeframe::Hour1, &eth_hourly).await.expect("save hourly ETH");

        let loaded = store.load_recent(2).await.expect("load");
        assert_eq!(
            loaded,
            vec![
                CandleSeries {
                    coin: "BTC".to_owned(),
                    timeframe: Timeframe::Hour1,
                    candles: btc_hourly[2..].to_vec(),
                },
                CandleSeries {
                    coin: "BTC".to_owned(),
                    timeframe: Timeframe::Hour4,
                    candles: btc_four_hour[1..].to_vec(),
                },
                CandleSeries {
                    coin: "ETH".to_owned(),
                    timeframe: Timeframe::Hour1,
                    candles: eth_hourly,
                },
            ]
        );
        assert!(store.load_recent(0).await.expect("load zero").is_empty());
    }

    #[tokio::test]
    async fn preserves_a_corrupt_database_for_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cw.db");
        let original = b"this is not a sqlite database";
        std::fs::write(&path, original).expect("write garbage");

        assert!(SqliteStore::open(&path).await.is_err());
        assert_eq!(std::fs::read(&path).expect("read preserved database"), original);
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
    async fn settings_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open");
        assert_eq!(store.get_setting("chart.indicators").await.expect("get"), None);
        store.set_setting("chart.indicators", "[{\"id\":\"rsi\"}]").await.expect("set");
        store
            .set_setting("chart.indicators", "[{\"id\":\"macd\"}]")
            .await
            .expect("update");
        assert_eq!(
            store.get_setting("chart.indicators").await.expect("get"),
            Some("[{\"id\":\"macd\"}]".to_owned())
        );
    }

    #[tokio::test]
    async fn signals_create_update_and_delete_are_distinct() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open");
        assert!(store.signals().await.expect("empty catalog").is_empty());

        store
            .create_signal("signal-b", "Zeta", r#"{"version":1}"#)
            .await
            .expect("create zeta");
        store
            .create_signal("signal-a", "Alpha", r#"{"version":1}"#)
            .await
            .expect("create alpha");
        assert!(matches!(
            store.create_signal("signal-a", "Other", "{}").await,
            Err(StoreError::DuplicateSignalId(id)) if id == "signal-a"
        ));
        assert_eq!(
            store.signals().await.expect("list signals"),
            vec![
                SignalRecord {
                    id: "signal-a".to_owned(),
                    name: "Alpha".to_owned(),
                    definition: r#"{"version":1}"#.to_owned(),
                },
                SignalRecord {
                    id: "signal-b".to_owned(),
                    name: "Zeta".to_owned(),
                    definition: r#"{"version":1}"#.to_owned(),
                },
            ]
        );

        store
            .update_signal("signal-b", "Beta", r#"{"version":2}"#)
            .await
            .expect("update signal");
        assert_eq!(
            store.signal("signal-b").await.expect("get signal"),
            Some(SignalRecord {
                id: "signal-b".to_owned(),
                name: "Beta".to_owned(),
                definition: r#"{"version":2}"#.to_owned(),
            })
        );
        assert!(matches!(
            store.update_signal("missing", "Missing", "{}").await,
            Err(StoreError::SignalNotFound(id)) if id == "missing"
        ));

        let duplicate = store.create_signal("signal-c", "alpha", "{}").await;
        assert!(matches!(duplicate, Err(StoreError::DuplicateSignalName(name)) if name == "alpha"));
        let duplicate = store.update_signal("signal-b", "ALPHA", "{}").await;
        assert!(matches!(duplicate, Err(StoreError::DuplicateSignalName(name)) if name == "ALPHA"));

        store.delete_signal("signal-a").await.expect("delete signal");
        assert_eq!(store.signals().await.expect("list after delete").len(), 1);
    }

    #[tokio::test]
    async fn indicator_presets_create_update_and_delete_are_distinct() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open");

        store
            .create_indicator_preset("rsi-fast", "rsi", "Fast", r#"{"period":7}"#)
            .await
            .expect("create RSI");
        store
            .create_indicator_preset("ema-fast", "ema", "Fast", r#"{"period":10}"#)
            .await
            .expect("create EMA");
        assert!(matches!(
            store
                .create_indicator_preset("rsi-fast", "rsi", "Other", "{}")
                .await,
            Err(StoreError::DuplicatePresetId(id)) if id == "rsi-fast"
        ));
        assert_eq!(
            store.indicator_presets().await.expect("list presets"),
            vec![
                IndicatorPresetRecord {
                    id: "ema-fast".to_owned(),
                    indicator_id: "ema".to_owned(),
                    name: "Fast".to_owned(),
                    params: r#"{"period":10}"#.to_owned(),
                },
                IndicatorPresetRecord {
                    id: "rsi-fast".to_owned(),
                    indicator_id: "rsi".to_owned(),
                    name: "Fast".to_owned(),
                    params: r#"{"period":7}"#.to_owned(),
                },
            ]
        );
        assert_eq!(
            store.indicator_preset("rsi-fast").await.expect("get preset"),
            Some(IndicatorPresetRecord {
                id: "rsi-fast".to_owned(),
                indicator_id: "rsi".to_owned(),
                name: "Fast".to_owned(),
                params: r#"{"period":7}"#.to_owned(),
            })
        );

        store
            .update_indicator_preset("rsi-fast", "rsi", "Quick", r#"{"period":5}"#)
            .await
            .expect("update preset");
        assert!(matches!(
            store.update_indicator_preset("rsi-fast", "ema", "Wrong", "{}").await,
            Err(StoreError::PresetNotFound(id)) if id == "rsi-fast"
        ));
        assert!(matches!(
            store.update_indicator_preset("missing", "rsi", "Missing", "{}").await,
            Err(StoreError::PresetNotFound(id)) if id == "missing"
        ));
        let duplicate = store.create_indicator_preset("rsi-other", "rsi", "quick", "{}").await;
        assert!(matches!(
            duplicate,
            Err(StoreError::DuplicatePresetName { indicator_id, name })
                if indicator_id == "rsi" && name == "quick"
        ));

        store.delete_indicator_preset("rsi-fast").await.expect("delete preset");
        assert_eq!(store.indicator_presets().await.expect("list after delete").len(), 1);
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
        assert_eq!(
            page.first().map(|bar| bar.open_time),
            Some(candle(3, "1", "1", "1", "1", "0").open_time)
        );
        assert_eq!(
            page.last().map(|bar| bar.open_time),
            Some(candle(5, "1", "1", "1", "1", "0").open_time)
        );
    }
}
