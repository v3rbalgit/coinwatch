//! `cw-storage` — the canonical bar store behind a [`Store`] trait.
//!
//! Point-in-time correctness is a storage responsibility: [`Store::load_bars`]
//! takes an explicit `[from, to]` window so callers cannot accidentally read
//! future bars. The Phase-1 backend is DuckDB (the shared research artifact);
//! a Postgres/Timescale backend can implement the same trait for scale.
//!
//! OHLCV values are stored as text to preserve exact [`Decimal`] precision across
//! the huge dynamic range of on-chain prices (memecoin dust → blue-chip). Bar
//! timestamps are stored as epoch milliseconds (BIGINT) for unambiguous ordering.

use std::str::FromStr;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, Instrument, Interval, Network, OnchainDaily, PoolId, Result};
use duckdb::{params, Connection};

/// A time window `[from, to]` (inclusive) used for point-in-time reads.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// Persistence seam for canonical bars. Implementations must be idempotent so
/// continuous ingestion can re-run a window without creating duplicates.
pub trait Store {
    /// Insert-or-replace bars for `(instrument, interval)`. Returns rows written.
    fn upsert_bars(&self, instrument: &Instrument, interval: Interval, bars: &[Bar]) -> Result<usize>;

    /// Load bars for `(instrument, interval)` within `range` (point-in-time).
    fn load_bars(&self, instrument: &Instrument, interval: Interval, range: TimeRange) -> Result<Vec<Bar>>;

    /// Record an instrument in the registry (idempotent) so the analysis side
    /// can reconstruct the universe from the store alone (offline research).
    fn register_instrument(&self, instrument: &Instrument) -> Result<()>;

    /// All registered instruments (the persisted universe).
    fn list_instruments(&self) -> Result<Vec<Instrument>>;

    /// Insert-or-replace daily on-chain aggregates for a token (idempotent).
    fn upsert_onchain(&self, network: &Network, token: &str, records: &[OnchainDaily]) -> Result<usize>;

    /// Load daily on-chain aggregates for a token within `range` (point-in-time).
    fn load_onchain(&self, network: &Network, token: &str, range: TimeRange) -> Result<Vec<OnchainDaily>>;
}

/// Stable string tag for an interval (matches the on-disk primary key).
fn interval_tag(iv: Interval) -> &'static str {
    match iv {
        Interval::Minute => "minute",
        Interval::Hour => "hour",
        Interval::Day => "day",
    }
}

/// DuckDB-backed store. The connection is wrapped in a `Mutex` so `&self`
/// methods are usable across threads (DuckDB `Connection` is `Send` not `Sync`).
pub struct DuckStore {
    pub path: String,
    conn: Mutex<Connection>,
}

impl DuckStore {
    /// Open (or create) a DuckDB database at `path`, or `":memory:"` for an
    /// ephemeral store. Ensures the `bars` schema exists.
    pub fn open(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(&path)
        }
        .map_err(|e| CoreError::Storage(format!("open {path}: {e}")))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS bars (
                network  VARCHAR NOT NULL,
                pool     VARCHAR NOT NULL,
                interval VARCHAR NOT NULL,
                ts       BIGINT  NOT NULL,
                open     VARCHAR NOT NULL,
                high     VARCHAR NOT NULL,
                low      VARCHAR NOT NULL,
                close    VARCHAR NOT NULL,
                volume   VARCHAR NOT NULL,
                PRIMARY KEY (network, pool, interval, ts)
            );
            CREATE TABLE IF NOT EXISTS instruments (
                network VARCHAR NOT NULL,
                pool    VARCHAR NOT NULL,
                name    VARCHAR NOT NULL,
                token   VARCHAR,
                PRIMARY KEY (network, pool)
            );
            CREATE TABLE IF NOT EXISTS onchain_daily (
                network       VARCHAR NOT NULL,
                token         VARCHAR NOT NULL,
                date          BIGINT  NOT NULL,
                trades        BIGINT  NOT NULL,
                volume_usd    VARCHAR NOT NULL,
                unique_buyers BIGINT  NOT NULL,
                PRIMARY KEY (network, token, date)
            );",
        )
        .map_err(|e| CoreError::Storage(format!("schema: {e}")))?;
        // Best-effort migration for stores created before the token column existed.
        let _ = conn.execute("ALTER TABLE instruments ADD COLUMN token VARCHAR", []);

        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }
}

impl Store for DuckStore {
    fn upsert_bars(&self, instrument: &Instrument, interval: Interval, bars: &[Bar]) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Storage(format!("lock: {e}")))?;
        let net = instrument.network.slug();
        let pool = instrument.pool.0.as_str();
        let tag = interval_tag(interval);

        let mut stmt = conn
            .prepare("INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?,?)")
            .map_err(|e| CoreError::Storage(format!("prepare upsert: {e}")))?;

        let mut written = 0usize;
        for b in bars {
            stmt.execute(params![
                net,
                pool,
                tag,
                b.ts.timestamp_millis(),
                b.open.to_string(),
                b.high.to_string(),
                b.low.to_string(),
                b.close.to_string(),
                b.volume.to_string(),
            ])
            .map_err(|e| CoreError::Storage(format!("upsert: {e}")))?;
            written += 1;
        }
        Ok(written)
    }

    fn load_bars(&self, instrument: &Instrument, interval: Interval, range: TimeRange) -> Result<Vec<Bar>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Storage(format!("lock: {e}")))?;
        let mut stmt = conn
            .prepare(
                "SELECT ts, open, high, low, close, volume FROM bars
                 WHERE network = ? AND pool = ? AND interval = ? AND ts BETWEEN ? AND ?
                 ORDER BY ts",
            )
            .map_err(|e| CoreError::Storage(format!("prepare load: {e}")))?;

        let raw = stmt
            .query_map(
                params![
                    instrument.network.slug(),
                    instrument.pool.0.as_str(),
                    interval_tag(interval),
                    range.from.timestamp_millis(),
                    range.to.timestamp_millis(),
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .map_err(|e| CoreError::Storage(format!("query: {e}")))?;

        let dec = |s: &str| -> Result<Decimal> {
            Decimal::from_str(s).map_err(|e| CoreError::InvalidData(format!("decimal '{s}': {e}")))
        };

        let mut out = Vec::new();
        for r in raw {
            let (ts_ms, open, high, low, close, volume) =
                r.map_err(|e| CoreError::Storage(format!("row: {e}")))?;
            let ts = DateTime::<Utc>::from_timestamp_millis(ts_ms)
                .ok_or_else(|| CoreError::InvalidData(format!("timestamp out of range: {ts_ms}")))?;
            out.push(Bar {
                ts,
                open: dec(&open)?,
                high: dec(&high)?,
                low: dec(&low)?,
                close: dec(&close)?,
                volume: dec(&volume)?,
            });
        }
        Ok(out)
    }

    fn register_instrument(&self, instrument: &Instrument) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| CoreError::Storage(format!("lock: {e}")))?;
        conn.execute(
            "INSERT OR REPLACE INTO instruments VALUES (?,?,?,?)",
            params![
                instrument.network.slug(),
                instrument.pool.0.as_str(),
                instrument.name.as_str(),
                instrument.token.as_deref()
            ],
        )
        .map_err(|e| CoreError::Storage(format!("register: {e}")))?;
        Ok(())
    }

    fn list_instruments(&self) -> Result<Vec<Instrument>> {
        let conn = self.conn.lock().map_err(|e| CoreError::Storage(format!("lock: {e}")))?;
        let mut stmt = conn
            .prepare("SELECT network, pool, name, token FROM instruments ORDER BY network, name")
            .map_err(|e| CoreError::Storage(format!("prepare list: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(|e| CoreError::Storage(format!("query: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            let (network, pool, name, token) = r.map_err(|e| CoreError::Storage(format!("row: {e}")))?;
            out.push(Instrument {
                network: Network::from_slug(&network),
                pool: PoolId(pool),
                name,
                token,
            });
        }
        Ok(out)
    }

    fn upsert_onchain(&self, network: &Network, token: &str, records: &[OnchainDaily]) -> Result<usize> {
        let conn = self.conn.lock().map_err(|e| CoreError::Storage(format!("lock: {e}")))?;
        let mut stmt = conn
            .prepare("INSERT OR REPLACE INTO onchain_daily VALUES (?,?,?,?,?,?)")
            .map_err(|e| CoreError::Storage(format!("prepare onchain upsert: {e}")))?;
        let mut written = 0usize;
        for r in records {
            stmt.execute(params![
                network.slug(),
                token,
                r.date.timestamp_millis(),
                r.trades,
                r.volume_usd.to_string(),
                r.unique_buyers,
            ])
            .map_err(|e| CoreError::Storage(format!("onchain upsert: {e}")))?;
            written += 1;
        }
        Ok(written)
    }

    fn load_onchain(&self, network: &Network, token: &str, range: TimeRange) -> Result<Vec<OnchainDaily>> {
        let conn = self.conn.lock().map_err(|e| CoreError::Storage(format!("lock: {e}")))?;
        let mut stmt = conn
            .prepare(
                "SELECT date, trades, volume_usd, unique_buyers FROM onchain_daily
                 WHERE network = ? AND token = ? AND date BETWEEN ? AND ?
                 ORDER BY date",
            )
            .map_err(|e| CoreError::Storage(format!("prepare onchain load: {e}")))?;
        let rows = stmt
            .query_map(
                params![network.slug(), token, range.from.timestamp_millis(), range.to.timestamp_millis()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(|e| CoreError::Storage(format!("onchain query: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            let (date_ms, trades, vol, buyers) = r.map_err(|e| CoreError::Storage(format!("row: {e}")))?;
            let date = DateTime::<Utc>::from_timestamp_millis(date_ms)
                .ok_or_else(|| CoreError::InvalidData(format!("date out of range: {date_ms}")))?;
            out.push(OnchainDaily {
                date,
                trades,
                volume_usd: Decimal::from_str(&vol)
                    .map_err(|e| CoreError::InvalidData(format!("decimal '{vol}': {e}")))?,
                unique_buyers: buyers,
            });
        }
        Ok(out)
    }
}
