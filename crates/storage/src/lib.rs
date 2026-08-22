//! `cw-storage` — the canonical bar store behind a [`Store`] trait.
//!
//! Point-in-time correctness is a storage responsibility: [`Store::load_bars`]
//! takes an explicit `[from, to]` window so callers cannot accidentally read
//! future bars. The Phase-1 backend is DuckDB/Parquet (research); a
//! Postgres/Timescale backend can implement the same trait for scale.

use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Instrument, Interval, Result};

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
    fn upsert_bars(
        &self,
        instrument: &Instrument,
        interval: Interval,
        bars: &[Bar],
    ) -> Result<usize>;

    /// Load bars for `(instrument, interval)` within `range` (point-in-time).
    fn load_bars(
        &self,
        instrument: &Instrument,
        interval: Interval,
        range: TimeRange,
    ) -> Result<Vec<Bar>>;
}

/// DuckDB/Parquet-backed store. Phase-1 skeleton: wiring only.
pub struct DuckStore {
    /// Path to the DuckDB file / Parquet dataset root.
    pub path: String,
}

impl DuckStore {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

impl Store for DuckStore {
    fn upsert_bars(&self, _i: &Instrument, _iv: Interval, _bars: &[Bar]) -> Result<usize> {
        Err(CoreError::Unimplemented("DuckStore::upsert_bars"))
    }

    fn load_bars(&self, _i: &Instrument, _iv: Interval, _r: TimeRange) -> Result<Vec<Bar>> {
        Err(CoreError::Unimplemented("DuckStore::load_bars"))
    }
}
