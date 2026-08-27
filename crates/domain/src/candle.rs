//! The canonical OHLCV candle.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::timeframe::Timeframe;

/// A single OHLCV candle. Prices are exchange-precise `Decimal`; consumers convert
/// closes to `f64` for the indicators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    /// Candle open time (UTC).
    pub open_time: DateTime<Utc>,
    /// Open price.
    pub open: Decimal,
    /// Highest price in the bar.
    pub high: Decimal,
    /// Lowest price in the bar.
    pub low: Decimal,
    /// Close price.
    pub close: Decimal,
    /// Volume in base-coin units.
    pub volume: Decimal,
}

/// Length of the fully-closed prefix of an oldest-first candle series at `at_ms`.
#[must_use]
pub fn completed_prefix_len(candles: &[Candle], timeframe: Timeframe, at_ms: i64) -> usize {
    candles
        .partition_point(|candle| candle.open_time.timestamp_millis().saturating_add(timeframe.duration_ms()) <= at_ms)
}
