//! The canonical OHLCV candle.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// A single OHLCV candle. Prices are exchange-precise `Decimal`; consumers convert
/// closes to `f64` for the indicators.
#[derive(Debug, Clone)]
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
