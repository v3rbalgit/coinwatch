//! Candle timeframes the setup-finder scans.

/// A higher-timeframe interval the setup-finder scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timeframe {
    /// One hour.
    Hour1,
    /// Four hours — the primary intraday timeframe.
    Hour4,
    /// One day.
    Day1,
    /// One week.
    Week1,
}

impl Timeframe {
    /// Hyperliquid's interval string.
    #[must_use]
    pub const fn as_hl(self) -> &'static str {
        match self {
            Self::Hour1 => "1h",
            Self::Hour4 => "4h",
            Self::Day1 => "1d",
            Self::Week1 => "1w",
        }
    }

    /// Nominal bar duration in milliseconds (for backfill windows).
    #[must_use]
    pub const fn duration_ms(self) -> i64 {
        match self {
            Self::Hour1 => 3_600_000,
            Self::Hour4 => 14_400_000,
            Self::Day1 => 86_400_000,
            Self::Week1 => 604_800_000,
        }
    }
}
