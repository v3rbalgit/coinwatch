//! Candle timeframes the setup-finder scans.

use strum::{EnumString, IntoStaticStr};

/// A higher-timeframe interval the setup-finder scans. String forms are
/// Hyperliquid's `candle` interval codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoStaticStr, EnumString)]
pub enum Timeframe {
    /// One hour.
    #[strum(serialize = "1h")]
    Hour1,
    /// Four hours.
    #[strum(serialize = "4h")]
    Hour4,
    /// One day.
    #[strum(serialize = "1d")]
    Day1,
    /// One week.
    #[strum(serialize = "1w")]
    Week1,
}

impl Timeframe {
    /// Hyperliquid's interval code.
    #[must_use]
    pub fn as_hl(self) -> &'static str {
        self.into()
    }

    /// Parse a Hyperliquid interval code.
    #[must_use]
    pub fn from_hl(interval: &str) -> Option<Self> {
        interval.parse().ok()
    }

    /// Nominal bar duration in milliseconds.
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

#[cfg(test)]
mod tests {
    use super::Timeframe;

    #[test]
    fn hl_codes_round_trip() {
        for (timeframe, code) in [
            (Timeframe::Hour1, "1h"),
            (Timeframe::Hour4, "4h"),
            (Timeframe::Day1, "1d"),
            (Timeframe::Week1, "1w"),
        ] {
            assert_eq!(timeframe.as_hl(), code);
            assert_eq!(Timeframe::from_hl(code), Some(timeframe));
        }
        assert_eq!(Timeframe::from_hl("2h"), None);
    }
}
