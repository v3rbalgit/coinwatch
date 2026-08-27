//! Candle timeframes supported by the market-data and signal layers.

use serde::{Deserialize, Deserializer, Serialize};
use strum::{EnumString, IntoStaticStr};

/// A supported market-data interval. String forms are Hyperliquid's `candle`
/// interval codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, IntoStaticStr, EnumString, Serialize)]
#[serde(into = "&'static str")]
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

// Deserialization must accept the same interval codes `Serialize` emits (the
// Hyperliquid codes, not variant names). Strum provides `FromStr` but no serde
// integration in this version, so this bridge delegates to that generated parser.
impl<'de> Deserialize<'de> for Timeframe {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let code = String::deserialize(deserializer)?;
        code.parse().map_err(serde::de::Error::custom)
    }
}

impl Timeframe {
    /// Every supported timeframe in ascending duration order.
    pub const ALL: [Self; 4] = [Self::Hour1, Self::Hour4, Self::Day1, Self::Week1];

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

    /// UTC open time of the bar containing `timestamp_ms`.
    ///
    /// Hyperliquid aligns intervals to Unix-epoch multiples. This yields UTC
    /// hour/day boundaries and its Thursday-anchored weekly boundary.
    #[must_use]
    pub const fn bar_open_ms(self, timestamp_ms: i64) -> i64 {
        let duration = self.duration_ms();
        timestamp_ms.div_euclid(duration).saturating_mul(duration)
    }

    /// Whether `open_ms` lies exactly on this timeframe's bar grid.
    #[must_use]
    pub const fn is_aligned(self, open_ms: i64) -> bool {
        open_ms.rem_euclid(self.duration_ms()) == 0
    }
}

#[cfg(test)]
mod tests {
    use super::Timeframe;

    #[test]
    fn ord_and_all_follow_ascending_duration() {
        for pair in Timeframe::ALL.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].duration_ms() < pair[1].duration_ms());
            assert_eq!(pair[1].duration_ms() % pair[0].duration_ms(), 0);
        }
    }

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

    #[test]
    fn bar_opens_share_the_epoch_aligned_grid() {
        let hour = Timeframe::Hour1.duration_ms();
        let day = Timeframe::Day1.duration_ms();
        assert_eq!(Timeframe::Hour4.bar_open_ms(5 * hour), 4 * hour);
        assert_eq!(Timeframe::Day1.bar_open_ms(day + hour), day);
        assert_eq!(Timeframe::Week1.bar_open_ms(6 * day), 0);
        assert_eq!(Timeframe::Week1.bar_open_ms(8 * day), 7 * day);
        assert!(Timeframe::Week1.is_aligned(7 * day));
        assert!(!Timeframe::Week1.is_aligned(7 * day + hour));
    }
}
