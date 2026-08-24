//! Candle backfill over Hyperliquid's REST `candleSnapshot`.
//!
//! Fetches recent OHLCV for a (coin, timeframe) to seed the buffers the scanner
//! runs indicators on. Live candle updates over the WebSocket come later, as a
//! `MarketEvent::Candle` variant.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

pub(crate) const INFO_URL: &str = "https://api.hyperliquid.xyz/info";

/// A higher-timeframe interval the setup-finder scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// Nominal bar duration in milliseconds (for the backfill window).
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

/// A single OHLCV candle. Prices are exchange-precise `Decimal`; the scanner
/// converts closes to `f64` for the indicators.
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

type FetchError = Box<dyn std::error::Error + Send + Sync>;

/// Fetch up to `limit` most-recent candles for `coin` at `timeframe`, oldest-first,
/// reusing the caller's `client` (the scanner shares one across the universe).
///
/// Requires a process-wide rustls provider (the binary installs it).
pub async fn fetch_candles(
    client: &reqwest::Client,
    coin: &str,
    timeframe: Timeframe,
    limit: usize,
) -> Result<Vec<Candle>, FetchError> {
    let end = Utc::now().timestamp_millis();
    let bars = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
    let start = end - timeframe.duration_ms().saturating_mul(bars);

    let body = serde_json::json!({
        "type": "candleSnapshot",
        "req": { "coin": coin, "interval": timeframe.as_hl(), "startTime": start, "endTime": end },
    });

    let raw: Vec<RawCandle> = client
        .post(INFO_URL)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut candles = Vec::with_capacity(raw.len());
    for bar in &raw {
        let open_time = DateTime::<Utc>::from_timestamp_millis(bar.t)
            .ok_or_else(|| FetchError::from(format!("candle timestamp out of range: {}", bar.t)))?;
        candles.push(Candle {
            open_time,
            open: to_decimal(&bar.o)?,
            high: to_decimal(&bar.h)?,
            low: to_decimal(&bar.l)?,
            close: to_decimal(&bar.c)?,
            volume: to_decimal(&bar.v)?,
        });
    }
    Ok(candles)
}

/// Hyperliquid candle fields (`o`/`h`/`l`/`c`/`v` arrive as JSON strings).
#[derive(Deserialize)]
struct RawCandle {
    t: i64,
    o: serde_json::Value,
    h: serde_json::Value,
    l: serde_json::Value,
    c: serde_json::Value,
    v: serde_json::Value,
}

fn to_decimal(value: &serde_json::Value) -> Result<Decimal, FetchError> {
    match value {
        serde_json::Value::String(text) => {
            text.parse().map_err(|_| FetchError::from(format!("malformed decimal: {text}")))
        }
        serde_json::Value::Number(number) => number
            .as_f64()
            .and_then(|float| Decimal::try_from(float).ok())
            .ok_or_else(|| FetchError::from(format!("malformed number: {number}"))),
        other => Err(FetchError::from(format!("expected a number, got {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{Timeframe, fetch_candles};

    /// Live smoke test: fetch recent BTC 1h candles and check OHLC consistency.
    /// Requires network access.
    #[tokio::test]
    async fn fetches_live_btc_candles() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::new();
        let candles = fetch_candles(&client, "BTC", Timeframe::Hour1, 50)
            .await
            .expect("fetch BTC 1h candles");

        assert!(!candles.is_empty(), "expected candles back");
        for candle in &candles {
            assert!(candle.high >= candle.low, "high below low");
            assert!(candle.high >= candle.open && candle.high >= candle.close, "high not the max");
            assert!(candle.low <= candle.open && candle.low <= candle.close, "low not the min");
        }
    }
}
