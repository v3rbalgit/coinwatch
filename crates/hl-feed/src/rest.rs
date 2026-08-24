//! Hyperliquid REST calls: candle backfill and the universe/metadata snapshot.
//!
//! These seed the cache once and refresh the slow-moving parts; the live updates
//! come over the WebSocket in [`stream`](crate::stream).

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use serde::Deserialize;

use crate::error::FeedError;
use crate::ratelimit::WeightLimiter;

const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const META_WEIGHT: u32 = 20;

fn candle_weight(limit: usize) -> u32 {
    20 + u32::try_from(limit.div_ceil(60)).unwrap_or(200)
}

/// Fetch up to `limit` most-recent candles for `coin` at `timeframe`, oldest-first,
/// reusing the caller's `client`. Requires a process-wide rustls provider.
pub(crate) async fn fetch_candles(
    client: &reqwest::Client,
    limiter: &WeightLimiter,
    coin: &str,
    timeframe: Timeframe,
    limit: usize,
) -> Result<Vec<Candle>, FeedError> {
    let end = Utc::now().timestamp_millis();
    let bars = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
    let start = end - timeframe.duration_ms().saturating_mul(bars);
    limiter.acquire(candle_weight(limit)).await;
    request_candles(client, coin, timeframe, start, end).await
}

/// Fetch `count` candles for `coin` at `timeframe` ending at `end_ms`, oldest-first —
/// for on-demand history paging on the detail chart (called rarely, so unmetered).
pub async fn fetch_candle_range(
    client: &reqwest::Client,
    coin: &str,
    timeframe: Timeframe,
    end_ms: i64,
    count: usize,
) -> Result<Vec<Candle>, FeedError> {
    let bars = i64::try_from(count.max(1)).unwrap_or(i64::MAX);
    let start = end_ms - timeframe.duration_ms().saturating_mul(bars);
    request_candles(client, coin, timeframe, start, end_ms).await
}

/// Post a `candleSnapshot` for `[start, end]` and parse the oldest-first candles.
async fn request_candles(
    client: &reqwest::Client,
    coin: &str,
    timeframe: Timeframe,
    start: i64,
    end: i64,
) -> Result<Vec<Candle>, FeedError> {
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
        let open_time = DateTime::<Utc>::from_timestamp_millis(bar.t).ok_or(FeedError::Timestamp(bar.t))?;
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

/// Parse a Hyperliquid decimal (string, or bare number as a fallback).
pub(crate) fn to_decimal(value: &serde_json::Value) -> Result<rust_decimal::Decimal, FeedError> {
    match value {
        serde_json::Value::String(text) => text.parse().map_err(|_| FeedError::Decimal(text.clone())),
        serde_json::Value::Number(number) => number
            .as_f64()
            .and_then(|value| rust_decimal::Decimal::try_from(value).ok())
            .ok_or_else(|| FeedError::Decimal(number.to_string())),
        other => Err(FeedError::Decimal(other.to_string())),
    }
}

/// A perp in the tradable universe with a live metadata snapshot from
/// `metaAndAssetCtxs`.
#[derive(Debug, Clone)]
pub struct UniverseCoin {
    /// Coin symbol (e.g. `BTC`).
    pub coin: String,
    /// Trailing 24h notional volume in USD.
    pub day_volume_usd: f64,
    /// Latest mark price.
    pub mark_px: Option<f64>,
    /// Previous-day price (for the 24h change).
    pub prev_day_px: Option<f64>,
    /// Current funding rate (per hour).
    pub funding: Option<f64>,
    /// Open interest in base-coin units.
    pub open_interest: Option<f64>,
}

/// Fetch the perp universe ranked by day volume (descending), keeping non-delisted
/// coins with at least `min_day_volume_usd` and capping at `limit`.
pub(crate) async fn fetch_universe(
    client: &reqwest::Client,
    limiter: &WeightLimiter,
    limit: usize,
    min_day_volume_usd: f64,
) -> Result<Vec<UniverseCoin>, FeedError> {
    limiter.acquire(META_WEIGHT).await;
    let (meta, contexts): (Meta, Vec<AssetContext>) = client
        .post(INFO_URL)
        .json(&serde_json::json!({ "type": "metaAndAssetCtxs" }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut coins: Vec<UniverseCoin> = meta
        .universe
        .into_iter()
        .zip(contexts)
        .filter_map(|(entry, context)| {
            if entry.is_delisted {
                return None;
            }
            let day_volume_usd = value_to_f64(&context.day_ntl_vlm)?;
            if day_volume_usd < min_day_volume_usd {
                return None;
            }
            Some(UniverseCoin {
                coin: entry.name,
                day_volume_usd,
                mark_px: context.mark_px.as_ref().and_then(value_to_f64),
                prev_day_px: context.prev_day_px.as_ref().and_then(value_to_f64),
                funding: context.funding.as_ref().and_then(value_to_f64),
                open_interest: context.open_interest.as_ref().and_then(value_to_f64),
            })
        })
        .collect();

    coins.sort_by(|a, b| b.day_volume_usd.total_cmp(&a.day_volume_usd));
    coins.truncate(limit);
    Ok(coins)
}

/// Hyperliquid reports volumes as JSON strings; tolerate a bare number too.
fn value_to_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::String(text) => text.parse().ok(),
        serde_json::Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

#[derive(Deserialize)]
struct Meta {
    universe: Vec<UniverseEntry>,
}

#[derive(Deserialize)]
struct UniverseEntry {
    name: String,
    #[serde(rename = "isDelisted", default)]
    is_delisted: bool,
}

#[derive(Deserialize)]
struct AssetContext {
    #[serde(rename = "dayNtlVlm")]
    day_ntl_vlm: serde_json::Value,
    #[serde(rename = "markPx")]
    mark_px: Option<serde_json::Value>,
    #[serde(rename = "prevDayPx")]
    prev_day_px: Option<serde_json::Value>,
    funding: Option<serde_json::Value>,
    #[serde(rename = "openInterest")]
    open_interest: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use domain::Timeframe;

    use super::{WeightLimiter, fetch_candles, fetch_universe};

    #[tokio::test]
    async fn fetches_live_btc_candles() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::new();
        let limiter = WeightLimiter::new(1100);
        let candles = fetch_candles(&client, &limiter, "BTC", Timeframe::Hour1, 50)
            .await
            .expect("fetch BTC 1h candles");
        assert!(!candles.is_empty(), "expected candles back");
        for candle in &candles {
            assert!(candle.high >= candle.low, "high below low");
            assert!(candle.high >= candle.open && candle.high >= candle.close, "high not the max");
            assert!(candle.low <= candle.open && candle.low <= candle.close, "low not the min");
        }
    }

    #[tokio::test]
    async fn fetches_ranked_universe() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::new();
        let limiter = WeightLimiter::new(1100);
        let coins = fetch_universe(&client, &limiter, 20, 1_000_000.0)
            .await
            .expect("fetch universe");
        assert!(!coins.is_empty(), "expected some coins");
        assert!(
            coins
                .windows(2)
                .all(|pair| matches!(pair, [a, b] if a.day_volume_usd >= b.day_volume_usd)),
            "universe must be ranked by day volume descending",
        );
    }
}
