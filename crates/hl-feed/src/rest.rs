//! Hyperliquid REST calls: candle backfill and the universe/metadata snapshot.
//!
//! These seed every candle series, repair detected stream gaps, and reconcile any
//! interval that has not advanced after a bar boundary. Configured live intervals
//! normally update first over the WebSocket in [`stream`](crate::stream).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use serde::Deserialize;

use crate::error::FeedError;
use crate::history::CandleProvider;
use crate::ratelimit::WeightLimiter;

const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const META_WEIGHT: u32 = 20;
const CANDLE_BASE_WEIGHT: u32 = 20;
const PERP_MAX_PRICE_DECIMALS: u32 = 6;
const MAX_PRICE_SIGNIFICANT_DIGITS: i32 = 5;
/// Operational candle request ceiling. Pagination advances from the actual rows
/// returned, so correctness does not depend on the provider honoring this size.
pub(crate) const CANDLE_PAGE_SIZE: usize = 2_000;

/// Shared Hyperliquid REST client and process-wide request-weight budget.
pub struct HyperliquidRest {
    client: reqwest::Client,
    limiter: WeightLimiter,
}

impl HyperliquidRest {
    /// Wrap a reusable HTTP client with the configured rolling-minute weight budget.
    #[must_use]
    pub const fn new(client: reqwest::Client, weight_budget_per_min: u32) -> Self {
        Self {
            client,
            limiter: WeightLimiter::new(weight_budget_per_min),
        }
    }

    pub(crate) async fn recent_candles(
        &self,
        coin: &str,
        timeframe: Timeframe,
        limit: usize,
    ) -> Result<Vec<Candle>, FeedError> {
        let limit = limit.clamp(1, CANDLE_PAGE_SIZE);
        let end = Utc::now().timestamp_millis();
        let preceding = i64::try_from(limit - 1).unwrap_or(i64::MAX);
        let start = timeframe
            .bar_open_ms(end)
            .saturating_sub(timeframe.duration_ms().saturating_mul(preceding));
        self.limiter.acquire(candle_weight(limit)).await;
        request_candles(&self.client, coin, timeframe, start, end).await
    }

    pub(crate) async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        count: usize,
    ) -> Result<Vec<Candle>, FeedError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let count = count.min(CANDLE_PAGE_SIZE);
        let duration = timeframe.duration_ms();
        let end = before_ms.checked_sub(duration).ok_or(FeedError::CandleRange)?;
        let preceding = i64::try_from(count - 1).map_err(|_| FeedError::CandleRange)?;
        let span = duration.checked_mul(preceding).ok_or(FeedError::CandleRange)?;
        let start = end.checked_sub(span).ok_or(FeedError::CandleRange)?;
        self.limiter.acquire(candle_weight(count)).await;
        request_candles(&self.client, coin, timeframe, start, end).await
    }

    pub(crate) async fn universe(&self, limit: usize, min_day_volume_usd: f64) -> Result<Vec<UniverseCoin>, FeedError> {
        self.limiter.acquire(META_WEIGHT).await;
        let (meta, contexts): (Meta, Vec<AssetContext>) = self
            .client
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
                let mark_px = context.mark_px.as_ref().and_then(value_to_f64);
                Some(UniverseCoin {
                    coin: entry.name,
                    day_volume_usd,
                    mark_px,
                    price_decimals: price_decimals(mark_px, entry.size_decimals),
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
}

#[async_trait]
impl CandleProvider for HyperliquidRest {
    async fn candles_before(
        &self,
        coin: &str,
        timeframe: Timeframe,
        before_ms: i64,
        count: usize,
    ) -> Result<Vec<Candle>, FeedError> {
        HyperliquidRest::candles_before(self, coin, timeframe, before_ms, count).await
    }
}

fn candle_weight(limit: usize) -> u32 {
    CANDLE_BASE_WEIGHT.saturating_add(u32::try_from(limit.max(1).div_ceil(60)).unwrap_or(u32::MAX))
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

/// Resolve the current Hyperliquid tick precision from its five-significant-digit
/// rule, capped by the asset's size-dependent decimal limit.
fn price_decimals(mark_px: Option<f64>, size_decimals: u32) -> u32 {
    let maximum = PERP_MAX_PRICE_DECIMALS.saturating_sub(size_decimals);
    let Some(price) = mark_px.filter(|price| price.is_finite() && *price > 0.0) else {
        return maximum;
    };
    let magnitude = price.log10().floor() as i32;
    let significant = (MAX_PRICE_SIGNIFICANT_DIGITS - 1 - magnitude).max(0);
    maximum.min(u32::try_from(significant).unwrap_or(maximum))
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
    /// Decimal places needed for the current valid price increment.
    pub price_decimals: u32,
    /// Previous-day price (for the 24h change).
    pub prev_day_px: Option<f64>,
    /// Current funding rate (per hour).
    pub funding: Option<f64>,
    /// Open interest in base-coin units.
    pub open_interest: Option<f64>,
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
    #[serde(rename = "szDecimals")]
    size_decimals: u32,
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

    use super::{CANDLE_PAGE_SIZE, HyperliquidRest, candle_weight, price_decimals};

    #[tokio::test]
    #[ignore = "live network"]
    async fn fetches_live_btc_candles() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let rest = HyperliquidRest::new(reqwest::Client::new(), 1100);
        let candles = rest
            .recent_candles("BTC", Timeframe::Hour1, 50)
            .await
            .expect("fetch BTC 1h candles");
        assert_eq!(candles.len(), 50, "requested window should not include an extra overlap bar");
        for candle in &candles {
            assert!(candle.high >= candle.low, "high below low");
            assert!(candle.high >= candle.open && candle.high >= candle.close, "high not the max");
            assert!(candle.low <= candle.open && candle.low <= candle.close, "low not the min");
        }
    }

    #[test]
    fn candle_weight_reserves_for_every_requested_row() {
        assert_eq!(CANDLE_PAGE_SIZE, 2_000);
        assert_eq!(candle_weight(0), 21);
        assert_eq!(candle_weight(60), 21);
        assert_eq!(candle_weight(61), 22);
        assert_eq!(candle_weight(CANDLE_PAGE_SIZE), 54);
        assert_eq!(candle_weight(usize::MAX), u32::MAX);
    }

    #[test]
    fn price_precision_follows_hyperliquid_tick_rules() {
        assert_eq!(price_decimals(Some(0.010_238), 0), 6);
        assert_eq!(price_decimals(Some(2_451.0), 4), 1);
        assert_eq!(price_decimals(Some(78_743.0), 5), 0);
        assert_eq!(price_decimals(Some(0.000_000_1), 0), 6);
        assert_eq!(price_decimals(None, 3), 3);
        assert_eq!(price_decimals(Some(f64::NAN), 3), 3);
    }

    #[tokio::test]
    #[ignore = "live network"]
    async fn fetches_ranked_universe() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let rest = HyperliquidRest::new(reqwest::Client::new(), 1100);
        let coins = rest.universe(20, 1_000_000.0).await.expect("fetch universe");
        assert!(!coins.is_empty(), "expected some coins");
        assert!(
            coins
                .windows(2)
                .all(|pair| matches!(pair, [a, b] if a.day_volume_usd >= b.day_volume_usd)),
            "universe must be ranked by day volume descending",
        );
    }
}
