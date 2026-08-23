//! Hyperliquid perps adapter over the free public `info` API (no auth).
//!
//! Verified in `research/spikes/hyperliquid/`: 232 perps, ~3y of clean daily
//! candles, hourly funding. This adapter provides the perp universe (ranked by
//! day volume) and candles (as canonical [`Bar`]s). Funding is added separately.
//!
//! The info API is weight-rate-limited per IP — and its fundingHistory endpoint
//! throttles bursty pagination hard — so requests go through a conservative
//! `Pacer` that spaces them under that throttle and backs off on 429/5xx, honoring
//! a `Retry-After` header.

use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, FundingPoint, Instrument, Interval, Network, PoolId, Result};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::{BarRequest, DataSource};

const ENDPOINT: &str = "https://api.hyperliquid.xyz/info";
const MAX_RETRIES: u32 = 4;

/// Adaptive request pacer (AIMD). Starts optimistic, additively speeds up on each
/// success, and multiplicatively backs off on 429/5xx — honoring a `Retry-After`
/// header when present. This converges near the server's true sustainable rate
/// instead of a hand-tuned fixed interval: HL's aggregate info limit is generous
/// (~1200/min), but its fundingHistory endpoint throttles bursty pagination, and the
/// pacer discovers that per-endpoint ceiling on its own.
struct Pacer {
    /// Earliest instant the next request may start (reserved under the lock).
    next_allowed: Mutex<Instant>,
}

impl Pacer {
    /// Fixed spacing that stays under HL's fundingHistory throttle. Probing for more
    /// throughput there is counterproductive — 429 retries cost far more than the
    /// spacing saves — so a safe constant plus `Retry-After` is both simpler and
    /// faster than an adaptive prober for this API.
    const INTERVAL: Duration = Duration::from_millis(1100);
    /// Fallback pause on a 429 that carries no `Retry-After` header.
    const DEFAULT_BACKOFF: Duration = Duration::from_secs(2);

    fn new() -> Self {
        Self {
            next_allowed: Mutex::new(Instant::now()),
        }
    }

    #[expect(clippy::expect_used, reason = "poisoned pacer mutex is an unrecoverable bug")]
    fn lock(&self) -> std::sync::MutexGuard<'_, Instant> {
        self.next_allowed.lock().expect("pacer mutex poisoned")
    }

    /// Reserve and wait for the next request slot.
    async fn acquire(&self) {
        let wait = {
            let mut na = self.lock();
            let now = Instant::now();
            let at = (*na).max(now);
            *na = at + Self::INTERVAL;
            at.checked_duration_since(now)
        };
        if let Some(w) = wait {
            tokio::time::sleep(w).await;
        }
    }

    /// Throttle (429/5xx): hold new requests for the server's `Retry-After` (or a
    /// default backoff) before the next attempt.
    fn on_throttle(&self, retry_after: Option<Duration>) {
        let backoff = retry_after.unwrap_or(Self::DEFAULT_BACKOFF);
        let mut na = self.lock();
        *na = (*na).max(Instant::now() + backoff);
    }
}

/// Parse a numeric `Retry-After` header (delta-seconds) into a [`Duration`].
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let secs = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(secs))
}

/// Hyperliquid info-API adapter. Market data needs no auth.
#[derive(Clone)]
pub struct Hyperliquid {
    endpoint: String,
    client: reqwest::Client,
    pacer: Arc<Pacer>,
}

impl Default for Hyperliquid {
    fn default() -> Self {
        Self::new()
    }
}

impl Hyperliquid {
    /// Construct a paced Hyperliquid `info`-API client with sane defaults.
    #[must_use]
    pub fn new() -> Self {
        #[expect(clippy::expect_used, reason = "infallible static reqwest builder config")]
        let client = reqwest::Client::builder()
            .user_agent("coinwatch/0.1")
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client builds with static config");
        Self {
            endpoint: ENDPOINT.to_string(),
            client,
            pacer: Arc::new(Pacer::new()),
        }
    }

    /// Paced POST of an info request. The [`Pacer`] handles inter-request spacing and
    /// AIMD backoff; on 429/5xx we record the throttle (honoring `Retry-After`) and
    /// retry until [`MAX_RETRIES`]. Network errors retry with a plain backoff.
    async fn post_json<T: DeserializeOwned>(&self, body: &serde_json::Value) -> Result<T> {
        let mut attempt = 0;
        loop {
            self.pacer.acquire().await;
            match self.client.post(&self.endpoint).json(body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp
                            .json::<T>()
                            .await
                            .map_err(|e| CoreError::DataSource(format!("hyperliquid decode: {e}")));
                    }
                    if status.as_u16() == 429 || status.is_server_error() {
                        self.pacer.on_throttle(parse_retry_after(resp.headers()));
                        if attempt < MAX_RETRIES {
                            attempt += 1;
                            continue;
                        }
                    }
                    return Err(CoreError::DataSource(format!("hyperliquid http {status}")));
                }
                Err(e) => {
                    if attempt >= MAX_RETRIES {
                        return Err(CoreError::DataSource(format!("hyperliquid request: {e}")));
                    }
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(2 * u64::from(attempt))).await;
                }
            }
        }
    }

    /// The perp universe ranked by 24h notional volume, filtered to a minimum,
    /// capped at `limit`. Each instrument is `network=hyperliquid, pool=<coin>`.
    pub async fn top_perps(&self, limit: usize, min_day_vol_usd: Decimal) -> Result<Vec<(Instrument, Decimal)>> {
        let (meta, ctxs): (Meta, Vec<AssetCtx>) = self.post_json(&json!({ "type": "metaAndAssetCtxs" })).await?;

        let mut out: Vec<(Instrument, Decimal)> = meta
            .universe
            .iter()
            .zip(ctxs.iter())
            .map(|(u, c)| {
                let day_vol = to_decimal(&c.day_ntl_vlm);
                let inst = Instrument {
                    network: Network::Other("hyperliquid".to_string()),
                    pool: PoolId(u.name.clone()),
                    name: u.name.clone(),
                    token: None,
                };
                (inst, day_vol)
            })
            .filter(|(_, vol)| *vol >= min_day_vol_usd)
            .collect();

        out.sort_by_key(|a| std::cmp::Reverse(a.1));
        out.truncate(limit);
        Ok(out)
    }

    /// Hourly funding history for `coin` since `since`, paginated (the API caps
    /// at ~500 points per call), deduped and oldest-first.
    pub async fn fetch_funding(&self, coin: &str, since: DateTime<Utc>) -> Result<Vec<FundingPoint>> {
        let now = Utc::now().timestamp_millis();
        let mut start = since.timestamp_millis();
        let mut out: Vec<FundingPoint> = Vec::new();
        loop {
            let body = json!({ "type": "fundingHistory", "coin": coin, "startTime": start, "endTime": now });
            let rows: Vec<FundingRow> = self.post_json(&body).await?;
            if rows.is_empty() {
                break;
            }
            let page = rows.len();
            let last_time = rows.iter().map(|r| r.time).max().unwrap_or(start);
            for r in rows {
                let time = DateTime::<Utc>::from_timestamp_millis(r.time)
                    .ok_or_else(|| CoreError::InvalidData(format!("funding ts out of range: {}", r.time)))?;
                out.push(FundingPoint {
                    time,
                    funding_rate: Decimal::from_str(&r.funding_rate).unwrap_or_default(),
                    premium: Decimal::from_str(&r.premium).unwrap_or_default(),
                });
            }
            if page < 500 || last_time >= now {
                break; // last page
            }
            start = last_time + 1;
        }
        out.sort_by_key(|p| p.time);
        out.dedup_by_key(|p| p.time);
        Ok(out)
    }
}

/// Hyperliquid candle interval + its millis-per-bar (for the start-time window).
const fn interval_spec(iv: Interval) -> (&'static str, i64) {
    match iv {
        Interval::Day => ("1d", 86_400_000),
        Interval::Hour => ("1h", 3_600_000),
        Interval::Minute => ("1m", 60_000),
    }
}

/// Convert a JSON numeric/string cell to [`Decimal`] via its text form.
fn to_decimal(v: &serde_json::Value) -> Decimal {
    match v {
        serde_json::Value::String(s) => Decimal::from_str(s).unwrap_or_default(),
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).unwrap_or_default(),
        _ => Decimal::ZERO,
    }
}

#[derive(Deserialize)]
struct Meta {
    universe: Vec<UnivItem>,
}
#[derive(Deserialize)]
struct UnivItem {
    name: String,
}
#[derive(Deserialize)]
struct AssetCtx {
    #[serde(rename = "dayNtlVlm", default)]
    day_ntl_vlm: serde_json::Value,
}

#[derive(Deserialize)]
struct Candle {
    t: i64,
    o: serde_json::Value,
    h: serde_json::Value,
    l: serde_json::Value,
    c: serde_json::Value,
    #[serde(default)]
    v: serde_json::Value,
}

#[derive(Deserialize)]
struct FundingRow {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(default)]
    premium: String,
    time: i64,
}

#[async_trait]
impl DataSource for Hyperliquid {
    fn name(&self) -> &'static str {
        "hyperliquid"
    }

    async fn fetch_bars(&self, req: &BarRequest) -> Result<Vec<Bar>> {
        let (interval, ms_per) = interval_spec(req.interval);
        let now = Utc::now().timestamp_millis();
        let start = now - (req.limit.max(1) as i64) * ms_per;
        let body = json!({
            "type": "candleSnapshot",
            "req": { "coin": req.instrument.pool.0, "interval": interval, "startTime": start, "endTime": now },
        });
        let candles: Vec<Candle> = self.post_json(&body).await?;

        let mut bars = Vec::with_capacity(candles.len());
        for c in &candles {
            let ts = DateTime::<Utc>::from_timestamp_millis(c.t)
                .ok_or_else(|| CoreError::InvalidData(format!("hyperliquid ts out of range: {}", c.t)))?;
            // HL candle volume `v` is in base-coin units; store USD volume
            // (v * close) so the backtester's depth/liquidity math is unit-correct.
            let close = to_decimal(&c.c);
            let bar = Bar {
                ts,
                open: to_decimal(&c.o),
                high: to_decimal(&c.h),
                low: to_decimal(&c.l),
                close,
                volume: to_decimal(&c.v) * close,
            };
            if bar.ohlc_consistent() {
                bars.push(bar);
            }
        }
        bars.sort_by_key(|b| b.ts);
        Ok(bars)
    }
}
