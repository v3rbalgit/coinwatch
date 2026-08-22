//! Hyperliquid perps adapter over the free public `info` API (no auth).
//!
//! Verified in `research/spikes/hyperliquid/`: 232 perps, ~3y of clean daily
//! candles, hourly funding. This adapter provides the perp universe (ranked by
//! day volume) and candles (as canonical [`Bar`]s). Funding is added separately.
//!
//! The info API is weight-rate-limited per IP, so requests are paced with a
//! minimum interval and 429s are retried with backoff.

use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, Instrument, Interval, Network, PoolId, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;

use crate::{BarRequest, DataSource};

const ENDPOINT: &str = "https://api.hyperliquid.xyz/info";
const DEFAULT_MIN_INTERVAL: Duration = Duration::from_millis(500);
const MAX_RETRIES: u32 = 4;

/// Hyperliquid info-API adapter. Market data needs no auth.
#[derive(Clone)]
pub struct Hyperliquid {
    endpoint: String,
    client: reqwest::Client,
    min_interval: Duration,
    last_request: Arc<Mutex<Option<Instant>>>,
}

impl Default for Hyperliquid {
    fn default() -> Self {
        Self::new()
    }
}

impl Hyperliquid {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .user_agent("coinwatch/0.1")
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client builds with static config");
        Self {
            endpoint: ENDPOINT.to_string(),
            client,
            min_interval: DEFAULT_MIN_INTERVAL,
            last_request: Arc::new(Mutex::new(None)),
        }
    }

    /// Block until the next request is allowed under the min-interval budget.
    async fn pace(&self) {
        let wait = {
            let mut guard = self.last_request.lock().expect("pace mutex poisoned");
            let now = Instant::now();
            let target = match *guard {
                Some(prev) => (prev + self.min_interval).max(now),
                None => now,
            };
            *guard = Some(target);
            target.checked_duration_since(now)
        };
        if let Some(w) = wait {
            tokio::time::sleep(w).await;
        }
    }

    /// Paced POST of an info request, retrying 429/5xx with backoff.
    async fn post_json<T: DeserializeOwned>(&self, body: &serde_json::Value) -> Result<T> {
        let mut attempt = 0;
        loop {
            self.pace().await;
            match self.client.post(&self.endpoint).json(body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp
                            .json::<T>()
                            .await
                            .map_err(|e| CoreError::DataSource(format!("hyperliquid decode: {e}")));
                    }
                    if (status.as_u16() == 429 || status.is_server_error()) && attempt < MAX_RETRIES {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                        continue;
                    }
                    return Err(CoreError::DataSource(format!("hyperliquid http {status}")));
                }
                Err(e) => {
                    if attempt >= MAX_RETRIES {
                        return Err(CoreError::DataSource(format!("hyperliquid request: {e}")));
                    }
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                }
            }
        }
    }

    /// The perp universe ranked by 24h notional volume, filtered to a minimum,
    /// capped at `limit`. Each instrument is `network=hyperliquid, pool=<coin>`.
    pub async fn top_perps(&self, limit: usize, min_day_vol_usd: Decimal) -> Result<Vec<(Instrument, Decimal)>> {
        let (meta, ctxs): (Meta, Vec<AssetCtx>) =
            self.post_json(&json!({ "type": "metaAndAssetCtxs" })).await?;

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

        out.sort_by(|a, b| b.1.cmp(&a.1));
        out.truncate(limit);
        Ok(out)
    }
}

/// Hyperliquid candle interval + its millis-per-bar (for the start-time window).
fn interval_spec(iv: Interval) -> (&'static str, i64) {
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

#[async_trait]
impl DataSource for Hyperliquid {
    fn name(&self) -> &str {
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
            let bar = Bar {
                ts,
                open: to_decimal(&c.o),
                high: to_decimal(&c.h),
                low: to_decimal(&c.l),
                close: to_decimal(&c.c),
                volume: to_decimal(&c.v),
            };
            if bar.ohlc_consistent() {
                bars.push(bar);
            }
        }
        bars.sort_by_key(|b| b.ts);
        Ok(bars)
    }
}
