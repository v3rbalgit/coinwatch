//! GeckoTerminal / CoinGecko on-chain adapter.
//!
//! Phase-0 validated this feed: clean daily OHLCV for liquid EVM-L2 pools,
//! ~6-month history on the keyless tier (see `research/spikes/gecko_ohlcv/`).
//! The keyless tier is aggressively rate-limited, so the adapter paces its own
//! requests with a configurable minimum interval shared across both endpoints.
//!
//! Endpoints used:
//! - `GET /networks/{net}/pools?page=N` — universe discovery.
//! - `GET /networks/{net}/pools/{pool}/ohlcv/{tf}` — bars, rows of
//!   `[ts, open, high, low, close, volume]`.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, Instrument, Interval, Network, PoolId, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::{BarRequest, DataSource, PoolInfo, UniverseRequest, UniverseSource};

/// Per-call bar cap enforced by the API.
const MAX_LIMIT: usize = 1000;
/// Default minimum spacing between requests on the keyless tier. The keyless
/// throttle is burst-then-cap; ~5s steady spacing tested reliable in Phase 0.
const DEFAULT_MIN_INTERVAL: Duration = Duration::from_millis(5000);
/// Retries for throttled/transient responses before giving up.
const MAX_RETRIES: u32 = 5;

/// Exponential backoff for retry `attempt` (1-based), capped at 30s.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs((1u64 << attempt).min(30))
}

/// Adapter configuration. An `api_key` of `None` uses the keyless tier.
#[derive(Debug, Clone)]
pub struct GeckoTerminal {
    pub base_url: String,
    pub api_key: Option<String>,
    client: reqwest::Client,
    min_interval: Duration,
    /// Last *scheduled* request time; shared across clones so all callers of one
    /// adapter share a single rate budget.
    last_request: Arc<Mutex<Option<Instant>>>,
}

impl Default for GeckoTerminal {
    fn default() -> Self {
        Self::new("https://api.geckoterminal.com/api/v2", None)
    }
}

impl GeckoTerminal {
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("coinwatch/0.1")
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client builds with static config");
        Self {
            base_url: base_url.into(),
            api_key,
            client,
            min_interval: DEFAULT_MIN_INTERVAL,
            last_request: Arc::new(Mutex::new(None)),
        }
    }

    /// Override the minimum spacing between requests (e.g. tighter with an API key).
    pub fn with_min_interval(mut self, interval: Duration) -> Self {
        self.min_interval = interval;
        self
    }

    /// Block until this request is allowed under the min-interval budget.
    /// Reserves the slot while holding the lock, then sleeps without it (so the
    /// guard is never held across an `.await`).
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

    /// Paced GET returning deserialized JSON, retrying throttled (429) and
    /// transient (5xx / network) responses with backoff (honoring `Retry-After`).
    async fn get_json<T: DeserializeOwned>(&self, url: &str, query: &[(&str, &str)]) -> Result<T> {
        let mut attempt = 0;
        loop {
            self.pace().await;
            let mut request = self.client.get(url).query(query).header("Accept", "application/json");
            if let Some(key) = &self.api_key {
                request = request.header("x-cg-pro-api-key", key);
            }
            match request.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp
                            .json::<T>()
                            .await
                            .map_err(|e| CoreError::DataSource(format!("decode: {e}")));
                    }
                    let retryable = status.as_u16() == 429 || status.is_server_error();
                    if !retryable || attempt >= MAX_RETRIES {
                        return Err(CoreError::DataSource(format!("http {status} for {url}")));
                    }
                    attempt += 1;
                    let retry_after = resp
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(Duration::from_secs);
                    tokio::time::sleep(retry_after.unwrap_or_else(|| backoff(attempt))).await;
                }
                Err(e) => {
                    if attempt >= MAX_RETRIES {
                        return Err(CoreError::DataSource(format!("request: {e}")));
                    }
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt)).await;
                }
            }
        }
    }
}

/// Provider-specific interval → timeframe path segment (aggregate is always 1 here).
fn timeframe(iv: Interval) -> &'static str {
    match iv {
        Interval::Day => "day",
        Interval::Hour => "hour",
        Interval::Minute => "minute",
    }
}

/// Convert a JSON numeric/string cell to [`Decimal`] via its text form to avoid
/// binary-float drift.
fn to_decimal(v: &serde_json::Value) -> Result<Decimal> {
    let s = match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        other => return Err(CoreError::InvalidData(format!("expected number, got {other}"))),
    };
    Decimal::from_str(&s).map_err(|e| CoreError::InvalidData(format!("decimal '{s}': {e}")))
}

/// Optional decimal from an optional JSON value; absent/invalid → `None`.
fn opt_decimal(v: &Option<serde_json::Value>) -> Option<Decimal> {
    v.as_ref().and_then(|x| to_decimal(x).ok())
}

// ---- OHLCV ----------------------------------------------------------------

#[derive(Deserialize)]
struct OhlcvResponse {
    data: OhlcvData,
}
#[derive(Deserialize)]
struct OhlcvData {
    attributes: OhlcvAttributes,
}
#[derive(Deserialize)]
struct OhlcvAttributes {
    #[serde(default)]
    ohlcv_list: Vec<Vec<serde_json::Value>>,
}

// ---- Pools (universe) -----------------------------------------------------

#[derive(Deserialize)]
struct PoolsResponse {
    #[serde(default)]
    data: Vec<PoolEntry>,
}
#[derive(Deserialize)]
struct PoolEntry {
    attributes: PoolAttrs,
    #[serde(default)]
    relationships: PoolRels,
}
#[derive(Deserialize)]
struct PoolAttrs {
    address: String,
    #[serde(default)]
    name: String,
    reserve_in_usd: Option<serde_json::Value>,
    #[serde(default)]
    volume_usd: VolumeUsd,
}
#[derive(Deserialize, Default)]
struct VolumeUsd {
    #[serde(default)]
    h24: Option<serde_json::Value>,
}
#[derive(Deserialize, Default)]
struct PoolRels {
    #[serde(default)]
    base_token: Option<TokenRel>,
}
#[derive(Deserialize)]
struct TokenRel {
    data: Option<TokenData>,
}
#[derive(Deserialize)]
struct TokenData {
    id: String,
}

#[async_trait]
impl DataSource for GeckoTerminal {
    fn name(&self) -> &str {
        "geckoterminal"
    }

    async fn fetch_bars(&self, req: &BarRequest) -> Result<Vec<Bar>> {
        let limit = req.limit.clamp(1, MAX_LIMIT).to_string();
        let url = format!(
            "{}/networks/{}/pools/{}/ohlcv/{}",
            self.base_url,
            req.instrument.network.slug(),
            req.instrument.pool.0,
            timeframe(req.interval),
        );
        let parsed: OhlcvResponse = self
            .get_json(&url, &[("aggregate", "1"), ("limit", limit.as_str()), ("currency", "usd")])
            .await?;

        let mut bars = Vec::with_capacity(parsed.data.attributes.ohlcv_list.len());
        for row in &parsed.data.attributes.ohlcv_list {
            if row.len() < 6 {
                return Err(CoreError::InvalidData(format!("short ohlcv row: {row:?}")));
            }
            let ts_secs = row[0]
                .as_i64()
                .ok_or_else(|| CoreError::InvalidData(format!("bad timestamp: {}", row[0])))?;
            let ts = DateTime::<Utc>::from_timestamp(ts_secs, 0)
                .ok_or_else(|| CoreError::InvalidData(format!("timestamp out of range: {ts_secs}")))?;
            let bar = Bar {
                ts,
                open: to_decimal(&row[1])?,
                high: to_decimal(&row[2])?,
                low: to_decimal(&row[3])?,
                close: to_decimal(&row[4])?,
                volume: to_decimal(&row[5])?,
            };
            if !bar.ohlc_consistent() {
                return Err(CoreError::InvalidData(format!("OHLC violation at {ts}")));
            }
            bars.push(bar);
        }
        bars.sort_by_key(|b| b.ts); // provider returns newest-first; store oldest-first
        Ok(bars)
    }
}

#[async_trait]
impl UniverseSource for GeckoTerminal {
    async fn top_pools(&self, network: &Network, req: &UniverseRequest) -> Result<Vec<PoolInfo>> {
        let url = format!("{}/networks/{}/pools", self.base_url, network.slug());
        let mut out: Vec<PoolInfo> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        for page in 1..=req.pages.max(1) {
            let resp: PoolsResponse = self.get_json(&url, &[("page", page.to_string().as_str())]).await?;
            if resp.data.is_empty() {
                break;
            }
            for entry in resp.data {
                let reserve = opt_decimal(&entry.attributes.reserve_in_usd).unwrap_or_default();
                if reserve < req.min_reserve_usd {
                    continue;
                }
                // Dedupe by base token so we don't collect five WETH/x pools.
                let base_id = entry
                    .relationships
                    .base_token
                    .and_then(|t| t.data)
                    .map(|d| d.id)
                    .unwrap_or_else(|| entry.attributes.address.clone());
                if !seen.insert(base_id) {
                    continue;
                }
                out.push(PoolInfo {
                    instrument: Instrument {
                        network: network.clone(),
                        pool: PoolId(entry.attributes.address),
                        name: entry.attributes.name,
                    },
                    reserve_usd: reserve,
                    volume_24h_usd: opt_decimal(&entry.attributes.volume_usd.h24).unwrap_or_default(),
                });
                if out.len() >= req.limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }
}
