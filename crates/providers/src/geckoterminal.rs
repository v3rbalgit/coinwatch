//! GeckoTerminal / CoinGecko on-chain adapter.
//!
//! Phase-0 validated this feed: clean daily OHLCV for liquid EVM-L2 pools,
//! ~6-month history on the keyless tier (see `research/spikes/gecko_ohlcv/`).
//!
//! Endpoint: `GET /networks/{network}/pools/{pool}/ohlcv/{timeframe}` returning
//! `data.attributes.ohlcv_list` as rows of `[ts, open, high, low, close, volume]`.

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cw_core::{Bar, CoreError, Decimal, Interval, Result};
use serde::Deserialize;

use crate::{BarRequest, DataSource};

/// Per-call bar cap enforced by the API.
const MAX_LIMIT: usize = 1000;

/// Adapter configuration. An `api_key` of `None` uses the keyless tier
/// (aggressively rate-limited — pace requests accordingly).
#[derive(Debug, Clone)]
pub struct GeckoTerminal {
    pub base_url: String,
    pub api_key: Option<String>,
    client: reqwest::Client,
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

        let mut request = self
            .client
            .get(&url)
            .query(&[
                ("aggregate", "1"),
                ("limit", limit.as_str()),
                ("currency", "usd"),
            ])
            .header("Accept", "application/json");
        if let Some(key) = &self.api_key {
            request = request.header("x-cg-pro-api-key", key);
        }

        let resp = request
            .send()
            .await
            .map_err(|e| CoreError::DataSource(format!("request: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(CoreError::DataSource(format!("http {status} for {url}")));
        }

        let parsed: OhlcvResponse = resp
            .json()
            .await
            .map_err(|e| CoreError::DataSource(format!("decode: {e}")))?;

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
