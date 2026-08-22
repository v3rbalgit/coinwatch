//! Bitquery v2 on-chain adapter (OAuth Bearer auth).
//!
//! Verified in `research/spikes/bitquery/`: the free tier serves the `realtime`
//! dataset only (~4-day lookback); historical depth needs a paid per-chain archive
//! add-on — but the *query is identical*, so this adapter works for both. It
//! aggregates DEX swaps server-side into per-day flow (trade count, USD volume,
//! unique buyers) via `DEXTradeByTokens`.

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use cw_core::{CoreError, Decimal, Network, OnchainDaily, Result};
use serde::Deserialize;

use crate::OnchainSource;

const ENDPOINT: &str = "https://streaming.bitquery.io/graphql";

/// Bitquery v2 GraphQL adapter. Auth is an OAuth access token passed as a
/// Bearer header. The token is read from config/env by the caller, never here.
#[derive(Clone)]
pub struct Bitquery {
    endpoint: String,
    token: String,
    client: reqwest::Client,
}

impl Bitquery {
    pub fn new(token: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("coinwatch/0.1")
            .timeout(Duration::from_secs(60))
            .build()
            .expect("reqwest client builds with static config");
        Self {
            endpoint: ENDPOINT.to_string(),
            token: token.into(),
            client,
        }
    }
}

// ---- response shapes ------------------------------------------------------

#[derive(Deserialize)]
struct GqlResponse {
    #[serde(default)]
    data: Option<GqlData>,
    #[serde(default)]
    errors: Option<Vec<GqlError>>,
}
#[derive(Deserialize)]
struct GqlError {
    message: String,
}
#[derive(Deserialize)]
struct GqlData {
    #[serde(rename = "EVM")]
    evm: Option<EvmData>,
}
#[derive(Deserialize)]
struct EvmData {
    #[serde(rename = "DEXTradeByTokens", default)]
    rows: Vec<FlowRow>,
}
#[derive(Deserialize)]
struct FlowRow {
    day: DayField,
    // Bitquery returns aggregates as JSON strings (sometimes numbers); accept both.
    trades: serde_json::Value,
    #[serde(rename = "volumeUsd")]
    volume_usd: serde_json::Value,
    #[serde(rename = "buyVol", default)]
    buy_vol: serde_json::Value,
    #[serde(rename = "sellVol", default)]
    sell_vol: serde_json::Value,
    buyers: serde_json::Value,
}
#[derive(Deserialize)]
struct DayField {
    #[serde(rename = "Date")]
    date: String,
}

fn val_i64(v: &serde_json::Value) -> i64 {
    match v {
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        serde_json::Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
        _ => 0,
    }
}

fn val_decimal(v: &serde_json::Value) -> Decimal {
    match v {
        serde_json::Value::String(s) => Decimal::from_str(s).unwrap_or_default(),
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).unwrap_or_default(),
        _ => Decimal::ZERO,
    }
}

fn parse_day(s: &str) -> Result<DateTime<Utc>> {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| CoreError::InvalidData(format!("bitquery date '{s}': {e}")))?;
    let dt = d
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| CoreError::InvalidData(format!("bitquery date '{s}': bad time")))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

#[async_trait]
impl OnchainSource for Bitquery {
    fn name(&self) -> &str {
        "bitquery"
    }

    async fn daily_flow(&self, network: &Network, token: &str, since: DateTime<Utc>) -> Result<Vec<OnchainDaily>> {
        // Network is an enum literal in the schema, so it's inlined; the token and
        // time bound stay parameterized (safe string/DateTime variables).
        let query = format!(
            r#"query ($token: String!, $since: DateTime!) {{
  EVM(network: {net}) {{
    DEXTradeByTokens(
      where: {{Trade: {{Currency: {{SmartContract: {{is: $token}}}}}}, Block: {{Time: {{since: $since}}}}}}
      orderBy: {{ascendingByField: "day_Date"}}
    ) {{
      day: Block {{ Date }}
      trades: count
      volumeUsd: sum(of: Trade_AmountInUSD)
      buyVol: sum(of: Trade_AmountInUSD, if: {{Trade: {{Side: {{Type: {{is: buy}}}}}}}})
      sellVol: sum(of: Trade_AmountInUSD, if: {{Trade: {{Side: {{Type: {{is: sell}}}}}}}})
      buyers: uniq(of: Trade_Buyer)
    }}
  }}
}}"#,
            net = network.slug()
        );
        let body = serde_json::json!({
            "query": query,
            "variables": { "token": token, "since": since.to_rfc3339() },
        });

        let resp = self
            .client
            .post(&self.endpoint)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| CoreError::DataSource(format!("bitquery request: {e}")))?;
        let status = resp.status();
        let payload: GqlResponse = resp
            .json()
            .await
            .map_err(|e| CoreError::DataSource(format!("bitquery decode ({status}): {e}")))?;

        if let Some(errors) = payload.errors.filter(|e| !e.is_empty()) {
            let msg = errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join("; ");
            return Err(CoreError::DataSource(format!("bitquery graphql ({status}): {msg}")));
        }

        let rows = payload.data.and_then(|d| d.evm).map(|e| e.rows).unwrap_or_default();
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            out.push(OnchainDaily {
                date: parse_day(&r.day.date)?,
                trades: val_i64(&r.trades),
                volume_usd: val_decimal(&r.volume_usd),
                unique_buyers: val_i64(&r.buyers),
                buy_volume_usd: val_decimal(&r.buy_vol),
                sell_volume_usd: val_decimal(&r.sell_vol),
            });
        }
        Ok(out)
    }
}
