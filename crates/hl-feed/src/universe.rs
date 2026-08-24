//! The perp universe: the coin list the scanner sweeps, ranked by day volume.

use serde::Deserialize;

use crate::candle::INFO_URL;

type FetchError = Box<dyn std::error::Error + Send + Sync>;

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

/// Fetch the perp universe ranked by day volume (descending), keeping coins with
/// at least `min_day_volume_usd` and capping at `limit`. Reuses the caller's
/// `client`. Requires a process-wide rustls provider (the binary installs it).
pub async fn fetch_universe(
    client: &reqwest::Client,
    limit: usize,
    min_day_volume_usd: f64,
) -> Result<Vec<UniverseCoin>, FetchError> {
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
    use super::fetch_universe;

    /// Live smoke test: the universe comes back non-empty and volume-ranked.
    #[tokio::test]
    async fn fetches_ranked_universe() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::new();
        let coins = fetch_universe(&client, 20, 1_000_000.0).await.expect("fetch universe");
        assert!(!coins.is_empty(), "expected some coins");
        assert!(
            coins.windows(2).all(|pair| matches!(pair, [a, b] if a.day_volume_usd >= b.day_volume_usd)),
            "universe must be ranked by day volume descending",
        );
    }
}
