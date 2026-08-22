//! GeckoTerminal / CoinGecko on-chain adapter.
//!
//! Phase-0 validated this feed: clean daily OHLCV for liquid EVM-L2 pools,
//! ~6-month history on the keyless tier (see `research/spikes/gecko_ohlcv/`).
//! Phase-1 skeleton: config + trait wiring; the HTTP client is added when the
//! ingester is implemented.

use async_trait::async_trait;
use cw_core::{Bar, CoreError, Result};

use crate::{BarRequest, DataSource};

/// Adapter configuration. An `api_key` of `None` uses the keyless tier
/// (aggressively rate-limited — pace requests accordingly).
#[derive(Debug, Clone)]
pub struct GeckoTerminal {
    pub base_url: String,
    pub api_key: Option<String>,
}

impl Default for GeckoTerminal {
    fn default() -> Self {
        Self {
            base_url: "https://api.geckoterminal.com/api/v2".to_string(),
            api_key: None,
        }
    }
}

#[async_trait]
impl DataSource for GeckoTerminal {
    fn name(&self) -> &str {
        "geckoterminal"
    }

    async fn fetch_bars(&self, _req: &BarRequest) -> Result<Vec<Bar>> {
        Err(CoreError::Unimplemented("GeckoTerminal::fetch_bars"))
    }
}
