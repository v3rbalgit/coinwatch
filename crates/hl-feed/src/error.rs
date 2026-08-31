//! Hyperliquid feed errors.

use thiserror::Error;
use tokio_tungstenite::tungstenite;

/// An error from a Hyperliquid REST or WebSocket interaction.
#[derive(Debug, Error)]
pub enum FeedError {
    /// An HTTP request failed.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// A WebSocket transport error.
    #[error(transparent)]
    WebSocket(Box<tungstenite::Error>),
    /// A response body could not be parsed as JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A Hyperliquid decimal field was malformed.
    #[error("malformed Hyperliquid decimal: {0}")]
    Decimal(String),
    /// A candle timestamp was outside the representable range.
    #[error("candle timestamp out of range: {0}")]
    Timestamp(i64),
    /// A requested candle range cannot be represented in epoch milliseconds.
    #[error("candle request range is outside the supported timestamp range")]
    CandleRange,
    /// Desired WebSocket subscriptions exceed Hyperliquid's per-IP capacity.
    #[error("websocket subscription limit exceeded: requested {requested}, limit {limit}")]
    SubscriptionLimit {
        /// Total base and candle subscriptions requested.
        requested: usize,
        /// Hyperliquid's maximum subscriptions per IP.
        limit: usize,
    },
}

impl From<tungstenite::Error> for FeedError {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}
