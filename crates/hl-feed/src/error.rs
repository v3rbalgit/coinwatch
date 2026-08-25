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
}

impl From<tungstenite::Error> for FeedError {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}
