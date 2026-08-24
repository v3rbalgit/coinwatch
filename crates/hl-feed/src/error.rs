//! Hyperliquid feed errors.

use thiserror::Error;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub(crate) enum FeedError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    WebSocket(Box<tungstenite::Error>),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("malformed Hyperliquid decimal: {0}")]
    Decimal(String),
    #[error("candle timestamp out of range: {0}")]
    Timestamp(i64),
}

impl From<tungstenite::Error> for FeedError {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}
