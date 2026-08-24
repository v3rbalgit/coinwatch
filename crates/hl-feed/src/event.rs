//! Typed market events published by the adapter onto a [`MarketFeed`](crate::MarketFeed).

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use rust_decimal::Decimal;

/// A market event for one coin. Prices flow continuously; candles arrive as the
/// forming bar updates and when a bar closes.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// Latest mid price.
    Mid(MidEvent),
    /// A new or updated candle for a (coin, timeframe).
    Candle(CandleEvent),
}

impl MarketEvent {
    /// The coin this event concerns.
    #[must_use]
    pub fn symbol(&self) -> &str {
        match self {
            Self::Mid(event) => &event.symbol,
            Self::Candle(event) => &event.coin,
        }
    }
}

/// A mid-price update.
#[derive(Debug, Clone)]
pub struct MidEvent {
    /// Coin symbol (e.g. `BTC`; Hyperliquid spot pairs arrive `@`-prefixed).
    pub symbol: String,
    /// Mid price.
    pub price: Decimal,
    /// When the update was received.
    pub timestamp: DateTime<Utc>,
}

/// A streamed candle update for a (coin, timeframe).
#[derive(Debug, Clone)]
pub struct CandleEvent {
    /// Coin symbol.
    pub coin: String,
    /// Timeframe of the candle.
    pub timeframe: Timeframe,
    /// The candle itself.
    pub candle: Candle,
}
