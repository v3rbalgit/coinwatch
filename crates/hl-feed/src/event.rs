//! Typed market events published by an adapter onto a [`MarketFeed`](crate::MarketFeed).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// A market event for one symbol. New variants are added as streams are wired
/// (trades, order book, candles, funding).
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// Latest mid price for the symbol.
    Mid(MidEvent),
}

impl MarketEvent {
    /// The symbol this event concerns.
    #[must_use]
    pub fn symbol(&self) -> &str {
        match self {
            Self::Mid(event) => &event.symbol,
        }
    }
}

/// A mid-price update from Hyperliquid's `allMids` stream.
#[derive(Debug, Clone)]
pub struct MidEvent {
    /// Coin symbol (e.g. `BTC`; Hyperliquid spot pairs arrive `@`-prefixed).
    pub symbol: String,
    /// Mid price.
    pub price: Decimal,
    /// When the update was received.
    pub timestamp: DateTime<Utc>,
}
