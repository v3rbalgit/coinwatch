//! Shared market domain types used across the market feed, indicators, and scanner.

mod candle;
mod timeframe;

pub use candle::Candle;
pub use timeframe::Timeframe;
