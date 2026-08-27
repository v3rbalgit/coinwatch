//! Shared market domain types used across the market feed, indicators, and signals.

mod candle;
mod timeframe;

pub use candle::{Candle, completed_prefix_len};
pub use timeframe::Timeframe;
