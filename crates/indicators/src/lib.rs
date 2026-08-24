//! Technical indicators and divergence detection over price/OHLCV series.
//!
//! Pure, synchronous math — no I/O and no async. Callers feed close-price slices
//! (converted from the feed's candle buffers) and receive indicator series and
//! detected divergences. Divergence detection is generic over any oscillator, so
//! RSI and MACD share one detector.

mod atr;
mod bollinger;
mod cci;
mod divergence;
mod donchian;
mod macd;
mod mfi;
mod obv;
mod pivot;
mod rsi;
mod sma;
mod stochastic;
mod vwap;
mod williams;
mod wma;

pub use atr::atr;
pub use bollinger::{Bollinger, bollinger};
pub use cci::cci;
pub use divergence::{Divergence, DivergenceDirection, DivergenceKind, detect_divergences};
pub use donchian::{Donchian, donchian};
pub use macd::{Macd, ema, macd};
pub use mfi::mfi;
pub use obv::obv;
pub use pivot::{Pivot, PivotKind, pivots};
pub use rsi::rsi;
pub use sma::sma;
pub use stochastic::{Stochastic, stochastic};
pub use vwap::vwap;
pub use williams::williams_r;
pub use wma::wma;
