//! Technical indicators and divergence detection over price/OHLCV series.
//!
//! Pure, synchronous math — no I/O and no async. Callers feed close-price slices
//! (converted from the feed's candle buffers) and receive indicator series and
//! detected divergences. Divergence detection is generic over any oscillator, so
//! RSI and MACD share one detector.

mod divergence;
mod macd;
mod pivot;
mod rsi;

pub use divergence::{Divergence, DivergenceDirection, DivergenceKind, detect_divergences};
pub use macd::{Macd, ema, macd};
pub use pivot::{Pivot, PivotKind, pivots};
pub use rsi::rsi;
