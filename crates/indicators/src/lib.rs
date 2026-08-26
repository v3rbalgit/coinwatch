//! Shared, instance-driven technical-indicator engine.
//!
//! Callers provide configured indicator instances and aligned OHLCV columns. The
//! descriptor registry, validation, history requirements, and computation dispatch
//! form the public contract; individual algorithms remain crate implementation details.

mod adx;
mod atr;
mod bollinger;
mod cci;
mod divergence;
mod donchian;
mod engine;
mod macd;
mod mfi;
mod obv;
mod pivot;
mod psar;
mod rsi;
mod sma;
mod stochastic;
mod supertrend;
mod typical;
mod vwap;
mod williams;
mod wma;

pub use divergence::{DivergenceDirection, DivergenceKind};
pub use engine::{
    Descriptor, DescriptorKind, DivergenceInput, Instance, InstanceError, InstanceOutput, Ohlcv, Output, OutputKind,
    OutputSeries, Param, Target, compute, descriptors, divergence_input, resolve_instance, series_input_requirement,
    validate_instance,
};
