//! Signal definitions and evaluation over cached market candles.
//!
//! A [`SignalDefinition`] composes configured indicator conditions into a boolean
//! tree. [`eval`] resolves each leaf on its own timeframe; [`run`] wires that pure
//! evaluator onto completed candle prefixes from the hot market cache.

pub mod definition;
pub mod eval;
pub mod run;

pub use definition::{
    ComparisonOp, Condition, CrossDirection, DEFINITION_VERSION, DefinitionError, Direction, DirectionFilter,
    KindFilter, Node, Operand, Predicate, PriceField, SignalDefinition,
};
pub use eval::{CoinData, ConditionOutcome, EvalReport, OutcomeStatus};
pub use run::{RunScope, collect_coin_data, evaluate_coin, referenced_timeframes, run_signal};
