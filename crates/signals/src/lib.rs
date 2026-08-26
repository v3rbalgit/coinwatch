//! Signal definitions and evaluation over cached market candles.
//!
//! A [`SignalDefinition`] composes configured indicator conditions into a boolean
//! tree. [`eval`] resolves each leaf on its own timeframe; [`run`] wires that pure
//! evaluator onto completed hot-cache candles with lazy persisted-history hydration.

pub mod definition;
pub mod eval;
pub mod run;

pub use definition::{
    ComparisonOp, Condition, CrossDirection, DEFINITION_VERSION, DefinitionError, Direction, DirectionFilter,
    KindFilter, Node, Operand, Predicate, PriceField, SignalDefinition,
};
pub use eval::{CoinData, ConditionOutcome, EvalReport, OutcomeStatus};
pub use run::{RunProgress, RunSummary, evaluate_coin, run_signal};
