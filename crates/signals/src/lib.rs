//! Signal definitions and evaluation over cached market candles.
//!
//! A [`SignalDefinition`] composes market and indicator conditions into a boolean
//! tree. [`eval`] resolves each leaf on its own timeframe; [`run`] wires that pure
//! evaluator onto completed hot-cache candles with lazy persisted-history hydration.

pub mod definition;
pub mod eval;
pub mod historical;
pub mod run;

pub use definition::{
    CandleField, ComparisonOp, Condition, CrossDirection, DEFINITION_VERSION, DefinitionDecodeError, DefinitionError,
    Direction, DirectionFilter, KindFilter, Node, Operand, OperandAdjustment, Predicate, SignalDefinition,
    decode_definition,
};
pub use eval::{CoinData, ConditionEvidence, ConditionOutcome, EvalReport, EvaluationVerdict, OutcomeStatus};
pub use historical::{
    HistoricalMatchSpan, HistoricalProgress, HistoricalRange, HistoricalValidation, HistoricalValidationError,
    evaluate_historical, validate_historical,
};
pub use run::{RunProgress, RunSummary, run_signal};
