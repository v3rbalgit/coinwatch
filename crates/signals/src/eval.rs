//! Signal evaluation: predicates become per-bar truth masks, freshness windows
//! turn leaves into booleans, and the logic tree combines those booleans.
//!
//! Masks are never combined across timeframes directly — their bar grids do not
//! align. Each condition resolves on its own timeframe first; only its current
//! boolean verdict flows through the tree. Insufficient warmup and undefined
//! operand values remain explicit unknown verdicts rather than being mistaken
//! for a negative result.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe, completed_prefix_len};
use indicators::{Instance, InstanceOutput, Ohlcv, compute};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

use crate::definition::{
    CandleField, ComparisonOp, Condition, ConditionRequirement, CrossDirection, DefinitionError, Node, Operand,
    OperandAdjustment, Predicate, SignalDefinition,
};

/// One condition's evaluation verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum OutcomeStatus {
    /// The predicate held within the condition's freshness window.
    Satisfied,
    /// Data was available but the predicate did not hold in the window.
    NotSatisfied,
    /// No completed candle data for the condition's timeframe was available.
    NoData,
    /// Required history could not be loaded because storage or the provider failed.
    Unavailable,
    /// Some candles exist, but not enough to evaluate the complete requested window.
    InsufficientHistory {
        /// Exact completed-candle requirement.
        required: usize,
        /// Completed candles supplied to the evaluator.
        available: usize,
    },
    /// A needed operand value is non-finite; the predicate is neither held nor refuted.
    UndefinedOperands,
}

/// Numeric evidence captured at the bar most relevant to a condition verdict.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ConditionEvidence {
    /// Values used by a state comparison.
    State {
        /// Open time of the observed completed candle.
        observed_at: DateTime<Utc>,
        /// Resolved left operand after any adjustment.
        left: f64,
        /// Resolved right operand after any adjustment.
        right: f64,
    },
    /// Values on both sides immediately before and at a crossing candidate.
    Cross {
        /// Open time of the current completed candle.
        observed_at: DateTime<Utc>,
        /// Left operand on the preceding candle.
        previous_left: f64,
        /// Right operand on the preceding candle.
        previous_right: f64,
        /// Left operand on the current candle.
        left: f64,
        /// Right operand on the current candle.
        right: f64,
    },
}

/// Per-condition result, including evidence for "why did it fire".
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionOutcome {
    /// Verdict.
    #[serde(flatten)]
    pub status: OutcomeStatus,
    /// Open time of the most recent bar where the predicate held, if any.
    pub last_true_bar: Option<DateTime<Utc>>,
    /// Operand values at the most recent true bar, or at the latest bar when false.
    pub evidence: Option<ConditionEvidence>,
}

/// Result of evaluating one signal against one coin.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvalReport {
    /// Coin symbol.
    pub coin: String,
    /// Whether the logic tree holds now.
    pub satisfied: bool,
    /// Most recent evidence among satisfied positive leaves. `None` is valid when
    /// a signal holds exclusively through negation.
    pub last_evidence_at: Option<DateTime<Utc>>,
    /// Verdict per stable condition id.
    pub conditions: BTreeMap<String, ConditionOutcome>,
    /// Timeframes whose conditions had no completed candle data. Their leaves
    /// propagate an unknown verdict and remain visible here rather than failing
    /// silently or satisfying negation.
    pub missing_data: Vec<Timeframe>,
    /// History-load failures by timeframe. Their conditions have
    /// [`OutcomeStatus::Unavailable`] and propagate unknown.
    pub unavailable_data: BTreeMap<Timeframe, String>,
}

#[derive(Debug, Clone)]
struct CompletedSeries {
    candles: Arc<Vec<Candle>>,
    completed_len: usize,
}

impl CompletedSeries {
    fn as_slice(&self) -> &[Candle] {
        self.candles.get(..self.completed_len).unwrap_or_default()
    }
}

/// Completed-bar candle series for one coin, keyed by timeframe.
///
/// The store-backed runner shares the cache's `Arc` buffers instead of copying
/// `Candle` values; tests and other pure callers can insert owned vectors.
#[derive(Debug, Default, Clone)]
pub struct CoinData {
    series: HashMap<Timeframe, CompletedSeries>,
    unavailable: HashMap<Timeframe, String>,
}

impl CoinData {
    /// Insert an owned completed-bar series, oldest first.
    pub fn insert(&mut self, timeframe: Timeframe, candles: Vec<Candle>) {
        let completed_len = candles.len();
        self.unavailable.remove(&timeframe);
        self.series.insert(
            timeframe,
            CompletedSeries {
                candles: Arc::new(candles),
                completed_len,
            },
        );
    }

    pub(crate) fn insert_shared(&mut self, timeframe: Timeframe, candles: Arc<Vec<Candle>>, completed_len: usize) {
        let completed_len = completed_len.min(candles.len());
        self.unavailable.remove(&timeframe);
        self.series.insert(timeframe, CompletedSeries { candles, completed_len });
    }

    pub(crate) fn mark_unavailable(&mut self, timeframe: Timeframe, reason: String) {
        self.series.remove(&timeframe);
        self.unavailable.insert(timeframe, reason);
    }

    pub(crate) fn get(&self, timeframe: Timeframe) -> Option<&[Candle]> {
        self.series.get(&timeframe).map(CompletedSeries::as_slice)
    }
}

/// f64 columns plus deduplicated instance outputs for one timeframe.
struct TimeframePrepared<'a> {
    candles: &'a [Candle],
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    volume: Vec<f64>,
    instances: Vec<Instance>,
    outputs: Vec<InstanceOutput>,
}

impl<'a> TimeframePrepared<'a> {
    fn build(candles: &'a [Candle], conditions: &[&Condition]) -> Self {
        // Slice iterators have an exact size hint, so each collect allocates its
        // final capacity once. Decimal failures become NaN and cannot satisfy a
        // predicate.
        let column = |pick: fn(&Candle) -> &rust_decimal::Decimal| -> Vec<f64> {
            candles.iter().map(|candle| pick(candle).to_f64().unwrap_or(f64::NAN)).collect()
        };
        let open = column(|candle| &candle.open);
        let high = column(|candle| &candle.high);
        let low = column(|candle| &candle.low);
        let close = column(|candle| &candle.close);
        let volume = column(|candle| &candle.volume);

        // Configured instances can be reused across conditions or both sides of
        // one predicate. Compute each distinct configuration once per timeframe.
        let mut instances = Vec::<Instance>::with_capacity(conditions.len());
        for condition in conditions {
            for instance in condition.instances().into_iter().flatten() {
                if !instances.iter().any(|built| built == instance) {
                    instances.push(instance.clone());
                }
            }
        }

        let ohlcv = Ohlcv {
            high: &high,
            low: &low,
            close: &close,
            volume: &volume,
        };
        let outputs = compute(&instances, &ohlcv);
        Self {
            candles,
            open,
            high,
            low,
            close,
            volume,
            instances,
            outputs,
        }
    }

    fn field(&self, field: CandleField) -> &[f64] {
        match field {
            CandleField::Open => &self.open,
            CandleField::High => &self.high,
            CandleField::Low => &self.low,
            CandleField::Close => &self.close,
            CandleField::Volume => &self.volume,
        }
    }

    fn output_for(&self, instance: &Instance) -> Option<&InstanceOutput> {
        self.instances
            .iter()
            .position(|built| built == instance)
            .and_then(|index| self.outputs.get(index))
    }

    fn series_of(&self, instance: &Instance, key: &str) -> &[f64] {
        self.output_for(instance)
            .and_then(|output| output.outputs.iter().find(|series| series.key == key))
            .map(|series| series.values.as_slice())
            .unwrap_or_default()
    }
}

struct PreparedCondition {
    requirement: ConditionRequirement,
    mask: Vec<bool>,
    /// Per-bar flag: every value the bar's verdict depends on is finite.
    defined: Vec<bool>,
    latest_true: Vec<Option<usize>>,
}

/// Indicator outputs and predicate masks shared by current and historical reports.
pub(crate) struct PreparedEvaluation<'a> {
    timeframes: HashMap<Timeframe, TimeframePrepared<'a>>,
    conditions: BTreeMap<String, PreparedCondition>,
    missing_data: Vec<Timeframe>,
    unavailable_data: BTreeMap<Timeframe, String>,
}

impl<'a> PreparedEvaluation<'a> {
    pub(crate) fn build(definition: &SignalDefinition, data: &'a CoinData) -> Result<Self, DefinitionError> {
        let mut by_timeframe: BTreeMap<Timeframe, Vec<&Condition>> = BTreeMap::new();
        for condition in definition.conditions.values() {
            by_timeframe.entry(condition.timeframe).or_default().push(condition);
        }

        let mut timeframes = HashMap::with_capacity(by_timeframe.len());
        let mut missing_data = Vec::with_capacity(by_timeframe.len());
        let mut unavailable_data = BTreeMap::new();
        for (timeframe, conditions) in &by_timeframe {
            if let Some(reason) = data.unavailable.get(timeframe) {
                unavailable_data.insert(*timeframe, reason.clone());
            } else if let Some(candles) = data.get(*timeframe) {
                timeframes.insert(*timeframe, TimeframePrepared::build(candles, conditions));
            } else {
                missing_data.push(*timeframe);
            }
        }

        let mut conditions = BTreeMap::new();
        for (id, condition) in &definition.conditions {
            let Some(timeframe) = timeframes.get(&condition.timeframe) else {
                continue;
            };
            let requirement = condition.requirement().map_err(|source| DefinitionError::InvalidIndicator {
                condition: id.clone(),
                source,
            })?;
            let (mask, defined) = build_mask(condition, timeframe, requirement.confirmation_bars);
            let mut last_true = None;
            let latest_true = mask
                .iter()
                .enumerate()
                .map(|(index, held)| {
                    if *held {
                        last_true = Some(index);
                    }
                    last_true
                })
                .collect();
            conditions.insert(
                id.clone(),
                PreparedCondition {
                    requirement,
                    mask,
                    defined,
                    latest_true,
                },
            );
        }

        Ok(Self {
            timeframes,
            conditions,
            missing_data,
            unavailable_data,
        })
    }

    fn full_lengths(&self) -> BTreeMap<Timeframe, usize> {
        self.timeframes
            .iter()
            .map(|(timeframe, prepared)| (*timeframe, prepared.candles.len()))
            .collect()
    }

    pub(crate) fn lengths_at(&self, evaluated_at: DateTime<Utc>) -> BTreeMap<Timeframe, usize> {
        self.timeframes
            .iter()
            .map(|(timeframe, prepared)| (*timeframe, completed_len(prepared.candles, *timeframe, evaluated_at)))
            .collect()
    }

    /// Tree verdict without report allocation, for per-instant historical replay.
    pub(crate) fn truth_at(
        &self,
        definition: &SignalDefinition,
        completed_lengths: &BTreeMap<Timeframe, usize>,
    ) -> Truth {
        eval_tree(&definition.root, &|id| {
            let Some(condition) = definition.conditions.get(id) else {
                return Truth::Unknown;
            };
            if self.unavailable_data.contains_key(&condition.timeframe) {
                return Truth::Unknown;
            }
            match (self.timeframes.get(&condition.timeframe), self.conditions.get(id)) {
                (Some(timeframe), Some(prepared_condition)) => {
                    let available = completed_lengths.get(&condition.timeframe).copied().unwrap_or_default();
                    truth_of(resolve_condition_at(condition, timeframe, prepared_condition, available).status)
                }
                _ => Truth::Unknown,
            }
        })
    }

    pub(crate) fn report_at(
        &self,
        definition: &SignalDefinition,
        coin: &str,
        completed_lengths: &BTreeMap<Timeframe, usize>,
    ) -> EvalReport {
        let mut outcomes = BTreeMap::new();
        for (id, condition) in &definition.conditions {
            let outcome = if self.unavailable_data.contains_key(&condition.timeframe) {
                ConditionOutcome {
                    status: OutcomeStatus::Unavailable,
                    last_true_bar: None,
                    evidence: None,
                }
            } else {
                match (self.timeframes.get(&condition.timeframe), self.conditions.get(id)) {
                    (Some(timeframe), Some(prepared_condition)) => evaluate_condition_at(
                        condition,
                        timeframe,
                        prepared_condition,
                        completed_lengths.get(&condition.timeframe).copied().unwrap_or_default(),
                    ),
                    _ => ConditionOutcome {
                        status: OutcomeStatus::NoData,
                        last_true_bar: None,
                        evidence: None,
                    },
                }
            };
            outcomes.insert(id.clone(), outcome);
        }

        let truth = eval_node(&definition.root, &outcomes);
        let satisfied = truth == Truth::True;
        let mut supporting = Vec::with_capacity(definition.conditions.len());
        if satisfied {
            collect_supporting(&definition.root, &outcomes, &mut supporting);
        }
        let last_evidence_at = supporting
            .into_iter()
            .filter_map(|id| outcomes.get(id))
            .filter_map(|outcome| outcome.last_true_bar)
            .max();

        EvalReport {
            coin: coin.to_owned(),
            satisfied,
            last_evidence_at,
            conditions: outcomes,
            missing_data: self.missing_data.clone(),
            unavailable_data: self.unavailable_data.clone(),
        }
    }
}

enum Source<'a> {
    Series(&'a [f64], Option<OperandAdjustment>),
    Scalar(f64),
}

impl Source<'_> {
    fn value_at(&self, index: usize) -> Option<f64> {
        match self {
            Self::Series(series, adjustment) => series
                .get(index)
                .copied()
                .map(|value| adjustment.map_or(value, |adjustment| adjustment.apply(value))),
            Self::Scalar(value) => Some(*value),
        }
    }
}

fn resolve_operand<'a>(operand: &'a Operand, prepared: &'a TimeframePrepared<'_>) -> Source<'a> {
    match operand {
        Operand::Indicator {
            instance,
            output,
            adjustment,
        } => Source::Series(prepared.series_of(instance, output), *adjustment),
        Operand::Field { field, adjustment } => Source::Series(prepared.field(*field), *adjustment),
        Operand::Constant { value } => Source::Scalar(*value),
    }
}

/// Validate and evaluate a signal definition against one coin's completed bars.
pub fn evaluate(definition: &SignalDefinition, coin: &str, data: &CoinData) -> Result<EvalReport, DefinitionError> {
    definition.validate()?;
    evaluate_validated(definition, coin, data)
}

pub(crate) fn evaluate_validated(
    definition: &SignalDefinition,
    coin: &str,
    data: &CoinData,
) -> Result<EvalReport, DefinitionError> {
    let prepared = PreparedEvaluation::build(definition, data)?;
    let completed_lengths = prepared.full_lengths();
    Ok(prepared.report_at(definition, coin, &completed_lengths))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Truth {
    True,
    False,
    Unknown,
}

/// Length of the completed prefix in an oldest-first candle series.
pub(crate) fn completed_len(candles: &[Candle], timeframe: Timeframe, at: DateTime<Utc>) -> usize {
    completed_prefix_len(candles, timeframe, at.timestamp_millis())
}

const fn truth_of(status: OutcomeStatus) -> Truth {
    match status {
        OutcomeStatus::Satisfied => Truth::True,
        OutcomeStatus::NotSatisfied => Truth::False,
        OutcomeStatus::NoData
        | OutcomeStatus::Unavailable
        | OutcomeStatus::InsufficientHistory { .. }
        | OutcomeStatus::UndefinedOperands => Truth::Unknown,
    }
}

fn eval_node(node: &Node, outcomes: &BTreeMap<String, ConditionOutcome>) -> Truth {
    eval_tree(node, &|id| {
        outcomes.get(id).map_or(Truth::Unknown, |outcome| truth_of(outcome.status))
    })
}

fn eval_tree(node: &Node, leaf: &dyn Fn(&str) -> Truth) -> Truth {
    match node {
        Node::All { children } => {
            let mut unknown = false;
            for child in children {
                match eval_tree(child, leaf) {
                    Truth::False => return Truth::False,
                    Truth::Unknown => unknown = true,
                    Truth::True => {}
                }
            }
            if unknown { Truth::Unknown } else { Truth::True }
        }
        Node::Any { children } => {
            let mut unknown = false;
            for child in children {
                match eval_tree(child, leaf) {
                    Truth::True => return Truth::True,
                    Truth::Unknown => unknown = true,
                    Truth::False => {}
                }
            }
            if unknown { Truth::Unknown } else { Truth::False }
        }
        Node::Not { child } => match eval_tree(child, leaf) {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Unknown => Truth::Unknown,
        },
        Node::Xor { children } => {
            let mut true_count = 0;
            for child in children {
                match eval_tree(child, leaf) {
                    Truth::True => true_count += 1,
                    Truth::False => {}
                    Truth::Unknown => return Truth::Unknown,
                }
            }
            if true_count % 2 == 1 { Truth::True } else { Truth::False }
        }
        Node::Condition { id } => leaf(id),
    }
}

/// Collect only positive leaves that contribute to a true root. Negation can
/// satisfy a signal but has no positive bar timestamp of its own.
fn collect_supporting<'a>(
    node: &'a Node,
    outcomes: &BTreeMap<String, ConditionOutcome>,
    supporting: &mut Vec<&'a str>,
) {
    match node {
        Node::All { children } => {
            for child in children {
                collect_supporting(child, outcomes, supporting);
            }
        }
        Node::Any { children } | Node::Xor { children } => {
            for child in children {
                if eval_node(child, outcomes) == Truth::True {
                    collect_supporting(child, outcomes, supporting);
                }
            }
        }
        Node::Not { .. } => {}
        Node::Condition { id } => {
            if eval_node(node, outcomes) == Truth::True {
                supporting.push(id);
            }
        }
    }
}

struct ConditionResolution {
    status: OutcomeStatus,
    available: usize,
    last_true_index: Option<usize>,
}

fn resolve_condition_at(
    condition: &Condition,
    prepared: &TimeframePrepared<'_>,
    prepared_condition: &PreparedCondition,
    available: usize,
) -> ConditionResolution {
    let available = available.min(prepared.candles.len()).min(prepared_condition.mask.len());
    let requirement = prepared_condition.requirement;
    if available < requirement.completed_bars {
        return ConditionResolution {
            status: OutcomeStatus::InsufficientHistory {
                required: requirement.completed_bars,
                available,
            },
            available,
            last_true_index: None,
        };
    }

    let last_true_index = available
        .checked_sub(1)
        .and_then(|index| prepared_condition.latest_true.get(index).copied().flatten());
    let window_start = available.saturating_sub(condition.within_bars as usize + 1);
    let satisfied = last_true_index.is_some_and(|index| index >= window_start);
    let undefined_in_window = prepared_condition
        .defined
        .get(window_start..available)
        .is_some_and(|window| window.iter().any(|defined| !defined));
    let status = if satisfied {
        OutcomeStatus::Satisfied
    } else if undefined_in_window {
        OutcomeStatus::UndefinedOperands
    } else {
        OutcomeStatus::NotSatisfied
    };
    ConditionResolution {
        status,
        available,
        last_true_index,
    }
}

fn evaluate_condition_at(
    condition: &Condition,
    prepared: &TimeframePrepared<'_>,
    prepared_condition: &PreparedCondition,
    available: usize,
) -> ConditionOutcome {
    let resolution = resolve_condition_at(condition, prepared, prepared_condition, available);
    if matches!(resolution.status, OutcomeStatus::InsufficientHistory { .. }) {
        return ConditionOutcome {
            status: resolution.status,
            last_true_bar: None,
            evidence: None,
        };
    }
    let last_true_bar = resolution
        .last_true_index
        .and_then(|index| prepared.candles.get(index))
        .map(|candle| candle.open_time);
    let evidence_index = if matches!(resolution.status, OutcomeStatus::Satisfied) {
        resolution.last_true_index
    } else {
        resolution.available.checked_sub(1)
    };
    let evidence = evidence_index.and_then(|index| condition_evidence(condition, prepared, index));
    ConditionOutcome {
        status: resolution.status,
        last_true_bar,
        evidence,
    }
}

fn operand_values(
    left: &Operand,
    right: &Operand,
    prepared: &TimeframePrepared<'_>,
    index: usize,
) -> Option<(f64, f64)> {
    finite_pair(&resolve_operand(left, prepared), &resolve_operand(right, prepared), index)
}

fn condition_evidence(
    condition: &Condition,
    prepared: &TimeframePrepared<'_>,
    index: usize,
) -> Option<ConditionEvidence> {
    let observed_at = prepared.candles.get(index)?.open_time;
    match &condition.predicate {
        Predicate::Threshold { left, right, .. } => {
            let (left, right) = operand_values(left, right, prepared, index)?;
            Some(ConditionEvidence::State {
                observed_at,
                left,
                right,
            })
        }
        Predicate::Cross { left, right, .. } => {
            let previous = index.checked_sub(1)?;
            let (previous_left, previous_right) = operand_values(left, right, prepared, previous)?;
            let (left, right) = operand_values(left, right, prepared, index)?;
            Some(ConditionEvidence::Cross {
                observed_at,
                previous_left,
                previous_right,
                left,
                right,
            })
        }
        Predicate::Divergence { .. } => None,
    }
}

fn build_mask(
    condition: &Condition,
    prepared: &TimeframePrepared<'_>,
    confirmation_bars: usize,
) -> (Vec<bool>, Vec<bool>) {
    match &condition.predicate {
        Predicate::Threshold { left, right, op } => {
            let left = resolve_operand(left, prepared);
            let right = resolve_operand(right, prepared);
            threshold_mask(*op, &left, &right, prepared.candles.len())
        }
        Predicate::Cross { left, right, direction } => {
            let left = resolve_operand(left, prepared);
            let right = resolve_operand(right, prepared);
            cross_mask(*direction, &left, &right, prepared.candles.len())
        }
        Predicate::Divergence {
            instance,
            kind,
            direction,
        } => {
            let mut mask = vec![false; prepared.candles.len()];
            let spans = prepared
                .output_for(instance)
                .map(|output| output.divergences.as_slice())
                .unwrap_or_default();
            for span in spans {
                if kind.matches(span.kind)
                    && direction.matches(span.direction)
                    && let Some(confirmation) = span.to_index.checked_add(confirmation_bars)
                    && let Some(slot) = mask.get_mut(confirmation)
                {
                    *slot = true;
                }
            }
            let defined = vec![true; prepared.candles.len()];
            (mask, defined)
        }
    }
}

fn finite_pair(left: &Source<'_>, right: &Source<'_>, index: usize) -> Option<(f64, f64)> {
    match (left.value_at(index), right.value_at(index)) {
        (Some(left), Some(right)) if left.is_finite() && right.is_finite() => Some((left, right)),
        _ => None,
    }
}

fn threshold_mask(op: ComparisonOp, left: &Source<'_>, right: &Source<'_>, len: usize) -> (Vec<bool>, Vec<bool>) {
    let mut mask = Vec::with_capacity(len);
    let mut defined = Vec::with_capacity(len);
    for index in 0..len {
        let pair = finite_pair(left, right, index);
        defined.push(pair.is_some());
        mask.push(pair.is_some_and(|(left, right)| op.holds(left, right)));
    }
    (mask, defined)
}

fn cross_mask(direction: CrossDirection, left: &Source<'_>, right: &Source<'_>, len: usize) -> (Vec<bool>, Vec<bool>) {
    let mut mask = Vec::with_capacity(len);
    let mut defined = Vec::with_capacity(len);
    for index in 0..len {
        let pairs = index.checked_sub(1).and_then(|previous| {
            let now = finite_pair(left, right, index)?;
            let before = finite_pair(left, right, previous)?;
            Some((now, before))
        });
        defined.push(pairs.is_some());
        mask.push(
            pairs.is_some_and(|((now_left, now_right), (before_left, before_right))| match direction {
                CrossDirection::Over => before_left <= before_right && now_left > now_right,
                CrossDirection::Under => before_left >= before_right && now_left < now_right,
            }),
        );
    }
    (mask, defined)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use indicators::{Instance, InstanceError};
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::{
        CoinData, ConditionEvidence, ConditionOutcome, OutcomeStatus, PreparedEvaluation, completed_len, evaluate,
    };
    use crate::definition::{
        CandleField, ComparisonOp, Condition, CrossDirection, DEFINITION_VERSION, DefinitionError, Direction,
        DirectionFilter, KindFilter, Node, Operand, OperandAdjustment, Predicate, SignalDefinition,
    };

    fn candle_with_volume(index: usize, close: f64, volume: f64) -> Candle {
        let price = Decimal::try_from(close).expect("finite close");
        Candle {
            open_time: Utc
                .timestamp_opt(i64::try_from(index).expect("small fixture") * 3_600, 0)
                .single()
                .expect("valid timestamp"),
            open: price,
            high: price,
            low: price,
            close: price,
            volume: Decimal::try_from(volume).expect("finite volume"),
        }
    }

    fn candle(index: usize, close: f64) -> Candle {
        candle_with_volume(index, close, 0.0)
    }

    fn candles(closes: &[f64]) -> Vec<Candle> {
        closes.iter().enumerate().map(|(index, &close)| candle(index, close)).collect()
    }

    fn indicator(id: &str, params: impl IntoIterator<Item = (&'static str, f64)>) -> Instance {
        Instance {
            id: id.to_owned(),
            params: params.into_iter().map(|(key, value)| (key.to_owned(), value)).collect(),
        }
    }

    fn indicator_operand(instance: Instance, output: &str, adjustment: Option<OperandAdjustment>) -> Operand {
        Operand::Indicator {
            instance,
            output: output.to_owned(),
            adjustment,
        }
    }

    fn sma_threshold(timeframe: Timeframe, value: f64, within_bars: u32) -> Condition {
        Condition {
            timeframe,
            predicate: Predicate::Threshold {
                left: indicator_operand(indicator("sma", [("period", 1.0)]), "sma", None),
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value },
            },
            within_bars,
        }
    }

    fn definition(conditions: Vec<(&str, Condition)>, root: Node) -> SignalDefinition {
        let signal = SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bullish,
            root,
            conditions: conditions
                .into_iter()
                .map(|(id, condition)| (id.to_owned(), condition))
                .collect(),
        };
        signal.validate().expect("valid fixture definition");
        signal
    }

    #[test]
    fn freshness_window_includes_recent_truth_but_excludes_stale_truth() {
        let recent = definition(
            vec![("hot", sma_threshold(Timeframe::Hour1, 80.0, 1))],
            Node::Condition { id: "hot".to_owned() },
        );
        let stale = definition(
            vec![("hot", sma_threshold(Timeframe::Hour1, 80.0, 0))],
            Node::Condition { id: "hot".to_owned() },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[10.0, 20.0, 100.0, 50.0]));

        let recent_report = evaluate(&recent, "BTC", &data).expect("valid recent signal");
        assert!(recent_report.satisfied);
        assert_eq!(recent_report.conditions["hot"].status, OutcomeStatus::Satisfied);

        let stale_report = evaluate(&stale, "BTC", &data).expect("valid stale signal");
        assert!(!stale_report.satisfied);
        assert_eq!(stale_report.conditions["hot"].status, OutcomeStatus::NotSatisfied);
        assert_eq!(
            stale_report.conditions["hot"].last_true_bar,
            Utc.timestamp_opt(2 * 3_600, 0).single()
        );
    }

    #[test]
    fn raw_price_and_volume_conditions_combine_without_an_indicator() {
        let price = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Threshold {
                left: Operand::Field {
                    field: CandleField::Close,
                    adjustment: None,
                },
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value: 100.0 },
            },
            within_bars: 0,
        };
        let volume = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Threshold {
                left: Operand::Field {
                    field: CandleField::Volume,
                    adjustment: None,
                },
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value: 1_000.0 },
            },
            within_bars: 0,
        };
        let signal = definition(
            vec![("price", price), ("volume", volume)],
            Node::All {
                children: vec![
                    Node::Condition { id: "price".to_owned() },
                    Node::Condition {
                        id: "volume".to_owned(),
                    },
                ],
            },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, vec![candle_with_volume(0, 110.0, 1_500.0)]);

        let report = evaluate(&signal, "BTC", &data).expect("valid raw market signal");
        assert!(report.satisfied);
        assert_eq!(report.conditions["price"].status, OutcomeStatus::Satisfied);
        assert_eq!(report.conditions["volume"].status, OutcomeStatus::Satisfied);
    }

    #[test]
    fn detects_close_crossing_three_percent_above_ema() {
        let condition = Condition {
            timeframe: Timeframe::Day1,
            predicate: Predicate::Cross {
                left: Operand::Field {
                    field: CandleField::Close,
                    adjustment: None,
                },
                right: indicator_operand(
                    indicator("ema", [("period", 3.0)]),
                    "ema",
                    Some(OperandAdjustment::Percent { value: 3.0 }),
                ),
                direction: CrossDirection::Over,
            },
            within_bars: 0,
        };
        let signal = definition(vec![("cross", condition)], Node::Condition { id: "cross".to_owned() });
        let mut closes = vec![100.0; 14];
        closes.extend([103.0, 110.0]);
        let mut data = CoinData::default();
        data.insert(Timeframe::Day1, candles(&closes));

        let report = evaluate(&signal, "BTC", &data).expect("valid adjusted cross signal");
        assert!(report.satisfied);
        assert_eq!(
            report.conditions["cross"].last_true_bar,
            Utc.timestamp_opt(15 * 3_600, 0).single()
        );
    }

    #[test]
    fn multi_timeframe_rsi_thresholds_combine_with_all() {
        let condition = |timeframe| Condition {
            timeframe,
            predicate: Predicate::Threshold {
                left: indicator_operand(indicator("rsi", [("period", 14.0)]), "rsi", None),
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value: 70.0 },
            },
            within_bars: 0,
        };
        let signal = definition(
            vec![
                ("rsi1h", condition(Timeframe::Hour1)),
                ("rsi4h", condition(Timeframe::Hour4)),
                ("rsi1d", condition(Timeframe::Day1)),
            ],
            Node::All {
                children: vec![
                    Node::Condition { id: "rsi1h".to_owned() },
                    Node::Condition { id: "rsi4h".to_owned() },
                    Node::Condition { id: "rsi1d".to_owned() },
                ],
            },
        );
        let rising = candles(&(1..=71).map(f64::from).collect::<Vec<_>>());
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, rising.clone());
        data.insert(Timeframe::Hour4, rising.clone());
        data.insert(Timeframe::Day1, rising);

        let report = evaluate(&signal, "BTC", &data).expect("valid multi-timeframe RSI signal");
        assert!(report.satisfied);
        assert!(
            report
                .conditions
                .values()
                .all(|outcome| outcome.status == OutcomeStatus::Satisfied)
        );
        assert!(matches!(
            report.conditions["rsi1h"].evidence,
            Some(ConditionEvidence::State {
                left: 100.0,
                right: 70.0,
                ..
            })
        ));
    }

    #[test]
    fn compares_independently_configured_indicator_instances() {
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Cross {
                left: indicator_operand(indicator("sma", [("period", 2.0)]), "sma", None),
                right: indicator_operand(indicator("sma", [("period", 3.0)]), "sma", None),
                direction: CrossDirection::Over,
            },
            within_bars: 0,
        };
        let signal = definition(vec![("cross", condition)], Node::Condition { id: "cross".to_owned() });
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[3.0, 2.0, 1.0, 2.0, 3.0]));

        let report = evaluate(&signal, "BTC", &data).expect("valid two-instance signal");
        assert!(report.satisfied);
        assert_eq!(
            report.conditions["cross"].last_true_bar,
            Utc.timestamp_opt(4 * 3_600, 0).single()
        );
    }

    #[test]
    fn detects_close_crossing_over_sma_on_latest_bar() {
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Cross {
                left: Operand::Field {
                    field: CandleField::Close,
                    adjustment: None,
                },
                right: indicator_operand(indicator("sma", [("period", 3.0)]), "sma", None),
                direction: CrossDirection::Over,
            },
            within_bars: 0,
        };
        let signal = definition(vec![("cross", condition)], Node::Condition { id: "cross".to_owned() });
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[10.0, 10.0, 10.0, 5.0, 10.0]));

        let report = evaluate(&signal, "BTC", &data).expect("valid cross signal");
        assert!(report.satisfied, "expected a close-over-SMA cross on the final bar");
        assert!(matches!(
            report.conditions["cross"].evidence,
            Some(ConditionEvidence::Cross {
                previous_left: 5.0,
                previous_right,
                left: 10.0,
                right,
                ..
            }) if (previous_right - 25.0 / 3.0).abs() < f64::EPSILON
                && (right - 25.0 / 3.0).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn invalid_definitions_fail_before_data_statuses_are_computed() {
        let mut signal = definition(
            vec![("invalid", sma_threshold(Timeframe::Hour1, 80.0, 0))],
            Node::Condition {
                id: "invalid".to_owned(),
            },
        );
        let Predicate::Threshold {
            left: Operand::Indicator { instance, .. },
            ..
        } = &mut signal.conditions.get_mut("invalid").expect("fixture condition").predicate
        else {
            panic!("fixture indicator operand")
        };
        instance.params.insert("typo".to_owned(), 1.0);

        assert!(matches!(
            evaluate(&signal, "BTC", &CoinData::default()),
            Err(DefinitionError::InvalidIndicator {
                source: InstanceError::UnknownParameter { parameter, .. },
                ..
            }) if parameter == "typo"
        ));
    }

    #[test]
    fn outcome_status_serialization_is_stable() {
        assert_eq!(
            serde_json::to_value(ConditionOutcome {
                status: OutcomeStatus::InsufficientHistory {
                    required: 12,
                    available: 7,
                },
                last_true_bar: None,
                evidence: None,
            })
            .expect("serializable outcome"),
            json!({
                "status": "insufficientHistory",
                "required": 12,
                "available": 7,
                "lastTrueBar": null,
                "evidence": null,
            })
        );
        assert_eq!(
            serde_json::to_value(OutcomeStatus::Unavailable).expect("serializable status"),
            json!({ "status": "unavailable" })
        );
        assert_eq!(
            serde_json::to_value(OutcomeStatus::UndefinedOperands).expect("serializable status"),
            json!({ "status": "undefinedOperands" })
        );
        assert_eq!(
            serde_json::to_value(ConditionEvidence::Cross {
                observed_at: Utc.timestamp_opt(7_200, 0).single().expect("timestamp"),
                previous_left: 1.0,
                previous_right: 2.0,
                left: 3.0,
                right: 4.0,
            })
            .expect("serializable evidence"),
            json!({
                "type": "cross",
                "observedAt": "1970-01-01T02:00:00Z",
                "previousLeft": 1.0,
                "previousRight": 2.0,
                "left": 3.0,
                "right": 4.0,
            })
        );
    }

    #[test]
    fn unavailable_history_is_unknown_and_preserves_the_failure() {
        let signal = definition(
            vec![("history", sma_threshold(Timeframe::Hour1, 80.0, 0))],
            Node::Not {
                child: Box::new(Node::Condition {
                    id: "history".to_owned(),
                }),
            },
        );
        let mut data = CoinData::default();
        data.mark_unavailable(Timeframe::Hour1, "provider unavailable".to_owned());

        let report = evaluate(&signal, "BTC", &data).expect("valid unavailable-history signal");
        assert_eq!(report.conditions["history"].status, OutcomeStatus::Unavailable);
        assert_eq!(
            report.unavailable_data.get(&Timeframe::Hour1).map(String::as_str),
            Some("provider unavailable")
        );
        assert!(report.missing_data.is_empty());
        assert!(!report.satisfied, "unavailable history must not satisfy negation");
    }

    #[test]
    fn known_empty_history_is_insufficient_not_missing() {
        let signal = definition(
            vec![("empty", sma_threshold(Timeframe::Hour1, 80.0, 0))],
            Node::Condition { id: "empty".to_owned() },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, Vec::new());

        let report = evaluate(&signal, "BTC", &data).expect("valid empty-history signal");
        assert_eq!(
            report.conditions["empty"].status,
            OutcomeStatus::InsufficientHistory {
                required: 1,
                available: 0,
            }
        );
        assert!(report.missing_data.is_empty());
    }

    #[test]
    fn insufficient_warmup_is_unknown_and_cannot_satisfy_negation() {
        let mut condition = sma_threshold(Timeframe::Hour1, 80.0, 1);
        let Predicate::Threshold {
            left: Operand::Indicator { instance, .. },
            ..
        } = &mut condition.predicate
        else {
            panic!("fixture indicator operand")
        };
        instance.params.insert("period".to_owned(), 3.0);
        let signal = definition(
            vec![("warmup", condition)],
            Node::Not {
                child: Box::new(Node::Condition {
                    id: "warmup".to_owned(),
                }),
            },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[10.0, 20.0, 30.0]));

        let report = evaluate(&signal, "BTC", &data).expect("valid warmup signal");
        assert_eq!(
            report.conditions["warmup"].status,
            OutcomeStatus::InsufficientHistory {
                required: 4,
                available: 3,
            }
        );
        assert!(!report.satisfied, "unknown history must not satisfy negation");
    }

    #[test]
    fn completed_len_excludes_forming_trailing_bar() {
        let series = candles(&[1.0, 1.0, 1.0]);
        let now = Utc.timestamp_opt(2 * 3_600 + 1_800, 0).single().expect("valid timestamp");
        assert_eq!(completed_len(&series, Timeframe::Hour1, now), 2);
    }

    #[test]
    fn completed_len_includes_bar_at_exact_close_time() {
        let series = candles(&[1.0, 1.0]);
        let now = Utc.timestamp_opt(2 * 3_600, 0).single().expect("valid timestamp");
        assert_eq!(completed_len(&series, Timeframe::Hour1, now), 2);
    }

    fn random_walk(seed: u64, bars: usize) -> Vec<f64> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut close = 100.0f64;
        (0..bars)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let step = (state >> 11) as f64 / (1u64 << 53) as f64 - 0.5;
                close = (close + step * 4.0).max(5.0);
                close
            })
            .collect()
    }

    fn hour4_candles(closes: &[f64]) -> Vec<Candle> {
        closes
            .iter()
            .enumerate()
            .map(|(index, &close)| {
                let open_time = Utc
                    .timestamp_opt(index as i64 * 4 * 3_600, 0)
                    .single()
                    .expect("valid timestamp");
                Candle {
                    open_time,
                    ..candle(0, close)
                }
            })
            .collect()
    }

    #[test]
    fn replay_at_truncated_lengths_matches_fresh_prefix_evaluation() {
        let conditions = vec![
            (
                "aboveSma",
                Condition {
                    timeframe: Timeframe::Hour1,
                    predicate: Predicate::Threshold {
                        left: Operand::Field {
                            field: CandleField::Close,
                            adjustment: None,
                        },
                        op: ComparisonOp::GreaterThan,
                        right: indicator_operand(indicator("sma", [("period", 20.0)]), "sma", None),
                    },
                    within_bars: 2,
                },
            ),
            (
                "crossEma",
                Condition {
                    timeframe: Timeframe::Hour1,
                    predicate: Predicate::Cross {
                        left: Operand::Field {
                            field: CandleField::Close,
                            adjustment: None,
                        },
                        direction: CrossDirection::Over,
                        right: indicator_operand(indicator("ema", [("period", 10.0)]), "ema", None),
                    },
                    within_bars: 5,
                },
            ),
            (
                "divergence",
                Condition {
                    timeframe: Timeframe::Hour1,
                    predicate: Predicate::Divergence {
                        instance: indicator(
                            "rsiDivergence",
                            [
                                ("period", 5.0),
                                ("pivotLeft", 3.0),
                                ("pivotRight", 3.0),
                                ("maxGap", 30.0),
                            ],
                        ),
                        kind: KindFilter::Any,
                        direction: DirectionFilter::Any,
                    },
                    within_bars: 8,
                },
            ),
            (
                "calmRsi",
                Condition {
                    timeframe: Timeframe::Hour4,
                    predicate: Predicate::Threshold {
                        left: indicator_operand(indicator("rsi", [("period", 14.0)]), "rsi", None),
                        op: ComparisonOp::LessThan,
                        right: Operand::Constant { value: 60.0 },
                    },
                    within_bars: 1,
                },
            ),
        ];
        let signal = SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bullish,
            root: Node::Any {
                children: conditions
                    .iter()
                    .map(|(id, _)| Node::Condition { id: (*id).to_owned() })
                    .collect(),
            },
            conditions: conditions
                .into_iter()
                .map(|(id, condition)| (id.to_owned(), condition))
                .collect(),
        };
        signal.validate().expect("valid replay fixture");

        for seed in [3u64, 11, 27] {
            let hour1 = candles(&random_walk(seed, 320));
            let hour4 = hour4_candles(&random_walk(seed.wrapping_add(99), 90));
            let mut full = CoinData::default();
            full.insert(Timeframe::Hour1, hour1.clone());
            full.insert(Timeframe::Hour4, hour4.clone());
            let prepared = PreparedEvaluation::build(&signal, &full).expect("prepared replay fixture");

            for len in 1..=hour1.len() {
                let instant = Utc.timestamp_opt(len as i64 * 3_600, 0).single().expect("valid timestamp");
                let lengths = prepared.lengths_at(instant);
                let replay = prepared.report_at(&signal, "BTC", &lengths);

                let mut truncated = CoinData::default();
                truncated.insert(Timeframe::Hour1, hour1.get(..len).expect("prefix").to_vec());
                let hour4_len = completed_len(&hour4, Timeframe::Hour4, instant);
                truncated.insert(Timeframe::Hour4, hour4.get(..hour4_len).expect("prefix").to_vec());
                let fresh = evaluate(&signal, "BTC", &truncated).expect("fresh prefix evaluation");

                assert_eq!(replay.satisfied, fresh.satisfied, "seed {seed} len {len}");
                for (id, outcome) in &fresh.conditions {
                    let replayed = replay.conditions.get(id).expect("replayed condition");
                    assert_eq!(replayed.status, outcome.status, "seed {seed} len {len} {id}");
                    assert_eq!(replayed.last_true_bar, outcome.last_true_bar, "seed {seed} len {len} {id}");
                }
            }
        }
    }

    #[test]
    fn undefined_operands_are_unknown_and_cannot_satisfy_negation() {
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Threshold {
                left: Operand::Field {
                    field: CandleField::Close,
                    adjustment: None,
                },
                op: ComparisonOp::GreaterThan,
                right: indicator_operand(
                    indicator("sma", [("period", 1.0)]),
                    "sma",
                    Some(OperandAdjustment::Percent { value: 1e308 }),
                ),
            },
            within_bars: 0,
        };
        let signal = definition(
            vec![("aboveSma", condition)],
            Node::Not {
                child: Box::new(Node::Condition {
                    id: "aboveSma".to_owned(),
                }),
            },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[1_000.0, 1_000.0, 1_000.0]));

        let report = evaluate(&signal, "BTC", &data).expect("valid undefined-operand signal");
        assert_eq!(report.conditions["aboveSma"].status, OutcomeStatus::UndefinedOperands);
        assert!(!report.satisfied, "undefined operands must not satisfy negation");
    }

    #[test]
    fn padded_rsi_requirement_is_depth_insensitive_at_the_final_bar() {
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Threshold {
                left: indicator_operand(indicator("rsi", [("period", 14.0)]), "rsi", None),
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value: 70.0 },
            },
            within_bars: 0,
        };
        let required = condition.required_completed_bars().expect("padded requirement");
        assert_eq!(required, 71, "RSI(14) requirement includes convergence padding");
        let signal = definition(vec![("rsi", condition)], Node::Condition { id: "rsi".to_owned() });

        let final_rsi = |closes: &[f64]| {
            let mut data = CoinData::default();
            data.insert(Timeframe::Hour1, candles(closes));
            let report = evaluate(&signal, "BTC", &data).expect("valid depth fixture");
            let Some(ConditionEvidence::State { left, .. }) = report.conditions["rsi"].evidence else {
                panic!("state evidence at the final bar")
            };
            (left, report.conditions["rsi"].status)
        };

        for seed in 1..=8u64 {
            let closes = random_walk(seed, 400);
            let (full, full_status) = final_rsi(&closes);
            let suffix = closes.get(closes.len() - required..).expect("padded suffix");
            let (shallow, shallow_status) = final_rsi(suffix);
            let delta = (full - shallow).abs();
            assert!(delta < 0.5, "seed {seed}: full {full} vs padded suffix {shallow}");
            if (full - 70.0).abs() >= 0.5 {
                assert_eq!(full_status, shallow_status, "seed {seed}: verdict flipped off-threshold");
            }
        }
    }

    #[test]
    fn missing_timeframe_is_unknown_and_cannot_satisfy_negation() {
        let signal = definition(
            vec![
                ("hot", sma_threshold(Timeframe::Hour4, 80.0, 0)),
                ("weekly", sma_threshold(Timeframe::Week1, 80.0, 0)),
            ],
            Node::All {
                children: vec![
                    Node::Condition { id: "hot".to_owned() },
                    Node::Condition {
                        id: "weekly".to_owned(),
                    },
                ],
            },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour4, candles(&[100.0, 100.0]));

        let report = evaluate(&signal, "BTC", &data).expect("valid missing-data signal");
        assert_eq!(report.conditions["hot"].status, OutcomeStatus::Satisfied);
        assert_eq!(report.conditions["weekly"].status, OutcomeStatus::NoData);
        assert_eq!(report.missing_data, vec![Timeframe::Week1]);
        assert!(!report.satisfied);

        let negated = definition(
            vec![("weekly", sma_threshold(Timeframe::Week1, 80.0, 0))],
            Node::Not {
                child: Box::new(Node::Condition {
                    id: "weekly".to_owned(),
                }),
            },
        );
        assert!(
            !evaluate(&negated, "BTC", &CoinData::default())
                .expect("valid negated signal")
                .satisfied
        );
    }

    #[test]
    fn nested_not_and_xor_combine_leaf_verdicts() {
        let signal = definition(
            vec![
                ("hot", sma_threshold(Timeframe::Hour1, 80.0, 0)),
                ("extreme", sma_threshold(Timeframe::Hour1, 200.0, 0)),
            ],
            Node::All {
                children: vec![
                    Node::Condition { id: "hot".to_owned() },
                    Node::Not {
                        child: Box::new(Node::Condition {
                            id: "extreme".to_owned(),
                        }),
                    },
                    Node::Xor {
                        children: vec![
                            Node::Condition { id: "hot".to_owned() },
                            Node::Condition {
                                id: "extreme".to_owned(),
                            },
                        ],
                    },
                ],
            },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[100.0, 100.0]));

        assert!(evaluate(&signal, "BTC", &data).expect("valid nested signal").satisfied);
    }

    #[test]
    fn divergence_filters_kind_and_direction() {
        // Strict highs at indices 14 and 16: price makes a higher high (6 -> 7)
        // while RSI(2) makes a lower high (~77 -> ~73), a regular bearish divergence.
        let mut closes = vec![10.0; 11];
        closes.extend([3.0, 3.0, 3.0, 6.0, 3.0, 7.0, 3.0]);
        let params = [
            ("period".to_owned(), 2.0),
            ("pivotLeft".to_owned(), 1.0),
            ("pivotRight".to_owned(), 1.0),
            ("maxGap".to_owned(), 5.0),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Divergence {
                instance: Instance {
                    id: "rsiDivergence".to_owned(),
                    params,
                },
                kind: KindFilter::Regular,
                direction: DirectionFilter::Bearish,
            },
            within_bars: 0,
        };
        let signal = definition(vec![("div", condition)], Node::Condition { id: "div".to_owned() });
        let mut insufficient = CoinData::default();
        insufficient.insert(Timeframe::Hour1, candles(closes.get(..16).expect("short divergence fixture")));
        let insufficient_report = evaluate(&signal, "BTC", &insufficient).expect("valid divergence signal");
        assert_eq!(
            insufficient_report.conditions["div"].status,
            OutcomeStatus::InsufficientHistory {
                required: 17,
                available: 16,
            }
        );

        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&closes));
        let report = evaluate(&signal, "BTC", &data).expect("valid divergence signal");
        assert!(report.satisfied);
        assert_eq!(
            report.conditions["div"].last_true_bar,
            Utc.timestamp_opt(17 * 3_600, 0).single()
        );
    }
}
