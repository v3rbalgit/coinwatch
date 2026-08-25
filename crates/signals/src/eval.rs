//! Signal evaluation: predicates become per-bar truth masks, freshness windows
//! turn leaves into booleans, and the logic tree combines those booleans.
//!
//! Masks are never combined across timeframes directly — their bar grids do not
//! align. Each condition resolves on its own timeframe first; only its current
//! boolean verdict flows through the tree. Warm-up values (`NaN`) and failed
//! decimal conversions evaluate to `false`, keeping results conservative.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use indicators::{Instance, InstanceOutput, Ohlcv, compute};
use rust_decimal::prelude::ToPrimitive;

use crate::definition::{
    ComparisonOp, Condition, CrossDirection, Node, Operand, Predicate, PriceField, SignalDefinition,
};

/// One condition's evaluation verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeStatus {
    /// The predicate held within the condition's freshness window.
    Satisfied,
    /// Data was available but the predicate did not hold in the window.
    NotSatisfied,
    /// No completed candle data for the condition's timeframe was available.
    NoData,
}

/// Per-condition result, including evidence for "why did it fire".
#[derive(Debug, Clone)]
pub struct ConditionOutcome {
    /// Verdict.
    pub status: OutcomeStatus,
    /// Open time of the most recent bar where the predicate held, if any.
    pub last_true_bar: Option<DateTime<Utc>>,
}

/// Result of evaluating one signal against one coin.
#[derive(Debug, Clone)]
pub struct EvalReport {
    /// Coin symbol.
    pub coin: String,
    /// Whether the logic tree holds now.
    pub satisfied: bool,
    /// Most recent evidence among satisfied positive leaves. `None` is valid when
    /// a signal holds exclusively through negation.
    pub triggered_at: Option<DateTime<Utc>>,
    /// Verdict per stable condition id.
    pub conditions: BTreeMap<String, ConditionOutcome>,
    /// Timeframes whose conditions had no completed candle data. Their leaves
    /// propagate an unknown verdict and remain visible here rather than failing
    /// silently or satisfying negation.
    pub missing_data: Vec<Timeframe>,
}

#[derive(Debug, Clone)]
struct CompletedSeries {
    candles: Arc<[Candle]>,
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
}

impl CoinData {
    /// Insert an owned completed-bar series, oldest first.
    pub fn insert(&mut self, timeframe: Timeframe, candles: Vec<Candle>) {
        let completed_len = candles.len();
        self.series.insert(
            timeframe,
            CompletedSeries {
                candles: candles.into(),
                completed_len,
            },
        );
    }

    pub(crate) fn insert_shared(&mut self, timeframe: Timeframe, candles: Arc<[Candle]>, completed_len: usize) {
        let completed_len = completed_len.min(candles.len());
        self.series.insert(timeframe, CompletedSeries { candles, completed_len });
    }

    fn get(&self, timeframe: Timeframe) -> Option<&[Candle]> {
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
    outputs: Vec<InstanceOutput>,
    instance_index: HashMap<&'a str, usize>,
}

impl<'a> TimeframePrepared<'a> {
    fn build(candles: &'a [Candle], conditions: &[(&'a str, &'a Condition)]) -> Self {
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

        // Signals commonly reuse the same configured instance in several
        // predicates; compute it once per timeframe and retain a leaf→output map.
        let mut unique_instances = Vec::<Instance>::with_capacity(conditions.len());
        let mut instance_index = HashMap::with_capacity(conditions.len());
        for (id, condition) in conditions {
            let index = unique_instances
                .iter()
                .position(|built| built == &condition.indicator)
                .unwrap_or_else(|| {
                    unique_instances.push(condition.indicator.clone());
                    unique_instances.len() - 1
                });
            instance_index.insert(*id, index);
        }

        let ohlcv = Ohlcv {
            high: &high,
            low: &low,
            close: &close,
            volume: &volume,
        };
        let outputs = compute(&unique_instances, &ohlcv);
        Self {
            candles,
            open,
            high,
            low,
            close,
            volume,
            outputs,
            instance_index,
        }
    }

    fn field(&self, field: PriceField) -> &[f64] {
        match field {
            PriceField::Open => &self.open,
            PriceField::High => &self.high,
            PriceField::Low => &self.low,
            PriceField::Close => &self.close,
            PriceField::Volume => &self.volume,
        }
    }

    fn output_for(&self, condition_id: &str) -> Option<&InstanceOutput> {
        self.instance_index.get(condition_id).and_then(|index| self.outputs.get(*index))
    }

    fn series_of(&self, condition_id: &str, key: &str) -> &[f64] {
        self.output_for(condition_id)
            .and_then(|output| output.outputs.iter().find(|series| series.key == key))
            .map(|series| series.values.as_slice())
            .unwrap_or_default()
    }
}

enum Source<'a> {
    Series(&'a [f64]),
    Scalar(f64),
}

impl Source<'_> {
    fn value_at(&self, index: usize) -> Option<f64> {
        match self {
            Self::Series(series) => series.get(index).copied(),
            Self::Scalar(value) => Some(*value),
        }
    }
}

fn resolve_operand<'a>(operand: &'a Operand, condition_id: &str, prepared: &'a TimeframePrepared<'_>) -> Source<'a> {
    match operand {
        Operand::Output { key } => Source::Series(prepared.series_of(condition_id, key)),
        Operand::Field { field } => Source::Series(prepared.field(*field)),
        Operand::Constant { value } => Source::Scalar(*value),
    }
}

/// Evaluate a validated signal definition against one coin's completed bars.
#[must_use]
pub fn evaluate(definition: &SignalDefinition, coin: &str, data: &CoinData) -> EvalReport {
    let mut by_timeframe: BTreeMap<Timeframe, Vec<(&str, &Condition)>> = BTreeMap::new();
    for (id, condition) in &definition.conditions {
        by_timeframe.entry(condition.timeframe).or_default().push((id, condition));
    }

    let mut prepared = HashMap::with_capacity(by_timeframe.len());
    let mut missing_data = Vec::with_capacity(by_timeframe.len());
    for (timeframe, conditions) in &by_timeframe {
        match data.get(*timeframe) {
            Some(candles) if !candles.is_empty() => {
                prepared.insert(*timeframe, TimeframePrepared::build(candles, conditions));
            }
            _ => missing_data.push(*timeframe),
        }
    }

    let mut outcomes = BTreeMap::new();
    for (id, condition) in &definition.conditions {
        let outcome = match prepared.get(&condition.timeframe) {
            Some(timeframe) => evaluate_condition(id, condition, timeframe),
            None => ConditionOutcome {
                status: OutcomeStatus::NoData,
                last_true_bar: None,
            },
        };
        outcomes.insert(id.clone(), outcome);
    }

    let truth = eval_node(&definition.root, &outcomes);
    let satisfied = truth == Truth::True;
    let mut supporting = Vec::with_capacity(definition.conditions.len());
    if satisfied {
        collect_supporting(&definition.root, &outcomes, &mut supporting);
    }
    let triggered_at = supporting
        .into_iter()
        .filter_map(|id| outcomes.get(id))
        .filter_map(|outcome| outcome.last_true_bar)
        .max();

    EvalReport {
        coin: coin.to_owned(),
        satisfied,
        triggered_at,
        conditions: outcomes,
        missing_data,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Truth {
    True,
    False,
    Unknown,
}

fn eval_node(node: &Node, outcomes: &BTreeMap<String, ConditionOutcome>) -> Truth {
    match node {
        Node::All { children } => {
            let mut unknown = false;
            for child in children {
                match eval_node(child, outcomes) {
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
                match eval_node(child, outcomes) {
                    Truth::True => return Truth::True,
                    Truth::Unknown => unknown = true,
                    Truth::False => {}
                }
            }
            if unknown { Truth::Unknown } else { Truth::False }
        }
        Node::Not { child } => match eval_node(child, outcomes) {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Unknown => Truth::Unknown,
        },
        Node::Xor { children } => {
            let mut true_count = 0;
            for child in children {
                match eval_node(child, outcomes) {
                    Truth::True => true_count += 1,
                    Truth::False => {}
                    Truth::Unknown => return Truth::Unknown,
                }
            }
            if true_count % 2 == 1 { Truth::True } else { Truth::False }
        }
        Node::Condition { id } => match outcomes.get(id).map(|outcome| outcome.status) {
            Some(OutcomeStatus::Satisfied) => Truth::True,
            Some(OutcomeStatus::NotSatisfied) => Truth::False,
            Some(OutcomeStatus::NoData) | None => Truth::Unknown,
        },
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

fn evaluate_condition(condition_id: &str, condition: &Condition, prepared: &TimeframePrepared<'_>) -> ConditionOutcome {
    let mask = build_mask(condition_id, condition, prepared);
    let window_len = usize::try_from(condition.within_bars).unwrap_or(usize::MAX).saturating_add(1);
    let satisfied = mask.iter().rev().take(window_len).any(|&held| held);
    let last_true_bar = mask
        .iter()
        .rposition(|&held| held)
        .and_then(|index| prepared.candles.get(index))
        .map(|candle| candle.open_time);
    let status = if satisfied {
        OutcomeStatus::Satisfied
    } else {
        OutcomeStatus::NotSatisfied
    };
    ConditionOutcome { status, last_true_bar }
}

fn build_mask(condition_id: &str, condition: &Condition, prepared: &TimeframePrepared<'_>) -> Vec<bool> {
    match &condition.predicate {
        Predicate::Threshold { left, right, op } => {
            let left = resolve_operand(left, condition_id, prepared);
            let right = resolve_operand(right, condition_id, prepared);
            threshold_mask(*op, &left, &right, prepared.candles.len())
        }
        Predicate::Cross { left, right, direction } => {
            let left = resolve_operand(left, condition_id, prepared);
            let right = resolve_operand(right, condition_id, prepared);
            cross_mask(*direction, &left, &right, prepared.candles.len())
        }
        Predicate::Divergence { kind, direction } => {
            let mut mask = vec![false; prepared.candles.len()];
            let spans = prepared
                .output_for(condition_id)
                .map(|output| output.divergences.as_slice())
                .unwrap_or_default();
            for span in spans {
                if kind.matches(span.kind)
                    && direction.matches(span.direction)
                    && let Some(slot) = mask.get_mut(span.to_index)
                {
                    *slot = true;
                }
            }
            mask
        }
    }
}

fn threshold_mask(op: ComparisonOp, left: &Source<'_>, right: &Source<'_>, len: usize) -> Vec<bool> {
    // Range is exact-size; `collect` preallocates the complete mask.
    (0..len)
        .map(|index| match (left.value_at(index), right.value_at(index)) {
            (Some(left), Some(right)) if left.is_finite() && right.is_finite() => op.holds(left, right),
            _ => false,
        })
        .collect()
}

fn cross_mask(direction: CrossDirection, left: &Source<'_>, right: &Source<'_>, len: usize) -> Vec<bool> {
    // Range is exact-size; `collect` preallocates the complete mask.
    (0..len)
        .map(|index| {
            let Some(previous) = index.checked_sub(1) else {
                return false;
            };
            let now = (left.value_at(index), right.value_at(index));
            let before = (left.value_at(previous), right.value_at(previous));
            match (now, before) {
                ((Some(now_left), Some(now_right)), (Some(before_left), Some(before_right)))
                    if now_left.is_finite()
                        && now_right.is_finite()
                        && before_left.is_finite()
                        && before_right.is_finite() =>
                {
                    match direction {
                        CrossDirection::Over => before_left <= before_right && now_left > now_right,
                        CrossDirection::Under => before_left >= before_right && now_left < now_right,
                    }
                }
                _ => false,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use indicators::Instance;
    use rust_decimal::Decimal;

    use super::{CoinData, OutcomeStatus, evaluate};
    use crate::definition::{
        ComparisonOp, Condition, CrossDirection, DEFINITION_VERSION, Direction, DirectionFilter, KindFilter, Node,
        Operand, Predicate, PriceField, SignalDefinition,
    };

    fn candle(index: usize, close: f64) -> Candle {
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
            volume: Decimal::ZERO,
        }
    }

    fn candles(closes: &[f64]) -> Vec<Candle> {
        closes.iter().enumerate().map(|(index, &close)| candle(index, close)).collect()
    }

    fn sma_threshold(timeframe: Timeframe, value: f64, within_bars: u32) -> Condition {
        Condition {
            timeframe,
            indicator: Instance {
                id: "sma".to_owned(),
                params: [("period".to_owned(), 1.0)].into_iter().collect(),
            },
            predicate: Predicate::Threshold {
                left: Operand::Output { key: "sma".to_owned() },
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

        let recent_report = evaluate(&recent, "BTC", &data);
        assert!(recent_report.satisfied);
        assert_eq!(recent_report.conditions["hot"].status, OutcomeStatus::Satisfied);

        let stale_report = evaluate(&stale, "BTC", &data);
        assert!(!stale_report.satisfied);
        assert_eq!(stale_report.conditions["hot"].status, OutcomeStatus::NotSatisfied);
        assert_eq!(
            stale_report.conditions["hot"].last_true_bar,
            Utc.timestamp_opt(2 * 3_600, 0).single()
        );
    }

    #[test]
    fn detects_close_crossing_over_sma_on_latest_bar() {
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            indicator: Instance {
                id: "sma".to_owned(),
                params: [("period".to_owned(), 3.0)].into_iter().collect(),
            },
            predicate: Predicate::Cross {
                left: Operand::Field {
                    field: PriceField::Close,
                },
                right: Operand::Output { key: "sma".to_owned() },
                direction: CrossDirection::Over,
            },
            within_bars: 0,
        };
        let signal = definition(vec![("cross", condition)], Node::Condition { id: "cross".to_owned() });
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&[10.0, 10.0, 10.0, 5.0, 10.0]));

        let report = evaluate(&signal, "BTC", &data);
        assert!(report.satisfied, "expected a close-over-SMA cross on the final bar");
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

        let report = evaluate(&signal, "BTC", &data);
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
        assert!(!evaluate(&negated, "BTC", &CoinData::default()).satisfied);
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

        assert!(evaluate(&signal, "BTC", &data).satisfied);
    }

    #[test]
    fn divergence_filters_kind_and_direction() {
        // Strict highs at indices 6 and 8: price makes a higher high (6 -> 7)
        // while RSI(2) makes a lower high (~77 -> ~73), a regular bearish divergence.
        let closes = [10.0, 10.0, 10.0, 3.0, 3.0, 3.0, 6.0, 3.0, 7.0, 3.0];
        let params = [
            ("period".to_owned(), 2.0),
            ("pivotLeft".to_owned(), 1.0),
            ("pivotRight".to_owned(), 1.0),
            ("maxGap".to_owned(), 20.0),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let condition = Condition {
            timeframe: Timeframe::Hour1,
            indicator: Instance {
                id: "rsiDivergence".to_owned(),
                params,
            },
            predicate: Predicate::Divergence {
                kind: KindFilter::Regular,
                direction: DirectionFilter::Bearish,
            },
            within_bars: 1,
        };
        let signal = definition(vec![("div", condition)], Node::Condition { id: "div".to_owned() });
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, candles(&closes));

        let report = evaluate(&signal, "BTC", &data);
        assert!(report.satisfied);
        assert_eq!(report.conditions["div"].last_true_bar, Utc.timestamp_opt(8 * 3_600, 0).single());
    }
}
