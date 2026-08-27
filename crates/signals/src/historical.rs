//! Historical signal validation over one coin and an explicit UTC range.
//!
//! History hydration stays cache-first through [`CandleHistory`]. The requested
//! range is extended backwards by each timeframe's exact signal warm-up, while
//! evaluation advances on the definition's finest completed-bar grid and returns
//! matched-state transitions with their evidence.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use chrono::{DateTime, Duration, TimeZone, Utc};
use domain::Timeframe;
use hl_feed::{CandleHistory, HistoryError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::definition::{DefinitionError, SignalDefinition};
use crate::eval::{CoinData, EvalReport, OutcomeStatus, PreparedEvaluation, Truth};

/// Inclusive evaluation bounds expressed as UTC instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalRange {
    /// First instant whose matched state is included.
    pub start: DateTime<Utc>,
    /// Last instant whose completed-bar transition is included.
    pub end: DateTime<Utc>,
}

/// One contiguous period where the complete definition was matched.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalMatchSpan {
    /// Matched-state onset, clipped to the requested start when already active.
    pub started_at: DateTime<Utc>,
    /// First false/unknown evaluation, or the requested end while still active.
    pub ended_at: DateTime<Utc>,
    /// The definition was already matched immediately before the requested start.
    pub active_at_start: bool,
    /// The definition remained matched at the requested end.
    pub active_at_end: bool,
    /// Complete condition evidence at the onset or clipped range start.
    pub evidence: EvalReport,
}

/// Historical validation result for one definition snapshot and coin.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalValidation {
    /// Coin symbol.
    pub coin: String,
    /// Requested UTC range.
    pub range: HistoricalRange,
    /// Finest timeframe referenced by the definition; its closes drive evaluation.
    pub anchor_timeframe: Timeframe,
    /// Number of completed anchor-bar instants evaluated inside the range.
    pub evaluated_points: usize,
    /// Disjoint matched-state spans, oldest first.
    pub spans: Vec<HistoricalMatchSpan>,
}

/// Progress through one historical validation: hydration, then evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalProgress {
    /// One signal timeframe hydrated from the shared cache.
    Hydrated {
        /// Number of timeframes hydrated, including this one.
        completed: usize,
        /// Fixed number of timeframes required by the definition.
        total: usize,
        /// Timeframe just hydrated.
        timeframe: Timeframe,
    },
    /// A batch of anchor instants evaluated.
    Evaluated {
        /// Number of instants evaluated so far.
        completed: usize,
        /// Fixed number of instants in the range.
        total: usize,
    },
}

/// Instants between evaluation progress callbacks.
const EVALUATION_PROGRESS_INTERVAL: usize = 256;

/// Historical validation failures preserve the timeframe and required depth.
#[derive(Debug, Error)]
pub enum HistoricalValidationError {
    /// Definition validation failed.
    #[error(transparent)]
    Definition(#[from] DefinitionError),
    /// The range is empty or reversed.
    #[error("historical validation range start must be before its end")]
    InvalidRange,
    /// The range lies entirely after the last completed anchor bar.
    #[error("historical validation range ends before any {timeframe:?} candle completes")]
    RangeBeyondCompleted {
        /// Anchor timeframe whose completed bars bound the range.
        timeframe: Timeframe,
    },
    /// Timestamp or candle-count arithmetic exceeded supported bounds.
    #[error("historical validation range is outside the supported candle bounds")]
    RangeOverflow,
    /// Cache/database/provider history loading failed.
    #[error("failed to load historical candles for {timeframe:?}: {source}")]
    History {
        /// Affected timeframe.
        timeframe: Timeframe,
        /// Cache or provider failure.
        #[source]
        source: HistoryError,
    },
    /// The provider could not supply the complete range plus warm-up.
    #[error(
        "historical validation needs {required} contiguous {timeframe:?} candles but only {available} are available"
    )]
    InsufficientHistory {
        /// Affected timeframe.
        timeframe: Timeframe,
        /// Complete range plus warm-up depth.
        required: usize,
        /// Contiguous candles available immediately before the range end.
        available: usize,
    },
    /// A pure caller omitted a required timeframe.
    #[error("historical validation has no candle data for {timeframe:?}")]
    MissingData {
        /// Affected timeframe.
        timeframe: Timeframe,
    },
    /// A pure caller marked a required timeframe unavailable.
    #[error("historical validation data for {timeframe:?} is unavailable: {message}")]
    UnavailableData {
        /// Affected timeframe.
        timeframe: Timeframe,
        /// Original unavailability reason.
        message: String,
    },
    /// The progress consumer stopped accepting updates.
    #[error("historical validation stopped before completion")]
    Stopped,
}

fn utc_millis(timestamp_ms: i64) -> Result<DateTime<Utc>, HistoricalValidationError> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .ok_or(HistoricalValidationError::RangeOverflow)
}

fn anchor_timeframe(requirements: &BTreeMap<Timeframe, usize>) -> Result<Timeframe, HistoricalValidationError> {
    requirements
        .keys()
        .next()
        .copied()
        .ok_or(HistoricalValidationError::InvalidRange)
}

fn first_and_pre_range_evaluation(
    range: HistoricalRange,
    anchor: Timeframe,
) -> Result<(DateTime<Utc>, DateTime<Utc>), HistoricalValidationError> {
    let start_ms = range.start.timestamp_millis();
    let duration = anchor.duration_ms();
    let first_ms = if anchor.is_aligned(start_ms) {
        start_ms
    } else {
        anchor
            .bar_open_ms(start_ms)
            .checked_add(duration)
            .ok_or(HistoricalValidationError::RangeOverflow)?
    };
    let pre_range_ms = first_ms.checked_sub(duration).ok_or(HistoricalValidationError::RangeOverflow)?;
    Ok((utc_millis(first_ms)?, utc_millis(pre_range_ms)?))
}

/// Clamp the range end to the last completed anchor close at `now`.
fn clamp_to_completed(
    range: HistoricalRange,
    anchor: Timeframe,
    now: DateTime<Utc>,
) -> Result<HistoricalRange, HistoricalValidationError> {
    let last_completed_close_ms = anchor.bar_open_ms(now.timestamp_millis());
    if range.end.timestamp_millis() <= last_completed_close_ms {
        return Ok(range);
    }
    if range.start.timestamp_millis() >= last_completed_close_ms {
        return Err(HistoricalValidationError::RangeBeyondCompleted { timeframe: anchor });
    }
    Ok(HistoricalRange {
        start: range.start,
        end: utc_millis(last_completed_close_ms)?,
    })
}

fn bars_between(
    timeframe: Timeframe,
    from_boundary_ms: i64,
    to_boundary_ms: i64,
) -> Result<usize, HistoricalValidationError> {
    let distance = to_boundary_ms
        .checked_sub(from_boundary_ms)
        .ok_or(HistoricalValidationError::RangeOverflow)?;
    if distance < 0 || distance.rem_euclid(timeframe.duration_ms()) != 0 {
        return Err(HistoricalValidationError::RangeOverflow);
    }
    usize::try_from(distance / timeframe.duration_ms()).map_err(|_| HistoricalValidationError::RangeOverflow)
}

fn ensure_known_report(definition: &SignalDefinition, report: &EvalReport) -> Result<(), HistoricalValidationError> {
    if let Some(timeframe) = report.missing_data.first() {
        return Err(HistoricalValidationError::MissingData { timeframe: *timeframe });
    }
    if let Some((timeframe, message)) = report.unavailable_data.first_key_value() {
        return Err(HistoricalValidationError::UnavailableData {
            timeframe: *timeframe,
            message: message.clone(),
        });
    }
    for (id, outcome) in &report.conditions {
        if let OutcomeStatus::InsufficientHistory { required, available } = outcome.status
            && let Some(condition) = definition.conditions.get(id)
        {
            return Err(HistoricalValidationError::InsufficientHistory {
                timeframe: condition.timeframe,
                required,
                available,
            });
        }
    }
    Ok(())
}

/// Anchor instants in range; errors when any referenced timeframe's data ends
/// before the last evaluated instant, which would silently freeze stale state.
fn coverage_and_points(
    data: &CoinData,
    requirements: &BTreeMap<Timeframe, usize>,
    anchor: Timeframe,
    first_evaluation: DateTime<Utc>,
    pre_range_evaluation: DateTime<Utc>,
    range_end: DateTime<Utc>,
) -> Result<usize, HistoricalValidationError> {
    let duration = anchor.duration_ms();
    let first_ms = first_evaluation.timestamp_millis();
    let end_ms = range_end.timestamp_millis();
    let (points, last_instant_ms) = if first_ms > end_ms {
        (0, pre_range_evaluation.timestamp_millis())
    } else {
        let steps = end_ms.checked_sub(first_ms).ok_or(HistoricalValidationError::RangeOverflow)? / duration;
        let final_ms = steps
            .checked_mul(duration)
            .and_then(|offset| first_ms.checked_add(offset))
            .ok_or(HistoricalValidationError::RangeOverflow)?;
        let points = steps
            .checked_add(1)
            .ok_or(HistoricalValidationError::RangeOverflow)
            .and_then(|points| usize::try_from(points).map_err(|_| HistoricalValidationError::RangeOverflow))?;
        (points, final_ms)
    };
    for &timeframe in requirements.keys() {
        series_covers(data, timeframe, last_instant_ms)?;
    }
    Ok(points)
}

fn series_covers(data: &CoinData, timeframe: Timeframe, instant_ms: i64) -> Result<(), HistoricalValidationError> {
    let duration = timeframe.duration_ms();
    let needed_close_ms = timeframe.bar_open_ms(instant_ms);
    let candles = data.get(timeframe).unwrap_or_default();
    let Some(last) = candles.last() else {
        return Err(HistoricalValidationError::MissingData { timeframe });
    };
    let last_close_ms = last
        .open_time
        .timestamp_millis()
        .checked_add(duration)
        .ok_or(HistoricalValidationError::RangeOverflow)?;
    if last_close_ms < needed_close_ms {
        let missing_ms = needed_close_ms
            .checked_sub(last_close_ms)
            .ok_or(HistoricalValidationError::RangeOverflow)?;
        let missing = u64::try_from(missing_ms)
            .ok()
            .map(|elapsed| elapsed.div_ceil(duration.unsigned_abs()))
            .and_then(|bars| usize::try_from(bars).ok())
            .ok_or(HistoricalValidationError::RangeOverflow)?;
        return Err(HistoricalValidationError::InsufficientHistory {
            timeframe,
            required: candles
                .len()
                .checked_add(missing)
                .ok_or(HistoricalValidationError::RangeOverflow)?,
            available: candles.len(),
        });
    }
    Ok(())
}

/// Evaluate already-hydrated completed candles over a historical range.
pub fn evaluate_historical(
    definition: &SignalDefinition,
    coin: &str,
    data: &CoinData,
    range: HistoricalRange,
) -> Result<HistoricalValidation, HistoricalValidationError> {
    definition.validate()?;
    evaluate_historical_validated(definition, coin, data, range, &mut |_| ControlFlow::Continue(()))
}

fn evaluate_historical_validated<F>(
    definition: &SignalDefinition,
    coin: &str,
    data: &CoinData,
    range: HistoricalRange,
    on_progress: &mut F,
) -> Result<HistoricalValidation, HistoricalValidationError>
where
    F: FnMut(HistoricalProgress) -> ControlFlow<()>,
{
    if range.start >= range.end {
        return Err(HistoricalValidationError::InvalidRange);
    }
    let requirements = definition.required_history()?;
    let anchor = anchor_timeframe(&requirements)?;
    let (first_evaluation, pre_range_evaluation) = first_and_pre_range_evaluation(range, anchor)?;
    let prepared = PreparedEvaluation::build(definition, data)?;

    let pre_lengths = prepared.lengths_at(pre_range_evaluation);
    let pre_report = prepared.report_at(definition, coin, &pre_lengths);
    ensure_known_report(definition, &pre_report)?;
    let total_points =
        coverage_and_points(data, &requirements, anchor, first_evaluation, pre_range_evaluation, range.end)?;

    struct ActiveSpan {
        started_at: DateTime<Utc>,
        active_at_start: bool,
        evidence: EvalReport,
    }

    let mut active = pre_report.satisfied.then_some(ActiveSpan {
        started_at: range.start,
        active_at_start: true,
        evidence: pre_report,
    });
    let mut spans = Vec::new();
    let mut evaluated_points = 0;
    let mut evaluated_at = first_evaluation;
    while evaluated_at <= range.end {
        let lengths = prepared.lengths_at(evaluated_at);
        let satisfied = prepared.truth_at(definition, &lengths) == Truth::True;
        evaluated_points += 1;

        match (active.take(), satisfied) {
            (None, true) => {
                active = Some(ActiveSpan {
                    started_at: evaluated_at,
                    active_at_start: false,
                    evidence: prepared.report_at(definition, coin, &lengths),
                });
            }
            (Some(mut current), true) => {
                if current.active_at_start && evaluated_at == range.start {
                    current.evidence = prepared.report_at(definition, coin, &lengths);
                }
                active = Some(current);
            }
            (Some(current), false) => {
                if current.started_at < evaluated_at || !current.active_at_start {
                    spans.push(HistoricalMatchSpan {
                        started_at: current.started_at,
                        ended_at: evaluated_at,
                        active_at_start: current.active_at_start,
                        active_at_end: false,
                        evidence: current.evidence,
                    });
                }
            }
            (None, false) => {}
        }

        if evaluated_points % EVALUATION_PROGRESS_INTERVAL == 0
            && on_progress(HistoricalProgress::Evaluated {
                completed: evaluated_points,
                total: total_points,
            })
            .is_break()
        {
            return Err(HistoricalValidationError::Stopped);
        }

        let Some(next) = evaluated_at.checked_add_signed(Duration::milliseconds(anchor.duration_ms())) else {
            return Err(HistoricalValidationError::RangeOverflow);
        };
        evaluated_at = next;
    }

    if let Some(current) = active
        && (current.started_at < range.end || !current.active_at_start)
    {
        spans.push(HistoricalMatchSpan {
            started_at: current.started_at,
            ended_at: range.end,
            active_at_start: current.active_at_start,
            active_at_end: true,
            evidence: current.evidence,
        });
    }

    Ok(HistoricalValidation {
        coin: coin.to_owned(),
        range,
        anchor_timeframe: anchor,
        evaluated_points,
        spans,
    })
}

/// Hydrate a definition's exact range and warm-up, then evaluate one coin.
pub async fn validate_historical<F>(
    history: &CandleHistory,
    definition: &SignalDefinition,
    coin: &str,
    range: HistoricalRange,
    now: DateTime<Utc>,
    mut on_progress: F,
) -> Result<HistoricalValidation, HistoricalValidationError>
where
    F: FnMut(HistoricalProgress) -> ControlFlow<()>,
{
    if range.start >= range.end {
        return Err(HistoricalValidationError::InvalidRange);
    }
    let requirements = definition.required_history()?;
    let anchor = anchor_timeframe(&requirements)?;
    let range = clamp_to_completed(range, anchor, now)?;
    let (_, pre_range_evaluation) = first_and_pre_range_evaluation(range, anchor)?;
    let total = requirements.len();
    let mut data = CoinData::default();

    for (completed, (timeframe, warmup)) in requirements.into_iter().enumerate() {
        let pre_boundary = timeframe.bar_open_ms(pre_range_evaluation.timestamp_millis());
        let end_boundary = timeframe.bar_open_ms(range.end.timestamp_millis());
        let range_bars = bars_between(timeframe, pre_boundary, end_boundary)?;
        let required = warmup.checked_add(range_bars).ok_or(HistoricalValidationError::RangeOverflow)?;
        let candles = history
            .load_before(coin, timeframe, end_boundary, required)
            .await
            .map_err(|source| HistoricalValidationError::History { timeframe, source })?;
        if candles.len() < required {
            return Err(HistoricalValidationError::InsufficientHistory {
                timeframe,
                required,
                available: candles.len(),
            });
        }
        data.insert(timeframe, candles);
        if on_progress(HistoricalProgress::Hydrated {
            completed: completed + 1,
            total,
            timeframe,
        })
        .is_break()
        {
            return Err(HistoricalValidationError::Stopped);
        }
    }

    evaluate_historical_validated(definition, coin, &data, range, &mut on_progress)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::ControlFlow;
    use std::sync::{Arc, Mutex, PoisonError};

    use async_trait::async_trait;
    use chrono::{DateTime, TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use hl_feed::{CandleHistory, CandleProvider, FeedError};

    struct UnusedProvider;

    #[async_trait]
    impl CandleProvider for UnusedProvider {
        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            _before_ms: i64,
            _count: usize,
        ) -> Result<Vec<Candle>, FeedError> {
            panic!("validation hydration must be served from the cache in this test")
        }
    }
    use rust_decimal::Decimal;
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::{
        HistoricalRange, HistoricalValidationError, evaluate_historical, evaluate_historical_validated,
        validate_historical,
    };
    use crate::definition::{
        CandleField, ComparisonOp, Condition, DEFINITION_VERSION, Direction, Node, Operand, Predicate, SignalDefinition,
    };
    use crate::eval::{CoinData, ConditionEvidence};

    fn at_hour(hour: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(hour * 3_600, 0).single().expect("timestamp")
    }

    fn candle(timeframe: Timeframe, index: i64, close: f64) -> Candle {
        let open_time = Utc
            .timestamp_millis_opt(index * timeframe.duration_ms())
            .single()
            .expect("timestamp");
        let close = Decimal::from_f64_retain(close).expect("finite close");
        Candle {
            open_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: Decimal::ONE,
        }
    }

    fn threshold_condition(timeframe: Timeframe, threshold: f64) -> Condition {
        Condition {
            timeframe,
            predicate: Predicate::Threshold {
                left: Operand::Field {
                    field: CandleField::Close,
                    adjustment: None,
                },
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value: threshold },
            },
            within_bars: 0,
        }
    }

    fn definition(conditions: Vec<(&str, Condition)>, root: Node) -> SignalDefinition {
        SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bullish,
            root,
            conditions: conditions
                .into_iter()
                .map(|(id, condition)| (id.to_owned(), condition))
                .collect(),
        }
    }

    #[test]
    fn emits_onsets_and_contiguous_matched_spans() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let mut data = CoinData::default();
        data.insert(
            Timeframe::Hour1,
            [
                (-1, 90.0),
                (0, 90.0),
                (1, 110.0),
                (2, 120.0),
                (3, 90.0),
                (4, 110.0),
                (5, 120.0),
            ]
            .into_iter()
            .map(|(index, close)| candle(Timeframe::Hour1, index, close))
            .collect(),
        );

        let result = evaluate_historical(
            &signal,
            "BTC",
            &data,
            HistoricalRange {
                start: at_hour(1),
                end: at_hour(6),
            },
        )
        .expect("historical validation");

        assert_eq!(result.anchor_timeframe, Timeframe::Hour1);
        assert_eq!(result.evaluated_points, 6);
        assert_eq!(result.spans.len(), 2);
        assert_eq!(result.spans[0].started_at, at_hour(2));
        assert_eq!(result.spans[0].ended_at, at_hour(4));
        assert!(!result.spans[0].active_at_start);
        assert!(!result.spans[0].active_at_end);
        assert_eq!(result.spans[1].started_at, at_hour(5));
        assert_eq!(result.spans[1].ended_at, at_hour(6));
        assert!(result.spans[1].active_at_end);
        assert!(matches!(
            result.spans[0].evidence.conditions["price"].evidence,
            Some(ConditionEvidence::State {
                observed_at,
                left: 110.0,
                right: 100.0,
            }) if observed_at == at_hour(1)
        ));
    }

    #[test]
    fn distinguishes_a_signal_already_active_at_the_range_start() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let mut data = CoinData::default();
        data.insert(
            Timeframe::Hour1,
            [(-1, 110.0), (0, 120.0), (1, 130.0)]
                .into_iter()
                .map(|(index, close)| candle(Timeframe::Hour1, index, close))
                .collect(),
        );

        let result = evaluate_historical(
            &signal,
            "BTC",
            &data,
            HistoricalRange {
                start: at_hour(1),
                end: at_hour(2),
            },
        )
        .expect("historical validation");

        assert_eq!(result.spans.len(), 1);
        assert_eq!(result.spans[0].started_at, at_hour(1));
        assert_eq!(result.spans[0].ended_at, at_hour(2));
        assert!(result.spans[0].active_at_start);
        assert!(result.spans[0].active_at_end);
    }

    #[test]
    fn refuses_to_validate_when_warmup_before_the_range_is_missing() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let mut data = CoinData::default();
        data.insert(Timeframe::Hour1, vec![candle(Timeframe::Hour1, 0, 110.0)]);

        assert!(matches!(
            evaluate_historical(
                &signal,
                "BTC",
                &data,
                HistoricalRange {
                    start: at_hour(1),
                    end: at_hour(2),
                },
            ),
            Err(HistoricalValidationError::InsufficientHistory {
                timeframe: Timeframe::Hour1,
                required: 1,
                available: 0,
            })
        ));
    }

    #[test]
    fn higher_timeframe_state_changes_only_after_its_candle_completes() {
        let signal = definition(
            vec![
                ("hour", threshold_condition(Timeframe::Hour1, 0.0)),
                ("fourHour", threshold_condition(Timeframe::Hour4, 100.0)),
            ],
            Node::All {
                children: vec![
                    Node::Condition { id: "hour".to_owned() },
                    Node::Condition {
                        id: "fourHour".to_owned(),
                    },
                ],
            },
        );
        let mut data = CoinData::default();
        data.insert(
            Timeframe::Hour1,
            (-1..5).map(|index| candle(Timeframe::Hour1, index, 1.0)).collect(),
        );
        data.insert(
            Timeframe::Hour4,
            vec![candle(Timeframe::Hour4, -1, 90.0), candle(Timeframe::Hour4, 0, 110.0)],
        );

        let result = evaluate_historical(
            &signal,
            "BTC",
            &data,
            HistoricalRange {
                start: at_hour(1),
                end: at_hour(5),
            },
        )
        .expect("historical validation");

        assert_eq!(result.spans.len(), 1);
        assert_eq!(result.spans[0].started_at, at_hour(4));
        assert_eq!(result.spans[0].ended_at, at_hour(5));
    }

    struct CachedStore {
        candles: HashMap<Timeframe, Vec<Candle>>,
        requests: Mutex<Vec<(Timeframe, i64, usize)>>,
    }

    #[async_trait]
    impl CandleStore for CachedStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Ok(Vec::new())
        }

        async fn save(&self, _coin: &str, _timeframe: Timeframe, _candles: &[Candle]) -> Result<(), StoreError> {
            Ok(())
        }

        async fn candles_before(
            &self,
            _coin: &str,
            timeframe: Timeframe,
            before_ms: i64,
            limit: usize,
        ) -> Result<Vec<Candle>, StoreError> {
            self.requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((timeframe, before_ms, limit));
            let mut candles = self
                .candles
                .get(&timeframe)
                .into_iter()
                .flatten()
                .filter(|candle| candle.open_time.timestamp_millis() < before_ms)
                .rev()
                .take(limit)
                .cloned()
                .collect::<Vec<_>>();
            candles.reverse();
            Ok(candles)
        }
    }

    #[tokio::test]
    async fn hydration_requests_the_range_plus_exact_warmup_from_the_shared_cache() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let store = Arc::new(CachedStore {
            candles: HashMap::from([(
                Timeframe::Hour1,
                (0..5).map(|index| candle(Timeframe::Hour1, index, 110.0)).collect(),
            )]),
            requests: Mutex::new(Vec::new()),
        });
        let persistence: Arc<dyn CandleStore> = store.clone();
        let history = CandleHistory::new(persistence, Arc::new(UnusedProvider));
        let mut progress = Vec::new();

        let result = validate_historical(
            &history,
            &signal,
            "BTC",
            HistoricalRange {
                start: at_hour(2),
                end: at_hour(5),
            },
            at_hour(5),
            |update| {
                progress.push(update);
                ControlFlow::Continue(())
            },
        )
        .await
        .expect("cached historical validation");

        assert_eq!(result.spans.len(), 1);
        assert!(result.spans[0].active_at_start);
        assert_eq!(progress.len(), 1);
        assert_eq!(
            store.requests.lock().unwrap_or_else(PoisonError::into_inner).as_slice(),
            &[(Timeframe::Hour1, at_hour(5).timestamp_millis(), 5)]
        );
    }

    #[tokio::test]
    async fn future_range_end_is_clamped_to_the_last_completed_anchor_close() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let store = Arc::new(CachedStore {
            candles: HashMap::from([(
                Timeframe::Hour1,
                (0..5).map(|index| candle(Timeframe::Hour1, index, 110.0)).collect(),
            )]),
            requests: Mutex::new(Vec::new()),
        });
        let persistence: Arc<dyn CandleStore> = store.clone();
        let history = CandleHistory::new(persistence, Arc::new(UnusedProvider));

        let result = validate_historical(
            &history,
            &signal,
            "BTC",
            HistoricalRange {
                start: at_hour(2),
                end: at_hour(8),
            },
            at_hour(5),
            |_| ControlFlow::Continue(()),
        )
        .await
        .expect("clamped historical validation");

        assert_eq!(result.range.end, at_hour(5));
        assert_eq!(
            store.requests.lock().unwrap_or_else(PoisonError::into_inner).as_slice(),
            &[(Timeframe::Hour1, at_hour(5).timestamp_millis(), 5)]
        );
    }

    #[tokio::test]
    async fn fully_future_range_is_rejected_before_any_fetch() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let store = Arc::new(CachedStore {
            candles: HashMap::new(),
            requests: Mutex::new(Vec::new()),
        });
        let persistence: Arc<dyn CandleStore> = store.clone();
        let history = CandleHistory::new(persistence, Arc::new(UnusedProvider));

        let result = validate_historical(
            &history,
            &signal,
            "BTC",
            HistoricalRange {
                start: at_hour(6),
                end: at_hour(8),
            },
            at_hour(5),
            |_| ControlFlow::Continue(()),
        )
        .await;

        assert!(matches!(
            result,
            Err(HistoricalValidationError::RangeBeyondCompleted {
                timeframe: Timeframe::Hour1
            })
        ));
        assert!(store.requests.lock().unwrap_or_else(PoisonError::into_inner).is_empty());
    }

    #[test]
    fn truncated_higher_timeframe_data_is_rejected_instead_of_freezing_stale_state() {
        let signal = definition(
            vec![
                ("hourly", threshold_condition(Timeframe::Hour1, 100.0)),
                ("fourHourly", threshold_condition(Timeframe::Hour4, 100.0)),
            ],
            Node::All {
                children: vec![
                    Node::Condition {
                        id: "hourly".to_owned(),
                    },
                    Node::Condition {
                        id: "fourHourly".to_owned(),
                    },
                ],
            },
        );
        let mut data = CoinData::default();
        data.insert(
            Timeframe::Hour1,
            (-1..12).map(|index| candle(Timeframe::Hour1, index, 110.0)).collect(),
        );
        data.insert(
            Timeframe::Hour4,
            vec![candle(Timeframe::Hour4, -1, 110.0), candle(Timeframe::Hour4, 0, 110.0)],
        );

        let result = evaluate_historical(
            &signal,
            "BTC",
            &data,
            HistoricalRange {
                start: at_hour(1),
                end: at_hour(12),
            },
        );

        assert!(matches!(
            result,
            Err(HistoricalValidationError::InsufficientHistory {
                timeframe: Timeframe::Hour4,
                required: 4,
                available: 2,
            })
        ));
    }

    #[test]
    fn evaluation_stops_when_the_progress_consumer_breaks_mid_range() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let bars = 600i64;
        let mut data = CoinData::default();
        data.insert(
            Timeframe::Hour1,
            (-1..bars).map(|index| candle(Timeframe::Hour1, index, 110.0)).collect(),
        );
        let range = HistoricalRange {
            start: at_hour(1),
            end: at_hour(bars),
        };

        signal.validate().expect("valid fixture definition");
        let mut updates = 0usize;
        let result = evaluate_historical_validated(&signal, "BTC", &data, range, &mut |_| {
            updates += 1;
            ControlFlow::Break(())
        });
        assert!(matches!(result, Err(HistoricalValidationError::Stopped)));
        assert_eq!(updates, 1);
    }

    #[test]
    fn serialized_contract_uses_camel_case_timestamps_and_span_flags() {
        let signal = definition(
            vec![("price", threshold_condition(Timeframe::Hour1, 100.0))],
            Node::Condition { id: "price".to_owned() },
        );
        let mut data = CoinData::default();
        data.insert(
            Timeframe::Hour1,
            vec![
                candle(Timeframe::Hour1, -1, 110.0),
                candle(Timeframe::Hour1, 0, 110.0),
                candle(Timeframe::Hour1, 1, 110.0),
            ],
        );
        let result = evaluate_historical(
            &signal,
            "BTC",
            &data,
            HistoricalRange {
                start: at_hour(1),
                end: at_hour(2),
            },
        )
        .expect("historical validation");
        let json = serde_json::to_value(result).expect("serializable validation");

        assert_eq!(json["anchorTimeframe"], "1h");
        assert_eq!(json["range"]["start"], "1970-01-01T01:00:00Z");
        assert_eq!(json["spans"][0]["activeAtStart"], true);
        assert_eq!(json["spans"][0]["activeAtEnd"], true);
        assert_eq!(
            json["spans"][0]["evidence"]["conditions"]["price"]["evidence"]["observedAt"],
            "1970-01-01T00:00:00Z"
        );
    }
}
