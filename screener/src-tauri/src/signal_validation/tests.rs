use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use domain::{Candle, Timeframe};
use hl_feed::{CandleHistory, CandleProvider, FeedError};
use rust_decimal::Decimal;
use serde_json::json;
use signals::{HistoricalRange, HistoricalValidation, SignalDefinition};
use storage::{CandleSeries, CandleStore, StoreError};
use tauri::ipc::{Channel, InvokeResponseBody};
use tokio::sync::watch;

use super::{
    SignalValidationEvent, SignalValidationFailure, SignalValidationState, StartValidationError, validation_worker,
};
use crate::task_lifecycle::{BeginError, HasTaskState};

#[test]
fn validation_state_allows_one_active_job_and_releases_the_slot() {
    let state = SignalValidationState::default();
    let (cancel, cancellation) = watch::channel(false);
    let id = state.begin(cancel).expect("first validation");
    let (other, _) = watch::channel(false);
    assert_eq!(state.begin(other), Err(BeginError::AlreadyActive));
    assert!(state.cancel(id));
    assert!(cancellation.has_changed().expect("cancellation sender remains active"));
    state.finish(id);

    let (next, _) = watch::channel(false);
    assert_eq!(state.begin(next), Ok(id + 1));
}

struct SeededStore {
    candles: Vec<Candle>,
}

#[async_trait]
impl CandleStore for SeededStore {
    async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
        Ok(Vec::new())
    }

    async fn save(&self, _coin: &str, _timeframe: Timeframe, _candles: &[Candle]) -> Result<(), StoreError> {
        Ok(())
    }

    async fn candles_before(
        &self,
        _coin: &str,
        _timeframe: Timeframe,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, StoreError> {
        let mut candles: Vec<Candle> = self
            .candles
            .iter()
            .rev()
            .filter(|candle| candle.open_time.timestamp_millis() < before_ms)
            .take(limit)
            .cloned()
            .collect();
        candles.reverse();
        Ok(candles)
    }
}

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

fn hourly_candle(hour: i64) -> Candle {
    Candle {
        open_time: Utc.timestamp_opt(hour * 3_600, 0).single().expect("valid timestamp"),
        open: Decimal::ONE_HUNDRED,
        high: Decimal::ONE_HUNDRED,
        low: Decimal::ONE_HUNDRED,
        close: Decimal::ONE_HUNDRED,
        volume: Decimal::ONE,
    }
}

fn threshold_definition() -> SignalDefinition {
    serde_json::from_value(json!({
        "version": 3,
        "direction": "bullish",
        "root": { "type": "condition", "id": "price" },
        "conditions": {
            "price": {
                "timeframe": "1h",
                "predicate": {
                    "type": "threshold",
                    "left": { "type": "field", "field": "close" },
                    "op": "greaterThan",
                    "right": { "type": "constant", "value": 50.0 }
                },
                "withinBars": 0
            }
        }
    }))
    .expect("valid signal definition")
}

#[tokio::test]
async fn cancellation_during_evaluation_yields_a_cancelled_terminal() {
    let store = Arc::new(SeededStore {
        candles: (-1..500).map(hourly_candle).collect(),
    });
    let history = Arc::new(CandleHistory::new(store, Arc::new(UnusedProvider)));
    let (cancel, cancellation) = watch::channel(false);
    let events = Channel::new(move |body| {
        let InvokeResponseBody::Json(payload) = body else {
            panic!("validation events should be JSON");
        };
        if payload.contains("\"evaluated\"") {
            cancel.send(true).expect("worker retains cancellation receiver");
        }
        Ok(())
    });

    let terminal = validation_worker(
        21,
        history,
        threshold_definition(),
        "BTC".to_owned(),
        HistoricalRange {
            start: Utc.timestamp_opt(3_600, 0).single().expect("start"),
            end: Utc.timestamp_opt(500 * 3_600, 0).single().expect("end"),
        },
        cancellation,
        events,
    )
    .await;

    assert!(matches!(terminal, SignalValidationEvent::Cancelled { validation_id: 21 }));
}

#[test]
fn start_error_and_new_event_serialization_match_the_frontend_contract() {
    assert_eq!(
        serde_json::to_value(StartValidationError::AlreadyActive).expect("serializable start error"),
        json!({ "type": "alreadyActive" })
    );
    assert_eq!(
        serde_json::to_value(StartValidationError::EventStream {
            message: "closed".to_owned()
        })
        .expect("serializable start error"),
        json!({ "type": "eventStream", "message": "closed" })
    );
    assert_eq!(
        serde_json::to_value(SignalValidationEvent::Evaluated {
            validation_id: 4,
            completed: 256,
            total: 500,
        })
        .expect("serializable evaluated event"),
        json!({
            "event": "evaluated",
            "data": { "validationId": 4, "completed": 256, "total": 500 }
        })
    );
    assert_eq!(
        serde_json::to_value(SignalValidationFailure::RangeBeyondCompleted {
            timeframe: Timeframe::Hour4,
        })
        .expect("serializable failure"),
        json!({ "type": "rangeBeyondCompleted", "timeframe": "4h" })
    );
}

#[test]
fn validation_events_and_failures_match_the_frontend_contract() {
    let range = HistoricalRange {
        start: Utc.timestamp_opt(3_600, 0).single().expect("start"),
        end: Utc.timestamp_opt(7_200, 0).single().expect("end"),
    };
    let result = HistoricalValidation {
        coin: "BTC".to_owned(),
        range,
        anchor_timeframe: Timeframe::Hour1,
        evaluated_points: 2,
        spans: Vec::new(),
    };

    assert_eq!(
        serde_json::to_value(SignalValidationEvent::Completed {
            validation_id: 7,
            result,
        })
        .expect("serializable completion"),
        json!({
            "event": "completed",
            "data": {
                "validationId": 7,
                "result": {
                    "coin": "BTC",
                    "range": {
                        "start": "1970-01-01T01:00:00Z",
                        "end": "1970-01-01T02:00:00Z",
                    },
                    "anchorTimeframe": "1h",
                    "evaluatedPoints": 2,
                    "spans": [],
                }
            }
        })
    );
    assert_eq!(
        serde_json::to_value(SignalValidationEvent::Failed {
            validation_id: 8,
            failure: SignalValidationFailure::InsufficientHistory {
                timeframe: Timeframe::Week1,
                required: 30,
                available: 12,
            },
        })
        .expect("serializable failure"),
        json!({
            "event": "failed",
            "data": {
                "validationId": 8,
                "failure": {
                    "type": "insufficientHistory",
                    "timeframe": "1w",
                    "required": 30,
                    "available": 12,
                }
            }
        })
    );
    assert_eq!(
        serde_json::to_value(SignalValidationEvent::Failed {
            validation_id: 9,
            failure: SignalValidationFailure::UndefinedOperands {
                condition: "price".to_owned(),
                timeframe: Timeframe::Hour1,
                evaluated_at: Utc.timestamp_opt(14_400, 0).single().expect("evaluated instant"),
            },
        })
        .expect("serializable undefined-operands failure"),
        json!({
            "event": "failed",
            "data": {
                "validationId": 9,
                "failure": {
                    "type": "undefinedOperands",
                    "condition": "price",
                    "timeframe": "1h",
                    "evaluatedAt": "1970-01-01T04:00:00Z",
                }
            }
        })
    );
}
