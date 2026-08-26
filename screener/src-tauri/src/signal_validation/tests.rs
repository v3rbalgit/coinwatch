use chrono::{TimeZone, Utc};
use domain::Timeframe;
use serde_json::json;
use signals::{HistoricalRange, HistoricalValidation};
use tokio::sync::watch;

use super::{SignalValidationEvent, SignalValidationFailure, SignalValidationState, StartValidationError};

#[test]
fn validation_state_allows_one_active_job_and_releases_the_slot() {
    let state = SignalValidationState::default();
    let (cancel, cancellation) = watch::channel(false);
    let id = state.begin(cancel).expect("first validation");
    let (other, _) = watch::channel(false);
    assert_eq!(state.begin(other), Err(StartValidationError::AlreadyActive));
    assert!(state.cancel(id));
    assert!(cancellation.has_changed().expect("cancellation sender remains active"));
    state.finish(id);

    let (next, _) = watch::channel(false);
    assert_eq!(state.begin(next), Ok(id + 1));
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
}
