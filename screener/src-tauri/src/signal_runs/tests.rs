use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use domain::{Candle, Timeframe};
use hl_feed::{CandleHistory, CandleProvider, FeedError, MarketStore};
use serde_json::json;
use signals::{ConditionOutcome, EvalReport, OutcomeStatus, SignalDefinition};
use storage::{CandleSeries, CandleStore, StoreError};
use tauri::ipc::{Channel, InvokeResponseBody};
use tokio::sync::watch;

use super::{SignalRunEvent, SignalRunScope, SignalRunState, StartRunError, execute_run, run_worker};
use crate::task_lifecycle::{BeginError, HasTaskState, TaskGuard};

struct NoopStore;

#[async_trait]
impl CandleStore for NoopStore {
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
        _before_ms: i64,
        _limit: usize,
    ) -> Result<Vec<Candle>, StoreError> {
        Ok(Vec::new())
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
        panic!("run hydration must not reach the network in tests")
    }
}

fn history() -> Arc<CandleHistory> {
    Arc::new(CandleHistory::new(Arc::new(NoopStore), Arc::new(UnusedProvider)))
}

fn definition() -> SignalDefinition {
    serde_json::from_value(json!({
        "version": 3,
        "direction": "bullish",
        "root": { "type": "condition", "id": "trend" },
        "conditions": {
            "trend": {
                "timeframe": "1h",
                "predicate": {
                    "type": "threshold",
                    "left": {
                        "type": "indicator",
                        "instance": { "id": "sma", "params": { "period": 1.0 } },
                        "output": "sma"
                    },
                    "op": "greaterThan",
                    "right": { "type": "constant", "value": 0.0 }
                },
                "withinBars": 0
            }
        }
    }))
    .expect("valid signal definition")
}

fn counting_channel(messages: Arc<AtomicUsize>) -> Channel<SignalRunEvent> {
    Channel::new(move |_body| {
        messages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    })
}

#[tokio::test]
async fn one_active_run_is_cancelled_by_matching_id_and_releases_on_finish() {
    let state = SignalRunState::default();
    let (first_cancel, mut first_cancellation) = watch::channel(false);
    let first = state.begin(first_cancel).expect("first run starts");
    let (blocked_cancel, _blocked_cancellation) = watch::channel(false);

    assert_eq!(state.begin(blocked_cancel), Err(BeginError::AlreadyActive));
    assert!(!state.cancel(first + 1));
    assert!(state.cancel(first));
    first_cancellation.changed().await.expect("cancellation sender remains active");
    assert!(*first_cancellation.borrow());

    state.finish(first);
    let (second_cancel, _second_cancellation) = watch::channel(false);
    assert_eq!(state.begin(second_cancel).expect("second run starts"), first + 1);
}

#[test]
fn active_guard_releases_the_run_slot_on_drop() {
    let state = Arc::new(SignalRunState::default());
    let (first_cancel, _first_cancellation) = watch::channel(false);
    let first = state.begin(first_cancel).expect("first run starts");
    {
        let _guard = TaskGuard {
            state: Arc::clone(&state),
            id: first,
            active: true,
        };
    }

    let (second_cancel, _second_cancellation) = watch::channel(false);
    assert_eq!(state.begin(second_cancel).expect("guard released run"), first + 1);
}

#[tokio::test]
async fn worker_panic_becomes_a_failed_terminal_and_releases_the_run_slot() {
    let state = Arc::new(SignalRunState::default());
    let (cancel, cancellation) = watch::channel(false);
    let run_id = state.begin(cancel).expect("run starts");
    let calls = Arc::new(AtomicUsize::new(0));
    let terminal = Arc::new(Mutex::new(None));
    let callback_calls = Arc::clone(&calls);
    let callback_terminal = Arc::clone(&terminal);
    let events = Channel::new(move |body| {
        if callback_calls.fetch_add(1, Ordering::Relaxed) == 0 {
            panic!("synthetic progress-delivery panic");
        }
        let InvokeResponseBody::Json(payload) = body else {
            panic!("terminal event should be JSON");
        };
        let event: serde_json::Value = serde_json::from_str(&payload)?;
        *callback_terminal.lock().unwrap_or_else(PoisonError::into_inner) = Some(event);
        Ok(())
    });

    execute_run(
        run_id,
        Arc::clone(&state),
        MarketStore::new(),
        history(),
        definition(),
        vec!["BTC".to_owned()],
        cancellation,
        events,
    )
    .await;

    assert_eq!(calls.load(Ordering::Relaxed), 2);
    let event = terminal
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("failed terminal event");
    assert_eq!(event.pointer("/event"), Some(&json!("failed")));
    assert_eq!(event.pointer("/data/runId"), Some(&json!(run_id)));
    let (next_cancel, _next_cancellation) = watch::channel(false);
    assert_eq!(state.begin(next_cancel).expect("panic released run"), run_id + 1);
}

#[tokio::test]
async fn worker_streams_each_coin_and_returns_one_terminal_event() {
    let messages = Arc::new(AtomicUsize::new(0));
    let events = counting_channel(Arc::clone(&messages));
    let (_cancel, cancellation) = watch::channel(false);

    let terminal = run_worker(
        9,
        MarketStore::new(),
        history(),
        definition(),
        vec!["BTC".to_owned()],
        cancellation,
        events,
    )
    .await;

    assert_eq!(messages.load(Ordering::Relaxed), 1);
    assert!(matches!(
        terminal,
        SignalRunEvent::Completed {
            run_id: 9,
            completed: 1,
            total: 1,
            matched: 0,
        }
    ));
}

#[tokio::test]
async fn worker_honors_cancellation_before_evaluating_a_coin() {
    let messages = Arc::new(AtomicUsize::new(0));
    let events = counting_channel(Arc::clone(&messages));
    let (cancel, cancellation) = watch::channel(false);
    cancel.send(true).expect("worker retains cancellation receiver");

    let terminal = run_worker(
        10,
        MarketStore::new(),
        history(),
        definition(),
        vec!["BTC".to_owned()],
        cancellation,
        events,
    )
    .await;

    assert_eq!(messages.load(Ordering::Relaxed), 0);
    assert!(matches!(
        terminal,
        SignalRunEvent::Cancelled {
            run_id: 10,
            completed: 0,
            total: 1,
            matched: 0,
        }
    ));
}

#[tokio::test]
async fn worker_honors_cancellation_requested_by_a_progress_consumer() {
    let messages = Arc::new(AtomicUsize::new(0));
    let callback_messages = Arc::clone(&messages);
    let (cancel, cancellation) = watch::channel(false);
    let callback_cancel = cancel.clone();
    let events = Channel::new(move |_body| {
        callback_messages.fetch_add(1, Ordering::Relaxed);
        callback_cancel.send(true).expect("worker retains cancellation receiver");
        Ok(())
    });

    let terminal = run_worker(
        11,
        MarketStore::new(),
        history(),
        definition(),
        vec!["BTC".to_owned(), "ETH".to_owned()],
        cancellation,
        events,
    )
    .await;

    assert_eq!(messages.load(Ordering::Relaxed), 1);
    assert!(matches!(
        terminal,
        SignalRunEvent::Cancelled {
            run_id: 11,
            completed: 1,
            total: 2,
            matched: 0,
        }
    ));
}

#[test]
fn explicit_scope_rejects_empty_and_duplicate_coins() {
    let store = MarketStore::new();
    assert_eq!(
        SignalRunScope::Coins(vec![String::new()]).resolve(&store),
        Err(StartRunError::EmptyCoin)
    );
    assert_eq!(
        SignalRunScope::Coins(vec!["BTC".to_owned(), "BTC".to_owned()]).resolve(&store),
        Err(StartRunError::DuplicateCoin { coin: "BTC".to_owned() })
    );
}

#[test]
fn start_error_serialization_matches_the_frontend_contract() {
    assert_eq!(
        serde_json::to_value(StartRunError::AlreadyActive).expect("serializable start error"),
        json!({ "type": "alreadyActive" })
    );
    assert_eq!(
        serde_json::to_value(StartRunError::DuplicateCoin { coin: "BTC".to_owned() })
            .expect("serializable start error"),
        json!({ "type": "duplicateCoin", "coin": "BTC" })
    );
}

#[test]
fn evaluated_event_serialization_matches_the_frontend_contract() {
    let report = EvalReport {
        coin: "BTC".to_owned(),
        satisfied: false,
        last_evidence_at: None,
        conditions: BTreeMap::from([(
            "trend".to_owned(),
            ConditionOutcome {
                status: OutcomeStatus::InsufficientHistory {
                    required: 12,
                    available: 7,
                },
                last_true_bar: None,
                evidence: None,
            },
        )]),
        missing_data: vec![Timeframe::Week1],
        unavailable_data: BTreeMap::from([(Timeframe::Hour4, "provider unavailable".to_owned())]),
    };

    assert_eq!(
        serde_json::to_value(SignalRunEvent::Evaluated {
            run_id: 7,
            completed: 3,
            total: 10,
            report,
        })
        .expect("serializable evaluated event"),
        json!({
            "event": "evaluated",
            "data": {
                "runId": 7,
                "completed": 3,
                "total": 10,
                "report": {
                    "coin": "BTC",
                    "satisfied": false,
                    "lastEvidenceAt": null,
                    "conditions": {
                        "trend": {
                            "status": "insufficientHistory",
                            "required": 12,
                            "available": 7,
                            "lastTrueBar": null,
                            "evidence": null,
                        }
                    },
                    "missingData": ["1w"],
                    "unavailableData": {
                        "4h": "provider unavailable",
                    }
                }
            }
        })
    );
}

#[test]
fn scope_and_terminal_event_serialization_are_stable() {
    let scope: SignalRunScope = serde_json::from_value(json!({
        "type": "coins",
        "coins": ["BTC", "ETH"],
    }))
    .expect("valid explicit scope");
    assert!(matches!(scope, SignalRunScope::Coins(coins) if coins == ["BTC", "ETH"]));

    assert_eq!(
        serde_json::to_value(SignalRunEvent::Cancelled {
            run_id: 7,
            completed: 3,
            total: 10,
            matched: 1,
        })
        .expect("serializable run event"),
        json!({
            "event": "cancelled",
            "data": {
                "runId": 7,
                "completed": 3,
                "total": 10,
                "matched": 1,
            }
        })
    );
}
