//! Cancellable historical signal-validation lifecycle and typed Tauri channel.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, PoisonError};

use domain::Timeframe;
use hl_feed::CandleHistory;
use serde::Serialize;
use signals::{
    HistoricalProgress, HistoricalRange, HistoricalValidation, HistoricalValidationError, SignalDefinition,
    validate_historical,
};
use tauri::ipc::Channel;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum SignalValidationFailure {
    InvalidDefinition {
        message: String,
    },
    InvalidRange,
    HistoryUnavailable {
        timeframe: Timeframe,
        message: String,
    },
    InsufficientHistory {
        timeframe: Timeframe,
        required: usize,
        available: usize,
    },
    Internal {
        message: String,
    },
}

impl From<HistoricalValidationError> for SignalValidationFailure {
    fn from(error: HistoricalValidationError) -> Self {
        match error {
            HistoricalValidationError::Definition(error) => Self::InvalidDefinition {
                message: error.to_string(),
            },
            HistoricalValidationError::InvalidRange => Self::InvalidRange,
            HistoricalValidationError::History { timeframe, source } => Self::HistoryUnavailable {
                timeframe,
                message: source.to_string(),
            },
            HistoricalValidationError::InsufficientHistory {
                timeframe,
                required,
                available,
            } => Self::InsufficientHistory {
                timeframe,
                required,
                available,
            },
            HistoricalValidationError::MissingData { timeframe } => Self::HistoryUnavailable {
                timeframe,
                message: "no completed candle data was loaded".to_owned(),
            },
            HistoricalValidationError::UnavailableData { timeframe, message } => {
                Self::HistoryUnavailable { timeframe, message }
            }
            HistoricalValidationError::RangeOverflow => Self::Internal {
                message: "the requested range is outside the supported candle bounds".to_owned(),
            },
            HistoricalValidationError::Stopped => Self::Internal {
                message: "validation progress could not be delivered".to_owned(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(
    tag = "event",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum SignalValidationEvent {
    Started {
        validation_id: u32,
        total: usize,
    },
    Hydrated {
        validation_id: u32,
        completed: usize,
        total: usize,
        timeframe: Timeframe,
    },
    Completed {
        validation_id: u32,
        result: HistoricalValidation,
    },
    Cancelled {
        validation_id: u32,
    },
    Failed {
        validation_id: u32,
        failure: SignalValidationFailure,
    },
}

struct ActiveValidation {
    id: u32,
    cancel: watch::Sender<bool>,
}

#[derive(Default)]
struct SignalValidationInner {
    next_id: u32,
    active: Option<ActiveValidation>,
}

#[derive(Default)]
pub(crate) struct SignalValidationState {
    inner: Mutex<SignalValidationInner>,
}

#[derive(Debug, PartialEq, Eq, Error)]
enum StartValidationError {
    #[error("historical signal validation is already active")]
    AlreadyActive,
    #[error("historical signal validation ID space is exhausted")]
    IdExhausted,
    #[error("a coin symbol is required for historical signal validation")]
    EmptyCoin,
}

impl SignalValidationState {
    fn begin(&self, cancel: watch::Sender<bool>) -> Result<u32, StartValidationError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.active.is_some() {
            return Err(StartValidationError::AlreadyActive);
        }
        let id = inner.next_id.checked_add(1).ok_or(StartValidationError::IdExhausted)?;
        inner.next_id = id;
        inner.active = Some(ActiveValidation { id, cancel });
        Ok(id)
    }

    fn cancel(&self, id: u32) -> bool {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .active
            .as_ref()
            .filter(|validation| validation.id == id)
            .is_some_and(|validation| validation.cancel.send(true).is_ok())
    }

    fn finish(&self, id: u32) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.active.as_ref().is_some_and(|validation| validation.id == id) {
            inner.active = None;
        }
    }
}

struct ActiveValidationGuard {
    state: Arc<SignalValidationState>,
    id: u32,
    active: bool,
}

impl ActiveValidationGuard {
    fn finish(&mut self) {
        if self.active {
            self.state.finish(self.id);
            self.active = false;
        }
    }
}

impl Drop for ActiveValidationGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

fn send_event(events: &Channel<SignalValidationEvent>, event: SignalValidationEvent) -> bool {
    match events.send(event) {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(%error, "failed to send historical signal validation event");
            false
        }
    }
}

enum ValidationOutcome {
    Cancelled,
    Finished(Result<HistoricalValidation, HistoricalValidationError>),
}

async fn validation_worker(
    validation_id: u32,
    history: Arc<CandleHistory>,
    definition: SignalDefinition,
    coin: String,
    range: HistoricalRange,
    mut cancellation: watch::Receiver<bool>,
    events: Channel<SignalValidationEvent>,
) -> SignalValidationEvent {
    let progress_events = events.clone();
    let validation = validate_historical(&history, &definition, &coin, range, |progress: HistoricalProgress| {
        if send_event(
            &progress_events,
            SignalValidationEvent::Hydrated {
                validation_id,
                completed: progress.completed,
                total: progress.total,
                timeframe: progress.timeframe,
            },
        ) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    });
    tokio::pin!(validation);
    let outcome = tokio::select! {
        biased;
        _ = cancellation.changed() => ValidationOutcome::Cancelled,
        result = &mut validation => ValidationOutcome::Finished(result),
    };

    match outcome {
        ValidationOutcome::Cancelled => SignalValidationEvent::Cancelled { validation_id },
        ValidationOutcome::Finished(Ok(result)) => SignalValidationEvent::Completed { validation_id, result },
        ValidationOutcome::Finished(Err(error)) => SignalValidationEvent::Failed {
            validation_id,
            failure: error.into(),
        },
    }
}

async fn execute_validation(
    validation_id: u32,
    state: Arc<SignalValidationState>,
    history: Arc<CandleHistory>,
    definition: SignalDefinition,
    coin: String,
    range: HistoricalRange,
    cancellation: watch::Receiver<bool>,
    events: Channel<SignalValidationEvent>,
) {
    let mut guard = ActiveValidationGuard {
        state,
        id: validation_id,
        active: true,
    };
    let worker = tokio::spawn(validation_worker(
        validation_id,
        history,
        definition,
        coin,
        range,
        cancellation,
        events.clone(),
    ));
    let terminal = match worker.await {
        Ok(terminal) => terminal,
        Err(error) => {
            tracing::error!(validation_id, %error, "historical signal validation worker failed");
            SignalValidationEvent::Failed {
                validation_id,
                failure: SignalValidationFailure::Internal {
                    message: error.to_string(),
                },
            }
        }
    };

    guard.finish();
    send_event(&events, terminal);
}

#[tauri::command]
pub(crate) fn start_signal_validation(
    state: tauri::State<'_, Arc<SignalValidationState>>,
    history: tauri::State<'_, Arc<CandleHistory>>,
    definition: SignalDefinition,
    coin: String,
    range: HistoricalRange,
    on_event: Channel<SignalValidationEvent>,
) -> Result<u32, String> {
    if coin.trim().is_empty() {
        return Err(StartValidationError::EmptyCoin.to_string());
    }
    let total = definition
        .conditions
        .values()
        .map(|condition| condition.timeframe)
        .collect::<BTreeSet<_>>()
        .len();
    let (cancel, cancellation) = watch::channel(false);
    let validation_id = state.begin(cancel).map_err(|error| error.to_string())?;
    if let Err(error) = on_event.send(SignalValidationEvent::Started { validation_id, total }) {
        state.finish(validation_id);
        return Err(format!("failed to start historical signal validation event stream: {error}"));
    }

    tauri::async_runtime::spawn(execute_validation(
        validation_id,
        Arc::clone(state.inner()),
        Arc::clone(history.inner()),
        definition,
        coin,
        range,
        cancellation,
        on_event,
    ));
    Ok(validation_id)
}

#[tauri::command]
pub(crate) fn cancel_signal_validation(
    state: tauri::State<'_, Arc<SignalValidationState>>,
    validation_id: u32,
) -> bool {
    state.cancel(validation_id)
}

#[cfg(test)]
mod tests;
