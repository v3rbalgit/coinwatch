//! Cancellable historical signal-validation lifecycle and typed Tauri channel.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use chrono::{DateTime, Utc};
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

use crate::task_lifecycle::{BeginError, HasTaskState, SingleTaskState, run_supervised, send_event};

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum SignalValidationFailure {
    InvalidDefinition {
        message: String,
    },
    InvalidRange,
    RangeBeyondCompleted {
        timeframe: Timeframe,
    },
    HistoryUnavailable {
        timeframe: Timeframe,
        message: String,
    },
    InsufficientHistory {
        timeframe: Timeframe,
        required: usize,
        available: usize,
    },
    UndefinedOperands {
        condition: String,
        timeframe: Timeframe,
        evaluated_at: DateTime<Utc>,
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
            HistoricalValidationError::RangeBeyondCompleted { timeframe } => Self::RangeBeyondCompleted { timeframe },
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
            HistoricalValidationError::UndefinedOperands {
                condition,
                timeframe,
                evaluated_at,
            } => Self::UndefinedOperands {
                condition,
                timeframe,
                evaluated_at,
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
    Evaluated {
        validation_id: u32,
        completed: usize,
        total: usize,
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

#[derive(Default)]
pub(crate) struct SignalValidationState(SingleTaskState);

impl HasTaskState for SignalValidationState {
    fn task_state(&self) -> &SingleTaskState {
        &self.0
    }
}

/// Why a validation could not start; serialized for the frontend to branch on.
#[derive(Debug, PartialEq, Eq, Error, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum StartValidationError {
    #[error("historical signal validation is already active")]
    AlreadyActive,
    #[error("historical signal validation ID space is exhausted")]
    IdExhausted,
    #[error("a coin symbol is required for historical signal validation")]
    EmptyCoin,
    #[error("failed to start historical signal validation event stream: {message}")]
    EventStream { message: String },
}

impl From<BeginError> for StartValidationError {
    fn from(error: BeginError) -> Self {
        match error {
            BeginError::AlreadyActive => Self::AlreadyActive,
            BeginError::IdExhausted => Self::IdExhausted,
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
    let mut cancellation_observed = false;
    let callback_cancellation = cancellation.clone();
    let outcome = {
        let validation = validate_historical(
            &history,
            &definition,
            &coin,
            range,
            Utc::now(),
            |progress: HistoricalProgress| {
                let event = match progress {
                    HistoricalProgress::Hydrated {
                        completed,
                        total,
                        timeframe,
                    } => SignalValidationEvent::Hydrated {
                        validation_id,
                        completed,
                        total,
                        timeframe,
                    },
                    HistoricalProgress::Evaluated { completed, total } => SignalValidationEvent::Evaluated {
                        validation_id,
                        completed,
                        total,
                    },
                };
                let delivered = send_event(&progress_events, event, "historical signal validation");
                cancellation_observed = *callback_cancellation.borrow();
                if delivered && !cancellation_observed {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            },
        );
        tokio::pin!(validation);
        tokio::select! {
            biased;
            _ = cancellation.changed() => ValidationOutcome::Cancelled,
            result = &mut validation => ValidationOutcome::Finished(result),
        }
    };

    match outcome {
        ValidationOutcome::Cancelled => SignalValidationEvent::Cancelled { validation_id },
        ValidationOutcome::Finished(Err(HistoricalValidationError::Stopped)) if cancellation_observed => {
            SignalValidationEvent::Cancelled { validation_id }
        }
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
    run_supervised(
        state,
        validation_id,
        events.clone(),
        validation_worker(validation_id, history, definition, coin, range, cancellation, events),
        "historical signal validation",
        |message| {
            tracing::error!(validation_id, %message, "historical signal validation worker failed");
            SignalValidationEvent::Failed {
                validation_id,
                failure: SignalValidationFailure::Internal { message },
            }
        },
    )
    .await;
}

#[tauri::command]
pub(crate) fn start_signal_validation(
    state: tauri::State<'_, Arc<SignalValidationState>>,
    history: tauri::State<'_, Arc<CandleHistory>>,
    definition: SignalDefinition,
    coin: String,
    range: HistoricalRange,
    on_event: Channel<SignalValidationEvent>,
) -> Result<u32, StartValidationError> {
    if coin.trim().is_empty() {
        return Err(StartValidationError::EmptyCoin);
    }
    let total = definition
        .conditions
        .values()
        .map(|condition| condition.timeframe)
        .collect::<BTreeSet<_>>()
        .len();
    let (cancel, cancellation) = watch::channel(false);
    let validation_id = state.begin(cancel)?;
    if let Err(error) = on_event.send(SignalValidationEvent::Started { validation_id, total }) {
        state.finish(validation_id);
        return Err(StartValidationError::EventStream {
            message: error.to_string(),
        });
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
