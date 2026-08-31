//! Cancellable signal-run lifecycle and typed Tauri channel transport.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use chrono::Utc;
use hl_feed::{CandleHistory, MarketStore};
use serde::{Deserialize, Serialize};
use signals::{DefinitionError, EvalReport, EvaluationVerdict, RunSummary, SignalDefinition, run_signal};
use tauri::ipc::Channel;
use thiserror::Error;
use tokio::sync::watch;

use crate::task_lifecycle::{BeginError, HasTaskState, SingleTaskState, run_supervised, send_event};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "coins", rename_all = "camelCase")]
pub(crate) enum SignalRunScope {
    Universe,
    Coins(Vec<String>),
}

impl SignalRunScope {
    fn resolve(self, store: &MarketStore) -> Result<Vec<String>, StartRunError> {
        match self {
            Self::Universe => Ok(store.universe().into_iter().map(|coin| coin.coin).collect()),
            Self::Coins(coins) => {
                let mut seen = HashSet::with_capacity(coins.len());
                for coin in &coins {
                    if coin.is_empty() {
                        return Err(StartRunError::EmptyCoin);
                    }
                    if !seen.insert(coin.clone()) {
                        return Err(StartRunError::DuplicateCoin { coin: coin.clone() });
                    }
                }
                Ok(coins)
            }
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
pub(crate) enum SignalRunEvent {
    Started {
        run_id: u32,
        total: usize,
    },
    Evaluated {
        run_id: u32,
        completed: usize,
        total: usize,
        report: EvalReport,
    },
    Completed {
        run_id: u32,
        completed: usize,
        total: usize,
        matched: usize,
        unknown: usize,
    },
    Cancelled {
        run_id: u32,
        completed: usize,
        total: usize,
        matched: usize,
        unknown: usize,
    },
    // Progress is already streamed to the frontend; failure adds only the terminal cause.
    Failed {
        run_id: u32,
        message: String,
    },
}

#[derive(Default)]
pub(crate) struct SignalRunState(SingleTaskState);

impl HasTaskState for SignalRunState {
    fn task_state(&self) -> &SingleTaskState {
        &self.0
    }
}

/// Why a signal run could not start; serialized for the frontend to branch on.
#[derive(Debug, PartialEq, Eq, Error, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum StartRunError {
    #[error("a signal run is already active")]
    AlreadyActive,
    #[error("signal run ID space is exhausted")]
    IdExhausted,
    #[error("an explicit signal-run scope contains an empty coin symbol")]
    EmptyCoin,
    #[error("an explicit signal-run scope contains duplicate coin '{coin}'")]
    DuplicateCoin { coin: String },
    #[error("failed to start signal run event stream: {message}")]
    EventStream { message: String },
}

impl From<BeginError> for StartRunError {
    fn from(error: BeginError) -> Self {
        match error {
            BeginError::AlreadyActive => Self::AlreadyActive,
            BeginError::IdExhausted => Self::IdExhausted,
        }
    }
}

enum RunOutcome {
    Cancelled,
    Finished(Result<RunSummary, DefinitionError>),
}

async fn run_worker(
    run_id: u32,
    store: Arc<MarketStore>,
    history: Arc<CandleHistory>,
    definition: SignalDefinition,
    coins: Vec<String>,
    mut cancellation: watch::Receiver<bool>,
    events: Channel<SignalRunEvent>,
) -> SignalRunEvent {
    let total = coins.len();
    let mut completed = 0;
    let mut matched = 0;
    let mut unknown = 0;
    let mut cancellation_observed = false;
    let callback_cancellation = cancellation.clone();
    let outcome = {
        let run = run_signal(&store, &history, &definition, &coins, Utc::now(), |progress| {
            completed = progress.completed;
            match progress.report.verdict {
                EvaluationVerdict::Matched => matched += 1,
                EvaluationVerdict::Unknown => unknown += 1,
                EvaluationVerdict::NotMatched => {}
            }
            let delivered = send_event(
                &events,
                SignalRunEvent::Evaluated {
                    run_id,
                    completed: progress.completed,
                    total: progress.total,
                    report: progress.report,
                },
                "signal run",
            );
            cancellation_observed = *callback_cancellation.borrow();
            if delivered && !cancellation_observed {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });
        tokio::pin!(run);
        tokio::select! {
            // Once observed, cancellation owns the terminal label even if completion
            // becomes ready in the same poll; the reported counters remain exact.
            biased;
            _ = cancellation.changed() => RunOutcome::Cancelled,
            result = &mut run => RunOutcome::Finished(result),
        }
    };

    match outcome {
        RunOutcome::Cancelled => SignalRunEvent::Cancelled {
            run_id,
            completed,
            total,
            matched,
            unknown,
        },
        RunOutcome::Finished(Ok(summary)) if summary.stopped && cancellation_observed => SignalRunEvent::Cancelled {
            run_id,
            completed: summary.completed,
            total: summary.total,
            matched,
            unknown,
        },
        RunOutcome::Finished(Ok(summary)) if summary.stopped => SignalRunEvent::Failed {
            run_id,
            message: "failed to deliver signal run progress".to_owned(),
        },
        RunOutcome::Finished(Ok(summary)) => SignalRunEvent::Completed {
            run_id,
            completed: summary.completed,
            total: summary.total,
            matched,
            unknown,
        },
        RunOutcome::Finished(Err(error)) => SignalRunEvent::Failed {
            run_id,
            message: error.to_string(),
        },
    }
}

async fn execute_run(
    run_id: u32,
    state: Arc<SignalRunState>,
    store: Arc<MarketStore>,
    history: Arc<CandleHistory>,
    definition: SignalDefinition,
    coins: Vec<String>,
    cancellation: watch::Receiver<bool>,
    events: Channel<SignalRunEvent>,
) {
    run_supervised(
        state,
        run_id,
        events.clone(),
        run_worker(run_id, store, history, definition, coins, cancellation, events),
        "signal run",
        |message| {
            tracing::error!(run_id, %message, "signal run worker failed");
            SignalRunEvent::Failed { run_id, message }
        },
    )
    .await;
}

#[tauri::command]
pub(crate) fn start_signal_run(
    state: tauri::State<'_, Arc<SignalRunState>>,
    store: tauri::State<'_, Arc<MarketStore>>,
    history: tauri::State<'_, Arc<CandleHistory>>,
    definition: SignalDefinition,
    scope: SignalRunScope,
    on_event: Channel<SignalRunEvent>,
) -> Result<u32, StartRunError> {
    let coins = scope.resolve(store.inner())?;
    let total = coins.len();
    let (cancel, cancellation) = watch::channel(false);
    let run_id = state.begin(cancel)?;
    if let Err(error) = on_event.send(SignalRunEvent::Started { run_id, total }) {
        state.finish(run_id);
        return Err(StartRunError::EventStream {
            message: error.to_string(),
        });
    }

    tauri::async_runtime::spawn(execute_run(
        run_id,
        Arc::clone(state.inner()),
        Arc::clone(store.inner()),
        Arc::clone(history.inner()),
        definition,
        coins,
        cancellation,
        on_event,
    ));
    Ok(run_id)
}

#[tauri::command]
pub(crate) fn cancel_signal_run(state: tauri::State<'_, Arc<SignalRunState>>, run_id: u32) -> bool {
    state.cancel(run_id)
}

#[cfg(test)]
mod tests;
