//! Cancellable signal-run lifecycle and typed Tauri channel transport.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::Utc;
use hl_feed::{CandleHistory, MarketStore};
use serde::{Deserialize, Serialize};
use signals::{DefinitionError, EvalReport, RunSummary, SignalDefinition, run_signal};
use tauri::ipc::Channel;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "coins", rename_all = "camelCase")]
pub(crate) enum SignalRunScope {
    Universe,
    Coins(Vec<String>),
}

impl SignalRunScope {
    fn resolve(self, store: &MarketStore) -> Result<Vec<String>, ScopeError> {
        match self {
            Self::Universe => Ok(store.universe().into_iter().map(|coin| coin.coin).collect()),
            Self::Coins(coins) => {
                let mut seen = HashSet::with_capacity(coins.len());
                for coin in &coins {
                    if coin.is_empty() {
                        return Err(ScopeError::EmptyCoin);
                    }
                    if !seen.insert(coin.clone()) {
                        return Err(ScopeError::DuplicateCoin(coin.clone()));
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
    },
    Cancelled {
        run_id: u32,
        completed: usize,
        total: usize,
        matched: usize,
    },
    // Progress is already streamed to the frontend; failure adds only the terminal cause.
    Failed {
        run_id: u32,
        message: String,
    },
}

struct ActiveSignalRun {
    id: u32,
    cancel: watch::Sender<bool>,
}

#[derive(Default)]
struct SignalRunInner {
    next_id: u32,
    active: Option<ActiveSignalRun>,
}

#[derive(Default)]
pub(crate) struct SignalRunState {
    inner: Mutex<SignalRunInner>,
}

#[derive(Debug, PartialEq, Eq, Error)]
enum ScopeError {
    #[error("an explicit signal-run scope contains an empty coin symbol")]
    EmptyCoin,
    #[error("an explicit signal-run scope contains duplicate coin '{0}'")]
    DuplicateCoin(String),
}

#[derive(Debug, PartialEq, Eq, Error)]
enum StartRunError {
    #[error("a signal run is already active")]
    AlreadyActive,
    #[error("signal run ID space is exhausted")]
    IdExhausted,
}

impl SignalRunState {
    fn begin(&self, cancel: watch::Sender<bool>) -> Result<u32, StartRunError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.active.is_some() {
            return Err(StartRunError::AlreadyActive);
        }
        let id = inner.next_id.checked_add(1).ok_or(StartRunError::IdExhausted)?;
        inner.next_id = id;
        inner.active = Some(ActiveSignalRun { id, cancel });
        Ok(id)
    }

    fn cancel(&self, id: u32) -> bool {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .active
            .as_ref()
            .filter(|run| run.id == id)
            .is_some_and(|run| run.cancel.send(true).is_ok())
    }

    fn finish(&self, id: u32) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.active.as_ref().is_some_and(|run| run.id == id) {
            inner.active = None;
        }
    }
}

struct ActiveRunGuard {
    state: Arc<SignalRunState>,
    id: u32,
    active: bool,
}

impl ActiveRunGuard {
    fn finish(&mut self) {
        if self.active {
            self.state.finish(self.id);
            self.active = false;
        }
    }
}

impl Drop for ActiveRunGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

enum RunOutcome {
    Cancelled,
    Finished(Result<RunSummary, DefinitionError>),
}

fn send_event(events: &Channel<SignalRunEvent>, event: SignalRunEvent) -> bool {
    match events.send(event) {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(%error, "failed to send signal run event");
            false
        }
    }
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
    let mut cancellation_observed = false;
    let callback_cancellation = cancellation.clone();
    let outcome = {
        let run = run_signal(&store, &history, &definition, &coins, Utc::now(), |progress| {
            completed = progress.completed;
            if progress.report.satisfied {
                matched += 1;
            }
            let delivered = send_event(
                &events,
                SignalRunEvent::Evaluated {
                    run_id,
                    completed: progress.completed,
                    total: progress.total,
                    report: progress.report,
                },
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
        },
        RunOutcome::Finished(Ok(summary)) if summary.stopped && cancellation_observed => SignalRunEvent::Cancelled {
            run_id,
            completed: summary.completed,
            total: summary.total,
            matched,
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
    let mut guard = ActiveRunGuard {
        state,
        id: run_id,
        active: true,
    };
    let worker = tokio::spawn(run_worker(
        run_id,
        store,
        history,
        definition,
        coins,
        cancellation,
        events.clone(),
    ));
    let terminal = match worker.await {
        Ok(terminal) => terminal,
        Err(error) => {
            tracing::error!(run_id, %error, "signal run worker failed");
            SignalRunEvent::Failed {
                run_id,
                message: error.to_string(),
            }
        }
    };

    // Release the slot before notifying the frontend so its terminal handler can
    // immediately start another run. Drop remains the panic-safe fallback.
    guard.finish();
    send_event(&events, terminal);
}

#[tauri::command]
pub(crate) fn start_signal_run(
    state: tauri::State<'_, Arc<SignalRunState>>,
    store: tauri::State<'_, Arc<MarketStore>>,
    history: tauri::State<'_, Arc<CandleHistory>>,
    definition: SignalDefinition,
    scope: SignalRunScope,
    on_event: Channel<SignalRunEvent>,
) -> Result<u32, String> {
    let coins = scope.resolve(store.inner()).map_err(|error| error.to_string())?;
    let total = coins.len();
    let (cancel, cancellation) = watch::channel(false);
    let run_id = state.begin(cancel).map_err(|error| error.to_string())?;
    if let Err(error) = on_event.send(SignalRunEvent::Started { run_id, total }) {
        state.finish(run_id);
        return Err(format!("failed to start signal run event stream: {error}"));
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
