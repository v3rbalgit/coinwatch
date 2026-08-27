//! Single-active cancellable task lifecycle shared by channel-streaming subsystems.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use serde::Serialize;
use tauri::ipc::Channel;
use tokio::sync::watch;

struct ActiveTask {
    id: u32,
    cancel: watch::Sender<bool>,
}

#[derive(Default)]
struct TaskInner {
    next_id: u32,
    active: Option<ActiveTask>,
}

/// Why a new task could not begin.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BeginError {
    AlreadyActive,
    IdExhausted,
}

/// At most one active task; ids only increase and are never reused in-process.
#[derive(Default)]
pub(crate) struct SingleTaskState {
    inner: Mutex<TaskInner>,
}

impl SingleTaskState {
    pub(crate) fn begin(&self, cancel: watch::Sender<bool>) -> Result<u32, BeginError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.active.is_some() {
            return Err(BeginError::AlreadyActive);
        }
        let id = inner.next_id.checked_add(1).ok_or(BeginError::IdExhausted)?;
        inner.next_id = id;
        inner.active = Some(ActiveTask { id, cancel });
        Ok(id)
    }

    pub(crate) fn cancel(&self, id: u32) -> bool {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .active
            .as_ref()
            .filter(|task| task.id == id)
            .is_some_and(|task| task.cancel.send(true).is_ok())
    }

    pub(crate) fn finish(&self, id: u32) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.active.as_ref().is_some_and(|task| task.id == id) {
            inner.active = None;
        }
    }
}

/// Managed state owning a [`SingleTaskState`] slot.
pub(crate) trait HasTaskState: Send + Sync + 'static {
    fn task_state(&self) -> &SingleTaskState;

    fn begin(&self, cancel: watch::Sender<bool>) -> Result<u32, BeginError> {
        self.task_state().begin(cancel)
    }

    fn cancel(&self, id: u32) -> bool {
        self.task_state().cancel(id)
    }

    fn finish(&self, id: u32) {
        self.task_state().finish(id);
    }
}

/// Panic-safe release of the active task slot.
pub(crate) struct TaskGuard<S: HasTaskState> {
    pub(crate) state: Arc<S>,
    pub(crate) id: u32,
    pub(crate) active: bool,
}

impl<S: HasTaskState> TaskGuard<S> {
    pub(crate) fn finish(&mut self) {
        if self.active {
            self.state.task_state().finish(self.id);
            self.active = false;
        }
    }
}

impl<S: HasTaskState> Drop for TaskGuard<S> {
    fn drop(&mut self) {
        self.finish();
    }
}

pub(crate) fn send_event<E: Serialize>(events: &Channel<E>, event: E, task: &'static str) -> bool {
    match events.send(event) {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(task, %error, "failed to send task event");
            false
        }
    }
}

/// Run the spawned worker to its terminal event, releasing the slot before the
/// terminal send so the frontend's handler can immediately start another task.
pub(crate) async fn run_supervised<S, E, W>(
    state: Arc<S>,
    id: u32,
    events: Channel<E>,
    worker: W,
    task: &'static str,
    on_join_error: impl FnOnce(String) -> E,
) where
    S: HasTaskState,
    E: Serialize,
    W: Future<Output = E> + Send + 'static,
    E: Send + 'static,
{
    let mut guard = TaskGuard {
        state,
        id,
        active: true,
    };
    let terminal = match tokio::spawn(worker).await {
        Ok(terminal) => terminal,
        Err(error) => on_join_error(error.to_string()),
    };
    guard.finish();
    send_event(&events, terminal, task);
}
