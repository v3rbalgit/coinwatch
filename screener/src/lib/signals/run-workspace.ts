import { errorMessage } from "$lib/errors"
import type { SignalDefinition } from "$lib/signals/definition"
import { idleSignalRunProgress, reduceSignalRunProgress, startSignalRunErrorMessage } from "$lib/signals/run"
import type { EvalReport, SignalRunClient, SignalRunEvent, SignalRunProgress, SignalRunScope } from "$lib/signals/run"
import { rankClosestSignalRunReports } from "$lib/signals/run-results"
import type { SignalRunResultFilter } from "$lib/signals/run-results"

export interface SignalRunSnapshot {
  name: string
  definition: SignalDefinition
  scope: SignalRunScope
  scopeLabel: string
  source: "draft" | "saved"
  startedAt: string
}

export interface SignalRunWorkspaceState {
  view: "definition" | "results"
  snapshot: SignalRunSnapshot | null
  progress: SignalRunProgress
  reports: EvalReport[]
  filter: SignalRunResultFilter
  starting: boolean
  cancellationPending: boolean
  error: string
}

export interface SignalRunWorkspaceController {
  state: SignalRunWorkspaceState
  isActive(): boolean
  showDefinition(): void
  showResults(): void
  setFilter(filter: SignalRunResultFilter): void
  start(snapshot: SignalRunSnapshot): Promise<void>
  cancel(): Promise<void>
}

export function snapshotSignalDefinition(definition: SignalDefinition): SignalDefinition {
  return structuredClone(definition)
}

export function createInitialSignalRunWorkspaceState(): SignalRunWorkspaceState {
  return {
    view: "definition",
    snapshot: null,
    progress: idleSignalRunProgress(),
    reports: [],
    filter: "matches",
    starting: false,
    cancellationPending: false,
    error: "",
  }
}

function workspaceIsActive(state: SignalRunWorkspaceState): boolean {
  return state.starting || state.progress.status === "running"
}

function applyWorkspaceEvent(state: SignalRunWorkspaceState, event: SignalRunEvent): void {
  if (event.event === "evaluated") state.reports.push(event.data.report)
  state.progress = reduceSignalRunProgress(state.progress, event)
  if (event.event === "started") state.starting = false
  if (event.event === "completed" && event.data.matched === 0 && state.filter === "matches") {
    const definition = state.snapshot?.definition
    state.filter =
      definition !== undefined && rankClosestSignalRunReports(definition, state.reports).length > 0 ? "closest" : "all"
  }
  if (event.event === "completed" || event.event === "cancelled" || event.event === "failed") {
    state.starting = false
    state.cancellationPending = false
  }
}

function recordStartFailure(state: SignalRunWorkspaceState, message: string): void {
  state.error = message
  if (state.progress.status !== "idle") {
    state.progress = {
      status: "failed",
      runId: state.progress.runId,
      completed: state.progress.completed,
      total: state.progress.total,
      matched: state.progress.matched,
      message,
    }
  }
  state.cancellationPending = false
}

export function createSignalRunWorkspace(
  runClient: SignalRunClient,
  state = createInitialSignalRunWorkspaceState()
): SignalRunWorkspaceController {
  function isActive(): boolean {
    return workspaceIsActive(state)
  }

  async function start(snapshot: SignalRunSnapshot): Promise<void> {
    if (isActive()) {
      state.view = "results"
      return
    }
    state.view = "results"
    state.snapshot = snapshot
    state.progress = idleSignalRunProgress()
    state.reports = []
    state.filter = "matches"
    state.starting = true
    state.cancellationPending = false
    state.error = ""

    try {
      await runClient.start(snapshot.definition, snapshot.scope, event => applyWorkspaceEvent(state, event))
    } catch (error) {
      recordStartFailure(state, startSignalRunErrorMessage(error))
    } finally {
      state.starting = false
    }
  }

  async function cancel(): Promise<void> {
    if (!isActive() || state.cancellationPending) return
    state.cancellationPending = true
    state.error = ""
    try {
      const accepted = await runClient.cancel()
      if (!accepted) state.cancellationPending = false
    } catch (error) {
      state.cancellationPending = false
      state.error = errorMessage(error)
    }
  }

  return {
    state,
    isActive,
    showDefinition() {
      state.view = "definition"
    },
    showResults() {
      if (state.snapshot !== null) state.view = "results"
    },
    setFilter(filter) {
      state.filter = filter
    },
    start,
    cancel,
  }
}
