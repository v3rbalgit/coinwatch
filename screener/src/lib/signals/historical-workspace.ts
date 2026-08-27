import { errorMessage } from "$lib/errors"
import type { SignalDefinition } from "$lib/signals/definition"
import {
  idleSignalValidationProgress,
  reduceSignalValidationProgress,
  startSignalValidationErrorMessage,
  type HistoricalRange,
  type HistoricalValidation,
  type SignalValidationClient,
  type SignalValidationEvent,
  type SignalValidationProgress,
} from "$lib/signals/historical"
import { snapshotSignalDefinition } from "$lib/signals/run-workspace"

export interface HistoricalValidationSnapshot {
  name: string
  definition: SignalDefinition
  coin: string
  range: HistoricalRange
  rangeLabel: string
  source: "draft" | "saved"
}

export interface HistoricalValidationWorkspaceState {
  snapshot: HistoricalValidationSnapshot | null
  progress: SignalValidationProgress
  result: HistoricalValidation | null
  selectedSpan: number | null
  visible: boolean
  panelOpen: boolean
  starting: boolean
  cancellationPending: boolean
  error: string
}

export interface HistoricalValidationWorkspaceController {
  state: HistoricalValidationWorkspaceState
  isActive(): boolean
  start(snapshot: HistoricalValidationSnapshot): Promise<void>
  cancel(): Promise<void>
  selectSpan(index: number | null): void
  setVisible(visible: boolean): void
  setPanelOpen(open: boolean): void
  clear(): void
}

export function createInitialHistoricalValidationWorkspaceState(): HistoricalValidationWorkspaceState {
  return {
    snapshot: null,
    progress: idleSignalValidationProgress(),
    result: null,
    selectedSpan: null,
    visible: true,
    panelOpen: false,
    starting: false,
    cancellationPending: false,
    error: "",
  }
}

function workspaceIsActive(state: HistoricalValidationWorkspaceState): boolean {
  return state.starting || state.progress.status === "running"
}

function applyEvent(state: HistoricalValidationWorkspaceState, event: SignalValidationEvent): void {
  state.error = ""
  state.progress = reduceSignalValidationProgress(state.progress, event)
  if (event.event === "started") state.starting = false
  if (event.event === "completed") {
    state.result = event.data.result
    state.selectedSpan = event.data.result.spans.length > 0 ? 0 : null
    state.visible = true
    state.panelOpen = true
  }
  if (event.event === "completed" || event.event === "cancelled" || event.event === "failed") {
    state.starting = false
    state.cancellationPending = false
  }
}

function recordStartFailure(state: HistoricalValidationWorkspaceState, message: string): void {
  state.starting = false
  state.cancellationPending = false
  state.error = message
  state.progress = idleSignalValidationProgress()
}

export function createHistoricalValidationWorkspace(
  client: SignalValidationClient,
  state = createInitialHistoricalValidationWorkspaceState()
): HistoricalValidationWorkspaceController {
  async function start(snapshot: HistoricalValidationSnapshot): Promise<void> {
    if (workspaceIsActive(state)) {
      state.error = "Another historical signal validation is already running."
      return
    }
    state.starting = true
    state.cancellationPending = false
    state.error = ""

    try {
      const definition = snapshotSignalDefinition(snapshot.definition)
      const range = { ...snapshot.range }
      const coin = snapshot.coin
      state.snapshot = {
        ...snapshot,
        definition,
        range,
      }
      state.progress = idleSignalValidationProgress()
      state.result = null
      state.selectedSpan = null
      state.visible = true
      state.panelOpen = false
      // Invoke with standalone plain values, decoupled from later `$state` edits.
      await client.start(definition, coin, range, event => applyEvent(state, event))
    } catch (error) {
      recordStartFailure(state, startSignalValidationErrorMessage(error))
    } finally {
      state.starting = false
    }
  }

  async function cancel(): Promise<void> {
    if (!workspaceIsActive(state) || state.cancellationPending) return
    state.cancellationPending = true
    state.error = ""
    try {
      const accepted = await client.cancel()
      if (!accepted) state.cancellationPending = false
    } catch (error) {
      state.cancellationPending = false
      state.error = errorMessage(error)
    }
  }

  return {
    state,
    isActive() {
      return workspaceIsActive(state)
    },
    start,
    cancel,
    selectSpan(index) {
      state.selectedSpan = index
      if (index !== null) state.panelOpen = true
    },
    setVisible(visible) {
      state.visible = visible
    },
    setPanelOpen(open) {
      state.panelOpen = open
    },
    clear() {
      if (workspaceIsActive(state)) return
      state.snapshot = null
      state.progress = idleSignalValidationProgress()
      state.result = null
      state.selectedSpan = null
      state.visible = true
      state.panelOpen = false
      state.error = ""
    },
  }
}
