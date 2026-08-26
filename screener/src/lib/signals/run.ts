import { Channel, invoke } from "@tauri-apps/api/core"

import type { SignalDefinition, SignalTimeframe } from "$lib/signals/definition"

export type SignalRunScope = { type: "universe" } | { type: "coins"; coins: string[] }

export type ConditionEvidence =
  | { type: "state"; observedAt: string; left: number; right: number }
  | {
      type: "cross"
      observedAt: string
      previousLeft: number
      previousRight: number
      left: number
      right: number
    }

export type ConditionOutcome = {
  lastTrueBar: string | null
  evidence: ConditionEvidence | null
} & (
  | { status: "satisfied" }
  | { status: "notSatisfied" }
  | { status: "noData" }
  | { status: "unavailable" }
  | { status: "insufficientHistory"; required: number; available: number }
)

export interface EvalReport {
  coin: string
  satisfied: boolean
  triggeredAt: string | null
  conditions: Record<string, ConditionOutcome>
  missingData: SignalTimeframe[]
  unavailableData: Partial<Record<SignalTimeframe, string>>
}

export type SignalRunEvent =
  | { event: "started"; data: { runId: number; total: number } }
  | {
      event: "evaluated"
      data: { runId: number; completed: number; total: number; report: EvalReport }
    }
  | {
      event: "completed"
      data: { runId: number; completed: number; total: number; matched: number }
    }
  | {
      event: "cancelled"
      data: { runId: number; completed: number; total: number; matched: number }
    }
  | { event: "failed"; data: { runId: number; message: string } }

export type SignalRunProgress =
  | { status: "idle" }
  | { status: "running"; runId: number; completed: number; total: number; matched: number }
  | { status: "completed"; runId: number; completed: number; total: number; matched: number }
  | { status: "cancelled"; runId: number; completed: number; total: number; matched: number }
  | {
      status: "failed"
      runId: number
      completed: number
      total: number
      matched: number
      message: string
    }

export function idleSignalRunProgress(): SignalRunProgress {
  return { status: "idle" }
}

export function reduceSignalRunProgress(state: SignalRunProgress, event: SignalRunEvent): SignalRunProgress {
  if (event.event === "started") {
    if (state.status === "running") return state
    return {
      status: "running",
      runId: event.data.runId,
      completed: 0,
      total: event.data.total,
      matched: 0,
    }
  }
  if (state.status !== "running" || state.runId !== event.data.runId) return state

  switch (event.event) {
    case "evaluated":
      return {
        status: "running",
        runId: state.runId,
        completed: event.data.completed,
        total: event.data.total,
        matched: state.matched + (event.data.report.satisfied ? 1 : 0),
      }
    case "completed":
      return { status: "completed", ...event.data }
    case "cancelled":
      return { status: "cancelled", ...event.data }
    case "failed":
      return {
        status: "failed",
        runId: state.runId,
        completed: state.completed,
        total: state.total,
        matched: state.matched,
        message: event.data.message,
      }
  }
}

export interface SignalRunTransport {
  start(definition: SignalDefinition, scope: SignalRunScope, onEvent: (event: SignalRunEvent) => void): Promise<number>
  cancel(runId: number): Promise<boolean>
}

export const tauriSignalRunTransport: SignalRunTransport = {
  start(definition, scope, onEvent) {
    const onEventChannel = new Channel<SignalRunEvent>(onEvent)
    return invoke<number>("start_signal_run", { definition, scope, onEvent: onEventChannel })
  },
  cancel(runId) {
    return invoke<boolean>("cancel_signal_run", { runId })
  },
}

export class SignalRunAlreadyActiveError extends Error {
  constructor() {
    super("a signal run is already active")
    this.name = "SignalRunAlreadyActiveError"
  }
}

export class SignalRunProtocolError extends Error {
  constructor(startedRunId: number, returnedRunId: number) {
    super(`signal run started as ${startedRunId} but command returned ${returnedRunId}`)
    this.name = "SignalRunProtocolError"
  }
}

export interface SignalRunClient {
  start(definition: SignalDefinition, scope: SignalRunScope, onEvent: (event: SignalRunEvent) => void): Promise<number>
  cancel(): Promise<boolean>
  activeRunId(): number | null
  isActive(): boolean
}

interface PendingCancellation {
  promise: Promise<boolean>
  resolve(value: boolean): void
  reject(reason: unknown): void
}

interface ActiveSession {
  runId: number | null
  terminal: boolean
  pendingCancellation: PendingCancellation | null
}

type ReleaseSession = (session: ActiveSession) => void

function createPendingCancellation(): PendingCancellation {
  let resolvePromise: (value: boolean) => void = () => undefined
  let rejectPromise: (reason: unknown) => void = () => undefined
  const promise = new Promise<boolean>((resolve, reject) => {
    resolvePromise = resolve
    rejectPromise = reject
  })
  return { promise, resolve: resolvePromise, reject: rejectPromise }
}

function isTerminalEvent(event: SignalRunEvent): boolean {
  return event.event === "completed" || event.event === "cancelled" || event.event === "failed"
}

function flushCancellation(session: ActiveSession, transport: SignalRunTransport): void {
  if (session.runId === null || session.pendingCancellation === null) return
  const pending = session.pendingCancellation
  session.pendingCancellation = null
  void transport.cancel(session.runId).then(
    cancelled => pending.resolve(cancelled),
    error => pending.reject(error)
  )
}

function handleRunEvent(
  session: ActiveSession,
  event: SignalRunEvent,
  onEvent: (event: SignalRunEvent) => void,
  release: ReleaseSession,
  transport: SignalRunTransport
): void {
  if (session.terminal) return
  session.runId ??= event.data.runId
  flushCancellation(session, transport)
  if (event.data.runId !== session.runId) return

  if (isTerminalEvent(event)) {
    session.terminal = true
    release(session)
  }
  onEvent(event)
}

function reconcileRunId(
  session: ActiveSession,
  returnedRunId: number,
  release: ReleaseSession,
  transport: SignalRunTransport
): number {
  if (session.runId !== null && session.runId !== returnedRunId) {
    session.terminal = true
    release(session)
    throw new SignalRunProtocolError(session.runId, returnedRunId)
  }
  session.runId = returnedRunId
  flushCancellation(session, transport)
  return returnedRunId
}

function rejectStart(session: ActiveSession, error: unknown, release: ReleaseSession): never {
  session.terminal = true
  session.pendingCancellation?.resolve(false)
  session.pendingCancellation = null
  release(session)
  throw error
}

export function createSignalRunClient(transport: SignalRunTransport = tauriSignalRunTransport): SignalRunClient {
  let active: ActiveSession | null = null
  const release: ReleaseSession = session => {
    if (active === session) active = null
  }

  return {
    start(definition, scope, onEvent) {
      if (active !== null) return Promise.reject(new SignalRunAlreadyActiveError())

      const session: ActiveSession = {
        runId: null,
        terminal: false,
        pendingCancellation: null,
      }
      active = session
      return transport
        .start(definition, scope, event => handleRunEvent(session, event, onEvent, release, transport))
        .then(
          runId => reconcileRunId(session, runId, release, transport),
          error => rejectStart(session, error, release)
        )
    },
    cancel() {
      if (active === null) return Promise.resolve(false)
      if (active.runId !== null) return transport.cancel(active.runId)
      active.pendingCancellation ??= createPendingCancellation()
      return active.pendingCancellation.promise
    },
    activeRunId() {
      return active?.runId ?? null
    },
    isActive() {
      return active !== null
    },
  }
}
