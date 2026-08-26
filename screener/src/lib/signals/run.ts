import { Channel, invoke } from "@tauri-apps/api/core"

import { createSignalChannelClient } from "$lib/signals/channel-client"
import type { SignalChannelTransport } from "$lib/signals/channel-client"
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

interface SignalRunStart {
  definition: SignalDefinition
  scope: SignalRunScope
}

export function createSignalRunClient(transport: SignalRunTransport = tauriSignalRunTransport): SignalRunClient {
  const channelTransport: SignalChannelTransport<SignalRunStart, SignalRunEvent> = {
    start(input, onEvent) {
      return transport.start(input.definition, input.scope, onEvent)
    },
    cancel(runId) {
      return transport.cancel(runId)
    },
  }
  const client = createSignalChannelClient(channelTransport, {
    eventId: event => event.data.runId,
    isTerminal: event => event.event === "completed" || event.event === "cancelled" || event.event === "failed",
    alreadyActiveError: () => new SignalRunAlreadyActiveError(),
    protocolError: (observedId, returnedId) => new SignalRunProtocolError(observedId, returnedId),
  })

  return {
    start(definition, scope, onEvent) {
      return client.start({ definition, scope }, onEvent)
    },
    cancel() {
      return client.cancel()
    },
    activeRunId() {
      return client.activeId()
    },
    isActive() {
      return client.isActive()
    },
  }
}
