import { Channel, invoke } from "@tauri-apps/api/core"

import { createSignalChannelClient } from "$lib/signals/channel-client"
import type { SignalChannelTransport } from "$lib/signals/channel-client"
import type { SignalDefinition, SignalTimeframe } from "$lib/signals/definition"
import type { EvalReport } from "$lib/signals/run"

export interface HistoricalRange {
  start: string
  end: string
}

export interface HistoricalMatchSpan {
  startedAt: string
  endedAt: string
  activeAtStart: boolean
  activeAtEnd: boolean
  evidence: EvalReport
}

export interface HistoricalValidation {
  coin: string
  range: HistoricalRange
  anchorTimeframe: SignalTimeframe
  evaluatedPoints: number
  spans: HistoricalMatchSpan[]
}

export type SignalValidationFailure =
  | { type: "invalidDefinition"; message: string }
  | { type: "invalidRange" }
  | { type: "historyUnavailable"; timeframe: SignalTimeframe; message: string }
  | { type: "insufficientHistory"; timeframe: SignalTimeframe; required: number; available: number }
  | { type: "internal"; message: string }

export type SignalValidationEvent =
  | { event: "started"; data: { validationId: number; total: number } }
  | {
      event: "hydrated"
      data: { validationId: number; completed: number; total: number; timeframe: SignalTimeframe }
    }
  | { event: "completed"; data: { validationId: number; result: HistoricalValidation } }
  | { event: "cancelled"; data: { validationId: number } }
  | { event: "failed"; data: { validationId: number; failure: SignalValidationFailure } }

export type SignalValidationProgress =
  | { status: "idle" }
  | { status: "running"; validationId: number; completed: number; total: number }
  | { status: "completed"; validationId: number; completed: number; total: number }
  | { status: "cancelled"; validationId: number; completed: number; total: number }
  | {
      status: "failed"
      validationId: number
      completed: number
      total: number
      failure: SignalValidationFailure
    }

export function idleSignalValidationProgress(): SignalValidationProgress {
  return { status: "idle" }
}

export function reduceSignalValidationProgress(
  state: SignalValidationProgress,
  event: SignalValidationEvent
): SignalValidationProgress {
  if (event.event === "started") {
    if (state.status === "running") return state
    return {
      status: "running",
      validationId: event.data.validationId,
      completed: 0,
      total: event.data.total,
    }
  }
  if (state.status !== "running" || state.validationId !== event.data.validationId) return state

  switch (event.event) {
    case "hydrated":
      return {
        status: "running",
        validationId: state.validationId,
        completed: event.data.completed,
        total: event.data.total,
      }
    case "completed":
      return {
        status: "completed",
        validationId: state.validationId,
        completed: state.completed,
        total: state.total,
      }
    case "cancelled":
      return {
        status: "cancelled",
        validationId: state.validationId,
        completed: state.completed,
        total: state.total,
      }
    case "failed":
      return {
        status: "failed",
        validationId: state.validationId,
        completed: state.completed,
        total: state.total,
        failure: event.data.failure,
      }
  }
}

export function signalValidationFailureMessage(failure: SignalValidationFailure): string {
  switch (failure.type) {
    case "invalidDefinition":
      return failure.message
    case "invalidRange":
      return "Choose a start date before the end date."
    case "historyUnavailable":
      return `${failure.timeframe} candle history is unavailable: ${failure.message}`
    case "insufficientHistory":
      return `Only ${failure.available} of ${failure.required} required contiguous ${failure.timeframe} candles are available. Choose a shorter range or try again when more history is available.`
    case "internal":
      return failure.message
  }
}

export interface SignalValidationTransport {
  start(
    definition: SignalDefinition,
    coin: string,
    range: HistoricalRange,
    onEvent: (event: SignalValidationEvent) => void
  ): Promise<number>
  cancel(validationId: number): Promise<boolean>
}

export const tauriSignalValidationTransport: SignalValidationTransport = {
  start(definition, coin, range, onEvent) {
    const onEventChannel = new Channel<SignalValidationEvent>(onEvent)
    return invoke<number>("start_signal_validation", { definition, coin, range, onEvent: onEventChannel })
  },
  cancel(validationId) {
    return invoke<boolean>("cancel_signal_validation", { validationId })
  },
}

export class SignalValidationAlreadyActiveError extends Error {
  constructor() {
    super("historical signal validation is already active")
    this.name = "SignalValidationAlreadyActiveError"
  }
}

export class SignalValidationProtocolError extends Error {
  constructor(observedId: number, returnedId: number) {
    super(`historical signal validation started as ${observedId} but command returned ${returnedId}`)
    this.name = "SignalValidationProtocolError"
  }
}

export interface SignalValidationClient {
  start(
    definition: SignalDefinition,
    coin: string,
    range: HistoricalRange,
    onEvent: (event: SignalValidationEvent) => void
  ): Promise<number>
  cancel(): Promise<boolean>
  activeValidationId(): number | null
  isActive(): boolean
}

interface SignalValidationStart {
  definition: SignalDefinition
  coin: string
  range: HistoricalRange
}

export function createSignalValidationClient(
  transport: SignalValidationTransport = tauriSignalValidationTransport
): SignalValidationClient {
  const channelTransport: SignalChannelTransport<SignalValidationStart, SignalValidationEvent> = {
    start(input, onEvent) {
      return transport.start(input.definition, input.coin, input.range, onEvent)
    },
    cancel(validationId) {
      return transport.cancel(validationId)
    },
  }
  const client = createSignalChannelClient(channelTransport, {
    eventId: event => event.data.validationId,
    isTerminal: event => event.event === "completed" || event.event === "cancelled" || event.event === "failed",
    alreadyActiveError: () => new SignalValidationAlreadyActiveError(),
    protocolError: (observedId, returnedId) => new SignalValidationProtocolError(observedId, returnedId),
  })

  return {
    start(definition, coin, range, onEvent) {
      return client.start({ definition, coin, range }, onEvent)
    },
    cancel() {
      return client.cancel()
    },
    activeValidationId() {
      return client.activeId()
    },
    isActive() {
      return client.isActive()
    },
  }
}
