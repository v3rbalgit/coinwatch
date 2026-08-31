import { describe, expect, it } from "vitest"

import type { SignalDefinition } from "$lib/signals/definition"
import {
  createSignalValidationClient,
  idleSignalValidationProgress,
  isStartSignalValidationError,
  reduceSignalValidationProgress,
  signalValidationFailureMessage,
  startSignalValidationErrorMessage,
  type HistoricalRange,
  type SignalValidationEvent,
  type SignalValidationTransport,
} from "$lib/signals/historical"

import { thresholdDefinition } from "./signals-fixture"

const definition = thresholdDefinition()

const range: HistoricalRange = {
  start: "2026-08-25T00:00:00Z",
  end: "2026-08-26T00:00:00Z",
}

interface FakeTransport {
  transport: SignalValidationTransport
  emit(event: SignalValidationEvent): void
  setStartResult(result: Promise<number>): void
  starts(): { definition: SignalDefinition; coin: string; range: HistoricalRange }[]
  cancellations(): number[]
}

function fakeTransport(): FakeTransport {
  let handler: ((event: SignalValidationEvent) => void) | undefined
  let startResult = Promise.resolve(7)
  const started: { definition: SignalDefinition; coin: string; range: HistoricalRange }[] = []
  const cancelled: number[] = []
  return {
    transport: {
      start(nextDefinition, coin, nextRange, onEvent) {
        handler = onEvent
        started.push({ definition: nextDefinition, coin, range: nextRange })
        return startResult
      },
      cancel(validationId) {
        cancelled.push(validationId)
        return Promise.resolve(true)
      },
    },
    emit(event) {
      if (handler === undefined) throw new Error("historical validation has not started")
      handler(event)
    },
    setStartResult(result) {
      startResult = result
    },
    starts() {
      return started
    },
    cancellations() {
      return cancelled
    },
  }
}

describe("validation start errors and progress phases", () => {
  it("narrows every structured start error without accepting malformed values", () => {
    expect(isStartSignalValidationError({ type: "alreadyActive" })).toBe(true)
    expect(isStartSignalValidationError({ type: "idExhausted" })).toBe(true)
    expect(isStartSignalValidationError({ type: "emptyCoin" })).toBe(true)
    expect(isStartSignalValidationError({ type: "eventStream", message: "closed" })).toBe(true)
    expect(isStartSignalValidationError({ type: "eventStream" })).toBe(false)
    expect(isStartSignalValidationError("already active")).toBe(false)
    expect(startSignalValidationErrorMessage(new Error("boom"))).toBe("boom")
  })

  it("tracks the hydrating and evaluating phases through progress events", () => {
    let progress = reduceSignalValidationProgress(idleSignalValidationProgress(), {
      event: "started",
      data: { validationId: 3, total: 2 },
    })
    expect(progress).toMatchObject({ status: "running", phase: "hydrating", completed: 0, total: 2 })
    progress = reduceSignalValidationProgress(progress, {
      event: "hydrated",
      data: { validationId: 3, completed: 2, total: 2, timeframe: "1h" },
    })
    expect(progress).toMatchObject({ status: "running", phase: "hydrating", completed: 2 })
    progress = reduceSignalValidationProgress(progress, {
      event: "evaluated",
      data: { validationId: 3, completed: 256, total: 500 },
    })
    expect(progress).toMatchObject({ status: "running", phase: "evaluating", completed: 256, total: 500 })
  })
})

describe("historical signal validation client", () => {
  it("reduces hydration progress and preserves a typed failure", () => {
    const started = reduceSignalValidationProgress(idleSignalValidationProgress(), {
      event: "started",
      data: { validationId: 7, total: 2 },
    })
    const hydrated = reduceSignalValidationProgress(started, {
      event: "hydrated",
      data: { validationId: 7, completed: 1, total: 2, timeframe: "1h" },
    })
    const failed = reduceSignalValidationProgress(hydrated, {
      event: "failed",
      data: {
        validationId: 7,
        failure: { type: "insufficientHistory", timeframe: "1w", required: 30, available: 12 },
      },
    })

    expect(hydrated).toMatchObject({ status: "running", completed: 1, total: 2 })
    expect(failed).toMatchObject({ status: "failed", completed: 1 })
    expect(
      signalValidationFailureMessage({ type: "insufficientHistory", timeframe: "1w", required: 30, available: 12 })
    ).toContain("12 of 30")
    expect(
      signalValidationFailureMessage({
        type: "undefinedOperands",
        condition: "price",
        timeframe: "1h",
        evaluatedAt: "2026-08-25T04:00:00Z",
      })
    ).toContain("'price'")
  })

  it("queues cancellation when the channel starts before invoke resolves", async () => {
    const fake = fakeTransport()
    let resolveStart: ((validationId: number) => void) | undefined
    fake.setStartResult(new Promise(resolve => (resolveStart = resolve)))
    const client = createSignalValidationClient(fake.transport)
    const events: SignalValidationEvent[] = []

    const starting = client.start(definition, "BTC", range, event => events.push(event))
    const cancelling = client.cancel()
    expect(fake.starts()).toEqual([{ definition, coin: "BTC", range }])
    expect(fake.cancellations()).toEqual([])

    fake.emit({ event: "started", data: { validationId: 7, total: 1 } })
    expect(fake.cancellations()).toEqual([7])
    await expect(cancelling).resolves.toBe(true)
    if (resolveStart === undefined) throw new Error("missing start resolver")
    resolveStart(7)
    await expect(starting).resolves.toBe(7)
    expect(events).toEqual([{ event: "started", data: { validationId: 7, total: 1 } }])
  })
})
