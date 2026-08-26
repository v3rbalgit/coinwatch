import { describe, expect, it } from "vitest"

import type { SignalDefinition } from "$lib/signals/definition"
import {
  createSignalValidationClient,
  idleSignalValidationProgress,
  reduceSignalValidationProgress,
  signalValidationFailureMessage,
  type HistoricalRange,
  type SignalValidationEvent,
  type SignalValidationTransport,
} from "$lib/signals/historical"

const definition: SignalDefinition = {
  version: 2,
  direction: "bullish",
  root: { type: "condition", id: "price" },
  conditions: {
    price: {
      timeframe: "1h",
      predicate: {
        type: "threshold",
        left: { type: "field", field: "close" },
        op: "greaterThan",
        right: { type: "constant", value: 100 },
      },
      withinBars: 0,
    },
  },
}

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
