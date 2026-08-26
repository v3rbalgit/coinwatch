import { describe, expect, it } from "vitest"

import type { SignalDefinition } from "$lib/signals/definition"
import {
  createSignalRunClient,
  idleSignalRunProgress,
  reduceSignalRunProgress,
  SignalRunAlreadyActiveError,
  SignalRunProtocolError,
} from "$lib/signals/run"
import type { EvalReport, SignalRunEvent, SignalRunScope, SignalRunTransport } from "$lib/signals/run"

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

function report(satisfied: boolean): EvalReport {
  return {
    coin: "BTC",
    satisfied,
    triggeredAt: satisfied ? "2026-01-01T00:00:00Z" : null,
    conditions: {
      price: { status: satisfied ? "satisfied" : "notSatisfied", lastTrueBar: null, evidence: null },
    },
    missingData: [],
    unavailableData: {},
  }
}

interface FakeTransport {
  transport: SignalRunTransport
  emit(event: SignalRunEvent): void
  setStartResult(result: Promise<number>): void
  starts(): number
  cancelled(): number[]
}

function fakeTransport(): FakeTransport {
  let handler: ((event: SignalRunEvent) => void) | undefined
  let startResult = Promise.resolve(7)
  let startCount = 0
  const cancelled: number[] = []

  return {
    transport: {
      start(_definition, _scope, onEvent) {
        handler = onEvent
        startCount += 1
        return startResult
      },
      cancel(runId) {
        cancelled.push(runId)
        return Promise.resolve(true)
      },
    },
    emit(event) {
      if (handler === undefined) throw new Error("signal run has not started")
      handler(event)
    },
    setStartResult(result) {
      startResult = result
    },
    starts() {
      return startCount
    },
    cancelled() {
      return [...cancelled]
    },
  }
}

describe("signal run contract", () => {
  it("serializes universe and explicit-coin scopes exactly", () => {
    const universe: SignalRunScope = { type: "universe" }
    const coins: SignalRunScope = { type: "coins", coins: ["BTC", "ETH"] }

    expect(JSON.parse(JSON.stringify(universe))).toEqual({ type: "universe" })
    expect(JSON.parse(JSON.stringify(coins))).toEqual({ type: "coins", coins: ["BTC", "ETH"] })
  })

  it("reduces ordered progress and terminal events", () => {
    let state = reduceSignalRunProgress(idleSignalRunProgress(), {
      event: "started",
      data: { runId: 7, total: 2 },
    })
    state = reduceSignalRunProgress(state, {
      event: "evaluated",
      data: { runId: 7, completed: 1, total: 2, report: report(true) },
    })
    state = reduceSignalRunProgress(state, {
      event: "evaluated",
      data: { runId: 7, completed: 2, total: 2, report: report(false) },
    })
    state = reduceSignalRunProgress(state, {
      event: "completed",
      data: { runId: 7, completed: 2, total: 2, matched: 1 },
    })

    expect(state).toEqual({ status: "completed", runId: 7, completed: 2, total: 2, matched: 1 })
  })

  it("ignores stale run IDs and preserves progress on failure", () => {
    const running = reduceSignalRunProgress(idleSignalRunProgress(), {
      event: "started",
      data: { runId: 7, total: 3 },
    })
    const stale = reduceSignalRunProgress(running, {
      event: "evaluated",
      data: { runId: 8, completed: 1, total: 3, report: report(true) },
    })
    const progressed = reduceSignalRunProgress(stale, {
      event: "evaluated",
      data: { runId: 7, completed: 1, total: 3, report: report(true) },
    })
    const failed = reduceSignalRunProgress(progressed, {
      event: "failed",
      data: { runId: 7, message: "history unavailable" },
    })

    expect(stale).toBe(running)
    expect(failed).toEqual({
      status: "failed",
      runId: 7,
      completed: 1,
      total: 3,
      matched: 1,
      message: "history unavailable",
    })
  })
})

describe("signal run client", () => {
  it("blocks a second start while the first command is pending", async () => {
    const fake = fakeTransport()
    fake.setStartResult(new Promise(() => undefined))
    const client = createSignalRunClient(fake.transport)

    void client.start(definition, { type: "universe" }, () => undefined)
    await expect(client.start(definition, { type: "universe" }, () => undefined)).rejects.toBeInstanceOf(
      SignalRunAlreadyActiveError
    )
    expect(fake.starts()).toBe(1)
  })

  it("accepts started before invoke resolves and releases before the terminal callback", async () => {
    const fake = fakeTransport()
    let resolveStart: ((runId: number) => void) | undefined
    fake.setStartResult(
      new Promise(resolve => {
        resolveStart = resolve
      })
    )
    const client = createSignalRunClient(fake.transport)
    let releasedBeforeCallback = false
    const started = client.start(definition, { type: "universe" }, event => {
      if (event.event === "completed") releasedBeforeCallback = !client.isActive()
    })

    fake.emit({ event: "started", data: { runId: 7, total: 1 } })
    expect(client.activeRunId()).toBe(7)
    fake.emit({ event: "completed", data: { runId: 7, completed: 1, total: 1, matched: 0 } })
    expect(client.activeRunId()).toBeNull()
    if (resolveStart === undefined) throw new Error("missing start resolver")
    resolveStart(7)

    await expect(started).resolves.toBe(7)
    expect(releasedBeforeCallback).toBe(true)
  })

  it("queues cancellation until the channel reveals the run ID", async () => {
    const fake = fakeTransport()
    fake.setStartResult(new Promise(() => undefined))
    const client = createSignalRunClient(fake.transport)
    void client.start(definition, { type: "universe" }, () => undefined)

    const cancellation = client.cancel()
    expect(fake.cancelled()).toEqual([])
    fake.emit({ event: "started", data: { runId: 7, total: 1 } })

    await expect(cancellation).resolves.toBe(true)
    expect(fake.cancelled()).toEqual([7])
  })

  it("rejects a command response whose run ID disagrees with the channel", async () => {
    const fake = fakeTransport()
    const client = createSignalRunClient(fake.transport)
    const started = client.start(definition, { type: "universe" }, () => undefined)
    fake.emit({ event: "started", data: { runId: 8, total: 1 } })

    await expect(started).rejects.toBeInstanceOf(SignalRunProtocolError)
    expect(client.isActive()).toBe(false)
  })
})
