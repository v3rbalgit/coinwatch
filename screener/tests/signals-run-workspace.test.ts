import { describe, expect, it } from "vitest"

import { createSignalRunClient } from "$lib/signals/run"
import type { SignalRunEvent, SignalRunTransport } from "$lib/signals/run"
import { createSignalRunWorkspace, snapshotSignalDefinition } from "$lib/signals/run-workspace"
import type { SignalRunSnapshot } from "$lib/signals/run-workspace"

import { thresholdDefinition } from "./signals-fixture"

const definition = thresholdDefinition()

function snapshot(name = "Trend"): SignalRunSnapshot {
  return {
    name,
    definition,
    scope: { type: "universe" },
    scopeLabel: "Entire universe",
    source: "draft",
    startedAt: "2026-08-26T10:00:00Z",
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

const evaluated: SignalRunEvent = {
  event: "evaluated",
  data: {
    runId: 7,
    completed: 1,
    total: 2,
    report: {
      coin: "BTC",
      satisfied: true,
      lastEvidenceAt: "2026-08-26T09:00:00Z",
      conditions: {
        price: { status: "satisfied", lastTrueBar: "2026-08-26T09:00:00Z", evidence: null },
      },
      missingData: [],
      unavailableData: {},
    },
  },
}

describe("signal run workspace", () => {
  it("deep-copies the definition captured for an asynchronous run", () => {
    const source = snapshotSignalDefinition(definition)
    const captured = snapshotSignalDefinition(source)
    const condition = source.conditions.price
    if (source.root.type !== "condition" || condition?.predicate.type !== "threshold") {
      throw new Error("missing signal definition fixture")
    }
    source.root.id = "changed"
    if (condition.predicate.right.type !== "constant") throw new Error("missing constant fixture")
    condition.predicate.right.value = 200

    expect(captured).toEqual(definition)
  })

  it("accepts a started event before the invoke promise resolves", async () => {
    const fake = fakeTransport()
    let resolveStart: ((runId: number) => void) | undefined
    fake.setStartResult(new Promise(resolve => (resolveStart = resolve)))
    const workspace = createSignalRunWorkspace(createSignalRunClient(fake.transport))

    const started = workspace.start(snapshot())
    expect(workspace.state.starting).toBe(true)
    fake.emit({ event: "started", data: { runId: 7, total: 2 } })
    expect(workspace.state.starting).toBe(false)
    expect(workspace.state.progress).toMatchObject({ status: "running", runId: 7, total: 2 })

    if (resolveStart === undefined) throw new Error("missing start resolver")
    resolveStart(7)
    await started
  })

  it("turns a post-start protocol rejection into a terminal workspace failure", async () => {
    const fake = fakeTransport()
    fake.setStartResult(Promise.resolve(8))
    const workspace = createSignalRunWorkspace(createSignalRunClient(fake.transport))

    const started = workspace.start(snapshot())
    fake.emit({ event: "started", data: { runId: 7, total: 2 } })
    await started

    expect(workspace.state.progress).toMatchObject({
      status: "failed",
      runId: 7,
      completed: 0,
      total: 2,
      matched: 0,
    })
    expect(workspace.state.error).toContain("started as 7 but command returned 8")
    expect(workspace.isActive()).toBe(false)
  })

  it("queues cancellation during startup and retains partial reports", async () => {
    const fake = fakeTransport()
    let resolveStart: ((runId: number) => void) | undefined
    fake.setStartResult(new Promise(resolve => (resolveStart = resolve)))
    const workspace = createSignalRunWorkspace(createSignalRunClient(fake.transport))
    const started = workspace.start(snapshot())

    const cancelled = workspace.cancel()
    expect(workspace.state.cancellationPending).toBe(true)
    expect(fake.cancelled()).toEqual([])
    fake.emit({ event: "started", data: { runId: 7, total: 2 } })
    await cancelled
    expect(fake.cancelled()).toEqual([7])
    expect(workspace.state.cancellationPending).toBe(true)

    fake.emit(evaluated)
    fake.emit({ event: "cancelled", data: { runId: 7, completed: 1, total: 2, matched: 1 } })
    expect(workspace.state.cancellationPending).toBe(false)
    expect(workspace.state.progress.status).toBe("cancelled")
    expect(workspace.state.reports.map(report => report.coin)).toEqual(["BTC"])

    if (resolveStart === undefined) throw new Error("missing start resolver")
    resolveStart(7)
    await started
  })

  it("shows closest candidates automatically when a completed run has no matches", async () => {
    const fake = fakeTransport()
    const workspace = createSignalRunWorkspace(createSignalRunClient(fake.transport))
    const started = workspace.start(snapshot())
    fake.emit({ event: "started", data: { runId: 7, total: 1 } })
    await started
    fake.emit({
      event: "evaluated",
      data: {
        runId: 7,
        completed: 1,
        total: 1,
        report: {
          coin: "BTC",
          satisfied: false,
          lastEvidenceAt: null,
          conditions: {
            price: {
              status: "notSatisfied",
              lastTrueBar: null,
              evidence: { type: "state", observedAt: "2026-08-26T09:00:00Z", left: 90, right: 100 },
            },
          },
          missingData: [],
          unavailableData: {},
        },
      },
    })
    fake.emit({ event: "completed", data: { runId: 7, completed: 1, total: 1, matched: 0 } })
    expect(workspace.state.filter).toBe("closest")
  })

  it("does not restart while active and resets terminal results for a deliberate new run", async () => {
    const fake = fakeTransport()
    let resolveStart: ((runId: number) => void) | undefined
    fake.setStartResult(new Promise(resolve => (resolveStart = resolve)))
    const workspace = createSignalRunWorkspace(createSignalRunClient(fake.transport))
    const first = workspace.start(snapshot("First"))
    fake.emit({ event: "started", data: { runId: 7, total: 1 } })
    fake.emit({ ...evaluated, data: { ...evaluated.data, total: 1 } })

    await workspace.start(snapshot("Blocked"))
    expect(fake.starts()).toBe(1)
    expect(workspace.state.snapshot?.name).toBe("First")
    expect(workspace.state.view).toBe("results")

    fake.emit({ event: "completed", data: { runId: 7, completed: 1, total: 1, matched: 1 } })
    if (resolveStart === undefined) throw new Error("missing start resolver")
    resolveStart(7)
    await first

    workspace.state.filter = "all"
    workspace.state.error = "old failure"
    fake.setStartResult(Promise.resolve(8))
    await workspace.start(snapshot("Second"))
    expect(fake.starts()).toBe(2)
    expect(workspace.state.snapshot?.name).toBe("Second")
    expect(workspace.state.reports).toEqual([])
    expect(workspace.state.filter).toBe("matches")
    expect(workspace.state.error).toBe("")
  })
})
