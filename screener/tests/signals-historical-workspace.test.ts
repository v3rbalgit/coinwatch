import { describe, expect, it } from "vitest"

import type {
  HistoricalRange,
  HistoricalValidation,
  SignalValidationClient,
  SignalValidationEvent,
} from "$lib/signals/historical"
import {
  createHistoricalValidationWorkspace,
  type HistoricalValidationSnapshot,
} from "$lib/signals/historical-workspace"

import { thresholdDefinition } from "./signals-fixture"

const definition = thresholdDefinition()

const range: HistoricalRange = {
  start: "2026-08-25T00:00:00Z",
  end: "2026-08-26T00:00:00Z",
}

function snapshot(): HistoricalValidationSnapshot {
  return {
    name: "Price state",
    definition,
    coin: "BTC",
    range,
    rangeLabel: "Past day",
    source: "draft",
  }
}

function completedResult(): HistoricalValidation {
  return {
    coin: "BTC",
    range,
    anchorTimeframe: "1h",
    evaluatedPoints: 24,
    spans: [
      {
        startedAt: "2026-08-25T08:00:00Z",
        endedAt: "2026-08-25T11:00:00Z",
        activeAtStart: false,
        activeAtEnd: false,
        evidence: {
          coin: "BTC",
          satisfied: true,
          lastEvidenceAt: "2026-08-25T07:00:00Z",
          conditions: {
            price: {
              status: "satisfied",
              lastTrueBar: "2026-08-25T07:00:00Z",
              evidence: {
                type: "state",
                observedAt: "2026-08-25T07:00:00Z",
                left: 105,
                right: 100,
              },
            },
          },
          missingData: [],
          unavailableData: {},
        },
      },
    ],
  }
}

interface FakeClient {
  client: SignalValidationClient
  emit(event: SignalValidationEvent): void
}

function fakeClient(): FakeClient {
  let handler: ((event: SignalValidationEvent) => void) | undefined
  return {
    client: {
      start(_definition, _coin, _range, onEvent) {
        handler = onEvent
        return Promise.resolve(7)
      },
      cancel() {
        return Promise.resolve(true)
      },
      activeValidationId() {
        return 7
      },
      isActive() {
        return handler !== undefined
      },
    },
    emit(event) {
      if (handler === undefined) throw new Error("historical validation has not started")
      handler(event)
    },
  }
}

describe("historical signal validation workspace", () => {
  it("snapshots the definition and exposes completed spans as one visible overlay", async () => {
    const fake = fakeClient()
    const workspace = createHistoricalValidationWorkspace(fake.client)
    const source = snapshot()
    const starting = workspace.start(source)
    fake.emit({ event: "started", data: { validationId: 7, total: 1 } })
    await starting

    await workspace.start({ ...snapshot(), coin: "ETH" })
    expect(workspace.state.error).toContain("already running")
    expect(workspace.state.snapshot?.coin).toBe("BTC")

    const condition = source.definition.conditions.price
    if (condition?.predicate.type !== "threshold" || condition.predicate.right.type !== "constant") {
      throw new Error("invalid test fixture")
    }
    condition.predicate.right.value = 200
    expect(workspace.state.snapshot?.definition.conditions.price?.predicate).toMatchObject({
      right: { type: "constant", value: 100 },
    })

    fake.emit({ event: "completed", data: { validationId: 7, result: completedResult() } })
    expect(workspace.state.error).toBe("")
    expect(workspace.state.result?.spans).toHaveLength(1)
    expect(workspace.state.selectedSpan).toBe(0)
    expect(workspace.state.visible).toBe(true)
    expect(workspace.state.panelOpen).toBe(true)

    workspace.setVisible(false)
    expect(workspace.state.visible).toBe(false)
    workspace.clear()
    expect(workspace.state.result).toBeNull()
    expect(workspace.state.snapshot).toBeNull()
  })
})
