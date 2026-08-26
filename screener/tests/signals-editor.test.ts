import { describe, expect, it } from "vitest"

import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { SignalCatalogEntry } from "$lib/signals/catalog"
import { compileSignalDraft, createConditionDraft } from "$lib/signals/draft"
import type { SignalDraft } from "$lib/signals/draft"
import {
  canSaveDraftSession,
  createDefaultOperand,
  createDefaultPredicate,
  createDraftSession,
  draftSnapshot,
  indicatorPresetErrorMessage,
  isDraftSessionDirty,
  openCatalogEntry,
  signalCatalogErrorMessage,
} from "$lib/signals/editor"
import { draftValidationMessage } from "$lib/signals/validation"

const descriptors: IndicatorDescriptor[] = [
  {
    id: "sma",
    label: "SMA",
    kind: "series",
    target: "overlay",
    params: [{ key: "period", label: "Period", default: 20, min: 1, max: 500, step: 1 }],
    outputs: [{ key: "sma", label: "SMA", kind: "line", color: "#fff" }],
    guides: [],
    range: null,
  },
  {
    id: "rsiDivergence",
    label: "RSI Divergence",
    kind: "divergence",
    target: "overlay",
    params: [
      { key: "period", label: "Period", default: 14, min: 1, max: 500, step: 1 },
      { key: "pivotLeft", label: "Pivot left", default: 5, min: 1, max: 50, step: 1 },
      { key: "pivotRight", label: "Pivot right", default: 5, min: 1, max: 50, step: 1 },
      { key: "maxGap", label: "Max gap", default: 60, min: 1, max: 500, step: 1 },
    ],
    outputs: [],
    guides: [],
    range: null,
  },
]

function draft(): SignalDraft {
  return {
    direction: "bullish",
    match: "all",
    conditions: [
      createConditionDraft(
        {
          timeframe: "1h",
          predicate: createDefaultPredicate("threshold", "bullish", descriptors),
          withinBars: 0,
        },
        "trend"
      ),
    ],
  }
}

describe("signal editor model", () => {
  it("tracks canonical dirty state for loaded and new drafts", () => {
    const source = draft()
    const entry: SignalCatalogEntry = {
      status: "ready",
      id: "signal-1",
      name: "Trend",
      definition: compileSignalDraft(source),
    }
    const loaded = openCatalogEntry(entry)

    expect(loaded.type).toBe("draft")
    expect(isDraftSessionDirty(loaded)).toBe(false)
    expect(canSaveDraftSession(loaded)).toBe(true)
    if (loaded.type !== "draft") throw new Error("expected draft session")
    loaded.name = "Renamed"
    expect(isDraftSessionDirty(loaded)).toBe(true)

    const fresh = createDraftSession("New trend", draft())
    expect(isDraftSessionDirty(fresh)).toBe(true)
    expect(canSaveDraftSession(fresh)).toBe(true)
    const blank = createDraftSession("Blank", { direction: "bullish", match: "all", conditions: [] })
    expect(canSaveDraftSession(blank)).toBe(false)
  })

  it("does not mark a single-leaf definition dirty merely because the editor canonicalizes it", () => {
    const source = draft()
    const entry: SignalCatalogEntry = {
      status: "ready",
      id: "signal-1",
      name: "Trend",
      definition: {
        ...compileSignalDraft(source),
        root: { type: "condition", id: "trend" },
      },
    }

    expect(isDraftSessionDirty(openCatalogEntry(entry))).toBe(false)
  })

  it("keeps rich and unavailable entries outside the flat editor", () => {
    const source = compileSignalDraft(draft())
    const rich: SignalCatalogEntry = {
      status: "ready",
      id: "rich",
      name: "Rich",
      definition: {
        ...source,
        root: { type: "not", child: { type: "condition", id: "trend" } },
      },
    }
    const unavailable: SignalCatalogEntry = {
      status: "unavailable",
      id: "old",
      name: "Old",
      issue: { type: "unsupportedVersion", version: 1 },
    }

    expect(openCatalogEntry(rich)).toEqual({ type: "rich", entry: rich })
    expect(openCatalogEntry(unavailable)).toEqual({ type: "unavailable", entry: unavailable })
  })

  it("creates descriptor-complete defaults for every editor shape", () => {
    expect(createDefaultOperand("field", descriptors)).toEqual({ type: "field", field: "close" })
    expect(createDefaultOperand("constant", descriptors)).toEqual({ type: "constant", value: 0 })
    expect(createDefaultOperand("indicator", descriptors)).toEqual({
      type: "indicator",
      instance: { id: "sma", params: { period: 20 } },
      output: "sma",
    })
    expect(createDefaultPredicate("cross", "bearish", descriptors)).toMatchObject({
      type: "cross",
      direction: "under",
    })
    expect(createDefaultPredicate("divergence", "bullish", descriptors)).toEqual({
      type: "divergence",
      instance: {
        id: "rsiDivergence",
        params: { period: 14, pivotLeft: 5, pivotRight: 5, maxGap: 60 },
      },
      kind: "any",
      direction: "bullish",
    })
  })

  it("rejects constant-only and invalid freshness conditions before IPC", () => {
    const constantOnly = draft()
    constantOnly.conditions[0] = createConditionDraft(
      {
        timeframe: "1h",
        predicate: {
          type: "threshold",
          left: { type: "constant", value: 1 },
          op: "greaterThan",
          right: { type: "constant", value: 0 },
        },
        withinBars: 0,
      },
      "constant-only"
    )
    expect(draftValidationMessage(constantOnly, descriptors)).toContain("market or indicator source")

    const stale = draft()
    const first = stale.conditions[0]
    if (first === undefined) throw new Error("missing condition fixture")
    stale.conditions[0] = { ...first, withinBars: 10001 }
    expect(draftValidationMessage(stale, descriptors)).toContain("0 to 10000")

    const offStep = draft()
    const trend = offStep.conditions[0]
    if (trend?.predicate.type !== "threshold" || trend.predicate.right.type !== "indicator") {
      throw new Error("missing indicator fixture")
    }
    trend.predicate.right.instance.params.period = 20.5
    expect(draftValidationMessage(offStep, descriptors)).toContain("increments of 1")
  })

  it("returns null snapshots for unsavable names, empty drafts, and duplicate IDs", () => {
    expect(draftSnapshot(" ", draft())).toBeNull()
    expect(draftSnapshot("Empty", { direction: "bullish", match: "all", conditions: [] })).toBeNull()
    const duplicate = draft()
    duplicate.conditions.push(createConditionDraft(duplicate.conditions[0], "trend"))
    expect(draftSnapshot("Duplicate", duplicate)).toBeNull()
  })

  it("turns structured IPC errors into actionable messages", () => {
    expect(signalCatalogErrorMessage({ type: "duplicateName", name: "Trend" })).toContain("already exists")
    expect(signalCatalogErrorMessage({ type: "notFound", id: "missing" })).toContain("Reload")
    expect(indicatorPresetErrorMessage({ type: "duplicateName", indicatorId: "sma", name: "Fast" })).toContain(
      "already exists"
    )
    expect(signalCatalogErrorMessage(new Error("transport failed"))).toBe("transport failed")
  })
})
