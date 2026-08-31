import { describe, expect, it } from "vitest"

import type { Condition, SignalDefinition } from "$lib/signals/definition"
import { compileSignalDraft, createConditionDraft, definitionToSignalDraft } from "$lib/signals/draft"
import type { SignalDraft } from "$lib/signals/draft"

function priceCondition(value: number): Condition {
  return {
    timeframe: "1h",
    predicate: {
      type: "threshold",
      left: { type: "field", field: "close" },
      op: "greaterThan",
      right: { type: "constant", value },
    },
    withinBars: 0,
  }
}

function draft(): SignalDraft {
  return {
    direction: "bullish",
    match: "all",
    conditions: [
      createConditionDraft(priceCondition(100), "price"),
      createConditionDraft(
        {
          timeframe: "4h",
          predicate: {
            type: "cross",
            left: { type: "field", field: "close" },
            right: {
              type: "indicator",
              instance: { id: "ema", params: { period: 20 } },
              output: "ema",
              adjustment: { type: "percent", value: 3 },
            },
            direction: "over",
          },
          withinBars: 2,
        },
        "ema-cross"
      ),
    ],
  }
}

describe("flat signal drafts", () => {
  it("compiles ordered conditions into an exact version-2 ALL tree", () => {
    const definition = compileSignalDraft(draft())

    expect(definition).toEqual({
      version: 3,
      direction: "bullish",
      root: {
        type: "all",
        children: [
          { type: "condition", id: "price" },
          { type: "condition", id: "ema-cross" },
        ],
      },
      conditions: {
        price: priceCondition(100),
        "ema-cross": {
          timeframe: "4h",
          predicate: {
            type: "cross",
            left: { type: "field", field: "close" },
            right: {
              type: "indicator",
              instance: { id: "ema", params: { period: 20 } },
              output: "ema",
              adjustment: { type: "percent", value: 3 },
            },
            direction: "over",
          },
          withinBars: 2,
        },
      },
    })
  })

  it("preserves IDs and snapshots nested values across repeated compiles", () => {
    const source = draft()
    const first = compileSignalDraft(source)
    const second = compileSignalDraft(source)
    const condition = source.conditions.find(candidate => candidate.id === "ema-cross")
    if (condition?.predicate.type !== "cross" || condition.predicate.right.type !== "indicator") {
      throw new Error("expected EMA cross fixture")
    }
    condition.predicate.right.instance.params.period = 50
    condition.predicate.right.adjustment = { type: "absolute", value: 5 }

    expect(first).toEqual(second)
    expect(first.conditions["ema-cross"]?.predicate).toMatchObject({
      right: {
        instance: { params: { period: 20 } },
        adjustment: { type: "percent", value: 3 },
      },
    })
  })

  it("round-trips definitions representable by the flat editor", () => {
    const source = draft()
    source.match = "any"
    const definition = compileSignalDraft(source)
    const restored = definitionToSignalDraft(definition)

    expect(restored).toEqual(source)
    if (restored === null) throw new Error("expected flat definition")
    expect(compileSignalDraft(restored)).toEqual(definition)
  })

  it("imports a single condition leaf as a one-card ALL draft", () => {
    const definition: SignalDefinition = {
      version: 3,
      direction: "bearish",
      root: { type: "condition", id: "price" },
      conditions: { price: priceCondition(90) },
    }

    expect(definitionToSignalDraft(definition)).toEqual({
      direction: "bearish",
      match: "all",
      conditions: [{ id: "price", ...priceCondition(90) }],
    })
  })

  it("imports constant-first predicates in source-first form without changing their meaning", () => {
    const definition: SignalDefinition = {
      version: 3,
      direction: "bullish",
      root: { type: "condition", id: "rsi" },
      conditions: {
        rsi: {
          timeframe: "1h",
          predicate: {
            type: "threshold",
            left: { type: "constant", value: 30 },
            op: "lessThan",
            right: { type: "indicator", instance: { id: "rsi", params: { period: 14 } }, output: "rsi" },
          },
          withinBars: 0,
        },
      },
    }

    const restored = definitionToSignalDraft(definition)
    expect(restored?.conditions[0]?.predicate).toEqual({
      type: "threshold",
      left: { type: "indicator", instance: { id: "rsi", params: { period: 14 } }, output: "rsi" },
      op: "greaterThan",
      right: { type: "constant", value: 30 },
    })
    if (restored === null) throw new Error("expected flat definition")
    const compiled = compileSignalDraft(restored)
    expect(compiled.conditions.rsi).toEqual({
      timeframe: "1h",
      predicate: restored.conditions[0]?.predicate,
      withinBars: 0,
    })
  })

  it("rejects empty and duplicate condition IDs before IPC", () => {
    expect(() => compileSignalDraft({ direction: "bullish", match: "all", conditions: [] })).toThrow(
      "at least one condition"
    )
    const duplicate = draft()
    duplicate.conditions[1] = createConditionDraft(priceCondition(200), "price")
    expect(() => compileSignalDraft(duplicate)).toThrow("Duplicate signal condition id 'price'")
  })

  it("does not flatten definitions that need the richer tree editor", () => {
    const flat = compileSignalDraft(draft())
    const nested: SignalDefinition = {
      ...flat,
      root: {
        type: "all",
        children: [
          { type: "condition", id: "price" },
          {
            type: "not",
            child: { type: "condition", id: "ema-cross" },
          },
        ],
      },
    }
    const unreferenced: SignalDefinition = {
      ...flat,
      root: { type: "all", children: [{ type: "condition", id: "price" }] },
    }

    expect(definitionToSignalDraft(nested)).toBeNull()
    expect(definitionToSignalDraft(unreferenced)).toBeNull()
  })
})
