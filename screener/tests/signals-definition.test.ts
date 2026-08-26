import { describe, expect, it } from "vitest"

import { SIGNAL_DEFINITION_VERSION } from "$lib/signals/definition"
import type { SignalDefinition } from "$lib/signals/definition"

function completeDefinition(): SignalDefinition {
  return {
    version: SIGNAL_DEFINITION_VERSION,
    direction: "bearish",
    root: {
      type: "all",
      children: [
        { type: "condition", id: "threshold" },
        {
          type: "any",
          children: [
            { type: "condition", id: "cross" },
            {
              type: "not",
              child: {
                type: "xor",
                children: [
                  { type: "condition", id: "divergence" },
                  { type: "condition", id: "threshold" },
                ],
              },
            },
          ],
        },
      ],
    },
    conditions: {
      threshold: {
        timeframe: "1h",
        predicate: {
          type: "threshold",
          left: { type: "field", field: "close" },
          op: "greaterOrEqual",
          right: {
            type: "indicator",
            instance: { id: "ema", params: { period: 20 } },
            output: "ema",
            adjustment: { type: "percent", value: 3 },
          },
        },
        withinBars: 0,
      },
      cross: {
        timeframe: "4h",
        predicate: {
          type: "cross",
          left: {
            type: "indicator",
            instance: { id: "sma", params: { period: 20 } },
            output: "sma",
            adjustment: { type: "absolute", value: 5 },
          },
          right: { type: "constant", value: 100 },
          direction: "under",
        },
        withinBars: 2,
      },
      divergence: {
        timeframe: "1d",
        predicate: {
          type: "divergence",
          instance: {
            id: "rsiDivergence",
            params: { period: 14, pivotLeft: 5, pivotRight: 5, maxGap: 60 },
          },
          kind: "hidden",
          direction: "bullish",
        },
        withinBars: 3,
      },
    },
  }
}

describe("signal definition v2 contract", () => {
  it("round-trips every tagged contract shape without renaming fields", () => {
    const definition = completeDefinition()
    const value = JSON.parse(JSON.stringify(definition))

    expect(value).toEqual(definition)
    expect(value.version).toBe(2)
    expect(value.root.type).toBe("all")
    expect(value.root.children[1].type).toBe("any")
    expect(value.root.children[1].children[1].type).toBe("not")
    expect(value.root.children[1].children[1].child.type).toBe("xor")
    expect(value.conditions.threshold.predicate.op).toBe("greaterOrEqual")
    expect(value.conditions.threshold.predicate.right.adjustment).toEqual({ type: "percent", value: 3 })
    expect(value.conditions.cross.predicate.left.adjustment).toEqual({ type: "absolute", value: 5 })
    expect(value.conditions.divergence.predicate).toMatchObject({
      type: "divergence",
      kind: "hidden",
      direction: "bullish",
    })
  })
})
