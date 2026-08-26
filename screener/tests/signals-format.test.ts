import { describe, expect, it } from "vitest"

import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { SignalDefinition } from "$lib/signals/definition"
import { conditionLabel, definitionLines, operandLabel, predicateLabel } from "$lib/signals/format"

const descriptors: IndicatorDescriptor[] = [
  {
    id: "ema",
    label: "EMA",
    kind: "series",
    target: "overlay",
    params: [{ key: "period", label: "Period", default: 20, min: 1, max: 500, step: 1 }],
    outputs: [{ key: "ema", label: "EMA", kind: "line", color: "#fff" }],
    guides: [],
    range: null,
  },
  {
    id: "rsiDivergence",
    label: "RSI Divergence",
    kind: "divergence",
    target: "overlay",
    params: [{ key: "period", label: "Period", default: 14, min: 1, max: 500, step: 1 }],
    outputs: [],
    guides: [],
    range: null,
  },
]

describe("signal definition labels", () => {
  it("formats operands, adjustments, comparisons, and freshness", () => {
    expect(
      operandLabel(
        {
          type: "indicator",
          instance: { id: "ema", params: { period: 50 } },
          output: "ema",
          adjustment: { type: "percent", value: -3 },
        },
        descriptors
      )
    ).toBe("EMA (Period 50) · EMA -3%")
    expect(
      predicateLabel(
        {
          type: "threshold",
          left: { type: "field", field: "close", adjustment: { type: "absolute", value: 5 } },
          op: "greaterOrEqual",
          right: { type: "constant", value: 100 },
        },
        descriptors
      )
    ).toBe("Close +5 ≥ 100")
    expect(
      conditionLabel(
        {
          timeframe: "4h",
          predicate: {
            type: "divergence",
            instance: { id: "rsiDivergence", params: { period: 14 } },
            kind: "regular",
            direction: "bullish",
          },
          withinBars: 3,
        },
        descriptors
      )
    ).toBe("4h · Regular bullish RSI Divergence (Period 14) · within 3 bars")
  })

  it("renders a rich boolean tree without flattening it", () => {
    const definition: SignalDefinition = {
      version: 2,
      direction: "bullish",
      root: {
        type: "not",
        child: {
          type: "xor",
          children: [
            { type: "condition", id: "price" },
            { type: "condition", id: "momentum" },
          ],
        },
      },
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
        momentum: {
          timeframe: "4h",
          predicate: {
            type: "cross",
            left: { type: "field", field: "close" },
            right: { type: "constant", value: 90 },
            direction: "over",
          },
          withinBars: 1,
        },
      },
    }

    expect(definitionLines(definition, descriptors)).toEqual([
      { depth: 0, label: "NOT" },
      { depth: 1, label: "XOR" },
      { depth: 2, label: "1h · Close > 100 · latest bar" },
      { depth: 2, label: "4h · Close crosses over 90 · within 1 bar" },
    ])
  })
})
