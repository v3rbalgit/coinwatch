import { describe, expect, it } from "vitest"

import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { SignalTimeframe } from "$lib/signals/definition"
import { compileSignalDraft } from "$lib/signals/draft"
import { buildSignalTemplate, SIGNAL_TEMPLATES } from "$lib/signals/templates"

type IndicatorParam = IndicatorDescriptor["params"][number]

function parameter(key: string, defaultValue: number): IndicatorParam {
  return { key, label: key, default: defaultValue, min: 1, max: 500, step: 1 }
}

function descriptor(
  id: string,
  kind: IndicatorDescriptor["kind"],
  outputDomain: IndicatorDescriptor["outputDomain"],
  params: IndicatorParam[],
  output?: string
): IndicatorDescriptor {
  return {
    id,
    label: id,
    kind,
    outputDomain,
    supportsSignals: true,
    target: "pane",
    params,
    outputs: output === undefined ? [] : [{ key: output, label: output, kind: "line", color: "#fff" }],
    guides: [],
    range: null,
  }
}

const descriptors: IndicatorDescriptor[] = [
  descriptor("sma", "series", "price", [parameter("period", 20)], "sma"),
  descriptor("ema", "series", "price", [parameter("period", 20)], "ema"),
  descriptor("rsi", "series", "boundedOscillator", [parameter("period", 14)], "rsi"),
  descriptor("rsiDivergence", "divergence", "boundedOscillator", [
    parameter("period", 14),
    parameter("pivotLeft", 5),
    parameter("pivotRight", 5),
    parameter("maxGap", 60),
  ]),
  descriptor("macdDivergence", "divergence", "unboundedOscillator", [
    parameter("fast", 12),
    parameter("slow", 26),
    parameter("signal", 9),
    parameter("pivotLeft", 5),
    parameter("pivotRight", 5),
    parameter("maxGap", 60),
  ]),
]

const allTimeframes: readonly SignalTimeframe[] = ["1h", "4h", "1d", "1w"]

describe("signal templates", () => {
  it("keeps every advertised template buildable", () => {
    expect(SIGNAL_TEMPLATES.map(template => template.id)).toEqual([
      "closeSma",
      "emaCross",
      "rsiMomentum",
      "rsiDivergence",
      "macdDivergence",
      "blank",
    ])

    for (const template of SIGNAL_TEMPLATES.filter(template => template.id !== "blank")) {
      const created = buildSignalTemplate(template.id, { direction: "bullish", timeframes: allTimeframes }, descriptors)
      expect(() => compileSignalDraft(created.draft)).not.toThrow()
    }
    const blank = buildSignalTemplate("blank", { direction: "bullish", timeframes: ["1h"] }, descriptors)
    expect(blank.draft.conditions).toEqual([])
    expect(() => compileSignalDraft(blank.draft)).toThrow("at least one condition")
  })

  it("makes trend and crossover templates direction-aware", () => {
    const bullishTrend = buildSignalTemplate("closeSma", { direction: "bullish", timeframes: ["4h"] }, descriptors)
    const bearishTrend = buildSignalTemplate("closeSma", { direction: "bearish", timeframes: ["4h"] }, descriptors)
    const bullishCross = buildSignalTemplate("emaCross", { direction: "bullish", timeframes: ["1h"] }, descriptors)
    const bearishCross = buildSignalTemplate("emaCross", { direction: "bearish", timeframes: ["1h"] }, descriptors)

    expect(bullishTrend.draft.conditions[0]?.predicate).toMatchObject({
      type: "threshold",
      op: "greaterThan",
    })
    expect(bearishTrend.draft.conditions[0]?.predicate).toMatchObject({
      type: "threshold",
      op: "lessThan",
    })
    expect(bullishCross.draft.conditions[0]?.predicate).toMatchObject({
      type: "cross",
      direction: "over",
      left: { instance: { params: { period: 20 } } },
      right: { instance: { params: { period: 50 } } },
    })
    expect(bearishCross.draft.conditions[0]?.predicate).toMatchObject({
      type: "cross",
      direction: "under",
    })
  })

  it("uses an RSI neutral-level cross for momentum", () => {
    const created = buildSignalTemplate("rsiMomentum", { direction: "bearish", timeframes: ["1d"] }, descriptors)

    expect(created.draft.conditions[0]).toMatchObject({
      timeframe: "1d",
      predicate: {
        type: "cross",
        left: { type: "indicator", instance: { id: "rsi", params: { period: 14 } }, output: "rsi" },
        right: { type: "constant", value: 50 },
        direction: "under",
      },
      withinBars: 0,
    })
  })

  it("builds divergence sweeps as fresh ANY conditions across unique timeframes", () => {
    const created = buildSignalTemplate(
      "macdDivergence",
      { direction: "bearish", timeframes: ["1h", "4h", "1h", "1d", "1w"] },
      descriptors
    )

    expect(created.draft.match).toBe("any")
    expect(created.draft.conditions.map(condition => condition.timeframe)).toEqual(allTimeframes)
    for (const condition of created.draft.conditions) {
      expect(condition).toMatchObject({
        predicate: {
          type: "divergence",
          instance: {
            id: "macdDivergence",
            params: { fast: 12, slow: 26, signal: 9, pivotLeft: 5, pivotRight: 5, maxGap: 60 },
          },
          kind: "any",
          direction: "bearish",
        },
        withinBars: 3,
      })
    }
  })

  it("fails explicitly when a required descriptor or timeframe is unavailable", () => {
    expect(() => buildSignalTemplate("closeSma", { direction: "bullish", timeframes: ["1h"] }, [])).toThrow(
      "Indicator descriptor 'sma' is unavailable"
    )
    expect(() => buildSignalTemplate("blank", { direction: "bullish", timeframes: [] }, descriptors)).toThrow(
      "at least one timeframe"
    )
  })
})
