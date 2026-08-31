import { describe, expect, it } from "vitest"

import type { IndicatorDescriptor } from "$lib/chart/indicators"
import {
  CANDLE_FIELD_SOURCE_DESCRIPTORS,
  compatibleSignalIndicators,
  OPERAND_TYPE_DESCRIPTORS,
} from "$lib/signals/source-catalog"

const descriptors: IndicatorDescriptor[] = [
  {
    id: "ema",
    label: "EMA",
    kind: "series",
    outputDomain: "price",
    supportsSignals: true,
    target: "overlay",
    params: [],
    outputs: [{ key: "ema", label: "EMA", kind: "line", color: "#fff" }],
    guides: [],
    range: null,
  },
  {
    id: "rsi",
    label: "RSI",
    kind: "series",
    outputDomain: "boundedOscillator",
    supportsSignals: true,
    target: "pane",
    params: [],
    outputs: [{ key: "rsi", label: "RSI", kind: "line", color: "#fff" }],
    guides: [30, 70],
    range: [0, 100],
  },
  {
    id: "obv",
    label: "OBV",
    kind: "series",
    outputDomain: "volume",
    supportsSignals: false,
    target: "pane",
    params: [],
    outputs: [{ key: "obv", label: "OBV", kind: "line", color: "#fff" }],
    guides: [],
    range: null,
  },
]

describe("signal source catalog", () => {
  it("keeps numbers available only as references", () => {
    expect(OPERAND_TYPE_DESCRIPTORS.find(type => type.value === "constant")?.subjectEligible).toBe(false)
    expect(OPERAND_TYPE_DESCRIPTORS.flatMap(type => (type.subjectEligible ? [type.value] : []))).toEqual([
      "field",
      "indicator",
    ])
  })

  it("describes candle-field domains centrally", () => {
    expect(CANDLE_FIELD_SOURCE_DESCRIPTORS.find(field => field.value === "close")?.domain).toBe("price")
    expect(CANDLE_FIELD_SOURCE_DESCRIPTORS.find(field => field.value === "volume")?.domain).toBe("volume")
  })

  it("offers only signal-compatible indicators in the required domain", () => {
    expect(compatibleSignalIndicators(descriptors, "price", null).map(descriptor => descriptor.id)).toEqual(["ema"])
    expect(compatibleSignalIndicators(descriptors, null, null).map(descriptor => descriptor.id)).toEqual(["ema", "rsi"])
    expect(compatibleSignalIndicators(descriptors, "price", "rsi").map(descriptor => descriptor.id)).toEqual([
      "ema",
      "rsi",
    ])
  })
})
