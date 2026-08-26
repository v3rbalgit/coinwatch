import { describe, expect, it } from "vitest"

import type { IndicatorDescriptor } from "$lib/chart/indicators"
import {
  applyIndicatorPreset,
  createIndicatorInstance,
  resolveIndicatorInstance,
  resolveIndicatorParams,
} from "$lib/signals/instances"

const descriptor: IndicatorDescriptor = {
  id: "macd",
  label: "MACD",
  kind: "series",
  target: "pane",
  params: [
    { key: "fast", label: "Fast", default: 12, min: 1, max: 100, step: 1 },
    { key: "slow", label: "Slow", default: 26, min: 1, max: 200, step: 1 },
    { key: "signal", label: "Signal", default: 9, min: 1, max: 100, step: 1 },
  ],
  outputs: [
    { key: "macd", label: "MACD", kind: "line", color: "#fff" },
    { key: "signal", label: "Signal", kind: "line", color: "#aaa" },
  ],
  guides: [],
  range: null,
}

describe("signal indicator instances", () => {
  it("fills every descriptor default into a fresh parameter map", () => {
    const supplied = { fast: 8 }
    const resolved = resolveIndicatorParams(descriptor, supplied)

    expect(resolved).toEqual({ fast: 8, slow: 26, signal: 9 })
    expect(resolved).not.toBe(supplied)
    expect(createIndicatorInstance(descriptor, supplied)).toEqual({
      id: "macd",
      params: { fast: 8, slow: 26, signal: 9 },
    })
  })

  it("rejects unknown parameter keys and mismatched descriptors", () => {
    expect(() => resolveIndicatorParams(descriptor, { typo: 8 })).toThrow("Unknown parameter 'typo'")
    expect(() =>
      resolveIndicatorInstance(descriptor, {
        id: "rsi",
        params: { period: 14 },
      })
    ).toThrow("does not match descriptor")
  })

  it("copies preset values without retaining identity or aliases", () => {
    const preset = {
      status: "ready",
      id: "preset-1",
      indicatorId: "macd",
      name: "Fast",
      params: { fast: 6, slow: 18, signal: 5 },
    }
    const instance = applyIndicatorPreset(descriptor, preset)
    preset.params.fast = 10

    expect(instance).toEqual({ id: "macd", params: { fast: 6, slow: 18, signal: 5 } })
    expect(Object.keys(instance).sort()).toEqual(["id", "params"])
    expect(Object.keys(instance.params).sort()).toEqual(["fast", "signal", "slow"])
  })

  it("builds structurally equal instances for backend deduplication", () => {
    const first = createIndicatorInstance(descriptor, { fast: 8 })
    const second = createIndicatorInstance(descriptor, { fast: 8 })

    expect(first).toEqual(second)
    expect(first).not.toBe(second)
    expect(first.params).not.toBe(second.params)
  })
})
