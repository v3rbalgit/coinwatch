import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"

import { beforeAll, describe, expect, it } from "vitest"

import {
  computeIndicators,
  indicatorDescriptors,
  type IndicatorInstance,
  type InstanceOutput,
} from "$lib/chart/indicators"
import init from "$lib/wasm/indicators.js"

function columns(values: readonly number[]) {
  const close = Float64Array.from(values)
  return {
    high: Float64Array.from(values, value => value + 0.5),
    low: Float64Array.from(values, value => value - 0.5),
    close,
    volume: Float64Array.from(values, (_value, index) => 1_000 + index),
  }
}

function computeOne(id: string, params: Record<string, number>, values: readonly number[]): InstanceOutput {
  const { high, low, close, volume } = columns(values)
  const result = computeIndicators([{ id, params }], { high, low, close, volume })[0]
  if (result === undefined) throw new Error(`missing output for '${id}'`)
  return result
}

function series(output: InstanceOutput, key: string): number[] {
  const values = output.outputs.find(candidate => candidate.key === key)?.values
  if (values === undefined) throw new Error(`missing '${key}' output`)
  return values
}

// Node's fetch can't load a file:// URL, so hand the wasm bytes to init directly.
beforeAll(async () => {
  const wasm = readFileSync(fileURLToPath(new URL("../src/lib/wasm/indicators_bg.wasm", import.meta.url)))
  await init({ module_or_path: wasm })
})

describe("wasm indicator boundary", () => {
  const rising = Array.from({ length: 60 }, (_, index) => index + 1)

  it("describes every indicator with targets, ranges, and guides", () => {
    const descriptors = indicatorDescriptors()
    expect(descriptors).toHaveLength(19)
    const sma = descriptors.find(descriptor => descriptor.id === "sma")
    expect(sma?.target).toBe("overlay")
    expect(sma?.outputDomain).toBe("price")
    expect(sma?.outputs[0]?.key).toBe("sma")
    const rsi = descriptors.find(descriptor => descriptor.id === "rsi")
    expect(rsi?.target).toBe("pane")
    expect(rsi?.outputDomain).toBe("boundedOscillator")
    expect(rsi?.range).toEqual([0, 100])
    expect(rsi?.guides).toEqual([30, 70])
    expect(rsi?.supportsSignals).toBe(true)
    expect(descriptors.find(descriptor => descriptor.id === "vwap")?.supportsSignals).toBe(false)
    expect(descriptors.find(descriptor => descriptor.id === "obv")?.supportsSignals).toBe(false)
    const divergence = descriptors.find(descriptor => descriptor.id === "rsiDivergence")
    expect(divergence?.target).toBe("overlay")
    expect(divergence?.outputDomain).toBe("boundedOscillator")
    expect(divergence?.outputs).toHaveLength(0)
  })

  it("serializes every registered default instance across the boundary", () => {
    const descriptors = indicatorDescriptors()
    const instances: IndicatorInstance[] = descriptors.map(descriptor => ({ id: descriptor.id, params: {} }))
    const values = Array.from({ length: 600 }, (_, index) => 100 + index * 0.05 + Math.sin(index / 9) * 4)
    const { high, low, close, volume } = columns(values)
    const results = computeIndicators(instances, { high, low, close, volume })

    expect(results).toHaveLength(descriptors.length)
    results.forEach((result, index) => {
      const descriptor = descriptors[index]
      if (descriptor === undefined) throw new Error(`missing descriptor at index ${index}`)
      expect(result.outputs.map(output => output.key)).toEqual(descriptor.outputs.map(output => output.key))
      result.outputs.forEach(output => expect(output.values).toHaveLength(values.length))
    })
  })

  it("computes an SMA aligned to the input length", () => {
    const output = series(computeOne("sma", { period: 5 }, rising), "sma")
    expect(output).toHaveLength(rising.length)
    expect(output[4]).toBeCloseTo(3, 9)
  })

  it("saturates RSI at 100 on an uninterrupted rise", () => {
    const values = series(computeOne("rsi", { period: 14 }, rising), "rsi").filter(value => !Number.isNaN(value))
    expect(values.at(-1)).toBeCloseTo(100, 9)
  })

  it("keeps MACD outputs aligned and derives the histogram from signal", () => {
    const output = computeOne("macd", { fast: 12, slow: 26, signal: 9 }, rising)
    const macd = series(output, "macd")
    const signal = series(output, "signal")
    const histogram = series(output, "histogram")
    expect(macd).toHaveLength(rising.length)
    expect(signal).toHaveLength(rising.length)
    expect(histogram).toHaveLength(rising.length)
    expect(macd.filter(value => !Number.isNaN(value)).at(-1)).toBeGreaterThan(0)
    macd.forEach((value, index) => {
      const signalValue = signal[index]
      const histogramValue = histogram[index]
      if (
        !Number.isNaN(value) &&
        signalValue !== undefined &&
        !Number.isNaN(signalValue) &&
        histogramValue !== undefined
      ) {
        expect(histogramValue).toBeCloseTo(value - signalValue, 9)
      }
    })
  })

  it("finds no default divergence in a monotonic trend", () => {
    const values = Array.from({ length: 200 }, (_, index) => index + 1)
    expect(computeOne("rsiDivergence", {}, values).divergences).toEqual([])
    expect(computeOne("macdDivergence", {}, values).divergences).toEqual([])
  })

  it("rejects an unknown indicator instead of silently rendering nothing", () => {
    expect(() => computeOne("nope", {}, rising)).toThrow("unknown indicator 'nope'")
  })
})
