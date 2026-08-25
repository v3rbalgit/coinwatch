import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { beforeAll, describe, expect, it } from "vitest";

import type { Analysis, IndicatorConfig, IndicatorDescriptor, InstanceOutput } from "$lib/chart/indicators";
import init, { analyzeSeries, computeIndicators, indicatorDescriptors } from "$lib/wasm/indicators.js";

const config: IndicatorConfig = {
  rsiPeriod: 14,
  macdFast: 12,
  macdSlow: 26,
  macdSignal: 9,
  pivotLeft: 5,
  pivotRight: 5,
  maxGap: 60,
};

function run(closes: number[]): Analysis {
  const analysis: Analysis = analyzeSeries(Float64Array.from(closes), config);
  return analysis;
}

// Node's fetch can't load a file:// URL, so hand the wasm bytes to init directly.
beforeAll(async () => {
  const wasm = readFileSync(fileURLToPath(new URL("../src/lib/wasm/indicators_bg.wasm", import.meta.url)));
  await init({ module_or_path: wasm });
});

describe("wasm indicator engine (parity with the Rust scanner)", () => {
  it("saturates RSI to 100 on a monotonic rise", () => {
    const rsi = run(Array.from({ length: 60 }, (_, index) => index + 1)).rsi.filter((value) => !Number.isNaN(value));
    expect(rsi.at(-1)).toBeCloseTo(100, 9);
  });

  it("lifts the MACD line above zero in an uptrend", () => {
    const macd = run(Array.from({ length: 60 }, (_, index) => index + 1)).macd.filter((value) => !Number.isNaN(value));
    expect(macd.at(-1)).toBeGreaterThan(0);
  });

  it("finds no divergence in a clean trend", () => {
    expect(run(Array.from({ length: 200 }, (_, index) => index + 1)).divergences).toHaveLength(0);
  });

  it("keeps series aligned and the histogram equal to macd - signal", () => {
    const closes = Array.from({ length: 120 }, (_, index) => 100 + Math.sin(index / 5) * 10);
    const result = run(closes);
    expect(result.rsi).toHaveLength(closes.length);
    expect(result.macd).toHaveLength(closes.length);
    result.macd.forEach((macd, index) => {
      const signal = result.signal[index];
      if (!Number.isNaN(macd) && signal !== undefined && !Number.isNaN(signal)) {
        expect(result.histogram[index]).toBeCloseTo(macd - signal, 9);
      }
    });
  });
});

describe("wasm indicator engine (instance-driven, toggleable)", () => {
  const rising = Array.from({ length: 60 }, (_, index) => index + 1);
  const columns = () => {
    const values = Float64Array.from(rising);
    return { high: values, low: values, close: values, volume: values };
  };

  it("describes 17 toggleable indicators with targets, ranges, and guides", () => {
    const descriptors: IndicatorDescriptor[] = indicatorDescriptors();
    expect(descriptors).toHaveLength(17);
    const sma = descriptors.find((descriptor) => descriptor.id === "sma");
    expect(sma?.target).toBe("overlay");
    expect(sma?.outputs[0]?.key).toBe("sma");
    const rsi = descriptors.find((descriptor) => descriptor.id === "rsi");
    expect(rsi?.target).toBe("pane");
    expect(rsi?.range).toEqual([0, 100]);
    expect(rsi?.guides).toEqual([30, 70]);
  });

  it("computes an SMA aligned to the input length", () => {
    const { high, low, close, volume } = columns();
    const results: InstanceOutput[] = computeIndicators([{ id: "sma", params: { period: 5 } }], high, low, close, volume);
    const output = results[0]?.outputs[0];
    expect(output?.key).toBe("sma");
    expect(output?.values).toHaveLength(rising.length);
    expect(output?.values[4]).toBeCloseTo(3, 9); // mean(1..5)
  });

  it("matches analyze() for RSI — shared Rust code, no drift", () => {
    const { high, low, close, volume } = columns();
    const results: InstanceOutput[] = computeIndicators([{ id: "rsi", params: { period: 14 } }], high, low, close, volume);
    const engineRsi = results[0]?.outputs[0]?.values ?? [];
    const analyzed: Analysis = analyzeSeries(Float64Array.from(rising), config);
    engineRsi.forEach((value, index) => {
      const reference = analyzed.rsi[index];
      if (!Number.isNaN(value) && reference !== undefined && !Number.isNaN(reference)) {
        expect(value).toBeCloseTo(reference, 9);
      }
    });
  });

  it("yields no outputs for an unknown id", () => {
    const { high, low, close, volume } = columns();
    const results: InstanceOutput[] = computeIndicators([{ id: "nope", params: {} }], high, low, close, volume);
    expect(results[0]?.outputs).toHaveLength(0);
  });
});
