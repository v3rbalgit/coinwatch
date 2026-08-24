import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { beforeAll, describe, expect, it } from "vitest";

import type { Analysis, IndicatorConfig } from "$lib/chart/indicators";
import init, { analyzeSeries } from "$lib/wasm/indicators.js";

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
