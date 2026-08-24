import init, { analyzeSeries } from "$lib/wasm/indicators.js";

/** Indicator parameters (camelCase for the wasm boundary; mirrors the scanner's ScanConfig). */
export type IndicatorConfig = {
  rsiPeriod: number;
  macdFast: number;
  macdSlow: number;
  macdSignal: number;
  pivotLeft: number;
  pivotRight: number;
  maxGap: number;
};

/** A divergence with pivot indices into the analyzed series. */
export type WasmDivergence = {
  source: "RSI" | "MACD";
  kind: "Regular" | "Hidden";
  direction: "Bullish" | "Bearish";
  priorIndex: number;
  recentIndex: number;
};

/** RSI/MACD series (NaN where undefined) plus detected divergences. */
export type Analysis = {
  rsi: number[];
  macd: number[];
  signal: number[];
  histogram: number[];
  divergences: WasmDivergence[];
};

/** Scanner defaults — the params the Signals page uses. */
export const defaultIndicatorConfig: IndicatorConfig = {
  rsiPeriod: 14,
  macdFast: 12,
  macdSlow: 26,
  macdSignal: 9,
  pivotLeft: 5,
  pivotRight: 5,
  maxGap: 60,
};

let ready: Promise<void> | undefined;

/** Load the wasm engine once; safe to await repeatedly. */
export function loadIndicators(): Promise<void> {
  ready ??= init().then(() => undefined);
  return ready;
}

/**
 * Compute indicators + divergences over `closes` using the shared Rust engine.
 * `loadIndicators()` must have resolved first.
 */
export function analyze(closes: Float64Array, config: IndicatorConfig): Analysis {
  const analysis: Analysis = analyzeSeries(closes, config);
  return analysis;
}
