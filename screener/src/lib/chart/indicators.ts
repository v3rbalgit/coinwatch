import init, {
  analyzeSeries,
  computeIndicators as computeIndicatorsWasm,
  indicatorDescriptors as indicatorDescriptorsWasm,
} from "$lib/wasm/indicators.js";

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

// ---- Indicator engine: the chart's instance-driven, toggleable overlays. ----

/** Where an indicator draws. */
export type IndicatorTarget = "overlay" | "pane";

/** How one output series is plotted. */
export type OutputKind = "line" | "histogram";

/** A tunable indicator parameter (defaults now, editable in the future signal builder). */
export type IndicatorParam = {
  key: string;
  label: string;
  default: number;
  min: number;
  max: number;
  step: number;
};

/** One plotted output of an indicator. */
export type IndicatorOutput = {
  key: string;
  label: string;
  kind: OutputKind;
  color: string;
};

/** Metadata for one indicator: drives the toggle picker and the renderer. */
export type IndicatorDescriptor = {
  id: string;
  label: string;
  target: IndicatorTarget;
  params: IndicatorParam[];
  outputs: IndicatorOutput[];
  guides: number[];
  range: [number, number] | null;
};

/** A configured indicator to compute: an id plus its params. */
export type IndicatorInstance = {
  id: string;
  params: Record<string, number>;
};

/** One computed output series (NaN where undefined), keyed to its {@link IndicatorOutput}. */
export type OutputSeries = {
  key: string;
  values: number[];
};

/** The outputs of one computed instance, in descriptor order. */
export type InstanceOutput = {
  outputs: OutputSeries[];
};

/** The indicator registry (metadata for the toggle picker). `loadIndicators()` must have resolved. */
export function indicatorDescriptors(): IndicatorDescriptor[] {
  const descriptors: IndicatorDescriptor[] = indicatorDescriptorsWasm();
  return descriptors;
}

/**
 * Compute configured indicator instances over an OHLCV series (columns equal length).
 * `loadIndicators()` must have resolved first.
 */
export function computeIndicators(
  instances: IndicatorInstance[],
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
  volume: Float64Array,
): InstanceOutput[] {
  const outputs: InstanceOutput[] = computeIndicatorsWasm(instances, high, low, close, volume);
  return outputs;
}
