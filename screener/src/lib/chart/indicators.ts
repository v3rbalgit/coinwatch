import init, {
  computeIndicators as computeIndicatorsWasm,
  indicatorDescriptors as indicatorDescriptorsWasm,
} from "$lib/wasm/indicators.js"

let ready: Promise<void> | undefined

/** Load the wasm engine once; safe to await repeatedly. A failed load is
 * forgotten so a later call retries instead of replaying the rejection. */
export function loadIndicators(): Promise<void> {
  if (ready === undefined) {
    const loading = init().then(
      () => undefined,
      (error: unknown) => {
        ready = undefined
        throw error
      }
    )
    ready = loading
    return loading
  }
  return ready
}

// ---- Indicator engine: the chart's instance-driven, toggleable overlays. ----

/** Where an indicator draws. */
type IndicatorTarget = "overlay" | "pane"

/** What an indicator instance produces. */
type IndicatorKind = "series" | "divergence"

/** How one output series is plotted. */
type OutputKind = "line" | "histogram"

/** Numeric family represented by an indicator's outputs. */
export type ValueDomain = "price" | "priceDelta" | "volume" | "boundedOscillator" | "unboundedOscillator"

/** A tunable indicator parameter (defaults now, editable in the future signal builder). */
type IndicatorParam = {
  key: string
  label: string
  default: number
  min: number
  max: number
  step: number
}

/** One plotted output of an indicator. */
type IndicatorOutput = {
  key: string
  label: string
  kind: OutputKind
  color: string
}

/** Metadata for one indicator: drives the toggle picker and the renderer. */
export type IndicatorDescriptor = {
  id: string
  label: string
  kind: IndicatorKind
  outputDomain: ValueDomain
  supportsSignals: boolean
  target: IndicatorTarget
  params: IndicatorParam[]
  outputs: IndicatorOutput[]
  guides: number[]
  range: [number, number] | null
}

/** A configured indicator to compute: an id plus its params. */
export type IndicatorInstance = {
  id: string
  params: Record<string, number>
}

/** One computed output series (NaN where undefined), keyed to its {@link IndicatorOutput}. */
type OutputSeries = {
  key: string
  values: number[]
}

/** A divergence line between two pivots (indices into the analyzed series). */
type DivergenceSpan = {
  fromIndex: number
  toIndex: number
  kind: "Regular" | "Hidden"
  direction: "Bullish" | "Bearish"
}

/** The outputs of one computed instance: plotted series and/or divergence spans. */
export type InstanceOutput = {
  outputs: OutputSeries[]
  divergences: DivergenceSpan[]
}

type IndicatorColumns = {
  high: Float64Array
  low: Float64Array
  close: Float64Array
  volume: Float64Array
}

/** The indicator registry (metadata for the toggle picker). `loadIndicators()` must have resolved. */
export function indicatorDescriptors(): IndicatorDescriptor[] {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- wasm/serde boundary; shape guaranteed by the Rust types
  const descriptors: IndicatorDescriptor[] = indicatorDescriptorsWasm()
  return descriptors
}

/**
 * Compute configured indicator instances over an OHLCV series (columns equal length).
 * `loadIndicators()` must have resolved first.
 */
export function computeIndicators(instances: IndicatorInstance[], columns: IndicatorColumns): InstanceOutput[] {
  const { high, low, close, volume } = columns
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- wasm/serde boundary; shape guaranteed by the Rust types
  const outputs: InstanceOutput[] = computeIndicatorsWasm(instances, high, low, close, volume)
  return outputs
}
