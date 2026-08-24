<script lang="ts" module>
  export type Bar = { time: number; open: number; high: number; low: number; close: number; volume: number };
</script>

<script lang="ts">
  import {
    CandlestickSeries,
    ColorType,
    CrosshairMode,
    HistogramSeries,
    LineSeries,
    LineStyle,
    PriceScaleMode,
    createChart,
    type IChartApi,
    type ISeriesApi,
    type UTCTimestamp,
  } from "lightweight-charts";
  import { onMount } from "svelte";

  import { analyze, defaultIndicatorConfig, loadIndicators, type Analysis, type IndicatorConfig } from "./indicators";
  import { chartTheme, fmtPrice } from "./theme";

  // `seed` changes when the whole series is replaced (coin / timeframe switch or a
  // history prepend), triggering a full setData + refit; otherwise bars flow in as
  // tail updates. Indicators are computed here from `bars` via the shared Rust
  // engine (WASM), so they match the scanner and update per tick over any depth.
  let {
    bars,
    seed = 0,
    config = defaultIndicatorConfig,
    onLoadOlder,
  }: { bars: Bar[]; seed?: number; config?: IndicatorConfig; onLoadOlder?: () => void } = $props();

  const VISIBLE_BARS = 150;
  const RIGHT_OFFSET = 6;
  const LOAD_OLDER_THRESHOLD = 10;

  let container: HTMLDivElement;
  let chart: IChartApi | undefined;
  let candles: ISeriesApi<"Candlestick"> | undefined;
  let volume: ISeriesApi<"Histogram"> | undefined;
  let rsiSeries: ISeriesApi<"Line"> | undefined;
  let macdSeries: ISeriesApi<"Line"> | undefined;
  let signalSeries: ISeriesApi<"Line"> | undefined;
  let histSeries: ISeriesApi<"Histogram"> | undefined;
  let divergenceLines: ISeriesApi<"Line">[] = [];
  let plottedSeed = -1;
  let plottedIndicatorSeed = -1;
  let plottedFirstTime = 0;
  let plottedIndicatorFirstTime = 0;
  let divergenceKey = "";
  let lastTime = 0;
  let suppressLoadOlder = false;
  let hovering = false;
  let ready = $state(false);
  let wasmReady = $state(false);

  type LegendData = {
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    change: number;
    rsi: number | null;
    macd: number | null;
  };
  let legend = $state<LegendData | null>(null);
  let logScale = $state(false);
  let latestAnalysis: Analysis | undefined;

  const defined = (value: number | undefined) => (value !== undefined && !Number.isNaN(value) ? value : null);
  const fmtVol = (value: number) =>
    value >= 1e9
      ? (value / 1e9).toFixed(2) + "B"
      : value >= 1e6
        ? (value / 1e6).toFixed(1) + "M"
        : value >= 1e3
          ? (value / 1e3).toFixed(0) + "k"
          : value.toFixed(0);

  function legendFor(bar: Bar, index: number): LegendData {
    return {
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.volume,
      change: bar.open !== 0 ? (bar.close / bar.open - 1) * 100 : 0,
      rsi: defined(latestAnalysis?.rsi[index]),
      macd: defined(latestAnalysis?.macd[index]),
    };
  }

  // lightweight-charts' UTCTimestamp is a branded number; the cast is the library's
  // required way to build one from epoch seconds.
  const stamp = (time: number) => time as UTCTimestamp;

  const toCandle = (bar: Bar) => ({ time: stamp(bar.time), open: bar.open, high: bar.high, low: bar.low, close: bar.close });
  const toVolume = (bar: Bar) => ({
    time: stamp(bar.time),
    value: bar.volume,
    color: bar.close >= bar.open ? chartTheme.volumeUp : chartTheme.volumeDown,
  });

  // Align a WASM series (NaN where undefined) to each bar's time, dropping gaps.
  function toLine(series: number[]) {
    const points: { time: UTCTimestamp; value: number }[] = [];
    bars.forEach((bar, index) => {
      const value = series[index];
      if (value !== undefined && !Number.isNaN(value)) points.push({ time: stamp(bar.time), value });
    });
    return points;
  }
  function toHistogram(series: number[]) {
    const points: { time: UTCTimestamp; value: number; color: string }[] = [];
    bars.forEach((bar, index) => {
      const value = series[index];
      if (value !== undefined && !Number.isNaN(value)) {
        points.push({ time: stamp(bar.time), value, color: value >= 0 ? chartTheme.histUp : chartTheme.histDown });
      }
    });
    return points;
  }
  function updateLast(series: ISeriesApi<"Line">, values: number[]) {
    for (let index = values.length - 1; index >= 0; index--) {
      const value = values[index];
      const bar = bars[index];
      if (bar && value !== undefined && !Number.isNaN(value)) {
        series.update({ time: stamp(bar.time), value });
        return;
      }
    }
  }

  onMount(() => {
    chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: chartTheme.background },
        textColor: chartTheme.text,
        fontSize: 11,
      },
      grid: { vertLines: { color: chartTheme.grid }, horzLines: { color: chartTheme.grid } },
      crosshair: { mode: CrosshairMode.Normal },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: chartTheme.border, rightOffset: RIGHT_OFFSET },
      rightPriceScale: { borderColor: chartTheme.border },
    });

    candles = chart.addSeries(CandlestickSeries, {
      upColor: chartTheme.up,
      downColor: chartTheme.down,
      borderUpColor: chartTheme.up,
      borderDownColor: chartTheme.down,
      wickUpColor: chartTheme.up,
      wickDownColor: chartTheme.down,
    });
    volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
      lastValueVisible: false,
      priceLineVisible: false,
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    // RSI pane (fixed 0–100 with 30/70 guides).
    rsiSeries = chart.addSeries(
      LineSeries,
      {
        color: chartTheme.rsi,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: true,
        autoscaleInfoProvider: () => ({ priceRange: { minValue: 0, maxValue: 100 } }),
      },
      1,
    );
    for (const level of [30, 70]) {
      rsiSeries.createPriceLine({
        price: level,
        color: chartTheme.guide,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: false,
        title: "",
      });
    }

    // MACD pane (histogram behind the two lines).
    histSeries = chart.addSeries(HistogramSeries, { priceLineVisible: false, lastValueVisible: false }, 2);
    macdSeries = chart.addSeries(LineSeries, { color: chartTheme.macd, lineWidth: 1, priceLineVisible: false }, 2);
    signalSeries = chart.addSeries(LineSeries, { color: chartTheme.signal, lineWidth: 1, priceLineVisible: false }, 2);

    const panes = chart.panes();
    panes[0]?.setStretchFactor(3);
    panes[1]?.setStretchFactor(1);
    panes[2]?.setStretchFactor(1);

    chart.subscribeCrosshairMove((param) => {
      const index = param.time === undefined ? -1 : bars.findIndex((bar) => bar.time === param.time);
      const bar = index >= 0 ? bars[index] : undefined;
      if (bar) {
        hovering = true;
        legend = legendFor(bar, index);
      } else {
        hovering = false;
        const last = bars.at(-1);
        legend = last ? legendFor(last, bars.length - 1) : null;
      }
    });

    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (suppressLoadOlder) {
        suppressLoadOlder = false;
        return;
      }
      if (range && range.from < LOAD_OLDER_THRESHOLD) onLoadOlder?.();
    });

    void loadIndicators().then(() => {
      wasmReady = true;
    });

    ready = true;
    return () => {
      chart?.remove();
      chart = undefined;
    };
  });

  // Candles + volume: full reseed on seed change, else tail updates.
  $effect(() => {
    if (!ready || !candles || !volume || bars.length === 0) return;
    const firstTime = bars[0].time;
    if (seed !== plottedSeed) {
      candles.setData(bars.map(toCandle));
      volume.setData(bars.map(toVolume));
      suppressLoadOlder = true;
      chart?.timeScale().setVisibleLogicalRange({
        from: Math.max(0, bars.length - VISIBLE_BARS),
        to: bars.length + RIGHT_OFFSET,
      });
      plottedSeed = seed;
    } else if (firstTime < plottedFirstTime) {
      // Older history paged in: replot without a refit, then shift the range so the
      // same bars stay in view.
      const prepended = bars.findIndex((bar) => bar.time >= plottedFirstTime);
      const range = chart?.timeScale().getVisibleLogicalRange();
      candles.setData(bars.map(toCandle));
      volume.setData(bars.map(toVolume));
      if (range && prepended > 0) {
        suppressLoadOlder = true;
        chart?.timeScale().setVisibleLogicalRange({ from: range.from + prepended, to: range.to + prepended });
      }
    } else {
      for (const bar of bars.filter((candle) => candle.time >= lastTime)) {
        candles.update(toCandle(bar));
        volume.update(toVolume(bar));
      }
    }
    plottedFirstTime = firstTime;
    lastTime = bars[bars.length - 1].time;
    if (!hovering) {
      const last = bars[bars.length - 1];
      legend = legendFor(last, bars.length - 1);
    }
  });

  // Toggle the price pane between linear and logarithmic.
  $effect(() => {
    chart?.priceScale("right").applyOptions({ mode: logScale ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal });
  });

  // Indicators: compute RSI/MACD + divergences via the WASM engine over the current
  // bars. Full replot on reseed, per-tick tail updates otherwise; divergence lines
  // are rebuilt only when the set actually changes.
  $effect(() => {
    if (!ready || !wasmReady || !chart || !rsiSeries || !macdSeries || !signalSeries || !histSeries || bars.length === 0) {
      return;
    }
    const closes = Float64Array.from(bars, (bar) => bar.close);
    const analysis = analyze(closes, config);
    latestAnalysis = analysis;
    const firstTime = bars[0].time;

    if (seed !== plottedIndicatorSeed || firstTime < plottedIndicatorFirstTime) {
      rsiSeries.setData(toLine(analysis.rsi));
      macdSeries.setData(toLine(analysis.macd));
      signalSeries.setData(toLine(analysis.signal));
      histSeries.setData(toHistogram(analysis.histogram));
      plottedIndicatorSeed = seed;
    } else {
      updateLast(rsiSeries, analysis.rsi);
      updateLast(macdSeries, analysis.macd);
      updateLast(signalSeries, analysis.signal);
      const point = analysis.histogram.reduceRight<{ index: number; value: number } | null>((found, value, index) => {
        if (found || value === undefined || Number.isNaN(value)) return found;
        return { index, value };
      }, null);
      const bar = point ? bars[point.index] : undefined;
      if (point && bar) {
        histSeries.update({ time: stamp(bar.time), value: point.value, color: point.value >= 0 ? chartTheme.histUp : chartTheme.histDown });
      }
    }
    plottedIndicatorFirstTime = firstTime;

    const key = analysis.divergences
      .map((div) => `${div.source}:${div.kind}:${div.direction}:${div.priorIndex}:${div.recentIndex}`)
      .join("|");
    if (key !== divergenceKey) {
      for (const line of divergenceLines) chart.removeSeries(line);
      divergenceLines = [];
      for (const div of analysis.divergences) {
        const from = bars[div.priorIndex];
        const to = bars[div.recentIndex];
        if (!from || !to) continue;
        const line = chart.addSeries(
          LineSeries,
          {
            color: div.direction === "Bullish" ? chartTheme.up : chartTheme.down,
            lineWidth: 2,
            lineStyle: div.kind === "Regular" ? LineStyle.Solid : LineStyle.Dashed,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
          },
          0,
        );
        line.setData([
          { time: stamp(from.time), value: from.close },
          { time: stamp(to.time), value: to.close },
        ]);
        divergenceLines.push(line);
      }
      divergenceKey = key;
    }
  });
</script>

<div class="flex h-full w-full">
  <div class="flex w-9 flex-shrink-0 flex-col gap-1 border-r border-border p-1">
    <button
      class="rounded py-1 text-[10px] font-medium {logScale
        ? 'bg-accent text-accent-foreground'
        : 'text-muted-foreground hover:bg-accent/50'}"
      title="Logarithmic price scale"
      onclick={() => (logScale = !logScale)}
    >
      log
    </button>
  </div>
  <div class="relative min-w-0 flex-1">
    <div bind:this={container} class="h-full w-full"></div>
    {#if legend}
      <div
        class="pointer-events-none absolute left-2 top-2 flex flex-wrap gap-x-3 gap-y-0.5 text-xs tabular-nums text-muted-foreground"
      >
        <span>O <span class="text-foreground">{fmtPrice(legend.open)}</span></span>
        <span>H <span class="text-foreground">{fmtPrice(legend.high)}</span></span>
        <span>L <span class="text-foreground">{fmtPrice(legend.low)}</span></span>
        <span>C <span class="text-foreground">{fmtPrice(legend.close)}</span></span>
        <span>V <span class="text-foreground">{fmtVol(legend.volume)}</span></span>
        <span class={legend.change > 0 ? "text-green-500" : legend.change < 0 ? "text-red-500" : ""}>
          {legend.change >= 0 ? "+" : ""}{legend.change.toFixed(2)}%
        </span>
        {#if legend.rsi !== null}<span>RSI <span style="color: {chartTheme.rsi}">{legend.rsi.toFixed(1)}</span></span>{/if}
        {#if legend.macd !== null}
          <span>MACD <span style="color: {chartTheme.macd}">{legend.macd.toFixed(4)}</span></span>
        {/if}
      </div>
    {/if}
  </div>
</div>
