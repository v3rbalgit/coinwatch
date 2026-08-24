<script lang="ts" module>
  export type Bar = { time: number; open: number; high: number; low: number; close: number; volume: number };
</script>

<script lang="ts">
  import {
    CandlestickSeries,
    ColorType,
    CrosshairMode,
    HistogramSeries,
    createChart,
    type IChartApi,
    type ISeriesApi,
    type UTCTimestamp,
  } from "lightweight-charts";
  import { onMount } from "svelte";

  import { chartTheme, fmtPrice } from "./theme";

  // `seed` changes when the whole series is replaced (coin / timeframe switch),
  // triggering a full setData + refit; otherwise bars flow in as tail updates.
  let { bars, seed = 0 }: { bars: Bar[]; seed?: number } = $props();

  const VISIBLE_BARS = 150;
  const RIGHT_OFFSET = 6;

  let container: HTMLDivElement;
  let chart: IChartApi | undefined;
  let candles: ISeriesApi<"Candlestick"> | undefined;
  let volume: ISeriesApi<"Histogram"> | undefined;
  let plottedSeed = -1;
  let lastTime = 0;
  let hovering = false;

  type Ohlc = { open: number; high: number; low: number; close: number };
  let legend = $state<Ohlc | null>(null);

  function toOhlc(data: unknown): Ohlc | null {
    if (
      data &&
      typeof data === "object" &&
      "open" in data &&
      "high" in data &&
      "low" in data &&
      "close" in data &&
      typeof data.open === "number" &&
      typeof data.high === "number" &&
      typeof data.low === "number" &&
      typeof data.close === "number"
    ) {
      return { open: data.open, high: data.high, low: data.low, close: data.close };
    }
    return null;
  }

  const toCandle = (bar: Bar) => ({
    time: bar.time as UTCTimestamp,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  });
  const toVolume = (bar: Bar) => ({
    time: bar.time as UTCTimestamp,
    value: bar.volume,
    color: bar.close >= bar.open ? chartTheme.volumeUp : chartTheme.volumeDown,
  });

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

    chart.subscribeCrosshairMove((param) => {
      const hovered = candles ? toOhlc(param.seriesData.get(candles)) : null;
      if (hovered) {
        hovering = true;
        legend = hovered;
      } else {
        hovering = false;
        const last = bars.at(-1);
        legend = last ? { open: last.open, high: last.high, low: last.low, close: last.close } : null;
      }
    });

    return () => {
      chart?.remove();
      chart = undefined;
    };
  });

  $effect(() => {
    if (!candles || !volume || bars.length === 0) return;
    if (seed !== plottedSeed) {
      candles.setData(bars.map(toCandle));
      volume.setData(bars.map(toVolume));
      chart?.timeScale().setVisibleLogicalRange({
        from: Math.max(0, bars.length - VISIBLE_BARS),
        to: bars.length + RIGHT_OFFSET,
      });
      plottedSeed = seed;
    } else {
      for (const bar of bars.filter((candle) => candle.time >= lastTime)) {
        candles.update(toCandle(bar));
        volume.update(toVolume(bar));
      }
    }
    lastTime = bars[bars.length - 1].time;
    if (!hovering) {
      const last = bars[bars.length - 1];
      legend = { open: last.open, high: last.high, low: last.low, close: last.close };
    }
  });
</script>

<div class="relative h-full w-full">
  <div bind:this={container} class="h-full w-full"></div>
  {#if legend}
    <div class="pointer-events-none absolute left-2 top-2 flex gap-3 text-xs tabular-nums text-muted-foreground">
      <span>O <span class="text-foreground">{fmtPrice(legend.open)}</span></span>
      <span>H <span class="text-foreground">{fmtPrice(legend.high)}</span></span>
      <span>L <span class="text-foreground">{fmtPrice(legend.low)}</span></span>
      <span>C <span class="text-foreground">{fmtPrice(legend.close)}</span></span>
    </div>
  {/if}
</div>
