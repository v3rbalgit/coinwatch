<script lang="ts">
  import { AlignJustify, GripVertical, Magnet, Minus, MoveUpRight, Square, Trash2, TrendingUp } from "@lucide/svelte"
  import {
    CandlestickSeries,
    ColorType,
    createChart,
    CrosshairMode,
    HistogramSeries,
    LineSeries,
    LineStyle,
    PriceScaleMode,
    type IChartApi,
    type ISeriesApi,
    type UTCTimestamp,
  } from "lightweight-charts"
  import { onMount } from "svelte"

  import { DrawingInteraction } from "./drawings/interaction"
  import { DrawingsPrimitive } from "./drawings/primitive"
  import { DrawingStore } from "./drawings/store"
  import { type Drawing, type DrawingStyle, type DrawingTool } from "./drawings/types"
  import {
    computeIndicators,
    indicatorDescriptors,
    loadIndicators,
    type IndicatorDescriptor,
    type IndicatorInstance,
    type InstanceOutput,
  } from "./indicators"
  import { chartTheme, fmtPrice } from "./theme"
  import type { Bar } from "./types"

  // `seed` changes when the whole series is replaced (coin / timeframe switch or a
  // history prepend), triggering a full setData + refit; otherwise bars flow in as
  // tail updates. Indicators are computed here from `bars` via the shared Rust
  // engine (WASM), so they match the scanner and update per tick over any depth.
  const {
    bars,
    seed = 0,
    instances = [
      { id: "rsi", params: {} },
      { id: "macd", params: {} },
      { id: "rsiDivergence", params: {} },
      { id: "macdDivergence", params: {} },
    ],
    onLoadOlder,
    onInstancesChange,
  }: {
    bars: Bar[]
    seed?: number
    instances?: IndicatorInstance[]
    onLoadOlder?: () => void
    onInstancesChange?: (instances: IndicatorInstance[]) => void
  } = $props()

  const VISIBLE_BARS = 150
  const RIGHT_OFFSET = 6
  const LOAD_OLDER_THRESHOLD = 10
  const BASE_PANES = 1 // price pane; indicator panes are appended after it

  let container: HTMLDivElement
  let chart: IChartApi | undefined
  let candles: ISeriesApi<"Candlestick"> | undefined
  let volume: ISeriesApi<"Histogram"> | undefined
  const descriptorsById = new Map<string, IndicatorDescriptor>()
  type RenderedSeries =
    | { outputKey: string; kind: "line"; api: ISeriesApi<"Line"> }
    | { outputKey: string; kind: "histogram"; api: ISeriesApi<"Histogram"> }
  type RenderedInstance = { series: RenderedSeries[]; spanLines: ISeriesApi<"Line">[]; spanKey: string }
  let rendered: RenderedInstance[] = []
  let latestComputed: InstanceOutput[] = []
  let latestChosen: IndicatorInstance[] = []
  let plottedRenderKey = ""
  let plottedRenderSeed = -1
  let plottedRenderFirstTime = 0
  let plottedSeed = -1
  let plottedFirstTime = 0
  let lastTime = 0
  let suppressLoadOlder = false
  let hovering = false
  let ready = $state(false)
  let wasmReady = $state(false)
  let pickerOpen = $state(false)
  let pickerPanel = $state<HTMLDivElement>()
  let fxButton = $state<HTMLButtonElement>()
  let availableDescriptors = $state<IndicatorDescriptor[]>([])
  let rootEl: HTMLDivElement
  let drawingsPrimitive: DrawingsPrimitive | undefined
  let drawingStore: DrawingStore | undefined
  let drawingInteraction: DrawingInteraction | undefined
  let activeTool = $state<DrawingTool | null>(null)
  let selectedDrawing = $state<Drawing | null>(null)
  let magnet = $state(false)
  let hasDrawings = $state(false)
  const drawingPalette = [chartTheme.drawing, chartTheme.signal, chartTheme.rsi, chartTheme.up, chartTheme.down]
  let styleBarPos = $state({ x: 16, y: 44 })

  type LegendData = { open: number; high: number; low: number; close: number; volume: number; change: number }
  type Readout = { label: string; value: number; color: string }
  let legend = $state<LegendData | null>(null)
  let readouts = $state<Readout[]>([])
  let logScale = $state(false)

  const fmtVol = (value: number) =>
    value >= 1e9
      ? (value / 1e9).toFixed(2) + "B"
      : value >= 1e6
        ? (value / 1e6).toFixed(1) + "M"
        : value >= 1e3
          ? (value / 1e3).toFixed(0) + "k"
          : value.toFixed(0)
  const fmtReadout = (value: number) =>
    Math.abs(value) >= 100 ? value.toFixed(0) : Math.abs(value) >= 1 ? value.toFixed(2) : value.toFixed(4)

  function legendFor(bar: Bar): LegendData {
    return {
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.volume,
      change: bar.open !== 0 ? (bar.close / bar.open - 1) * 100 : 0,
    }
  }

  // Toggle an indicator in/out of the visible set (parent owns the state + persistence).
  function toggleIndicator(id: string) {
    const exists = instances.some(instance => instance.id === id)
    onInstancesChange?.(exists ? instances.filter(instance => instance.id !== id) : [...instances, { id, params: {} }])
  }

  // Arm a drawing tool (or toggle it off); the interaction owns placement.
  function selectTool(tool: DrawingTool) {
    activeTool = activeTool === tool ? null : tool
  }
  const toolClass = (tool: DrawingTool) =>
    `flex justify-center rounded py-1.5 ${
      activeTool === tool ? "bg-accent text-accent-foreground" : "text-muted-foreground hover:bg-accent/50"
    }`

  function changeStyle(patch: Partial<DrawingStyle>) {
    if (!selectedDrawing || !drawingStore) return
    drawingStore.update(selectedDrawing.id, { style: { ...selectedDrawing.style, ...patch } })
    selectedDrawing = drawingStore.byId(selectedDrawing.id) ?? null
  }
  function deleteSelected() {
    if (!selectedDrawing || !drawingStore) return
    drawingStore.remove(selectedDrawing.id)
    drawingInteraction?.deselect()
  }

  // Drag the style bar by its handle; positions are relative to the chart area.
  function onStyleBarDragStart(event: PointerEvent) {
    event.preventDefault()
    const rect = container.getBoundingClientRect()
    const offsetX = event.clientX - rect.left - styleBarPos.x
    const offsetY = event.clientY - rect.top - styleBarPos.y
    const move = (moveEvent: PointerEvent) => {
      styleBarPos = {
        x: Math.max(0, Math.min(moveEvent.clientX - rect.left - offsetX, rect.width - 40)),
        y: Math.max(0, Math.min(moveEvent.clientY - rect.top - offsetY, rect.height - 30)),
      }
    }
    const up = () => {
      window.removeEventListener("pointermove", move)
      window.removeEventListener("pointerup", up)
    }
    window.addEventListener("pointermove", move)
    window.addEventListener("pointerup", up)
  }

  // Each visible pane indicator's primary value at a bar, for the crosshair readout.
  function readoutsFor(index: number): Readout[] {
    const result: Readout[] = []
    latestChosen.forEach((instance, position) => {
      const descriptor = descriptorsById.get(instance.id)
      const primary = descriptor?.outputs[0]
      const output = latestComputed[position]
      if (descriptor?.target !== "pane" || !primary || !output) return
      const value = output.outputs.find(series => series.key === primary.key)?.values[index]
      if (value !== undefined && !Number.isNaN(value)) {
        result.push({ label: descriptor.label, value, color: primary.color })
      }
    })
    return result
  }

  // lightweight-charts' UTCTimestamp is a branded number; the cast is the library's
  // required way to build one from epoch seconds.
  const stamp = (time: number) => time as UTCTimestamp

  const toCandle = (bar: Bar) => ({
    time: stamp(bar.time),
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  })
  const toVolume = (bar: Bar) => ({
    time: stamp(bar.time),
    value: bar.volume,
    color: bar.close >= bar.open ? chartTheme.volumeUp : chartTheme.volumeDown,
  })

  // Align a WASM series (NaN where undefined) to each bar's time, dropping gaps.
  function toLine(series: number[]) {
    const points: { time: UTCTimestamp; value: number }[] = []
    bars.forEach((bar, index) => {
      const value = series[index]
      if (value !== undefined && !Number.isNaN(value)) points.push({ time: stamp(bar.time), value })
    })
    return points
  }
  function toHistogram(series: number[]) {
    const points: { time: UTCTimestamp; value: number; color: string }[] = []
    bars.forEach((bar, index) => {
      const value = series[index]
      if (value !== undefined && !Number.isNaN(value)) {
        points.push({ time: stamp(bar.time), value, color: value >= 0 ? chartTheme.histUp : chartTheme.histDown })
      }
    })
    return points
  }
  function updateLast(series: ISeriesApi<"Line">, values: number[]) {
    for (let index = values.length - 1; index >= 0; index--) {
      const value = values[index]
      const bar = bars[index]
      if (bar && value !== undefined && !Number.isNaN(value)) {
        series.update({ time: stamp(bar.time), value })
        return
      }
    }
  }
  function updateLastHistogram(series: ISeriesApi<"Histogram">, values: number[]) {
    for (let index = values.length - 1; index >= 0; index--) {
      const value = values[index]
      const bar = bars[index]
      if (bar && value !== undefined && !Number.isNaN(value)) {
        series.update({ time: stamp(bar.time), value, color: value >= 0 ? chartTheme.histUp : chartTheme.histDown })
        return
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
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: chartTheme.border,
        rightOffset: RIGHT_OFFSET,
      },
      rightPriceScale: { borderColor: chartTheme.border },
    })

    candles = chart.addSeries(CandlestickSeries, {
      upColor: chartTheme.up,
      downColor: chartTheme.down,
      borderUpColor: chartTheme.up,
      borderDownColor: chartTheme.down,
      wickUpColor: chartTheme.up,
      wickDownColor: chartTheme.down,
    })
    volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
      lastValueVisible: false,
      priceLineVisible: false,
    })
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })

    drawingsPrimitive = new DrawingsPrimitive()
    candles.attachPrimitive(drawingsPrimitive)
    drawingStore = new DrawingStore()
    // The full interaction engine: click-click or drag-to-draw with live preview,
    // hit-tested selection, body/handle drags, Escape/Delete, magnet, tool hotkeys.
    drawingInteraction = new DrawingInteraction(
      container,
      chart,
      candles,
      drawingStore,
      drawingsPrimitive,
      {
        onToolDone: () => (activeTool = null),
        onRequestText: () => {},
        onSelectionChange: id => (selectedDrawing = id ? (drawingStore?.byId(id) ?? null) : null),
        onToolHotkey: tool => (activeTool = activeTool === tool ? null : tool),
        onToggleMagnet: () => (magnet = !magnet),
      },
      rootEl
    )
    // Interaction set its own onChange (redraw); ours supersedes it, keeping the
    // redraw and also tracking emptiness for the clear button.
    drawingStore.onChange = () => {
      drawingsPrimitive?.setDrawings(drawingStore?.all() ?? [])
      hasDrawings = (drawingStore?.all().length ?? 0) > 0
    }

    chart.subscribeCrosshairMove(param => {
      const index = param.time === undefined ? -1 : bars.findIndex(bar => bar.time === param.time)
      const bar = index >= 0 ? bars[index] : undefined
      if (bar) {
        hovering = true
        legend = legendFor(bar)
        readouts = readoutsFor(index)
      } else {
        hovering = false
        const last = bars.at(-1)
        legend = last ? legendFor(last) : null
        readouts = readoutsFor(bars.length - 1)
      }
    })

    chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
      if (suppressLoadOlder) {
        suppressLoadOlder = false
        return
      }
      if (range && range.from < LOAD_OLDER_THRESHOLD) onLoadOlder?.()
    })

    void loadIndicators().then(() => {
      const list = indicatorDescriptors()
      for (const descriptor of list) descriptorsById.set(descriptor.id, descriptor)
      availableDescriptors = list
      wasmReady = true
    })

    ready = true
    return () => {
      drawingInteraction?.destroy()
      chart?.remove()
      chart = undefined
    }
  })

  // Candles + volume: full reseed on seed change, else tail updates.
  $effect(() => {
    if (!ready || !candles || !volume || bars.length === 0) return
    const firstTime = bars[0].time
    if (seed !== plottedSeed) {
      candles.setData(bars.map(toCandle))
      volume.setData(bars.map(toVolume))
      suppressLoadOlder = true
      chart?.timeScale().setVisibleLogicalRange({
        from: Math.max(0, bars.length - VISIBLE_BARS),
        to: bars.length + RIGHT_OFFSET,
      })
      plottedSeed = seed
    } else if (firstTime < plottedFirstTime) {
      // Older history paged in: replot without a refit, then shift the range so the
      // same bars stay in view.
      const prepended = bars.findIndex(bar => bar.time >= plottedFirstTime)
      const range = chart?.timeScale().getVisibleLogicalRange()
      candles.setData(bars.map(toCandle))
      volume.setData(bars.map(toVolume))
      if (range && prepended > 0) {
        suppressLoadOlder = true
        chart?.timeScale().setVisibleLogicalRange({ from: range.from + prepended, to: range.to + prepended })
      }
    } else {
      for (const bar of bars.filter(candle => candle.time >= lastTime)) {
        candles.update(toCandle(bar))
        volume.update(toVolume(bar))
      }
    }
    plottedFirstTime = firstTime
    lastTime = bars[bars.length - 1].time
    if (!hovering) {
      const last = bars[bars.length - 1]
      legend = legendFor(last)
      readouts = readoutsFor(bars.length - 1)
    }
  })

  // Toggle the price pane between linear and logarithmic.
  $effect(() => {
    chart?.priceScale("right").applyOptions({ mode: logScale ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal })
  })

  // Drive the interaction's armed tool + magnet from the toolbar state.
  $effect(() => {
    drawingInteraction?.setTool(activeTool)
  })
  $effect(() => {
    drawingInteraction?.setMagnet(magnet)
  })

  // Close the indicator picker on a click outside it (or the ƒx button).
  $effect(() => {
    if (!pickerOpen) return
    const onDown = (event: PointerEvent) => {
      const target = event.target
      if (target instanceof Node && !pickerPanel?.contains(target) && !fxButton?.contains(target)) {
        pickerOpen = false
      }
    }
    window.addEventListener("pointerdown", onDown)
    return () => window.removeEventListener("pointerdown", onDown)
  })

  // Instance-driven indicators: overlays (SMA/EMA/Bollinger/…) as lines on the price
  // pane, oscillators (Stochastic/Williams/CCI/MFI/ADX/ATR/OBV) in their own panes
  // after the pinned RSI/MACD. Rebuild series on instance-set change; otherwise fill
  // from the bars (reseed on coin/tf switch or history prepend, tail per tick).
  const renderKey = (instance: IndicatorInstance) => `${instance.id}:${JSON.stringify(instance.params)}`

  $effect(() => {
    if (!ready || !wasmReady || !chart || bars.length === 0) return
    const activeChart = chart
    const chosen = instances.filter(instance => descriptorsById.has(instance.id))

    const key = chosen.map(renderKey).join("|")
    if (key !== plottedRenderKey) {
      for (const item of rendered) {
        for (const line of item.series) activeChart.removeSeries(line.api)
        for (const spanLine of item.spanLines) activeChart.removeSeries(spanLine)
      }
      rendered = []
      while (activeChart.panes().length > BASE_PANES) activeChart.removePane(activeChart.panes().length - 1)
      let nextPane = BASE_PANES
      for (const instance of chosen) {
        const descriptor = descriptorsById.get(instance.id)
        if (!descriptor) continue
        const paneIndex = descriptor.target === "overlay" ? 0 : nextPane++
        const range = descriptor.range
        const scale = range
          ? { autoscaleInfoProvider: () => ({ priceRange: { minValue: range[0], maxValue: range[1] } }) }
          : {}
        const series = descriptor.outputs.map((output): RenderedSeries => {
          if (output.kind === "histogram") {
            return {
              outputKey: output.key,
              kind: "histogram",
              api: activeChart.addSeries(
                HistogramSeries,
                { priceLineVisible: false, lastValueVisible: false, ...scale },
                paneIndex
              ),
            }
          }
          return {
            outputKey: output.key,
            kind: "line",
            api: activeChart.addSeries(
              LineSeries,
              {
                color: output.color,
                lineWidth: 1,
                priceLineVisible: false,
                lastValueVisible: descriptor.target === "pane",
                ...scale,
              },
              paneIndex
            ),
          }
        })
        if (descriptor.target === "pane") {
          // Keep oscillator panes tight to their range: near-flush for fixed-range
          // indicators (RSI/Stoch 0–100), a small pad for auto-range ones.
          series[0]?.api.priceScale().applyOptions({
            scaleMargins: range ? { top: 0.05, bottom: 0.05 } : { top: 0.12, bottom: 0.1 },
          })
        }
        for (const level of descriptor.guides) {
          series[0]?.api.createPriceLine({
            price: level,
            color: chartTheme.guide,
            lineWidth: 1,
            lineStyle: LineStyle.Dashed,
            axisLabelVisible: false,
            title: "",
          })
        }
        rendered.push({ series, spanLines: [], spanKey: "" })
      }
      activeChart.panes().forEach((pane, index) => pane.setStretchFactor(index === 0 ? 3 : 1))
      plottedRenderKey = key
      plottedRenderSeed = -1
    }
    if (rendered.length === 0) return

    const high = Float64Array.from(bars, bar => bar.high)
    const low = Float64Array.from(bars, bar => bar.low)
    const close = Float64Array.from(bars, bar => bar.close)
    const volume = Float64Array.from(bars, bar => bar.volume)
    const computed = computeIndicators(chosen, high, low, close, volume)
    latestComputed = computed
    latestChosen = chosen
    if (!hovering) readouts = readoutsFor(bars.length - 1)
    const firstTime = bars[0].time
    const reseed = seed !== plottedRenderSeed || firstTime < plottedRenderFirstTime
    rendered.forEach((item, index) => {
      const output = computed[index]
      if (!output) return
      for (const line of item.series) {
        const values = output.outputs.find(series => series.key === line.outputKey)?.values
        if (!values) continue
        if (line.kind === "histogram") {
          if (reseed) line.api.setData(toHistogram(values))
          else updateLastHistogram(line.api, values)
        } else {
          if (reseed) line.api.setData(toLine(values))
          else updateLast(line.api, values)
        }
      }
      const spanKey = output.divergences
        .map(span => `${span.direction}:${span.kind}:${span.fromIndex}:${span.toIndex}`)
        .join("|")
      if (spanKey !== item.spanKey) {
        for (const spanLine of item.spanLines) activeChart.removeSeries(spanLine)
        item.spanLines = output.divergences.flatMap(span => {
          const from = bars[span.fromIndex]
          const to = bars[span.toIndex]
          if (!from || !to) return []
          const spanLine = activeChart.addSeries(
            LineSeries,
            {
              color: span.direction === "Bullish" ? chartTheme.up : chartTheme.down,
              lineWidth: 2,
              lineStyle: span.kind === "Regular" ? LineStyle.Solid : LineStyle.Dashed,
              priceLineVisible: false,
              lastValueVisible: false,
              crosshairMarkerVisible: false,
            },
            0
          )
          spanLine.setData([
            { time: stamp(from.time), value: from.close },
            { time: stamp(to.time), value: to.close },
          ])
          return [spanLine]
        })
        item.spanKey = spanKey
      }
    })
    plottedRenderSeed = seed
    plottedRenderFirstTime = firstTime
  })
</script>

<div class="flex h-full w-full" bind:this={rootEl}>
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
    <button
      bind:this={fxButton}
      class="rounded py-1 text-[10px] font-medium {pickerOpen
        ? 'bg-accent text-accent-foreground'
        : 'text-muted-foreground hover:bg-accent/50'}"
      title="Indicators"
      onclick={() => (pickerOpen = !pickerOpen)}
    >
      ƒx
    </button>
    <div class="my-0.5 h-px w-full bg-border"></div>
    <button class={toolClass("trendline")} title="Trend line (t)" onclick={() => selectTool("trendline")}>
      <TrendingUp class="size-3.5" />
    </button>
    <button class={toolClass("ray")} title="Ray (y)" onclick={() => selectTool("ray")}>
      <MoveUpRight class="size-3.5" />
    </button>
    <button class={toolClass("hline")} title="Horizontal line (h)" onclick={() => selectTool("hline")}>
      <Minus class="size-3.5" />
    </button>
    <button class={toolClass("vline")} title="Vertical line (v)" onclick={() => selectTool("vline")}>
      <Minus class="size-3.5 rotate-90" />
    </button>
    <button class={toolClass("rect")} title="Rectangle (r)" onclick={() => selectTool("rect")}>
      <Square class="size-3.5" />
    </button>
    <button class={toolClass("fib")} title="Fibonacci retracement (f)" onclick={() => selectTool("fib")}>
      <AlignJustify class="size-3.5" />
    </button>
    <div class="my-0.5 h-px w-full bg-border"></div>
    <button
      class="flex justify-center rounded py-1.5 {magnet
        ? 'bg-accent text-accent-foreground'
        : 'text-muted-foreground hover:bg-accent/50'}"
      title="Magnet — snap to OHLC (m)"
      onclick={() => (magnet = !magnet)}
    >
      <Magnet class="size-3.5" />
    </button>
    <button
      class="flex justify-center rounded py-1.5 text-muted-foreground hover:bg-accent/50 disabled:opacity-30"
      title="Clear drawings"
      disabled={!hasDrawings}
      onclick={() => drawingStore?.clearAll()}
    >
      <Trash2 class="size-3.5" />
    </button>
  </div>
  <div class="relative min-w-0 flex-1">
    <div bind:this={container} class="h-full w-full {activeTool ? 'cursor-crosshair' : ''}"></div>
    {#if legend}
      <div
        class="pointer-events-none absolute top-2 left-2 z-10 flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-foreground tabular-nums"
      >
        <span>O <span class="text-foreground">{fmtPrice(legend.open)}</span></span>
        <span>H <span class="text-foreground">{fmtPrice(legend.high)}</span></span>
        <span>L <span class="text-foreground">{fmtPrice(legend.low)}</span></span>
        <span>C <span class="text-foreground">{fmtPrice(legend.close)}</span></span>
        <span>V <span class="text-foreground">{fmtVol(legend.volume)}</span></span>
        <span class={legend.change > 0 ? "text-green-500" : legend.change < 0 ? "text-red-500" : ""}>
          {legend.change >= 0 ? "+" : ""}{legend.change.toFixed(2)}%
        </span>
        {#each readouts as readout (readout.label)}
          <span>{readout.label} <span style="color: {readout.color}">{fmtReadout(readout.value)}</span></span>
        {/each}
      </div>
    {/if}
    {#if pickerOpen}
      <div
        bind:this={pickerPanel}
        class="absolute top-2 left-2 z-20 flex max-h-[85%] w-44 flex-col overflow-y-auto rounded-md border border-border bg-background/95 p-1 text-xs shadow-lg backdrop-blur"
      >
        {#each availableDescriptors as descriptor (descriptor.id)}
          <label class="flex cursor-pointer items-center gap-2 rounded px-1.5 py-1 hover:bg-accent/50">
            <input
              type="checkbox"
              checked={instances.some(instance => instance.id === descriptor.id)}
              onchange={() => toggleIndicator(descriptor.id)}
            />
            <span class="text-foreground">{descriptor.label}</span>
          </label>
        {/each}
      </div>
    {/if}
    {#if selectedDrawing}
      <div
        class="absolute z-20 flex items-center gap-1 rounded-md border border-border bg-background/95 p-1 shadow-lg backdrop-blur"
        style="left: {styleBarPos.x}px; top: {styleBarPos.y}px"
      >
        <button
          class="flex h-6 w-4 cursor-grab items-center justify-center text-muted-foreground active:cursor-grabbing"
          aria-label="Move toolbar"
          onpointerdown={onStyleBarDragStart}
        >
          <GripVertical class="size-3.5" />
        </button>
        <div class="mx-0.5 h-5 w-px bg-border"></div>
        {#each drawingPalette as color (color)}
          <button
            class="size-4 rounded-full border {(selectedDrawing?.style?.color ?? chartTheme.drawing) === color
              ? 'border-foreground'
              : 'border-transparent'}"
            style="background-color: {color}"
            aria-label="Color"
            onclick={() => changeStyle({ color })}
          ></button>
        {/each}
        <div class="mx-0.5 h-5 w-px bg-border"></div>
        {#each [1, 2, 3] as width (width)}
          <button
            class="flex h-6 w-6 items-center justify-center rounded {(selectedDrawing?.style?.width ?? 1) === width
              ? 'bg-accent'
              : 'hover:bg-accent/50'}"
            aria-label="Line width {width}"
            onclick={() => changeStyle({ width })}
          >
            <span class="w-4 rounded bg-foreground" style="height: {width}px"></span>
          </button>
        {/each}
        <button
          class="flex h-6 w-6 items-center justify-center rounded {selectedDrawing?.style?.dashed
            ? 'bg-accent'
            : 'hover:bg-accent/50'}"
          aria-label="Dashed line"
          onclick={() => changeStyle({ dashed: !selectedDrawing?.style?.dashed })}
        >
          <span class="w-4 border-t-2 border-dashed border-foreground"></span>
        </button>
        <div class="mx-0.5 h-5 w-px bg-border"></div>
        <button
          class="flex h-6 w-6 items-center justify-center rounded text-muted-foreground hover:text-red-500"
          aria-label="Delete drawing"
          onclick={deleteSelected}
        >
          <Trash2 class="size-3.5" />
        </button>
      </div>
    {/if}
  </div>
</div>
