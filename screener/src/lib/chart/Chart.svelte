<script lang="ts">
  import {
    Brush,
    Check,
    Equal,
    GripVertical,
    Magnet,
    Maximize2,
    Minus,
    MoveUpRight,
    RotateCcw,
    Rows3,
    Rows4,
    Ruler,
    Square,
    Trash2,
    TrendingUp,
  } from "@lucide/svelte"
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
  import { onMount, untrack } from "svelte"

  import { errorMessage } from "$lib/errors"
  import { formatCompactNumber, formatPrice } from "$lib/format"

  import { logicalFor, timeGridOf } from "./drawings/coords"
  import { DrawingInteraction } from "./drawings/interaction"
  import { DrawingsPrimitive } from "./drawings/primitive"
  import { DrawingStore } from "./drawings/store"
  import { type Drawing, type DrawingStyle, type InteractiveDrawingTool } from "./drawings/types"
  import {
    computeIndicators,
    indicatorDescriptors,
    loadIndicators,
    type IndicatorDescriptor,
    type IndicatorInstance,
    type InstanceOutput,
  } from "./indicators"
  import { priceFormat } from "./price-format"
  import { PRICE_PANE_LAYOUT_ID, type PaneLayoutEntry, type ToolbarPosition } from "./settings"
  import { SignalValidationPrimitive } from "./signal-validation-primitive"
  import { chartTheme } from "./theme"
  import type { Bar, SignalValidationMarking } from "./types"

  // `seed` changes when the whole series is replaced (coin / timeframe switch or a
  // history prepend), triggering a full setData + refit; otherwise bars flow in as
  // tail updates. Indicators are computed here from `bars` via the shared Rust
  // engine (WASM), so they match signal evaluation and update per tick over any depth.
  const {
    bars,
    seed = 0,
    priceDecimals,
    instances = [
      { id: "rsi", params: {} },
      { id: "macd", params: {} },
      { id: "rsiDivergence", params: {} },
      { id: "macdDivergence", params: {} },
    ],
    drawings = [],
    logScale,
    magnet,
    styleBarPosition,
    paneLayout,
    validationSpans = [],
    selectedValidationSpan = null,
    focusTime = null,
    focusRevision = 0,
    onLoadOlder,
    onValidationSpanSelect,
    onInstancesChange,
    onDrawingsChange,
    onLogScaleChange,
    onMagnetChange,
    onStyleBarPositionChange,
    onPaneLayoutChange,
  }: {
    bars: Bar[]
    seed?: number
    priceDecimals?: number
    instances?: IndicatorInstance[]
    drawings?: Drawing[]
    logScale: boolean
    magnet: boolean
    styleBarPosition: ToolbarPosition
    paneLayout: PaneLayoutEntry[]
    validationSpans?: SignalValidationMarking[]
    selectedValidationSpan?: number | null
    focusTime?: number | null
    focusRevision?: number
    onLoadOlder?: () => void
    onValidationSpanSelect?: (index: number) => void
    onInstancesChange?: (instances: IndicatorInstance[]) => void
    onDrawingsChange?: (drawings: readonly Drawing[]) => void
    onLogScaleChange: (enabled: boolean) => void
    onMagnetChange: (enabled: boolean) => void
    onStyleBarPositionChange: (position: ToolbarPosition) => void
    onPaneLayoutChange: (layout: PaneLayoutEntry[]) => void
  } = $props()

  const VISIBLE_BARS = 150
  const RIGHT_OFFSET = 6
  const LOAD_OLDER_THRESHOLD = 10
  const BASE_PANES = 1 // price pane; indicator panes are appended after it
  const PRICE_PANE_STRETCH = 3
  const INDICATOR_PANE_STRETCH = 1
  const PANE_STRETCH_EPSILON = 1e-6
  const FOCUS_HALF_WINDOW = 60

  const candlePriceFormat = $derived(priceFormat(priceDecimals))

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
  let latestComputed: (InstanceOutput | undefined)[] = []
  let latestChosen: IndicatorInstance[] = []
  let plottedRenderKey = ""
  let plottedRenderSeed = -1
  let plottedRenderFirstTime = 0
  let plottedSeed = -1
  let plottedFirstTime = 0
  let lastTime = 0
  let suppressLoadOlder = false
  // Range events fire on the library's draw frame (rAF); suppress until the frame
  // after, then expire, so a no-op set cannot leave the flag stuck armed.
  function suppressNextRangeEvent() {
    suppressLoadOlder = true
    requestAnimationFrame(() => requestAnimationFrame(() => (suppressLoadOlder = false)))
  }
  let hovering = false
  let ready = $state(false)
  let wasmReady = $state(false)
  let indicatorError = $state("")
  let pickerOpen = $state(false)
  let pickerPanel = $state<HTMLDivElement>()
  let fxButton = $state<HTMLButtonElement>()
  let availableDescriptors = $state<IndicatorDescriptor[]>([])
  let rootEl: HTMLDivElement
  let drawingStore: DrawingStore | undefined
  let drawingInteraction: DrawingInteraction | undefined
  let validationPrimitive: SignalValidationPrimitive | undefined
  let activeTool = $state<InteractiveDrawingTool | null>(null)
  let selectedDrawing = $state<Drawing | null>(null)
  let hasDrawings = $state(false)
  const drawingPalette = [chartTheme.drawing, chartTheme.signal, chartTheme.rsi, chartTheme.up, chartTheme.down]
  let styleBarPos = $state<ToolbarPosition>({ x: 0, y: 0 })
  let styleBarEl = $state<HTMLDivElement>()
  let contextMenu = $state<ToolbarPosition | null>(null)
  let contextMenuEl = $state<HTMLDivElement>()
  let renderedPaneIds = $state<string[]>([PRICE_PANE_LAYOUT_ID])

  type LegendData = { open: number; high: number; low: number; close: number; volume: number; change: number }
  type Readout = { label: string; value: number; color: string }
  let legend = $state<LegendData | null>(null)
  let readouts = $state<Readout[]>([])

  const CONTEXT_MENU_MARGIN = 4
  const contextMenuItem =
    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-xs text-foreground hover:bg-accent/70 disabled:opacity-40"

  const fmtReadout = (value: number) =>
    Math.abs(value) >= 100 ? value.toFixed(0) : Math.abs(value) >= 1 ? value.toFixed(2) : value.toFixed(4)

  function loadWasm() {
    indicatorError = ""
    loadIndicators()
      .then(() => {
        const list = indicatorDescriptors()
        for (const descriptor of list) descriptorsById.set(descriptor.id, descriptor)
        availableDescriptors = list
        wasmReady = true
      })
      .catch((error: unknown) => {
        indicatorError = errorMessage(error)
      })
  }

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
  function selectTool(tool: InteractiveDrawingTool) {
    activeTool = activeTool === tool ? null : tool
  }
  const toolClass = (tool: InteractiveDrawingTool) =>
    `flex justify-center rounded py-1.5 ${
      activeTool === tool ? "bg-accent text-accent-foreground" : "text-muted-foreground hover:bg-accent/50"
    }`

  function isDrawingColorSelected(color: string): boolean {
    if (!selectedDrawing) return false
    const fallback = selectedDrawing.tool === "priceRange" ? undefined : chartTheme.drawing
    return (selectedDrawing.style?.color ?? fallback) === color
  }

  function changeStyle(patch: Partial<DrawingStyle>) {
    if (!selectedDrawing || !drawingStore) return
    drawingStore.update(selectedDrawing.id, { style: { ...selectedDrawing.style, ...patch } })
    selectedDrawing = drawingStore.byId(selectedDrawing.id) ?? null
  }
  function changeColor(color: string | undefined) {
    if (!selectedDrawing || !drawingStore) return
    const current = selectedDrawing.style
    const style: DrawingStyle = {
      ...(color !== undefined ? { color } : {}),
      ...(current?.width !== undefined ? { width: current.width } : {}),
      ...(current?.dashed !== undefined ? { dashed: current.dashed } : {}),
    }
    drawingStore.update(selectedDrawing.id, { style })
    selectedDrawing = drawingStore.byId(selectedDrawing.id) ?? null
  }

  function deleteSelected() {
    if (!selectedDrawing || !drawingStore) return
    drawingStore.remove(selectedDrawing.id)
    drawingInteraction?.deselect()
  }

  function clearDrawings() {
    drawingStore?.clearAll()
    drawingInteraction?.deselect()
  }

  function paneLayoutsEqual(left: readonly PaneLayoutEntry[], right: readonly PaneLayoutEntry[]): boolean {
    return (
      left.length === right.length &&
      left.every(
        (entry, index) =>
          entry.id === right[index]?.id &&
          Math.abs(entry.stretchFactor - (right[index]?.stretchFactor ?? 0)) < PANE_STRETCH_EPSILON
      )
    )
  }

  function applyPaneLayout(activeChart: IChartApi, paneIds: readonly string[], layout: readonly PaneLayoutEntry[]) {
    activeChart.panes().forEach((pane, index) => {
      const id = paneIds[index]
      if (id === undefined) return
      const saved = layout.find(entry => entry.id === id)
      pane.setStretchFactor(
        saved?.stretchFactor ?? (id === PRICE_PANE_LAYOUT_ID ? PRICE_PANE_STRETCH : INDICATOR_PANE_STRETCH)
      )
    })
  }

  function capturePaneLayout() {
    if (!chart) return
    const panes = chart.panes()
    if (panes.length !== renderedPaneIds.length) return
    const visible: PaneLayoutEntry[] = []
    for (const [index, pane] of panes.entries()) {
      const id = renderedPaneIds[index]
      const stretchFactor = pane.getStretchFactor()
      if (id === undefined || !Number.isFinite(stretchFactor) || stretchFactor <= 0) return
      visible.push({ id, stretchFactor })
    }
    const visibleById = new Map(visible.map(entry => [entry.id, entry]))
    const next = paneLayout.map(entry => visibleById.get(entry.id) ?? { ...entry })
    const savedIds = new Set(paneLayout.map(entry => entry.id))
    for (const entry of visible) {
      if (!savedIds.has(entry.id)) next.push(entry)
    }
    if (!paneLayoutsEqual(next, paneLayout)) onPaneLayoutChange(next)
  }

  function clampToChart(position: ToolbarPosition, width: number, height: number, margin = 0): ToolbarPosition {
    const rect = container.getBoundingClientRect()
    return {
      x: Math.max(margin, Math.min(position.x, Math.max(margin, rect.width - width - margin))),
      y: Math.max(margin, Math.min(position.y, Math.max(margin, rect.height - height - margin))),
    }
  }

  function toggleLogScale() {
    onLogScaleChange(!logScale)
  }

  function toggleMagnet() {
    onMagnetChange(!magnet)
  }

  function resetChartView() {
    if (!chart || bars.length === 0) return
    suppressNextRangeEvent()
    chart.timeScale().setVisibleLogicalRange({
      from: Math.max(0, bars.length - VISIBLE_BARS),
      to: bars.length + RIGHT_OFFSET,
    })
    chart.priceScale("right").applyOptions({ autoScale: true })
  }

  function fitAllData() {
    if (!chart) return
    suppressNextRangeEvent()
    chart.timeScale().fitContent()
    chart.priceScale("right").applyOptions({ autoScale: true })
  }

  function closeChrome(): boolean {
    if (contextMenu) {
      contextMenu = null
      container.focus()
      return true
    }
    if (pickerOpen) {
      pickerOpen = false
      fxButton?.focus()
      return true
    }
    return false
  }

  function runContextAction(action: () => void) {
    contextMenu = null
    action()
    container.focus()
  }

  function onContextMenuKeyDown(event: KeyboardEvent) {
    if (event.key === "Tab") {
      contextMenu = null
      return
    }
    if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key) || !contextMenuEl) return
    const items = Array.from(contextMenuEl.querySelectorAll<HTMLButtonElement>('[role="menuitem"]'))
    if (items.length === 0) return
    const current = items.findIndex(item => item === document.activeElement)
    let next: number
    if (event.key === "ArrowDown") next = current < 0 ? 0 : (current + 1) % items.length
    else if (event.key === "ArrowUp")
      next = current < 0 ? items.length - 1 : (current - 1 + items.length) % items.length
    else if (event.key === "Home") next = 0
    else next = items.length - 1
    event.preventDefault()
    items[next]?.focus()
  }

  function openContextMenu(event: MouseEvent) {
    event.preventDefault()
    pickerOpen = false
    const rect = container.getBoundingClientRect()
    contextMenu = { x: event.clientX - rect.left, y: event.clientY - rect.top }
  }

  // Drag the style bar by its handle; positions are relative to the chart area.
  function onStyleBarDragStart(event: PointerEvent) {
    event.preventDefault()
    const rect = container.getBoundingClientRect()
    const offsetX = event.clientX - rect.left - styleBarPos.x
    const offsetY = event.clientY - rect.top - styleBarPos.y
    const move = (moveEvent: PointerEvent) => {
      styleBarPos = clampToChart(
        { x: moveEvent.clientX - rect.left - offsetX, y: moveEvent.clientY - rect.top - offsetY },
        styleBarEl?.offsetWidth ?? 0,
        styleBarEl?.offsetHeight ?? 0
      )
    }
    const stop = () => {
      window.removeEventListener("pointermove", move)
      window.removeEventListener("pointerup", stop)
      window.removeEventListener("pointercancel", stop)
      onStyleBarPositionChange({ ...styleBarPos })
    }
    window.addEventListener("pointermove", move)
    window.addEventListener("pointerup", stop)
    window.addEventListener("pointercancel", stop)
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
      priceFormat: candlePriceFormat,
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

    const primitive = new DrawingsPrimitive()
    candles.attachPrimitive(primitive)
    validationPrimitive = new SignalValidationPrimitive()
    candles.attachPrimitive(validationPrimitive)
    const store = new DrawingStore(next => onDrawingsChange?.(next))
    drawingStore = store
    store.onChange = () => {
      primitive.setDrawings(store.all())
      hasDrawings = store.all().length > 0
    }
    // The interaction engine owns gestures and selection; Chart owns redraw
    // composition so store changes also update chrome state.
    drawingInteraction = new DrawingInteraction({
      container,
      chart,
      series: candles,
      store,
      primitive,
      callbacks: {
        onToolDone: () => (activeTool = null),
        onSelectionChange: id => (selectedDrawing = id ? (store.byId(id) ?? null) : null),
        onToolHotkey: tool => (activeTool = activeTool === tool ? null : tool),
        onToggleMagnet: toggleMagnet,
        onToggleLogScale: toggleLogScale,
        onResetView: resetChartView,
        onEscape: closeChrome,
      },
      uiRoot: rootEl,
    })

    chart.subscribeClick(param => {
      if (activeTool !== null || param.point === undefined) return
      const index = validationPrimitive?.hitTestAt(param.point.x)
      if (index !== null && index !== undefined) onValidationSpanSelect?.(index)
    })

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
      if (suppressLoadOlder) return
      if (range && range.from < LOAD_OLDER_THRESHOLD) onLoadOlder?.()
    })

    loadWasm()

    const resizeObserver = new ResizeObserver(() => {
      if (styleBarEl) {
        styleBarPos = clampToChart(styleBarPos, styleBarEl.offsetWidth, styleBarEl.offsetHeight)
      }
      if (contextMenu && contextMenuEl) {
        contextMenu = clampToChart(
          contextMenu,
          contextMenuEl.offsetWidth,
          contextMenuEl.offsetHeight,
          CONTEXT_MENU_MARGIN
        )
      }
    })
    resizeObserver.observe(container)

    let panePointerActive = false
    let paneCaptureFrame = 0
    const onPanePointerDown = () => {
      panePointerActive = true
    }
    const onPanePointerEnd = () => {
      if (!panePointerActive) return
      panePointerActive = false
      cancelAnimationFrame(paneCaptureFrame)
      paneCaptureFrame = requestAnimationFrame(capturePaneLayout)
    }
    rootEl.addEventListener("pointerdown", onPanePointerDown)
    window.addEventListener("pointerup", onPanePointerEnd)
    window.addEventListener("pointercancel", onPanePointerEnd)

    ready = true
    return () => {
      cancelAnimationFrame(paneCaptureFrame)
      rootEl.removeEventListener("pointerdown", onPanePointerDown)
      window.removeEventListener("pointerup", onPanePointerEnd)
      window.removeEventListener("pointercancel", onPanePointerEnd)
      resizeObserver.disconnect()
      drawingInteraction?.destroy()
      chart?.remove()
      chart = undefined
      validationPrimitive = undefined
    }
  })

  // Keep the external chart formatter in sync when navigation changes the market metadata.
  $effect(() => {
    candles?.applyOptions({ priceFormat: candlePriceFormat })
  })

  // Candles + volume: full reseed on seed change, else tail updates.
  $effect(() => {
    if (!ready || !candles || !volume || bars.length === 0) return
    const firstTime = bars[0].time
    if (seed !== plottedSeed) {
      candles.setData(bars.map(toCandle))
      volume.setData(bars.map(toVolume))
      suppressNextRangeEvent()
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
        suppressNextRangeEvent()
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

  $effect(() => {
    void seed
    void bars[0]?.time
    void bars.at(-1)?.time
    if (!ready || !validationPrimitive) return
    validationPrimitive.setSpans(
      validationSpans.map(span => ({
        ...span,
        startTime: stamp(span.startTime),
        endTime: stamp(span.endTime),
      }))
    )
    validationPrimitive.setSelected(selectedValidationSpan)
  })

  // Reads bars untracked: focusing must not re-center on every live tick.
  $effect(() => {
    void focusRevision
    const target = focusTime
    if (!ready || !chart || target === null) return
    const activeChart = chart
    untrack(() => {
      if (bars.length < 2) return
      const grid = timeGridOf(bars.map(bar => stamp(bar.time)))
      if (grid === null || target < bars[0].time || target > bars[bars.length - 1].time) return
      const center = logicalFor(stamp(target), grid)
      suppressNextRangeEvent()
      activeChart.timeScale().setVisibleLogicalRange({
        from: center - FOCUS_HALF_WINDOW,
        to: center + FOCUS_HALF_WINDOW,
      })
    })
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

  // Parent-owned position loads globally; clamp it to the current chart whenever
  // the toolbar is rendered (or a resized chart changes its available area).
  $effect(() => {
    const requested = styleBarPosition
    styleBarPos = ready
      ? clampToChart(requested, styleBarEl?.offsetWidth ?? 0, styleBarEl?.offsetHeight ?? 0)
      : { ...requested }
  })

  $effect(() => {
    const layout = paneLayout
    const paneIds = renderedPaneIds
    if (!ready || !chart) return
    applyPaneLayout(chart, paneIds, layout)
  })

  // Load the parent-provided drawings into the store (the parent swaps them per coin).
  $effect(() => {
    if (!ready) return
    drawingStore?.load(drawings)
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

  $effect(() => {
    const menu = contextMenu
    const menuElement = contextMenuEl
    if (!menu || !menuElement) return
    const clamped = clampToChart(menu, menuElement.offsetWidth, menuElement.offsetHeight, CONTEXT_MENU_MARGIN)
    if (clamped.x !== menu.x || clamped.y !== menu.y) contextMenu = clamped
    menuElement.querySelector<HTMLButtonElement>('[role="menuitem"]')?.focus()
  })

  $effect(() => {
    if (!contextMenu) return
    const onDown = (event: PointerEvent) => {
      const target = event.target
      if (target instanceof Node && !contextMenuEl?.contains(target)) contextMenu = null
    }
    window.addEventListener("pointerdown", onDown)
    return () => window.removeEventListener("pointerdown", onDown)
  })

  // Instance-driven indicators: overlays (SMA/EMA/Bollinger/…) as lines on the price
  // pane, oscillators (Stochastic/Williams/CCI/MFI/ADX/ATR/OBV) in their own panes
  // after the pinned RSI/MACD. Rebuild series on instance-set change; otherwise fill
  // from the bars (reseed on coin/tf switch or history prepend, tail per tick).
  const renderKey = (instance: IndicatorInstance) => `${instance.id}:${JSON.stringify(instance.params)}`

  function pivotRightOf(instance: IndicatorInstance | undefined): number {
    if (!instance) return 0
    const fallback = descriptorsById.get(instance.id)?.params.find(param => param.key === "pivotRight")?.default ?? 0
    return Math.round(instance.params.pivotRight ?? fallback)
  }

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
      const paneIds = [PRICE_PANE_LAYOUT_ID]
      for (const instance of chosen) {
        const descriptor = descriptorsById.get(instance.id)
        if (!descriptor) continue
        let paneIndex = 0
        if (descriptor.target === "pane") {
          paneIndex = nextPane++
          paneIds.push(instance.id)
        }
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
      renderedPaneIds = paneIds
      applyPaneLayout(activeChart, paneIds, paneLayout)
      plottedRenderKey = key
      plottedRenderSeed = -1
    }
    if (rendered.length === 0) {
      latestComputed = []
      latestChosen = []
      readouts = []
      return
    }

    const high = Float64Array.from(bars, bar => bar.high)
    const low = Float64Array.from(bars, bar => bar.low)
    const close = Float64Array.from(bars, bar => bar.close)
    const volume = Float64Array.from(bars, bar => bar.volume)
    // Divergences confirm on completed bars only; the forming bar would repaint pivots.
    const divergenceIndexes = chosen.flatMap((instance, index) =>
      descriptorsById.get(instance.id)?.kind === "divergence" ? [index] : []
    )
    const seriesIndexes = chosen.flatMap((instance, index) =>
      descriptorsById.get(instance.id)?.kind === "divergence" ? [] : [index]
    )
    const computed: (InstanceOutput | undefined)[] = new Array<InstanceOutput | undefined>(chosen.length)
    let completedDivergences: InstanceOutput[] = []
    try {
      const seriesComputed = computeIndicators(
        seriesIndexes.map(index => chosen[index]),
        { high, low, close, volume }
      )
      seriesIndexes.forEach((chosenIndex, position) => {
        computed[chosenIndex] = seriesComputed[position]
      })
      if (divergenceIndexes.length > 0 && bars.length > 1) {
        const completed = bars.length - 1
        completedDivergences = computeIndicators(
          divergenceIndexes.map(index => chosen[index]),
          {
            high: high.subarray(0, completed),
            low: low.subarray(0, completed),
            close: close.subarray(0, completed),
            volume: volume.subarray(0, completed),
          }
        )
      }
      indicatorError = ""
    } catch (error) {
      indicatorError = errorMessage(error)
      return
    }
    const divergencesByIndex = new Map(
      divergenceIndexes.map((chosenIndex, position) => [chosenIndex, completedDivergences[position]])
    )
    latestComputed = computed
    latestChosen = chosen
    if (!hovering) readouts = readoutsFor(bars.length - 1)
    const firstTime = bars[0].time
    const reseed = seed !== plottedRenderSeed || firstTime < plottedRenderFirstTime
    rendered.forEach((item, index) => {
      const output = computed[index]
      for (const line of item.series) {
        const values = output?.outputs.find(series => series.key === line.outputKey)?.values
        if (!values) continue
        if (line.kind === "histogram") {
          if (reseed) line.api.setData(toHistogram(values))
          else updateLastHistogram(line.api, values)
        } else {
          if (reseed) line.api.setData(toLine(values))
          else updateLast(line.api, values)
        }
      }
      const divergences = divergencesByIndex.get(index)?.divergences ?? []
      const confirmationOffset = pivotRightOf(chosen[index])
      // Seed + first bar pin the key to this dataset: span indices alone can
      // collide across reseeds and prepends, leaving stale span lines in place.
      const spanKey = `${seed}:${firstTime}|`.concat(
        divergences.map(span => `${span.direction}:${span.kind}:${span.fromIndex}:${span.toIndex}`).join("|")
      )
      if (spanKey !== item.spanKey) {
        for (const spanLine of item.spanLines) activeChart.removeSeries(spanLine)
        item.spanLines = divergences.flatMap(span => {
          const from = bars[span.fromIndex]
          const to = bars[span.toIndex]
          if (!from || !to) return []
          const color = span.direction === "Bullish" ? chartTheme.up : chartTheme.down
          const spanLine = activeChart.addSeries(
            LineSeries,
            {
              color,
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
          const confirmation = bars[span.toIndex + confirmationOffset]
          if (!confirmation) return [spanLine]
          const marker = activeChart.addSeries(
            LineSeries,
            {
              color,
              lineWidth: 1,
              priceLineVisible: false,
              lastValueVisible: false,
              crosshairMarkerVisible: false,
              pointMarkersVisible: true,
              pointMarkersRadius: 3,
            },
            0
          )
          marker.setData([{ time: stamp(confirmation.time), value: confirmation.close }])
          return [spanLine, marker]
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
      title="Logarithmic price scale (l)"
      onclick={toggleLogScale}
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
    <button class={toolClass("priceRange")} title="Price range" onclick={() => selectTool("priceRange")}>
      <Ruler class="size-3.5" />
    </button>
    <button class={toolClass("fib")} title="Fibonacci retracement (f)" onclick={() => selectTool("fib")}>
      <Rows3 class="size-3.5" />
    </button>
    <button class={toolClass("fibext")} title="Fibonacci extension (e)" onclick={() => selectTool("fibext")}>
      <Rows4 class="size-3.5" />
    </button>
    <button class={toolClass("channel")} title="Parallel channel (c)" onclick={() => selectTool("channel")}>
      <Equal class="size-3.5" />
    </button>
    <button class={toolClass("brush")} title="Brush (b)" onclick={() => selectTool("brush")}>
      <Brush class="size-3.5" />
    </button>
    <div class="my-0.5 h-px w-full bg-border"></div>
    <button
      class="flex justify-center rounded py-1.5 {magnet
        ? 'bg-accent text-accent-foreground'
        : 'text-muted-foreground hover:bg-accent/50'}"
      title="Magnet — snap to OHLC (m)"
      onclick={toggleMagnet}
    >
      <Magnet class="size-3.5" />
    </button>
    <button
      class="flex justify-center rounded py-1.5 text-muted-foreground hover:bg-accent/50 disabled:opacity-30"
      title="Clear drawings"
      disabled={!hasDrawings}
      onclick={clearDrawings}
    >
      <Trash2 class="size-3.5" />
    </button>
  </div>
  <div class="relative min-w-0 flex-1">
    <div
      bind:this={container}
      class="h-full w-full {activeTool ? 'cursor-crosshair' : ''}"
      role="application"
      aria-label="Interactive price chart"
      tabindex="-1"
      oncontextmenu={openContextMenu}
    ></div>
    {#if legend}
      <div
        class="pointer-events-none absolute top-2 left-2 z-10 flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-foreground tabular-nums"
      >
        <span>O <span class="text-foreground">{formatPrice(legend.open)}</span></span>
        <span>H <span class="text-foreground">{formatPrice(legend.high)}</span></span>
        <span>L <span class="text-foreground">{formatPrice(legend.low)}</span></span>
        <span>C <span class="text-foreground">{formatPrice(legend.close)}</span></span>
        <span>V <span class="text-foreground">{formatCompactNumber(legend.volume)}</span></span>
        <span class={legend.change > 0 ? "text-green-500" : legend.change < 0 ? "text-red-500" : ""}>
          {legend.change >= 0 ? "+" : ""}{legend.change.toFixed(2)}%
        </span>
        {#each readouts as readout (readout.label)}
          <span>{readout.label} <span style="color: {readout.color}">{fmtReadout(readout.value)}</span></span>
        {/each}
      </div>
    {/if}
    {#if indicatorError !== ""}
      <div
        class="absolute top-8 left-2 z-10 flex items-center gap-2 rounded-md border border-amber-500/40 bg-background/95 px-2 py-1 text-xs text-amber-500 shadow"
      >
        <span>Indicators unavailable: {indicatorError}</span>
        {#if !wasmReady}
          <button
            class="rounded border border-border px-1.5 py-0.5 text-foreground hover:bg-accent/70"
            onclick={loadWasm}
          >
            Retry
          </button>
        {/if}
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
    {#if contextMenu}
      <div
        bind:this={contextMenuEl}
        class="absolute z-30 w-52 rounded-md border border-border bg-background/95 p-1 shadow-xl backdrop-blur"
        style="left: {contextMenu.x}px; top: {contextMenu.y}px"
        role="menu"
        aria-label="Chart menu"
        tabindex="-1"
        onkeydown={onContextMenuKeyDown}
      >
        <button class={contextMenuItem} role="menuitem" onclick={() => runContextAction(resetChartView)}>
          <RotateCcw class="size-3.5" />
          <span class="flex-1">Reset view</span>
          <kbd class="text-[10px] text-muted-foreground">0</kbd>
        </button>
        <button class={contextMenuItem} role="menuitem" onclick={() => runContextAction(fitAllData)}>
          <Maximize2 class="size-3.5" />
          <span class="flex-1">Fit all loaded data</span>
        </button>
        <div class="my-1 h-px bg-border"></div>
        <button class={contextMenuItem} role="menuitem" onclick={() => runContextAction(toggleLogScale)}>
          {#if logScale}<Check class="size-3.5" />{:else}<span class="size-3.5"></span>{/if}
          <span class="flex-1">Logarithmic scale</span>
          <kbd class="text-[10px] text-muted-foreground">L</kbd>
        </button>
        <button class={contextMenuItem} role="menuitem" onclick={() => runContextAction(toggleMagnet)}>
          {#if magnet}<Check class="size-3.5" />{:else}<span class="size-3.5"></span>{/if}
          <span class="flex-1">Magnet</span>
          <kbd class="text-[10px] text-muted-foreground">M</kbd>
        </button>
        {#if hasDrawings}
          <div class="my-1 h-px bg-border"></div>
        {/if}
        {#if selectedDrawing}
          <button
            class="{contextMenuItem} text-red-400"
            role="menuitem"
            onclick={() => runContextAction(deleteSelected)}
          >
            <Trash2 class="size-3.5" />
            <span>Delete selected drawing</span>
          </button>
        {/if}
        {#if hasDrawings}
          <button
            class="{contextMenuItem} text-red-400"
            role="menuitem"
            onclick={() => runContextAction(clearDrawings)}
          >
            <Trash2 class="size-3.5" />
            <span>Clear all drawings</span>
          </button>
        {/if}
      </div>
    {/if}
    {#if selectedDrawing}
      <div
        bind:this={styleBarEl}
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
        {#if selectedDrawing.tool === "priceRange"}
          <button
            class="size-4 rounded-full border {selectedDrawing.style?.color === undefined
              ? 'border-foreground'
              : 'border-transparent'}"
            style={`background: linear-gradient(135deg, ${chartTheme.up} 0 50%, ${chartTheme.down} 50% 100%)`}
            aria-label="Automatic direction color"
            title="Automatic direction color"
            onclick={() => changeColor(undefined)}
          ></button>
        {/if}
        {#each drawingPalette as color (color)}
          <button
            class="size-4 rounded-full border {isDrawingColorSelected(color)
              ? 'border-foreground'
              : 'border-transparent'}"
            style="background-color: {color}"
            aria-label="Color"
            onclick={() => changeColor(color)}
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
