<script lang="ts">
  import { ArrowLeft, Ban, CalendarRange, Eye, EyeOff, ListTree, LoaderCircle, RotateCcw, Trash2 } from "@lucide/svelte"
  import { onMount } from "svelte"

  import { goto } from "$app/navigation"
  import { page } from "$app/state"
  import { candleHealthMessage, candleSnapshotDecision, type CandleLoadRequest } from "$lib/chart/candle-snapshot"
  import Chart from "$lib/chart/Chart.svelte"
  import { parseDrawingsPayload, serializeDrawings, type Drawing } from "$lib/chart/drawings/types"
  import {
    indicatorDescriptors,
    loadIndicators,
    type IndicatorDescriptor,
    type IndicatorInstance,
  } from "$lib/chart/indicators"
  import {
    CHART_CHROME_SETTINGS_KEY,
    CHART_TIMEFRAMES,
    chartViewSettingsKey,
    defaultChartChromeSettings,
    defaultChartViewSettings,
    parseChartChromeSettings,
    parseChartViewSettings,
    serializeChartChromeSettings,
    serializeChartViewSettings,
    type ChartChromeSettings,
    type ChartTimeframe,
    type ChartViewSettings,
    type PaneLayoutEntry,
    type ToolbarPosition,
  } from "$lib/chart/settings"
  import type { Bar, SignalValidationMarking } from "$lib/chart/types"
  import SignalValidationDialog from "$lib/components/signals/signal-validation-dialog.svelte"
  import SignalValidationPanel from "$lib/components/signals/signal-validation-panel.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import CoinIcon from "$lib/components/ui/coin-icon.svelte"
  import Skeleton from "$lib/components/ui/skeleton.svelte"
  import { errorMessage } from "$lib/errors"
  import { formatCompactNumber, formatPrice, prevDayChangePct } from "$lib/format"
  import type { CandleHealth } from "$lib/ipc/candles"
  import { onCandle, onCandleSnapshotUpdated, onMid } from "$lib/ipc/events"
  import { isRecord } from "$lib/ipc/guards"
  import { coinCandles, coinHistory, getSetting, screenerRows, setActiveCoin, setSetting } from "$lib/ipc/screener"
  import type { ScreenerRow } from "$lib/ipc/screener"
  import { signalValidationFailureMessage, type HistoricalRange } from "$lib/signals/historical"
  import { loadHistoricalSignalOptions, type HistoricalSignalOption } from "$lib/signals/historical-options"
  import {
    cancelHistoricalValidation,
    clearHistoricalValidation,
    historicalValidationWorkspace,
    selectHistoricalValidationSpan,
    setHistoricalValidationPanelOpen,
    setHistoricalValidationVisible,
    startHistoricalValidation,
  } from "$lib/signals/historical-workspace.svelte"

  const initialViewSettings = defaultChartViewSettings()
  const initialChromeSettings = defaultChartChromeSettings()
  const coin = $derived(page.params.coin ?? "")
  let tf = $state<ChartTimeframe>(initialViewSettings.timeframe)
  let logScale = $state(initialViewSettings.logScale)
  let magnet = $state(initialChromeSettings.magnet)
  let styleBarPosition = $state<ToolbarPosition>({ ...initialChromeSettings.toolbarPosition })
  let paneLayout = $state<PaneLayoutEntry[]>(initialChromeSettings.paneLayout.map(entry => ({ ...entry })))
  let settingsCoin = $state("")
  let viewLoadRevision = 0
  // Clicks racing the initial load are overlaid on it, so persists never write unloaded defaults.
  let chromeLoaded = false
  const pendingChrome: Partial<ChartChromeSettings> = {}
  let viewSaveQueue = Promise.resolve()
  let chromeSaveQueue = Promise.resolve()
  let bars = $state<Bar[]>([])
  // Snapshot revisions prevent REST/IPC responses from replacing newer live bars.
  let barsKey = ""
  let candleHealth = $state<CandleHealth>("loading")
  let candleRevision = 0
  let candleLoadSequence = 0
  let candleSeriesGeneration = 0
  let seed = $state(0)
  let meta = $state<ScreenerRow | null>(null)
  let livePrice = $state<number | null>(null)
  // Seed the chart with the core momentum and divergence set; the user toggles
  // additional instances through the chart picker.
  let instancesTouched = false
  let indicatorInstances = $state<IndicatorInstance[]>([
    { id: "rsi", params: {} },
    { id: "macd", params: {} },
    { id: "rsiDivergence", params: {} },
    { id: "macdDivergence", params: {} },
  ])
  // Drawings persist per coin (a trendline on BTC is meaningless on ETH).
  let chartDrawings = $state<Drawing[]>([])
  let foreignDrawings: unknown[] = []
  let candlesError = $state("")
  let validationDialogOpen = $state(false)
  let validationOptions = $state<HistoricalSignalOption[]>([])
  let validationOptionsLoading = $state(false)
  let validationOptionsError = $state("")
  let validationDescriptors = $state<IndicatorDescriptor[]>([])
  let focusTime = $state<number | null>(null)
  let focusRevision = $state(0)
  let validationNavigationError = $state("")

  const validationForCoin = $derived(historicalValidationWorkspace.snapshot?.coin === coin)
  const validationSnapshot = $derived(validationForCoin ? historicalValidationWorkspace.snapshot : null)
  const validationResult = $derived(validationForCoin ? historicalValidationWorkspace.result : null)
  const anyValidationActive = $derived(
    historicalValidationWorkspace.starting || historicalValidationWorkspace.progress.status === "running"
  )
  const validationActive = $derived(validationForCoin && anyValidationActive)
  const validationMarkings = $derived.by((): SignalValidationMarking[] => {
    if (!validationForCoin || !historicalValidationWorkspace.visible || validationResult === null) return []
    return validationResult.spans.map((span, index) => ({
      index,
      startTime: Math.floor(new Date(span.startedAt).getTime() / 1000),
      endTime: Math.floor(new Date(span.endedAt).getTime() / 1000),
      activeAtStart: span.activeAtStart,
      activeAtEnd: span.activeAtEnd,
    }))
  })
  const validationError = $derived.by(() => {
    if (!validationForCoin) return historicalValidationWorkspace.error || validationNavigationError
    const progress = historicalValidationWorkspace.progress
    if (progress.status === "failed") return signalValidationFailureMessage(progress.failure)
    return historicalValidationWorkspace.error || validationNavigationError
  })

  async function loadCandles(minimumRevision = 0) {
    const seriesKey = `${coin}|${tf}`
    const request: CandleLoadRequest = {
      seriesKey,
      sequence: ++candleLoadSequence,
      minimumRevision,
    }
    try {
      const snapshot = await coinCandles(coin, tf)
      const decision = candleSnapshotDecision(
        request,
        `${coin}|${tf}`,
        candleLoadSequence,
        candleRevision,
        snapshot.revision
      )
      if (decision === "apply") {
        bars = snapshot.candles
        barsKey = seriesKey
        candleHealth = snapshot.health
        candleRevision = snapshot.revision
        seed += 1
        candlesError = ""
      } else if (decision === "retry") {
        void loadCandles(Math.max(request.minimumRevision, candleRevision))
      }
    } catch (error) {
      if (request.seriesKey === `${coin}|${tf}` && request.sequence === candleLoadSequence) {
        candlesError = errorMessage(error)
      }
    }
  }

  let olderInFlight: Promise<boolean> | null = null
  let exhausted = false
  // Page older history in on scroll-back; the backend serves the cache and fetches
  // from Hyperliquid + stores on a miss, so depth accumulates. Concurrent callers
  // share the in-flight load instead of reading busy as a failure.
  function loadOlder(): Promise<boolean> {
    olderInFlight ??= loadOlderPage().finally(() => {
      olderInFlight = null
    })
    return olderInFlight
  }

  async function loadOlderPage(): Promise<boolean> {
    const oldest = bars[0]?.time
    if (exhausted || oldest === undefined) return false
    const [c, t, generation] = [coin, tf, candleSeriesGeneration]
    try {
      const older = await coinHistory(c, t, oldest)
      if (c !== coin || t !== tf || generation !== candleSeriesGeneration) return false
      const fresh = older.filter(bar => bar.time < oldest)
      if (fresh.length === 0) {
        exhausted = true
        return false
      }
      bars = [...fresh, ...bars]
      return true
    } catch (error) {
      console.error("failed to load older chart history", error)
      return false
    }
  }
  async function loadMeta() {
    const rows = await screenerRows()
    meta = rows.find(row => row.coin === coin) ?? null
  }

  async function openValidationDialog() {
    validationDialogOpen = true
    validationOptionsLoading = true
    validationOptionsError = ""
    try {
      validationOptions = await loadHistoricalSignalOptions()
      validationDescriptors = [...indicatorDescriptors()]
    } catch (error) {
      validationOptionsError = errorMessage(error)
      validationOptions = []
    } finally {
      validationOptionsLoading = false
    }
  }

  function validateSignal(option: HistoricalSignalOption, range: HistoricalRange, rangeLabel: string) {
    if (option.definition === null) {
      validationNavigationError = option.disabledReason ?? "The selected definition cannot be validated."
      return
    }
    validationNavigationError = ""
    void startHistoricalValidation({
      name: option.name,
      definition: $state.snapshot(option.definition),
      coin,
      range,
      rangeLabel,
      source: option.source,
    })
  }

  async function focusValidationSpan(index: number) {
    const span = validationResult?.spans[index]
    if (span === undefined) return
    const [activeCoin, activeTimeframe] = [coin, tf]
    selectHistoricalValidationSpan(index)
    validationNavigationError = ""
    const target = Math.floor(new Date(span.startedAt).getTime() / 1000)
    while ((bars[0]?.time ?? target) > target) {
      const loaded = await loadOlder()
      if (activeCoin !== coin || activeTimeframe !== tf || !validationForCoin) return
      if (!loaded) {
        validationNavigationError = "The chart could not load candles around this matched period."
        return
      }
    }
    if (activeCoin !== coin || activeTimeframe !== tf || !validationForCoin) return
    focusTime = target
    focusRevision += 1
  }

  function selectValidationMarking(index: number) {
    selectHistoricalValidationSpan(index)
    setHistoricalValidationPanelOpen(true)
  }

  // The chart's visible indicators persist globally in SQLite.
  function persistInstances(next: IndicatorInstance[]) {
    instancesTouched = true
    indicatorInstances = next
    setSetting("chart.indicators", JSON.stringify(next)).catch(error =>
      console.error("failed to save chart indicators", error)
    )
  }

  // Drawings persist per coin under their own key; foreign entries from newer
  // builds ride along untouched so this build's persists cannot destroy them.
  function persistDrawings(next: readonly Drawing[]) {
    setSetting(`chart.drawings.${coin}`, serializeDrawings(next, foreignDrawings)).catch(error =>
      console.error("failed to save chart drawings", error)
    )
  }

  function persistViewSettings(settings: ChartViewSettings) {
    if (!coin || settingsCoin !== coin) return
    const key = chartViewSettingsKey(coin)
    const value = serializeChartViewSettings(settings)
    viewSaveQueue = viewSaveQueue
      .then(() => setSetting(key, value))
      .catch(error => console.error("failed to save chart view settings", error))
  }

  function changeTimeframe(timeframe: ChartTimeframe) {
    if (settingsCoin !== coin) return
    tf = timeframe
    persistViewSettings({ timeframe, logScale })
  }

  function changeLogScale(enabled: boolean) {
    if (settingsCoin !== coin) return
    logScale = enabled
    persistViewSettings({ timeframe: tf, logScale: enabled })
  }

  function persistChromeSettings(settings: ChartChromeSettings) {
    if (!chromeLoaded) return
    const value = serializeChartChromeSettings(settings)
    chromeSaveQueue = chromeSaveQueue
      .then(() => setSetting(CHART_CHROME_SETTINGS_KEY, value))
      .catch(error => console.error("failed to save chart chrome settings", error))
  }

  function changeMagnet(enabled: boolean) {
    magnet = enabled
    if (!chromeLoaded) pendingChrome.magnet = enabled
    persistChromeSettings({ magnet: enabled, toolbarPosition: styleBarPosition, paneLayout })
  }

  function changeStyleBarPosition(position: ToolbarPosition) {
    styleBarPosition = position
    if (!chromeLoaded) pendingChrome.toolbarPosition = position
    persistChromeSettings({ magnet, toolbarPosition: position, paneLayout })
  }

  function changePaneLayout(next: PaneLayoutEntry[]) {
    paneLayout = next
    if (!chromeLoaded) pendingChrome.paneLayout = next
    persistChromeSettings({ magnet, toolbarPosition: styleBarPosition, paneLayout: next })
  }

  async function loadViewSettings(active: string, revision: number) {
    let settings = defaultChartViewSettings()
    try {
      await viewSaveQueue
      const saved = await getSetting(chartViewSettingsKey(active))
      settings = parseChartViewSettings(saved)
    } catch (error) {
      console.error("failed to load chart view settings", error)
    }
    if (revision !== viewLoadRevision || active !== coin) return
    tf = settings.timeframe
    logScale = settings.logScale
    settingsCoin = active
  }

  function parseInstances(value: unknown): IndicatorInstance[] | null {
    if (!Array.isArray(value)) return null
    const result: IndicatorInstance[] = []
    for (const item of value) {
      if (!isRecord(item) || typeof item.id !== "string") return null
      const params: Record<string, number> = {}
      const rawParams = item.params
      if (isRecord(rawParams)) {
        for (const [key, raw] of Object.entries(rawParams)) {
          if (typeof raw === "number") params[key] = raw
        }
      }
      result.push({ id: item.id, params })
    }
    return result
  }

  function mergeBar(candle: Bar) {
    const last = bars.at(-1)
    if (last?.time === candle.time) bars = [...bars.slice(0, -1), candle]
    else if (last && candle.time < last.time) return
    else bars = [...bars, candle]
  }

  // Stream only the viewed coin's live candles from the backend.
  $effect(() => {
    if (!coin) return
    void setActiveCoin(coin)
    return () => {
      void setActiveCoin(null)
    }
  })

  // Resolve the coin's view before loading candles, avoiding an unnecessary
  // request at the default timeframe followed by a second request at the saved one.
  $effect(() => {
    const active = coin
    const revision = ++viewLoadRevision
    settingsCoin = ""
    bars = []
    barsKey = ""
    candleHealth = "loading"
    candleRevision = 0
    candleLoadSequence += 1
    candleSeriesGeneration += 1
    if (active) void loadViewSettings(active, revision)
  })

  // Reseed the chart whenever the resolved coin or timeframe changes.
  $effect(() => {
    void tf
    if (coin && settingsCoin === coin) {
      exhausted = false
      candlesError = ""
      bars = []
      barsKey = ""
      candleHealth = "loading"
      candleRevision = 0
      candleLoadSequence += 1
      candleSeriesGeneration += 1
      void loadCandles()
    }
  })

  $effect(() => {
    if (!validationForCoin || validationResult === null || validationDescriptors.length > 0) return
    void loadIndicators()
      .then(() => (validationDescriptors = [...indicatorDescriptors()]))
      .catch(error => (validationNavigationError = errorMessage(error)))
  })

  // Load this coin's saved drawings; clear first so the previous coin's don't linger.
  $effect(() => {
    const active = coin
    if (!active) return
    chartDrawings = []
    foreignDrawings = []
    void getSetting(`chart.drawings.${active}`)
      .then(saved => {
        if (active !== coin) return
        const payload = parseDrawingsPayload(saved)
        chartDrawings = payload.drawings
        foreignDrawings = payload.foreign
      })
      .catch(error => console.error("failed to load chart drawings", error))
  })

  onMount(() => {
    void loadMeta()
    void getSetting(CHART_CHROME_SETTINGS_KEY)
      .then(saved => {
        const settings = parseChartChromeSettings(saved)
        magnet = pendingChrome.magnet ?? settings.magnet
        styleBarPosition = pendingChrome.toolbarPosition ?? settings.toolbarPosition
        paneLayout = pendingChrome.paneLayout ?? settings.paneLayout
        chromeLoaded = true
        if (Object.keys(pendingChrome).length > 0) {
          persistChromeSettings({ magnet, toolbarPosition: styleBarPosition, paneLayout })
        }
      })
      .catch(error => {
        console.error("failed to load chart chrome settings", error)
        chromeLoaded = true
      })
    void getSetting("chart.indicators")
      .then(saved => {
        const parsed = saved === null ? null : parseInstances(JSON.parse(saved))
        // An empty saved set is valid (the user turned everything off) — restore it as
        // empty rather than falling back to the seed default.
        if (parsed && !instancesTouched) indicatorInstances = parsed
      })
      .catch(error => console.error("failed to load chart indicators", error))
    const reload = setInterval(loadMeta, 5000)
    const unMid = onMid(event => {
      if (event.coin === coin) livePrice = event.price
    })
    const unCandle = onCandle(event => {
      const activeKey = `${coin}|${tf}`
      if (event.coin !== coin || event.timeframe !== tf || event.revision <= candleRevision) return
      candleHealth = event.health
      if (barsKey !== activeKey) {
        void loadCandles(event.revision)
        return
      }
      candleRevision = event.revision
      if (event.health !== "ready") return
      mergeBar(event.candle)
    })
    const unSnapshot = onCandleSnapshotUpdated(event => {
      const activeKey = `${coin}|${tf}`
      if (event.coin !== coin || event.timeframe !== tf || event.revision < candleRevision) return
      candleHealth = event.health
      if (event.revision > candleRevision || barsKey !== activeKey) void loadCandles(event.revision)
    })
    return () => {
      clearInterval(reload)
      void unMid.then(off => off())
      void unCandle.then(off => off())
      void unSnapshot.then(off => off())
    }
  })

  const candleStatusMessage = $derived(candleHealthMessage(candleHealth))
  const price = $derived(livePrice ?? meta?.price ?? 0)
  const change = $derived(meta ? prevDayChangePct(meta.prev_day, price) : 0)
  const hasPrice = $derived(livePrice !== null || meta !== null)
</script>

<div class="flex h-full flex-col gap-3 p-4">
  <header class="flex flex-shrink-0 items-center gap-3">
    <Button variant="ghost" size="sm" onclick={() => goto("/browse")}>
      <ArrowLeft class="size-4" />
      Back
    </Button>
    <CoinIcon {coin} size={28} />
    <h1 class="text-lg font-semibold text-foreground">{coin}</h1>
    {#if hasPrice}
      <span class="text-lg text-foreground tabular-nums">{formatPrice(price)}</span>
      <span
        class="tabular-nums {change > 0 ? 'text-green-500' : change < 0 ? 'text-red-500' : 'text-muted-foreground'}"
      >
        {change >= 0 ? "+" : ""}{change.toFixed(2)}%
      </span>
    {:else}
      <Skeleton class="h-6 w-24" />
      <Skeleton class="h-5 w-14" />
    {/if}
    <div class="ml-auto flex items-center gap-2">
      <Button
        variant="outline"
        size="sm"
        disabled={anyValidationActive}
        title={anyValidationActive && !validationForCoin ? "Another market is being validated" : undefined}
        onclick={openValidationDialog}
      >
        <CalendarRange class="size-4" />Validate signal
      </Button>
      <div class="flex items-center gap-1">
        {#each CHART_TIMEFRAMES as t (t)}
          <button
            class="rounded px-2.5 py-1 text-xs font-medium disabled:opacity-40 {tf === t
              ? 'bg-accent text-accent-foreground'
              : 'text-muted-foreground hover:bg-accent/50'}"
            disabled={settingsCoin !== coin}
            onclick={() => changeTimeframe(t)}
          >
            {t}
          </button>
        {/each}
      </div>
    </div>
  </header>

  <div class="flex flex-shrink-0 items-center gap-6 text-xs text-muted-foreground">
    {#if meta}
      <span>Vol <span class="text-foreground tabular-nums">{formatCompactNumber(meta.volume_usd)}</span></span>
      <span>OI <span class="text-foreground tabular-nums">{formatCompactNumber(meta.open_interest_usd)}</span></span>
      <span>
        Funding
        <span
          class="tabular-nums {meta.funding > 0
            ? 'text-green-500'
            : meta.funding < 0
              ? 'text-red-500'
              : 'text-foreground'}"
        >
          {(meta.funding * 100).toFixed(4)}%
        </span>
      </span>
    {:else}
      <Skeleton class="h-4 w-24" />
      <Skeleton class="h-4 w-24" />
      <Skeleton class="h-4 w-28" />
    {/if}
  </div>

  {#if validationForCoin && validationSnapshot}
    <section class="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-card px-3 py-2 text-xs">
      {#if validationActive}
        <LoaderCircle class="size-4 animate-spin text-primary" />
        <span class="font-medium text-foreground">Validating {validationSnapshot.name}</span>
        {#if historicalValidationWorkspace.progress.status === "running"}
          <span class="text-muted-foreground">
            {historicalValidationWorkspace.progress.phase === "hydrating"
              ? `Loaded ${historicalValidationWorkspace.progress.completed} / ${historicalValidationWorkspace.progress.total} timeframes`
              : `Checked ${historicalValidationWorkspace.progress.completed} / ${historicalValidationWorkspace.progress.total} candles`}
          </span>
        {:else}
          <span class="text-muted-foreground">Preparing completed-candle history…</span>
        {/if}
        <Button
          variant="ghost"
          size="sm"
          class="ml-auto"
          disabled={historicalValidationWorkspace.cancellationPending}
          onclick={cancelHistoricalValidation}
        >
          {#if historicalValidationWorkspace.cancellationPending}
            <LoaderCircle class="size-4 animate-spin" />Cancelling…
          {:else}
            <Ban class="size-4" />Cancel
          {/if}
        </Button>
      {:else if validationResult}
        <span class="font-medium text-foreground">{validationSnapshot.name}</span>
        <span class="text-muted-foreground">{validationSnapshot.rangeLabel}</span>
        <span class="text-muted-foreground">
          <strong class="text-foreground">{validationResult.spans.filter(span => !span.activeAtStart).length}</strong>
          onsets · <strong class="text-foreground">{validationResult.spans.length}</strong> matched periods
        </span>
        <div class="ml-auto flex items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            onclick={() => setHistoricalValidationPanelOpen(!historicalValidationWorkspace.panelOpen)}
          >
            <ListTree class="size-4" />{historicalValidationWorkspace.panelOpen ? "Hide details" : "Details"}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onclick={() => setHistoricalValidationVisible(!historicalValidationWorkspace.visible)}
          >
            {#if historicalValidationWorkspace.visible}
              <EyeOff class="size-4" />Hide markings
            {:else}
              <Eye class="size-4" />Show markings
            {/if}
          </Button>
          <Button variant="ghost" size="sm" onclick={openValidationDialog}>
            <RotateCcw class="size-4" />Change
          </Button>
          <Button variant="ghost" size="icon" aria-label="Remove signal validation" onclick={clearHistoricalValidation}>
            <Trash2 class="size-4" />
          </Button>
        </div>
      {:else if historicalValidationWorkspace.progress.status === "cancelled"}
        <CalendarRange class="size-4 text-muted-foreground" />
        <span class="font-medium text-foreground">{validationSnapshot.name}</span>
        <span class="text-muted-foreground">Validation cancelled before completion.</span>
      {:else}
        <CalendarRange class="size-4 text-muted-foreground" />
        <span class="font-medium text-foreground">{validationSnapshot.name}</span>
      {/if}
      {#if validationError}
        <p class="w-full text-xs text-destructive">{validationError}</p>
      {/if}
    </section>
  {:else if validationError}
    <p class="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
      {validationError}
    </p>
  {/if}

  <div class="relative min-h-0 flex-1 overflow-hidden rounded-lg border border-border">
    <Chart
      {bars}
      {seed}
      priceDecimals={meta?.coin === coin ? meta.price_decimals : undefined}
      instances={indicatorInstances}
      drawings={chartDrawings}
      {logScale}
      {magnet}
      {styleBarPosition}
      {paneLayout}
      validationSpans={validationMarkings}
      selectedValidationSpan={historicalValidationWorkspace.selectedSpan}
      focusTime={validationForCoin ? focusTime : null}
      {focusRevision}
      onValidationSpanSelect={selectValidationMarking}
      onInstancesChange={persistInstances}
      onDrawingsChange={persistDrawings}
      onLogScaleChange={changeLogScale}
      onMagnetChange={changeMagnet}
      onStyleBarPositionChange={changeStyleBarPosition}
      onPaneLayoutChange={changePaneLayout}
      onLoadOlder={loadOlder}
    />
    {#if candlesError !== "" && bars.length === 0}
      <div class="absolute inset-0 flex flex-col items-center justify-center gap-2 text-sm text-muted-foreground">
        <span>Chart candles failed to load: {candlesError}</span>
        <Button variant="outline" size="sm" onclick={() => void loadCandles()}>Retry</Button>
      </div>
    {:else if settingsCoin !== coin || bars.length === 0}
      <div class="absolute inset-0 flex items-center justify-center gap-2 text-sm text-muted-foreground">
        <LoaderCircle class="size-4 animate-spin" />
        {candleStatusMessage || "Loading chart…"}
      </div>
    {/if}
    {#if bars.length > 0 && (candlesError !== "" || candleHealth !== "ready")}
      <div
        class="absolute top-3 left-1/2 z-10 flex -translate-x-1/2 items-center gap-2 rounded-md border border-border bg-background/90 px-3 py-2 text-xs text-muted-foreground shadow-sm backdrop-blur"
        role="status"
      >
        <span>{candlesError !== "" ? `Chart refresh failed: ${candlesError}` : candleStatusMessage}</span>
        {#if candlesError !== ""}
          <Button variant="outline" size="sm" onclick={() => void loadCandles(candleRevision)}>Retry</Button>
        {/if}
      </div>
    {/if}
    {#if validationForCoin && validationSnapshot && validationResult && historicalValidationWorkspace.panelOpen && validationDescriptors.length > 0}
      <SignalValidationPanel
        snapshot={validationSnapshot}
        result={validationResult}
        selected={historicalValidationWorkspace.selectedSpan}
        descriptors={validationDescriptors}
        onSelect={focusValidationSpan}
        onClose={() => setHistoricalValidationPanelOpen(false)}
      />
    {/if}
  </div>

  <SignalValidationDialog
    bind:open={validationDialogOpen}
    {coin}
    options={validationOptions}
    loading={validationOptionsLoading}
    error={validationOptionsError}
    busy={anyValidationActive}
    onValidate={validateSignal}
  />
</div>
