<script lang="ts">
  import { ArrowLeft, LoaderCircle } from "@lucide/svelte"
  import { invoke } from "@tauri-apps/api/core"
  import { listen } from "@tauri-apps/api/event"
  import { onMount } from "svelte"

  import { goto } from "$app/navigation"
  import { page } from "$app/state"
  import Chart from "$lib/chart/Chart.svelte"
  import { parseDrawingsPayload, serializeDrawings, type Drawing } from "$lib/chart/drawings/types"
  import type { IndicatorInstance } from "$lib/chart/indicators"
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
  import type { Bar } from "$lib/chart/types"
  import Button from "$lib/components/ui/button.svelte"
  import CoinIcon from "$lib/components/ui/coin-icon.svelte"
  import Skeleton from "$lib/components/ui/skeleton.svelte"
  import { formatCompactNumber, formatPrice } from "$lib/format"

  type Row = {
    coin: string
    price: number
    prev_day: number
    volume_usd: number
    open_interest_usd: number
    funding: number
  }
  type CandleEvent = { coin: string; timeframe: string; candle: Bar }

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
  let chromeRevision = 0
  let viewSaveQueue = Promise.resolve()
  let chromeSaveQueue = Promise.resolve()
  let bars = $state<Bar[]>([])
  let seed = $state(0)
  let meta = $state<Row | null>(null)
  let livePrice = $state<number | null>(null)
  // Seed the chart with the core momentum and divergence set; the user toggles
  // additional instances through the chart picker.
  let indicatorInstances = $state<IndicatorInstance[]>([
    { id: "rsi", params: {} },
    { id: "macd", params: {} },
    { id: "rsiDivergence", params: {} },
    { id: "macdDivergence", params: {} },
  ])
  // Drawings persist per coin (a trendline on BTC is meaningless on ETH).
  let chartDrawings = $state<Drawing[]>([])

  async function loadCandles() {
    const [c, t] = [coin, tf]
    const data = await invoke<Bar[]>("coin_candles", { coin: c, timeframe: t })
    // Ignore a stale response if the coin/timeframe changed while it was in flight.
    if (c === coin && t === tf) {
      bars = data
      seed += 1
    }
  }

  let loadingOlder = false
  let exhausted = false
  // Page older history in on scroll-back; the backend serves the cache and fetches
  // from Hyperliquid + stores on a miss, so depth accumulates.
  async function loadOlder() {
    const oldest = bars[0]?.time
    if (loadingOlder || exhausted || oldest === undefined) return
    loadingOlder = true
    const [c, t] = [coin, tf]
    try {
      const older = await invoke<Bar[]>("coin_history", { coin: c, timeframe: t, beforeSeconds: oldest })
      if (c === coin && t === tf) {
        const fresh = older.filter(bar => bar.time < oldest)
        if (fresh.length === 0) exhausted = true
        else bars = [...fresh, ...bars]
      }
    } finally {
      loadingOlder = false
    }
  }
  async function loadMeta() {
    const rows = await invoke<Row[]>("screener_rows")
    meta = rows.find(row => row.coin === coin) ?? null
  }

  // The chart's visible indicators persist globally in SQLite.
  function persistInstances(next: IndicatorInstance[]) {
    indicatorInstances = next
    invoke("set_setting", { key: "chart.indicators", value: JSON.stringify(next) }).catch(error =>
      console.error("failed to save chart indicators", error)
    )
  }

  // Drawings persist per coin under their own key.
  function persistDrawings(next: readonly Drawing[]) {
    invoke("set_setting", { key: `chart.drawings.${coin}`, value: serializeDrawings(next) }).catch(error =>
      console.error("failed to save chart drawings", error)
    )
  }

  function persistViewSettings(settings: ChartViewSettings) {
    if (!coin || settingsCoin !== coin) return
    const key = chartViewSettingsKey(coin)
    const value = serializeChartViewSettings(settings)
    viewSaveQueue = viewSaveQueue
      .then(() => invoke<void>("set_setting", { key, value }))
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
    const value = serializeChartChromeSettings(settings)
    chromeSaveQueue = chromeSaveQueue
      .then(() => invoke<void>("set_setting", { key: CHART_CHROME_SETTINGS_KEY, value }))
      .catch(error => console.error("failed to save chart chrome settings", error))
  }

  function changeMagnet(enabled: boolean) {
    chromeRevision += 1
    magnet = enabled
    persistChromeSettings({ magnet: enabled, toolbarPosition: styleBarPosition, paneLayout })
  }

  function changeStyleBarPosition(position: ToolbarPosition) {
    chromeRevision += 1
    styleBarPosition = position
    persistChromeSettings({ magnet, toolbarPosition: position, paneLayout })
  }

  function changePaneLayout(next: PaneLayoutEntry[]) {
    chromeRevision += 1
    paneLayout = next
    persistChromeSettings({ magnet, toolbarPosition: styleBarPosition, paneLayout: next })
  }

  async function loadViewSettings(active: string, revision: number) {
    let settings = defaultChartViewSettings()
    try {
      await viewSaveQueue
      const saved = await invoke<string | null>("get_setting", { key: chartViewSettingsKey(active) })
      settings = parseChartViewSettings(saved)
    } catch (error) {
      console.error("failed to load chart view settings", error)
    }
    if (revision !== viewLoadRevision || active !== coin) return
    tf = settings.timeframe
    logScale = settings.logScale
    settingsCoin = active
  }

  const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null

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
    void invoke("set_active_coin", { coin })
    return () => {
      void invoke("set_active_coin", { coin: null })
    }
  })

  // Resolve the coin's view before loading candles, avoiding an unnecessary
  // request at the default timeframe followed by a second request at the saved one.
  $effect(() => {
    const active = coin
    const revision = ++viewLoadRevision
    settingsCoin = ""
    bars = []
    if (active) void loadViewSettings(active, revision)
  })

  // Reseed the chart whenever the resolved coin or timeframe changes.
  $effect(() => {
    void tf
    if (coin && settingsCoin === coin) {
      exhausted = false
      void loadCandles()
    }
  })

  // Load this coin's saved drawings; clear first so the previous coin's don't linger.
  $effect(() => {
    const active = coin
    if (!active) return
    chartDrawings = []
    void invoke<string | null>("get_setting", { key: `chart.drawings.${active}` })
      .then(saved => {
        if (active === coin) chartDrawings = parseDrawingsPayload(saved).drawings
      })
      .catch(error => console.error("failed to load chart drawings", error))
  })

  onMount(() => {
    void loadMeta()
    const chromeLoadRevision = chromeRevision
    void invoke<string | null>("get_setting", { key: CHART_CHROME_SETTINGS_KEY })
      .then(saved => {
        if (chromeRevision !== chromeLoadRevision) return
        const settings = parseChartChromeSettings(saved)
        magnet = settings.magnet
        styleBarPosition = settings.toolbarPosition
        paneLayout = settings.paneLayout
      })
      .catch(error => console.error("failed to load chart chrome settings", error))
    void invoke<string | null>("get_setting", { key: "chart.indicators" })
      .then(saved => {
        const parsed = saved === null ? null : parseInstances(JSON.parse(saved))
        // An empty saved set is valid (the user turned everything off) — restore it as
        // empty rather than falling back to the seed default.
        if (parsed) indicatorInstances = parsed
      })
      .catch(error => console.error("failed to load chart indicators", error))
    const reload = setInterval(loadMeta, 5000)
    const unMid = listen<{ coin: string; price: number }>("mid", event => {
      if (event.payload.coin !== coin) return
      livePrice = event.payload.price
      const last = bars.at(-1)
      if (!last) return
      const close = event.payload.price
      bars = [
        ...bars.slice(0, -1),
        { ...last, close, high: Math.max(last.high, close), low: Math.min(last.low, close) },
      ]
    })
    const unCandle = listen<CandleEvent>("candle", event => {
      if (event.payload.coin !== coin || event.payload.timeframe !== tf) return
      mergeBar(event.payload.candle)
    })
    return () => {
      clearInterval(reload)
      void unMid.then(off => off())
      void unCandle.then(off => off())
    }
  })

  const price = $derived(livePrice ?? meta?.price ?? 0)
  const change = $derived(meta && meta.prev_day > 0 ? (price / meta.prev_day - 1) * 100 : 0)
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
    <div class="ml-auto flex items-center gap-1">
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

  <div class="relative min-h-0 flex-1 overflow-hidden rounded-lg border border-border">
    <Chart
      {bars}
      {seed}
      instances={indicatorInstances}
      drawings={chartDrawings}
      {logScale}
      {magnet}
      {styleBarPosition}
      {paneLayout}
      onInstancesChange={persistInstances}
      onDrawingsChange={persistDrawings}
      onLogScaleChange={changeLogScale}
      onMagnetChange={changeMagnet}
      onStyleBarPositionChange={changeStyleBarPosition}
      onPaneLayoutChange={changePaneLayout}
      onLoadOlder={loadOlder}
    />
    {#if settingsCoin !== coin || bars.length === 0}
      <div class="absolute inset-0 flex items-center justify-center gap-2 text-sm text-muted-foreground">
        <LoaderCircle class="size-4 animate-spin" />
        Loading chart…
      </div>
    {/if}
  </div>
</div>
