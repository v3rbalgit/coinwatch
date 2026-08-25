export type ChartTimeframe = "1h" | "4h" | "1d" | "1w"

export const CHART_TIMEFRAMES: readonly ChartTimeframe[] = ["1h", "4h", "1d", "1w"]
export const CHART_CHROME_SETTINGS_KEY = "chart.chrome"
export const PRICE_PANE_LAYOUT_ID = "price"

export interface ToolbarPosition {
  x: number
  y: number
}

export interface ChartViewSettings {
  timeframe: ChartTimeframe
  logScale: boolean
}

export interface PaneLayoutEntry {
  id: string
  stretchFactor: number
}

export interface ChartChromeSettings {
  magnet: boolean
  toolbarPosition: ToolbarPosition
  paneLayout: PaneLayoutEntry[]
}

const VIEW_SETTINGS_VERSION = 1
const LEGACY_CHROME_SETTINGS_VERSION = 1
const CHROME_SETTINGS_VERSION = 2
const DEFAULT_TIMEFRAME: ChartTimeframe = "1h"
const DEFAULT_TOOLBAR_POSITION: ToolbarPosition = { x: 16, y: 44 }

export function defaultChartViewSettings(): ChartViewSettings {
  return { timeframe: DEFAULT_TIMEFRAME, logScale: false }
}

export function defaultChartChromeSettings(): ChartChromeSettings {
  return { magnet: false, toolbarPosition: { ...DEFAULT_TOOLBAR_POSITION }, paneLayout: [] }
}

export function chartViewSettingsKey(coin: string): string {
  return `chart.view.${coin}`
}

function isChartTimeframe(value: string): value is ChartTimeframe {
  return CHART_TIMEFRAMES.some(timeframe => timeframe === value)
}

export function parseChartViewSettings(raw: string | null): ChartViewSettings {
  let value: unknown = null
  if (raw !== null) {
    try {
      value = JSON.parse(raw)
    } catch (error) {
      throw new Error("Malformed chart view settings", { cause: error })
    }
  }
  if (
    typeof value !== "object" ||
    value === null ||
    !("version" in value) ||
    value.version !== VIEW_SETTINGS_VERSION ||
    !("timeframe" in value) ||
    typeof value.timeframe !== "string" ||
    !isChartTimeframe(value.timeframe) ||
    !("logScale" in value) ||
    typeof value.logScale !== "boolean"
  ) {
    return defaultChartViewSettings()
  }
  return { timeframe: value.timeframe, logScale: value.logScale }
}

export function serializeChartViewSettings(settings: ChartViewSettings): string {
  return JSON.stringify({ version: VIEW_SETTINGS_VERSION, ...settings })
}

export function parseChartChromeSettings(raw: string | null): ChartChromeSettings {
  let value: unknown = null
  if (raw !== null) {
    try {
      value = JSON.parse(raw)
    } catch (error) {
      throw new Error("Malformed chart chrome settings", { cause: error })
    }
  }
  if (
    typeof value !== "object" ||
    value === null ||
    !("version" in value) ||
    (value.version !== LEGACY_CHROME_SETTINGS_VERSION && value.version !== CHROME_SETTINGS_VERSION) ||
    !("magnet" in value) ||
    typeof value.magnet !== "boolean" ||
    !("toolbarPosition" in value)
  ) {
    return defaultChartChromeSettings()
  }
  const position = value.toolbarPosition
  if (
    typeof position !== "object" ||
    position === null ||
    !("x" in position) ||
    typeof position.x !== "number" ||
    !Number.isFinite(position.x) ||
    position.x < 0 ||
    !("y" in position) ||
    typeof position.y !== "number" ||
    !Number.isFinite(position.y) ||
    position.y < 0
  ) {
    return defaultChartChromeSettings()
  }
  const toolbarPosition = { x: position.x, y: position.y }
  if (value.version === LEGACY_CHROME_SETTINGS_VERSION) {
    return { magnet: value.magnet, toolbarPosition, paneLayout: [] }
  }
  if (!("paneLayout" in value) || !Array.isArray(value.paneLayout)) {
    return defaultChartChromeSettings()
  }
  const paneLayout: PaneLayoutEntry[] = []
  const paneIds = new Set<string>()
  for (const rawEntry of value.paneLayout) {
    const entry: unknown = rawEntry
    if (
      typeof entry !== "object" ||
      entry === null ||
      !("id" in entry) ||
      typeof entry.id !== "string" ||
      entry.id.length === 0 ||
      paneIds.has(entry.id) ||
      !("stretchFactor" in entry) ||
      typeof entry.stretchFactor !== "number" ||
      !Number.isFinite(entry.stretchFactor) ||
      entry.stretchFactor <= 0
    ) {
      return defaultChartChromeSettings()
    }
    paneIds.add(entry.id)
    paneLayout.push({ id: entry.id, stretchFactor: entry.stretchFactor })
  }
  return { magnet: value.magnet, toolbarPosition, paneLayout }
}

export function serializeChartChromeSettings(settings: ChartChromeSettings): string {
  return JSON.stringify({ version: CHROME_SETTINGS_VERSION, ...settings })
}
