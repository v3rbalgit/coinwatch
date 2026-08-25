export type ChartTimeframe = "1h" | "4h" | "1d" | "1w"

export const CHART_TIMEFRAMES: readonly ChartTimeframe[] = ["1h", "4h", "1d", "1w"]
export const CHART_CHROME_SETTINGS_KEY = "chart.chrome"

export interface ToolbarPosition {
  x: number
  y: number
}

export interface ChartViewSettings {
  timeframe: ChartTimeframe
  logScale: boolean
}

export interface ChartChromeSettings {
  magnet: boolean
  toolbarPosition: ToolbarPosition
}

const SETTINGS_VERSION = 1
const DEFAULT_TIMEFRAME: ChartTimeframe = "1h"
const DEFAULT_TOOLBAR_POSITION: ToolbarPosition = { x: 16, y: 44 }

export function defaultChartViewSettings(): ChartViewSettings {
  return { timeframe: DEFAULT_TIMEFRAME, logScale: false }
}

export function defaultChartChromeSettings(): ChartChromeSettings {
  return { magnet: false, toolbarPosition: { ...DEFAULT_TOOLBAR_POSITION } }
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
    value.version !== SETTINGS_VERSION ||
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
  return JSON.stringify({ version: SETTINGS_VERSION, ...settings })
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
    value.version !== SETTINGS_VERSION ||
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
  return { magnet: value.magnet, toolbarPosition: { x: position.x, y: position.y } }
}

export function serializeChartChromeSettings(settings: ChartChromeSettings): string {
  return JSON.stringify({ version: SETTINGS_VERSION, ...settings })
}
