import { isRecord } from "$lib/ipc/guards"
import { TIMEFRAMES } from "$lib/timeframes"
import type { Timeframe } from "$lib/timeframes"

export type ChartTimeframe = Timeframe

export const CHART_TIMEFRAMES: readonly ChartTimeframe[] = TIMEFRAMES
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

type PersistedSettingsName = "chart view" | "chart chrome"

type ChartChromeBase = Pick<ChartChromeSettings, "magnet" | "toolbarPosition">

interface PersistedSettings {
  version?: unknown
  timeframe?: unknown
  logScale?: unknown
  magnet?: unknown
  toolbarPosition?: unknown
  paneLayout?: unknown
}

interface PersistedToolbarPosition {
  x?: unknown
  y?: unknown
}

interface PersistedPaneLayoutEntry {
  id?: unknown
  stretchFactor?: unknown
}

function isPersistedSettings(value: unknown): value is PersistedSettings {
  return isRecord(value)
}

function parsePersistedSettings(raw: string | null, name: PersistedSettingsName): PersistedSettings | null {
  if (raw === null) return null
  try {
    const value: unknown = JSON.parse(raw)
    return isPersistedSettings(value) ? value : null
  } catch (error) {
    throw new Error(`Malformed ${name} settings`, { cause: error })
  }
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value)
}

function decodeChartViewSettings(value: PersistedSettings | null): ChartViewSettings | null {
  if (value === null) return null
  if (
    value.version !== VIEW_SETTINGS_VERSION ||
    typeof value.timeframe !== "string" ||
    !isChartTimeframe(value.timeframe) ||
    typeof value.logScale !== "boolean"
  ) {
    return null
  }
  return { timeframe: value.timeframe, logScale: value.logScale }
}

function isPersistedToolbarPosition(value: unknown): value is PersistedToolbarPosition {
  return isRecord(value)
}

function decodeToolbarPosition(value: unknown): ToolbarPosition | null {
  if (
    !isPersistedToolbarPosition(value) ||
    !isFiniteNumber(value.x) ||
    value.x < 0 ||
    !isFiniteNumber(value.y) ||
    value.y < 0
  ) {
    return null
  }
  return { x: value.x, y: value.y }
}

function decodeChartChromeBase(value: PersistedSettings): ChartChromeBase | null {
  if (typeof value.magnet !== "boolean") return null
  const toolbarPosition = decodeToolbarPosition(value.toolbarPosition)
  return toolbarPosition === null ? null : { magnet: value.magnet, toolbarPosition }
}

function isPersistedPaneLayoutEntry(value: unknown): value is PersistedPaneLayoutEntry {
  return isRecord(value)
}

function decodePaneLayoutEntry(value: unknown): PaneLayoutEntry | null {
  if (
    !isPersistedPaneLayoutEntry(value) ||
    typeof value.id !== "string" ||
    value.id.length === 0 ||
    !isFiniteNumber(value.stretchFactor) ||
    value.stretchFactor <= 0
  ) {
    return null
  }
  return { id: value.id, stretchFactor: value.stretchFactor }
}

function decodePaneLayout(value: unknown): PaneLayoutEntry[] | null {
  if (!Array.isArray(value)) return null
  const paneLayout: PaneLayoutEntry[] = []
  const paneIds = new Set<string>()
  for (const valueEntry of value) {
    const entry = decodePaneLayoutEntry(valueEntry)
    if (entry === null || paneIds.has(entry.id)) return null
    paneIds.add(entry.id)
    paneLayout.push(entry)
  }
  return paneLayout
}

function decodeLegacyChartChromeSettings(value: PersistedSettings): ChartChromeSettings | null {
  const base = decodeChartChromeBase(value)
  return base === null ? null : { ...base, paneLayout: [] }
}

function decodeCurrentChartChromeSettings(value: PersistedSettings): ChartChromeSettings | null {
  const base = decodeChartChromeBase(value)
  if (base === null) return null
  const paneLayout = decodePaneLayout(value.paneLayout)
  return paneLayout === null ? null : { ...base, paneLayout }
}

function decodeChartChromeSettings(value: PersistedSettings | null): ChartChromeSettings | null {
  if (value === null) return null
  switch (value.version) {
    case LEGACY_CHROME_SETTINGS_VERSION:
      return decodeLegacyChartChromeSettings(value)
    case CHROME_SETTINGS_VERSION:
      return decodeCurrentChartChromeSettings(value)
    default:
      return null
  }
}

export function parseChartViewSettings(raw: string | null): ChartViewSettings {
  return decodeChartViewSettings(parsePersistedSettings(raw, "chart view")) ?? defaultChartViewSettings()
}

export function serializeChartViewSettings(settings: ChartViewSettings): string {
  return JSON.stringify({ version: VIEW_SETTINGS_VERSION, ...settings })
}

export function parseChartChromeSettings(raw: string | null): ChartChromeSettings {
  return decodeChartChromeSettings(parsePersistedSettings(raw, "chart chrome")) ?? defaultChartChromeSettings()
}

export function serializeChartChromeSettings(settings: ChartChromeSettings): string {
  return JSON.stringify({ version: CHROME_SETTINGS_VERSION, ...settings })
}
