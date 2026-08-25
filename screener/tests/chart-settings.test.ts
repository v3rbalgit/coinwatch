import { describe, expect, it } from "vitest"

import {
  defaultChartChromeSettings,
  defaultChartViewSettings,
  parseChartChromeSettings,
  parseChartViewSettings,
  PRICE_PANE_LAYOUT_ID,
  serializeChartChromeSettings,
  serializeChartViewSettings,
  type ChartChromeSettings,
  type ChartViewSettings,
} from "../src/lib/chart/settings"

describe("chart settings", () => {
  it("round-trips per-coin view settings", () => {
    const settings: ChartViewSettings = { timeframe: "4h", logScale: true }

    expect(parseChartViewSettings(serializeChartViewSettings(settings))).toEqual(settings)
  })

  it("falls back when per-coin view settings are incomplete", () => {
    expect(parseChartViewSettings(JSON.stringify({ version: 1, timeframe: "5m", logScale: true }))).toEqual(
      defaultChartViewSettings()
    )
  })

  it("reports malformed persisted JSON to the I/O boundary", () => {
    expect(() => parseChartViewSettings("{")).toThrow("Malformed chart view settings")
    expect(() => parseChartChromeSettings("{")).toThrow("Malformed chart chrome settings")
  })

  it("round-trips global chrome settings", () => {
    const settings: ChartChromeSettings = {
      magnet: true,
      toolbarPosition: { x: 120, y: 80 },
      paneLayout: [
        { id: PRICE_PANE_LAYOUT_ID, stretchFactor: 2.5 },
        { id: "rsi", stretchFactor: 0.5 },
      ],
    }

    expect(parseChartChromeSettings(serializeChartChromeSettings(settings))).toEqual(settings)
  })

  it("migrates legacy chrome settings without losing existing preferences", () => {
    expect(
      parseChartChromeSettings(JSON.stringify({ version: 1, magnet: true, toolbarPosition: { x: 120, y: 80 } }))
    ).toEqual({ magnet: true, toolbarPosition: { x: 120, y: 80 }, paneLayout: [] })
  })

  it("falls back when pane layout entries are invalid", () => {
    expect(
      parseChartChromeSettings(
        JSON.stringify({
          version: 2,
          magnet: true,
          toolbarPosition: { x: 120, y: 80 },
          paneLayout: [{ id: "rsi", stretchFactor: 0 }],
        })
      )
    ).toEqual(defaultChartChromeSettings())
  })

  it("falls back when the toolbar position is invalid", () => {
    expect(
      parseChartChromeSettings(JSON.stringify({ version: 1, magnet: true, toolbarPosition: { x: -1, y: Number.NaN } }))
    ).toEqual(defaultChartChromeSettings())
  })
})
