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
  it("keeps the version 1 per-coin view schema stable", () => {
    const settings: ChartViewSettings = { timeframe: "4h", logScale: true }
    const serialized = serializeChartViewSettings(settings)
    const persisted: unknown = JSON.parse(serialized)

    expect(persisted).toEqual({ version: 1, timeframe: "4h", logScale: true })
    expect(parseChartViewSettings(serialized)).toEqual(settings)
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

  it("keeps the version 2 global chrome schema stable", () => {
    const settings: ChartChromeSettings = {
      magnet: true,
      toolbarPosition: { x: 120, y: 80 },
      paneLayout: [
        { id: PRICE_PANE_LAYOUT_ID, stretchFactor: 2.5 },
        { id: "rsi", stretchFactor: 0.5 },
      ],
    }
    const serialized = serializeChartChromeSettings(settings)
    const persisted: unknown = JSON.parse(serialized)

    expect(persisted).toEqual({ version: 2, ...settings })
    expect(parseChartChromeSettings(serialized)).toEqual(settings)
  })

  it("migrates legacy chrome settings without losing existing preferences", () => {
    expect(
      parseChartChromeSettings(JSON.stringify({ version: 1, magnet: true, toolbarPosition: { x: 120, y: 80 } }))
    ).toEqual({ magnet: true, toolbarPosition: { x: 120, y: 80 }, paneLayout: [] })
  })

  it.each([
    ["non-positive stretch", [{ id: "rsi", stretchFactor: 0 }]],
    [
      "duplicate pane ids",
      [
        { id: "rsi", stretchFactor: 1 },
        { id: "rsi", stretchFactor: 2 },
      ],
    ],
    ["empty pane id", [{ id: "", stretchFactor: 1 }]],
  ])("falls back for an invalid pane layout: %s", (_reason, paneLayout) => {
    expect(
      parseChartChromeSettings(
        JSON.stringify({ version: 2, magnet: true, toolbarPosition: { x: 120, y: 80 }, paneLayout })
      )
    ).toEqual(defaultChartChromeSettings())
  })

  it("falls back for unsupported or non-object settings versions", () => {
    expect(parseChartChromeSettings(JSON.stringify({ version: 3 }))).toEqual(defaultChartChromeSettings())
    expect(parseChartChromeSettings(JSON.stringify([]))).toEqual(defaultChartChromeSettings())
  })

  it("falls back when the toolbar position is invalid", () => {
    expect(
      parseChartChromeSettings(JSON.stringify({ version: 1, magnet: true, toolbarPosition: { x: -1, y: Number.NaN } }))
    ).toEqual(defaultChartChromeSettings())
  })
})
