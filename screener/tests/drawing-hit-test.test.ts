import { describe, expect, it } from "vitest"

import type { Px } from "$lib/chart/drawings/geometry"
import { drawingHandleEntries, hitDrawingBody } from "$lib/chart/drawings/hit-test"
import type { ResolvedDrawing } from "$lib/chart/drawings/resolved-drawing"
import type { DrawingTool } from "$lib/chart/drawings/types"

const viewport = { width: 100, height: 100 }

function resolved(tool: DrawingTool, px: readonly Px[], text?: string): ResolvedDrawing {
  return {
    drawing: { id: tool, tool, points: [], ...(text === undefined ? {} : { text }) },
    px,
  }
}

function hits(item: ResolvedDrawing, cursor: Px, textWidth = 0): boolean {
  return hitDrawingBody({ cursor, item, tolerance: 2, viewport, textWidth })
}

describe("drawing body hit testing", () => {
  it("tests finite and viewport-extended lines", () => {
    const anchors = [
      { x: 10, y: 10 },
      { x: 20, y: 20 },
    ]

    expect(hits(resolved("trendline", anchors), { x: 15, y: 15 })).toBe(true)
    expect(hits(resolved("trendline", anchors), { x: 80, y: 80 })).toBe(false)
    expect(hits(resolved("ray", anchors), { x: 80, y: 80 })).toBe(true)
  })

  it("tests full-width horizontal and vertical lines by their fixed coordinate", () => {
    expect(hits(resolved("hline", [{ x: 10, y: 30 }]), { x: 90, y: 31 })).toBe(true)
    expect(hits(resolved("vline", [{ x: 30, y: 10 }]), { x: 29, y: 90 })).toBe(true)
  })

  it.each(["rect", "priceRange", "fib"] satisfies readonly DrawingTool[])(
    "tests %s across its anchor rectangle",
    tool => {
      const item = resolved(tool, [
        { x: 10, y: 20 },
        { x: 40, y: 60 },
      ])

      expect(hits(item, { x: 25, y: 40 })).toBe(true)
      expect(hits(item, { x: 70, y: 70 })).toBe(false)
    }
  )

  it("uses every extension anchor for the hit bounds", () => {
    const item = resolved("fibext", [
      { x: 10, y: 20 },
      { x: 30, y: 10 },
      { x: 60, y: 70 },
    ])

    expect(hits(item, { x: 55, y: 60 })).toBe(true)
    expect(hits(item, { x: 80, y: 80 })).toBe(false)
  })

  it("tests both channel rails", () => {
    const item = resolved("channel", [
      { x: 10, y: 10 },
      { x: 40, y: 10 },
      { x: 10, y: 30 },
    ])

    expect(hits(item, { x: 25, y: 30 })).toBe(true)
    expect(hits(item, { x: 25, y: 20 })).toBe(false)
  })

  it("tests every brush segment", () => {
    const item = resolved("brush", [
      { x: 10, y: 10 },
      { x: 20, y: 30 },
      { x: 50, y: 30 },
    ])

    expect(hits(item, { x: 40, y: 31 })).toBe(true)
    expect(hits(item, { x: 70, y: 70 })).toBe(false)
  })

  it("includes the measured text label width", () => {
    const item = resolved("text", [{ x: 10, y: 20 }], "label")

    expect(hits(item, { x: 55, y: 20 }, 40)).toBe(true)
    expect(hits(item, { x: 70, y: 20 }, 40)).toBe(false)
  })

  it("exposes only brush endpoints as draggable handles", () => {
    const item = resolved("brush", [
      { x: 10, y: 10 },
      { x: 20, y: 30 },
      { x: 50, y: 30 },
    ])

    expect(drawingHandleEntries(item).map(entry => entry.index)).toEqual([0, 2])
  })
})
