import { describe, expect, it } from "vitest"

import { renderDrawing, renderSnapIndicator } from "$lib/chart/drawings/renderer"
import type { ResolvedDrawing } from "$lib/chart/drawings/resolved-drawing"
import { DRAWING_TOOLS } from "$lib/chart/drawings/types"

import { drawingFixture } from "./drawing-fixture"

class RecordingCanvas {
  strokeStyle: string | CanvasGradient | CanvasPattern = ""
  fillStyle: string | CanvasGradient | CanvasPattern = ""
  lineWidth = 1
  lineJoin: CanvasLineJoin = "miter"
  lineCap: CanvasLineCap = "butt"
  font = ""
  textAlign: CanvasTextAlign = "start"
  textBaseline: CanvasTextBaseline = "alphabetic"
  depth = 0
  operations = 0

  save(): void {
    this.depth += 1
    this.operations += 1
  }
  restore(): void {
    this.depth -= 1
    this.operations += 1
  }
  setLineDash(_segments: number[]): void {
    this.operations += 1
  }
  beginPath(): void {
    this.operations += 1
  }
  moveTo(_x: number, _y: number): void {
    this.operations += 1
  }
  lineTo(_x: number, _y: number): void {
    this.operations += 1
  }
  stroke(): void {
    this.operations += 1
  }
  fill(): void {
    this.operations += 1
  }
  fillRect(_x: number, _y: number, _width: number, _height: number): void {
    this.operations += 1
  }
  strokeRect(_x: number, _y: number, _width: number, _height: number): void {
    this.operations += 1
  }
  arc(_x: number, _y: number, _radius: number, _startAngle: number, _endAngle: number): void {
    this.operations += 1
  }
  measureText(text: string): { width: number } {
    this.operations += 1
    return { width: text.length * 6 }
  }
  fillText(_text: string, _x: number, _y: number): void {
    this.operations += 1
  }
}

const series = {
  priceToCoordinate: (price: number) => price,
  priceFormatter: () => ({ format: (price: number) => price.toFixed(2) }),
}

describe("drawing renderer dispatch", () => {
  it.each(DRAWING_TOOLS)("renders and balances canvas state for %s", tool => {
    const fixture = drawingFixture()
    const canvas = new RecordingCanvas()
    const item: ResolvedDrawing = {
      drawing: { ...fixture, id: tool, tool, text: "note" },
      px: [
        { x: 10, y: 20 },
        { x: 60, y: 70 },
        { x: 30, y: 40 },
      ],
    }

    renderDrawing({
      ctx: canvas,
      item,
      selected: true,
      viewport: { width: 200, height: 150 },
      grid: { lastTime: fixture.points[2].time, lastIndex: 2, intervalSec: 3_600 },
      series,
      textWidths: new Map(),
    })

    expect(canvas.operations).toBeGreaterThan(0)
    expect(canvas.depth).toBe(0)
  })

  it("renders a balanced snap indicator", () => {
    const canvas = new RecordingCanvas()

    renderSnapIndicator(canvas, { x: 20, y: 30 })

    expect(canvas.operations).toBeGreaterThan(0)
    expect(canvas.depth).toBe(0)
  })
})
