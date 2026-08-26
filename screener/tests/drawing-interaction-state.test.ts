import { describe, expect, it } from "vitest"

import { moveDrawingPoints } from "$lib/chart/drawings/drag"
import type { Px } from "$lib/chart/drawings/geometry"
import { DrawingPlacement } from "$lib/chart/drawings/placement"
import { DrawingStore } from "$lib/chart/drawings/store"
import type { Drawing, DrawingPoint } from "$lib/chart/drawings/types"

import { drawingFixture } from "./drawing-fixture"

interface PlacementHarness {
  placement: DrawingPlacement
  added: { drawing: Drawing; select: boolean }[]
  previews: (Drawing | null)[]
  done: () => number
}

function placementHarness(points: readonly DrawingPoint[]): PlacementHarness {
  const added: { drawing: Drawing; select: boolean }[] = []
  const previews: (Drawing | null)[] = []
  let doneCount = 0
  const pointAt = (px: Px): DrawingPoint | null => points[Math.floor(px.x / 10)] ?? null
  return {
    placement: new DrawingPlacement({
      toMarket: pointAt,
      toMarketSmooth: pointAt,
      addDrawing: (drawing, select) => added.push({ drawing, select }),
      setPreview: drawing => previews.push(drawing),
      onToolDone: () => {
        doneCount += 1
      },
    }),
    added,
    previews,
    done: () => doneCount,
  }
}

describe("drawing placement state", () => {
  const points = drawingFixture().points

  it("completes a single-anchor tool on pointer down", () => {
    const harness = placementHarness(points)
    harness.placement.setTool("hline")

    expect(harness.placement.pointerDown({ x: 0, y: 0 })).toBe(true)
    expect(harness.added).toHaveLength(1)
    expect(harness.added[0]).toMatchObject({ select: true, drawing: { tool: "hline", points: [points[0]] } })
    expect(harness.done()).toBe(1)
    expect(harness.placement.inFlight).toBe(false)
  })

  it("previews and completes click-click placement", () => {
    const harness = placementHarness(points)
    harness.placement.setTool("trendline")

    harness.placement.pointerDown({ x: 0, y: 0 })
    expect(harness.placement.inFlight).toBe(true)
    harness.placement.pointerMove({ x: 10, y: 0 })
    expect(harness.previews.at(-1)).toMatchObject({ tool: "trendline", points: [points[0], points[1]] })
    harness.placement.pointerDown({ x: 10, y: 0 })

    expect(harness.added[0]).toMatchObject({
      select: true,
      drawing: { tool: "trendline", points: [points[0], points[1]] },
    })
    expect(harness.done()).toBe(1)
  })

  it("completes two-anchor tools by dragging beyond the threshold", () => {
    const harness = placementHarness(points)
    harness.placement.setTool("rect")

    harness.placement.pointerDown({ x: 0, y: 0 })
    expect(harness.placement.pointerUp({ x: 10, y: 0 })).toBe(true)

    expect(harness.added[0]).toMatchObject({ select: true, drawing: { tool: "rect", points: [points[0], points[1]] } })
    expect(harness.done()).toBe(1)
  })

  it("records lean brush paths and leaves the brush armed", () => {
    const harness = placementHarness(points)
    harness.placement.setTool("brush")

    harness.placement.pointerDown({ x: 0, y: 0 })
    harness.placement.pointerMove({ x: 2, y: 0 })
    harness.placement.pointerMove({ x: 10, y: 0 })
    expect(harness.placement.pointerUp({ x: 10, y: 0 })).toBe(true)

    expect(harness.added[0]).toMatchObject({
      select: false,
      drawing: { tool: "brush", points: [points[0], points[1]] },
    })
    expect(harness.placement.tool).toBe("brush")
    expect(harness.done()).toBe(0)
  })

  it("requires all three channel anchors", () => {
    const harness = placementHarness(points)
    harness.placement.setTool("channel")

    harness.placement.pointerDown({ x: 0, y: 0 })
    harness.placement.pointerDown({ x: 10, y: 0 })
    expect(harness.added).toHaveLength(0)
    harness.placement.pointerDown({ x: 20, y: 0 })

    expect(harness.added[0]).toMatchObject({
      select: true,
      drawing: { tool: "channel", points: [points[0], points[1], points[2]] },
    })
  })

  it("cancels only an in-flight gesture on pointer interruption", () => {
    const harness = placementHarness(points)
    harness.placement.setTool("trendline")

    expect(harness.placement.cancelGesture()).toBe(false)
    harness.placement.pointerDown({ x: 0, y: 0 })
    expect(harness.placement.cancelGesture()).toBe(true)

    expect(harness.previews.at(-1)).toBeNull()
    expect(harness.added).toHaveLength(0)
    expect(harness.done()).toBe(1)
  })
})

describe("drawing drag transforms", () => {
  const points = drawingFixture().points

  it("translates every body anchor by the incremental market delta", () => {
    const moved = moveDrawingPoints(points, "body", points[0], points[1])

    expect(moved).toEqual([
      { time: 1_700_003_600, price: 105 },
      { time: 1_700_007_200, price: 110 },
      { time: 1_700_010_800, price: 115 },
    ])
  })

  it("replaces only the dragged handle", () => {
    const moved = moveDrawingPoints(points, 1, points[0], points[2])

    expect(moved).toEqual([points[0], points[2], points[2]])
    expect(moved[0]).toBe(points[0])
  })
})

describe("drawing store persistence", () => {
  it("redraws transient drag updates and persists once on release", () => {
    const drawing = drawingFixture()
    const persisted: (readonly Drawing[])[] = []
    let changes = 0
    const store = new DrawingStore(drawings => persisted.push([...drawings]))
    store.onChange = () => {
      changes += 1
    }

    store.load([drawing])
    const moved = moveDrawingPoints(drawing.points, 0, drawing.points[0], drawing.points[1])
    store.update(drawing.id, { points: moved }, { persist: false })

    expect(changes).toBe(2)
    expect(persisted).toHaveLength(0)
    store.persist()
    expect(persisted).toEqual([[{ ...drawing, points: moved }]])
  })
})
