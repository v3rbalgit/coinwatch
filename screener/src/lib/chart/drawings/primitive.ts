import type {
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  ISeriesPrimitive,
  ISeriesPrimitiveAxisView,
  Logical,
  PrimitivePaneViewZOrder,
  SeriesAttachedParameter,
  UTCTimestamp,
} from "lightweight-charts"

import { chartTheme, withAlpha } from "../theme"
import { logicalFor, timeGridFromData, type TimeGrid } from "./coords"
import { distToSegment, extendRay, FIB_LEVELS, fibPrice, inRect, nearPoint, type Px, type Viewport } from "./geometry"
import type { Drawing } from "./types"

/** fancy-canvas rendering target, without depending on the transitive package. */
type CanvasTarget = Parameters<IPrimitivePaneRenderer["draw"]>[0]

// The chart renders UTCTimestamp ticks without timezone conversion — the
// axis label must agree with the axis.
const TIME_LABEL_FORMAT = new Intl.DateTimeFormat("en-US", {
  day: "2-digit",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
})

function axisView(coordinate: number, text: string): ISeriesPrimitiveAxisView {
  return {
    coordinate: () => coordinate,
    text: () => text,
    textColor: () => chartTheme.drawingHandleFill,
    backColor: () => chartTheme.drawing,
  }
}

/** Cursor-to-shape distance that still counts as a hit. */
const HIT_TOLERANCE = 6

/** Trend-based extension ratios projected from the third anchor. */
const FIBEXT_LEVELS = [0, 0.618, 1, 1.618, 2.618] as const

function boundingBox(points: readonly Px[]): { min: Px; max: Px } | null {
  const first = points[0]
  if (!first) return null
  const min = { ...first }
  const max = { ...first }
  for (const point of points) {
    min.x = Math.min(min.x, point.x)
    min.y = Math.min(min.y, point.y)
    max.x = Math.max(max.x, point.x)
    max.y = Math.max(max.y, point.y)
  }
  return { min, max }
}

/** A brush path's grabbable anchors are its endpoints — not every sample.
 * Indices are REAL point indices so a handle drag moves the right anchor. */
function handleEntries(item: { drawing: Drawing; px: Px[] }): { point: Px; index: number }[] {
  if (item.drawing.tool !== "brush") return item.px.map((point, index) => ({ point, index }))
  const first = item.px[0]
  const last = item.px[item.px.length - 1]
  if (!first || !last) return []
  return [
    { point: first, index: 0 },
    { point: last, index: item.px.length - 1 },
  ]
}
const HANDLE_SIZE = 4
const FONT = "10px ui-monospace, SFMono-Regular, Menlo, monospace"

export type DrawingHit = { id: string; target: "body" | number }

interface ResolvedDrawing {
  drawing: Drawing
  px: Px[]
}

/**
 * Single series primitive rendering every drawing on the price pane, plus the
 * in-progress preview and the selected drawing's handles. Market coordinates
 * resolve to pixels in `updateAllViews` (viewport changes) and cache; hit
 * tests run against the same cache so the renderer and interaction layer can
 * never disagree about where a drawing is.
 */
export class DrawingsPrimitive implements ISeriesPrimitive {
  private param: SeriesAttachedParameter | null = null
  private drawings: readonly Drawing[] = []
  private preview: Drawing | null = null
  private selectedId: string | null = null
  private resolved: ResolvedDrawing[] = []
  private resolvedPreview: ResolvedDrawing | null = null
  private snap: { time: UTCTimestamp; price: number } | null = null
  private resolvedSnap: Px | null = null
  private viewport: Viewport = { width: 0, height: 0 }
  private grid: TimeGrid | null = null
  /** Measured label widths by drawing id — the text hit zone covers the label. */
  private textWidths = new Map<string, number>()
  private priceLabels: ISeriesPrimitiveAxisView[] = []
  private timeLabels: ISeriesPrimitiveAxisView[] = []
  private readonly view: IPrimitivePaneView
  private readonly views: IPrimitivePaneView[]

  constructor() {
    const renderer: IPrimitivePaneRenderer = {
      draw: (target: CanvasTarget) => {
        target.useMediaCoordinateSpace(scope => {
          this.viewport = { width: scope.mediaSize.width, height: scope.mediaSize.height }
          for (const item of this.resolved) {
            this.drawOne(scope.context, item, item.drawing.id === this.selectedId)
          }
          if (this.resolvedPreview) this.drawOne(scope.context, this.resolvedPreview, false)
          if (this.resolvedSnap) this.drawSnap(scope.context, this.resolvedSnap)
        })
      },
    }
    this.view = {
      zOrder: (): PrimitivePaneViewZOrder => "top",
      renderer: () => renderer,
    }
    // The library caches on array identity — must stay the same reference.
    this.views = [this.view]
  }

  attached(param: SeriesAttachedParameter): void {
    this.param = param
  }

  detached(): void {
    this.param = null
  }

  paneViews(): readonly IPrimitivePaneView[] {
    return this.views
  }

  updateAllViews(): void {
    this.grid = timeGridFromData(this.param?.series.data() ?? [])
    this.resolved = this.drawings
      .map(drawing => this.resolve(drawing))
      .filter((item): item is ResolvedDrawing => item !== null)
    this.resolvedPreview = this.preview ? this.resolve(this.preview) : null
    this.resolvedSnap = this.snap ? this.toPx(this.snap.time, this.snap.price) : null
    this.rebuildAxisLabels()
  }

  /** hlines announce their price, vlines their time — on the matching axis. */
  private rebuildAxisLabels(): void {
    const formatter = this.param?.series.priceFormatter()
    this.priceLabels = []
    this.timeLabels = []
    const labeled = this.resolvedPreview ? [...this.resolved, this.resolvedPreview] : this.resolved
    for (const { drawing, px } of labeled) {
      const anchor = drawing.points[0]
      const pixel = px[0]
      if (!anchor || !pixel) continue
      if (drawing.tool === "hline" && formatter) {
        this.priceLabels.push(axisView(pixel.y, formatter.format(anchor.price)))
      } else if (drawing.tool === "vline") {
        this.timeLabels.push(axisView(pixel.x, TIME_LABEL_FORMAT.format(anchor.time * 1000)))
      }
    }
  }

  priceAxisViews(): readonly ISeriesPrimitiveAxisView[] {
    return this.priceLabels
  }

  timeAxisViews(): readonly ISeriesPrimitiveAxisView[] {
    return this.timeLabels
  }

  setDrawings(drawings: readonly Drawing[]): void {
    this.drawings = drawings
    this.requestUpdate()
  }

  setPreview(preview: Drawing | null): void {
    this.preview = preview
    this.requestUpdate()
  }

  setSelected(id: string | null): void {
    this.selectedId = id
    this.requestUpdate()
  }

  /** The magnet's current snap target (a bar O/H/L/C level), or null when not snapping. */
  setSnapIndicator(point: { time: UTCTimestamp; price: number } | null): void {
    this.snap = point
    this.requestUpdate()
  }

  /**
   * Topmost hit at cursor: a handle index of the selected drawing wins over
   * bodies. Named apart from the base interface's `hitTest(x, y)`, which
   * serves the library's hover plumbing — this one drives our interaction.
   */
  hitTestAt(cursor: Px, toleranceScale = 1): DrawingHit | null {
    const tolerance = HIT_TOLERANCE * toleranceScale
    if (this.selectedId) {
      const selected = this.resolved.find(item => item.drawing.id === this.selectedId)
      if (selected) {
        for (const entry of handleEntries(selected)) {
          if (nearPoint(cursor, entry.point, (HANDLE_SIZE + HIT_TOLERANCE) * toleranceScale)) {
            return { id: selected.drawing.id, target: entry.index }
          }
        }
      }
    }
    for (let i = this.resolved.length - 1; i >= 0; i--) {
      const item = this.resolved[i]
      if (item && this.hitBody(cursor, item, tolerance)) return { id: item.drawing.id, target: "body" }
    }
    return null
  }

  private requestUpdate(): void {
    this.param?.requestUpdate()
  }

  private toPx(time: UTCTimestamp, price: number): Px | null {
    const param = this.param
    if (!param) return null
    const timeScale = param.chart.timeScale()
    let x = timeScale.timeToCoordinate(time)
    if (x === null && this.grid) {
      // Off the loaded range (unloaded history, future whitespace) — index
      // space extends linearly, so extrapolate the logical position instead
      // of snapping to the data edge, which would corrupt slopes.
      x = timeScale.logicalToCoordinate(logicalFor(time, this.grid) as Logical)
    }
    const y = param.series.priceToCoordinate(price)
    if (x === null || y === null) return null
    return { x, y }
  }

  private resolve(drawing: Drawing): ResolvedDrawing | null {
    const px = drawing.points.map(point => this.toPx(point.time, point.price))
    if (px.some(point => point === null)) return null
    return { drawing, px: px.filter((point): point is Px => point !== null) }
  }

  private hitBody(cursor: Px, item: ResolvedDrawing, tolerance: number): boolean {
    const [a, b] = item.px
    if (!a) return false
    switch (item.drawing.tool) {
      case "trendline":
        return b !== undefined && distToSegment(cursor, a, b) <= tolerance
      case "ray":
        return b !== undefined && distToSegment(cursor, a, extendRay(a, b, this.viewport)) <= tolerance
      case "hline":
        return Math.abs(cursor.y - a.y) <= tolerance
      case "vline":
        return Math.abs(cursor.x - a.x) <= tolerance
      case "rect":
        return b !== undefined && inRect(cursor, a, b, tolerance)
      case "fib":
        return b !== undefined && inRect(cursor, a, b, tolerance)
      case "fibext": {
        const bounds = boundingBox(item.px)
        return bounds !== null && inRect(cursor, bounds.min, bounds.max, tolerance)
      }
      case "channel": {
        const c = item.px[2]
        if (b === undefined) return false
        if (distToSegment(cursor, a, b) <= tolerance) return true
        if (!c) return false
        const offset = { x: c.x + (b.x - a.x), y: c.y + (b.y - a.y) }
        return distToSegment(cursor, c, offset) <= tolerance
      }
      case "brush": {
        for (let i = 1; i < item.px.length; i++) {
          const from = item.px[i - 1]
          const to = item.px[i]
          if (from && to && distToSegment(cursor, from, to) <= tolerance) return true
        }
        return false
      }
      case "text": {
        const width = this.textWidths.get(item.drawing.id) ?? (item.drawing.text?.length ?? 0) * 6
        return inRect(cursor, { x: a.x - 4, y: a.y - 8 }, { x: a.x + 6 + width, y: a.y + 8 }, tolerance)
      }
    }
  }

  private drawOne(ctx: CanvasRenderingContext2D, item: ResolvedDrawing, selected: boolean): void {
    const [a, b] = item.px
    if (!a) return
    const stroke = item.drawing.style?.color ?? chartTheme.drawing
    ctx.save()
    ctx.strokeStyle = stroke
    ctx.fillStyle = item.drawing.style?.color ? withAlpha(stroke, 0.1) : chartTheme.drawingFill
    ctx.lineWidth = item.drawing.style?.width ?? 1
    if (item.drawing.style?.dashed) ctx.setLineDash([6, 4])

    switch (item.drawing.tool) {
      case "trendline":
        if (b) this.strokeLine(ctx, a, b)
        break
      case "ray":
        if (b) this.strokeLine(ctx, a, extendRay(a, b, this.viewport))
        break
      case "hline":
        this.strokeLine(ctx, { x: 0, y: a.y }, { x: this.viewport.width, y: a.y })
        break
      case "vline":
        this.strokeLine(ctx, { x: a.x, y: 0 }, { x: a.x, y: this.viewport.height })
        break
      case "rect":
        if (b) {
          ctx.fillRect(a.x, a.y, b.x - a.x, b.y - a.y)
          ctx.strokeRect(a.x, a.y, b.x - a.x, b.y - a.y)
        }
        break
      case "fib":
        if (b) this.drawFib(ctx, item.drawing, a, b)
        break
      case "fibext":
        if (b) this.drawFibExtension(ctx, item, a, b)
        break
      case "channel": {
        if (!b) break
        this.strokeLine(ctx, a, b)
        const c = item.px[2]
        if (c) {
          this.strokeLine(ctx, c, { x: c.x + (b.x - a.x), y: c.y + (b.y - a.y) })
          // Dashed midline between the two rails reads as the channel axis.
          ctx.save()
          ctx.setLineDash([4, 4])
          this.strokeLine(
            ctx,
            { x: (a.x + c.x) / 2, y: (a.y + c.y) / 2 },
            { x: (b.x + c.x + (b.x - a.x)) / 2, y: (b.y + c.y + (b.y - a.y)) / 2 }
          )
          ctx.restore()
        }
        break
      }
      case "brush":
        if (item.px.length >= 2) {
          ctx.lineJoin = "round"
          ctx.lineCap = "round"
          ctx.beginPath()
          ctx.moveTo(a.x, a.y)
          for (let i = 1; i < item.px.length; i++) {
            const point = item.px[i]
            if (point) ctx.lineTo(point.x, point.y)
          }
          ctx.stroke()
        }
        break
      case "text":
        ctx.font = FONT
        ctx.fillStyle = item.drawing.style?.color ?? chartTheme.drawingText
        ctx.textBaseline = "middle"
        this.textWidths.set(item.drawing.id, ctx.measureText(item.drawing.text ?? "").width)
        ctx.fillText(item.drawing.text ?? "", a.x + 6, a.y)
        ctx.fillStyle = stroke
        ctx.beginPath()
        ctx.arc(a.x, a.y, 2, 0, Math.PI * 2)
        ctx.fill()
        break
    }

    if (selected) {
      // Handles render crisp regardless of the drawing's own dash/width.
      ctx.setLineDash([])
      ctx.lineWidth = 1
      for (const entry of handleEntries(item)) this.drawHandle(ctx, entry.point)
    }
    ctx.restore()
  }

  private strokeLine(ctx: CanvasRenderingContext2D, a: Px, b: Px): void {
    ctx.beginPath()
    ctx.moveTo(a.x, a.y)
    ctx.lineTo(b.x, b.y)
    ctx.stroke()
  }

  private drawFib(ctx: CanvasRenderingContext2D, drawing: Drawing, a: Px, b: Px): void {
    const from = drawing.points[0]
    const to = drawing.points[1]
    const param = this.param
    if (!from || !to || !param) return
    const stroke = drawing.style?.color ?? chartTheme.drawing
    const fill = drawing.style?.color ? withAlpha(stroke, 0.1) : chartTheme.drawingFill
    const left = Math.min(a.x, b.x)
    const right = Math.max(a.x, b.x)
    ctx.font = FONT
    ctx.textBaseline = "bottom"
    let previousY: number | null = null
    for (const ratio of FIB_LEVELS) {
      const price = fibPrice(from.price, to.price, ratio)
      const y = param.series.priceToCoordinate(price)
      if (y === null) continue
      if (previousY !== null) {
        ctx.fillStyle = fill
        ctx.fillRect(left, Math.min(previousY, y), right - left, Math.abs(y - previousY))
      }
      ctx.strokeStyle = stroke
      this.strokeLine(ctx, { x: left, y }, { x: right, y })
      ctx.fillStyle = chartTheme.drawingText
      ctx.fillText(`${ratio} ${param.series.priceFormatter().format(price)}`, left + 4, y - 2)
      previousY = y
    }
  }

  /**
   * Trend-based fib extension: levels are the A→B move projected from the
   * third anchor C, spanning the drawing's horizontal extent. With only two
   * anchors placed (preview), just the base leg renders.
   */
  private drawFibExtension(ctx: CanvasRenderingContext2D, item: ResolvedDrawing, a: Px, b: Px): void {
    const param = this.param
    const [from, to, pivot] = item.drawing.points
    ctx.save()
    ctx.setLineDash([2, 3])
    this.strokeLine(ctx, a, b)
    const c = item.px[2]
    if (c) this.strokeLine(ctx, b, c)
    ctx.restore()
    if (!param || !from || !to || !pivot || !c) return

    const bounds = boundingBox(item.px)
    if (!bounds) return
    const move = to.price - from.price
    ctx.font = FONT
    ctx.textBaseline = "bottom"
    for (const ratio of FIBEXT_LEVELS) {
      const price = pivot.price + move * ratio
      const y = param.series.priceToCoordinate(price)
      if (y === null) continue
      this.strokeLine(ctx, { x: bounds.min.x, y }, { x: bounds.max.x, y })
      ctx.fillStyle = chartTheme.drawingText
      ctx.fillText(`${ratio} ${param.series.priceFormatter().format(price)}`, bounds.min.x + 4, y - 2)
    }
  }

  private drawHandle(ctx: CanvasRenderingContext2D, point: Px): void {
    ctx.fillStyle = chartTheme.drawingHandleFill
    ctx.strokeStyle = chartTheme.drawing
    ctx.fillRect(point.x - HANDLE_SIZE, point.y - HANDLE_SIZE, HANDLE_SIZE * 2, HANDLE_SIZE * 2)
    ctx.strokeRect(point.x - HANDLE_SIZE, point.y - HANDLE_SIZE, HANDLE_SIZE * 2, HANDLE_SIZE * 2)
  }

  private drawSnap(ctx: CanvasRenderingContext2D, point: Px): void {
    ctx.save()
    ctx.beginPath()
    ctx.arc(point.x, point.y, 4, 0, Math.PI * 2)
    ctx.fillStyle = chartTheme.drawing
    ctx.strokeStyle = chartTheme.drawingHandleFill
    ctx.lineWidth = 1.5
    ctx.fill()
    ctx.stroke()
    ctx.restore()
  }
}
