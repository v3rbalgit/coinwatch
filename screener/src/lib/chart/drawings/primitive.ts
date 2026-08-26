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

import { chartTheme } from "../theme"
import { logicalFor, timeGridFromData, type TimeGrid } from "./coords"
import { nearPoint, type Px, type Viewport } from "./geometry"
import { DRAWING_HANDLE_SIZE, drawingHandleEntries, hitDrawingBody } from "./hit-test"
import { renderDrawing, renderSnapIndicator } from "./renderer"
import type { ResolvedDrawing } from "./resolved-drawing"
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

type DrawingHit = { id: string; target: "body" | number }

/**
 * Lightweight Charts lifecycle and coordinate adapter for the drawing layer.
 * Market anchors resolve once per view update; rendering and interaction both
 * consume that same pixel cache so they cannot disagree about shape position.
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
          for (const item of this.resolved) this.render(scope.context, item, item.drawing.id === this.selectedId)
          if (this.resolvedPreview) this.render(scope.context, this.resolvedPreview, false)
          if (this.resolvedSnap) renderSnapIndicator(scope.context, this.resolvedSnap)
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

  /** Topmost hit at the cursor; selected handles take priority over bodies. */
  hitTestAt(cursor: Px, toleranceScale = 1): DrawingHit | null {
    const tolerance = HIT_TOLERANCE * toleranceScale
    if (this.selectedId) {
      const selected = this.resolved.find(item => item.drawing.id === this.selectedId)
      if (selected) {
        for (const entry of drawingHandleEntries(selected)) {
          if (nearPoint(cursor, entry.point, (DRAWING_HANDLE_SIZE + HIT_TOLERANCE) * toleranceScale)) {
            return { id: selected.drawing.id, target: entry.index }
          }
        }
      }
    }
    for (let index = this.resolved.length - 1; index >= 0; index--) {
      const item = this.resolved[index]
      if (
        item &&
        hitDrawingBody({
          cursor,
          item,
          tolerance,
          viewport: this.viewport,
          textWidth: this.textWidths.get(item.drawing.id) ?? (item.drawing.text?.length ?? 0) * 6,
        })
      ) {
        return { id: item.drawing.id, target: "body" }
      }
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
      // space extends linearly instead of snapping to a data edge.
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

  private render(ctx: CanvasRenderingContext2D, item: ResolvedDrawing, selected: boolean): void {
    renderDrawing({
      ctx,
      item,
      selected,
      viewport: this.viewport,
      grid: this.grid,
      series: this.param?.series ?? null,
      textWidths: this.textWidths,
    })
  }
}
