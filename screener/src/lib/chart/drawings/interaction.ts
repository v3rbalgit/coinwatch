import type { IChartApi, ISeriesApi, UTCTimestamp } from "lightweight-charts"

import { timeAtLogical, timeAtLogicalExact, timeGridFromData } from "./coords"
import { nearestValueWithin, type Px } from "./geometry"
import type { DrawingsPrimitive } from "./primitive"
import type { DrawingStore } from "./store"
import { newDrawing, pointsRequired, type Drawing, type DrawingPoint, type DrawingTool } from "./types"

export interface InteractionCallbacks {
  /** Placement finished or cancelled — the toolbar disarms back to pointer mode. */
  onToolDone: () => void
  /** The text tool wants input at a pixel position for a market anchor. */
  onRequestText: (at: Px, point: DrawingPoint) => void
  /** Selection changed — the style bar follows it. */
  onSelectionChange?: (id: string | null) => void
  /** A tool hotkey was pressed while the cursor was over the chart. */
  onToolHotkey?: (tool: DrawingTool) => void
  /** The magnet hotkey was pressed while the cursor was over the chart. */
  onToggleMagnet?: () => void
}

/** Single-key tool hotkeys, active while the cursor is over the chart. */
export const TOOL_HOTKEYS: Record<string, DrawingTool> = {
  t: "trendline",
  y: "ray",
  h: "hline",
  v: "vline",
  r: "rect",
  f: "fib",
  e: "fibext",
  c: "channel",
  b: "brush",
  n: "text",
}

export const MAGNET_HOTKEY = "m"

interface DragState {
  id: string
  /** "body" translates every anchor; a number drags that single anchor. */
  target: "body" | number
  last: DrawingPoint
  /** Anchor snapshot at drag start — Escape/pointercancel revert to it. */
  original: DrawingPoint[]
  /** Only a drag that actually moved persists — a bare select click must not. */
  moved: boolean
}

/** Pixels of travel between anchor press and release that reads as drag-to-draw. */
const DRAG_DRAW_THRESHOLD = 8

/** Cursor-to-level distance at which the magnet grabs an OHLC value. */
const MAGNET_TOLERANCE = 8

/** Minimum pixel travel between recorded brush points — keeps paths lean. */
const BRUSH_MIN_STEP = 3

function shift(time: UTCTimestamp, delta: number): UTCTimestamp {
  return (time + delta) as UTCTimestamp
}

/** Finger-sized hit targets: touch pointers double every hit tolerance. */
function touchScale(event: PointerEvent): number {
  return event.pointerType === "touch" ? 2 : 1
}

function distance(a: Px, b: Px): number {
  return Math.hypot(a.x - b.x, a.y - b.y)
}

/**
 * Pointer/keyboard state machine over the drawings layer: armed-tool
 * placement (click-click or drag-to-draw, with live preview), selection,
 * handle and body drags, Escape to cancel, Delete to remove. Chart pan/zoom
 * is suspended while a tool is armed or a drag is active so the two never
 * fight; every teardown path (pointerup, pointercancel, Escape, destroy)
 * restores it and settles pending drag state.
 */
export class DrawingInteraction {
  private tool: DrawingTool | null = null
  /** Anchors placed so far for a click-click tool (2- or 3-point). */
  private pending: DrawingPoint[] = []
  private pendingStartPx: Px | null = null
  /** Freehand path being captured while the brush is held down. */
  private brushing: DrawingPoint[] | null = null
  private lastBrushPx: Px | null = null
  private drag: DragState | null = null
  private selectedId: string | null = null
  private magnet = false
  /** Hotkeys apply only while the cursor is over the chart. */
  private pointerInside = false

  constructor(
    private container: HTMLElement,
    private chart: IChartApi,
    private series: ISeriesApi<"Candlestick">,
    private store: DrawingStore,
    private primitive: DrawingsPrimitive,
    private callbacks: InteractionCallbacks,
    /** Outside-click deselect boundary — the chart UI root, so clicks on the
     * rail or style bar don't drop the selection (and unmount the bar mid-click). */
    private uiRoot: HTMLElement = container
  ) {
    store.onChange = () => primitive.setDrawings(store.all())
    primitive.setDrawings(store.all())
    container.addEventListener("pointerdown", this.onPointerDown)
    container.addEventListener("pointermove", this.onPointerMove)
    container.addEventListener("pointerenter", this.onPointerEnter)
    container.addEventListener("pointerleave", this.onPointerLeave)
    window.addEventListener("pointerup", this.onPointerUp)
    window.addEventListener("pointercancel", this.onPointerCancel)
    window.addEventListener("keydown", this.onKeyDown)
    document.addEventListener("pointerdown", this.onDocumentPointerDown)
  }

  destroy(): void {
    // A drag in flight at teardown (chart remount) must not lose its moves.
    if (this.drag) this.store.persist()
    this.drag = null
    this.setChartInteraction(true)
    this.container.removeEventListener("pointerdown", this.onPointerDown)
    this.container.removeEventListener("pointermove", this.onPointerMove)
    this.container.removeEventListener("pointerenter", this.onPointerEnter)
    this.container.removeEventListener("pointerleave", this.onPointerLeave)
    window.removeEventListener("pointerup", this.onPointerUp)
    window.removeEventListener("pointercancel", this.onPointerCancel)
    window.removeEventListener("keydown", this.onKeyDown)
    document.removeEventListener("pointerdown", this.onDocumentPointerDown)
  }

  private onPointerEnter = (): void => {
    this.pointerInside = true
  }

  private onPointerLeave = (): void => {
    this.pointerInside = false
    this.primitive.setSnapIndicator(null)
  }

  setTool(tool: DrawingTool | null): void {
    this.tool = tool
    this.pending = []
    this.pendingStartPx = null
    this.brushing = null
    this.lastBrushPx = null
    this.primitive.setPreview(null)
    // Arming a tool clears any selection; disarming (after a placement) keeps it so
    // the just-drawn shape stays selected.
    if (tool !== null) this.select(null)
    this.setChartInteraction(tool === null)
  }

  setMagnet(magnet: boolean): void {
    this.magnet = magnet
    if (!magnet) this.primitive.setSnapIndicator(null)
  }

  /** Drops the selection — the style bar's close/delete path. */
  deselect(): void {
    this.select(null)
  }

  /** Programmatic selection — the context menu's pick-under-cursor path. */
  selectDrawing(id: string): void {
    this.select(id)
  }

  private select(id: string | null): void {
    if (this.selectedId === id) return
    this.selectedId = id
    this.primitive.setSelected(id)
    this.callbacks.onSelectionChange?.(id)
  }

  private setChartInteraction(enabled: boolean): void {
    this.chart.applyOptions({ handleScroll: enabled, handleScale: enabled })
  }

  private cursor(event: PointerEvent): Px {
    const rect = this.container.getBoundingClientRect()
    return { x: event.clientX - rect.left, y: event.clientY - rect.top }
  }

  /**
   * Main-pane pixel → bar time. `coordinateToTime` is null outside the loaded
   * bars (notably the permanent right-offset whitespace), so fall back to
   * extrapolating the bar grid through logical space.
   */
  private timeAt(px: Px): UTCTimestamp | null {
    const time = this.chart.timeScale().coordinateToTime(px.x)
    if (typeof time === "number") return time
    const logical = this.chart.timeScale().coordinateToLogical(px.x)
    const grid = timeGridFromData(this.series.data())
    if (logical === null || !grid) return null
    return timeAtLogical(logical, grid)
  }

  /** Main-pane pixel → market coordinates, snapping price to O/H/L/C under the magnet. */
  private toMarket(px: Px): DrawingPoint | null {
    const price = this.series.coordinateToPrice(px.y)
    if (price === null) return null
    const time = this.timeAt(px)
    if (time === null) return null
    return { time, price: this.magnet ? (this.snapTarget(time, px.y) ?? price) : price }
  }

  /** Magnet: the bar's O/H/L/C level nearest the cursor within tolerance, or null if none. */
  private snapTarget(time: UTCTimestamp, cursorY: number): number | null {
    const bar = this.series.data().find(item => item.time === time)
    if (!bar || !("open" in bar)) return null
    const candidates = [bar.open, bar.high, bar.low, bar.close].flatMap(value => {
      const y = this.series.priceToCoordinate(value)
      return y === null ? [] : [{ value, y }]
    })
    return nearestValueWithin(cursorY, candidates, MAGNET_TOLERANCE)
  }

  /** Show the magnet's snap target as a marker so the grab is visible before a click. */
  private updateSnapIndicator(px: Px): void {
    const time = this.magnet && this.inMainPane(px) ? this.timeAt(px) : null
    const price = time !== null ? this.snapTarget(time, px.y) : null
    this.primitive.setSnapIndicator(time !== null && price !== null ? { time, price } : null)
  }

  /**
   * Freehand conversion: unrounded time so brush strokes don't quantize to
   * candle width. Price still respects the magnet.
   */
  private toMarketSmooth(px: Px): DrawingPoint | null {
    const point = this.toMarket(px)
    if (!point) return null
    const logical = this.chart.timeScale().coordinateToLogical(px.x)
    const grid = timeGridFromData(this.series.data())
    if (logical === null || !grid) return point
    return { time: timeAtLogicalExact(logical, grid), price: point.price }
  }

  /** Drawings live on the price pane only — ignore indicator panes below it. */
  private inMainPane(px: Px): boolean {
    return px.y <= this.chart.paneSize(0).height
  }

  /** Cursor→market conversion for UI layers (context menu) — the same bar-grid extrapolation as tool placement. */
  marketPointAt(px: Px): DrawingPoint | null {
    return this.toMarket(px)
  }

  private onPointerDown = (event: PointerEvent): void => {
    if (event.button !== 0) return
    const px = this.cursor(event)
    if (!this.inMainPane(px)) return

    if (this.tool === "brush") {
      const point = this.toMarketSmooth(px)
      if (!point) return
      event.preventDefault()
      this.brushing = [point]
      this.lastBrushPx = px
      this.container.setPointerCapture(event.pointerId)
      return
    }
    if (this.tool) {
      this.placePoint(this.tool, px, event)
      // Keep the drag-to-draw preview alive when the pointer leaves the
      // container; capture auto-releases on pointerup.
      if (this.pending.length > 0) this.container.setPointerCapture(event.pointerId)
      return
    }

    const hit = this.primitive.hitTestAt(px, touchScale(event))
    if (!hit) {
      this.select(null)
      return
    }
    const point = this.toMarket(px)
    this.select(hit.id)
    const drawing = this.store.byId(hit.id)
    if (!point || !drawing) return
    this.drag = { id: hit.id, target: hit.target, last: point, original: drawing.points, moved: false }
    this.container.setPointerCapture(event.pointerId)
    this.setChartInteraction(false)
    event.preventDefault()
  }

  /** Clicking anything outside the chart drops the selection, so a global
   * Delete keypress can never reach a drawing the user is no longer on. */
  private onDocumentPointerDown = (event: PointerEvent): void => {
    if (event.target instanceof Node && !this.uiRoot.contains(event.target)) this.select(null)
  }

  private placePoint(tool: DrawingTool, px: Px, event: PointerEvent): void {
    const point = this.toMarket(px)
    if (!point) return
    event.preventDefault()

    if (tool === "text") {
      this.callbacks.onRequestText(px, point)
      this.finishPlacement()
      return
    }
    if (pointsRequired(tool) === 1) {
      this.addDrawing(newDrawing(tool, [point]))
      this.finishPlacement()
      return
    }
    if (this.pending.length === 0) this.pendingStartPx = px
    this.pending.push(point)
    if (this.pending.length >= pointsRequired(tool)) {
      this.addDrawing(newDrawing(tool, this.pending))
      this.finishPlacement()
      return
    }
    this.primitive.setPreview(newDrawing(tool, [...this.pending, point]))
  }

  private finishPlacement(): void {
    this.pending = []
    this.pendingStartPx = null
    this.brushing = null
    this.lastBrushPx = null
    this.primitive.setPreview(null)
    this.callbacks.onToolDone()
  }

  /** Add a drawing and select it, so its handles + style bar are immediately available. */
  private addDrawing(drawing: Drawing): void {
    this.store.add(drawing)
    this.select(drawing.id)
  }

  /** Abandons a click-click / drag-to-draw in progress (Escape, pointercancel). */
  private cancelPlacement(): void {
    if (!this.tool) return
    this.finishPlacement()
  }

  private onPointerMove = (event: PointerEvent): void => {
    const px = this.cursor(event)
    this.updateSnapIndicator(px)

    if (this.brushing && this.lastBrushPx) {
      if (distance(px, this.lastBrushPx) < BRUSH_MIN_STEP) return
      const point = this.toMarketSmooth(px)
      if (!point) return
      this.brushing.push(point)
      this.lastBrushPx = px
      this.primitive.setPreview(newDrawing("brush", this.brushing))
      return
    }

    if (this.tool && this.pending.length > 0) {
      const point = this.toMarket(px)
      if (point) this.primitive.setPreview(newDrawing(this.tool, [...this.pending, point]))
      return
    }

    const drag = this.drag
    if (drag) {
      const point = this.toMarket(px)
      const drawing = this.store.byId(drag.id)
      if (!point || !drawing) return
      const points =
        drag.target === "body"
          ? drawing.points.map(anchor => ({
              time: shift(anchor.time, point.time - drag.last.time),
              price: anchor.price + point.price - drag.last.price,
            }))
          : drawing.points.map((anchor, index) => (index === drag.target ? point : anchor))
      this.store.update(drag.id, { points }, { persist: false })
      drag.last = point
      drag.moved = true
      return
    }

    if (!this.tool) {
      // Own only the "move" affordance — the price-line layer's cursors
      // (ns-resize/grab/pointer) were set moments earlier on this same event
      // and an unconditional reset here would stomp them.
      if (this.inMainPane(px) && this.primitive.hitTestAt(px, touchScale(event))) {
        this.container.style.cursor = "move"
      } else if (this.container.style.cursor === "move") {
        this.container.style.cursor = ""
      }
    }
  }

  private onPointerUp = (event: PointerEvent): void => {
    // A held brush finalizes on release; a stray click without travel
    // discards. Either way the brush STAYS ARMED for the next stroke —
    // disarming here made every follow-up press select instead of draw.
    if (this.brushing) {
      if (this.brushing.length >= 2) this.store.add(newDrawing("brush", this.brushing))
      this.brushing = null
      this.lastBrushPx = null
      this.primitive.setPreview(null)
      return
    }
    // Drag-to-draw: press at the first anchor, release far away = second
    // anchor. Two-point tools only — 3-point tools stay click-click-click.
    if (this.tool && pointsRequired(this.tool) === 2 && this.pending.length === 1 && this.pendingStartPx) {
      const px = this.cursor(event)
      if (distance(px, this.pendingStartPx) > DRAG_DRAW_THRESHOLD) {
        const first = this.pending[0]
        const point = this.toMarket(px)
        if (first && point) {
          this.addDrawing(newDrawing(this.tool, [first, point]))
          this.finishPlacement()
        }
      }
      return
    }
    const drag = this.drag
    if (!drag) return
    this.drag = null
    if (drag.moved) this.store.persist()
    if (this.container.hasPointerCapture(event.pointerId)) this.container.releasePointerCapture(event.pointerId)
    this.setChartInteraction(this.tool === null)
  }

  /** Interrupted gesture (touch scroll takeover, palm rejection) — revert. */
  private onPointerCancel = (event: PointerEvent): void => {
    if (this.drag) {
      this.revertDrag()
      if (this.container.hasPointerCapture(event.pointerId)) this.container.releasePointerCapture(event.pointerId)
      return
    }
    // Only an in-flight placement cancels; an idle armed tool stays armed.
    if (this.brushing || this.pending.length > 0) this.cancelPlacement()
  }

  private revertDrag(): void {
    const drag = this.drag
    if (!drag) return
    this.drag = null
    this.store.update(drag.id, { points: drag.original })
    this.setChartInteraction(this.tool === null)
  }

  private onKeyDown = (event: KeyboardEvent): void => {
    const target = event.target
    if (
      target instanceof HTMLInputElement ||
      target instanceof HTMLTextAreaElement ||
      (target instanceof HTMLElement && target.isContentEditable)
    ) {
      return
    }

    if (event.key === "Escape") {
      if (this.drag) {
        this.revertDrag()
      } else if (this.tool) {
        this.cancelPlacement()
      } else if (this.selectedId) {
        this.select(null)
      }
      return
    }

    if ((event.key === "Delete" || event.key === "Backspace") && this.selectedId) {
      // Backspace can still mean back-navigation in some browser configs.
      event.preventDefault()
      this.store.remove(this.selectedId)
      this.select(null)
      return
    }

    // Single-key hotkeys, only over the chart and never with modifiers.
    if (!this.pointerInside || event.ctrlKey || event.metaKey || event.altKey) return
    const key = event.key.toLowerCase()
    if (key === MAGNET_HOTKEY) {
      event.preventDefault()
      this.callbacks.onToggleMagnet?.()
      return
    }
    const tool = TOOL_HOTKEYS[key]
    if (tool) {
      event.preventDefault()
      this.callbacks.onToolHotkey?.(tool)
    }
  }
}
