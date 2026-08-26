import type { IChartApi, ISeriesApi } from "lightweight-charts"

import { moveDrawingPoints, type DrawingDragTarget } from "./drag"
import type { Px } from "./geometry"
import { DrawingMarketCoordinates } from "./market-coordinates"
import { DrawingPlacement } from "./placement"
import type { DrawingsPrimitive } from "./primitive"
import type { DrawingStore } from "./store"
import type { DrawingPoint, InteractiveDrawingTool } from "./types"

interface InteractionCallbacks {
  /** Placement finished or cancelled — the toolbar disarms back to pointer mode. */
  onToolDone: () => void
  /** Selection changed — the style bar follows it. */
  onSelectionChange?: (id: string | null) => void
  /** A tool hotkey was pressed while the cursor was over the chart. */
  onToolHotkey?: (tool: InteractiveDrawingTool) => void
  /** The magnet hotkey was pressed while the cursor was over the chart. */
  onToggleMagnet?: () => void
  /** The log-scale hotkey was pressed while the cursor was over the chart. */
  onToggleLogScale?: () => void
  /** The reset-view hotkey was pressed while the cursor was over the chart. */
  onResetView?: () => void
  /** Chrome gets first refusal on Escape; true means it consumed the key. */
  onEscape?: () => boolean
}

interface DrawingInteractionOptions {
  container: HTMLElement
  chart: IChartApi
  series: ISeriesApi<"Candlestick">
  store: DrawingStore
  primitive: DrawingsPrimitive
  callbacks: InteractionCallbacks
  /** Outside-click boundary; controls inside it do not drop selection. */
  uiRoot?: HTMLElement
}

const TOOL_HOTKEYS = new Map<string, InteractiveDrawingTool>([
  ["t", "trendline"],
  ["y", "ray"],
  ["h", "hline"],
  ["v", "vline"],
  ["r", "rect"],
  ["f", "fib"],
  ["e", "fibext"],
  ["c", "channel"],
  ["b", "brush"],
])
const MAGNET_HOTKEY = "m"
const LOG_SCALE_HOTKEY = "l"
const RESET_VIEW_HOTKEY = "0"

interface DragState {
  id: string
  target: DrawingDragTarget
  last: DrawingPoint
  /** Anchor snapshot at drag start — Escape/pointercancel restore it. */
  original: DrawingPoint[]
  /** Only a drag that moved persists; a bare selection click does not. */
  moved: boolean
}

/** Finger-sized hit targets double mouse tolerance. */
function touchScale(event: PointerEvent): number {
  return event.pointerType === "touch" ? 2 : 1
}

function isEditableTarget(target: EventTarget | null): boolean {
  return (
    target instanceof HTMLInputElement ||
    target instanceof HTMLTextAreaElement ||
    (target instanceof HTMLElement && target.isContentEditable)
  )
}

/** DOM event orchestrator for placement, selection, dragging, and chart hotkeys. */
export class DrawingInteraction {
  private container: HTMLElement
  private chart: IChartApi
  private store: DrawingStore
  private primitive: DrawingsPrimitive
  private callbacks: InteractionCallbacks
  private uiRoot: HTMLElement
  private coordinates: DrawingMarketCoordinates
  private placement: DrawingPlacement
  private drag: DragState | null = null
  private selectedId: string | null = null
  private magnet = false
  private pointerInside = false
  private chartInteractionEnabled = true

  constructor(options: DrawingInteractionOptions) {
    this.container = options.container
    this.chart = options.chart
    this.store = options.store
    this.primitive = options.primitive
    this.callbacks = options.callbacks
    this.uiRoot = options.uiRoot ?? options.container
    this.coordinates = new DrawingMarketCoordinates(options.chart, options.series)
    this.placement = new DrawingPlacement({
      toMarket: px => this.coordinates.toMarket(px, this.magnet),
      toMarketSmooth: px => this.coordinates.toMarketSmooth(px, this.magnet),
      addDrawing: (drawing, select) => {
        this.store.add(drawing)
        if (select) this.select(drawing.id)
      },
      setPreview: drawing => this.primitive.setPreview(drawing),
      onToolDone: () => this.callbacks.onToolDone(),
    })

    this.primitive.setDrawings(this.store.all())
    this.container.addEventListener("pointerdown", this.onPointerDown)
    this.container.addEventListener("pointermove", this.onPointerMove)
    this.container.addEventListener("pointerenter", this.onPointerEnter)
    this.container.addEventListener("pointerleave", this.onPointerLeave)
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

  setTool(tool: InteractiveDrawingTool | null): void {
    this.placement.setTool(tool)
    // Arming clears selection; disarming leaves a newly placed drawing selected.
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

  private onPointerEnter = (): void => {
    this.pointerInside = true
  }

  private onPointerLeave = (): void => {
    this.pointerInside = false
    this.primitive.setSnapIndicator(null)
  }

  private select(id: string | null): void {
    if (this.selectedId === id) return
    this.selectedId = id
    this.primitive.setSelected(id)
    this.callbacks.onSelectionChange?.(id)
  }

  private setChartInteraction(enabled: boolean): void {
    if (this.chartInteractionEnabled === enabled) return
    this.chartInteractionEnabled = enabled
    this.chart.applyOptions({ handleScroll: enabled, handleScale: enabled })
  }

  private cursor(event: PointerEvent): Px {
    const rect = this.container.getBoundingClientRect()
    return { x: event.clientX - rect.left, y: event.clientY - rect.top }
  }

  private updateSnapIndicator(px: Px): void {
    this.primitive.setSnapIndicator(this.coordinates.snapIndicator(px, this.magnet))
  }

  private onPointerDown = (event: PointerEvent): void => {
    if (event.button !== 0) return
    const px = this.cursor(event)
    if (!this.coordinates.inMainPane(px)) return

    if (this.placement.tool !== null) {
      if (this.placement.pointerDown(px)) event.preventDefault()
      if (this.placement.inFlight) this.container.setPointerCapture(event.pointerId)
      return
    }

    const hit = this.primitive.hitTestAt(px, touchScale(event))
    if (!hit) {
      this.select(null)
      return
    }
    const point = this.coordinates.toMarket(px, this.magnet)
    this.select(hit.id)
    const drawing = this.store.byId(hit.id)
    if (point === null || drawing === undefined) return
    this.drag = { id: hit.id, target: hit.target, last: point, original: drawing.points, moved: false }
    this.container.setPointerCapture(event.pointerId)
    this.setChartInteraction(false)
    event.preventDefault()
  }

  /** Clicking outside chart chrome drops selection before global Delete can act on it. */
  private onDocumentPointerDown = (event: PointerEvent): void => {
    if (event.target instanceof Node && !this.uiRoot.contains(event.target)) this.select(null)
  }

  private onPointerMove = (event: PointerEvent): void => {
    const px = this.cursor(event)
    this.updateSnapIndicator(px)
    if (this.placement.pointerMove(px)) return
    if (this.updateDrag(px)) return
    this.updateHoverCursor(px, event)
  }

  private updateDrag(px: Px): boolean {
    const drag = this.drag
    if (drag === null) return false
    const point = this.coordinates.toMarket(px, this.magnet)
    const drawing = this.store.byId(drag.id)
    if (point === null || drawing === undefined) return true
    const points = moveDrawingPoints(drawing.points, drag.target, drag.last, point)
    this.store.update(drag.id, { points }, { persist: false })
    drag.last = point
    drag.moved = true
    return true
  }

  private updateHoverCursor(px: Px, event: PointerEvent): void {
    if (this.placement.tool !== null) return
    // Own only the move affordance; resetting any other cursor would stomp the
    // price-line layer's resize/grab cursor set earlier on this same event.
    if (this.coordinates.inMainPane(px) && this.primitive.hitTestAt(px, touchScale(event))) {
      this.container.style.cursor = "move"
    } else if (this.container.style.cursor === "move") {
      this.container.style.cursor = ""
    }
  }

  private onPointerUp = (event: PointerEvent): void => {
    if (this.placement.inFlight && this.placement.pointerUp(this.cursor(event))) return
    this.finishDrag(event)
  }

  private finishDrag(event: PointerEvent): void {
    const drag = this.drag
    if (drag === null) return
    this.drag = null
    if (drag.moved) this.store.persist()
    if (this.container.hasPointerCapture(event.pointerId)) this.container.releasePointerCapture(event.pointerId)
    this.setChartInteraction(this.placement.tool === null)
  }

  /** Interrupted gestures revert drags and cancel in-flight placement. */
  private onPointerCancel = (event: PointerEvent): void => {
    if (this.drag !== null) {
      this.revertDrag()
      if (this.container.hasPointerCapture(event.pointerId)) this.container.releasePointerCapture(event.pointerId)
      return
    }
    this.placement.cancelGesture()
  }

  private revertDrag(): void {
    const drag = this.drag
    if (drag === null) return
    this.drag = null
    this.store.update(drag.id, { points: drag.original })
    this.setChartInteraction(this.placement.tool === null)
  }

  private onKeyDown = (event: KeyboardEvent): void => {
    if (isEditableTarget(event.target)) return
    if (this.handleEscape(event)) return
    if (this.handleDelete(event)) return
    this.handleShortcut(event)
  }

  private handleEscape(event: KeyboardEvent): boolean {
    if (event.key !== "Escape") return false
    if (this.callbacks.onEscape?.()) {
      event.preventDefault()
    } else if (this.drag !== null) {
      this.revertDrag()
    } else if (this.placement.cancelTool()) {
      // Placement handles the callback and preview reset.
    } else if (this.selectedId !== null) {
      this.select(null)
    }
    return true
  }

  private handleDelete(event: KeyboardEvent): boolean {
    if ((event.key !== "Delete" && event.key !== "Backspace") || this.selectedId === null) return false
    event.preventDefault()
    this.store.remove(this.selectedId)
    this.select(null)
    return true
  }

  private handleShortcut(event: KeyboardEvent): void {
    if (!this.pointerInside || event.ctrlKey || event.metaKey || event.altKey) return
    const key = event.key.toLowerCase()
    if (key === RESET_VIEW_HOTKEY) {
      event.preventDefault()
      this.callbacks.onResetView?.()
      return
    }
    if (key === LOG_SCALE_HOTKEY) {
      event.preventDefault()
      this.callbacks.onToggleLogScale?.()
      return
    }
    if (key === MAGNET_HOTKEY) {
      event.preventDefault()
      this.callbacks.onToggleMagnet?.()
      return
    }
    const tool = TOOL_HOTKEYS.get(key)
    if (tool !== undefined) {
      event.preventDefault()
      this.callbacks.onToolHotkey?.(tool)
    }
  }
}
