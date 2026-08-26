import type { Px } from "./geometry"
import { newDrawing, pointsRequired, type Drawing, type DrawingPoint, type InteractiveDrawingTool } from "./types"

/** Pixels between press and release that turn a two-anchor placement into drag-to-draw. */
const DRAG_DRAW_THRESHOLD = 8
/** Minimum pixel travel between recorded brush samples. */
const BRUSH_MIN_STEP = 3

interface PlacementCallbacks {
  toMarket: (px: Px) => DrawingPoint | null
  toMarketSmooth: (px: Px) => DrawingPoint | null
  addDrawing: (drawing: Drawing, select: boolean) => void
  setPreview: (drawing: Drawing | null) => void
  onToolDone: () => void
}

function distance(a: Px, b: Px): number {
  return Math.hypot(a.x - b.x, a.y - b.y)
}

/** Owns armed-tool placement state independently of DOM pointer routing. */
export class DrawingPlacement {
  private activeTool: InteractiveDrawingTool | null = null
  private pending: DrawingPoint[] = []
  private pendingStartPx: Px | null = null
  private brushing: DrawingPoint[] | null = null
  private lastBrushPx: Px | null = null

  constructor(private callbacks: PlacementCallbacks) {}

  get tool(): InteractiveDrawingTool | null {
    return this.activeTool
  }

  get inFlight(): boolean {
    return this.brushing !== null || this.pending.length > 0
  }

  setTool(tool: InteractiveDrawingTool | null): void {
    this.activeTool = tool
    this.resetGesture()
  }

  /** Returns whether a market anchor was accepted and should own the pointer event. */
  pointerDown(px: Px): boolean {
    const tool = this.activeTool
    if (tool === null) return false
    if (tool === "brush") return this.startBrush(px)
    return this.placePoint(tool, px)
  }

  /** Returns true while placement owns pointer movement, even if conversion fails. */
  pointerMove(px: Px): boolean {
    if (this.brushing !== null && this.lastBrushPx !== null) {
      if (distance(px, this.lastBrushPx) < BRUSH_MIN_STEP) return true
      const point = this.callbacks.toMarketSmooth(px)
      if (point === null) return true
      this.brushing.push(point)
      this.lastBrushPx = px
      this.callbacks.setPreview(newDrawing("brush", this.brushing))
      return true
    }

    if (this.activeTool !== null && this.pending.length > 0) {
      const point = this.callbacks.toMarket(px)
      if (point !== null) this.callbacks.setPreview(newDrawing(this.activeTool, [...this.pending, point]))
      return true
    }
    return false
  }

  /** Finalizes brush or drag-to-draw gestures; click-click placement completes on pointer down. */
  pointerUp(px: Px): boolean {
    if (this.brushing !== null) {
      if (this.brushing.length >= 2) this.callbacks.addDrawing(newDrawing("brush", this.brushing), false)
      this.brushing = null
      this.lastBrushPx = null
      this.callbacks.setPreview(null)
      return true
    }

    const tool = this.activeTool
    if (tool === null || pointsRequired(tool) !== 2 || this.pending.length !== 1 || this.pendingStartPx === null) {
      return false
    }
    if (distance(px, this.pendingStartPx) > DRAG_DRAW_THRESHOLD) {
      const first = this.pending[0]
      const point = this.callbacks.toMarket(px)
      if (first !== undefined && point !== null) {
        this.callbacks.addDrawing(newDrawing(tool, [first, point]), true)
        this.finishPlacement()
      }
    }
    return true
  }

  /** Escape disarms even an idle tool. */
  cancelTool(): boolean {
    if (this.activeTool === null) return false
    this.finishPlacement()
    return true
  }

  /** Pointer cancellation disarms only a gesture already in progress. */
  cancelGesture(): boolean {
    if (!this.inFlight) return false
    this.finishPlacement()
    return true
  }

  private startBrush(px: Px): boolean {
    const point = this.callbacks.toMarketSmooth(px)
    if (point === null) return false
    this.brushing = [point]
    this.lastBrushPx = px
    return true
  }

  private placePoint(tool: InteractiveDrawingTool, px: Px): boolean {
    const point = this.callbacks.toMarket(px)
    if (point === null) return false
    if (pointsRequired(tool) === 1) {
      this.callbacks.addDrawing(newDrawing(tool, [point]), true)
      this.finishPlacement()
      return true
    }

    if (this.pending.length === 0) this.pendingStartPx = px
    this.pending.push(point)
    if (this.pending.length >= pointsRequired(tool)) {
      this.callbacks.addDrawing(newDrawing(tool, this.pending), true)
      this.finishPlacement()
    } else {
      this.callbacks.setPreview(newDrawing(tool, [...this.pending, point]))
    }
    return true
  }

  private finishPlacement(): void {
    this.resetGesture()
    this.callbacks.onToolDone()
  }

  private resetGesture(): void {
    this.pending = []
    this.pendingStartPx = null
    this.brushing = null
    this.lastBrushPx = null
    this.callbacks.setPreview(null)
  }
}
