import type {
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  ISeriesPrimitive,
  PrimitivePaneViewZOrder,
  SeriesAttachedParameter,
  UTCTimestamp,
} from "lightweight-charts"

import { coordinateForTime, timeGridFromData, type TimeGrid } from "./drawings/coords"
import { chartTheme, withAlpha } from "./theme"

type CanvasTarget = Parameters<IPrimitivePaneRenderer["draw"]>[0]

export interface ValidationOverlaySpan {
  index: number
  startTime: UTCTimestamp
  endTime: UTCTimestamp
  activeAtStart: boolean
  activeAtEnd: boolean
}

interface ResolvedSpan {
  span: ValidationOverlaySpan
  startX: number
  endX: number
}

/** Non-persisted matched-state bands and onset markers for one validation result. */
export class SignalValidationPrimitive implements ISeriesPrimitive {
  private param: SeriesAttachedParameter | null = null
  private spans: readonly ValidationOverlaySpan[] = []
  private selectedIndex: number | null = null
  private resolved: ResolvedSpan[] = []
  private grid: TimeGrid | null = null
  private readonly view: IPrimitivePaneView
  private readonly views: IPrimitivePaneView[]

  constructor() {
    const renderer: IPrimitivePaneRenderer = {
      draw: (target: CanvasTarget) => {
        target.useMediaCoordinateSpace(scope => {
          const { context: ctx } = scope
          const height = scope.mediaSize.height
          for (const item of this.resolved) {
            const selected = item.span.index === this.selectedIndex
            const left = Math.min(item.startX, item.endX)
            const right = Math.max(item.startX, item.endX)
            const width = Math.max(1, right - left)
            ctx.fillStyle = withAlpha(chartTheme.signal, selected ? 0.18 : 0.08)
            ctx.fillRect(left, 0, width, height)

            if (!item.span.activeAtStart) {
              ctx.strokeStyle = withAlpha(chartTheme.signal, selected ? 0.95 : 0.72)
              ctx.lineWidth = selected ? 2 : 1
              ctx.beginPath()
              ctx.moveTo(item.startX, 0)
              ctx.lineTo(item.startX, height)
              ctx.stroke()

              ctx.fillStyle = chartTheme.signal
              ctx.beginPath()
              ctx.moveTo(item.startX - 5, 0)
              ctx.lineTo(item.startX + 5, 0)
              ctx.lineTo(item.startX, 7)
              ctx.closePath()
              ctx.fill()
            }

            if (!item.span.activeAtEnd) {
              ctx.strokeStyle = withAlpha(chartTheme.signal, 0.38)
              ctx.lineWidth = 1
              ctx.beginPath()
              ctx.moveTo(item.endX, 0)
              ctx.lineTo(item.endX, height)
              ctx.stroke()
            }

            if (selected) {
              ctx.strokeStyle = withAlpha(chartTheme.signal, 0.85)
              ctx.lineWidth = 1
              ctx.strokeRect(left, 0.5, width, Math.max(0, height - 1))
            }
          }
        })
      },
    }
    this.view = {
      zOrder: (): PrimitivePaneViewZOrder => "bottom",
      renderer: () => renderer,
    }
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
    const param = this.param
    this.grid = timeGridFromData(param?.series.data() ?? [])
    if (param === null) {
      this.resolved = []
      return
    }
    this.resolved = this.spans.flatMap(span => {
      const startX = coordinateForTime(span.startTime, param, this.grid)
      const endX = coordinateForTime(span.endTime, param, this.grid)
      return startX === null || endX === null ? [] : [{ span, startX, endX }]
    })
  }

  setSpans(spans: readonly ValidationOverlaySpan[]): void {
    this.spans = spans
    this.updateAllViews()
    this.requestUpdate()
  }

  setSelected(index: number | null): void {
    this.selectedIndex = index
    this.requestUpdate()
  }

  hitTestAt(x: number, tolerance = 6): number | null {
    for (let index = this.resolved.length - 1; index >= 0; index--) {
      const item = this.resolved[index]
      if (!item) continue
      if (!item.span.activeAtStart && Math.abs(item.startX - x) <= tolerance) return item.span.index
    }
    return null
  }

  private requestUpdate(): void {
    this.param?.requestUpdate()
  }
}
