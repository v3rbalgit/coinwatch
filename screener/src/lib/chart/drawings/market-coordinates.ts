import type { IChartApi, ISeriesApi, UTCTimestamp } from "lightweight-charts"

import { timeAtLogical, timeAtLogicalExact, timeGridFromData } from "./coords"
import { nearestValueWithin, type Px } from "./geometry"
import type { DrawingPoint } from "./types"

/** Pixel/market conversion shared by placement, dragging, and the snap marker. */
export class DrawingMarketCoordinates {
  private static readonly MAGNET_TOLERANCE = 8

  constructor(
    private chart: IChartApi,
    private series: ISeriesApi<"Candlestick">
  ) {}

  /** Drawings live on the price pane only — indicator panes are excluded. */
  inMainPane(px: Px): boolean {
    return px.y <= this.chart.paneSize(0).height
  }

  toMarket(px: Px, magnet: boolean): DrawingPoint | null {
    const price = this.series.coordinateToPrice(px.y)
    if (price === null) return null
    const time = this.timeAt(px)
    if (time === null) return null
    return { time, price: magnet ? (this.snapTarget(time, px.y) ?? price) : price }
  }

  /** Freehand time remains fractional between bars so brush strokes stay smooth. */
  toMarketSmooth(px: Px, magnet: boolean): DrawingPoint | null {
    const point = this.toMarket(px, magnet)
    if (point === null) return null
    const logical = this.chart.timeScale().coordinateToLogical(px.x)
    const grid = timeGridFromData(this.series.data())
    if (logical === null || grid === null) return point
    return { time: timeAtLogicalExact(logical, grid), price: point.price }
  }

  snapIndicator(px: Px, magnet: boolean): DrawingPoint | null {
    if (!magnet || !this.inMainPane(px)) return null
    const time = this.timeAt(px)
    if (time === null) return null
    const price = this.snapTarget(time, px.y)
    return price === null ? null : { time, price }
  }

  /** `coordinateToTime` cannot resolve chart whitespace, so extrapolate its bar grid. */
  private timeAt(px: Px): UTCTimestamp | null {
    const timeScale = this.chart.timeScale()
    const time = timeScale.coordinateToTime(px.x)
    if (typeof time === "number") return time
    const logical = timeScale.coordinateToLogical(px.x)
    const grid = timeGridFromData(this.series.data())
    if (logical === null || grid === null) return null
    return timeAtLogical(logical, grid)
  }

  private snapTarget(time: UTCTimestamp, cursorY: number): number | null {
    const bar = this.series.data().find(item => item.time === time)
    if (!bar || !("open" in bar)) return null
    const candidates = [bar.open, bar.high, bar.low, bar.close].flatMap(value => {
      const y = this.series.priceToCoordinate(value)
      return y === null ? [] : [{ value, y }]
    })
    return nearestValueWithin(cursorY, candidates, DrawingMarketCoordinates.MAGNET_TOLERANCE)
  }
}
