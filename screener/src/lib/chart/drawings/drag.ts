import type { UTCTimestamp } from "lightweight-charts"

import type { DrawingPoint } from "./types"

export type DrawingDragTarget = "body" | number

function shift(time: UTCTimestamp, delta: number): UTCTimestamp {
  return (time + delta) as UTCTimestamp
}

/** Apply one incremental body or handle drag to immutable drawing anchors. */
export function moveDrawingPoints(
  points: readonly DrawingPoint[],
  target: DrawingDragTarget,
  previous: DrawingPoint,
  next: DrawingPoint
): DrawingPoint[] {
  if (target !== "body") return points.map((point, index) => (index === target ? next : point))
  const timeDelta = next.time - previous.time
  const priceDelta = next.price - previous.price
  return points.map(point => ({ time: shift(point.time, timeDelta), price: point.price + priceDelta }))
}
