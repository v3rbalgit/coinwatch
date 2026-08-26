import type { Time, UTCTimestamp } from "lightweight-charts"

/**
 * Uniform time grid inferred from the loaded bars, for extrapolating beyond
 * them: `coordinateToTime`/`timeToCoordinate` are null outside the data range
 * (the right-offset whitespace, anchors in unloaded history), but bar index
 * space extends linearly in both directions.
 */
export interface TimeGrid {
  lastTime: UTCTimestamp
  lastIndex: number
  intervalSec: number
}

/**
 * Grid from series data — the single constructor both the renderer and the
 * interaction layer use, so they can never disagree about the bar grid. Walks
 * from the end (only the last two bars matter — no full scan on redraw paths)
 * and keeps the real array index, which is the logical space
 * `logicalToCoordinate` expects even if non-numeric times were skipped.
 */
export function timeGridFromData(data: readonly { time: Time }[]): TimeGrid | null {
  let lastTime: UTCTimestamp | undefined
  let lastIndex = -1
  let previousTime: UTCTimestamp | undefined
  for (let i = data.length - 1; i >= 0; i--) {
    const time = data[i]?.time
    if (typeof time !== "number") continue
    if (lastTime === undefined) {
      lastTime = time
      lastIndex = i
    } else {
      previousTime = time
      break
    }
  }
  if (lastTime === undefined || previousTime === undefined) return null
  const intervalSec = lastTime - previousTime
  if (intervalSec <= 0) return null
  return { lastTime, lastIndex, intervalSec }
}

export function timeGridOf(times: readonly UTCTimestamp[]): TimeGrid | null {
  const lastTime = times.at(-1)
  const previousTime = times.at(-2)
  if (lastTime === undefined || previousTime === undefined) return null
  const intervalSec = lastTime - previousTime
  if (intervalSec <= 0) return null
  return { lastTime, lastIndex: times.length - 1, intervalSec }
}

/** Fractional logical index for a time, extrapolated off either end of the grid. */
export function logicalFor(time: UTCTimestamp, grid: TimeGrid): number {
  return grid.lastIndex + (time - grid.lastTime) / grid.intervalSec
}

/** Bar-snapped time for a logical index, extrapolated off either end of the grid. */
export function timeAtLogical(logical: number, grid: TimeGrid): UTCTimestamp {
  return (grid.lastTime + (Math.round(logical) - grid.lastIndex) * grid.intervalSec) as UTCTimestamp
}

/**
 * Unrounded variant for freehand anchors (brush): fractional times land
 * between bars, so strokes stay smooth instead of quantizing to candle width.
 * Safe because drawing anchors are library-owned — never fed to the series.
 */
export function timeAtLogicalExact(logical: number, grid: TimeGrid): UTCTimestamp {
  return (grid.lastTime + (logical - grid.lastIndex) * grid.intervalSec) as UTCTimestamp
}
