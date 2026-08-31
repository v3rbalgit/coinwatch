import type { Bar } from "./types"

/** Backend trust state for one chart candle snapshot. */
export type CandleHealth = "loading" | "ready" | "stale" | "verifying" | "repairing"

/** Cached chart bars with the health and revision that produced them. */
export type CandleSnapshot = {
  candles: Bar[]
  health: CandleHealth
  revision: number
}

/** Identity captured when an asynchronous candle load begins. */
export type CandleLoadRequest = {
  seriesKey: string
  sequence: number
  minimumRevision: number
}

/** User-facing explanation for non-ready chart data. */
export function candleHealthMessage(health: CandleHealth): string {
  switch (health) {
    case "loading":
      return "Loading chart candles…"
    case "stale":
      return "Chart data is catching up. Last-known candles are shown."
    case "verifying":
      return "Verifying candle continuity after reconnect. Last-known candles are shown."
    case "repairing":
      return "Repairing candle history. Last-known candles are shown."
    case "ready":
      return ""
  }
}

export type CandleSnapshotDecision = "apply" | "ignore" | "retry"

/** Decide whether a response is current, superseded, or must be re-read after a live race. */
export function candleSnapshotDecision(
  request: CandleLoadRequest,
  currentSeriesKey: string,
  currentSequence: number,
  currentRevision: number,
  snapshotRevision: number
): CandleSnapshotDecision {
  if (request.seriesKey !== currentSeriesKey || request.sequence !== currentSequence) return "ignore"
  if (snapshotRevision < request.minimumRevision || snapshotRevision < currentRevision) return "retry"
  return "apply"
}
