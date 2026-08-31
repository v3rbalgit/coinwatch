import type { Bar } from "$lib/chart/types"

/** Backend trust state for one candle series. */
export type CandleHealth = "loading" | "ready" | "stale" | "verifying" | "repairing"

/** Cached chart bars with the health and revision that produced them. */
export type CandleSnapshot = {
  candles: Bar[]
  health: CandleHealth
  revision: number
}
