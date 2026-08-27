/** The supported bar timeframes; source of truth is crates/domain/src/timeframe.rs. */
export const TIMEFRAMES = ["1h", "4h", "1d", "1w"] as const

export type Timeframe = (typeof TIMEFRAMES)[number]

export const TIMEFRAME_HOURS: Readonly<Record<Timeframe, number>> = {
  "1h": 1,
  "4h": 4,
  "1d": 24,
  "1w": 168,
}
