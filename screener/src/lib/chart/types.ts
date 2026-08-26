/** One OHLCV bar in chart-native shape (time is epoch seconds). */
export type Bar = { time: number; open: number; high: number; low: number; close: number; volume: number }

/** One non-persisted matched-state span projected onto the chart time axis. */
export interface SignalValidationMarking {
  index: number
  startTime: number
  endTime: number
  activeAtStart: boolean
  activeAtEnd: boolean
}
