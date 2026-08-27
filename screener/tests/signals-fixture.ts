import type { SignalDefinition } from "$lib/signals/definition"

/** Minimal valid definition: 1h close above 100, evaluated on the latest bar. */
export function thresholdDefinition(): SignalDefinition {
  return {
    version: 3,
    direction: "bullish",
    root: { type: "condition", id: "price" },
    conditions: {
      price: {
        timeframe: "1h",
        predicate: {
          type: "threshold",
          left: { type: "field", field: "close" },
          op: "greaterThan",
          right: { type: "constant", value: 100 },
        },
        withinBars: 0,
      },
    },
  }
}
