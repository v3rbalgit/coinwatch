import { describe, expect, it } from "vitest"

import type { ComparisonOp, Predicate } from "$lib/signals/definition"
import {
  canonicalPredicate,
  crossShape,
  invertComparisonOp,
  invertCrossDirection,
  presentationValues,
  thresholdShape,
} from "$lib/signals/predicate-shape"

const inverseComparisons: readonly [ComparisonOp, ComparisonOp][] = [
  ["lessThan", "greaterThan"],
  ["lessOrEqual", "greaterOrEqual"],
  ["greaterThan", "lessThan"],
  ["greaterOrEqual", "lessOrEqual"],
]

describe("signal predicate presentation", () => {
  it.each(inverseComparisons)("inverts %s to %s", (source, expected) => {
    expect(invertComparisonOp(source)).toBe(expected)
    expect(invertComparisonOp(expected)).toBe(source)
  })

  it("puts a threshold source first without changing its relationship", () => {
    const predicate: Extract<Predicate, { type: "threshold" }> = {
      type: "threshold",
      left: { type: "constant", value: 30 },
      op: "lessThan",
      right: { type: "indicator", instance: { id: "rsi", params: { period: 14 } }, output: "rsi" },
    }

    expect(thresholdShape(predicate)).toEqual({
      subject: predicate.right,
      op: "greaterThan",
      reference: predicate.left,
      reversed: true,
    })
    expect(canonicalPredicate(predicate)).toEqual({
      type: "threshold",
      left: predicate.right,
      op: "greaterThan",
      right: predicate.left,
    })
    expect(presentationValues(true, 30, 54.2711)).toEqual({ subject: 54.2711, reference: 30 })
  })

  it("keeps source-to-source thresholds in their authored direction", () => {
    const predicate: Extract<Predicate, { type: "threshold" }> = {
      type: "threshold",
      left: { type: "field", field: "close" },
      op: "greaterThan",
      right: { type: "indicator", instance: { id: "ema", params: { period: 20 } }, output: "ema" },
    }

    expect(thresholdShape(predicate)).toEqual({
      subject: predicate.left,
      op: "greaterThan",
      reference: predicate.right,
      reversed: false,
    })
    expect(canonicalPredicate(predicate)).toEqual(predicate)
  })

  it("normalizes constant-first crosses by swapping and inverting direction", () => {
    const predicate: Extract<Predicate, { type: "cross" }> = {
      type: "cross",
      left: { type: "constant", value: 50 },
      right: { type: "field", field: "close" },
      direction: "over",
    }

    expect(invertCrossDirection("over")).toBe("under")
    expect(invertCrossDirection("under")).toBe("over")
    expect(crossShape(predicate)).toEqual({
      subject: predicate.right,
      direction: "under",
      reference: predicate.left,
      reversed: true,
    })
    expect(canonicalPredicate(predicate)).toEqual({
      type: "cross",
      left: predicate.right,
      right: predicate.left,
      direction: "under",
    })
  })

  it("leaves invalid constant-only predicates available for validation to reject", () => {
    const predicate: Predicate = {
      type: "threshold",
      left: { type: "constant", value: 1 },
      op: "lessThan",
      right: { type: "constant", value: 2 },
    }

    expect(thresholdShape(predicate)).toBeNull()
    expect(canonicalPredicate(predicate)).toBe(predicate)
  })
})
