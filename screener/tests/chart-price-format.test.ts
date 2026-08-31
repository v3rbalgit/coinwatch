import { describe, expect, it } from "vitest"

import { DEFAULT_PRICE_DECIMALS, priceFormat } from "$lib/chart/price-format"

describe("chart price format", () => {
  it("uses fine-grained metadata for small-denomination markets", () => {
    expect(priceFormat(6)).toEqual({ type: "price", precision: 6, base: 1_000_000, minMove: 0.000_001 })
  })

  it.each([0, 1, 2])("preserves the existing two-decimal display floor for %i", requested => {
    expect(priceFormat(requested)).toEqual({ type: "price", precision: 2, base: 100, minMove: 0.01 })
  })

  it("falls back safely for absent or invalid metadata", () => {
    const fallback = {
      type: "price",
      precision: DEFAULT_PRICE_DECIMALS,
      base: 100,
      minMove: 0.01,
    }
    expect(priceFormat(undefined)).toEqual(fallback)
    expect(priceFormat(Number.NaN)).toEqual(fallback)
    expect(priceFormat(-3)).toEqual(fallback)
  })
})
