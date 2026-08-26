import { describe, expect, it } from "vitest"

import { formatCompactNumber, formatPrice } from "$lib/format"

describe("market number formatting", () => {
  it("uses stable compact suffixes", () => {
    expect(formatCompactNumber(999)).toBe("999")
    expect(formatCompactNumber(1_234)).toBe("1k")
    expect(formatCompactNumber(1_250_000)).toBe("1.3M")
    expect(formatCompactNumber(1_234_000_000)).toBe("1.23B")
  })

  it("adjusts price precision by magnitude", () => {
    expect(formatPrice(123.456)).toBe("123.46")
    expect(formatPrice(12.3456)).toBe("12.346")
    expect(formatPrice(0.012345)).toBe("0.01235")
  })
})
