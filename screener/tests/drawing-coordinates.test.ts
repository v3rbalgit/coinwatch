import { describe, expect, it } from "vitest"

import { coordinateForLogical } from "$lib/chart/drawings/coords"

describe("chart drawing coordinates", () => {
  it("interpolates fractional times between integer logical coordinates", () => {
    const requested: number[] = []
    const coordinate = coordinateForLogical(322.5, index => {
      requested.push(index)
      return index * 10
    })

    expect(coordinate).toBe(3225)
    expect(requested).toEqual([322, 323])
  })

  it("uses exact integer coordinates and propagates unavailable endpoints", () => {
    expect(coordinateForLogical(12, index => index * 5)).toBe(60)
    expect(coordinateForLogical(12.5, index => (index === 13 ? null : index * 5))).toBeNull()
  })
})
