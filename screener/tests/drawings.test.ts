import { describe, expect, it } from "vitest"

import { parseDrawingsPayload, pointsRequired, serializeDrawings } from "../src/lib/chart/drawings/types"

describe("price range drawings", () => {
  it("uses the generic two-anchor interaction contract", () => {
    expect(pointsRequired("priceRange")).toBe(2)
  })

  it("survives persisted payload parsing and serialization", () => {
    const saved = JSON.stringify({
      version: 1,
      drawings: [
        {
          id: "range-1",
          tool: "priceRange",
          points: [
            { time: 1_700_000_000, price: 100 },
            { time: 1_700_003_600, price: 105 },
          ],
        },
      ],
    })

    const parsed = parseDrawingsPayload(saved)

    expect(parsed.foreign).toEqual([])
    expect(parsed.drawings).toEqual([
      {
        id: "range-1",
        tool: "priceRange",
        points: [
          { time: 1_700_000_000, price: 100 },
          { time: 1_700_003_600, price: 105 },
        ],
      },
    ])
    expect(parseDrawingsPayload(serializeDrawings(parsed.drawings)).drawings).toEqual(parsed.drawings)
  })
})
