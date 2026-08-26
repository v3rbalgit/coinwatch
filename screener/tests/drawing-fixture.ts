import { parseDrawingsPayload, type Drawing } from "$lib/chart/drawings/types"

export function drawingFixture(): Drawing {
  const payload = parseDrawingsPayload(
    JSON.stringify({
      version: 1,
      drawings: [
        {
          id: "fixture",
          tool: "brush",
          points: [
            { time: 1_700_000_000, price: 100 },
            { time: 1_700_003_600, price: 105 },
            { time: 1_700_007_200, price: 110 },
          ],
        },
      ],
    })
  )
  const drawing = payload.drawings[0]
  if (drawing === undefined) throw new Error("drawing fixture did not decode")
  return drawing
}
