import { describe, expect, it } from "vitest"

import { candleHealthMessage, candleSnapshotDecision, type CandleLoadRequest } from "../src/lib/chart/candle-snapshot"

const request: CandleLoadRequest = {
  seriesKey: "BTC|1h",
  sequence: 4,
  minimumRevision: 7,
}

describe("chart candle snapshots", () => {
  it("accepts only the current series request at a non-stale revision", () => {
    expect(candleSnapshotDecision(request, "BTC|1h", 4, 7, 7)).toBe("apply")
    expect(candleSnapshotDecision(request, "ETH|1h", 4, 7, 7)).toBe("ignore")
    expect(candleSnapshotDecision(request, "BTC|1h", 5, 7, 8)).toBe("ignore")
    expect(candleSnapshotDecision(request, "BTC|1h", 4, 7, 6)).toBe("retry")
    expect(candleSnapshotDecision(request, "BTC|1h", 4, 9, 8)).toBe("retry")
  })

  it("explains every reduced-trust health state", () => {
    expect(candleHealthMessage("ready")).toBe("")
    expect(candleHealthMessage("loading")).toContain("Loading")
    expect(candleHealthMessage("stale")).toContain("catching up")
    expect(candleHealthMessage("verifying")).toContain("Verifying")
    expect(candleHealthMessage("repairing")).toContain("Repairing")
  })
})
