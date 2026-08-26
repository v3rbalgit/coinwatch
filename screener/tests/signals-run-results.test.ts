import { describe, expect, it } from "vitest"

import type { SignalDefinition } from "$lib/signals/definition"
import type { EvalReport } from "$lib/signals/run"
import {
  conditionEvidence,
  filterSignalRunReports,
  orderedConditionIds,
  reportHasDataIssues,
} from "$lib/signals/run-results"

const definition: SignalDefinition = {
  version: 2,
  direction: "bullish",
  root: {
    type: "all",
    children: [
      { type: "condition", id: "volume" },
      { type: "not", child: { type: "condition", id: "trend" } },
    ],
  },
  conditions: {
    trend: {
      timeframe: "1h",
      predicate: {
        type: "threshold",
        left: { type: "field", field: "close" },
        op: "greaterThan",
        right: { type: "constant", value: 100 },
      },
      withinBars: 0,
    },
    volume: {
      timeframe: "4h",
      predicate: {
        type: "threshold",
        left: { type: "field", field: "volume" },
        op: "greaterOrEqual",
        right: { type: "constant", value: 1000 },
      },
      withinBars: 2,
    },
    retained: {
      timeframe: "1d",
      predicate: {
        type: "threshold",
        left: { type: "field", field: "close" },
        op: "lessThan",
        right: { type: "constant", value: 80 },
      },
      withinBars: 0,
    },
  },
}

function report(
  coin: string,
  satisfied: boolean,
  overrides: Partial<Pick<EvalReport, "missingData" | "unavailableData">> = {}
): EvalReport {
  return {
    coin,
    satisfied,
    triggeredAt: satisfied ? "2026-08-26T10:00:00Z" : null,
    conditions: {
      trend: { status: "notSatisfied", lastTrueBar: null },
      volume: { status: "satisfied", lastTrueBar: "2026-08-26T08:00:00Z" },
      retained: { status: "notSatisfied", lastTrueBar: null },
    },
    missingData: overrides.missingData ?? [],
    unavailableData: overrides.unavailableData ?? {},
  }
}

describe("signal run result helpers", () => {
  it("orders evidence by the definition tree and retains unreferenced conditions", () => {
    expect(orderedConditionIds(definition)).toEqual(["volume", "trend", "retained"])
    const evidence = conditionEvidence(definition, report("BTC", true), [])
    expect(evidence.map(item => item.id)).toEqual(["volume", "trend", "retained"])
    expect(evidence[0]?.label).toContain("Volume")
    expect(evidence[0]?.outcome?.status).toBe("satisfied")
  })

  it("filters matches, all reports, and every explicit data issue", () => {
    const matched = report("BTC", true)
    const plain = report("ETH", false)
    const missing = report("SOL", false, { missingData: ["1d"] })
    const unavailable = report("HYPE", false, { unavailableData: { "4h": "provider unavailable" } })
    unavailable.conditions.volume = { status: "unavailable", lastTrueBar: null }
    const insufficient = report("DOGE", false)
    insufficient.conditions.trend = {
      status: "insufficientHistory",
      required: 20,
      available: 12,
      lastTrueBar: null,
    }
    const reports = [matched, plain, missing, unavailable, insufficient]

    expect(filterSignalRunReports(reports, "matches").map(item => item.coin)).toEqual(["BTC"])
    expect(filterSignalRunReports(reports, "all")).toEqual(reports)
    expect(filterSignalRunReports(reports, "issues").map(item => item.coin)).toEqual(["SOL", "HYPE", "DOGE"])
    expect(reportHasDataIssues(plain)).toBe(false)
    expect(reportHasDataIssues(insufficient)).toBe(true)
  })
})
