import { describe, expect, it } from "vitest"

import type { SignalDefinition } from "$lib/signals/definition"
import type { EvalReport } from "$lib/signals/run"
import {
  conditionEvidence,
  filterSignalRunReports,
  orderedConditionIds,
  pendingCandleMessage,
  rankClosestSignalRunReports,
  reportHasDataIssues,
} from "$lib/signals/run-results"

const definition: SignalDefinition = {
  version: 3,
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
  overrides: Partial<Pick<EvalReport, "pendingData" | "unavailableData">> = {}
): EvalReport {
  const unavailable =
    Object.keys(overrides.pendingData ?? {}).length > 0 || Object.keys(overrides.unavailableData ?? {}).length > 0
  return {
    coin,
    verdict: unavailable ? "unknown" : satisfied ? "matched" : "notMatched",
    lastEvidenceAt: satisfied ? "2026-08-26T10:00:00Z" : null,
    conditions: {
      trend: { status: "notSatisfied", lastTrueBar: null, evidence: null },
      volume: { status: "satisfied", lastTrueBar: "2026-08-26T08:00:00Z", evidence: null },
      retained: { status: "notSatisfied", lastTrueBar: null, evidence: null },
    },
    pendingData: overrides.pendingData ?? {},
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
    const missing = report("SOL", false, { pendingData: { "1d": "repairing" } })
    const partialMatch = report("ARB", true, { pendingData: { "1d": "stale" } })
    partialMatch.verdict = "matched"
    const unavailable = report("HYPE", false, { unavailableData: { "4h": "provider unavailable" } })
    unavailable.conditions.volume = { status: "unavailable", lastTrueBar: null, evidence: null }
    const insufficient = report("DOGE", false)
    insufficient.conditions.trend = {
      status: "insufficientHistory",
      required: 20,
      available: 12,
      lastTrueBar: null,
      evidence: null,
    }
    const reports = [matched, plain, missing, partialMatch, unavailable, insufficient]

    expect(filterSignalRunReports(definition, reports, "matches").map(item => item.coin)).toEqual(["BTC", "ARB"])
    expect(filterSignalRunReports(definition, reports, "all")).toEqual(reports)
    expect(filterSignalRunReports(definition, reports, "issues").map(item => item.coin)).toEqual([
      "SOL",
      "ARB",
      "HYPE",
      "DOGE",
    ])
    expect(reportHasDataIssues(plain)).toBe(false)
    expect(reportHasDataIssues(insufficient)).toBe(true)
    expect(pendingCandleMessage("loading")).toContain("loading")
    expect(pendingCandleMessage("stale")).toContain("current candle")
    expect(pendingCandleMessage("verifying")).toContain("reconnect")
    expect(pendingCandleMessage("repairing")).toContain("structural")
  })

  it("ranks honest closest candidates by satisfied leaves and normalized threshold miss", () => {
    const flatDefinition: SignalDefinition = {
      ...definition,
      root: {
        type: "all",
        children: [
          { type: "condition", id: "trend" },
          { type: "condition", id: "volume" },
        ],
      },
    }
    const near = report("NEAR", false)
    near.conditions.trend = {
      status: "notSatisfied",
      lastTrueBar: null,
      evidence: { type: "state", observedAt: "2026-08-26T10:00:00Z", left: 95, right: 100 },
    }
    const far = report("FAR", false)
    far.conditions.trend = {
      status: "notSatisfied",
      lastTrueBar: null,
      evidence: { type: "state", observedAt: "2026-08-26T10:00:00Z", left: 60, right: 100 },
    }
    const noConditions = report("NONE", false)
    noConditions.conditions.volume = {
      status: "notSatisfied",
      lastTrueBar: null,
      evidence: { type: "state", observedAt: "2026-08-26T08:00:00Z", left: 900, right: 1000 },
    }
    const dataIssue = report("ISSUE", false, { pendingData: { "1h": "verifying" } })
    dataIssue.verdict = "notMatched"
    const matched = report("MATCH", true)

    expect(
      rankClosestSignalRunReports(flatDefinition, [far, noConditions, dataIssue, near, matched]).map(r => r.coin)
    ).toEqual(["NEAR", "FAR", "NONE"])
    expect(filterSignalRunReports(flatDefinition, [far, near], "closest").map(r => r.coin)).toEqual(["NEAR", "FAR"])
    expect(rankClosestSignalRunReports(definition, [near])).toEqual([])

    const eventDefinition: SignalDefinition = {
      version: 3,
      direction: "bullish",
      root: { type: "condition", id: "cross" },
      conditions: {
        cross: {
          timeframe: "1h",
          predicate: {
            type: "cross",
            left: { type: "field", field: "close" },
            right: { type: "constant", value: 100 },
            direction: "over",
          },
          withinBars: 0,
        },
      },
    }
    const eventOnly = report("EVENT", false)
    eventOnly.conditions = {
      cross: {
        status: "notSatisfied",
        lastTrueBar: null,
        evidence: {
          type: "cross",
          observedAt: "2026-08-26T10:00:00Z",
          previousLeft: 90,
          previousRight: 100,
          left: 95,
          right: 100,
        },
      },
    }
    expect(rankClosestSignalRunReports(eventDefinition, [eventOnly])).toEqual([])
  })
})
