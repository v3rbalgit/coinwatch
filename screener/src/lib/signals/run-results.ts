import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { CandleHealth } from "$lib/ipc/candles"
import type { Condition, SignalDefinition, SignalNode } from "$lib/signals/definition"
import { conditionLabel, directConditionIds } from "$lib/signals/format"
import type { ConditionOutcome, EvalReport } from "$lib/signals/run"

export type SignalRunResultFilter = "matches" | "closest" | "all" | "issues"

export interface SignalRunConditionEvidence {
  id: string
  condition: Condition | null
  label: string
  outcome: ConditionOutcome | null
}

function appendConditionIds(node: SignalNode, ids: string[], seen: Set<string>): void {
  switch (node.type) {
    case "condition":
      if (!seen.has(node.id)) {
        seen.add(node.id)
        ids.push(node.id)
      }
      return
    case "not":
      appendConditionIds(node.child, ids, seen)
      return
    case "all":
    case "any":
    case "xor":
      for (const child of node.children) appendConditionIds(child, ids, seen)
  }
}

export function orderedConditionIds(definition: SignalDefinition): string[] {
  const ids: string[] = []
  const seen = new Set<string>()
  appendConditionIds(definition.root, ids, seen)
  for (const id of Object.keys(definition.conditions)) {
    if (!seen.has(id)) ids.push(id)
  }
  return ids
}

export function conditionEvidence(
  definition: SignalDefinition,
  report: EvalReport,
  descriptors: readonly IndicatorDescriptor[]
): SignalRunConditionEvidence[] {
  return orderedConditionIds(definition).map(id => {
    const condition = definition.conditions[id] ?? null
    return {
      id,
      condition,
      label: condition === null ? `Missing condition ${id}` : conditionLabel(condition, descriptors),
      outcome: report.conditions[id] ?? null,
    }
  })
}

export function pendingCandleMessage(health: CandleHealth): string {
  switch (health) {
    case "loading":
      return "Candle data is still loading."
    case "stale":
      return "Waiting for the current candle boundary."
    case "verifying":
      return "Verifying candle continuity after reconnect."
    case "repairing":
      return "Repairing a structural candle gap."
    case "ready":
      return "Candle data was unexpectedly marked ready."
  }
}

export function reportHasDataIssues(report: EvalReport): boolean {
  if (Object.keys(report.pendingData).length > 0 || Object.keys(report.unavailableData).length > 0) return true
  return Object.values(report.conditions).some(
    outcome =>
      outcome.status === "noData" ||
      outcome.status === "unavailable" ||
      outcome.status === "insufficientHistory" ||
      outcome.status === "undefinedOperands"
  )
}

function directPositiveConditionIds(definition: SignalDefinition): string[] {
  return directConditionIds(definition.root)?.ids ?? []
}

function normalizedThresholdDistance(
  definition: SignalDefinition,
  conditionId: string,
  report: EvalReport
): number | null {
  const condition = definition.conditions[conditionId]
  const outcome = report.conditions[conditionId]
  if (condition?.predicate.type !== "threshold" || outcome?.evidence?.type !== "state") return null
  const { left, right } = outcome.evidence
  const gap =
    condition.predicate.op === "lessThan" || condition.predicate.op === "lessOrEqual" ? left - right : right - left
  return Math.max(0, gap) / Math.max(Math.abs(right), 1)
}

interface CandidateScore {
  report: EvalReport
  satisfiedConditions: number
  thresholdDistance: number
  measuredThresholds: number
}

export function rankClosestSignalRunReports(
  definition: SignalDefinition,
  reports: readonly EvalReport[]
): EvalReport[] {
  const conditionIds = directPositiveConditionIds(definition)
  if (conditionIds.length === 0) return []
  const scores: CandidateScore[] = reports
    .filter(report => report.verdict === "notMatched" && !reportHasDataIssues(report))
    .map(report => {
      let satisfiedConditions = 0
      let thresholdDistance = 0
      let measuredThresholds = 0
      for (const id of conditionIds) {
        if (report.conditions[id]?.status === "satisfied") satisfiedConditions += 1
        const distance = normalizedThresholdDistance(definition, id, report)
        if (distance !== null) {
          thresholdDistance += distance
          measuredThresholds += 1
        }
      }
      return { report, satisfiedConditions, thresholdDistance, measuredThresholds }
    })
  const distinctSatisfiedCounts = new Set(scores.map(score => score.satisfiedConditions))
  const hasThresholdDistance = scores.some(score => score.measuredThresholds > 0)
  if (!hasThresholdDistance && distinctSatisfiedCounts.size < 2) return []

  scores.sort((left, right) => {
    const satisfied = right.satisfiedConditions - left.satisfiedConditions
    if (satisfied !== 0) return satisfied
    const leftDistance =
      left.measuredThresholds === 0 ? Number.POSITIVE_INFINITY : left.thresholdDistance / left.measuredThresholds
    const rightDistance =
      right.measuredThresholds === 0 ? Number.POSITIVE_INFINITY : right.thresholdDistance / right.measuredThresholds
    const distance = leftDistance - rightDistance
    return distance === 0 || Number.isNaN(distance) ? left.report.coin.localeCompare(right.report.coin) : distance
  })
  return scores.map(score => score.report)
}

export function filterSignalRunReports(
  definition: SignalDefinition,
  reports: readonly EvalReport[],
  filter: SignalRunResultFilter
): EvalReport[] {
  switch (filter) {
    case "matches":
      return reports.filter(report => report.verdict === "matched")
    case "closest":
      return rankClosestSignalRunReports(definition, reports).slice(0, 20)
    case "issues":
      return reports.filter(reportHasDataIssues)
    case "all":
      return [...reports]
  }
}
