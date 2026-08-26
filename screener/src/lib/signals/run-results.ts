import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { SignalDefinition, SignalNode } from "$lib/signals/definition"
import { conditionLabel } from "$lib/signals/format"
import type { ConditionOutcome, EvalReport } from "$lib/signals/run"

export type SignalRunResultFilter = "matches" | "all" | "issues"

export interface SignalRunConditionEvidence {
  id: string
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
  return orderedConditionIds(definition).map(id => ({
    id,
    label: conditionLabel(definition.conditions[id], descriptors),
    outcome: report.conditions[id] ?? null,
  }))
}

export function reportHasDataIssues(report: EvalReport): boolean {
  if (report.missingData.length > 0 || Object.keys(report.unavailableData).length > 0) return true
  return Object.values(report.conditions).some(
    outcome =>
      outcome.status === "noData" || outcome.status === "unavailable" || outcome.status === "insufficientHistory"
  )
}

export function filterSignalRunReports(reports: readonly EvalReport[], filter: SignalRunResultFilter): EvalReport[] {
  switch (filter) {
    case "matches":
      return reports.filter(report => report.satisfied)
    case "issues":
      return reports.filter(reportHasDataIssues)
    case "all":
      return [...reports]
  }
}
