import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { Operand } from "$lib/signals/definition"
import type { SignalDraft } from "$lib/signals/draft"
import { draftSnapshot } from "$lib/signals/editor"

function stepAligned(value: number, parameter: IndicatorDescriptor["params"][number]): boolean {
  const steps = (value - parameter.min) / parameter.step
  const nearest = Math.round(steps)
  const scale = Math.max(Math.abs(steps), Math.abs(nearest), 1)
  return Math.abs(steps - nearest) <= Number.EPSILON * scale
}

interface InstanceValidationOptions {
  id: string
  params: Readonly<Record<string, number>>
  expectedKind: IndicatorDescriptor["kind"]
  output?: string
  descriptors: readonly IndicatorDescriptor[]
}

function instanceValidationMessage(options: InstanceValidationOptions): string | null {
  const { id, params, expectedKind, output, descriptors } = options
  const descriptor = descriptors.find(candidate => candidate.id === id)
  if (descriptor?.kind !== expectedKind) return `Indicator '${id}' is unavailable.`
  if (output !== undefined && !descriptor.outputs.some(candidate => candidate.key === output)) {
    return `Indicator '${descriptor.label}' has no '${output}' output.`
  }
  const knownKeys = new Set(descriptor.params.map(parameter => parameter.key))
  for (const key of Object.keys(params)) {
    if (!knownKeys.has(key)) return `Indicator '${descriptor.label}' has no '${key}' parameter.`
  }
  for (const parameter of descriptor.params) {
    const value = params[parameter.key]
    if (value === undefined) return `Indicator '${descriptor.label}' is missing ${parameter.label}.`
    if (!Number.isFinite(value) || value < parameter.min || value > parameter.max) {
      return `${descriptor.label} ${parameter.label} must be between ${parameter.min} and ${parameter.max}.`
    }
    if (!stepAligned(value, parameter)) {
      return `${descriptor.label} ${parameter.label} must use increments of ${parameter.step}.`
    }
  }
  return null
}

function operandValidationMessage(operand: Operand, descriptors: readonly IndicatorDescriptor[]): string | null {
  if (operand.type === "constant") return Number.isFinite(operand.value) ? null : "Constants must be finite numbers."
  if (operand.adjustment !== undefined && !Number.isFinite(operand.adjustment.value)) {
    return "Source adjustments must be finite numbers."
  }
  return operand.type === "indicator"
    ? instanceValidationMessage({
        id: operand.instance.id,
        params: operand.instance.params,
        expectedKind: "series",
        output: operand.output,
        descriptors,
      })
    : null
}

export function draftValidationMessage(draft: SignalDraft, descriptors: readonly IndicatorDescriptor[]): string | null {
  if (draft.conditions.length === 0) return "Add at least one condition before saving."
  const conditionIds = new Set<string>()
  for (const [index, condition] of draft.conditions.entries()) {
    if (condition.id.length === 0 || conditionIds.has(condition.id)) {
      return `Condition ${index + 1} needs a unique stable id.`
    }
    conditionIds.add(condition.id)
    if (!Number.isInteger(condition.withinBars) || condition.withinBars < 0 || condition.withinBars > 10000) {
      return `Condition ${index + 1} freshness must be a whole number from 0 to 10000.`
    }
    const predicate = condition.predicate
    if (predicate.type === "divergence") {
      const issue = instanceValidationMessage({
        id: predicate.instance.id,
        params: predicate.instance.params,
        expectedKind: "divergence",
        descriptors,
      })
      if (issue !== null) return `Condition ${index + 1}: ${issue}`
      continue
    }
    if (predicate.left.type === "constant" && predicate.right.type === "constant") {
      return `Condition ${index + 1} needs at least one market or indicator source.`
    }
    const leftIssue = operandValidationMessage(predicate.left, descriptors)
    if (leftIssue !== null) return `Condition ${index + 1}: ${leftIssue}`
    const rightIssue = operandValidationMessage(predicate.right, descriptors)
    if (rightIssue !== null) return `Condition ${index + 1}: ${rightIssue}`
  }
  return null
}

export function draftCanSave(name: string, draft: SignalDraft, descriptors: readonly IndicatorDescriptor[]): boolean {
  return draftSnapshot(name, draft) !== null && draftValidationMessage(draft, descriptors) === null
}
