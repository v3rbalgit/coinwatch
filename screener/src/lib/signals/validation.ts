import type { IndicatorDescriptor, ValueDomain } from "$lib/chart/indicators"
import type { Operand, OperandAdjustment } from "$lib/signals/definition"
import { findConditionIdIssue } from "$lib/signals/draft"
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
  if (operand.adjustment?.type === "percent" && operand.adjustment.value <= -100) {
    return "Percent adjustments must stay above -100."
  }
  if (operand.type !== "indicator") return null
  const issue = instanceValidationMessage({
    id: operand.instance.id,
    params: operand.instance.params,
    expectedKind: "series",
    output: operand.output,
    descriptors,
  })
  if (issue !== null) return issue
  const descriptor = descriptors.find(candidate => candidate.id === operand.instance.id)
  return descriptor?.supportsSignals === false
    ? `${descriptor.label} cannot be used in signals until it has stable history anchoring.`
    : null
}

function effectiveAdjustment(adjustment: OperandAdjustment | undefined): OperandAdjustment | undefined {
  return adjustment?.value === 0 ? undefined : adjustment
}

function adjustmentsEqual(left: OperandAdjustment | undefined, right: OperandAdjustment | undefined): boolean {
  const effectiveLeft = effectiveAdjustment(left)
  const effectiveRight = effectiveAdjustment(right)
  if (effectiveLeft === undefined || effectiveRight === undefined) return effectiveLeft === effectiveRight
  return effectiveLeft.type === effectiveRight.type && effectiveLeft.value === effectiveRight.value
}

function paramsEquivalent(
  id: string,
  left: Readonly<Record<string, number>>,
  right: Readonly<Record<string, number>>,
  descriptors: readonly IndicatorDescriptor[]
): boolean {
  const descriptor = descriptors.find(candidate => candidate.id === id)
  if (descriptor === undefined) return false
  return descriptor.params.every(parameter => {
    const leftValue = left[parameter.key] ?? parameter.default
    const rightValue = right[parameter.key] ?? parameter.default
    return leftValue === rightValue
  })
}

export function operandsEquivalent(
  left: Operand,
  right: Operand,
  descriptors: readonly IndicatorDescriptor[]
): boolean {
  if (left.type !== right.type) return false
  switch (left.type) {
    case "constant":
      return right.type === "constant" && left.value === right.value
    case "field":
      return right.type === "field" && left.field === right.field && adjustmentsEqual(left.adjustment, right.adjustment)
    case "indicator":
      return (
        right.type === "indicator" &&
        left.instance.id === right.instance.id &&
        paramsEquivalent(left.instance.id, left.instance.params, right.instance.params, descriptors) &&
        left.output === right.output &&
        adjustmentsEqual(left.adjustment, right.adjustment)
      )
  }
}

export function operandDomain(operand: Operand, descriptors: readonly IndicatorDescriptor[]): ValueDomain | null {
  switch (operand.type) {
    case "constant":
      return null
    case "field":
      return operand.field === "volume" ? "volume" : "price"
    case "indicator":
      return descriptors.find(descriptor => descriptor.id === operand.instance.id)?.outputDomain ?? null
  }
}

function applyAdjustment(adjustment: OperandAdjustment, value: number): number {
  return adjustment.type === "absolute" ? value + adjustment.value : value * (1 + adjustment.value / 100)
}

/** Post-adjustment range; adjustments are monotone (percent > -100), so endpoints map in order. */
function operandRange(operand: Operand, descriptors: readonly IndicatorDescriptor[]): [number, number] | null {
  if (operand.type !== "indicator") return null
  const range = descriptors.find(descriptor => descriptor.id === operand.instance.id)?.range
  if (range === undefined || range === null) return null
  const adjustment = operand.adjustment
  if (adjustment === undefined) return [range[0], range[1]]
  return [applyAdjustment(adjustment, range[0]), applyAdjustment(adjustment, range[1])]
}

function domainLabel(domain: ValueDomain): string {
  return {
    price: "price",
    priceDelta: "price-change",
    volume: "volume",
    boundedOscillator: "bounded-oscillator",
    unboundedOscillator: "unbounded-oscillator",
  }[domain]
}

export function draftValidationMessage(draft: SignalDraft, descriptors: readonly IndicatorDescriptor[]): string | null {
  if (draft.conditions.length === 0) return "Add at least one condition before saving."
  const idIssue = findConditionIdIssue(draft.conditions)
  if (idIssue !== null) return `Condition ${idIssue.index + 1} needs a unique stable id.`
  for (const [index, condition] of draft.conditions.entries()) {
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
      const descriptor = descriptors.find(candidate => candidate.id === predicate.instance.id)
      if (descriptor?.supportsSignals === false) {
        return `Condition ${index + 1}: ${descriptor.label} cannot be used in signals until it has stable history anchoring.`
      }
      continue
    }
    if (predicate.left.type === "constant" && predicate.right.type === "constant") {
      return `Condition ${index + 1} needs at least one market or indicator source.`
    }
    const leftIssue = operandValidationMessage(predicate.left, descriptors)
    if (leftIssue !== null) return `Condition ${index + 1}: ${leftIssue}`
    const rightIssue = operandValidationMessage(predicate.right, descriptors)
    if (rightIssue !== null) return `Condition ${index + 1}: ${rightIssue}`
    if (operandsEquivalent(predicate.left, predicate.right, descriptors)) {
      return `Condition ${index + 1} uses the same source on both sides.`
    }
    const leftDomain = operandDomain(predicate.left, descriptors)
    const rightDomain = operandDomain(predicate.right, descriptors)
    if (leftDomain !== null && rightDomain !== null && leftDomain !== rightDomain) {
      return `Condition ${index + 1} compares incompatible ${domainLabel(leftDomain)} and ${domainLabel(rightDomain)} sources.`
    }
    const leftRange = operandRange(predicate.left, descriptors)
    const rightRange = operandRange(predicate.right, descriptors)
    if (leftRange !== null && rightRange !== null && (leftRange[1] <= rightRange[0] || rightRange[1] <= leftRange[0])) {
      return `Condition ${index + 1} compares sources whose value ranges never overlap.`
    }
  }
  return null
}

export function draftCanSave(name: string, draft: SignalDraft, descriptors: readonly IndicatorDescriptor[]): boolean {
  return draftSnapshot(name, draft) !== null && draftValidationMessage(draft, descriptors) === null
}
