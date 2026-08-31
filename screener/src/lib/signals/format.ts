import type { IndicatorDescriptor, IndicatorInstance } from "$lib/chart/indicators"
import type {
  Condition,
  Operand,
  OperandAdjustment,
  Predicate,
  SignalDefinition,
  SignalNode,
} from "$lib/signals/definition"
import { crossShape, presentationValues, thresholdShape } from "$lib/signals/predicate-shape"
import { TIMEFRAME_HOURS } from "$lib/timeframes"

export interface DefinitionLine {
  depth: number
  label: string
}

export interface DefinitionPreview {
  heading: string
  lines: string[]
}

function formatNumber(value: number): string {
  return Number.isInteger(value) ? value.toFixed(0) : String(value)
}

function adjustmentLabel(adjustment: OperandAdjustment): string {
  const value = formatNumber(adjustment.value)
  const sign = adjustment.value >= 0 ? "+" : ""
  return adjustment.type === "percent" ? `${sign}${value}%` : `${sign}${value}`
}

function descriptorFor(
  descriptors: readonly IndicatorDescriptor[],
  instance: IndicatorInstance
): IndicatorDescriptor | undefined {
  return descriptors.find(descriptor => descriptor.id === instance.id)
}

function instanceLabel(instance: IndicatorInstance, descriptors: readonly IndicatorDescriptor[]): string {
  const descriptor = descriptorFor(descriptors, instance)
  const label = descriptor?.label ?? instance.id
  const params = descriptor?.params.map(parameter => {
    const value = instance.params[parameter.key] ?? parameter.default
    return `${parameter.label} ${formatNumber(value)}`
  })
  return params === undefined || params.length === 0 ? label : `${label} (${params.join(", ")})`
}

export function operandLabel(operand: Operand, descriptors: readonly IndicatorDescriptor[]): string {
  switch (operand.type) {
    case "indicator": {
      const descriptor = descriptorFor(descriptors, operand.instance)
      const output = descriptor?.outputs.find(candidate => candidate.key === operand.output)
      const outputSuffix = descriptor?.outputs.length === 1 ? "" : ` · ${output?.label ?? operand.output}`
      const adjustment = operand.adjustment === undefined ? "" : ` ${adjustmentLabel(operand.adjustment)}`
      return `${instanceLabel(operand.instance, descriptors)}${outputSuffix}${adjustment}`
    }
    case "field": {
      const label = operand.field.charAt(0).toUpperCase() + operand.field.slice(1)
      return operand.adjustment === undefined ? label : `${label} ${adjustmentLabel(operand.adjustment)}`
    }
    case "constant":
      return formatNumber(operand.value)
  }
}

function comparisonSymbol(op: Extract<Predicate, { type: "threshold" }>["op"]): string {
  return {
    lessThan: "<",
    lessOrEqual: "≤",
    greaterThan: ">",
    greaterOrEqual: "≥",
  }[op]
}

function comparisonPhrase(op: Extract<Predicate, { type: "threshold" }>["op"]): string {
  return {
    lessThan: "is below",
    lessOrEqual: "is at or below",
    greaterThan: "is above",
    greaterOrEqual: "is at or above",
  }[op]
}

function crossPhrase(direction: Extract<Predicate, { type: "cross" }>["direction"]): string {
  return direction === "over" ? "crosses above" : "crosses below"
}

function crossedPhrase(direction: Extract<Predicate, { type: "cross" }>["direction"]): string {
  return direction === "over" ? "crossed above" : "crossed below"
}

export function predicateLabel(predicate: Predicate, descriptors: readonly IndicatorDescriptor[]): string {
  switch (predicate.type) {
    case "threshold": {
      const shape = thresholdShape(predicate)
      if (shape === null) {
        return `${operandLabel(predicate.left, descriptors)} ${comparisonSymbol(predicate.op)} ${operandLabel(predicate.right, descriptors)}`
      }
      return `${operandLabel(shape.subject, descriptors)} ${comparisonSymbol(shape.op)} ${operandLabel(shape.reference, descriptors)}`
    }
    case "cross": {
      const shape = crossShape(predicate)
      if (shape === null) {
        return `${operandLabel(predicate.left, descriptors)} ${crossPhrase(predicate.direction)} ${operandLabel(predicate.right, descriptors)}`
      }
      return `${operandLabel(shape.subject, descriptors)} ${crossPhrase(shape.direction)} ${operandLabel(shape.reference, descriptors)}`
    }
    case "divergence": {
      const kind = predicate.kind === "any" ? "Any" : predicate.kind === "regular" ? "Regular" : "Hidden"
      const direction =
        predicate.direction === "any" ? "bullish or bearish" : predicate.direction === "bullish" ? "bullish" : "bearish"
      return `${kind} ${direction} ${instanceLabel(predicate.instance, descriptors)}`
    }
  }
}

export function formatObservedValue(value: number): string {
  return Number(value.toPrecision(6)).toLocaleString(undefined, { maximumSignificantDigits: 6 })
}

function evidenceOperandLabel(
  operand: Operand,
  descriptors: readonly IndicatorDescriptor[],
  constantLabel: string
): string {
  return operand.type === "constant" ? constantLabel : operandLabel(operand, descriptors)
}

export function thresholdEvidenceLabel(
  predicate: Extract<Predicate, { type: "threshold" }>,
  left: number,
  right: number,
  descriptors: readonly IndicatorDescriptor[]
): string {
  const shape = thresholdShape(predicate)
  if (shape === null) return `Observed ${formatObservedValue(left)} · ${formatObservedValue(right)}`
  const values = presentationValues(shape.reversed, left, right)
  const subject = operandLabel(shape.subject, descriptors)
  const reference = evidenceOperandLabel(shape.reference, descriptors, "Threshold")
  return `Observed ${subject}: ${formatObservedValue(values.subject)} · ${reference}: ${formatObservedValue(values.reference)}`
}

export function crossEvidenceLabel(
  predicate: Extract<Predicate, { type: "cross" }>,
  previousLeft: number,
  previousRight: number,
  left: number,
  right: number,
  descriptors: readonly IndicatorDescriptor[]
): string {
  const shape = crossShape(predicate)
  if (shape === null) {
    return `Previous ${formatObservedValue(previousLeft)} · ${formatObservedValue(previousRight)} → current ${formatObservedValue(left)} · ${formatObservedValue(right)}`
  }
  const previous = presentationValues(shape.reversed, previousLeft, previousRight)
  const current = presentationValues(shape.reversed, left, right)
  const subject = operandLabel(shape.subject, descriptors)
  const reference = evidenceOperandLabel(shape.reference, descriptors, "Threshold")
  return `Previous ${subject}: ${formatObservedValue(previous.subject)} · ${reference}: ${formatObservedValue(previous.reference)} → current ${subject}: ${formatObservedValue(current.subject)} · ${reference}: ${formatObservedValue(current.reference)}`
}

export function recencyDurationLabel(timeframe: Condition["timeframe"], bars: number): string {
  const hours = TIMEFRAME_HOURS[timeframe] * bars
  if (hours % 168 === 0) {
    const weeks = hours / 168
    return `${weeks} ${weeks === 1 ? "week" : "weeks"}`
  }
  if (hours % 24 === 0) {
    const days = hours / 24
    return `${days} ${days === 1 ? "day" : "days"}`
  }
  return `${hours} ${hours === 1 ? "hour" : "hours"}`
}

function timingLabel(condition: Condition): string {
  if (condition.withinBars === 0) return "latest completed candle"
  const age = `${condition.withinBars} completed ${condition.withinBars === 1 ? "bar" : "bars"} ago (~${recencyDurationLabel(condition.timeframe, condition.withinBars)})`
  return condition.predicate.type === "threshold" ? `true now or up to ${age}` : `confirmed up to ${age}`
}

export function conditionLabel(condition: Condition, descriptors: readonly IndicatorDescriptor[]): string {
  return `${condition.timeframe} · ${predicateLabel(condition.predicate, descriptors)} · ${timingLabel(condition)}`
}

export function conditionSentence(condition: Condition, descriptors: readonly IndicatorDescriptor[]): string {
  const predicate = condition.predicate
  if (predicate.type === "threshold") {
    const shape = thresholdShape(predicate)
    const relationship =
      shape === null
        ? `${operandLabel(predicate.left, descriptors)} ${comparisonPhrase(predicate.op)} ${operandLabel(predicate.right, descriptors)}`
        : `${operandLabel(shape.subject, descriptors)} ${comparisonPhrase(shape.op)} ${operandLabel(shape.reference, descriptors)}`
    return condition.withinBars === 0
      ? `Latest completed ${condition.timeframe} candle: ${relationship}.`
      : `On ${condition.timeframe} candles: ${relationship} at least once no more than ${condition.withinBars} completed ${condition.withinBars === 1 ? "bar" : "bars"} ago (~${recencyDurationLabel(condition.timeframe, condition.withinBars)}).`
  }
  if (predicate.type === "cross") {
    const shape = crossShape(predicate)
    const event =
      shape === null
        ? `${operandLabel(predicate.left, descriptors)} ${crossedPhrase(predicate.direction)} ${operandLabel(predicate.right, descriptors)}`
        : `${operandLabel(shape.subject, descriptors)} ${crossedPhrase(shape.direction)} ${operandLabel(shape.reference, descriptors)}`
    return condition.withinBars === 0
      ? `Latest completed ${condition.timeframe} candle: ${event}.`
      : `On ${condition.timeframe} candles: ${event} no more than ${condition.withinBars} completed ${condition.withinBars === 1 ? "bar" : "bars"} ago (~${recencyDurationLabel(condition.timeframe, condition.withinBars)}).`
  }
  const event = `${predicateLabel(predicate, descriptors)} was confirmed`
  return condition.withinBars === 0
    ? `Latest completed ${condition.timeframe} candle: ${event}.`
    : `On ${condition.timeframe} candles: ${event} no more than ${condition.withinBars} completed ${condition.withinBars === 1 ? "bar" : "bars"} ago (~${recencyDurationLabel(condition.timeframe, condition.withinBars)}).`
}

function appendNodeLines(
  lines: DefinitionLine[],
  node: SignalNode,
  conditions: Readonly<Record<string, Condition>>,
  descriptors: readonly IndicatorDescriptor[],
  depth: number
): void {
  switch (node.type) {
    case "all":
      lines.push({ depth, label: "ALL" })
      for (const child of node.children) appendNodeLines(lines, child, conditions, descriptors, depth + 1)
      break
    case "any":
      lines.push({ depth, label: "ANY" })
      for (const child of node.children) appendNodeLines(lines, child, conditions, descriptors, depth + 1)
      break
    case "not":
      lines.push({ depth, label: "NOT" })
      appendNodeLines(lines, node.child, conditions, descriptors, depth + 1)
      break
    case "xor":
      lines.push({ depth, label: "XOR" })
      for (const child of node.children) appendNodeLines(lines, child, conditions, descriptors, depth + 1)
      break
    case "condition": {
      const condition = conditions[node.id]
      lines.push({
        depth,
        label: condition === undefined ? `Missing condition ${node.id}` : conditionLabel(condition, descriptors),
      })
      break
    }
  }
}

export function definitionLines(
  definition: SignalDefinition,
  descriptors: readonly IndicatorDescriptor[]
): DefinitionLine[] {
  const lines: DefinitionLine[] = []
  appendNodeLines(lines, definition.root, definition.conditions, descriptors, 0)
  return lines
}

/** Condition ids under a flat root (a bare condition or one all/any of conditions). */
export function directConditionIds(node: SignalNode): { mode: "all" | "any"; ids: string[] } | null {
  if (node.type === "condition") return { mode: "all", ids: [node.id] }
  if (node.type !== "all" && node.type !== "any") return null
  const ids: string[] = []
  for (const child of node.children) {
    if (child.type !== "condition") return null
    ids.push(child.id)
  }
  return { mode: node.type, ids }
}

export function definitionPreview(
  definition: SignalDefinition,
  descriptors: readonly IndicatorDescriptor[]
): DefinitionPreview {
  const direct = directConditionIds(definition.root)
  if (direct !== null) {
    const lines = direct.ids.map(id => {
      const condition = definition.conditions[id]
      return condition === undefined ? `Missing condition ${id}.` : conditionSentence(condition, descriptors)
    })
    const heading =
      lines.length === 1
        ? "Require this condition:"
        : direct.mode === "all"
          ? "Require all conditions:"
          : "Require any condition:"
    return { heading, lines }
  }
  return {
    heading: "Evaluate this signal logic:",
    lines: definitionLines(definition, descriptors).map(line => `${"  ".repeat(line.depth)}${line.label}`),
  }
}
