import type { IndicatorDescriptor, IndicatorInstance } from "$lib/chart/indicators"
import type {
  Condition,
  Operand,
  OperandAdjustment,
  Predicate,
  SignalDefinition,
  SignalNode,
} from "$lib/signals/definition"

export interface DefinitionLine {
  depth: number
  label: string
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
      const adjustment = operand.adjustment === undefined ? "" : ` ${adjustmentLabel(operand.adjustment)}`
      return `${instanceLabel(operand.instance, descriptors)} · ${output?.label ?? operand.output}${adjustment}`
    }
    case "field": {
      const label = operand.field.charAt(0).toUpperCase() + operand.field.slice(1)
      return operand.adjustment === undefined ? label : `${label} ${adjustmentLabel(operand.adjustment)}`
    }
    case "constant":
      return formatNumber(operand.value)
  }
}

export function predicateLabel(predicate: Predicate, descriptors: readonly IndicatorDescriptor[]): string {
  switch (predicate.type) {
    case "threshold": {
      const operator = {
        lessThan: "<",
        lessOrEqual: "≤",
        greaterThan: ">",
        greaterOrEqual: "≥",
      }[predicate.op]
      return `${operandLabel(predicate.left, descriptors)} ${operator} ${operandLabel(predicate.right, descriptors)}`
    }
    case "cross":
      return `${operandLabel(predicate.left, descriptors)} crosses ${predicate.direction} ${operandLabel(predicate.right, descriptors)}`
    case "divergence": {
      const kind = predicate.kind === "any" ? "Any" : predicate.kind === "regular" ? "Regular" : "Hidden"
      const direction =
        predicate.direction === "any" ? "bullish or bearish" : predicate.direction === "bullish" ? "bullish" : "bearish"
      return `${kind} ${direction} ${instanceLabel(predicate.instance, descriptors)}`
    }
  }
}

export function conditionLabel(condition: Condition, descriptors: readonly IndicatorDescriptor[]): string {
  const freshness =
    condition.withinBars === 0
      ? "latest bar"
      : `within ${condition.withinBars} ${condition.withinBars === 1 ? "bar" : "bars"}`
  return `${condition.timeframe} · ${predicateLabel(condition.predicate, descriptors)} · ${freshness}`
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
