import type { IndicatorInstance } from "$lib/chart/indicators"
import { SIGNAL_DEFINITION_VERSION } from "$lib/signals/definition"
import type {
  Condition,
  Operand,
  OperandAdjustment,
  Predicate,
  SignalDefinition,
  SignalDirection,
  SignalNode,
} from "$lib/signals/definition"

export type FlatMatch = "all" | "any"

export interface ConditionDraft extends Condition {
  id: string
}

export interface SignalDraft {
  direction: SignalDirection
  match: FlatMatch
  conditions: ConditionDraft[]
}

export function newConditionId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `condition-${crypto.randomUUID()}`
  }
  return `condition-${Math.random().toString(36).slice(2)}${Math.random().toString(36).slice(2)}`
}

function copyInstance(instance: IndicatorInstance): IndicatorInstance {
  return { id: instance.id, params: { ...instance.params } }
}

function copyAdjustment(adjustment: OperandAdjustment): OperandAdjustment {
  switch (adjustment.type) {
    case "absolute":
      return { type: "absolute", value: adjustment.value }
    case "percent":
      return { type: "percent", value: adjustment.value }
  }
}

function copyOperand(operand: Operand): Operand {
  switch (operand.type) {
    case "indicator":
      if (operand.adjustment === undefined) {
        return {
          type: "indicator",
          instance: copyInstance(operand.instance),
          output: operand.output,
        }
      }
      return {
        type: "indicator",
        instance: copyInstance(operand.instance),
        output: operand.output,
        adjustment: copyAdjustment(operand.adjustment),
      }
    case "field":
      if (operand.adjustment === undefined) return { type: "field", field: operand.field }
      return {
        type: "field",
        field: operand.field,
        adjustment: copyAdjustment(operand.adjustment),
      }
    case "constant":
      return { type: "constant", value: operand.value }
  }
}

function copyPredicate(predicate: Predicate): Predicate {
  switch (predicate.type) {
    case "threshold":
      return {
        type: "threshold",
        left: copyOperand(predicate.left),
        op: predicate.op,
        right: copyOperand(predicate.right),
      }
    case "cross":
      return {
        type: "cross",
        left: copyOperand(predicate.left),
        right: copyOperand(predicate.right),
        direction: predicate.direction,
      }
    case "divergence":
      return {
        type: "divergence",
        instance: copyInstance(predicate.instance),
        kind: predicate.kind,
        direction: predicate.direction,
      }
  }
}

function copyCondition(condition: Condition): Condition {
  return {
    timeframe: condition.timeframe,
    predicate: copyPredicate(condition.predicate),
    withinBars: condition.withinBars,
  }
}

export function createConditionDraft(condition: Condition, id = newConditionId()): ConditionDraft {
  return { id, ...copyCondition(condition) }
}

export function compileSignalDraft(draft: SignalDraft): SignalDefinition {
  if (draft.conditions.length === 0) throw new Error("A signal draft needs at least one condition")

  const ids = new Set<string>()
  for (const condition of draft.conditions) {
    if (condition.id.length === 0) throw new Error("A signal condition needs a stable id")
    if (ids.has(condition.id)) throw new Error(`Duplicate signal condition id '${condition.id}'`)
    ids.add(condition.id)
  }

  const children: SignalNode[] = draft.conditions.map(condition => ({
    type: "condition",
    id: condition.id,
  }))
  const conditions = Object.fromEntries(draft.conditions.map(condition => [condition.id, copyCondition(condition)]))

  return {
    version: SIGNAL_DEFINITION_VERSION,
    direction: draft.direction,
    root: { type: draft.match, children },
    conditions,
  }
}

/** Return null when a valid core definition needs the richer tree editor. */
export function definitionToSignalDraft(definition: SignalDefinition): SignalDraft | null {
  let match: FlatMatch
  let children: SignalNode[]
  switch (definition.root.type) {
    case "condition":
      match = "all"
      children = [definition.root]
      break
    case "all":
    case "any":
      match = definition.root.type
      children = definition.root.children
      break
    case "not":
    case "xor":
      return null
  }
  if (children.length === 0) return null

  const ids: string[] = []
  const seen = new Set<string>()
  for (const child of children) {
    if (child.type !== "condition" || seen.has(child.id)) return null
    seen.add(child.id)
    ids.push(child.id)
  }

  const conditionKeys = Object.keys(definition.conditions)
  if (ids.length !== conditionKeys.length) return null

  const conditions: ConditionDraft[] = []
  for (const id of ids) {
    const condition = definition.conditions[id]
    if (condition === undefined) return null
    conditions.push({ id, ...copyCondition(condition) })
  }

  return {
    direction: definition.direction,
    match,
    conditions,
  }
}
