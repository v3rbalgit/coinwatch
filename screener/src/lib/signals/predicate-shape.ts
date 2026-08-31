import type { ComparisonOp, CrossDirection, Operand, Predicate } from "$lib/signals/definition"

export type SourceOperand = Exclude<Operand, { type: "constant" }>

export interface ThresholdShape {
  subject: SourceOperand
  op: ComparisonOp
  reference: Operand
  reversed: boolean
}

export interface CrossShape {
  subject: SourceOperand
  direction: CrossDirection
  reference: Operand
  reversed: boolean
}

export function isSourceOperand(operand: Operand): operand is SourceOperand {
  return operand.type !== "constant"
}

export function invertComparisonOp(op: ComparisonOp): ComparisonOp {
  switch (op) {
    case "lessThan":
      return "greaterThan"
    case "lessOrEqual":
      return "greaterOrEqual"
    case "greaterThan":
      return "lessThan"
    case "greaterOrEqual":
      return "lessOrEqual"
  }
}

export function invertCrossDirection(direction: CrossDirection): CrossDirection {
  return direction === "over" ? "under" : "over"
}

export function thresholdShape(predicate: Extract<Predicate, { type: "threshold" }>): ThresholdShape | null {
  if (isSourceOperand(predicate.left)) {
    return {
      subject: predicate.left,
      op: predicate.op,
      reference: predicate.right,
      reversed: false,
    }
  }
  if (isSourceOperand(predicate.right)) {
    return {
      subject: predicate.right,
      op: invertComparisonOp(predicate.op),
      reference: predicate.left,
      reversed: true,
    }
  }
  return null
}

export function crossShape(predicate: Extract<Predicate, { type: "cross" }>): CrossShape | null {
  if (isSourceOperand(predicate.left)) {
    return {
      subject: predicate.left,
      direction: predicate.direction,
      reference: predicate.right,
      reversed: false,
    }
  }
  if (isSourceOperand(predicate.right)) {
    return {
      subject: predicate.right,
      direction: invertCrossDirection(predicate.direction),
      reference: predicate.left,
      reversed: true,
    }
  }
  return null
}

/** Put the observable source first without changing the predicate's truth conditions. */
export function canonicalPredicate(predicate: Predicate): Predicate {
  switch (predicate.type) {
    case "threshold": {
      const shape = thresholdShape(predicate)
      return shape === null
        ? predicate
        : { type: "threshold", left: shape.subject, op: shape.op, right: shape.reference }
    }
    case "cross": {
      const shape = crossShape(predicate)
      return shape === null
        ? predicate
        : { type: "cross", left: shape.subject, right: shape.reference, direction: shape.direction }
    }
    case "divergence":
      return predicate
  }
}

export function presentationValues(
  reversed: boolean,
  left: number,
  right: number
): { subject: number; reference: number } {
  return reversed ? { subject: right, reference: left } : { subject: left, reference: right }
}
