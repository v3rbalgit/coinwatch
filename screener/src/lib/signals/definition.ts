import type { IndicatorInstance } from "$lib/chart/indicators"
import { TIMEFRAMES } from "$lib/timeframes"
import type { Timeframe } from "$lib/timeframes"

export const SIGNAL_DEFINITION_VERSION = 3

export type SignalTimeframe = Timeframe
export const SIGNAL_TIMEFRAMES: readonly SignalTimeframe[] = TIMEFRAMES

export type SignalDirection = "bullish" | "bearish"

export type OperandAdjustment = { type: "absolute"; value: number } | { type: "percent"; value: number }

export type CandleField = "open" | "high" | "low" | "close" | "volume"

export type Operand =
  | {
      type: "indicator"
      instance: IndicatorInstance
      output: string
      adjustment?: OperandAdjustment
    }
  | {
      type: "field"
      field: CandleField
      adjustment?: OperandAdjustment
    }
  | {
      type: "constant"
      value: number
    }

export type ComparisonOp = "lessThan" | "lessOrEqual" | "greaterThan" | "greaterOrEqual"
export type CrossDirection = "over" | "under"
export type DivergenceKindFilter = "any" | "regular" | "hidden"
export type DivergenceDirectionFilter = "any" | "bullish" | "bearish"

export type Predicate =
  | {
      type: "threshold"
      left: Operand
      op: ComparisonOp
      right: Operand
    }
  | {
      type: "cross"
      left: Operand
      right: Operand
      direction: CrossDirection
    }
  | {
      type: "divergence"
      instance: IndicatorInstance
      kind: DivergenceKindFilter
      direction: DivergenceDirectionFilter
    }

export interface Condition {
  timeframe: SignalTimeframe
  predicate: Predicate
  withinBars: number
}

export type SignalNode =
  | { type: "all"; children: SignalNode[] }
  | { type: "any"; children: SignalNode[] }
  | { type: "not"; child: SignalNode }
  | { type: "xor"; children: SignalNode[] }
  | { type: "condition"; id: string }

export interface SignalDefinition {
  version: typeof SIGNAL_DEFINITION_VERSION
  direction: SignalDirection
  root: SignalNode
  conditions: Record<string, Condition>
}
