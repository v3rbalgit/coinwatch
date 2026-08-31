import type { IndicatorDescriptor, ValueDomain } from "$lib/chart/indicators"
import type { CandleField, Operand } from "$lib/signals/definition"

export type OperandType = Operand["type"]

export interface OperandTypeDescriptor {
  value: OperandType
  label: string
  subjectEligible: boolean
}

export interface CandleFieldSourceDescriptor {
  value: CandleField
  label: string
  domain: ValueDomain
}

export const OPERAND_TYPE_DESCRIPTORS: readonly OperandTypeDescriptor[] = [
  { value: "field", label: "Market field", subjectEligible: true },
  { value: "indicator", label: "Indicator", subjectEligible: true },
  { value: "constant", label: "Number", subjectEligible: false },
]

export const CANDLE_FIELD_SOURCE_DESCRIPTORS: readonly CandleFieldSourceDescriptor[] = [
  { value: "open", label: "Open", domain: "price" },
  { value: "high", label: "High", domain: "price" },
  { value: "low", label: "Low", domain: "price" },
  { value: "close", label: "Close", domain: "price" },
  { value: "volume", label: "Volume", domain: "volume" },
]

export function compatibleSignalIndicators(
  descriptors: readonly IndicatorDescriptor[],
  requiredDomain: ValueDomain | null,
  currentIndicatorId: string | null
): IndicatorDescriptor[] {
  return descriptors.filter(
    descriptor =>
      descriptor.kind === "series" &&
      descriptor.supportsSignals &&
      (requiredDomain === null || descriptor.outputDomain === requiredDomain || descriptor.id === currentIndicatorId)
  )
}
