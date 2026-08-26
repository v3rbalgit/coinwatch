import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { Predicate, SignalDirection, SignalTimeframe } from "$lib/signals/definition"
import { createConditionDraft } from "$lib/signals/draft"
import type { SignalDraft } from "$lib/signals/draft"
import { createIndicatorInstance } from "$lib/signals/instances"

export type SignalTemplateId = "closeSma" | "emaCross" | "rsiMomentum" | "rsiDivergence" | "macdDivergence" | "blank"

export interface SignalTemplate {
  id: SignalTemplateId
  label: string
  description: string
  supportsMultipleTimeframes: boolean
}

export interface SignalTemplateOptions {
  direction: SignalDirection
  timeframes: readonly SignalTimeframe[]
}

export interface NewSignalDraft {
  name: string
  draft: SignalDraft
}

export const SIGNAL_TEMPLATES: readonly SignalTemplate[] = [
  {
    id: "closeSma",
    label: "Close vs SMA",
    description: "A persistent trend filter comparing the latest close with SMA(20).",
    supportsMultipleTimeframes: false,
  },
  {
    id: "emaCross",
    label: "EMA crossover",
    description: "EMA(20) crossing EMA(50) in the selected direction.",
    supportsMultipleTimeframes: false,
  },
  {
    id: "rsiMomentum",
    label: "RSI momentum",
    description: "RSI(14) crossing the neutral 50 level.",
    supportsMultipleTimeframes: false,
  },
  {
    id: "rsiDivergence",
    label: "RSI divergence sweep",
    description: "Any regular or hidden RSI divergence across the selected timeframes.",
    supportsMultipleTimeframes: true,
  },
  {
    id: "macdDivergence",
    label: "MACD divergence sweep",
    description: "Any regular or hidden MACD divergence across the selected timeframes.",
    supportsMultipleTimeframes: true,
  },
  {
    id: "blank",
    label: "Blank signal",
    description: "Start with an empty flat signal and add conditions manually.",
    supportsMultipleTimeframes: false,
  },
]

function getDescriptor(descriptors: readonly IndicatorDescriptor[], id: string): IndicatorDescriptor {
  const descriptor = descriptors.find(candidate => candidate.id === id)
  if (descriptor === undefined) throw new Error(`Indicator descriptor '${id}' is unavailable`)
  return descriptor
}

function uniqueTimeframes(timeframes: readonly SignalTimeframe[]): SignalTimeframe[] {
  const unique = [...new Set(timeframes)]
  if (unique.length === 0) throw new Error("A signal template needs at least one timeframe")
  return unique
}

function singleConditionDraft(
  direction: SignalDirection,
  timeframe: SignalTimeframe,
  predicate: Predicate
): SignalDraft {
  return {
    direction,
    match: "all",
    conditions: [createConditionDraft({ timeframe, predicate, withinBars: 0 })],
  }
}

function divergenceDraft(
  direction: SignalDirection,
  timeframes: readonly SignalTimeframe[],
  descriptor: IndicatorDescriptor
): SignalDraft {
  return {
    direction,
    match: "any",
    conditions: timeframes.map(timeframe =>
      createConditionDraft({
        timeframe,
        predicate: {
          type: "divergence",
          instance: createIndicatorInstance(descriptor),
          kind: "any",
          direction,
        },
        withinBars: 3,
      })
    ),
  }
}

function directionLabel(direction: SignalDirection): string {
  return direction === "bullish" ? "Bullish" : "Bearish"
}

function unreachableTemplate(templateId: never): never {
  throw new Error(`Unknown signal template '${String(templateId)}'`)
}

export function buildSignalTemplate(
  templateId: SignalTemplateId,
  options: SignalTemplateOptions,
  descriptors: readonly IndicatorDescriptor[]
): NewSignalDraft {
  const timeframes = uniqueTimeframes(options.timeframes)
  const timeframe = timeframes[0]
  const label = directionLabel(options.direction)

  switch (templateId) {
    case "closeSma": {
      const sma = getDescriptor(descriptors, "sma")
      return {
        name: `${label} SMA trend`,
        draft: singleConditionDraft(options.direction, timeframe, {
          type: "threshold",
          left: { type: "field", field: "close" },
          op: options.direction === "bullish" ? "greaterThan" : "lessThan",
          right: {
            type: "indicator",
            instance: createIndicatorInstance(sma),
            output: "sma",
          },
        }),
      }
    }
    case "emaCross": {
      const ema = getDescriptor(descriptors, "ema")
      return {
        name: `${label} EMA crossover`,
        draft: singleConditionDraft(options.direction, timeframe, {
          type: "cross",
          left: {
            type: "indicator",
            instance: createIndicatorInstance(ema, { period: 20 }),
            output: "ema",
          },
          right: {
            type: "indicator",
            instance: createIndicatorInstance(ema, { period: 50 }),
            output: "ema",
          },
          direction: options.direction === "bullish" ? "over" : "under",
        }),
      }
    }
    case "rsiMomentum": {
      const rsi = getDescriptor(descriptors, "rsi")
      return {
        name: `${label} RSI momentum`,
        draft: singleConditionDraft(options.direction, timeframe, {
          type: "cross",
          left: {
            type: "indicator",
            instance: createIndicatorInstance(rsi),
            output: "rsi",
          },
          right: { type: "constant", value: 50 },
          direction: options.direction === "bullish" ? "over" : "under",
        }),
      }
    }
    case "rsiDivergence":
      return {
        name: `${label} RSI divergence`,
        draft: divergenceDraft(options.direction, timeframes, getDescriptor(descriptors, "rsiDivergence")),
      }
    case "macdDivergence":
      return {
        name: `${label} MACD divergence`,
        draft: divergenceDraft(options.direction, timeframes, getDescriptor(descriptors, "macdDivergence")),
      }
    case "blank":
      return {
        name: `${label} signal`,
        draft: { direction: options.direction, match: "all", conditions: [] },
      }
    default:
      return unreachableTemplate(templateId)
  }
}
