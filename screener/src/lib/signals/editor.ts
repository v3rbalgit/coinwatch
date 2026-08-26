import type { IndicatorDescriptor } from "$lib/chart/indicators"
import type { SignalCatalogEntry, SignalCatalogError } from "$lib/signals/catalog"
import { isSignalCatalogError } from "$lib/signals/catalog"
import type { Operand, Predicate, SignalDirection } from "$lib/signals/definition"
import { compileSignalDraft, definitionToSignalDraft } from "$lib/signals/draft"
import type { SignalDraft } from "$lib/signals/draft"
import { createIndicatorInstance } from "$lib/signals/instances"
import type { IndicatorPresetError } from "$lib/signals/presets"
import { isIndicatorPresetError } from "$lib/signals/presets"

export type ReadySignalCatalogEntry = Extract<SignalCatalogEntry, { status: "ready" }>
export type UnavailableSignalCatalogEntry = Extract<SignalCatalogEntry, { status: "unavailable" }>
export type OperandType = Operand["type"]
export type PredicateType = Predicate["type"]

export type SignalEditorSession =
  | { type: "empty" }
  | {
      type: "draft"
      id: string | null
      name: string
      draft: SignalDraft
      baseline: string | null
    }
  | { type: "rich"; entry: ReadySignalCatalogEntry }
  | { type: "unavailable"; entry: UnavailableSignalCatalogEntry }

function descriptorById(descriptors: readonly IndicatorDescriptor[], id: string): IndicatorDescriptor {
  const descriptor = descriptors.find(candidate => candidate.id === id)
  if (descriptor === undefined) throw new Error(`Indicator descriptor '${id}' is unavailable`)
  return descriptor
}

function hasStableConditionIds(draft: SignalDraft): boolean {
  if (draft.conditions.length === 0) return false
  const ids = new Set<string>()
  for (const condition of draft.conditions) {
    if (condition.id.length === 0 || ids.has(condition.id)) return false
    ids.add(condition.id)
  }
  return true
}

export function draftSnapshot(name: string, draft: SignalDraft): string | null {
  if (name.trim().length === 0 || !hasStableConditionIds(draft)) return null
  return JSON.stringify({ name, definition: compileSignalDraft(draft) })
}

export function createDraftSession(name: string, draft: SignalDraft): SignalEditorSession {
  return { type: "draft", id: null, name, draft, baseline: null }
}

export function openCatalogEntry(entry: SignalCatalogEntry): SignalEditorSession {
  if (entry.status === "unavailable") return { type: "unavailable", entry }
  const draft = definitionToSignalDraft(entry.definition)
  if (draft === null) return { type: "rich", entry }
  return {
    type: "draft",
    id: entry.id,
    name: entry.name,
    draft,
    baseline: draftSnapshot(entry.name, draft),
  }
}

export function canSaveDraftSession(session: SignalEditorSession): boolean {
  return session.type === "draft" && draftSnapshot(session.name, session.draft) !== null
}

export function isDraftSessionDirty(session: SignalEditorSession): boolean {
  if (session.type !== "draft") return false
  if (session.id === null) return true
  return draftSnapshot(session.name, session.draft) !== session.baseline
}

function unreachable(value: never): never {
  throw new Error(`Unexpected signal editor value '${String(value)}'`)
}

export function createDefaultOperand(type: OperandType, descriptors: readonly IndicatorDescriptor[]): Operand {
  switch (type) {
    case "field":
      return { type: "field", field: "close" }
    case "constant":
      return { type: "constant", value: 0 }
    case "indicator": {
      const descriptor = descriptorById(
        descriptors.filter(candidate => candidate.kind === "series"),
        "sma"
      )
      const output = descriptor.outputs[0]
      if (output === undefined) throw new Error(`Indicator '${descriptor.id}' has no numeric output`)
      return {
        type: "indicator",
        instance: createIndicatorInstance(descriptor),
        output: output.key,
      }
    }
    default:
      return unreachable(type)
  }
}

export function createDefaultPredicate(
  type: PredicateType,
  direction: SignalDirection,
  descriptors: readonly IndicatorDescriptor[]
): Predicate {
  switch (type) {
    case "threshold":
      return {
        type: "threshold",
        left: { type: "field", field: "close" },
        op: direction === "bullish" ? "greaterThan" : "lessThan",
        right: createDefaultOperand("indicator", descriptors),
      }
    case "cross":
      return {
        type: "cross",
        left: { type: "field", field: "close" },
        right: createDefaultOperand("indicator", descriptors),
        direction: direction === "bullish" ? "over" : "under",
      }
    case "divergence": {
      const descriptor = descriptorById(descriptors, "rsiDivergence")
      return {
        type: "divergence",
        instance: createIndicatorInstance(descriptor),
        kind: "any",
        direction,
      }
    }
    default:
      return unreachable(type)
  }
}

function unknownErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function signalCatalogErrorMessage(error: unknown): string {
  if (!isSignalCatalogError(error)) return unknownErrorMessage(error)
  const typed: SignalCatalogError = error
  switch (typed.type) {
    case "invalidName":
      return "Enter a signal name."
    case "invalidDefinition":
      return typed.message
    case "duplicateName":
      return `A signal named '${typed.name}' already exists.`
    case "notFound":
      return "This signal no longer exists. Reload the catalog."
    case "storage":
      return "Signal storage is unavailable."
    default:
      return unreachable(typed)
  }
}

export function indicatorPresetErrorMessage(error: unknown): string {
  if (!isIndicatorPresetError(error)) return unknownErrorMessage(error)
  const typed: IndicatorPresetError = error
  switch (typed.type) {
    case "invalidName":
      return "Enter a preset name."
    case "invalidPreset":
      return typed.message
    case "duplicateName":
      return `A preset named '${typed.name}' already exists for this indicator.`
    case "notFound":
      return "This preset no longer exists. Reload presets."
    case "storage":
      return "Preset storage is unavailable."
    default:
      return unreachable(typed)
  }
}
