import { indicatorDescriptors, loadIndicators } from "$lib/chart/indicators"
import { listSignals } from "$lib/signals/catalog"
import type { SignalDefinition } from "$lib/signals/definition"
import { compileSignalDraft } from "$lib/signals/draft"
import { isDraftSessionDirty } from "$lib/signals/editor"
import { signalEditorWorkspace } from "$lib/signals/editor-workspace.svelte"
import { draftValidationMessage } from "$lib/signals/validation"

export interface HistoricalSignalOption {
  key: string
  name: string
  source: "draft" | "saved"
  definition: SignalDefinition | null
  disabledReason?: string
}

/** Ready catalog entries plus a materially different in-memory editor draft. */
export async function loadHistoricalSignalOptions(): Promise<HistoricalSignalOption[]> {
  const [, entries] = await Promise.all([loadIndicators(), listSignals()])
  const descriptors = indicatorDescriptors()
  const options: HistoricalSignalOption[] = entries
    .filter(entry => entry.status === "ready")
    .map((entry): HistoricalSignalOption => ({
      key: `saved:${entry.id}`,
      name: entry.name,
      source: "saved",
      definition: entry.definition,
    }))
    .sort((left, right) => left.name.localeCompare(right.name))

  const session = signalEditorWorkspace.session
  if (
    session.type === "draft" &&
    session.draft.conditions.length > 0 &&
    (session.id === null || isDraftSessionDirty(session))
  ) {
    let issue = draftValidationMessage(session.draft, descriptors)
    let definition: SignalDefinition | null = null
    if (issue === null) {
      try {
        definition = compileSignalDraft(session.draft)
      } catch (error) {
        issue = error instanceof Error ? error.message : String(error)
      }
    }
    options.unshift({
      key: `draft:${session.id ?? "new"}`,
      name: session.name.trim() || "Current draft",
      source: "draft",
      definition,
      ...(issue === null ? {} : { disabledReason: issue }),
    })
  }

  return options
}
