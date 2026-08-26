<script lang="ts">
  import { Plus, Save } from "@lucide/svelte"

  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import ConditionCard from "$lib/components/signals/condition-card.svelte"
  import Badge from "$lib/components/ui/badge.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import Input from "$lib/components/ui/input.svelte"
  import type { SignalDirection } from "$lib/signals/definition"
  import { createConditionDraft } from "$lib/signals/draft"
  import type { ConditionDraft, FlatMatch } from "$lib/signals/draft"
  import { createDefaultPredicate, isDraftSessionDirty } from "$lib/signals/editor"
  import type { SignalEditorSession } from "$lib/signals/editor"
  import type { IndicatorPresetEntry } from "$lib/signals/presets"
  import { draftCanSave, draftValidationMessage } from "$lib/signals/validation"

  type DraftSession = Extract<SignalEditorSession, { type: "draft" }>
  const directions: readonly SignalDirection[] = ["bullish", "bearish"]
  const matches: readonly FlatMatch[] = ["all", "any"]

  const {
    session,
    descriptors,
    presets,
    busy = false,
    error = "",
    onChange,
    onSave,
    onCreatePreset,
    onUpdatePreset,
    onDeletePreset,
  }: {
    session: DraftSession
    descriptors: readonly IndicatorDescriptor[]
    presets: IndicatorPresetEntry[]
    busy?: boolean
    error?: string
    onChange: (session: DraftSession) => void
    onSave: () => void
    onCreatePreset: (indicatorId: string, name: string, params: Record<string, number>) => void
    onUpdatePreset: (id: string, name: string, params: Record<string, number>) => void
    onDeletePreset: (id: string) => void
  } = $props()

  const dirty = $derived(isDraftSessionDirty(session))
  const validation = $derived(
    session.name.trim().length === 0 ? "Enter a signal name." : draftValidationMessage(session.draft, descriptors)
  )
  const canSave = $derived(draftCanSave(session.name, session.draft, descriptors) && dirty && !busy)

  function changeDraft(draft: DraftSession["draft"]) {
    onChange({ ...session, draft })
  }

  function addCondition() {
    changeDraft({
      ...session.draft,
      conditions: [
        ...session.draft.conditions,
        createConditionDraft({
          timeframe: "1h",
          predicate: createDefaultPredicate("threshold", session.draft.direction, descriptors),
          withinBars: 0,
        }),
      ],
    })
  }

  function updateCondition(index: number, condition: DraftSession["draft"]["conditions"][number]) {
    changeDraft({
      ...session.draft,
      conditions: session.draft.conditions.map((candidate, position) => (position === index ? condition : candidate)),
    })
  }

  function removeCondition(index: number) {
    changeDraft({
      ...session.draft,
      conditions: session.draft.conditions.filter((_condition, position) => position !== index),
    })
  }

  function duplicateCondition(index: number) {
    const source = session.draft.conditions[index]
    if (source === undefined) return
    const conditions = [...session.draft.conditions]
    conditions.splice(index + 1, 0, createConditionDraft(source))
    changeDraft({ ...session.draft, conditions })
  }

  function moveCondition(index: number, offset: -1 | 1) {
    const destination = index + offset
    if (destination < 0 || destination >= session.draft.conditions.length) return
    const conditions = [...session.draft.conditions]
    const source = conditions[index]
    const target = conditions[destination]
    if (source === undefined || target === undefined) return
    conditions[index] = target
    conditions[destination] = source
    changeDraft({ ...session.draft, conditions })
  }
</script>

<div class="flex h-full min-w-0 flex-1 flex-col">
  <header class="border-b border-border bg-background/80 px-5 py-4 backdrop-blur-sm">
    <div class="flex flex-wrap items-start gap-3">
      <div class="min-w-64 flex-1">
        <label class="text-xs font-medium tracking-wide text-muted-foreground uppercase" for="signal-name">
          Signal name
        </label>
        <Input
          id="signal-name"
          value={session.name}
          onValueChange={(name: string) => onChange({ ...session, name })}
          class="mt-1 h-10 text-base font-semibold"
          placeholder="Name this signal"
          disabled={busy}
        />
      </div>

      <div class="flex items-center gap-2 pt-5">
        {#if dirty}<Badge variant="outline">Unsaved</Badge>{/if}
        <Button onclick={onSave} disabled={!canSave}>
          <Save class="size-4" />{busy ? "Saving…" : session.id === null ? "Create signal" : "Save changes"}
        </Button>
      </div>
    </div>

    <div class="mt-4 flex flex-wrap items-center gap-5">
      <fieldset class="flex items-center gap-2">
        <legend class="sr-only">Signal direction</legend>
        <span class="text-xs text-muted-foreground">Intent</span>
        <div class="inline-flex rounded-md border border-border bg-card p-1">
          {#each directions as direction (direction)}
            <button
              type="button"
              class="rounded px-3 py-1.5 text-xs font-medium capitalize transition-colors {session.draft.direction ===
              direction
                ? 'bg-accent text-accent-foreground'
                : 'text-muted-foreground hover:text-foreground'}"
              disabled={busy}
              aria-pressed={session.draft.direction === direction}
              onclick={() => changeDraft({ ...session.draft, direction })}
            >
              {direction}
            </button>
          {/each}
        </div>
      </fieldset>

      <fieldset class="flex items-center gap-2">
        <legend class="sr-only">Condition composition</legend>
        <span class="text-xs text-muted-foreground">Match</span>
        <div class="inline-flex rounded-md border border-border bg-card p-1">
          {#each matches as match (match)}
            <button
              type="button"
              class="rounded px-3 py-1.5 text-xs font-medium uppercase transition-colors {session.draft.match === match
                ? 'bg-accent text-accent-foreground'
                : 'text-muted-foreground hover:text-foreground'}"
              disabled={busy}
              aria-pressed={session.draft.match === match}
              onclick={() => changeDraft({ ...session.draft, match })}
            >
              {match}
            </button>
          {/each}
        </div>
      </fieldset>

      <p class="text-xs text-muted-foreground">
        {session.draft.conditions.length}
        {session.draft.conditions.length === 1 ? "condition" : "conditions"}
      </p>
    </div>

    {#if error}
      <p class="mt-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
        {error}
      </p>
    {:else if validation !== null && session.draft.conditions.length > 0}
      <p
        class="mt-3 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-600 dark:text-amber-400"
      >
        {validation}
      </p>
    {/if}
  </header>

  <div class="scrollable min-h-0 flex-1 overflow-auto bg-muted/20 p-5">
    {#if session.draft.conditions.length === 0}
      <div
        class="flex min-h-72 flex-col items-center justify-center rounded-xl border border-dashed border-border bg-card/50 p-8 text-center"
      >
        <h2 class="text-sm font-medium text-foreground">This draft has no conditions</h2>
        <p class="mt-1 max-w-md text-xs leading-relaxed text-muted-foreground">
          Add a comparison, cross, or divergence condition. A signal needs at least one condition before it can be
          saved.
        </p>
        <Button class="mt-4" variant="outline" onclick={addCondition} disabled={busy}>
          <Plus class="size-4" />Add condition
        </Button>
      </div>
    {:else}
      <div class="mx-auto max-w-6xl space-y-4">
        {#each session.draft.conditions as condition, index (condition.id)}
          <ConditionCard
            {condition}
            position={index + 1}
            direction={session.draft.direction}
            {descriptors}
            {presets}
            {busy}
            canMoveUp={index > 0}
            canMoveDown={index < session.draft.conditions.length - 1}
            onChange={(next: ConditionDraft) => updateCondition(index, next)}
            onRemove={() => removeCondition(index)}
            onDuplicate={() => duplicateCondition(index)}
            onMoveUp={() => moveCondition(index, -1)}
            onMoveDown={() => moveCondition(index, 1)}
            {onCreatePreset}
            {onUpdatePreset}
            {onDeletePreset}
          />
        {/each}
        <Button variant="outline" onclick={addCondition} disabled={busy}>
          <Plus class="size-4" />Add condition
        </Button>
      </div>
    {/if}
  </div>
</div>
