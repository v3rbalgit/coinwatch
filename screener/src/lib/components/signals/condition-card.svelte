<script lang="ts">
  import { ArrowDown, ArrowUp, Copy, Trash2 } from "@lucide/svelte"

  import type { IndicatorDescriptor, IndicatorInstance } from "$lib/chart/indicators"
  import IndicatorEditor from "$lib/components/signals/indicator-editor.svelte"
  import OperandEditor from "$lib/components/signals/operand-editor.svelte"
  import NumberField from "$lib/components/ui/number-field.svelte"
  import Select from "$lib/components/ui/select.svelte"
  import { SIGNAL_TIMEFRAMES } from "$lib/signals/definition"
  import type {
    ComparisonOp,
    CrossDirection,
    DivergenceDirectionFilter,
    DivergenceKindFilter,
    Operand,
    Predicate,
    SignalDirection,
  } from "$lib/signals/definition"
  import type { ConditionDraft } from "$lib/signals/draft"
  import { createDefaultPredicate } from "$lib/signals/editor"
  import type { PredicateType } from "$lib/signals/editor"
  import type { IndicatorPresetEntry } from "$lib/signals/presets"

  const predicateTypes: readonly { value: PredicateType; label: string }[] = [
    { value: "threshold", label: "Comparison" },
    { value: "cross", label: "Cross" },
    { value: "divergence", label: "Divergence" },
  ]
  const comparisonOps: readonly { value: ComparisonOp; label: string }[] = [
    { value: "lessThan", label: "<" },
    { value: "lessOrEqual", label: "≤" },
    { value: "greaterThan", label: ">" },
    { value: "greaterOrEqual", label: "≥" },
  ]
  const crossDirections: readonly CrossDirection[] = ["over", "under"]
  const divergenceKinds: readonly DivergenceKindFilter[] = ["any", "regular", "hidden"]
  const divergenceDirections: readonly DivergenceDirectionFilter[] = ["any", "bullish", "bearish"]

  const {
    condition,
    position,
    direction,
    descriptors,
    presets,
    busy = false,
    canMoveUp,
    canMoveDown,
    onChange,
    onRemove,
    onDuplicate,
    onMoveUp,
    onMoveDown,
    onCreatePreset,
    onUpdatePreset,
    onDeletePreset,
  }: {
    condition: ConditionDraft
    position: number
    direction: SignalDirection
    descriptors: readonly IndicatorDescriptor[]
    presets: IndicatorPresetEntry[]
    busy?: boolean
    canMoveUp: boolean
    canMoveDown: boolean
    onChange: (condition: ConditionDraft) => void
    onRemove: () => void
    onDuplicate: () => void
    onMoveUp: () => void
    onMoveDown: () => void
    onCreatePreset: (indicatorId: string, name: string, params: Record<string, number>) => void
    onUpdatePreset: (id: string, name: string, params: Record<string, number>) => void
    onDeletePreset: (id: string) => void
  } = $props()

  const divergenceDescriptors = $derived(descriptors.filter(descriptor => descriptor.kind === "divergence"))

  function replacePredicate(predicate: Predicate) {
    onChange({ ...condition, predicate })
  }

  function changePredicateType(value: string) {
    const type = predicateTypes.find(candidate => candidate.value === value)?.value
    if (type !== undefined) replacePredicate(createDefaultPredicate(type, direction, descriptors))
  }

  function changeTimeframe(value: string) {
    const timeframe = SIGNAL_TIMEFRAMES.find(candidate => candidate === value)
    if (timeframe !== undefined) onChange({ ...condition, timeframe })
  }

  function changeComparison(value: string) {
    if (condition.predicate.type !== "threshold") return
    const op = comparisonOps.find(candidate => candidate.value === value)?.value
    if (op !== undefined) replacePredicate({ ...condition.predicate, op })
  }

  function changeCrossDirection(value: string) {
    if (condition.predicate.type !== "cross") return
    const next = crossDirections.find(candidate => candidate === value)
    if (next !== undefined) replacePredicate({ ...condition.predicate, direction: next })
  }

  function changeDivergenceInstance(instance: IndicatorInstance) {
    if (condition.predicate.type === "divergence") replacePredicate({ ...condition.predicate, instance })
  }

  function changeDivergenceKind(value: string) {
    if (condition.predicate.type !== "divergence") return
    const kind = divergenceKinds.find(candidate => candidate === value)
    if (kind !== undefined) replacePredicate({ ...condition.predicate, kind })
  }

  function changeDivergenceDirection(value: string) {
    if (condition.predicate.type !== "divergence") return
    const next = divergenceDirections.find(candidate => candidate === value)
    if (next !== undefined) replacePredicate({ ...condition.predicate, direction: next })
  }
</script>

<article class="rounded-xl border border-border bg-card">
  <header class="flex flex-wrap items-center gap-2 border-b border-border px-4 py-3">
    <span
      class="flex size-6 items-center justify-center rounded-full bg-muted text-xs font-semibold text-muted-foreground"
    >
      {position}
    </span>
    <Select
      value={condition.predicate.type}
      onValueChange={changePredicateType}
      class="h-8 w-40"
      disabled={busy}
      aria-label={`Condition ${position} type`}
    >
      {#each predicateTypes as type (type.value)}
        <option value={type.value}>{type.label}</option>
      {/each}
    </Select>
    <Select
      value={condition.timeframe}
      onValueChange={changeTimeframe}
      class="h-8 w-24"
      disabled={busy}
      aria-label={`Condition ${position} timeframe`}
    >
      {#each SIGNAL_TIMEFRAMES as timeframe (timeframe)}
        <option value={timeframe}>{timeframe}</option>
      {/each}
    </Select>
    <label class="flex items-center gap-2 text-xs text-muted-foreground">
      within
      <NumberField
        value={condition.withinBars}
        min={0}
        max={10000}
        step={1}
        class="h-8 w-20"
        disabled={busy}
        aria-label={`Condition ${position} freshness bars`}
        onValueChange={(withinBars: number) => onChange({ ...condition, withinBars })}
      />
      bars
    </label>

    <div class="ml-auto flex items-center gap-0.5">
      <button
        type="button"
        class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground disabled:opacity-30"
        disabled={busy || !canMoveUp}
        aria-label={`Move condition ${position} up`}
        onclick={onMoveUp}
      >
        <ArrowUp class="size-3.5" />
      </button>
      <button
        type="button"
        class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground disabled:opacity-30"
        disabled={busy || !canMoveDown}
        aria-label={`Move condition ${position} down`}
        onclick={onMoveDown}
      >
        <ArrowDown class="size-3.5" />
      </button>
      <button
        type="button"
        class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
        disabled={busy}
        aria-label={`Duplicate condition ${position}`}
        onclick={onDuplicate}
      >
        <Copy class="size-3.5" />
      </button>
      <button
        type="button"
        class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-destructive"
        disabled={busy}
        aria-label={`Delete condition ${position}`}
        onclick={onRemove}
      >
        <Trash2 class="size-3.5" />
      </button>
    </div>
  </header>

  <div class="p-4">
    {#if condition.predicate.type === "threshold"}
      <div class="grid items-start gap-3 xl:grid-cols-[minmax(0,1fr)_6rem_minmax(0,1fr)]">
        <OperandEditor
          operand={condition.predicate.left}
          {descriptors}
          {presets}
          {busy}
          label="Left source"
          onChange={(left: Operand) =>
            condition.predicate.type === "threshold" && replacePredicate({ ...condition.predicate, left })}
          {onCreatePreset}
          {onUpdatePreset}
          {onDeletePreset}
        />
        <label class="space-y-1 text-xs text-muted-foreground">
          <span>Operator</span>
          <Select value={condition.predicate.op} onValueChange={changeComparison} disabled={busy}>
            {#each comparisonOps as operator (operator.value)}
              <option value={operator.value}>{operator.label}</option>
            {/each}
          </Select>
        </label>
        <OperandEditor
          operand={condition.predicate.right}
          {descriptors}
          {presets}
          {busy}
          label="Right source"
          onChange={(right: Operand) =>
            condition.predicate.type === "threshold" && replacePredicate({ ...condition.predicate, right })}
          {onCreatePreset}
          {onUpdatePreset}
          {onDeletePreset}
        />
      </div>
    {:else if condition.predicate.type === "cross"}
      <div class="grid items-start gap-3 xl:grid-cols-[minmax(0,1fr)_7rem_minmax(0,1fr)]">
        <OperandEditor
          operand={condition.predicate.left}
          {descriptors}
          {presets}
          {busy}
          label="Crossing source"
          onChange={(left: Operand) =>
            condition.predicate.type === "cross" && replacePredicate({ ...condition.predicate, left })}
          {onCreatePreset}
          {onUpdatePreset}
          {onDeletePreset}
        />
        <label class="space-y-1 text-xs text-muted-foreground">
          <span>Direction</span>
          <Select value={condition.predicate.direction} onValueChange={changeCrossDirection} disabled={busy}>
            {#each crossDirections as crossDirection (crossDirection)}
              <option value={crossDirection}>{crossDirection}</option>
            {/each}
          </Select>
        </label>
        <OperandEditor
          operand={condition.predicate.right}
          {descriptors}
          {presets}
          {busy}
          label="Reference source"
          onChange={(right: Operand) =>
            condition.predicate.type === "cross" && replacePredicate({ ...condition.predicate, right })}
          {onCreatePreset}
          {onUpdatePreset}
          {onDeletePreset}
        />
      </div>
    {:else}
      <div class="grid gap-3 lg:grid-cols-[minmax(0,1fr)_10rem_11rem]">
        <IndicatorEditor
          instance={condition.predicate.instance}
          descriptors={divergenceDescriptors}
          {presets}
          {busy}
          onChange={changeDivergenceInstance}
          {onCreatePreset}
          {onUpdatePreset}
          {onDeletePreset}
        />
        <label class="space-y-1 text-xs text-muted-foreground">
          <span>Kind</span>
          <Select value={condition.predicate.kind} onValueChange={changeDivergenceKind} disabled={busy}>
            {#each divergenceKinds as kind (kind)}
              <option value={kind}>{kind === "any" ? "Any kind" : kind}</option>
            {/each}
          </Select>
        </label>
        <label class="space-y-1 text-xs text-muted-foreground">
          <span>Direction</span>
          <Select value={condition.predicate.direction} onValueChange={changeDivergenceDirection} disabled={busy}>
            {#each divergenceDirections as divergenceDirection (divergenceDirection)}
              <option value={divergenceDirection}>
                {divergenceDirection === "any" ? "Either direction" : divergenceDirection}
              </option>
            {/each}
          </Select>
        </label>
      </div>
    {/if}
  </div>
</article>
