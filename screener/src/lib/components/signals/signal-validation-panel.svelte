<script lang="ts">
  import { CalendarClock, ChevronRight, X } from "@lucide/svelte"

  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import ConditionOutcomes from "$lib/components/signals/condition-outcomes.svelte"
  import Badge from "$lib/components/ui/badge.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import { formatTimestamp } from "$lib/format"
  import type { HistoricalValidation } from "$lib/signals/historical"
  import type { HistoricalValidationSnapshot } from "$lib/signals/historical-workspace"

  const {
    snapshot,
    result,
    selected,
    descriptors,
    onSelect,
    onClose,
  }: {
    snapshot: HistoricalValidationSnapshot
    result: HistoricalValidation
    selected: number | null
    descriptors: readonly IndicatorDescriptor[]
    onSelect: (index: number) => void
    onClose: () => void
  } = $props()

  const selectedSpan = $derived(selected === null ? undefined : result.spans[selected])
  const onsetCount = $derived(result.spans.filter(span => !span.activeAtStart).length)

  function spanLabel(index: number): string {
    const span = result.spans[index]
    if (span === undefined) return "Unknown matched period"
    return span.activeAtStart ? "Active at range start" : `Onset ${formatTimestamp(span.startedAt)}`
  }
</script>

<aside
  class="absolute top-3 right-3 bottom-3 z-20 flex w-[min(24rem,calc(100%-1.5rem))] flex-col overflow-hidden rounded-lg border border-border bg-card/95 text-card-foreground shadow-2xl backdrop-blur-sm"
  aria-label="Historical signal validation"
>
  <header class="flex items-start gap-3 border-b border-border px-4 py-3">
    <CalendarClock class="mt-0.5 size-4 shrink-0 text-primary" />
    <div class="min-w-0 flex-1">
      <div class="flex flex-wrap items-center gap-2">
        <h2 class="truncate text-sm font-semibold">{snapshot.name}</h2>
        <Badge variant="outline">{snapshot.source === "draft" ? "Draft" : "Saved"}</Badge>
      </div>
      <p class="mt-1 text-xs text-muted-foreground">
        {snapshot.rangeLabel} · Checked when each {result.anchorTimeframe} candle completed
      </p>
    </div>
    <Button variant="ghost" size="icon" aria-label="Close validation details" onclick={onClose}>
      <X class="size-4" />
    </Button>
  </header>

  <div class="flex items-center gap-4 border-b border-border px-4 py-2 text-xs text-muted-foreground">
    <span><strong class="text-foreground">{onsetCount}</strong> onsets</span>
    <span><strong class="text-foreground">{result.spans.length}</strong> matched periods</span>
    <span><strong class="text-foreground">{result.evaluatedPoints}</strong> checks</span>
  </div>

  <div class="scrollable min-h-0 flex-1 overflow-auto p-3">
    {#if result.spans.length === 0}
      <div class="rounded-lg border border-dashed border-border p-5 text-center">
        <p class="text-sm font-medium text-foreground">No matched periods</p>
        <p class="mt-1 text-xs text-muted-foreground">
          The complete definition never entered a matched state in this range.
        </p>
      </div>
    {:else}
      <div class="space-y-1">
        {#each result.spans as span, index (span.startedAt)}
          <button
            type="button"
            class="flex w-full items-center gap-2 rounded-md border px-3 py-2 text-left transition-colors {selected ===
            index
              ? 'border-primary bg-primary/10'
              : 'border-transparent hover:border-border hover:bg-accent/40'}"
            aria-pressed={selected === index}
            onclick={() => onSelect(index)}
          >
            <span class="min-w-0 flex-1">
              <span class="block truncate text-xs font-medium text-foreground">{spanLabel(index)}</span>
              <span class="mt-0.5 block truncate text-[11px] text-muted-foreground">
                Through {formatTimestamp(span.endedAt)}
                {span.activeAtEnd ? " · Active at range end" : ""}
              </span>
            </span>
            <ChevronRight class="size-3.5 shrink-0 text-muted-foreground" />
          </button>
        {/each}
      </div>

      {#if selectedSpan}
        <div class="mt-4 border-t border-border pt-4">
          <h3 class="mb-2 text-xs font-medium tracking-wide text-muted-foreground uppercase">
            {selectedSpan.activeAtStart ? "Range-start evidence" : "Onset evidence"}
          </h3>
          <ConditionOutcomes
            definition={snapshot.definition}
            report={selectedSpan.evidence}
            {descriptors}
            heading={false}
          />
        </div>
      {/if}
    {/if}
  </div>
</aside>
