<script lang="ts">
  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import type { SignalDefinition } from "$lib/signals/definition"
  import type { ConditionOutcome, EvalReport } from "$lib/signals/run"
  import { conditionEvidence } from "$lib/signals/run-results"

  const {
    definition,
    report,
    descriptors,
    heading = true,
  }: {
    definition: SignalDefinition
    report: EvalReport
    descriptors: readonly IndicatorDescriptor[]
    heading?: boolean
  } = $props()

  function formatTimestamp(value: string | null): string {
    if (value === null) return "—"
    const parsed = new Date(value)
    return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString()
  }

  function outcomeLabel(outcome: ConditionOutcome | null): string {
    if (outcome === null) return "No outcome"
    switch (outcome.status) {
      case "satisfied":
        return "Satisfied"
      case "notSatisfied":
        return "Not satisfied"
      case "noData":
        return "No data"
      case "unavailable":
        return "Unavailable"
      case "insufficientHistory":
        return `Needs ${outcome.required} bars · ${outcome.available} available`
    }
  }

  function outcomeTone(outcome: ConditionOutcome | null): string {
    const status = outcome?.status
    if (status === "satisfied") return "text-emerald-500"
    if (status === "notSatisfied") return "text-muted-foreground"
    return "text-amber-500"
  }

  function formatObservedValue(value: number): string {
    return Number(value.toPrecision(6)).toLocaleString(undefined, { maximumSignificantDigits: 6 })
  }

  function evidenceLabel(outcome: ConditionOutcome | null): string | null {
    const evidence = outcome?.evidence
    if (evidence === null || evidence === undefined) return null
    if (evidence.type === "state") {
      return `Observed ${formatObservedValue(evidence.left)} vs ${formatObservedValue(evidence.right)} · ${formatTimestamp(evidence.observedAt)}`
    }
    return `Previous ${formatObservedValue(evidence.previousLeft)} vs ${formatObservedValue(evidence.previousRight)} → current ${formatObservedValue(evidence.left)} vs ${formatObservedValue(evidence.right)} · ${formatTimestamp(evidence.observedAt)}`
  }

  function lastTrueLabel(outcome: ConditionOutcome | null): string | null {
    if (
      outcome?.lastTrueBar === null ||
      outcome?.lastTrueBar === undefined ||
      outcome.lastTrueBar === outcome.evidence?.observedAt
    ) {
      return null
    }
    return `Last true ${formatTimestamp(outcome.lastTrueBar)}`
  }
</script>

{#if heading}
  <h3 class="text-xs font-medium tracking-wide text-muted-foreground uppercase">Condition outcomes</h3>
{/if}
<div class="{heading ? 'mt-2' : ''} divide-y divide-border/60 rounded-md border border-border/70">
  {#each conditionEvidence(definition, report, descriptors) as condition (condition.id)}
    <div class="grid gap-1 px-3 py-2 text-xs lg:grid-cols-[minmax(0,1fr)_auto] lg:items-center">
      <span class="text-foreground">{condition.label}</span>
      <span class={outcomeTone(condition.outcome)}>{outcomeLabel(condition.outcome)}</span>
      {#if evidenceLabel(condition.outcome) !== null}
        <span class="text-muted-foreground lg:col-span-2">{evidenceLabel(condition.outcome)}</span>
      {/if}
      {#if lastTrueLabel(condition.outcome) !== null}
        <span class="text-muted-foreground lg:col-span-2">{lastTrueLabel(condition.outcome)}</span>
      {/if}
    </div>
  {/each}
</div>
