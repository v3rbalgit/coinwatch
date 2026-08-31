<script lang="ts">
  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import { formatTimestamp } from "$lib/format"
  import type { Condition, SignalDefinition } from "$lib/signals/definition"
  import { crossEvidenceLabel, formatObservedValue, thresholdEvidenceLabel } from "$lib/signals/format"
  import type { ConditionOutcome, EvalReport } from "$lib/signals/run"
  import { conditionEvidence, pendingCandleMessage } from "$lib/signals/run-results"

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

  function outcomeLabel(outcome: ConditionOutcome | null): string {
    if (outcome === null) return "No outcome"
    switch (outcome.status) {
      case "satisfied":
        return "Satisfied"
      case "notSatisfied":
        return "Not satisfied"
      case "noData":
        return pendingCandleMessage(outcome.reason)
      case "unavailable":
        return "Unavailable"
      case "insufficientHistory":
        return `Needs ${outcome.required} bars · ${outcome.available} available`
      case "undefinedOperands":
        return "No defined value"
    }
  }

  function outcomeTone(outcome: ConditionOutcome | null): string {
    const status = outcome?.status
    if (status === "satisfied") return "text-emerald-500"
    if (status === "notSatisfied") return "text-muted-foreground"
    return "text-amber-500"
  }

  function evidenceLabel(condition: Condition | null, outcome: ConditionOutcome | null): string | null {
    const evidence = outcome?.evidence
    if (evidence === null || evidence === undefined) return null
    if (evidence.type === "state") {
      const observed =
        condition?.predicate.type === "threshold"
          ? thresholdEvidenceLabel(condition.predicate, evidence.left, evidence.right, descriptors)
          : `Observed ${formatObservedValue(evidence.left)} · ${formatObservedValue(evidence.right)}`
      return `${observed} · ${formatTimestamp(evidence.observedAt)}`
    }
    const observed =
      condition?.predicate.type === "cross"
        ? crossEvidenceLabel(
            condition.predicate,
            evidence.previousLeft,
            evidence.previousRight,
            evidence.left,
            evidence.right,
            descriptors
          )
        : `Previous ${formatObservedValue(evidence.previousLeft)} · ${formatObservedValue(evidence.previousRight)} → current ${formatObservedValue(evidence.left)} · ${formatObservedValue(evidence.right)}`
    return `${observed} · ${formatTimestamp(evidence.observedAt)}`
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
  {#each conditionEvidence(definition, report, descriptors) as item (item.id)}
    <div class="grid gap-1 px-3 py-2 text-xs lg:grid-cols-[minmax(0,1fr)_auto] lg:items-center">
      <span class="text-foreground">{item.label}</span>
      <span class={outcomeTone(item.outcome)}>{outcomeLabel(item.outcome)}</span>
      {#if evidenceLabel(item.condition, item.outcome) !== null}
        <span class="text-muted-foreground lg:col-span-2">{evidenceLabel(item.condition, item.outcome)}</span>
      {/if}
      {#if lastTrueLabel(item.outcome) !== null}
        <span class="text-muted-foreground lg:col-span-2">{lastTrueLabel(item.outcome)}</span>
      {/if}
    </div>
  {/each}
</div>
