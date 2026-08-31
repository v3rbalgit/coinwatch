<script lang="ts">
  import {
    ArrowLeft,
    Ban,
    ChevronDown,
    CircleAlert,
    CircleCheck,
    ExternalLink,
    LoaderCircle,
    OctagonX,
  } from "@lucide/svelte"

  import { goto } from "$app/navigation"
  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import ConditionOutcomes from "$lib/components/signals/condition-outcomes.svelte"
  import Badge from "$lib/components/ui/badge.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import { formatTimestamp } from "$lib/format"
  import type { EvalReport, SignalRunProgress } from "$lib/signals/run"
  import {
    filterSignalRunReports,
    pendingCandleMessage,
    rankClosestSignalRunReports,
    reportHasDataIssues,
  } from "$lib/signals/run-results"
  import type { SignalRunResultFilter } from "$lib/signals/run-results"
  import type { SignalRunSnapshot } from "$lib/signals/run-workspace.svelte"

  const {
    snapshot,
    progress,
    reports,
    filter,
    descriptors,
    starting = false,
    cancellationPending = false,
    error = "",
    onBack,
    onCancel,
    onFilterChange,
  }: {
    snapshot: SignalRunSnapshot
    progress: SignalRunProgress
    reports: EvalReport[]
    filter: SignalRunResultFilter
    descriptors: readonly IndicatorDescriptor[]
    starting?: boolean
    cancellationPending?: boolean
    error?: string
    onBack: () => void
    onCancel: () => void
    onFilterChange: (filter: SignalRunResultFilter) => void
  } = $props()

  const filters: readonly { value: SignalRunResultFilter; label: string }[] = [
    { value: "matches", label: "Matches" },
    { value: "closest", label: "Closest" },
    { value: "all", label: "All processed" },
    { value: "issues", label: "Data issues" },
  ]
  const filteredReports = $derived(filterSignalRunReports(snapshot.definition, reports, filter))
  const closestCount = $derived(Math.min(20, rankClosestSignalRunReports(snapshot.definition, reports).length))
  const issueCount = $derived(reports.filter(reportHasDataIssues).length)
  const conditionCount = $derived(Object.keys(snapshot.definition.conditions).length)
  const active = $derived(starting || progress.status === "running")
  const completed = $derived(progress.status === "idle" ? 0 : progress.completed)
  const total = $derived(progress.status === "idle" ? 0 : progress.total)
  const matched = $derived(progress.status === "idle" ? 0 : progress.matched)
  const unknown = $derived(progress.status === "idle" ? 0 : progress.unknown)
  const percentage = $derived(total === 0 ? 0 : Math.min(100, Math.round((completed / total) * 100)))
  const displayedError = $derived(progress.status === "failed" ? progress.message : error)

  function satisfiedConditionCount(report: EvalReport): number {
    return Object.values(report.conditions).filter(outcome => outcome.status === "satisfied").length
  }

  function verdictLabel(report: EvalReport): string {
    switch (report.verdict) {
      case "matched":
        return "Match"
      case "notMatched":
        return "No match"
      case "unknown":
        return "Could not evaluate"
    }
  }
</script>

<div class="flex h-full min-w-0 flex-1 flex-col">
  <header class="border-b border-border bg-background/80 px-5 py-4 backdrop-blur-sm">
    <div class="flex flex-wrap items-start gap-3">
      <Button variant="ghost" size="sm" onclick={onBack}><ArrowLeft class="size-4" />Definition</Button>
      <div class="min-w-0 flex-1">
        <div class="flex flex-wrap items-center gap-2">
          <h1 class="truncate text-base font-semibold text-foreground">{snapshot.name}</h1>
          <Badge variant="outline">{snapshot.source === "draft" ? "Draft snapshot" : "Saved definition"}</Badge>
          <Badge variant="secondary">{snapshot.scopeLabel}</Badge>
        </div>
        <p class="mt-1 text-xs text-muted-foreground">Started {formatTimestamp(snapshot.startedAt)}</p>
      </div>
      {#if active}
        <Button variant="destructive" onclick={onCancel} disabled={cancellationPending}>
          {#if cancellationPending}
            <LoaderCircle class="size-4 animate-spin" />Cancelling…
          {:else}
            <Ban class="size-4" />Cancel run
          {/if}
        </Button>
      {/if}
    </div>

    <section class="mt-4 rounded-lg border border-border bg-card p-3" aria-label="Signal run progress">
      <div class="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
        <span class="flex items-center gap-1.5 font-medium text-foreground">
          {#if starting}
            <LoaderCircle class="size-3.5 animate-spin text-primary" />Starting
          {:else if progress.status === "running"}
            <LoaderCircle class="size-3.5 animate-spin text-primary" />Running
          {:else if progress.status === "completed"}
            <CircleCheck class="size-3.5 text-emerald-500" />Completed
          {:else if progress.status === "cancelled"}
            <Ban class="size-3.5 text-amber-500" />Cancelled
          {:else if progress.status === "failed" || displayedError}
            <OctagonX class="size-3.5 text-destructive" />Failed
          {:else}
            Preparing
          {/if}
        </span>
        <span class="text-muted-foreground"
          ><strong class="text-foreground">{completed}</strong> / {total} processed</span
        >
        <span class="text-muted-foreground"><strong class="text-foreground">{matched}</strong> matches</span>
        <span class="text-muted-foreground"><strong class="text-foreground">{unknown}</strong> inconclusive</span>
        <span class="text-muted-foreground"><strong class="text-foreground">{issueCount}</strong> data issues</span>
      </div>
      <div
        class="mt-3 h-1.5 overflow-hidden rounded-full bg-muted"
        role="progressbar"
        aria-label="Markets processed"
        aria-valuemin="0"
        aria-valuemax={total}
        aria-valuenow={completed}
      >
        <div class="h-full rounded-full bg-primary transition-[width]" style:width={`${percentage}%`}></div>
      </div>
      {#if displayedError}
        <p class="mt-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {displayedError}
        </p>
      {/if}
    </section>

    <div class="mt-4 flex flex-wrap items-center gap-2">
      {#each filters as option (option.value)}
        <button
          type="button"
          class="rounded-md border px-3 py-1.5 text-xs font-medium transition-colors {filter === option.value
            ? 'border-primary bg-primary/10 text-primary'
            : 'border-border text-muted-foreground hover:bg-accent/50 hover:text-foreground'}"
          aria-pressed={filter === option.value}
          onclick={() => onFilterChange(option.value)}
        >
          {option.label}
          {#if option.value === "matches"}<span class="ml-1 tabular-nums">{matched}</span>{/if}
          {#if option.value === "closest"}<span class="ml-1 tabular-nums">{closestCount}</span>{/if}
          {#if option.value === "all"}<span class="ml-1 tabular-nums">{reports.length}</span>{/if}
          {#if option.value === "issues"}<span class="ml-1 tabular-nums">{issueCount}</span>{/if}
        </button>
      {/each}
    </div>
  </header>

  <div class="scrollable min-h-0 flex-1 overflow-auto bg-muted/20 p-5">
    {#if filteredReports.length === 0}
      <div
        class="mx-auto flex min-h-64 max-w-xl flex-col items-center justify-center rounded-xl border border-dashed border-border bg-card/50 p-8 text-center"
      >
        {#if active && reports.length === 0}
          <LoaderCircle class="size-6 animate-spin text-primary" />
          <h2 class="mt-3 text-sm font-medium text-foreground">Waiting for the first market</h2>
          <p class="mt-1 text-xs text-muted-foreground">Results appear as each market is processed.</p>
        {:else}
          <CircleAlert class="size-6 text-muted-foreground" />
          <h2 class="mt-3 text-sm font-medium text-foreground">No results in this view</h2>
          <p class="mt-1 text-xs text-muted-foreground">
            {filter === "matches"
              ? "No processed markets have matched this signal."
              : filter === "closest"
                ? "No defensible proximity ranking is available for these condition outcomes."
                : filter === "issues"
                  ? "No processed markets have reported data issues."
                  : "No markets have been processed yet."}
          </p>
        {/if}
      </div>
    {:else}
      <div class="mx-auto max-w-5xl space-y-2">
        {#each filteredReports as report (report.coin)}
          <details class="group rounded-lg border border-border bg-card">
            <summary class="flex cursor-pointer list-none flex-wrap items-center gap-3 px-4 py-3 marker:hidden">
              <span class="min-w-24 text-sm font-semibold text-foreground">{report.coin}</span>
              <Badge
                variant={report.verdict === "matched" ? "success" : "outline"}
                class={report.verdict === "unknown" ? "border-amber-500/40 text-amber-500" : ""}
              >
                {verdictLabel(report)}
              </Badge>
              {#if report.verdict !== "unknown" && reportHasDataIssues(report)}
                <Badge variant="outline" class="border-amber-500/40 text-amber-500">Partial data</Badge>
              {/if}
              {#if filter === "closest"}
                <Badge variant="secondary">{satisfiedConditionCount(report)} / {conditionCount} conditions</Badge>
              {/if}
              <span class="ml-auto text-xs text-muted-foreground">
                {report.lastEvidenceAt === null
                  ? "No supporting evidence"
                  : `Last evidence ${formatTimestamp(report.lastEvidenceAt)}`}
              </span>
              <a
                class="inline-flex shrink-0 items-center gap-1 text-xs font-medium text-primary hover:underline"
                href={`/browse/${encodeURIComponent(report.coin)}`}
                onclick={event => {
                  event.preventDefault()
                  event.stopPropagation()
                  void goto(`/browse/${encodeURIComponent(report.coin)}`)
                }}
              >
                Open market <ExternalLink class="size-3" />
              </a>
              <ChevronDown
                class="size-4 text-muted-foreground transition-transform group-open:rotate-180"
                aria-hidden="true"
              />
            </summary>

            <div class="border-t border-border px-4 py-4">
              <ConditionOutcomes definition={snapshot.definition} {report} {descriptors} />

              {#if Object.keys(report.pendingData).length > 0 || Object.keys(report.unavailableData).length > 0}
                <h3 class="mt-4 text-xs font-medium tracking-wide text-muted-foreground uppercase">Data details</h3>
                <div
                  class="mt-2 space-y-1 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-600 dark:text-amber-400"
                >
                  {#each Object.entries(report.pendingData) as [timeframe, health] (timeframe)}
                    <p>{timeframe}: {pendingCandleMessage(health)}</p>
                  {/each}
                  {#each Object.entries(report.unavailableData) as [timeframe, message] (timeframe)}
                    <p>{timeframe}: {message}</p>
                  {/each}
                </div>
              {/if}
            </div>
          </details>
        {/each}
      </div>
    {/if}
  </div>
</div>
