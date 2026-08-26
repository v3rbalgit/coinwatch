<script lang="ts">
  import { Copy, Trash2 } from "@lucide/svelte"

  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import Badge from "$lib/components/ui/badge.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import type { ReadySignalCatalogEntry } from "$lib/signals/editor"
  import { definitionLines } from "$lib/signals/format"

  const {
    entry,
    descriptors,
    busy = false,
    error = "",
    onDuplicate,
    onDelete,
  } = $props<{
    entry: ReadySignalCatalogEntry
    descriptors: readonly IndicatorDescriptor[]
    busy?: boolean
    error?: string
    onDuplicate: () => void
    onDelete: () => void
  }>()

  const lines = $derived(definitionLines(entry.definition, descriptors))
</script>

<div class="flex h-full min-w-0 flex-1 flex-col">
  <header class="border-b border-border px-5 py-4">
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div>
        <div class="flex items-center gap-2">
          <h1 class="text-base font-semibold text-foreground">{entry.name}</h1>
          <Badge variant="outline">Advanced</Badge>
          <Badge variant={entry.definition.direction === "bullish" ? "success" : "destructive"}>
            {entry.definition.direction}
          </Badge>
        </div>
        <p class="mt-1 max-w-2xl text-xs leading-relaxed text-muted-foreground">
          This definition uses richer boolean logic than the flat ALL/ANY editor can represent. It remains preserved
          exactly and will become runnable in the next checkpoint.
        </p>
      </div>
      <div class="flex gap-2">
        <Button variant="outline" onclick={onDuplicate} disabled={busy}>
          <Copy class="size-4" />Duplicate
        </Button>
        <Button variant="destructive" onclick={onDelete} disabled={busy}>
          <Trash2 class="size-4" />Delete
        </Button>
      </div>
    </div>
    {#if error}
      <p class="mt-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
        {error}
      </p>
    {/if}
  </header>

  <div class="scrollable min-h-0 flex-1 overflow-auto bg-muted/20 p-5">
    <div class="mx-auto max-w-4xl rounded-xl border border-border bg-card p-5 shadow-sm">
      <h2 class="text-xs font-medium tracking-wide text-muted-foreground uppercase">Definition tree</h2>
      <div class="mt-4 space-y-2">
        {#each lines as line, index (`${index}-${line.depth}-${line.label}`)}
          <div
            class="rounded-md border border-border/60 bg-background px-3 py-2 text-sm text-foreground"
            style:margin-left={`${line.depth * 1.25}rem`}
          >
            {line.label}
          </div>
        {/each}
      </div>
    </div>
  </div>
</div>
