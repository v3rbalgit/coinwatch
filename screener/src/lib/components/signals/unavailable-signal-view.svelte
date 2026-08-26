<script lang="ts">
  import { Trash2, TriangleAlert } from "@lucide/svelte"

  import Button from "$lib/components/ui/button.svelte"
  import type { UnavailableSignalCatalogEntry } from "$lib/signals/editor"

  const {
    entry,
    busy = false,
    error = "",
    onDelete,
  } = $props<{
    entry: UnavailableSignalCatalogEntry
    busy?: boolean
    error?: string
    onDelete: () => void
  }>()

  const issue = $derived(
    entry.issue.type === "unsupportedVersion"
      ? `Definition version ${entry.issue.version} is not supported by this application.`
      : entry.issue.message
  )
</script>

<div class="flex h-full min-w-0 flex-1 items-center justify-center bg-muted/20 p-6">
  <div class="w-full max-w-xl rounded-xl border border-destructive/30 bg-card p-6 shadow-sm">
    <div class="flex items-start gap-3">
      <div class="rounded-full bg-destructive/10 p-2 text-destructive"><TriangleAlert class="size-5" /></div>
      <div class="min-w-0 flex-1">
        <h1 class="text-base font-semibold text-foreground">{entry.name}</h1>
        <p class="mt-1 text-sm leading-relaxed text-muted-foreground">{issue}</p>
        <p class="mt-3 text-xs leading-relaxed text-muted-foreground">
          The catalog row remains visible so it can be recovered by a future version or deleted deliberately. It cannot
          be edited or duplicated without a valid typed definition.
        </p>
      </div>
    </div>
    {#if error}<p class="mt-4 text-sm text-destructive">{error}</p>{/if}
    <div class="mt-5 flex justify-end">
      <Button variant="destructive" onclick={onDelete} disabled={busy}>
        <Trash2 class="size-4" />Delete unavailable signal
      </Button>
    </div>
  </div>
</div>
