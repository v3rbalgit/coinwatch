<script lang="ts">
  import { Copy, EllipsisVertical, Plus, Trash2 } from "@lucide/svelte"
  import { DropdownMenu } from "bits-ui"

  import Badge from "$lib/components/ui/badge.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import Input from "$lib/components/ui/input.svelte"
  import Skeleton from "$lib/components/ui/skeleton.svelte"
  import type { SignalCatalogEntry } from "$lib/signals/catalog"
  import { definitionToSignalDraft } from "$lib/signals/draft"

  const menuContent =
    "z-50 min-w-36 rounded-md border border-border bg-card p-1 text-foreground shadow-lg focus:outline-none"
  const menuItem =
    "flex w-full cursor-pointer select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none data-[highlighted]:bg-accent data-[highlighted]:text-accent-foreground"

  const {
    entries,
    selectedId,
    loading,
    busy = false,
    onSelect,
    onNew,
    onDuplicate,
    onDelete,
  }: {
    entries: SignalCatalogEntry[]
    selectedId: string | null
    loading: boolean
    busy?: boolean
    onSelect: (entry: SignalCatalogEntry) => void
    onNew: () => void
    onDuplicate: (entry: SignalCatalogEntry) => void
    onDelete: (entry: SignalCatalogEntry) => void
  } = $props()

  let query = $state("")
  const filtered = $derived.by(() => {
    const normalized = query.trim().toLocaleLowerCase()
    return normalized.length === 0
      ? entries
      : entries.filter(entry => entry.name.toLocaleLowerCase().includes(normalized))
  })
  const statusLabels = $derived.by(() => {
    const labels = new Map<string, string>()
    for (const entry of entries) {
      if (entry.status === "unavailable") {
        labels.set(entry.id, entry.issue.type === "unsupportedVersion" ? `v${entry.issue.version}` : "Invalid")
      } else {
        labels.set(entry.id, definitionToSignalDraft(entry.definition) === null ? "Advanced" : "Ready")
      }
    }
    return labels
  })

  function statusLabel(entry: SignalCatalogEntry): string {
    return statusLabels.get(entry.id) ?? "Unknown"
  }
</script>

<aside
  class="flex max-h-56 w-full shrink-0 flex-col border-b border-border bg-card/50 md:h-full md:max-h-none md:w-60 md:border-r md:border-b-0 lg:w-72"
>
  <div class="border-b border-border p-3">
    <div class="flex items-center justify-between gap-2">
      <div>
        <h1 class="text-sm font-semibold text-foreground">Personal signals</h1>
        <p class="text-xs text-muted-foreground">{entries.length} saved</p>
      </div>
      <Button size="sm" onclick={onNew} disabled={loading || busy}><Plus class="mr-1 size-3.5" />New</Button>
    </div>
    <Input bind:value={query} class="mt-3 h-8" placeholder="Filter signals…" aria-label="Filter signals" />
  </div>

  <div class="scrollable min-h-0 flex-1 overflow-auto p-2">
    {#if loading}
      <div class="space-y-2 p-1">
        {#each [0, 1, 2, 3, 4] as row (row)}
          <Skeleton class="h-14 w-full" />
        {/each}
      </div>
    {:else if filtered.length === 0}
      <div class="px-3 py-10 text-center text-xs leading-relaxed text-muted-foreground">
        {entries.length === 0 ? "Create your first reusable market signal." : "No signals match this filter."}
      </div>
    {:else}
      <div class="space-y-1">
        {#each filtered as entry (entry.id)}
          <div
            class="group flex items-center rounded-md border transition-colors {selectedId === entry.id
              ? 'border-primary/50 bg-primary/10'
              : 'border-transparent hover:bg-accent/50'}"
          >
            <button
              type="button"
              class="min-w-0 flex-1 px-3 py-2 text-left disabled:cursor-not-allowed disabled:opacity-60"
              disabled={busy}
              onclick={() => onSelect(entry)}
            >
              <span class="block truncate text-sm font-medium text-foreground">{entry.name}</span>
              <span class="mt-1 flex items-center gap-2">
                <Badge
                  variant={entry.status === "unavailable" ? "destructive" : "outline"}
                  class="px-1.5 py-0 text-[10px]"
                >
                  {statusLabel(entry)}
                </Badge>
                {#if entry.status === "ready"}
                  <span class="text-[11px] text-muted-foreground capitalize">{entry.definition.direction}</span>
                {/if}
              </span>
            </button>

            <DropdownMenu.Root>
              <DropdownMenu.Trigger
                class="mr-1 rounded p-1.5 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100 hover:bg-accent hover:text-foreground data-[state=open]:opacity-100"
                aria-label={`Actions for ${entry.name}`}
                disabled={busy}
              >
                <EllipsisVertical class="size-4" />
              </DropdownMenu.Trigger>
              <DropdownMenu.Portal>
                <DropdownMenu.Content class={menuContent} align="end" sideOffset={4}>
                  {#if entry.status === "ready"}
                    <DropdownMenu.Item class={menuItem} onSelect={() => onDuplicate(entry)}>
                      <Copy class="size-3.5" />Duplicate
                    </DropdownMenu.Item>
                  {/if}
                  <DropdownMenu.Item
                    class={`${menuItem} text-destructive data-[highlighted]:text-destructive`}
                    onSelect={() => onDelete(entry)}
                  >
                    <Trash2 class="size-3.5" />Delete
                  </DropdownMenu.Item>
                </DropdownMenu.Content>
              </DropdownMenu.Portal>
            </DropdownMenu.Root>
          </div>
        {/each}
      </div>
    {/if}
  </div>
</aside>
