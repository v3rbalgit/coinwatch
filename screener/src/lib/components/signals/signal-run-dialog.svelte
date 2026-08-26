<script lang="ts">
  import { Globe, List, Play, X } from "@lucide/svelte"
  import { Dialog } from "bits-ui"

  import Button from "$lib/components/ui/button.svelte"
  import type { SignalRunScope } from "$lib/signals/run"
  import type { SignalWatchlist } from "$lib/signals/watchlists"

  type ScopeChoice = { type: "universe" } | { type: "watchlist"; name: string }
  const scopeFieldName = "signal-run-scope"

  let {
    open = $bindable(false),
    signalName,
    watchlists,
    watchlistError = "",
    replacesPrevious = false,
    onRun,
  }: {
    open?: boolean
    signalName: string
    watchlists: SignalWatchlist[]
    watchlistError?: string
    replacesPrevious?: boolean
    onRun: (scope: SignalRunScope, scopeLabel: string) => void
  } = $props()

  let choice = $state<ScopeChoice>({ type: "universe" })
  let wasOpen = $state(false)

  $effect(() => {
    if (open && !wasOpen) choice = { type: "universe" }
    if (open && choice.type === "watchlist") {
      const name = choice.name
      const selected = watchlists.find(watchlist => watchlist.name === name)
      if (selected === undefined || selected.coins.length === 0) choice = { type: "universe" }
    }
    wasOpen = open
  })

  function startRun() {
    if (choice.type === "universe") {
      open = false
      onRun({ type: "universe" }, "Entire universe")
      return
    }
    const name = choice.name
    const watchlist = watchlists.find(candidate => candidate.name === name)
    if (watchlist === undefined || watchlist.coins.length === 0) return
    open = false
    onRun({ type: "coins", coins: [...watchlist.coins] }, watchlist.name)
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Portal>
    <Dialog.Overlay class="fixed inset-0 z-40 bg-background/80 backdrop-blur-sm" />
    <Dialog.Content
      class="fixed top-1/2 left-1/2 z-50 flex max-h-[min(42rem,calc(100vh-2rem))] w-[min(34rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-xl border border-border bg-card text-card-foreground shadow-2xl focus:outline-none"
    >
      <header class="flex items-start justify-between gap-3 border-b border-border px-5 py-4">
        <div>
          <Dialog.Title class="text-base font-semibold">Run {signalName}</Dialog.Title>
          <Dialog.Description class="mt-1 text-sm text-muted-foreground">
            The current definition is copied into this run without saving it.
            {#if replacesPrevious}
              <span class="mt-1 block">Starting a new run replaces the latest in-memory results.</span>
            {/if}
          </Dialog.Description>
        </div>
        <Dialog.Close
          class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
          aria-label="Close run dialog"
        >
          <X class="size-4" />
        </Dialog.Close>
      </header>

      <div class="scrollable min-h-0 flex-1 overflow-auto p-5">
        <fieldset class="space-y-2">
          <legend class="mb-3 text-xs font-medium tracking-wide text-muted-foreground uppercase">Run scope</legend>
          <label
            class="flex cursor-pointer items-center gap-3 rounded-lg border p-3 transition-colors {choice.type ===
            'universe'
              ? 'border-primary bg-primary/10'
              : 'border-border hover:bg-accent/40'}"
          >
            <input
              type="radio"
              name={scopeFieldName}
              checked={choice.type === "universe"}
              onchange={() => (choice = { type: "universe" })}
            />
            <Globe class="size-4 text-muted-foreground" />
            <span class="min-w-0 flex-1">
              <span class="block text-sm font-medium text-foreground">Entire universe</span>
              <span class="block text-xs text-muted-foreground">Evaluate every currently available market.</span>
            </span>
          </label>

          {#each watchlists as watchlist (watchlist.name)}
            <label
              class="flex items-center gap-3 rounded-lg border p-3 transition-colors {watchlist.coins.length === 0
                ? 'cursor-not-allowed opacity-50'
                : choice.type === 'watchlist' && choice.name === watchlist.name
                  ? 'cursor-pointer border-primary bg-primary/10'
                  : 'cursor-pointer border-border hover:bg-accent/40'}"
            >
              <input
                type="radio"
                name={scopeFieldName}
                checked={choice.type === "watchlist" && choice.name === watchlist.name}
                disabled={watchlist.coins.length === 0}
                onchange={() => (choice = { type: "watchlist", name: watchlist.name })}
              />
              <List class="size-4 text-muted-foreground" />
              <span class="min-w-0 flex-1">
                <span class="block truncate text-sm font-medium text-foreground">{watchlist.name}</span>
                <span class="block text-xs text-muted-foreground">
                  {watchlist.coins.length}
                  {watchlist.coins.length === 1 ? "market" : "markets"}
                  {watchlist.coins.length === 0 ? " · Empty lists cannot be run" : ""}
                </span>
              </span>
            </label>
          {/each}
        </fieldset>

        {#if watchlists.length === 0 && !watchlistError}
          <p class="mt-3 rounded-md border border-dashed border-border px-3 py-2 text-xs text-muted-foreground">
            No watchlists yet. Create one from Browse or run the entire universe.
          </p>
        {/if}
        {#if watchlistError}
          <p class="mt-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            Watchlists are unavailable: {watchlistError}
          </p>
        {/if}
      </div>

      <footer class="flex justify-end gap-2 border-t border-border px-5 py-4">
        <Button variant="ghost" onclick={() => (open = false)}>Cancel</Button>
        <Button onclick={startRun}><Play class="size-4" />Run signal</Button>
      </footer>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
