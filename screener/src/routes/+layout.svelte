<script lang="ts">
  import "../app.css"

  import { onMount, type Snippet } from "svelte"

  import { page } from "$app/state"
  import { candleEngineIsDegraded, storageIsDegraded, type MarketEngineStatus } from "$lib/engine-status"
  import { marketStatus, onStatus } from "$lib/ipc/events"

  const { children }: { children: Snippet } = $props()

  let status = $state<MarketEngineStatus>({
    connection: "connecting",
    phase: "loading",
    universe: 0,
    series_done: 0,
    series_total: 0,
    series_errors: 0,
    streaming: 0,
    series_jobs_per_minute: 0,
    storage_mode: "initializing",
    storage_failures: 0,
    last_storage_error: null,
    reconciliation_failures: 0,
    last_reconciliation_error: null,
  })

  const connDot = $derived(
    status.connection === "live"
      ? "bg-green-500"
      : status.connection === "reconnecting"
        ? "bg-amber-500 animate-pulse"
        : "bg-muted-foreground animate-pulse"
  )
  const connLabel = $derived(
    status.connection === "live" ? "Live marks" : status.connection === "reconnecting" ? "Reconnecting…" : "Connecting…"
  )

  const candleDegraded = $derived(candleEngineIsDegraded(status))
  const storageDegraded = $derived(storageIsDegraded(status))
  const storageLabel = $derived(
    status.storage_mode === "temporary"
      ? "temporary"
      : status.storage_mode === "initializing"
        ? "starting…"
        : storageDegraded
          ? `degraded · ${status.storage_failures}`
          : "local"
  )
  const pct = $derived(status.series_total > 0 ? Math.round((status.series_done / status.series_total) * 100) : 0)
  const etaMin = $derived(
    status.series_total - status.series_done > 0 && status.series_jobs_per_minute > 0
      ? Math.ceil((status.series_total - status.series_done) / status.series_jobs_per_minute)
      : 0
  )

  onMount(() => {
    let active = true
    let eventReceived = false
    const unlisten = onStatus(payload => {
      if (!active) return
      eventReceived = true
      status = payload
    })
    void unlisten
      .then(() => marketStatus())
      .then(snapshot => {
        if (active && !eventReceived) status = snapshot
      })
      .catch(error => console.error("failed to initialize engine status", error))
    return () => {
      active = false
      void unlisten.then(off => off())
    }
  })

  const nav = [
    { href: "/", label: "Home" },
    { href: "/browse", label: "Browse" },
    { href: "/signals", label: "Signals" },
  ]
</script>

<div class="flex h-screen">
  <aside class="flex w-52 shrink-0 flex-col border-r border-border bg-card">
    <div class="flex h-14 items-center border-b border-border px-4">
      <span class="text-sm font-semibold tracking-tight text-primary">HL&nbsp;Screener</span>
    </div>
    <nav class="flex-1 space-y-1 p-2">
      {#each nav as item (item.href)}
        <a
          href={item.href}
          class="block rounded-md px-3 py-2 text-sm transition-colors {page.url.pathname === item.href ||
          (item.href !== '/' && page.url.pathname.startsWith(item.href + '/'))
            ? 'bg-accent text-accent-foreground'
            : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground'}"
        >
          {item.label}
        </a>
      {/each}
    </nav>
    <div class="space-y-2 border-t border-border px-4 py-3 text-xs">
      <div class="flex items-center gap-2">
        <span class="h-2 w-2 rounded-full {connDot}"></span>
        <span class="font-medium text-foreground">{connLabel}</span>
      </div>
      <div class="flex items-center justify-between text-muted-foreground">
        <span>Markets</span><span class="text-foreground tabular-nums">{status.universe}</span>
      </div>
      <div
        class="flex items-center justify-between text-muted-foreground"
        title={status.last_reconciliation_error ?? undefined}
      >
        <span>History series</span>
        {#if status.phase === "syncing_history"}
          <span class="text-foreground tabular-nums">
            {status.series_done}/{status.series_total}{status.series_errors
              ? ` · ${status.series_errors} failed`
              : etaMin
                ? ` · ~${etaMin}m`
                : ""}
          </span>
        {:else if status.phase === "live"}
          <span class={candleDegraded ? "text-amber-500" : "text-green-500"}>
            {candleDegraded ? `degraded · ${status.reconciliation_failures}` : "current"}
          </span>
        {:else}
          <span class={candleDegraded ? "text-amber-500" : ""}>{candleDegraded ? "retrying…" : "planning…"}</span>
        {/if}
      </div>
      {#if status.phase === "syncing_history"}
        <div class="h-1 w-full overflow-hidden rounded-full bg-muted">
          <div class="h-full bg-primary transition-all" style="width: {pct}%"></div>
        </div>
      {/if}
      <div
        class="flex items-center justify-between text-muted-foreground"
        title={status.last_storage_error ?? undefined}
      >
        <span>Storage</span>
        <span class={storageDegraded ? "text-amber-500" : "text-foreground"}>{storageLabel}</span>
      </div>
      {#if status.storage_mode === "temporary"}
        <p
          class="rounded-md border border-amber-500/30 bg-amber-500/10 px-2 py-1.5 leading-relaxed text-amber-700 dark:text-amber-300"
        >
          Local storage is unavailable. Changes are temporary.
        </p>
      {:else if status.last_storage_error}
        <p
          class="rounded-md border border-amber-500/30 bg-amber-500/10 px-2 py-1.5 leading-relaxed text-amber-700 dark:text-amber-300"
        >
          Recent local data could not be saved.
        </p>
      {/if}
    </div>
  </aside>

  <main class="min-w-0 flex-1 overflow-hidden">
    {@render children()}
  </main>
</div>
