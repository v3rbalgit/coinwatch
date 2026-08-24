<script lang="ts">
  import "../app.css";
  import { page } from "$app/stores";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  let { children } = $props();

  type Status = {
    connection: string;
    phase: string;
    universe: number;
    backfill_done: number;
    backfill_total: number;
    backfill_errors: number;
    streaming: number;
    fetch_rate: number;
  };
  let status = $state<Status>({
    connection: "connecting",
    phase: "loading",
    universe: 0,
    backfill_done: 0,
    backfill_total: 0,
    backfill_errors: 0,
    streaming: 0,
    fetch_rate: 0,
  });

  const connDot = $derived(
    status.connection === "live"
      ? "bg-green-500"
      : status.connection === "reconnecting"
        ? "bg-amber-500 animate-pulse"
        : "bg-muted-foreground animate-pulse",
  );
  const connLabel = $derived(
    status.connection === "live"
      ? "Live prices"
      : status.connection === "reconnecting"
        ? "Reconnecting…"
        : "Connecting…",
  );

  const pct = $derived(
    status.backfill_total > 0 ? Math.round((status.backfill_done / status.backfill_total) * 100) : 0,
  );
  const etaMin = $derived(
    status.backfill_total - status.backfill_done > 0 && status.fetch_rate > 0
      ? Math.ceil((status.backfill_total - status.backfill_done) / status.fetch_rate)
      : 0,
  );

  onMount(() => {
    const unlisten = listen<Status>("status", (event) => {
      status = event.payload;
    });
    return () => void unlisten.then((off) => off());
  });

  const nav = [
    { href: "/", label: "Home" },
    { href: "/screener", label: "Screener" },
    { href: "/signals", label: "Signals" },
  ];
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
          class="block rounded-md px-3 py-2 text-sm transition-colors {$page.url.pathname === item.href
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
        <span>Markets</span><span class="tabular-nums text-foreground">{status.universe}</span>
      </div>
      <div class="flex items-center justify-between text-muted-foreground">
        <span>Candles</span>
        {#if status.phase === "backfilling"}
          <span class="tabular-nums text-foreground">
            {status.backfill_done}/{status.backfill_total}{etaMin ? ` · ~${etaMin}m` : ""}
          </span>
        {:else if status.phase === "live"}
          <span class="text-green-500">ready</span>
        {:else}
          <span>starting…</span>
        {/if}
      </div>
      {#if status.phase === "backfilling"}
        <div class="h-1 w-full overflow-hidden rounded-full bg-muted">
          <div class="h-full bg-primary transition-all" style="width: {pct}%"></div>
        </div>
      {/if}
    </div>
  </aside>

  <main class="min-w-0 flex-1 overflow-hidden">
    {@render children()}
  </main>
</div>
