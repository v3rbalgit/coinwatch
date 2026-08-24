<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  import Badge from "$lib/components/ui/badge.svelte";
  import Button from "$lib/components/ui/button.svelte";

  type Signal = {
    coin: string;
    timeframe: string;
    source: string;
    kind: string;
    direction: string;
    at: string;
  };

  let signals = $state<Signal[]>([]);
  let scanning = $state(false);
  let total = $state(0);
  let done = $state(0);
  let failures = $state(0);

  function fmtTime(iso: string): string {
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? iso : date.toLocaleString();
  }

  onMount(() => {
    const subs = [
      listen<number>("scan:started", (event) => {
        signals = [];
        failures = 0;
        total = event.payload;
        done = 0;
        scanning = true;
      }),
      listen<Signal>("scan:signal", (event) => {
        signals = [...signals, event.payload];
      }),
      listen<number>("scan:progress", (event) => {
        done = event.payload;
      }),
      listen("scan:failure", () => {
        failures += 1;
      }),
      listen("scan:done", () => {
        scanning = false;
      }),
      listen("scan:error", () => {
        scanning = false;
      }),
    ];
    return () => {
      for (const sub of subs) void sub.then((off) => off());
    };
  });

  async function startScan() {
    if (scanning) return;
    scanning = true;
    signals = [];
    try {
      await invoke("start_scan");
    } catch {
      scanning = false;
    }
  }
</script>

<div class="flex h-full flex-col p-4">
  <div class="flex flex-wrap items-center gap-3 pb-1">
    <h1 class="text-base font-semibold tracking-tight text-primary">Signals</h1>
    <Button size="sm" onclick={startScan} disabled={scanning}>
      {scanning ? `Scanning ${done}/${total}…` : "Scan"}
    </Button>
    <span class="text-sm text-muted-foreground">
      {signals.length} signals{failures ? ` · ${failures} failed` : ""}
    </span>
  </div>

  <p class="mt-1 text-xs text-muted-foreground">
    RSI &amp; MACD divergences (regular + hidden) on 1h / 4h / 1d / 1w &mdash; freshest setups only.
  </p>

  <div class="mt-4 min-h-0 flex-1 overflow-auto rounded-lg border border-border">
    {#if signals.length === 0}
      <div class="flex h-full items-center justify-center text-sm text-muted-foreground">
        {scanning ? "Scanning the universe…" : "Run a scan to surface divergence setups."}
      </div>
    {:else}
      <table class="w-full text-sm">
        <thead class="sticky top-0 bg-card text-left text-xs text-muted-foreground">
          <tr>
            <th class="px-3 py-2 font-medium">Coin</th>
            <th class="px-3 py-2 font-medium">TF</th>
            <th class="px-3 py-2 font-medium">Signal</th>
            <th class="px-3 py-2 font-medium">Direction</th>
            <th class="px-3 py-2 font-medium">When</th>
          </tr>
        </thead>
        <tbody>
          {#each signals as s (s.coin + s.timeframe + s.source + s.kind + s.at)}
            <tr class="border-t border-border/60 hover:bg-accent/40">
              <td class="px-3 py-1.5 font-medium text-foreground">{s.coin}</td>
              <td class="px-3 py-1.5 text-muted-foreground">{s.timeframe}</td>
              <td class="px-3 py-1.5">{s.source} &middot; {s.kind}</td>
              <td class="px-3 py-1.5">
                <Badge variant={s.direction === "Bullish" ? "success" : "destructive"}>{s.direction}</Badge>
              </td>
              <td class="px-3 py-1.5 text-muted-foreground">{fmtTime(s.at)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>
