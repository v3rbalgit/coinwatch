<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  import Button from "$lib/components/ui/button.svelte";
  import Card from "$lib/components/ui/card.svelte";
  import CoinIcon from "$lib/components/ui/coin-icon.svelte";

  type Row = {
    coin: string;
    price: number;
    prev_day: number;
    volume_usd: number;
    open_interest_usd: number;
    funding: number;
  };

  const coin = $derived($page.params.coin ?? "");
  let meta = $state<Row | null>(null);
  let livePrice = $state<number | null>(null);

  async function loadMeta() {
    const rows = await invoke<Row[]>("screener_rows");
    meta = rows.find((r) => r.coin === coin) ?? null;
  }

  onMount(() => {
    loadMeta();
    const reload = setInterval(loadMeta, 5000);
    const unlisten = listen<{ coin: string; price: number }>("mid", (event) => {
      if (event.payload.coin === coin) livePrice = event.payload.price;
    });
    return () => {
      clearInterval(reload);
      void unlisten.then((off) => off());
    };
  });

  const price = $derived(livePrice ?? meta?.price ?? 0);
  const change = $derived(meta && meta.prev_day > 0 ? (price / meta.prev_day - 1) * 100 : 0);
  const fmtUsd = (n: number) =>
    n >= 1e9 ? (n / 1e9).toFixed(2) + "B" : n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(0) + "k" : n.toFixed(0);
  const fmtPrice = (n: number) => (n >= 100 ? n.toFixed(2) : n >= 1 ? n.toFixed(3) : n.toPrecision(4));
</script>

<div class="flex h-full flex-col gap-4 p-4">
  <header class="flex flex-shrink-0 items-center gap-3">
    <Button variant="ghost" size="sm" onclick={() => goto("/browse")}>← Back</Button>
    <CoinIcon {coin} size={28} />
    <h1 class="text-lg font-semibold text-foreground">{coin}</h1>
    <span class="text-lg tabular-nums text-foreground">{fmtPrice(price)}</span>
    <span
      class="tabular-nums {change > 0 ? 'text-green-500' : change < 0 ? 'text-red-500' : 'text-muted-foreground'}"
    >
      {change >= 0 ? "+" : ""}{change.toFixed(2)}%
    </span>
  </header>

  <div class="grid min-h-0 flex-1 grid-cols-1 gap-4 lg:grid-cols-[1fr_260px]">
    <Card class="flex items-center justify-center text-sm text-muted-foreground">Chart — coming next</Card>
    <Card class="space-y-3 p-4">
      <div class="text-xs font-medium uppercase tracking-wide text-muted-foreground">Market</div>
      {#if meta}
        <div class="flex justify-between text-sm">
          <span class="text-muted-foreground">24h Volume</span><span class="tabular-nums">{fmtUsd(meta.volume_usd)}</span>
        </div>
        <div class="flex justify-between text-sm">
          <span class="text-muted-foreground">Open Interest</span>
          <span class="tabular-nums">{fmtUsd(meta.open_interest_usd)}</span>
        </div>
        <div class="flex justify-between text-sm">
          <span class="text-muted-foreground">Funding</span>
          <span class="tabular-nums {meta.funding > 0 ? 'text-green-500' : meta.funding < 0 ? 'text-red-500' : ''}">
            {(meta.funding * 100).toFixed(4)}%
          </span>
        </div>
      {:else}
        <div class="text-sm text-muted-foreground">Loading…</div>
      {/if}
    </Card>
  </div>
</div>
