<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { ArrowLeft, LoaderCircle } from "@lucide/svelte";
  import { onMount } from "svelte";

  import Chart, { type Bar } from "$lib/chart/Chart.svelte";
  import { fmtPrice } from "$lib/chart/theme";
  import Button from "$lib/components/ui/button.svelte";
  import CoinIcon from "$lib/components/ui/coin-icon.svelte";
  import Skeleton from "$lib/components/ui/skeleton.svelte";

  type Row = {
    coin: string;
    price: number;
    prev_day: number;
    volume_usd: number;
    open_interest_usd: number;
    funding: number;
  };
  type CandleEvent = { coin: string; timeframe: string; candle: Bar };

  const timeframes = ["1h", "4h", "1d", "1w"] as const;
  const coin = $derived($page.params.coin ?? "");
  let tf = $state<(typeof timeframes)[number]>("1h");
  let bars = $state<Bar[]>([]);
  let seed = $state(0);
  let meta = $state<Row | null>(null);
  let livePrice = $state<number | null>(null);

  async function loadCandles() {
    const [c, t] = [coin, tf];
    const data = await invoke<Bar[]>("coin_candles", { coin: c, timeframe: t });
    // Ignore a stale response if the coin/timeframe changed while it was in flight.
    if (c === coin && t === tf) {
      bars = data;
      seed += 1;
    }
  }
  async function loadMeta() {
    const rows = await invoke<Row[]>("screener_rows");
    meta = rows.find((row) => row.coin === coin) ?? null;
  }

  function mergeBar(candle: Bar) {
    const last = bars.at(-1);
    if (last && last.time === candle.time) bars = [...bars.slice(0, -1), candle];
    else if (last && candle.time < last.time) return;
    else bars = [...bars, candle];
  }

  // Stream only the viewed coin's live candles from the backend.
  $effect(() => {
    if (!coin) return;
    invoke("set_active_coin", { coin });
    return () => {
      invoke("set_active_coin", { coin: null });
    };
  });

  // Reseed the chart whenever the coin or timeframe changes.
  $effect(() => {
    void coin;
    void tf;
    if (coin) loadCandles();
  });

  onMount(() => {
    loadMeta();
    const reload = setInterval(loadMeta, 5000);
    const unMid = listen<{ coin: string; price: number }>("mid", (event) => {
      if (event.payload.coin !== coin) return;
      livePrice = event.payload.price;
      const last = bars.at(-1);
      if (!last) return;
      const close = event.payload.price;
      bars = [
        ...bars.slice(0, -1),
        { ...last, close, high: Math.max(last.high, close), low: Math.min(last.low, close) },
      ];
    });
    const unCandle = listen<CandleEvent>("candle", (event) => {
      if (event.payload.coin !== coin || event.payload.timeframe !== tf) return;
      mergeBar(event.payload.candle);
    });
    return () => {
      clearInterval(reload);
      void unMid.then((off) => off());
      void unCandle.then((off) => off());
    };
  });

  const price = $derived(livePrice ?? meta?.price ?? 0);
  const change = $derived(meta && meta.prev_day > 0 ? (price / meta.prev_day - 1) * 100 : 0);
  const hasPrice = $derived(livePrice !== null || meta !== null);
  const fmtUsd = (n: number) =>
    n >= 1e9 ? (n / 1e9).toFixed(2) + "B" : n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(0) + "k" : n.toFixed(0);
</script>

<div class="flex h-full flex-col gap-3 p-4">
  <header class="flex flex-shrink-0 items-center gap-3">
    <Button variant="ghost" size="sm" onclick={() => goto("/browse")}>
      <ArrowLeft class="size-4" />
      Back
    </Button>
    <CoinIcon {coin} size={28} />
    <h1 class="text-lg font-semibold text-foreground">{coin}</h1>
    {#if hasPrice}
      <span class="text-lg tabular-nums text-foreground">{fmtPrice(price)}</span>
      <span
        class="tabular-nums {change > 0 ? 'text-green-500' : change < 0 ? 'text-red-500' : 'text-muted-foreground'}"
      >
        {change >= 0 ? "+" : ""}{change.toFixed(2)}%
      </span>
    {:else}
      <Skeleton class="h-6 w-24" />
      <Skeleton class="h-5 w-14" />
    {/if}
    <div class="ml-auto flex items-center gap-1">
      {#each timeframes as t (t)}
        <button
          class="rounded px-2.5 py-1 text-xs font-medium {tf === t
            ? 'bg-accent text-accent-foreground'
            : 'text-muted-foreground hover:bg-accent/50'}"
          onclick={() => (tf = t)}
        >
          {t}
        </button>
      {/each}
    </div>
  </header>

  <div class="flex flex-shrink-0 items-center gap-6 text-xs text-muted-foreground">
    {#if meta}
      <span>Vol <span class="tabular-nums text-foreground">{fmtUsd(meta.volume_usd)}</span></span>
      <span>OI <span class="tabular-nums text-foreground">{fmtUsd(meta.open_interest_usd)}</span></span>
      <span>
        Funding
        <span class="tabular-nums {meta.funding > 0 ? 'text-green-500' : meta.funding < 0 ? 'text-red-500' : 'text-foreground'}">
          {(meta.funding * 100).toFixed(4)}%
        </span>
      </span>
    {:else}
      <Skeleton class="h-4 w-24" />
      <Skeleton class="h-4 w-24" />
      <Skeleton class="h-4 w-28" />
    {/if}
  </div>

  <div class="relative min-h-0 flex-1 overflow-hidden rounded-lg border border-border">
    <Chart {bars} {seed} />
    {#if bars.length === 0}
      <div class="absolute inset-0 flex items-center justify-center gap-2 text-sm text-muted-foreground">
        <LoaderCircle class="size-4 animate-spin" />
        Loading chart…
      </div>
    {/if}
  </div>
</div>
