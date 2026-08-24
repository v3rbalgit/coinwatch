<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  import Badge from "$lib/components/ui/badge.svelte";
  import Input from "$lib/components/ui/input.svelte";

  type Row = {
    coin: string;
    price: number;
    prev_day: number;
    volume_usd: number;
    open_interest_usd: number;
    funding: number;
  };
  // 24h % is derived live from the streaming price and the previous-day price.
  type DisplayRow = Row & { change_24h_pct: number };

  let rows = $state<Row[]>([]);
  let loading = $state(true);
  let error = $state("");
  let query = $state("");
  let sortKey = $state<keyof DisplayRow>("volume_usd");
  let sortDir = $state<1 | -1>(-1);
  let livePrices = $state<Record<string, number>>({});
  let live = $state(false);

  async function load() {
    loading = true;
    error = "";
    try {
      rows = await invoke<Row[]>("screener_rows");
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  }
  load();

  onMount(() => {
    const unlisten = listen<{ coin: string; price: number }>("mid", (event) => {
      live = true;
      livePrices[event.payload.coin] = event.payload.price;
    });
    // The cache updates itself; reload rows quietly so newly-synced coins appear.
    const reload = setInterval(load, 5000);
    return () => {
      clearInterval(reload);
      void unlisten.then((off) => off());
    };
  });

  function sortBy(key: keyof DisplayRow) {
    if (sortKey === key) sortDir = (sortDir === 1 ? -1 : 1) as 1 | -1;
    else {
      sortKey = key;
      sortDir = -1;
    }
  }

  const filtered = $derived.by(() => {
    const q = query.trim().toLowerCase();
    const mapped: DisplayRow[] = rows.map((row) => {
      const price = livePrices[row.coin] ?? row.price;
      return { ...row, change_24h_pct: row.prev_day > 0 ? (price / row.prev_day - 1) * 100 : 0 };
    });
    const r = q ? mapped.filter((x) => x.coin.toLowerCase().includes(q)) : mapped;
    r.sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * sortDir;
      return String(av).localeCompare(String(bv)) * sortDir;
    });
    return r;
  });

  const fmtUsd = (n: number) =>
    n >= 1e9 ? (n / 1e9).toFixed(2) + "B" : n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(0) + "k" : n.toFixed(0);
  const fmtPrice = (n: number) => (n >= 100 ? n.toFixed(2) : n >= 1 ? n.toFixed(3) : n.toPrecision(4));

  const cols: { key: keyof DisplayRow; label: string; num: boolean }[] = [
    { key: "coin", label: "Coin", num: false },
    { key: "price", label: "Price", num: true },
    { key: "change_24h_pct", label: "24h %", num: true },
    { key: "volume_usd", label: "Volume", num: true },
    { key: "open_interest_usd", label: "OI", num: true },
    { key: "funding", label: "Funding", num: true },
  ];
</script>

<div class="flex h-full flex-col p-4">
  <header class="flex flex-shrink-0 items-center justify-between gap-4 pb-3 pt-1">
    <h1 class="text-base font-semibold tracking-tight text-primary">Hyperliquid Screener</h1>
    <div class="flex items-center gap-3">
      {#if live}<Badge variant="success">● live</Badge>{/if}
      <Input placeholder="Filter coin…" bind:value={query} class="h-8 w-44" />
      <span class="text-xs tabular-nums text-muted-foreground">{filtered.length} / {rows.length}</span>
    </div>
  </header>

  {#if loading && rows.length === 0}
    <p class="py-8 text-sm text-muted-foreground">Warming up the market cache…</p>
  {:else if error}
    <p class="py-8 text-sm text-destructive">{error}</p>
    {#if error.includes("__TAURI") || error.includes("invoke")}
      <p class="text-sm text-muted-foreground">
        Open the desktop window from <code class="rounded bg-muted px-1 py-0.5">pnpm tauri dev</code> — the backend isn't
        reachable from the plain browser preview.
      </p>
    {/if}
  {:else if rows.length === 0}
    <p class="py-8 text-sm text-muted-foreground">Warming up the market cache…</p>
  {:else}
    <div class="min-h-0 flex-1 overflow-auto rounded-lg border border-border">
      <table class="w-full text-sm tabular-nums">
        <thead class="sticky top-0 bg-card text-left text-xs text-muted-foreground">
          <tr>
            {#each cols as c (c.key)}
              <th
                class="cursor-pointer select-none whitespace-nowrap px-3 py-2 font-medium hover:text-foreground {c.num
                  ? 'text-right'
                  : ''}"
                onclick={() => sortBy(c.key)}
              >
                {c.label}{sortKey === c.key ? (sortDir === 1 ? " ▲" : " ▼") : ""}
              </th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#each filtered as r (r.coin)}
            <tr class="border-t border-border/60 hover:bg-accent/40">
              <td class="whitespace-nowrap px-3 py-1.5 font-medium text-foreground">{r.coin}</td>
              <td class="px-3 py-1.5 text-right">{fmtPrice(livePrices[r.coin] ?? r.price)}</td>
              <td
                class="px-3 py-1.5 text-right {r.change_24h_pct > 0
                  ? 'text-green-500'
                  : r.change_24h_pct < 0
                    ? 'text-red-500'
                    : ''}"
              >
                {r.change_24h_pct.toFixed(2)}%
              </td>
              <td class="px-3 py-1.5 text-right text-muted-foreground">{fmtUsd(r.volume_usd)}</td>
              <td class="px-3 py-1.5 text-right text-muted-foreground">{fmtUsd(r.open_interest_usd)}</td>
              <td
                class="px-3 py-1.5 text-right {r.funding > 0 ? 'text-green-500' : r.funding < 0 ? 'text-red-500' : ''}"
              >
                {(r.funding * 100).toFixed(4)}%
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>
