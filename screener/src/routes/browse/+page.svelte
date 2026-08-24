<script lang="ts">
  import { goto } from "$app/navigation";
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  import Badge from "$lib/components/ui/badge.svelte";
  import CoinIcon from "$lib/components/ui/coin-icon.svelte";
  import Input from "$lib/components/ui/input.svelte";

  type Row = {
    coin: string;
    price: number;
    prev_day: number;
    volume_usd: number;
    open_interest_usd: number;
    funding: number;
  };
  type SortKey = "coin" | "price" | "change_24h_pct" | "volume_usd" | "open_interest_usd" | "funding";

  let rows = $state<Row[]>([]);
  let loading = $state(true);
  let error = $state("");
  let query = $state("");
  let sortKey = $state<SortKey>("volume_usd");
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
    // The cache updates itself; reload quietly so newly-synced coins appear.
    const reload = setInterval(load, 5000);
    return () => {
      clearInterval(reload);
      void unlisten.then((off) => off());
    };
  });

  function sortBy(key: SortKey) {
    if (sortKey === key) sortDir = (sortDir === 1 ? -1 : 1) as 1 | -1;
    else {
      sortKey = key;
      sortDir = -1;
    }
  }

  const change = (row: Row, price: number) => (row.prev_day > 0 ? (price / row.prev_day - 1) * 100 : 0);

  // Depends only on rows/query/sort, never livePrices, so a price tick can't
  // re-sort or re-render the table — the live overlay is applied per cell below.
  const sorted = $derived.by(() => {
    const q = query.trim().toLowerCase();
    const list = q ? rows.filter((r) => r.coin.toLowerCase().includes(q)) : rows.slice();
    list.sort((a, b) => {
      if (sortKey === "coin") return a.coin.localeCompare(b.coin) * sortDir;
      const av = sortKey === "change_24h_pct" ? change(a, a.price) : a[sortKey];
      const bv = sortKey === "change_24h_pct" ? change(b, b.price) : b[sortKey];
      return (av - bv) * sortDir;
    });
    return list;
  });

  const fmtUsd = (n: number) =>
    n >= 1e9 ? (n / 1e9).toFixed(2) + "B" : n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(0) + "k" : n.toFixed(0);
  const fmtPrice = (n: number) => (n >= 100 ? n.toFixed(2) : n >= 1 ? n.toFixed(3) : n.toPrecision(4));

  const cols: { key: SortKey; label: string; num: boolean }[] = [
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
    <h1 class="text-base font-semibold tracking-tight text-primary">Browse</h1>
    <div class="flex items-center gap-3">
      {#if live}<Badge variant="success">● live</Badge>{/if}
      <Input placeholder="Filter coin…" bind:value={query} class="h-8 w-44" />
      <span class="text-xs tabular-nums text-muted-foreground">{sorted.length} / {rows.length}</span>
    </div>
  </header>

  {#if loading && rows.length === 0}
    <p class="py-8 text-sm text-muted-foreground">Warming up the market cache…</p>
  {:else if error}
    <p class="py-8 text-sm text-destructive">{error}</p>
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
          {#each sorted as row (row.coin)}
            {@const price = livePrices[row.coin] ?? row.price}
            {@const chg = change(row, price)}
            <tr
              class="cursor-pointer border-t border-border/60 hover:bg-accent/40"
              onclick={() => goto(`/browse/${row.coin}`)}
            >
              <td class="whitespace-nowrap px-3 py-1.5 font-medium text-foreground">
                <span class="flex items-center gap-2">
                  <CoinIcon coin={row.coin} />
                  {row.coin}
                </span>
              </td>
              <td class="px-3 py-1.5 text-right">{fmtPrice(price)}</td>
              <td class="px-3 py-1.5 text-right {chg > 0 ? 'text-green-500' : chg < 0 ? 'text-red-500' : ''}">
                {chg.toFixed(2)}%
              </td>
              <td class="px-3 py-1.5 text-right text-muted-foreground">{fmtUsd(row.volume_usd)}</td>
              <td class="px-3 py-1.5 text-right text-muted-foreground">{fmtUsd(row.open_interest_usd)}</td>
              <td
                class="px-3 py-1.5 text-right {row.funding > 0 ? 'text-green-500' : row.funding < 0 ? 'text-red-500' : ''}"
              >
                {(row.funding * 100).toFixed(4)}%
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>
