<script lang="ts">
  import { goto } from "$app/navigation";
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { DropdownMenu } from "bits-ui";
  import { Check, ChevronDown, EllipsisVertical, ExternalLink, Plus, ScanSearch, X } from "@lucide/svelte";
  import { onMount } from "svelte";

  import Badge from "$lib/components/ui/badge.svelte";
  import Button from "$lib/components/ui/button.svelte";
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
  type ListView = { name: string; coins: string[] };
  type SortKey = "coin" | "price" | "change_24h_pct" | "volume_usd" | "open_interest_usd" | "funding";

  let rows = $state<Row[]>([]);
  let lists = $state<ListView[]>([]);
  let activeList = $state<string | null>(null);
  let loading = $state(true);
  let error = $state("");
  let query = $state("");
  let sortKey = $state<SortKey>("volume_usd");
  let sortDir = $state<1 | -1>(-1);
  let livePrices = $state<Record<string, number>>({});
  let live = $state(false);
  let toast = $state("");

  // Inline list creation (window.prompt is unreliable in the webview).
  let creating = $state(false);
  let creatingCoin = $state<string | null>(null);
  let newListName = $state("");

  async function loadRows() {
    try {
      rows = await invoke<Row[]>("screener_rows");
      error = "";
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  }
  async function loadLists() {
    lists = await invoke<ListView[]>("lists");
  }
  loadRows();
  loadLists();

  onMount(() => {
    const unlisten = listen<{ coin: string; price: number }>("mid", (event) => {
      live = true;
      livePrices[event.payload.coin] = event.payload.price;
    });
    const reload = setInterval(loadRows, 5000);
    return () => {
      clearInterval(reload);
      void unlisten.then((off) => off());
    };
  });

  // coin -> the lists it belongs to (drives the row menu checkmarks).
  const coinLists = $derived.by(() => {
    const map = new Map<string, Set<string>>();
    for (const list of lists) {
      for (const coin of list.coins) {
        (map.get(coin) ?? map.set(coin, new Set()).get(coin))!.add(list.name);
      }
    }
    return map;
  });
  const activeCoins = $derived(
    activeList === null ? null : new Set(lists.find((l) => l.name === activeList)?.coins ?? []),
  );

  function sortBy(key: SortKey) {
    if (sortKey === key) sortDir = (sortDir === 1 ? -1 : 1) as 1 | -1;
    else {
      sortKey = key;
      sortDir = -1;
    }
  }

  const change = (row: Row, price: number) => (row.prev_day > 0 ? (price / row.prev_day - 1) * 100 : 0);

  // Depends only on rows/query/list/sort — never livePrices — so a price tick
  // updates only the affected cells rather than re-sorting the whole table.
  const sorted = $derived.by(() => {
    const q = query.trim().toLowerCase();
    const coins = activeCoins;
    const list = rows.filter((r) => (!q || r.coin.toLowerCase().includes(q)) && (!coins || coins.has(r.coin)));
    list.sort((a, b) => {
      if (sortKey === "coin") return a.coin.localeCompare(b.coin) * sortDir;
      const av = sortKey === "change_24h_pct" ? change(a, a.price) : a[sortKey];
      const bv = sortKey === "change_24h_pct" ? change(b, b.price) : b[sortKey];
      return (av - bv) * sortDir;
    });
    return list;
  });

  async function toggleMembership(list: string, coin: string) {
    if (coinLists.get(coin)?.has(list)) await invoke("remove_from_list", { list, coin });
    else await invoke("add_to_list", { list, coin });
    await loadLists();
  }
  function startNewList(coin: string | null) {
    creating = true;
    creatingCoin = coin;
    newListName = "";
  }
  async function submitNewList() {
    const name = newListName.trim();
    creating = false;
    if (!name) return;
    if (creatingCoin) await invoke("add_to_list", { list: name, coin: creatingCoin });
    else await invoke("create_list", { name });
    creatingCoin = null;
    await loadLists();
    activeList = name;
  }
  async function deleteList(name: string) {
    await invoke("delete_list", { name });
    if (activeList === name) activeList = null;
    await loadLists();
  }
  async function scanCoin(coin: string) {
    const signals = await invoke<unknown[]>("scan_coin", { coin });
    flash(signals.length ? `${coin}: ${signals.length} divergence(s)` : `${coin}: no divergences`);
  }
  function flash(message: string) {
    toast = message;
    setTimeout(() => (toast = message === toast ? "" : toast), 3000);
  }

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

  const menuContent =
    "z-50 min-w-[13rem] rounded-md border border-border bg-card p-1 text-foreground shadow-lg focus:outline-none";
  const menuItem =
    "flex w-full cursor-pointer select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none data-[highlighted]:bg-accent data-[highlighted]:text-accent-foreground";
</script>

<div class="flex h-full flex-col p-4">
  <header class="flex flex-shrink-0 items-center justify-between gap-4 pb-3 pt-1">
    <div class="flex items-center gap-2">
      <!-- List selector -->
      <DropdownMenu.Root>
        <DropdownMenu.Trigger
          class="flex h-8 items-center gap-1.5 rounded-md border border-border px-2.5 text-sm font-semibold text-primary hover:bg-accent/50"
        >
          {activeList ?? "All"}
          <ChevronDown class="size-4 text-muted-foreground" />
        </DropdownMenu.Trigger>
        <DropdownMenu.Portal>
          <DropdownMenu.Content class={menuContent} align="start" sideOffset={4}>
            <DropdownMenu.Item class={menuItem} onSelect={() => (activeList = null)}>
              <Check class="size-3.5 {activeList === null ? 'opacity-100' : 'opacity-0'}" />
              <span class="flex-1">All</span>
              <span class="text-xs text-muted-foreground">{rows.length}</span>
            </DropdownMenu.Item>
            {#each lists as list (list.name)}
              <DropdownMenu.Item class={menuItem} onSelect={() => (activeList = list.name)}>
                <Check class="size-3.5 {activeList === list.name ? 'opacity-100' : 'opacity-0'}" />
                <span class="flex-1">{list.name}</span>
                <span class="text-xs text-muted-foreground">{list.coins.length}</span>
                <button
                  class="ml-1 rounded p-0.5 text-muted-foreground hover:text-destructive"
                  aria-label="Delete list"
                  onpointerdown={(e) => e.stopPropagation()}
                  onclick={(e) => {
                    e.stopPropagation();
                    e.preventDefault();
                    deleteList(list.name);
                  }}
                >
                  <X class="size-3.5" />
                </button>
              </DropdownMenu.Item>
            {/each}
            <DropdownMenu.Separator class="my-1 h-px bg-border" />
            <DropdownMenu.Item class={menuItem} onSelect={() => startNewList(null)}>
              <Plus class="size-3.5" />
              New list…
            </DropdownMenu.Item>
          </DropdownMenu.Content>
        </DropdownMenu.Portal>
      </DropdownMenu.Root>

      {#if creating}
        <form
          class="flex items-center gap-1"
          onsubmit={(e) => {
            e.preventDefault();
            submitNewList();
          }}
        >
          <Input bind:value={newListName} placeholder="List name…" class="h-8 w-36" />
          <Button size="sm" type="submit">Create</Button>
          <Button size="sm" variant="ghost" onclick={() => (creating = false)}>Cancel</Button>
        </form>
      {/if}
    </div>

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
    <div class="scrollable min-h-0 flex-1 overflow-auto rounded-lg border border-border">
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
            <th class="w-8"></th>
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
              <td class="py-1.5 pl-1 pr-3 text-right">
                <div class="flex items-center justify-end gap-0.5" onclick={(e) => e.stopPropagation()} role="none">
                  {#if activeList !== null}
                    <button
                      class="rounded p-1 text-muted-foreground hover:text-destructive"
                      aria-label="Remove from {activeList}"
                      onclick={() => toggleMembership(activeList!, row.coin)}
                    >
                      <X class="size-4" />
                    </button>
                  {/if}
                  <DropdownMenu.Root>
                    <DropdownMenu.Trigger
                      class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                      aria-label="Actions"
                    >
                      <EllipsisVertical class="size-4" />
                    </DropdownMenu.Trigger>
                    <DropdownMenu.Portal>
                      <DropdownMenu.Content class={menuContent} align="end" sideOffset={4}>
                        <DropdownMenu.Group>
                          <div class="px-2 py-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                            Add to list
                          </div>
                          {#each lists as list (list.name)}
                            <DropdownMenu.Item
                              class={menuItem}
                              closeOnSelect={false}
                              onSelect={() => toggleMembership(list.name, row.coin)}
                            >
                              <Check
                                class="size-3.5 {coinLists.get(row.coin)?.has(list.name) ? 'opacity-100' : 'opacity-0'}"
                              />
                              <span class="flex-1">{list.name}</span>
                            </DropdownMenu.Item>
                          {/each}
                          <DropdownMenu.Item class={menuItem} onSelect={() => startNewList(row.coin)}>
                            <Plus class="size-3.5" />
                            New list…
                          </DropdownMenu.Item>
                        </DropdownMenu.Group>
                        <DropdownMenu.Separator class="my-1 h-px bg-border" />
                        <DropdownMenu.Item class={menuItem} onSelect={() => invoke("open_on_hyperliquid", { coin: row.coin })}>
                          <ExternalLink class="size-3.5" />
                          Open on Hyperliquid
                        </DropdownMenu.Item>
                        <DropdownMenu.Item class={menuItem} onSelect={() => scanCoin(row.coin)}>
                          <ScanSearch class="size-3.5" />
                          Scan this coin
                        </DropdownMenu.Item>
                      </DropdownMenu.Content>
                    </DropdownMenu.Portal>
                  </DropdownMenu.Root>
                </div>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}

  {#if toast}
    <div
      class="pointer-events-none fixed bottom-4 left-1/2 -translate-x-1/2 rounded-md border border-border bg-card px-3 py-1.5 text-sm text-foreground shadow-lg"
    >
      {toast}
    </div>
  {/if}
</div>
