<script lang="ts">
  import { Check, ChevronDown, EllipsisVertical, ExternalLink, Plus, X } from "@lucide/svelte"
  import { DropdownMenu } from "bits-ui"
  import { onMount } from "svelte"

  import { goto } from "$app/navigation"
  import { browseView, type SortKey } from "$lib/browse-view.svelte"
  import Badge from "$lib/components/ui/badge.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import CoinIcon from "$lib/components/ui/coin-icon.svelte"
  import Input from "$lib/components/ui/input.svelte"
  import Skeleton from "$lib/components/ui/skeleton.svelte"
  import { formatCompactNumber, formatPrice, prevDayChangePct } from "$lib/format"
  import { onMid } from "$lib/ipc/events"
  import {
    addToList,
    createList,
    deleteList as deleteListCommand,
    logos,
    openOnHyperliquid,
    removeFromList,
    screenerRows,
    watchlists,
  } from "$lib/ipc/screener"
  import type { ListView, ScreenerRow as Row } from "$lib/ipc/screener"

  let rows = $state<Row[]>([])
  let lists = $state<ListView[]>([])
  let logoMap = $state<Record<string, string>>({})
  let loading = $state(true)
  let error = $state("")
  const livePrices = $state<Record<string, number>>({})
  let live = $state(false)

  // Inline list creation (window.prompt is unreliable in the webview).
  let creating = $state(false)
  let creatingCoin = $state<string | null>(null)
  let newListName = $state("")

  async function loadRows() {
    try {
      rows = await screenerRows()
      error = ""
    } catch (e) {
      error = String(e)
    } finally {
      loading = false
    }
  }
  async function loadLists() {
    lists = await watchlists()
  }
  async function loadLogos() {
    logoMap = await logos()
  }
  void loadRows()
  void loadLists()
  void loadLogos()

  onMount(() => {
    const unlisten = onMid(event => {
      live = true
      livePrices[event.coin] = event.price
    })
    const reload = setInterval(loadRows, 5000)
    // Pick up logos as the background cache task fills them in this session.
    const logoReload = setInterval(loadLogos, 20000)
    return () => {
      clearInterval(reload)
      clearInterval(logoReload)
      void unlisten.then(off => off())
    }
  })

  // coin -> the lists it belongs to (drives the row menu checkmarks).
  const coinLists = $derived.by(() => {
    const map = new Map<string, Set<string>>()
    for (const list of lists) {
      for (const coin of list.coins) {
        let members = map.get(coin)
        if (!members) {
          members = new Set()
          map.set(coin, members)
        }
        members.add(list.name)
      }
    }
    return map
  })
  const activeCoins = $derived(
    browseView.activeList === null ? null : new Set(lists.find(l => l.name === browseView.activeList)?.coins ?? [])
  )

  function sortBy(key: SortKey) {
    if (browseView.sortKey === key) browseView.sortDir = browseView.sortDir === 1 ? -1 : 1
    else {
      browseView.sortKey = key
      browseView.sortDir = -1
    }
  }

  const change = (row: Row, price: number) => prevDayChangePct(row.prev_day, price)

  // Depends only on rows and the browse view — never livePrices — so a price tick
  // updates only the affected cells rather than re-sorting the whole table.
  const sorted = $derived.by(() => {
    const q = browseView.query.trim().toLowerCase()
    const coins = activeCoins
    const list = rows.filter(r => (!q || r.coin.toLowerCase().includes(q)) && (!coins || coins.has(r.coin)))
    list.sort((a, b) => {
      if (browseView.sortKey === "coin") return a.coin.localeCompare(b.coin) * browseView.sortDir
      const av = browseView.sortKey === "change_24h_pct" ? change(a, a.price) : a[browseView.sortKey]
      const bv = browseView.sortKey === "change_24h_pct" ? change(b, b.price) : b[browseView.sortKey]
      return (av - bv) * browseView.sortDir
    })
    return list
  })

  async function toggleMembership(list: string, coin: string) {
    if (coinLists.get(coin)?.has(list)) await removeFromList(list, coin)
    else await addToList(list, coin)
    await loadLists()
  }
  function startNewList(coin: string | null) {
    creating = true
    creatingCoin = coin
    newListName = ""
  }
  async function submitNewList() {
    const name = newListName.trim()
    creating = false
    if (!name) return
    if (creatingCoin) await addToList(name, creatingCoin)
    else await createList(name)
    creatingCoin = null
    await loadLists()
    browseView.activeList = name
  }
  async function deleteList(name: string) {
    await deleteListCommand(name)
    if (browseView.activeList === name) browseView.activeList = null
    await loadLists()
  }
  const cols: { key: SortKey; label: string; num: boolean }[] = [
    { key: "coin", label: "Coin", num: false },
    { key: "price", label: "Price", num: true },
    { key: "change_24h_pct", label: "24h %", num: true },
    { key: "volume_usd", label: "Volume", num: true },
    { key: "open_interest_usd", label: "OI", num: true },
    { key: "funding", label: "Funding", num: true },
  ]

  const skeletonRows = Array.from({ length: 14 }, (_, index) => index)

  const menuContent =
    "z-50 min-w-[13rem] rounded-md border border-border bg-card p-1 text-foreground shadow-lg focus:outline-none"
  const menuItem =
    "flex w-full cursor-pointer select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none data-[highlighted]:bg-accent data-[highlighted]:text-accent-foreground"
</script>

<div class="flex h-full flex-col p-4">
  <header class="flex flex-shrink-0 items-center justify-between gap-4 pt-1 pb-3">
    <div class="flex items-center gap-2">
      <!-- List selector -->
      <DropdownMenu.Root>
        <DropdownMenu.Trigger
          class="flex h-8 items-center gap-1.5 rounded-md border border-border px-2.5 text-sm font-semibold text-primary hover:bg-accent/50"
        >
          {browseView.activeList ?? "All"}
          <ChevronDown class="size-4 text-muted-foreground" />
        </DropdownMenu.Trigger>
        <DropdownMenu.Portal>
          <DropdownMenu.Content class={menuContent} align="start" sideOffset={4}>
            <DropdownMenu.Item class={menuItem} onSelect={() => (browseView.activeList = null)}>
              <Check class="size-3.5 {browseView.activeList === null ? 'opacity-100' : 'opacity-0'}" />
              <span class="flex-1">All</span>
              <span class="text-xs text-muted-foreground">{rows.length}</span>
            </DropdownMenu.Item>
            {#each lists as list (list.name)}
              <DropdownMenu.Item class={menuItem} onSelect={() => (browseView.activeList = list.name)}>
                <Check class="size-3.5 {browseView.activeList === list.name ? 'opacity-100' : 'opacity-0'}" />
                <span class="flex-1">{list.name}</span>
                <span class="text-xs text-muted-foreground">{list.coins.length}</span>
                <button
                  class="ml-1 rounded p-0.5 text-muted-foreground hover:text-destructive"
                  aria-label="Delete list"
                  onpointerdown={e => e.stopPropagation()}
                  onclick={e => {
                    e.stopPropagation()
                    e.preventDefault()
                    void deleteList(list.name)
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
          onsubmit={e => {
            e.preventDefault()
            void submitNewList()
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
      <Input placeholder="Filter coin…" bind:value={browseView.query} class="h-8 w-44" />
      <span class="text-xs text-muted-foreground tabular-nums">{sorted.length} / {rows.length}</span>
    </div>
  </header>

  {#if loading && rows.length === 0}
    <div class="min-h-0 flex-1 space-y-1 overflow-hidden rounded-lg border border-border p-2">
      {#each skeletonRows as index (index)}
        <div class="flex items-center gap-3 px-1 py-1.5">
          <Skeleton class="size-5 rounded-full" />
          <Skeleton class="h-4 w-16" />
          <Skeleton class="ml-auto h-4 w-16" />
          <Skeleton class="h-4 w-12" />
          <Skeleton class="h-4 w-16" />
          <Skeleton class="h-4 w-14" />
        </div>
      {/each}
    </div>
  {:else if error}
    <p class="py-8 text-sm text-destructive">{error}</p>
  {:else}
    <div class="scrollable min-h-0 flex-1 overflow-auto rounded-lg border border-border">
      <table class="w-full text-sm tabular-nums">
        <thead class="sticky top-0 bg-card text-left text-xs text-muted-foreground">
          <tr>
            {#each cols as c (c.key)}
              <th
                class="cursor-pointer px-3 py-2 font-medium whitespace-nowrap select-none hover:text-foreground {c.num
                  ? 'text-right'
                  : ''}"
                onclick={() => sortBy(c.key)}
              >
                {c.label}{browseView.sortKey === c.key ? (browseView.sortDir === 1 ? " ▲" : " ▼") : ""}
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
              <td class="px-3 py-1.5 font-medium whitespace-nowrap text-foreground">
                <span class="flex items-center gap-2">
                  <CoinIcon coin={row.coin} svg={logoMap[row.coin]} />
                  {row.coin}
                </span>
              </td>
              <td class="px-3 py-1.5 text-right">{formatPrice(price)}</td>
              <td class="px-3 py-1.5 text-right {chg > 0 ? 'text-green-500' : chg < 0 ? 'text-red-500' : ''}">
                {chg.toFixed(2)}%
              </td>
              <td class="px-3 py-1.5 text-right text-muted-foreground">{formatCompactNumber(row.volume_usd)}</td>
              <td class="px-3 py-1.5 text-right text-muted-foreground">{formatCompactNumber(row.open_interest_usd)}</td>
              <td
                class="px-3 py-1.5 text-right {row.funding > 0
                  ? 'text-green-500'
                  : row.funding < 0
                    ? 'text-red-500'
                    : ''}"
              >
                {(row.funding * 100).toFixed(4)}%
              </td>
              <td class="py-1.5 pr-3 pl-1 text-right">
                <div class="flex items-center justify-end gap-0.5" onclick={e => e.stopPropagation()} role="none">
                  {#if browseView.activeList !== null}
                    <button
                      class="rounded p-1 text-muted-foreground hover:text-destructive"
                      aria-label="Remove from {browseView.activeList}"
                      onclick={() => {
                        if (browseView.activeList !== null) void toggleMembership(browseView.activeList, row.coin)
                      }}
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
                          <div class="px-2 py-1 text-xs font-medium tracking-wide text-muted-foreground uppercase">
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
                        <DropdownMenu.Item class={menuItem} onSelect={() => openOnHyperliquid(row.coin)}>
                          <ExternalLink class="size-3.5" />
                          Open on Hyperliquid
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
</div>
