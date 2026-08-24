<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  type Row = {
    coin: string;
    price: number;
    change_24h_pct: number;
    volume_usd: number;
    momentum: number;
    rsi: number;
    vol: number;
    illiq: number;
    funding_rate: number;
    funding_z: number;
  };

  let rows = $state<Row[]>([]);
  let loading = $state(true);
  let error = $state("");
  let query = $state("");
  let sortKey = $state<keyof Row>("volume_usd");
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
    return () => {
      void unlisten.then((off) => off());
    };
  });

  function sortBy(key: keyof Row) {
    if (sortKey === key) sortDir = (sortDir === 1 ? -1 : 1) as 1 | -1;
    else {
      sortKey = key;
      sortDir = -1;
    }
  }

  const filtered = $derived.by(() => {
    const q = query.trim().toLowerCase();
    const r = q ? rows.filter((x) => x.coin.toLowerCase().includes(q)) : rows.slice();
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

  const cols: { key: keyof Row; label: string; num: boolean }[] = [
    { key: "coin", label: "Coin", num: false },
    { key: "price", label: "Price", num: true },
    { key: "change_24h_pct", label: "24h %", num: true },
    { key: "volume_usd", label: "Volume", num: true },
    { key: "momentum", label: "Mom", num: true },
    { key: "rsi", label: "RSI", num: true },
    { key: "vol", label: "Vol", num: true },
    { key: "illiq", label: "Illiq", num: true },
    { key: "funding_rate", label: "Funding", num: true },
    { key: "funding_z", label: "Fund-Z", num: true },
  ];
</script>

<div class="screener">
  <header>
    <h1>Hyperliquid Screener</h1>
    <div class="controls">
      {#if live}<span class="live">● live</span>{/if}
      <input placeholder="Filter coin…" bind:value={query} />
      <span class="count">{filtered.length} / {rows.length}</span>
      <button onclick={load} disabled={loading}>{loading ? "…" : "Refresh"}</button>
    </div>
  </header>

  {#if loading}
    <p class="msg">Loading market…</p>
  {:else if error}
    <p class="msg err">{error}</p>
    {#if error.includes("__TAURI") || error.includes("invoke")}
      <p class="msg">Open the desktop window from <code>pnpm tauri dev</code> — the backend isn't reachable from the plain browser preview at localhost:1420.</p>
    {/if}
  {:else if rows.length === 0}
    <p class="msg">No data yet. Run <code>coinwatch ingest</code> to populate the store.</p>
  {:else}
    <div class="tablewrap">
      <table>
        <thead>
          <tr>
            {#each cols as c (c.key)}
              <th class:num={c.num} onclick={() => sortBy(c.key)}>
                {c.label}{sortKey === c.key ? (sortDir === 1 ? " ▲" : " ▼") : ""}
              </th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#each filtered as r (r.coin)}
            <tr>
              <td class="coin">{r.coin}</td>
              <td class="num">{fmtPrice(livePrices[r.coin] ?? r.price)}</td>
              <td class="num" class:up={r.change_24h_pct > 0} class:down={r.change_24h_pct < 0}>{r.change_24h_pct.toFixed(2)}%</td>
              <td class="num">{fmtUsd(r.volume_usd)}</td>
              <td class="num">{r.momentum.toFixed(2)}</td>
              <td class="num" class:hot={r.rsi >= 70} class:cold={r.rsi <= 30}>{r.rsi.toFixed(0)}</td>
              <td class="num">{(r.vol * 100).toFixed(1)}%</td>
              <td class="num">{r.illiq.toFixed(2)}</td>
              <td class="num" class:up={r.funding_rate > 0} class:down={r.funding_rate < 0}>{(r.funding_rate * 100).toFixed(3)}%</td>
              <td class="num">{r.funding_z.toFixed(2)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<style>
  .screener {
    height: 100%;
    display: flex;
    flex-direction: column;
    padding: 0 1rem;
    box-sizing: border-box;
  }
  header {
    flex: 0 0 auto;
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 1rem;
    padding: 0.8rem 0 0.6rem;
  }
  h1 {
    font-size: 1.1rem;
    margin: 0;
    color: #58a6ff;
    letter-spacing: 0.02em;
  }
  .controls {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }
  .count {
    color: #8b949e;
    font-size: 0.8rem;
    font-variant-numeric: tabular-nums;
  }
  input {
    background: #161b22;
    border: 1px solid #30363d;
    color: #c9d1d9;
    border-radius: 6px;
    padding: 0.35rem 0.6rem;
    font-size: 0.85rem;
    outline: none;
  }
  input:focus {
    border-color: #58a6ff;
  }
  button {
    background: #21262d;
    border: 1px solid #30363d;
    color: #c9d1d9;
    border-radius: 6px;
    padding: 0.35rem 0.8rem;
    cursor: pointer;
    font-size: 0.85rem;
  }
  button:hover:not(:disabled) {
    border-color: #58a6ff;
  }
  .msg {
    color: #8b949e;
    padding: 2rem 0;
  }
  .msg.err {
    color: #f85149;
  }
  code {
    background: #161b22;
    padding: 0.1rem 0.3rem;
    border-radius: 4px;
  }
  .tablewrap {
    flex: 1 1 auto;
    overflow: auto;
  }
  table {
    border-collapse: collapse;
    width: 100%;
    font-size: 0.82rem;
    font-variant-numeric: tabular-nums;
  }
  thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: #161b22;
    color: #8b949e;
    text-align: left;
    padding: 0.4rem 0.7rem;
    border-bottom: 1px solid #30363d;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }
  th.num {
    text-align: right;
  }
  tbody td {
    padding: 0.3rem 0.7rem;
    border-bottom: 1px solid #21262d;
    white-space: nowrap;
  }
  td.num {
    text-align: right;
  }
  td.coin {
    font-weight: 600;
    color: #e6edf3;
  }
  tbody tr:hover {
    background: #161b22;
  }
  .up {
    color: #3fb950;
  }
  .down {
    color: #f85149;
  }
  .live {
    color: #3fb950;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  .hot {
    color: #f0883e;
  }
  .cold {
    color: #58a6ff;
  }
</style>
