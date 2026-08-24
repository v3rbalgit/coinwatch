<script lang="ts">
  import "../app.css";
  import { page } from "$app/stores";
  import { listen } from "@tauri-apps/api/event";
  import { onMount } from "svelte";

  let { children } = $props();

  type Status = { state: string; done: number; total: number };
  let status = $state<Status>({ state: "connecting", done: 0, total: 0 });

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
    <div class="border-t border-border px-4 py-3 text-xs text-muted-foreground">
      {#if status.state === "live"}
        <div class="flex items-center gap-2">
          <span class="h-2 w-2 rounded-full bg-green-500"></span>
          Live · synced
        </div>
      {:else if status.state === "backfilling"}
        <div class="flex items-center gap-2">
          <span class="h-2 w-2 animate-pulse rounded-full bg-amber-500"></span>
          Backfilling {status.done}/{status.total}
        </div>
      {:else}
        <div class="flex items-center gap-2">
          <span class="h-2 w-2 animate-pulse rounded-full bg-muted-foreground"></span>
          Connecting…
        </div>
      {/if}
    </div>
  </aside>

  <main class="min-w-0 flex-1 overflow-hidden">
    {@render children()}
  </main>
</div>
