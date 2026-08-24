<script lang="ts">
  import "../app.css";
  import { page } from "$app/stores";

  let { children } = $props();

  const nav = [
    { href: "/", label: "Home" },
    { href: "/screener", label: "Screener" },
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
    <div class="border-t border-border px-4 py-3 text-xs text-muted-foreground">Hyperliquid</div>
  </aside>

  <main class="min-w-0 flex-1 overflow-hidden">
    {@render children()}
  </main>
</div>
