<script lang="ts">
  import Card from "$lib/components/ui/card.svelte"

  const widgets = ["Hottest", "Gainers", "Losers", "Favourites"]
</script>

<div class="flex h-full flex-col overflow-auto p-6">
  <h1 class="text-lg font-semibold text-primary">Mission Control</h1>
  <p class="mt-1 text-sm text-muted-foreground">
    Live Hyperliquid overview — hottest movers, gainers/losers, favourites. Widget-based and customizable; coming as the
    widgets land.
  </p>

  <div class="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
    {#each widgets as widget (widget)}
      <Card class="p-4">
        <div class="text-xs font-medium tracking-wide text-muted-foreground uppercase">{widget}</div>
        <div class="mt-3 text-sm text-muted-foreground/60">Widget placeholder</div>
      </Card>
    {/each}
  </div>
</div>
