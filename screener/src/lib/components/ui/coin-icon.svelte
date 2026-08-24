<script lang="ts">
  import { cn } from "$lib/utils";

  let { coin, size = 20, class: className }: { coin: string; size?: number; class?: string } = $props();

  // Hyperliquid serves per-coin SVGs; missing ones fall back to a monogram.
  let failed = $state(false);
</script>

{#if failed}
  <span
    class={cn(
      "inline-flex shrink-0 items-center justify-center rounded-full bg-muted text-[9px] font-semibold uppercase text-muted-foreground",
      className,
    )}
    style="width: {size}px; height: {size}px"
  >
    {coin.slice(0, 2)}
  </span>
{:else}
  <img
    src="https://app.hyperliquid.xyz/coins/{coin}.svg"
    alt=""
    width={size}
    height={size}
    loading="lazy"
    class={cn("shrink-0 rounded-full", className)}
    onerror={() => (failed = true)}
  />
{/if}
