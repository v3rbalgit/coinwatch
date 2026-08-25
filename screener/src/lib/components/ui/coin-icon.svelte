<script lang="ts">
  import { cn } from "$lib/utils"

  const {
    coin,
    svg,
    size = 20,
    class: className,
  }: { coin: string; svg?: string; size?: number; class?: string } = $props()

  // Prefer the locally-cached SVG; otherwise load Hyperliquid's, with a monogram fallback.
  let failed = $state(false)
  const src = $derived(
    svg ? `data:image/svg+xml,${encodeURIComponent(svg)}` : `https://app.hyperliquid.xyz/coins/${coin}.svg`
  )
  $effect(() => {
    void src
    failed = false
  })
</script>

{#if failed}
  <span
    class={cn(
      "inline-flex shrink-0 items-center justify-center rounded-full bg-muted text-[9px] font-semibold text-muted-foreground uppercase",
      className
    )}
    style="width: {size}px; height: {size}px"
  >
    {coin.slice(0, 2)}
  </span>
{:else}
  <img
    {src}
    alt=""
    width={size}
    height={size}
    loading="lazy"
    class={cn("shrink-0 rounded-full", className)}
    onerror={() => (failed = true)}
  />
{/if}
