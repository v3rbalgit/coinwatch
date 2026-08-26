<script lang="ts">
  import type { HTMLInputAttributes } from "svelte/elements"

  import { cn } from "$lib/utils"

  type Props = Omit<HTMLInputAttributes, "oninput" | "value"> & {
    class?: string
    value?: string
    onValueChange?: (value: string) => void
  }

  let { class: className, value = $bindable(""), onValueChange, ...rest }: Props = $props()
</script>

<input
  bind:value
  class={cn(
    "flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm text-foreground transition-colors outline-none placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50",
    className
  )}
  oninput={event => onValueChange?.(event.currentTarget.value)}
  {...rest}
/>
