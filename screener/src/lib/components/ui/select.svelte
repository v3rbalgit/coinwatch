<script lang="ts">
  import { ChevronDown } from "@lucide/svelte"
  import type { Snippet } from "svelte"
  import type { HTMLSelectAttributes } from "svelte/elements"

  import { cn } from "$lib/utils"

  type Props = Omit<HTMLSelectAttributes, "onchange" | "value"> & {
    class?: string
    value?: string
    children?: Snippet
    onValueChange?: (value: string) => void
  }

  let { class: className, value = $bindable(""), children, onValueChange, ...rest }: Props = $props()
</script>

<span class={cn("relative block h-9 w-full", className)}>
  <select
    bind:value
    class="h-full w-full appearance-none rounded-md border border-input bg-background py-1 pr-8 pl-3 text-sm text-foreground transition-colors outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
    onchange={event => onValueChange?.(event.currentTarget.value)}
    {...rest}
  >
    {@render children?.()}
  </select>
  <ChevronDown
    class="pointer-events-none absolute top-1/2 right-2.5 size-4 -translate-y-1/2 text-muted-foreground"
    aria-hidden="true"
  />
</span>
