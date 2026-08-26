<script lang="ts">
  import type { HTMLInputAttributes } from "svelte/elements"

  import { cn } from "$lib/utils"

  type Props = Omit<HTMLInputAttributes, "onchange" | "type" | "value"> & {
    class?: string
    value: number
    onValueChange: (value: number) => void
  }

  const { class: className, value, onValueChange, ...rest }: Props = $props()
</script>

<input
  type="number"
  {value}
  class={cn(
    "flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm text-foreground transition-colors outline-none placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50",
    className
  )}
  onchange={event => {
    if (!event.currentTarget.checkValidity() || !Number.isFinite(event.currentTarget.valueAsNumber)) {
      event.currentTarget.value = String(value)
      return
    }
    onValueChange(event.currentTarget.valueAsNumber)
  }}
  {...rest}
/>
