<script lang="ts">
  import { cn } from "$lib/utils"

  const { class: className }: { class?: string } = $props()
</script>

<div class={cn("animate-pulse rounded-md bg-muted", className)}></div>
