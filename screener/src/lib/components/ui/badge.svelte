<script lang="ts" module>
  import { tv, type VariantProps } from "tailwind-variants";

  export const badgeVariants = tv({
    base: "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-xs font-medium",
    variants: {
      variant: {
        default: "border-transparent bg-primary text-primary-foreground",
        secondary: "border-transparent bg-secondary text-secondary-foreground",
        outline: "text-foreground",
        success: "border-transparent bg-green-500/15 text-green-500",
        destructive: "border-transparent bg-red-500/15 text-red-500",
      },
    },
    defaultVariants: { variant: "default" },
  });

  export type BadgeVariant = VariantProps<typeof badgeVariants>["variant"];
</script>

<script lang="ts">
  import type { Snippet } from "svelte";

  import { cn } from "$lib/utils";

  let { variant = "default", class: className, children }: { variant?: BadgeVariant; class?: string; children?: Snippet } =
    $props();
</script>

<span class={cn(badgeVariants({ variant }), className)}>
  {@render children?.()}
</span>
