<script lang="ts">
  import { AlertDialog } from "bits-ui"

  let {
    open = $bindable(false),
    title,
    description,
    confirmLabel = "Confirm",
    destructive = false,
    busy = false,
    onConfirm,
  }: {
    open?: boolean
    title: string
    description: string
    confirmLabel?: string
    destructive?: boolean
    busy?: boolean
    onConfirm: () => void
  } = $props()

  function confirm() {
    open = false
    onConfirm()
  }
</script>

<AlertDialog.Root bind:open>
  <AlertDialog.Portal>
    <AlertDialog.Overlay class="fixed inset-0 z-40 bg-background/80 backdrop-blur-sm" />
    <AlertDialog.Content
      class="fixed top-1/2 left-1/2 z-50 w-[min(28rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-border bg-card p-5 text-card-foreground shadow-2xl focus:outline-none"
    >
      <AlertDialog.Title class="text-base font-semibold">{title}</AlertDialog.Title>
      <AlertDialog.Description class="mt-2 text-sm leading-relaxed text-muted-foreground">
        {description}
      </AlertDialog.Description>
      <div class="mt-5 flex justify-end gap-2">
        <AlertDialog.Cancel
          class="inline-flex h-9 items-center justify-center rounded-md px-4 py-2 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50"
          disabled={busy}
        >
          Cancel
        </AlertDialog.Cancel>
        <AlertDialog.Action
          class="inline-flex h-9 items-center justify-center rounded-md px-4 py-2 text-sm font-medium transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50 {destructive
            ? 'bg-destructive text-white hover:bg-destructive/90'
            : 'bg-primary text-primary-foreground hover:bg-primary/90'}"
          disabled={busy}
          onclick={confirm}
        >
          {busy ? "Working…" : confirmLabel}
        </AlertDialog.Action>
      </div>
    </AlertDialog.Content>
  </AlertDialog.Portal>
</AlertDialog.Root>
