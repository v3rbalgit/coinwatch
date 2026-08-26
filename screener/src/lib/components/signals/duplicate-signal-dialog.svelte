<script lang="ts">
  import { Copy, X } from "@lucide/svelte"
  import { Dialog } from "bits-ui"

  import Button from "$lib/components/ui/button.svelte"
  import Input from "$lib/components/ui/input.svelte"

  let {
    open = $bindable(false),
    sourceName,
    busy = false,
    onDuplicate,
  }: {
    open?: boolean
    sourceName: string
    busy?: boolean
    onDuplicate: (name: string) => void
  } = $props()

  let name = $state("")
  let previousSource = $state("")
  $effect(() => {
    if (!open) {
      previousSource = ""
    } else if (sourceName !== previousSource) {
      previousSource = sourceName
      name = `${sourceName} copy`
    }
  })

  function submit() {
    const normalized = name.trim()
    if (normalized.length === 0) return
    onDuplicate(normalized)
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Portal>
    <Dialog.Overlay class="fixed inset-0 z-40 bg-background/80 backdrop-blur-sm" />
    <Dialog.Content
      class="fixed top-1/2 left-1/2 z-50 w-[min(28rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-border bg-card p-5 text-card-foreground shadow-2xl focus:outline-none"
    >
      <div class="flex items-start justify-between gap-3">
        <div>
          <Dialog.Title class="text-base font-semibold">Duplicate signal</Dialog.Title>
          <Dialog.Description class="mt-1 text-sm text-muted-foreground">
            Create an independent copy of {sourceName}.
          </Dialog.Description>
        </div>
        <Dialog.Close
          class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
          aria-label="Close duplicate dialog"
        >
          <X class="size-4" />
        </Dialog.Close>
      </div>
      <form
        class="mt-5"
        onsubmit={event => {
          event.preventDefault()
          submit()
        }}
      >
        <label class="text-xs font-medium text-muted-foreground" for="duplicate-signal-name">New name</label>
        <Input id="duplicate-signal-name" bind:value={name} class="mt-1" disabled={busy} autofocus />
        <div class="mt-5 flex justify-end gap-2">
          <Button variant="ghost" type="button" onclick={() => (open = false)} disabled={busy}>Cancel</Button>
          <Button type="submit" disabled={busy || name.trim().length === 0}>
            <Copy class="size-4" />{busy ? "Duplicating…" : "Duplicate"}
          </Button>
        </div>
      </form>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
