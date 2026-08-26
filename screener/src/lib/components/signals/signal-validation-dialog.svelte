<script lang="ts">
  import { CalendarRange, FilePenLine, Library, Play, X } from "@lucide/svelte"
  import { Dialog } from "bits-ui"

  import Button from "$lib/components/ui/button.svelte"
  import type { HistoricalRange } from "$lib/signals/historical"
  import type { HistoricalSignalOption } from "$lib/signals/historical-options"
  import {
    customHistoricalRange,
    dateInputValue,
    HISTORICAL_RANGE_PRESETS,
    presetHistoricalRange,
    type HistoricalRangePreset,
  } from "$lib/signals/historical-range"

  let {
    open = $bindable(false),
    coin,
    options,
    loading = false,
    error = "",
    busy = false,
    onValidate,
  }: {
    open?: boolean
    coin: string
    options: HistoricalSignalOption[]
    loading?: boolean
    error?: string
    busy?: boolean
    onValidate: (option: HistoricalSignalOption, range: HistoricalRange, rangeLabel: string) => void
  } = $props()

  let selectedKey = $state("")
  let rangePreset = $state<HistoricalRangePreset>("week1")
  let customFrom = $state("")
  let customThrough = $state("")
  let wasOpen = $state(false)
  const selected = $derived(options.find(option => option.key === selectedKey))
  const customRange = $derived(customHistoricalRange(customFrom, customThrough))
  const today = $derived(dateInputValue(new Date()))

  $effect(() => {
    if (open) {
      const available = options.find(option => option.disabledReason === undefined)
      if (!options.some(option => option.key === selectedKey && option.disabledReason === undefined)) {
        selectedKey = available?.key ?? ""
      }
    }
    if (open && !wasOpen) {
      rangePreset = "week1"
      const now = new Date()
      const weekAgo = new Date(now.getTime() - 7 * 86_400_000)
      customFrom = dateInputValue(weekAgo)
      customThrough = dateInputValue(now)
    }
    wasOpen = open
  })

  function startValidation() {
    if (
      selected?.definition === null ||
      selected?.definition === undefined ||
      selected.disabledReason !== undefined ||
      busy
    ) {
      return
    }
    const chosen = rangePreset === "custom" ? customRange : presetHistoricalRange(rangePreset, new Date())
    if (chosen === null) return
    open = false
    onValidate(selected, chosen.range, chosen.label)
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Portal>
    <Dialog.Overlay class="fixed inset-0 z-40 bg-background/80 backdrop-blur-sm" />
    <Dialog.Content
      class="fixed top-1/2 left-1/2 z-50 flex max-h-[min(46rem,calc(100vh-2rem))] w-[min(38rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-xl border border-border bg-card text-card-foreground shadow-2xl focus:outline-none"
    >
      <header class="flex items-start justify-between gap-3 border-b border-border px-5 py-4">
        <div>
          <Dialog.Title class="text-base font-semibold">Validate a signal on {coin}</Dialog.Title>
          <Dialog.Description class="mt-1 text-sm text-muted-foreground">
            Find when the complete definition entered a matched state using completed candles only.
          </Dialog.Description>
        </div>
        <Dialog.Close
          class="rounded p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
          aria-label="Close validation dialog"
        >
          <X class="size-4" />
        </Dialog.Close>
      </header>

      <div class="scrollable min-h-0 flex-1 space-y-5 overflow-auto p-5">
        <fieldset class="space-y-2">
          <legend class="mb-3 text-xs font-medium tracking-wide text-muted-foreground uppercase">Signal</legend>
          {#if loading}
            <p class="rounded-md border border-dashed border-border px-3 py-4 text-sm text-muted-foreground">
              Loading personal signals…
            </p>
          {:else if options.length === 0}
            <p class="rounded-md border border-dashed border-border px-3 py-4 text-sm text-muted-foreground">
              Create a signal in the Signals workspace before validating it here.
            </p>
          {:else}
            {#each options as option (option.key)}
              <label
                class="flex items-start gap-3 rounded-lg border p-3 transition-colors {option.disabledReason !==
                undefined
                  ? 'cursor-not-allowed opacity-55'
                  : selectedKey === option.key
                    ? 'cursor-pointer border-primary bg-primary/10'
                    : 'cursor-pointer border-border hover:bg-accent/40'}"
              >
                <input
                  type="radio"
                  name="historical-signal"
                  value={option.key}
                  checked={selectedKey === option.key}
                  disabled={option.disabledReason !== undefined}
                  onchange={() => (selectedKey = option.key)}
                />
                {#if option.source === "draft"}
                  <FilePenLine class="mt-0.5 size-4 text-muted-foreground" />
                {:else}
                  <Library class="mt-0.5 size-4 text-muted-foreground" />
                {/if}
                <span class="min-w-0 flex-1">
                  <span class="block truncate text-sm font-medium text-foreground">{option.name}</span>
                  <span class="block text-xs text-muted-foreground">
                    {option.source === "draft" ? "Current editor snapshot" : "Saved definition"}
                  </span>
                  {#if option.disabledReason}
                    <span class="mt-1 block text-xs text-destructive">{option.disabledReason}</span>
                  {/if}
                </span>
              </label>
            {/each}
          {/if}
        </fieldset>

        <fieldset>
          <legend class="mb-3 text-xs font-medium tracking-wide text-muted-foreground uppercase">History range</legend>
          <div class="flex flex-wrap gap-2">
            {#each HISTORICAL_RANGE_PRESETS as preset (preset.value)}
              <button
                type="button"
                class="rounded-md border px-3 py-1.5 text-xs font-medium transition-colors {rangePreset === preset.value
                  ? 'border-primary bg-primary/10 text-primary'
                  : 'border-border text-muted-foreground hover:bg-accent/50 hover:text-foreground'}"
                aria-pressed={rangePreset === preset.value}
                onclick={() => (rangePreset = preset.value)}
              >
                {preset.label}
              </button>
            {/each}
          </div>

          {#if rangePreset === "custom"}
            <div class="mt-3 grid gap-3 sm:grid-cols-2">
              <label class="grid gap-1 text-xs text-muted-foreground">
                From
                <input
                  type="date"
                  class="h-9 rounded-md border border-border bg-background px-3 text-sm text-foreground"
                  bind:value={customFrom}
                  max={customThrough || today}
                />
              </label>
              <label class="grid gap-1 text-xs text-muted-foreground">
                Through
                <input
                  type="date"
                  class="h-9 rounded-md border border-border bg-background px-3 text-sm text-foreground"
                  bind:value={customThrough}
                  min={customFrom}
                  max={today}
                />
              </label>
            </div>
            {#if customRange === null}
              <p class="mt-2 text-xs text-destructive">Choose a valid date range ending today or earlier.</p>
            {/if}
            <p class="mt-2 text-xs text-muted-foreground">Dates use your local timezone.</p>
          {/if}
        </fieldset>

        <div class="flex gap-3 rounded-lg border border-border bg-muted/30 p-3">
          <CalendarRange class="mt-0.5 size-4 shrink-0 text-primary" />
          <p class="text-xs leading-relaxed text-muted-foreground">
            Coinwatch fetches and caches the selected period plus every timeframe’s required warm-up. This does not
            change the chart’s visible range; older markings appear as you navigate back to them.
          </p>
        </div>

        {#if error}
          <p class="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            {error}
          </p>
        {/if}
      </div>

      <footer class="flex items-center justify-end gap-2 border-t border-border px-5 py-4">
        <Button variant="ghost" onclick={() => (open = false)}>Cancel</Button>
        <Button
          disabled={loading ||
            busy ||
            selected?.definition === null ||
            selected?.definition === undefined ||
            selected.disabledReason !== undefined ||
            (rangePreset === "custom" && customRange === null)}
          onclick={startValidation}
        >
          <Play class="size-4" />Validate signal
        </Button>
      </footer>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
