<script lang="ts">
  import { X } from "@lucide/svelte"
  import { Dialog } from "bits-ui"

  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import Button from "$lib/components/ui/button.svelte"
  import { SIGNAL_TIMEFRAMES } from "$lib/signals/definition"
  import type { SignalDirection, SignalTimeframe } from "$lib/signals/definition"
  import { buildSignalTemplate, SIGNAL_TEMPLATES } from "$lib/signals/templates"
  import type { NewSignalDraft, SignalTemplateId } from "$lib/signals/templates"

  let {
    open = $bindable(false),
    descriptors,
    onCreate,
  }: {
    open?: boolean
    descriptors: readonly IndicatorDescriptor[]
    onCreate: (created: NewSignalDraft) => void
  } = $props()

  const directions: readonly SignalDirection[] = ["bullish", "bearish"]

  let selectedId = $state<SignalTemplateId>("closeSma")
  let direction = $state<SignalDirection>("bullish")
  let timeframes = $state<SignalTimeframe[]>(["1h"])
  const selected = $derived(SIGNAL_TEMPLATES.find(template => template.id === selectedId))

  function selectTemplate(templateId: SignalTemplateId) {
    const wasMultiple = selected?.supportsMultipleTimeframes === true
    selectedId = templateId
    const template = SIGNAL_TEMPLATES.find(candidate => candidate.id === templateId)
    if (template?.supportsMultipleTimeframes === true) {
      if (!wasMultiple) timeframes = [...SIGNAL_TIMEFRAMES]
    } else {
      timeframes = [timeframes[0] ?? "1h"]
    }
  }

  function chooseTimeframe(timeframe: SignalTimeframe) {
    if (selected?.supportsMultipleTimeframes !== true) {
      timeframes = [timeframe]
      return
    }
    if (timeframes.includes(timeframe)) {
      if (timeframes.length > 1) timeframes = timeframes.filter(candidate => candidate !== timeframe)
    } else {
      timeframes = [...timeframes, timeframe]
    }
  }

  function createDraft() {
    onCreate(buildSignalTemplate(selectedId, { direction, timeframes }, descriptors))
    open = false
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Portal>
    <Dialog.Overlay class="fixed inset-0 z-40 bg-background/80 backdrop-blur-sm" />
    <Dialog.Content
      class="fixed top-1/2 left-1/2 z-50 flex max-h-[88vh] w-[min(56rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-xl border border-border bg-card text-card-foreground shadow-2xl focus:outline-none"
    >
      <div class="flex items-start justify-between border-b border-border px-5 py-4">
        <div>
          <Dialog.Title class="text-base font-semibold">Create a personal signal</Dialog.Title>
          <Dialog.Description class="mt-1 text-sm text-muted-foreground">
            Templates only initialize the draft. Every condition remains editable.
          </Dialog.Description>
        </div>
        <Dialog.Close
          class="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          aria-label="Close template picker"
        >
          <X class="size-4" />
        </Dialog.Close>
      </div>

      <div class="min-h-0 flex-1 overflow-auto p-5">
        <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {#each SIGNAL_TEMPLATES as template (template.id)}
            <button
              type="button"
              class="rounded-lg border p-4 text-left transition-colors {selectedId === template.id
                ? 'border-primary bg-primary/10'
                : 'border-border hover:bg-accent/50'}"
              aria-pressed={selectedId === template.id}
              onclick={() => selectTemplate(template.id)}
            >
              <span class="text-sm font-medium text-foreground">{template.label}</span>
              <span class="mt-1 block text-xs leading-relaxed text-muted-foreground">{template.description}</span>
            </button>
          {/each}
        </div>

        <div class="mt-5 grid gap-5 border-t border-border pt-5 md:grid-cols-2">
          <fieldset>
            <legend class="text-xs font-medium tracking-wide text-muted-foreground uppercase">Direction</legend>
            <div class="mt-2 inline-flex rounded-md border border-border bg-background p-1">
              {#each directions as option (option)}
                <button
                  type="button"
                  class="rounded px-3 py-1.5 text-xs font-medium capitalize transition-colors {direction === option
                    ? 'bg-accent text-accent-foreground'
                    : 'text-muted-foreground hover:text-foreground'}"
                  aria-pressed={direction === option}
                  onclick={() => (direction = option)}
                >
                  {option}
                </button>
              {/each}
            </div>
          </fieldset>

          <fieldset>
            <legend class="text-xs font-medium tracking-wide text-muted-foreground uppercase">
              {selected?.supportsMultipleTimeframes === true ? "Timeframes" : "Timeframe"}
            </legend>
            <div class="mt-2 flex flex-wrap gap-1">
              {#each SIGNAL_TIMEFRAMES as timeframe (timeframe)}
                <button
                  type="button"
                  class="rounded border px-3 py-1.5 text-xs font-medium transition-colors {timeframes.includes(
                    timeframe
                  )
                    ? 'border-primary bg-primary/10 text-primary'
                    : 'border-border text-muted-foreground hover:bg-accent/50 hover:text-foreground'}"
                  aria-pressed={timeframes.includes(timeframe)}
                  onclick={() => chooseTimeframe(timeframe)}
                >
                  {timeframe}
                </button>
              {/each}
            </div>
          </fieldset>
        </div>
      </div>

      <div class="flex justify-end gap-2 border-t border-border px-5 py-4">
        <Button variant="ghost" onclick={() => (open = false)}>Cancel</Button>
        <Button onclick={createDraft}>Create draft</Button>
      </div>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
