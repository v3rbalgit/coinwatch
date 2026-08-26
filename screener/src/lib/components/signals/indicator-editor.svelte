<script lang="ts">
  import { Check, Pencil, Plus, RotateCcw, Trash2, X } from "@lucide/svelte"

  import type { IndicatorDescriptor, IndicatorInstance } from "$lib/chart/indicators"
  import Button from "$lib/components/ui/button.svelte"
  import Input from "$lib/components/ui/input.svelte"
  import NumberField from "$lib/components/ui/number-field.svelte"
  import Select from "$lib/components/ui/select.svelte"
  import { applyIndicatorPreset, createIndicatorInstance } from "$lib/signals/instances"
  import type { IndicatorPresetEntry } from "$lib/signals/presets"

  const {
    instance,
    output,
    descriptors,
    presets,
    busy = false,
    onChange,
    onCreatePreset,
    onUpdatePreset,
    onDeletePreset,
  }: {
    instance: IndicatorInstance
    output?: string
    descriptors: readonly IndicatorDescriptor[]
    presets: IndicatorPresetEntry[]
    busy?: boolean
    onChange: (instance: IndicatorInstance, output?: string) => void
    onCreatePreset: (indicatorId: string, name: string, params: Record<string, number>) => void
    onUpdatePreset: (id: string, name: string, params: Record<string, number>) => void
    onDeletePreset: (id: string) => void
  } = $props()

  let creatingPreset = $state(false)
  let presetName = $state("")
  let managingPresets = $state(false)
  let renamingId = $state<string | null>(null)
  let renameValue = $state("")
  let presetChoice = $state("")

  const descriptor = $derived(descriptors.find(candidate => candidate.id === instance.id))
  const indicatorPresets = $derived(presets.filter(preset => preset.indicatorId === instance.id))
  const readyPresets = $derived(indicatorPresets.filter(preset => preset.status === "ready"))

  function changeDescriptor(id: string) {
    const next = descriptors.find(candidate => candidate.id === id)
    if (next === undefined) return
    presetChoice = ""
    onChange(createIndicatorInstance(next), next.outputs[0]?.key)
  }

  function changeParameter(key: string, value: number) {
    onChange({ id: instance.id, params: { ...instance.params, [key]: value } }, output)
  }

  function applyPreset(id: string) {
    presetChoice = ""
    const preset = readyPresets.find(candidate => candidate.id === id)
    if (preset === undefined || descriptor === undefined) return
    onChange(applyIndicatorPreset(descriptor, preset), output)
  }

  function submitPreset() {
    const name = presetName.trim()
    if (name.length === 0) return
    onCreatePreset(instance.id, name, { ...instance.params })
    presetName = ""
    creatingPreset = false
  }

  function startRename(id: string, name: string) {
    renamingId = id
    renameValue = name
  }

  function submitRename(id: string, params: Record<string, number>) {
    const name = renameValue.trim()
    if (name.length === 0) return
    onUpdatePreset(id, name, { ...params })
    renamingId = null
    renameValue = ""
  }
</script>

<div class="space-y-3 rounded-md border border-border/70 bg-background/50 p-3">
  <div class="grid gap-3 sm:grid-cols-2">
    <label class="space-y-1 text-xs text-muted-foreground">
      <span>Indicator</span>
      <Select value={instance.id} onValueChange={changeDescriptor} disabled={busy}>
        {#if descriptor === undefined}
          <option value={instance.id}>Unavailable: {instance.id}</option>
        {/if}
        {#each descriptors as candidate (candidate.id)}
          <option value={candidate.id}>{candidate.label}</option>
        {/each}
      </Select>
    </label>

    {#if descriptor !== undefined && descriptor.outputs.length > 0 && output !== undefined}
      <label class="space-y-1 text-xs text-muted-foreground">
        <span>Output</span>
        <Select value={output} onValueChange={(next: string) => onChange(instance, next)} disabled={busy}>
          {#each descriptor.outputs as candidate (candidate.key)}
            <option value={candidate.key}>{candidate.label}</option>
          {/each}
        </Select>
      </label>
    {/if}
  </div>

  {#if descriptor === undefined}
    <p class="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
      Indicator '{instance.id}' is no longer available. Choose a replacement before saving.
    </p>
  {:else if descriptor.params.length > 0}
    <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
      {#each descriptor.params as parameter (parameter.key)}
        <label class="space-y-1 text-xs text-muted-foreground">
          <span>{parameter.label}</span>
          <NumberField
            value={instance.params[parameter.key] ?? parameter.default}
            min={parameter.min}
            max={parameter.max}
            step={parameter.step}
            disabled={busy}
            onValueChange={(value: number) => changeParameter(parameter.key, value)}
          />
        </label>
      {/each}
    </div>
  {/if}

  <div class="border-t border-border/60 pt-3">
    <div class="flex flex-wrap items-end gap-2">
      <label class="min-w-48 flex-1 space-y-1 text-xs text-muted-foreground">
        <span>Apply preset</span>
        <Select bind:value={presetChoice} onValueChange={applyPreset} disabled={busy || readyPresets.length === 0}>
          <option value="">{readyPresets.length === 0 ? "No presets" : "Choose preset…"}</option>
          {#each readyPresets as preset (preset.id)}
            <option value={preset.id}>{preset.name}</option>
          {/each}
        </Select>
      </label>
      <Button size="sm" variant="outline" onclick={() => (creatingPreset = !creatingPreset)} disabled={busy}>
        <Plus class="size-3.5" />Save current
      </Button>
      {#if indicatorPresets.length > 0}
        <Button size="sm" variant="ghost" onclick={() => (managingPresets = !managingPresets)} disabled={busy}>
          {managingPresets ? "Hide presets" : "Manage"}
        </Button>
      {/if}
    </div>
    <p class="mt-1.5 text-[11px] text-muted-foreground">
      Applying a preset copies its values. Later preset edits do not change this condition.
    </p>

    {#if creatingPreset}
      <form
        class="mt-3 flex items-center gap-2"
        onsubmit={event => {
          event.preventDefault()
          submitPreset()
        }}
      >
        <Input bind:value={presetName} class="h-8 max-w-64" placeholder="Preset name…" autofocus />
        <Button size="sm" type="submit" disabled={busy || presetName.trim().length === 0}>Create</Button>
        <Button size="sm" variant="ghost" type="button" onclick={() => (creatingPreset = false)}>Cancel</Button>
      </form>
    {/if}

    {#if managingPresets}
      <div class="mt-3 space-y-1 rounded-md border border-border/60 p-2">
        {#each indicatorPresets as preset (preset.id)}
          <div class="flex min-h-9 items-center gap-2 rounded px-2 hover:bg-accent/40">
            {#if renamingId === preset.id}
              <Input bind:value={renameValue} class="h-7 min-w-0 flex-1" aria-label={`Rename ${preset.name}`} />
              <button
                type="button"
                class="rounded p-1 text-muted-foreground hover:text-foreground"
                aria-label="Save preset name"
                onclick={() => submitRename(preset.id, preset.status === "ready" ? preset.params : {})}
              >
                <Check class="size-3.5" />
              </button>
              <button
                type="button"
                class="rounded p-1 text-muted-foreground hover:text-foreground"
                aria-label="Cancel preset rename"
                onclick={() => (renamingId = null)}
              >
                <X class="size-3.5" />
              </button>
            {:else}
              <span class="min-w-0 flex-1 truncate text-xs text-foreground">{preset.name}</span>
              {#if preset.status === "ready"}
                <button
                  type="button"
                  class="rounded p-1 text-muted-foreground hover:text-foreground"
                  title="Replace this preset with the current parameters"
                  aria-label={`Replace ${preset.name}`}
                  onclick={() => onUpdatePreset(preset.id, preset.name, { ...instance.params })}
                >
                  <RotateCcw class="size-3.5" />
                </button>
                <button
                  type="button"
                  class="rounded p-1 text-muted-foreground hover:text-foreground"
                  aria-label={`Rename ${preset.name}`}
                  onclick={() => startRename(preset.id, preset.name)}
                >
                  <Pencil class="size-3.5" />
                </button>
              {:else}
                <span class="text-[10px] text-destructive">Invalid</span>
              {/if}
              <button
                type="button"
                class="rounded p-1 text-muted-foreground hover:text-destructive"
                aria-label={`Delete ${preset.name}`}
                onclick={() => onDeletePreset(preset.id)}
              >
                <Trash2 class="size-3.5" />
              </button>
            {/if}
          </div>
        {/each}
      </div>
    {/if}
  </div>
</div>
