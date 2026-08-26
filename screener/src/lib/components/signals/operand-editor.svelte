<script lang="ts">
  import type { IndicatorDescriptor, IndicatorInstance } from "$lib/chart/indicators"
  import IndicatorEditor from "$lib/components/signals/indicator-editor.svelte"
  import NumberField from "$lib/components/ui/number-field.svelte"
  import Select from "$lib/components/ui/select.svelte"
  import type { CandleField, Operand, OperandAdjustment } from "$lib/signals/definition"
  import { createDefaultOperand } from "$lib/signals/editor"
  import type { OperandType } from "$lib/signals/editor"
  import type { IndicatorPresetEntry } from "$lib/signals/presets"

  const operandTypes: readonly { value: OperandType; label: string }[] = [
    { value: "field", label: "OHLCV field" },
    { value: "indicator", label: "Indicator" },
    { value: "constant", label: "Constant" },
  ]
  const candleFields: readonly { value: CandleField; label: string }[] = [
    { value: "open", label: "Open" },
    { value: "high", label: "High" },
    { value: "low", label: "Low" },
    { value: "close", label: "Close" },
    { value: "volume", label: "Volume" },
  ]

  const {
    operand,
    descriptors,
    presets,
    busy = false,
    label,
    onChange,
    onCreatePreset,
    onUpdatePreset,
    onDeletePreset,
  }: {
    operand: Operand
    descriptors: readonly IndicatorDescriptor[]
    presets: IndicatorPresetEntry[]
    busy?: boolean
    label: string
    onChange: (operand: Operand) => void
    onCreatePreset: (indicatorId: string, name: string, params: Record<string, number>) => void
    onUpdatePreset: (id: string, name: string, params: Record<string, number>) => void
    onDeletePreset: (id: string) => void
  } = $props()

  const seriesDescriptors = $derived(descriptors.filter(descriptor => descriptor.kind === "series"))

  function changeType(value: string) {
    const type = operandTypes.find(candidate => candidate.value === value)?.value
    if (type !== undefined) onChange(createDefaultOperand(type, seriesDescriptors))
  }

  function changeField(value: string) {
    if (operand.type !== "field") return
    const field = candleFields.find(candidate => candidate.value === value)?.value
    if (field !== undefined) onChange({ ...operand, field })
  }

  function changeIndicator(instance: IndicatorInstance, output?: string) {
    if (operand.type !== "indicator") return
    onChange({ ...operand, instance, output: output ?? operand.output })
  }

  function withAdjustment(adjustment: OperandAdjustment | undefined): Operand {
    if (operand.type === "constant") return operand
    if (operand.type === "field") {
      return adjustment === undefined
        ? { type: "field", field: operand.field }
        : { type: "field", field: operand.field, adjustment }
    }
    return adjustment === undefined
      ? { type: "indicator", instance: operand.instance, output: operand.output }
      : { type: "indicator", instance: operand.instance, output: operand.output, adjustment }
  }

  function changeAdjustmentType(value: string) {
    if (operand.type === "constant" || operand.adjustment === undefined) return
    if (value === "absolute" || value === "percent") {
      onChange(withAdjustment({ type: value, value: operand.adjustment.value }))
    }
  }
</script>

<fieldset class="space-y-3 rounded-lg border border-border p-3">
  <legend class="px-1 text-xs font-medium tracking-wide text-muted-foreground uppercase">{label}</legend>

  <label class="block space-y-1 text-xs text-muted-foreground">
    <span>Source</span>
    <Select value={operand.type} onValueChange={changeType} disabled={busy}>
      {#each operandTypes as option (option.value)}
        <option value={option.value}>{option.label}</option>
      {/each}
    </Select>
  </label>

  {#if operand.type === "field"}
    <label class="block space-y-1 text-xs text-muted-foreground">
      <span>Field</span>
      <Select value={operand.field} onValueChange={changeField} disabled={busy}>
        {#each candleFields as field (field.value)}
          <option value={field.value}>{field.label}</option>
        {/each}
      </Select>
    </label>
  {:else if operand.type === "constant"}
    <label class="block space-y-1 text-xs text-muted-foreground">
      <span>Value</span>
      <NumberField
        value={operand.value}
        step="any"
        disabled={busy}
        onValueChange={(value: number) => onChange({ type: "constant", value })}
      />
    </label>
  {:else}
    <IndicatorEditor
      instance={operand.instance}
      output={operand.output}
      descriptors={seriesDescriptors}
      {presets}
      {busy}
      onChange={changeIndicator}
      {onCreatePreset}
      {onUpdatePreset}
      {onDeletePreset}
    />
  {/if}

  {#if operand.type !== "constant"}
    <div class="border-t border-border/60 pt-3">
      <label class="flex items-center gap-2 text-xs text-muted-foreground">
        <input
          type="checkbox"
          checked={operand.adjustment !== undefined}
          disabled={busy}
          onchange={() =>
            onChange(withAdjustment(operand.adjustment === undefined ? { type: "percent", value: 0 } : undefined))}
        />
        Adjust this source
      </label>
      {#if operand.adjustment !== undefined}
        <div class="mt-2 grid grid-cols-2 gap-2">
          <Select value={operand.adjustment.type} onValueChange={changeAdjustmentType} disabled={busy}>
            <option value="percent">Percent</option>
            <option value="absolute">Absolute</option>
          </Select>
          <NumberField
            value={operand.adjustment.value}
            step="any"
            disabled={busy}
            onValueChange={(value: number) =>
              onChange(withAdjustment({ type: operand.adjustment?.type ?? "percent", value }))}
          />
        </div>
      {/if}
    </div>
  {/if}
</fieldset>
