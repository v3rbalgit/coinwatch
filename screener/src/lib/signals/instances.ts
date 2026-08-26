import type { IndicatorDescriptor, IndicatorInstance } from "$lib/chart/indicators"

interface IndicatorPresetSnapshot {
  indicatorId: string
  params: Readonly<Record<string, number>>
}

/**
 * Materialize every descriptor parameter without duplicating Rust's range/step
 * validation. Complete instances stay stable if registry defaults later change.
 */
function resolveIndicatorParams(
  descriptor: IndicatorDescriptor,
  params: Readonly<Record<string, number>>
): Record<string, number> {
  const knownKeys = new Set(descriptor.params.map(parameter => parameter.key))
  for (const key of Object.keys(params)) {
    if (!knownKeys.has(key)) throw new Error(`Unknown parameter '${key}' for indicator '${descriptor.id}'`)
  }

  return Object.fromEntries(
    descriptor.params.map(parameter => [parameter.key, params[parameter.key] ?? parameter.default])
  )
}

export function createIndicatorInstance(
  descriptor: IndicatorDescriptor,
  params: Readonly<Record<string, number>> = {}
): IndicatorInstance {
  return {
    id: descriptor.id,
    params: resolveIndicatorParams(descriptor, params),
  }
}

/** Selecting a preset copies its values into a new instance; no preset identity survives. */
export function applyIndicatorPreset(
  descriptor: IndicatorDescriptor,
  preset: IndicatorPresetSnapshot
): IndicatorInstance {
  if (preset.indicatorId !== descriptor.id) {
    throw new Error(`Preset for '${preset.indicatorId}' cannot configure '${descriptor.id}'`)
  }
  return createIndicatorInstance(descriptor, preset.params)
}
