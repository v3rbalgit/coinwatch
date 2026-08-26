import { invoke } from "@tauri-apps/api/core"

import { hasString, isStringRecord } from "$lib/signals/ipc"

export type IndicatorPresetIssue = { type: "invalidPreset"; message: string }

export type IndicatorPresetEntry =
  | {
      status: "ready"
      id: string
      indicatorId: string
      name: string
      params: Record<string, number>
    }
  | {
      status: "unavailable"
      id: string
      indicatorId: string
      name: string
      issue: IndicatorPresetIssue
    }

export type IndicatorPresetError =
  | { type: "invalidName" }
  | { type: "invalidPreset"; message: string }
  | { type: "duplicateName"; indicatorId: string; name: string }
  | { type: "notFound"; id: string }
  | { type: "storage" }

export function isIndicatorPresetError(value: unknown): value is IndicatorPresetError {
  if (!isStringRecord(value) || !hasString(value, "type")) return false
  switch (value.type) {
    case "invalidName":
    case "storage":
      return true
    case "invalidPreset":
      return hasString(value, "message")
    case "duplicateName":
      return hasString(value, "indicatorId") && hasString(value, "name")
    case "notFound":
      return hasString(value, "id")
    default:
      return false
  }
}

export function isIndicatorPresetIssue(value: unknown): value is IndicatorPresetIssue {
  return isStringRecord(value) && value.type === "invalidPreset" && hasString(value, "message")
}

export function listIndicatorPresets(): Promise<IndicatorPresetEntry[]> {
  return invoke<IndicatorPresetEntry[]>("list_indicator_presets")
}

export function createIndicatorPreset(
  indicatorId: string,
  name: string,
  params: Record<string, number>
): Promise<IndicatorPresetEntry> {
  return invoke<IndicatorPresetEntry>("create_indicator_preset", { indicatorId, name, params })
}

export function updateIndicatorPreset(
  id: string,
  name: string,
  params: Record<string, number>
): Promise<IndicatorPresetEntry> {
  return invoke<IndicatorPresetEntry>("update_indicator_preset", { id, name, params })
}

export function deleteIndicatorPreset(id: string): Promise<void> {
  return invoke<void>("delete_indicator_preset", { id })
}
