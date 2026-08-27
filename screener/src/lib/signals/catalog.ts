import { invoke } from "@tauri-apps/api/core"

import { hasNumber, hasString, isRecord } from "$lib/ipc/guards"
import type { SignalDefinition } from "$lib/signals/definition"

export type SignalCatalogIssue =
  { type: "unsupportedVersion"; version: number } | { type: "invalidDefinition"; message: string }

export type SignalCatalogEntry =
  | {
      status: "ready"
      id: string
      name: string
      definition: SignalDefinition
    }
  | {
      status: "unavailable"
      id: string
      name: string
      issue: SignalCatalogIssue
    }

export type SignalCatalogError =
  | { type: "invalidName" }
  | { type: "invalidDefinition"; message: string }
  | { type: "duplicateName"; name: string }
  | { type: "notFound"; id: string }
  | { type: "storage" }

export function isSignalCatalogError(value: unknown): value is SignalCatalogError {
  if (!isRecord(value) || !hasString(value, "type")) return false
  switch (value.type) {
    case "invalidName":
    case "storage":
      return true
    case "invalidDefinition":
      return hasString(value, "message")
    case "duplicateName":
      return hasString(value, "name")
    case "notFound":
      return hasString(value, "id")
    default:
      return false
  }
}

export function isSignalCatalogIssue(value: unknown): value is SignalCatalogIssue {
  if (!isRecord(value) || !hasString(value, "type")) return false
  switch (value.type) {
    case "unsupportedVersion":
      return hasNumber(value, "version")
    case "invalidDefinition":
      return hasString(value, "message")
    default:
      return false
  }
}

export function listSignals(): Promise<SignalCatalogEntry[]> {
  return invoke<SignalCatalogEntry[]>("list_signals")
}

export function getSignal(id: string): Promise<SignalCatalogEntry> {
  return invoke<SignalCatalogEntry>("get_signal", { id })
}

export function createSignal(name: string, definition: SignalDefinition): Promise<SignalCatalogEntry> {
  return invoke<SignalCatalogEntry>("create_signal", { name, definition })
}

export function updateSignal(id: string, name: string, definition: SignalDefinition): Promise<SignalCatalogEntry> {
  return invoke<SignalCatalogEntry>("update_signal", { id, name, definition })
}

export function deleteSignal(id: string): Promise<void> {
  return invoke<void>("delete_signal", { id })
}
