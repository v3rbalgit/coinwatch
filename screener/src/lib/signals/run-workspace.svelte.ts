import { createSignalRunClient } from "$lib/signals/run"
import type { SignalRunResultFilter } from "$lib/signals/run-results"
import { createInitialSignalRunWorkspaceState, createSignalRunWorkspace } from "$lib/signals/run-workspace"
import type { SignalRunSnapshot } from "$lib/signals/run-workspace"

export type { SignalRunSnapshot, SignalRunWorkspaceState } from "$lib/signals/run-workspace"

const state = $state(createInitialSignalRunWorkspaceState())
const defaultWorkspace = createSignalRunWorkspace(createSignalRunClient(), state)
export const signalRunWorkspace = defaultWorkspace.state

export function signalRunIsActive(): boolean {
  return defaultWorkspace.isActive()
}

export function showSignalRunDefinition(): void {
  defaultWorkspace.showDefinition()
}

export function showSignalRunResults(): void {
  defaultWorkspace.showResults()
}

export function setSignalRunResultFilter(filter: SignalRunResultFilter): void {
  defaultWorkspace.setFilter(filter)
}

export function startSignalWorkspaceRun(snapshot: SignalRunSnapshot): Promise<void> {
  return defaultWorkspace.start(snapshot)
}

export function cancelSignalWorkspaceRun(): Promise<void> {
  return defaultWorkspace.cancel()
}
