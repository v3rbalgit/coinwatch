import { createSignalValidationClient } from "$lib/signals/historical"
import {
  createHistoricalValidationWorkspace,
  createInitialHistoricalValidationWorkspaceState,
  type HistoricalValidationSnapshot,
} from "$lib/signals/historical-workspace"

const state = $state(createInitialHistoricalValidationWorkspaceState())
const controller = createHistoricalValidationWorkspace(createSignalValidationClient(), state)

export const historicalValidationWorkspace = controller.state

export function startHistoricalValidation(snapshot: HistoricalValidationSnapshot): Promise<void> {
  return controller.start(snapshot)
}

export function cancelHistoricalValidation(): Promise<void> {
  return controller.cancel()
}

export function selectHistoricalValidationSpan(index: number | null): void {
  controller.selectSpan(index)
}

export function setHistoricalValidationVisible(visible: boolean): void {
  controller.setVisible(visible)
}

export function setHistoricalValidationPanelOpen(open: boolean): void {
  controller.setPanelOpen(open)
}

export function clearHistoricalValidation(): void {
  controller.clear()
}
