import type { MarketEngineStatus } from "$lib/ipc/events"

export type { MarketEngineStatus } from "$lib/ipc/events"

/** A completed sync phase is degraded while its latest retry still has an error. */
export function candleEngineIsDegraded(status: MarketEngineStatus): boolean {
  return status.backfill_errors > 0 || status.last_reconciliation_error !== null
}

/** Temporary storage and unresolved operation failures both require user attention. */
export function storageIsDegraded(status: MarketEngineStatus): boolean {
  return status.storage_mode === "temporary" || status.last_storage_error !== null
}
