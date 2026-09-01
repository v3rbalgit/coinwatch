import { describe, expect, it } from "vitest"

import { candleEngineIsDegraded, storageIsDegraded } from "../src/lib/engine-status"
import type { MarketEngineStatus } from "../src/lib/ipc/events"

function status(overrides: Partial<MarketEngineStatus> = {}): MarketEngineStatus {
  return {
    connection: "live",
    phase: "live",
    universe: 177,
    series_done: 0,
    series_total: 0,
    series_errors: 0,
    streaming: 354,
    series_jobs_per_minute: 0,
    storage_mode: "persistent",
    storage_failures: 0,
    last_storage_error: null,
    reconciliation_failures: 0,
    last_reconciliation_error: null,
    ...overrides,
  }
}

describe("engine status degradation", () => {
  it("keeps a failed reconciliation visible after the sync phase completes", () => {
    expect(candleEngineIsDegraded(status())).toBe(false)
    expect(candleEngineIsDegraded(status({ series_errors: 1 }))).toBe(true)
    expect(candleEngineIsDegraded(status({ last_reconciliation_error: "universe refresh failed" }))).toBe(true)
  })

  it("distinguishes durable storage from temporary and unresolved failures", () => {
    expect(storageIsDegraded(status())).toBe(false)
    expect(storageIsDegraded(status({ storage_mode: "temporary" }))).toBe(true)
    expect(storageIsDegraded(status({ last_storage_error: "database is locked" }))).toBe(true)
    expect(storageIsDegraded(status({ storage_failures: 3 }))).toBe(false)
  })
})
