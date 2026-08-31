import { invoke } from "@tauri-apps/api/core"
import { listen } from "@tauri-apps/api/event"
import type { UnlistenFn } from "@tauri-apps/api/event"

import type { Bar } from "$lib/chart/types"
import type { CandleHealth } from "$lib/ipc/candles"

export type MidEvent = { coin: string; price: number }

export type CandleEvent = {
  coin: string
  timeframe: string
  candle: Bar
  health: CandleHealth
  revision: number
}

export type CandleSnapshotUpdate = {
  coin: string
  timeframe: string
  health: CandleHealth
  revision: number
}

/** Engine status snapshot; field names mirror the Rust `MarketStatus`. */
export type MarketEngineStatus = {
  connection: "connecting" | "live" | "reconnecting"
  phase: "loading" | "backfilling" | "live"
  universe: number
  backfill_done: number
  backfill_total: number
  backfill_errors: number
  streaming: number
  fetch_rate: number
  storage_mode: "initializing" | "persistent" | "temporary"
  storage_failures: number
  last_storage_error: string | null
  reconciliation_failures: number
  last_reconciliation_error: string | null
}

export function onMid(handler: (event: MidEvent) => void): Promise<UnlistenFn> {
  return listen<MidEvent>("mid", event => handler(event.payload))
}

export function onCandle(handler: (event: CandleEvent) => void): Promise<UnlistenFn> {
  return listen<CandleEvent>("candle", event => handler(event.payload))
}

export function onCandleSnapshotUpdated(handler: (event: CandleSnapshotUpdate) => void): Promise<UnlistenFn> {
  return listen<CandleSnapshotUpdate>("candle-snapshot-updated", event => handler(event.payload))
}

export function marketStatus(): Promise<MarketEngineStatus> {
  return invoke<MarketEngineStatus>("market_status")
}

export function onStatus(handler: (status: MarketEngineStatus) => void): Promise<UnlistenFn> {
  return listen<MarketEngineStatus>("status", event => handler(event.payload))
}
