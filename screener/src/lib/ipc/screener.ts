import { invoke } from "@tauri-apps/api/core"

import type { Bar } from "$lib/chart/types"

/** One `screener_rows` row; field names mirror the Rust `ScreenerRow` payload. */
export type ScreenerRow = {
  coin: string
  price: number
  prev_day: number
  volume_usd: number
  open_interest_usd: number
  funding: number
}

export type ListView = { name: string; coins: string[] }

export function screenerRows(): Promise<ScreenerRow[]> {
  return invoke<ScreenerRow[]>("screener_rows")
}

export function watchlists(): Promise<ListView[]> {
  return invoke<ListView[]>("lists")
}

export function logos(): Promise<Record<string, string>> {
  return invoke<Record<string, string>>("logos")
}

export function addToList(list: string, coin: string): Promise<void> {
  return invoke<void>("add_to_list", { list, coin })
}

export function removeFromList(list: string, coin: string): Promise<void> {
  return invoke<void>("remove_from_list", { list, coin })
}

export function createList(name: string): Promise<void> {
  return invoke<void>("create_list", { name })
}

export function deleteList(name: string): Promise<void> {
  return invoke<void>("delete_list", { name })
}

export function openOnHyperliquid(coin: string): Promise<void> {
  return invoke<void>("open_on_hyperliquid", { coin })
}

export function setActiveCoin(coin: string | null): Promise<void> {
  return invoke<void>("set_active_coin", { coin })
}

export function coinCandles(coin: string, timeframe: string): Promise<Bar[]> {
  return invoke<Bar[]>("coin_candles", { coin, timeframe })
}

export function coinHistory(coin: string, timeframe: string, beforeSeconds: number): Promise<Bar[]> {
  return invoke<Bar[]>("coin_history", { coin, timeframe, beforeSeconds })
}

export function getSetting(key: string): Promise<string | null> {
  return invoke<string | null>("get_setting", { key })
}

export function setSetting(key: string, value: string): Promise<void> {
  return invoke<void>("set_setting", { key, value })
}
