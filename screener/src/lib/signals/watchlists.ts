import { invoke } from "@tauri-apps/api/core"

export interface SignalWatchlist {
  name: string
  coins: string[]
}

export function listSignalWatchlists(): Promise<SignalWatchlist[]> {
  return invoke<SignalWatchlist[]>("lists")
}
