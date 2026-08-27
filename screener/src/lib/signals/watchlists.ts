import { watchlists } from "$lib/ipc/screener"
import type { ListView } from "$lib/ipc/screener"

export type SignalWatchlist = ListView

export function listSignalWatchlists(): Promise<SignalWatchlist[]> {
  return watchlists()
}
