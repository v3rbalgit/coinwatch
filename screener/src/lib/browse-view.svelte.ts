export type SortKey = "coin" | "price" | "change_24h_pct" | "volume_usd" | "open_interest_usd" | "funding"

// Browse table view state, kept module-level so it survives navigating into a coin
// and back. The page component remounts on `goto`, which loses component-local state
// (and SvelteKit snapshots only restore on history back/forward, not a fresh goto).
export const browseView = $state<{ sortKey: SortKey; sortDir: 1 | -1; query: string; activeList: string | null }>({
  sortKey: "volume_usd",
  sortDir: -1,
  query: "",
  activeList: null,
})
