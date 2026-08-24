# Coinwatch — Design

> The real-time Hyperliquid screener. This supersedes the earlier backtest-research design
> (preserved on `experiment/backtest-research`); that history is not the current direction.

## Product

A single-user, GUI-first **mission-control screener** for Hyperliquid perps: stream the live
universe, screen it on live metadata, scan candle history for divergence setups, and — next —
chart, filter-by-indicator, and journal. It is decision support, not an auto-trader.

## Shape

Tauri v2 + SvelteKit (Svelte 5 runes) over a Rust market-data engine, as a Cargo workspace:

```
domain      shared market types (Candle, Timeframe)
hl-feed     the Hyperliquid engine — WS + REST + in-memory cache + sync
indicators  pure indicator / divergence math (RSI, MACD; regular + hidden)
scanner     pure divergence scan over the cache
screener/   Tauri app: commands + event streams to the SvelteKit UI
```

Data flows one way into a hot cache; reads never touch the network:

```
Hyperliquid ──WS (allMids + fastAssetCtxs + candle)──┐
            └─REST (metaAndAssetCtxs + candleSnapshot)┤
                                                       ▼
                                              MarketStore (in-memory)
                                                       ▼
                                     screener rows · divergence scan · UI
```

## hl-feed internals

- **`stream`** — one WebSocket session. Subscribes `allMids` + `fastAssetCtxs` (prices) and,
  once the universe is known, `candle` streams for the configured timeframes. Publishes
  `MarketEvent::Mid` / `MarketEvent::Candle` on a shared `MarketFeed` and merges candles into
  the store. Reconnects with exponential backoff.
- **`rest`** — `metaAndAssetCtxs` (universe + funding / OI / mark / prev-day) and
  `candleSnapshot` (candle-history backfill).
- **`ratelimit`** — a weight-aware sliding-window limiter. Every REST call declares its
  Hyperliquid request weight (`candleSnapshot` ≈ 20 + candles/60, `metaAndAssetCtxs` = 20)
  and blocks until the trailing-60s total is within budget (default 1100/min, under HL's
  1200/min per-IP cap). One limiter is shared across all REST callers.
- **`store`** — `MarketStore`: candle buffers per (coin, timeframe), the volume-ranked
  universe, and a granular `MarketStatus` (connection state, sync phase, universe size,
  backfill progress, fetch rate) published on a `watch` channel.
- **`sync`** — refreshes the universe each cycle and backfills each (coin, timeframe) **once**;
  the WebSocket then keeps the streamed timeframes live. Hybrid candles: WS for the
  live/short timeframes, REST for history.
- **`event` / `feed`** — typed market events and a broadcast `MarketFeed` the UI and future
  consumers subscribe to.

## Key decisions

- **One WS session** for prices and candles, not two connections.
- **Hybrid candles**: the WebSocket keeps short timeframes live; REST backfills history once,
  paced to the weight limit and tracked so it isn't re-downloaded every cycle.
- **Metadata-only screener**: it needs one `metaAndAssetCtxs` call, so it is usable
  immediately; the slower candle backfill only gates the scanner.
- **Granular status** on a `watch` channel, surfaced plainly in the UI.

## Persistence & deployment

Single-user and local. **Being added:** a `CandleStore` **trait seam** with a **local SQLite**
implementation (via `sqlx`), so restarts reuse downloaded candle history instead of
re-backfilling, and so it becomes the foundation for future user state (journal, alerts,
settings). The trait keeps the backend swappable: a **Postgres** implementation can drop in
later **if** the app ever grows into a server-backed deployment (always-on alerts, multiple
clients). We deliberately do **not** stand up managed Postgres/Redis now — nothing durable yet
needs them, and the in-memory store is the hot cache.

## History

Python CEX collector (`v1-python`) → Rust backtest-research engine
(`experiment/backtest-research`) → this real-time screener.
