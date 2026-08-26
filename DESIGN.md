# Coinwatch — Design

> The real-time Hyperliquid screener. This supersedes the earlier backtest-research design
> (preserved on `experiment/backtest-research`); that history is not the current direction.

## Product

A single-user, GUI-first **mission-control screener** for Hyperliquid perps: stream the live
universe, screen it on live metadata, chart any instrument, and define and run reusable
multi-timeframe personal signals. It is decision support, not an auto-trader.

The current product layers are the **live screener**, the **coin-detail chart**, and the
**personal signal catalog and run workspace**. **Alerting** is the next major workflow: standing
runs and delivery channels turn on-demand evaluation into continuous decision support.

## Shape

Tauri v2 + SvelteKit (Svelte 5 runes) over a Rust market-data engine, as a Cargo workspace:

```text
domain           shared market types (Candle, Timeframe)
hl-feed          the Hyperliquid engine — WS + REST + in-memory cache + sync
storage          SQLite candles and user data behind a CandleStore trait
indicators       indicator/divergence math, descriptors, validation, and compute engine
indicators-wasm  wasm-bindgen chart boundary over the shared indicator engine
signals          validated definitions, history requirements, evaluation, and run orchestration
screener/        Tauri commands/event channels and the SvelteKit UI
```

Live data flows into the hot cache while completed candle history accumulates in SQLite.
Signal definitions declare their exact history requirements; a run uses the hot window first,
then hydrates older completed candles through `CandleHistory` when necessary.

```text
Hyperliquid ──WS (allMids + fastAssetCtxs + candle)──▶ MarketStore ──▶ Browse · chart
            └─REST (metaAndAssetCtxs + candleSnapshot)──┬────────────▶ SQLite history
                                                        └────────────▶ signal hydration
SQLite user data ──▶ watchlists · signal catalog · presets ──────────▶ Signals workspace
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
- **Metadata-first Browse workspace**: it needs one `metaAndAssetCtxs` call, so it is usable
  immediately; chart depth and signal evidence improve as candle history accumulates.
- **Granular status** on a `watch` channel, surfaced plainly in the UI.

## Roadmap: chart, signals & alerting

The chart and personal-signal workflows share one indicator definition and compute engine, so
visual analysis and completed-bar evaluation use the same parameter contracts.

**Coin-detail chart**. A lightweight-charts terminal with candles, volume, lazy scroll-back
history, crosshair / OHLCV legend, log scale, drawing tools, trading price-lines, and toggleable
indicator instances. The chart computes through `indicators-wasm`, which wraps the same Rust
`indicators` crate used by signal evaluation.

**Personal signals**. Conditions own their indicator parameters and timeframe and combine through
AND/OR/NOT/XOR trees. Definitions are named and stored in a personal catalog; current drafts and
saved rich definitions can run against the complete universe or an existing watchlist. Runs stream
progress, per-market condition evidence, and data issues, with only the latest run retained in
frontend memory. A multi-timeframe result cannot fully render on one chart; reviewing all of its
components still requires switching chart timeframes.

**Alerting** (the payoff). Running a signal on a button is still only the seed; the value is
standing **alerts** that evaluate a signal continuously and push notifications over channels
added gradually — a **Telegram** bot first, then Discord, then email / in-app / push. This is
where a server-backed deployment (the Postgres seam below) starts to matter, for always-on
alerts.

## Persistence & deployment

Single-user and local. A `CandleStore` **trait seam** with a **local SQLite** implementation
(via `sqlx`) is in place: the DB is the accumulating candle cache, while the in-memory store keeps
a bounded hot window. Charts lazy-page older history and signal runs hydrate their exact completed-
bar requirements, so restarts reuse persisted candles instead of re-backfilling everything. SQLite
also stores watchlists, chart settings, saved signals, indicator presets, and cached logos. The trait
keeps candle persistence swappable: a **Postgres** implementation can drop in later if the app grows
into a server-backed deployment for always-on alerts or multiple clients.

## History

Python CEX collector (`v1-python`) → Rust backtest-research engine
(`experiment/backtest-research`) → this real-time screener.
