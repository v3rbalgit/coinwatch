# Coinwatch — Design

> The real-time Hyperliquid screener. This supersedes the earlier backtest-research design
> (preserved on `experiment/backtest-research`); that history is not the current direction.

## Product

A single-user, GUI-first **mission-control screener** for Hyperliquid perps: stream the live
universe, screen it on live metadata, chart any instrument, and scan candle history for setups.
It is decision support, not an auto-trader.

The trajectory, in order: **live screener** (done) → **coin-detail chart** with drawing tools,
price-lines, and toggleable indicators (in progress) → a modular **signal builder** →
**alerting**. The screener and one-shot scan are the seed; the builder and standing alerts are
where the value compounds (see *Roadmap*).

## Shape

Tauri v2 + SvelteKit (Svelte 5 runes) over a Rust market-data engine, as a Cargo workspace:

```
domain           shared market types (Candle, Timeframe)
hl-feed          the Hyperliquid engine — WS + REST + in-memory cache + sync
storage          accumulating SQLite candle cache (sqlx) behind a CandleStore trait
indicators       pure indicator + divergence math (17 indicators; regular + hidden divergence)
indicators-wasm  wasm-bindgen wrapper so the chart runs the same indicator code as the scanner
scanner          pure divergence scan over the cache
screener/        Tauri app: commands + event streams to the SvelteKit UI
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

## Roadmap: chart, signals & alerting

The screener and one-shot scan are the seed; the value compounds through three stages.

**Coin-detail chart** (in progress). A lightweight-charts terminal: candles + volume, lazy
scroll-back history, crosshair / OHLCV legend, log scale, drawing tools, trading price-lines,
and **toggleable indicators**. Every indicator is computed in Rust (through `indicators-wasm`),
so the chart and the scanner can never drift.

**Indicators on the chart are visibility toggles, not configuration.** The chart renders an
*instance-driven* list — each drawn indicator is `{id, params, target, outputs}` — but the chart
UI only shows/hides them; params come from conventional Rust defaults, and RSI/MACD/divergence
stay pinned to the scanner's config. That same renderer later draws a fired signal's exact
conditions ("why did it fire"), so no rework is needed when the builder lands. Indicator
*configuration* lives in the signal builder, never on the chart.

**Signal builder** (next). A modular workflow that composes signals from indicator
**conditions**: each condition is an indicator with its own params **and its own timeframe**
(e.g. RSI > 80 on 1h *and* 4h *and* 1d), combined with logical operators (AND/OR/NOT/XOR) and,
where it fits, weighting. Signals are named, saved to a personal **catalog**, then run on demand
against a Browse **list** or a single instrument — RSI/MACD/divergence are just the most basic
conditions, and signals may themselves compose other signals. A multi-timeframe signal can't
fully render on one chart; reviewing all of its components means switching the chart's timeframe
(an accepted limitation).

**Alerting** (the payoff). Running a signal on a button is still only the seed; the value is
standing **alerts** that evaluate a signal continuously and push notifications over channels
added gradually — a **Telegram** bot first, then Discord, then email / in-app / push. This is
where a server-backed deployment (the Postgres seam below) starts to matter, for always-on
alerts.

## Persistence & deployment

Single-user and local. A `CandleStore` **trait seam** with a **local SQLite** implementation
(via `sqlx`) is in place: the DB is the **accumulating candle cache** — charts lazy-page history
into it while the in-memory store keeps the scanner's hot window — so restarts reuse history
instead of re-backfilling. It is the foundation for user state: chart settings, saved signals,
and alerts. The trait keeps the backend swappable: a **Postgres** implementation can drop in
later **if** the app grows into a server-backed deployment (always-on alerts, multiple clients).
We deliberately do **not** stand up managed Postgres/Redis now — nothing durable yet needs them,
and the in-memory store is the hot cache.

## History

Python CEX collector (`v1-python`) → Rust backtest-research engine
(`experiment/backtest-research`) → this real-time screener.
