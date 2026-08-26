# Coinwatch — Design

> The real-time Hyperliquid screener. This supersedes the earlier backtest-research design
> (preserved on `experiment/backtest-research`); that history is not the current direction.

## Product

A single-user, GUI-first **mission-control screener** for Hyperliquid perps: stream the live
universe, screen it on live metadata, chart any instrument, and define and run reusable
multi-timeframe personal signals. It is decision support, not an auto-trader.

The current product layers are the **live screener**, the **coin-detail chart**, and the
**personal signal catalog and run workspace**. On-demand runs are primarily an authoring and
diagnostic tool. The product payoff is **alerting**: encode opportunities that would otherwise
require watching charts, validate that the definitions behave as intended, then let standing
alerts bring the user back only when action may be warranted.

The core workflow is:

1. Express an opportunity as a reusable signal.
2. Test its current behavior and understand why markets do or do not match.
3. Validate past triggers in price-chart context.
4. Attach an alert to a coin, watchlist, or universe scope.
5. Wait for a notification instead of continuously watching charts.

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

## Roadmap: chart, signal authoring, validation & alerting

The chart and personal-signal workflows share one indicator definition and compute engine, so
visual analysis and completed-bar evaluation use the same parameter contracts.

### Coin-detail chart

A lightweight-charts terminal with candles, volume, lazy scroll-back history, crosshair / OHLCV
legend, log scale, drawing tools, trading price-lines, and toggleable indicator instances. The
chart computes through `indicators-wasm`, which wraps the same Rust `indicators` crate used by
signal evaluation.

### Personal signals

Conditions own their indicator parameters and timeframe and combine through AND/OR/NOT/XOR trees.
Definitions are named and stored in a personal catalog; current drafts and saved rich definitions
can run against the complete universe or an existing watchlist. A run is a **current cross-sectional
scan**: it produces one report per market from the latest completed candles, not a list of every
historical occurrence. Runs stream progress, per-market condition evidence, and data issues, with
only the latest run retained in frontend memory.

The generic evaluator remains expressive, but the authoring UI should present trading intent rather
than raw evaluator structure:

- A **state** predicate such as `RSI(14) < 30` means the relationship holds on the latest completed
  candle by default.
- An **event** predicate such as a cross or confirmed divergence may optionally remain actionable
  for a recency window.
- Recent-state lookback remains available as an advanced option, stated explicitly as “was true at
  least once no more than N bars ago.”
- `withinBars` remains a persisted age bound (`0` means latest completed candle), but it is not a
  mandatory-looking generic field. The UI gives it predicate-specific language and shows the
  approximate wall-clock duration for the selected timeframe.

The builder should retain broad source combinations while preventing or warning about nonsense.
Indicator outputs need domain metadata such as price, volume, percentage, bounded oscillator,
unbounded oscillator, and unitless. Source pickers can then recommend or allow compatible operands,
reject constant-versus-constant predicates, and warn about equivalent sources that make comparisons
tautological or crosses impossible. An advanced escape hatch is appropriate only where the units
remain meaningful.

Every definition should have a prominent plain-language preview, for example:

```text
Require all conditions:
- Latest completed 1h candle: RSI(14) is below 30.
- Latest completed 4h candle: RSI(14) is below 30.
```

Run results are part of authoring, not merely a match list. Evidence should show actual operand
values beside the required relationship. When nothing matches, the UI should explain which
conditions excluded each market and surface closest candidates by conditions satisfied or distance
to thresholds. Bullish/bearish **bias** is descriptive metadata unless the predicates themselves
encode that direction.

Completed candles are the reliable default. Forming-candle indicators can fluctuate and create
alerts that disappear before close; intrabar preview or alerting may be added later as an explicit,
more aggressive mode.

### Historical chart validation

A signal should be runnable over the open coin chart and a selected history range. Coinwatch marks
bars where the complete definition would have entered a matched state; selecting a marker reveals
the condition values and evidence at that time. This answers “would it have alerted where I expected
it to?” before the user trusts the definition as a standing alert.

This is **historical validation**, not yet a trading backtest: it does not claim PnL without explicit
entry, exit, sizing, fee, and execution assumptions. Multi-timeframe evaluation must align completed
higher-timeframe bars onto the chart timeline, and divergence markers must remain on their
confirmation bars so the display never introduces look-ahead.

### Alerting

Running a signal manually is the seed; standing **alerts** are the product payoff. An alert applies a
signal to a coin, watchlist, or universe scope and evaluates when any relevant completed timeframe
closes. The default lifecycle is edge-triggered and deduplicated per alert and market:

1. False or unknown becomes true: notify once.
2. True remains true: do not repeat.
3. True becomes false: rearm.
4. A later false-to-true transition: notify again.

Optional repeat-while-true, setup-cleared, cooldown, and intrabar policies can follow after the
basic lifecycle is trustworthy. Delivery starts with local desktop/in-app notifications, then adds
Telegram, Discord, email, and push as deployment needs grow. Always-on remote delivery is where a
server-backed deployment (the Postgres seam below) starts to matter.

Implementation sequence:

1. Improve signal-authoring semantics, source compatibility, and diagnostic evidence.
2. Add historical validation markers and evidence to the coin-detail chart.
3. Build the completed-bar, false-to-true alerting MVP and its first notification channel.

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
