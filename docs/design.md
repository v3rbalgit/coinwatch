# Coinwatch — Design

> The real-time Hyperliquid screener. This supersedes the earlier backtest-research direction.

## Product

A single-user, GUI-first **mission-control screener** for Hyperliquid perps: stream the live
universe, screen it on live metadata, chart any instrument, and define and run reusable
multi-timeframe personal signals. It is decision support, not an auto-trader.

The current product layers are the **live screener**, the **coin-detail chart and historical
validation overlay**, and the **personal signal catalog and run workspace**. On-demand runs and
historical validation are primarily authoring and diagnostic tools. The product payoff is
**alerting**: encode opportunities that would otherwise require watching charts, validate that the
definitions behave as intended, then let standing alerts bring the user back only when action may
be warranted.

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
signals          validated definitions, exact history, current and historical evaluation
screener/        Tauri commands/event channels and the SvelteKit UI
```

Live data flows into the hot cache while completed candle history accumulates in SQLite.
Signal definitions declare their exact history requirements; a run uses the hot window first,
then hydrates older completed candles through `CandleHistory` when necessary.

```text
Hyperliquid ──WS (allMids + fastAssetCtxs + candle)──▶ MarketStore ──▶ Browse · chart
            │                                               └─completed streamed bars──▶ SQLite history
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
  `candleSnapshot` (candle-history backfill). History uses a 2,000-row operational request
  ceiling, but advances from the actual oldest returned row so provider truncation remains safe.
- **`ratelimit`** — a weight-aware sliding-window limiter. Every REST call declares its
  Hyperliquid request weight (`candleSnapshot` ≈ 20 + candles/60, `metaAndAssetCtxs` = 20)
  and blocks until the trailing-60s total is within budget (default 1100/min, under HL's
  1200/min per-IP cap). One limiter is shared across all REST callers.
- **`store`** — `MarketStore`: candle buffers per (coin, timeframe), the volume-ranked
  universe, and a granular `MarketStatus` (connection state, sync phase, universe size,
  backfill progress, failure diagnostics, fetch rate, and storage durability) published on
  a `watch` channel. Hot buffers are retained only for the current universe.
- **`sync`** — refreshes the universe each cycle and re-plans fetches from per-series health:
  the initial backfill runs once per (coin, timeframe) and is not re-downloaded while current;
  the WebSocket then keeps the streamed timeframes live, REST advances non-streamed timeframes
  one bar at a time after a boundary grace, fully repairs gapped or disconnected series, and
  writes streamed series' freshly completed bars through to SQLite each cycle.
- **`event` / `feed`** — typed market events and a broadcast `MarketFeed` the UI and future
  consumers subscribe to.

## Key decisions

- **One WS session** for prices and candles, not two connections.
- **Hybrid candles**: the WebSocket keeps short timeframes live; REST backfills history,
  paced to the weight limit, and stays the liveness mechanism for the slow timeframes,
  whose bars complete rarely enough that one small fetch per close is nearly free.
- **Metadata-first Browse workspace**: it needs one `metaAndAssetCtxs` call, so it is usable
  immediately; chart depth and signal evidence improve as candle history accumulates.
- **Granular status** on a `watch` channel, surfaced plainly in the UI.

### Candle retention and degraded storage

- The hot `MarketStore` contains only markets in the latest successful active-universe snapshot.
  Removed or delisted markets are evicted, and late WebSocket frames cannot recreate them.
- SQLite candle rows are retained across universe removals and relistings. Hyperliquid does not
  provide a historical candle archive, so automatic deletion would discard data that may not be
  recoverable from its rolling API. Any future pruning must be an explicit user data policy.
- If the file database cannot open, the application uses a migrated in-memory SQLite store so
  live sync and history remain functional for the session. `MarketStatus` and the sidebar mark
  this mode as temporary; user-authored changes and accumulated candles will not survive restart.
- Runtime persistence and reconciliation failures remain visible through cumulative counters,
  the latest unresolved error, and the existing status event instead of a second health registry.

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
only the latest run retained in frontend memory. At most one run and one historical validation
execute at a time; starting another requires the previous to finish or be cancelled.

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
Indicator outputs carry domain metadata — price, price-delta (volatility), volume, bounded
oscillator, and unbounded oscillator; constants are domainless and compatible with any side.
Source pickers recommend or allow compatible operands, reject constant-versus-constant predicates
and disjoint-range comparisons, and reject equivalent sources that make comparisons tautological
or crosses impossible. Cumulative VWAP and OBV plus path-dependent Parabolic SAR and Supertrend
remain available as chart indicators but are excluded from signal definitions: their values
depend on where the supplied candle buffer begins and do not converge under finite history.

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

A saved signal or a materially changed current draft can run over the open coin chart and a preset
or custom history range. Coinwatch hydrates the selected range plus each timeframe's exact warm-up
through the shared SQLite/cache/provider history path; validation does not expand the chart's visible
history merely because older candles were fetched.

Evaluation advances on the definition's finest referenced completed-candle grid. At each point,
every condition sees only candles that had completed by then, including the latest completed
higher-timeframe candle. Divergences become visible on their confirmation bars, never on the earlier
pivot bars. Results are false-to-true onset markers and contiguous matched-state spans for the
complete definition, with the same condition evidence used by current signal runs. A signal already
active at the range start is distinguished from an onset at the start.

Selecting an occurrence pages older chart candles only when necessary, focuses the chart, and shows
the observed condition values. The chart keeps one non-persisted validation overlay: it can be
hidden, replaced, or removed without modifying the saved signal or chart settings. Historical
validation reports signal state and evidence; trade execution assumptions are outside this contract.

### Alerting

Running and historically validating a signal are the seed; standing **alerts** are the product
payoff. Creating or updating an alert captures a validated definition snapshot and applies it to a
coin, watchlist, or universe scope. Later edits to the source signal do not silently change a
standing alert: the alert remains on its snapshot until the user explicitly updates and revalidates
it. Watchlist and universe membership are resolved dynamically when the alert evaluates.

The completed-bar MVP evaluates once whenever the definition's finest referenced timeframe closes.
All conditions use their latest completed candles at that instant, so a higher-timeframe condition
cannot observe a forming bar and coincident timeframe-close events cannot produce duplicate
evaluations. Lifecycle state is persisted per alert and market:

1. No known prior state or a known false state becomes true: notify once.
2. True remains true: do not repeat.
3. True becomes false: rearm.
4. An unavailable or unknown evaluation neither notifies nor changes the last known lifecycle state.
5. A later known false-to-true transition: notify again.

The local MVP evaluates while Coinwatch is running. On startup it reconciles the latest completed
state with persisted lifecycle state instead of replaying every missed interval: a previously true
alert does not notify again merely because the app restarted, while a persisted false state that is
now true produces one notification. Notifications first appear in-app and through the local desktop
notification system; selecting one opens the affected coin and its signal evidence.

Optional repeat-while-true, setup-cleared, cooldown, intrabar, and missed-interval replay policies can
follow after the basic lifecycle is trustworthy. Telegram, Discord, email, and push can follow as
deployment needs grow. Always-on remote delivery is where a server-backed deployment (the Postgres
seam below) starts to matter.

Implementation sequence:

1. **Completed** — improve signal-authoring semantics, source compatibility, and diagnostic evidence.
2. **Completed** — add causal historical validation spans and evidence to the coin-detail chart.
3. **Next** — build the persisted completed-bar, false-to-true alerting MVP and local notifications.

## Persistence & deployment

Single-user and local. A `CandleStore` **trait seam** with a **local SQLite** implementation
(via `sqlx`) is in place: the DB is the accumulating candle cache, while the in-memory store keeps
a bounded hot window. Charts lazy-page older history and signal runs hydrate their exact completed-
bar requirements, so restarts reuse persisted candles instead of re-backfilling everything. SQLite
also stores watchlists, chart settings, saved signals, indicator presets, and cached logos, and will
store standing-alert snapshots plus their per-market lifecycle state. Historical validation results
and overlays remain ephemeral frontend state. The trait keeps candle persistence swappable: a
**Postgres** implementation can drop in later if the app grows into a server-backed deployment for
always-on alerts or multiple clients.

## History

Python CEX collector (`v1-python`) → exploratory Rust backtest-research engine → this real-time
screener.
