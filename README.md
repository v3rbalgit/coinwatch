# Coinwatch

A real-time **Hyperliquid market screener and mission-control** desktop app — a Tauri v2 +
SvelteKit front end over a Rust market-data engine. It streams the live perp universe,
screens it on live metadata (price, 24h change, volume, open interest, funding), evaluates
saved or draft multi-timeframe personal signals against the universe or a watchlist, and
validates a signal over the open coin chart's history, marking where it would have matched.

Single-user and GUI-first: screen the market, chart it with configurable indicators and
drawings, and build reusable multi-timeframe signals — a decision-support tool, not an
auto-trader.

> **History:** Coinwatch began as a Python CEX collector (archived on
> [`v1-python`](../../tree/v1-python), tag `v1.0-python`), then a Rust backtest-research
> engine (preserved on [`experiment/backtest-research`](../../tree/experiment/backtest-research)).
> This branch is the real-time screener. See [`DESIGN.md`](DESIGN.md) for the architecture.

## Architecture

A Cargo workspace of focused Rust libraries plus the Tauri app (details in [`DESIGN.md`](DESIGN.md)):

| Crate                             | Responsibility                                                                                                  |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `domain`                          | Shared market types (`Candle`, `Timeframe`)                                                                     |
| `hl-feed`                         | Hyperliquid engine: WebSocket prices/candles, REST backfill, the in-memory `MarketStore`, and rate-limited sync |
| `storage`                         | SQLite migrations and persistence for candles, watchlists, signals, presets, settings, and logos                |
| `indicators`                      | Pure indicator/divergence math plus the configured-instance registry and compute engine                         |
| `indicators-wasm`                 | Thin WASM boundary exposing the shared indicator engine to the chart and builder                                |
| `signals`                         | Validated signal definitions plus completed-bar, multi-timeframe evaluation, run orchestration, and historical validation |
| `screener` (`screener/src-tauri`) | Tauri v2 app: commands expose market/user data, signal runs, and historical validation; mids/candles stream to the UI |

Prices stream through the in-memory hot cache over WebSocket (~1s). Candle history accumulates
in SQLite and is backfilled over REST within Hyperliquid's rate limit; signal runs hydrate the
exact completed-bar history required by each definition.

## Prerequisites

- **Rust** (stable toolchain).
- **Node + pnpm** for the front end.
- **System WebKitGTK** for the Tauri window (Linux: `webkit2gtk-4.1`, `libgtk-3`, and the
  usual Tauri build dependencies).
- A **display** — it is a desktop GUI and cannot run headless.

## Run

```bash
just setup   # install front-end deps (once, and after dependency changes)
just run     # launch the desktop app in dev (Vite on :1420 + the Rust app)
```

Or directly: `cd screener && pnpm install && pnpm tauri dev`.

## Development

```bash
just check        # cargo check across the workspace (SQLx offline metadata)
just lint         # clippy, warnings denied (lint policy in Cargo.toml)
just test         # cargo test across the workspace
just sqlx-check   # verify migrations, compile-time queries, and .sqlx metadata
just quality      # full gate: fmt + clippy + test + doc + cargo-deny + diff check
```

Install `cargo-sqlx` when changing database queries or migrations. Run
`just sqlx-prepare` after such changes and commit the generated `.sqlx` metadata.
Regular builds and CI use that metadata and do not need a database connection.

Front-end checks: `cd screener && pnpm check` (svelte-check).

## License

MIT.
