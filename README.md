# Coinwatch

A real-time **Hyperliquid market screener and mission-control** desktop app — a Tauri v2 +
SvelteKit front end over a Rust market-data engine. It streams the live perp universe,
screens it on live metadata (price, 24h change, volume, open interest, funding), and scans
candle history for RSI/MACD divergence setups across 1h / 4h / 1d / 1w.

Single-user and GUI-first: screen the market, (soon) chart it, filter by indicator, and keep
a journal — a decision-support tool, not an auto-trader.

> **History:** Coinwatch began as a Python CEX collector (archived on
> [`v1-python`](../../tree/v1-python), tag `v1.0-python`), then a Rust backtest-research
> engine (preserved on [`experiment/backtest-research`](../../tree/experiment/backtest-research)).
> This branch is the real-time screener. See [`DESIGN.md`](DESIGN.md) for the architecture.

## Architecture

A Cargo workspace of four library crates plus the Tauri app (details in [`DESIGN.md`](DESIGN.md)):

| Crate | Responsibility |
| ------ | --------------- |
| `domain` | Shared market types (`Candle`, `Timeframe`) |
| `hl-feed` | Hyperliquid engine: one WebSocket session (prices + candles), REST backfill (universe + candle history), an in-memory `MarketStore` hot cache, and a rate-limited sync |
| `indicators` | Pure indicator + divergence math (RSI, MACD; regular + hidden) |
| `scanner` | Pure divergence scan over the cache, per timeframe |
| `screener` (`screener/src-tauri`) | Tauri v2 app: commands read the cache; mids/candles stream to the UI |

The engine feeds an in-memory cache; the UI never waits on the network. Prices stream over the
WebSocket (~1s); candle history backfills over REST within Hyperliquid's rate limit.

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
just check   # cargo check across the workspace
just lint    # clippy, warnings denied (lint policy in Cargo.toml [workspace.lints])
just test    # cargo test across the workspace
just quality # full gate: fmt + clippy + test + doc + cargo-deny + git diff --check
```

Front-end checks: `cd screener && pnpm check` (svelte-check).

## License

MIT.
