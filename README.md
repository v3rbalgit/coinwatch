# Coinwatch

A modular, **backtest-first quant research platform** for on-chain (DEX) crypto markets,
written in Rust. It ingests on-chain market data, computes pluggable feature modules,
backtests strategies with honest transaction costs and point-in-time correctness, and
validates them with walk-forward cross-validation and overfitting-aware statistics.

The goal is a durable **research harness** — strategies decay, the harness outlives them —
on the path from *finding* an edge to *verifying* it (realistic backtest + paper trading)
to *executing* it on-chain.

> **History:** Coinwatch began as a Python CEX data-collection system. That original
> project is archived on the [`v1-python`](../../tree/v1-python) branch (tag `v1.0-python`).
> `main` is the Rust rewrite. See [`DESIGN.md`](DESIGN.md) for architecture and rationale.

## Status

Working end-to-end research pipeline (research-grade, **not** an alpha generator yet):
ingest → store → features → strategy → cost-aware backtest → walk-forward CV → metrics.
Demo backtests are architecture confirmation on a small in-sample window — the platform's
own Deflated Sharpe Ratio says so out loud. The current focus is deepening data and adding
differentiated on-chain features so the search can surface real, verifiable edges.

## Architecture

A Cargo workspace of small crates wired as a clean DAG (details in [`DESIGN.md`](DESIGN.md)):

| Crate | Responsibility |
| ------- | ---------------- |
| `cw-core` | Canonical domain types, fixed-point `Decimal`, shared errors |
| `cw-storage` | DuckDB-backed bar store + instrument registry (point-in-time reads) |
| `cw-providers` | `DataSource` / `UniverseSource` seams; GeckoTerminal adapter |
| `cw-features` | `FeatureModule` seam; technical + liquidity modules; `combine()` |
| `cw-strategy` | `Strategy` seam; cross-sectional strategy |
| `cw-backtest` | Point-in-time engine; fees + slippage + gas; equity curve + ledger |
| `cw-eval` | Sharpe/Sortino/maxDD/turnover/hit-rate + PSR + Deflated Sharpe |
| `cw-harness` | Walk-forward CV: fold sweep, OOS selection, deflation |
| `cw-app` | `coinwatch` CLI: `ingest` / `analyze` |

The engine sees only canonical types and trait seams, so new data providers and edge
hypotheses (feature modules) slot in without touching it.

## Quickstart

Requires a Rust toolchain and a C++ compiler (for the bundled DuckDB build).

```bash
# Discover the universe (Base + Arbitrum) and backfill daily bars into DuckDB.
# Networked + incremental; safe to re-run (skips up-to-date pools).
cargo run -p cw-app -- ingest

# Offline research over the stored universe: features, leaderboard,
# in-sample backtests, and honest walk-forward CV with Deflated Sharpe.
cargo run -p cw-app -- analyze
```

Data lands in `research/data/coinwatch.duckdb`. The research layer (Polars/DuckDB) reads
the same artifacts for ad-hoc analysis.

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## License

MIT.
