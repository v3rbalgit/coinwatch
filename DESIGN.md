# Coinwatch — Quant Research & Execution Platform (Rework)

> Status: **Design / Phase 0**. This document is the source of truth for the rework of
> `coinwatch` from a Python CEX data-collection system into a modular, Rust-core,
> backtest-first quant research platform for **on-chain (DEX)** opportunities.

## 1. Vision

Build a **modular research platform** that can discover, validate, and eventually act on
statistical edges in on-chain crypto markets. The durable asset is the
**research/backtest harness**, not any single strategy — strategies decay, the harness
outlives them.

Each "edge hypothesis" (technical, tokenomics, on-chain activity, wallet flow,
new-launch discovery, cross-venue arbitrage) is a **pluggable module** that feeds one
shared pipeline:

```
ingest → canonical store → feature modules → strategy → cost-aware backtest → evaluation
                                                              │
                                                              └── (later) live paper-trade → real execution
```

The same `Strategy` implementation runs in **backtest and live** — that single source of
truth is what lets a backtested edge be trusted in deployment.

### Non-negotiable principles

- **Backtest-first.** No real capital until an edge survives cost-aware, walk-forward,
  overfitting-resistant evaluation.
- **Point-in-time correctness.** No lookahead. Features/signals at time *t* may only use
  data available at *t*.
- **Honest costs.** Every backtest models fees, gas, and slippage vs. actual pool
  liquidity. A backtest that ignores costs is a lie.
- **Modularity via trait seams.** Adding an edge hypothesis must not require touching the
  engine.
- **Research velocity where it matters.** Rust for the durable pipeline; Polars/DuckDB
  (both Rust-core) for fast hypothesis iteration over the *same* artifacts.

## 2. Scope decisions (locked for Phase 1)

| Decision            | Choice                                                        | Rationale |
|---------------------|--------------------------------------------------------------|-----------|
| First deliverable   | Research + backtest platform                                 | De-risks everything; live trading only after a proven edge. |
| Chain focus         | **EVM L2s first (Base / Arbitrum)**                           | Standardized ERC-20/Uniswap-style events → ~90% code reuse across EVM chains; gentler infra; cleanly backtestable. |
| First focus module  | **Cross-sectional systematic strategy on liquid EVM tokens** | Most cleanly backtestable with the cheapest data; exercises the hardest parts of the harness (point-in-time, costs, portfolio construction, survivorship). Not a latency race. |
| Stack               | **Rust core + Polars/DuckDB research layer**                 | Correctness/performance in the pipeline; fast iteration in research. Both share one Parquet/DuckDB data contract. |
| Deployment target   | Small always-on VPS (e.g. DigitalOcean), observable in real time | Cheap, controllable; no self-hosted node required initially. |

**Explicitly deferred:** new-launch sniping (adversarial latency race), self-hosted RPC
node (HDD unsuitable; revisit only after an edge justifies it), real execution.

### Honest expectation setting

Pure technical-analysis edge on liquid tokens is thin and well-mined. The **purpose of
the first module is architecture confirmation + an honest baseline + a reusable,
overfitting-resistant evaluation harness** — not to strike gold on day one. Higher-alpha
bets (on-chain flow, tokenomics, early-launch) inherit all this plumbing and are where the
search most likely pays off. Expect a *portfolio of thin, decaying edges*, not one magic
algorithm.

## 3. Data sources (verified 2026)

| Source                         | What it gives                                                        | Cost / limits | Used for |
|--------------------------------|----------------------------------------------------------------------|---------------|----------|
| **GeckoTerminal / CoinGecko onchain** | DEX pool OHLCV to 1s granularity back to 2021; 250+ networks, 1900+ DEXes | Free keyless (~30 req/min, ~6 months history); paid deeper | Phase 1 OHLCV backtesting |
| **Bitquery**                   | Full DEX trade history from first swap; per-wallet trade history/PnL; liquidity; multi-chain (ETH/Base/Arbitrum/OP/Polygon/BSC) | Free tier limited; paid | On-chain feature modules (swap-level, holder, wallet flow) |
| **Dune / The Graph**           | SQL / subgraph access to decoded events                              | Free tiers + paid | Ad-hoc research, holder snapshots |
| **DexScreener**                | Live snapshots only — **no history**                                 | Free | Live scanner only; **not** backtesting |
| Self-hosted RPC / archive node | Full raw data, low latency                                           | Heavy infra (needs NVMe) | Deferred |

Data-source adapters sit behind a single `DataSource` trait so providers are swappable.

## 4. Architecture — Cargo workspace

```
core        Instrument, Bar, Swap, LiquidityEvent, Holder; fixed-point Decimal; time utils
storage     canonical store — DuckDB/Parquet (research) + Postgres/Timescale (scale);
            point-in-time query API (no lookahead by construction)
providers   adapters behind DataSource trait: geckoterminal, bitquery, rpc (later)
features    FeatureModule trait → (entity × time × feature) matrix.
            Edge hypotheses live here: technical, tokenomics, onchain_activity, wallet_flow, …
strategy    Strategy trait: features → target weights / entry-exit signals
backtest    engine + cost/slippage/gas models + walk-forward + purged/embargoed CV
eval        Sharpe/Sortino/maxDD/turnover/hit-rate + deflated Sharpe / PBO (anti-overfit)
live        (later) same Strategy trait wired to live feed + execution adapter; paper-trade first
app/cli     orchestration, config, observability (tracing + Prometheus/Grafana)
```

### Key trait seams (the modularity contract)

- **`DataSource`** — fetch raw provider data, normalize into canonical typed records,
  persist. Providers are adapters; the rest of the system never sees provider-specific
  shapes.
- **`FeatureModule`** — given canonical data for a universe over a time range, produce a
  standard feature matrix `(entity × time × feature)`. Independent, unit-testable, combinable.
- **`Strategy`** — consume features → produce target positions/weights (or entry/exit
  signals). The *same* implementation runs in `backtest` and `live`.
- **`Backtester`** — point-in-time correct; models transaction costs, slippage vs.
  liquidity, gas; emits a trade ledger + equity curve.
- **`Evaluator`** — metrics + overfitting diagnostics; produces reports.

### Research layer

Python/Polars/DuckDB notebooks read the **same** Parquet/DuckDB artifacts the Rust
pipeline writes. One shared data contract → fast iteration with no language friction. Rust
is the system of record; Python is scratch space.

### Reuse from legacy `coinwatch/`

Conceptual, not code: domain-modeling rigor, TimescaleDB/partitioning patterns,
collector→repository→service separation, and the Prometheus/Grafana observability
scaffolding. The Bybit/CEX clients and the HTTP microservice mesh are retired.

## 5. Phased plan

1. **Phase 0 — Data spike** ✅ *(done)*: pulled GeckoTerminal OHLCV for 15 liquid Base/Arbitrum
   pools into DuckDB/Parquet. **Result: data quality is pristine** (0 gaps / 0 dupes / 0 OHLC
   violations across 2,274 daily bars); **binding constraint is history depth ≈ 6 months on the
   keyless tier** — enough to build/validate the harness, too shallow for robust CV of an edge.
   Full findings: `research/spikes/gecko_ohlcv/README.md`.
2. **Phase 1 — Harness MVP**: `core` + `storage` + `providers/geckoterminal` +
   `features/technical` + one `strategy` + `backtest` (real cost model) + `eval`.
   End-to-end walk-forward backtest with honest metrics on a liquid EVM token universe.
3. **Phase 2 — Confirm modularity**: add a second `FeatureModule` (simple tokenomics or an
   on-chain liquidity feature via Bitquery) into the **unchanged** engine. Clean slot-in =
   architecture confirmed.
4. **Phase 3 — Research loop + paper trading**: hypothesis iteration in Polars/DuckDB; wire
   the winning strategy to a live paper-trade runner; then decide on real execution.

## 6. Risks & open questions

- **On-chain backtest realism** is the top risk: slippage vs. block-level liquidity, failed
  txns, MEV/sandwiching, priority fees. Must be modeled explicitly, not assumed away.
- **Overfitting**: many tokens × many features × many params invites false edges. Mandate
  walk-forward + purged/embargoed CV + deflated Sharpe / PBO from day one.
- **Survivorship bias**: delisted/rugged tokens must remain in the historical universe.
- **Free-tier data depth** (~6 months keyless, *confirmed* in Phase 0) is too shallow for robust
  CV → mitigate by continuous forward ingestion + a paid CoinGecko tier once an edge warrants it.
- **Universe definition & point-in-time membership**: needs care to avoid lookahead in
  "which tokens were liquid at time *t*".

## 7. Changelog

- *Phase 0*: Design captured; scope decisions locked; data spike run — GeckoTerminal free OHLCV
  validated (pristine quality; ~6-month keyless history is the binding constraint).
