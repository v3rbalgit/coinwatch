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
| Deployment target   | Small always-on VPS (e.g. DigitalOcean), monitored via a TUI | Cheap, controllable; no self-hosted node — managed RPC/data providers instead. |

**Direction (locked):** find edge → verify (realistic backtest + paper-trading sim) →
execute on-chain, built step by step. The eventual UI is a **TUI** (ratatui) introduced
*with* the paper-trading loop — not before; no GUI, and no static reports as an end goal.

**Explicitly deferred:** new-launch sniping (adversarial latency race); real execution
(only after paper trading validates an edge). **Self-hosted node dropped** — use managed
RPC (Alchemy/QuickNode/Helius) for the live path and analytical APIs (Bitquery/Dune) for
research.

### Honest expectation setting

Pure technical-analysis edge on liquid tokens is thin and well-mined. The **purpose of
the first module is architecture confirmation + an honest baseline + a reusable,
overfitting-resistant evaluation harness** — not to strike gold on day one. Higher-alpha
bets (on-chain flow, tokenomics, early-launch) inherit all this plumbing and are where the
search most likely pays off. Expect a *portfolio of thin, decaying edges*, not one magic
algorithm.

## 3. Data sources (verified 2026)

| Source | What it gives | Cost / limits | Used for |
| -------------------------------- | ---------------------------------------------------------------------- | --------------- | ---------- |
| **GeckoTerminal / CoinGecko onchain** | DEX pool OHLCV to 1s granularity back to 2021; 250+ networks, 1900+ DEXes | Free keyless (~30 req/min, ~6 months history); paid deeper | Phase 1 OHLCV backtesting |
| **Bitquery** ⭐ (chosen next) | Full DEX trade history from first swap; per-wallet trade history/PnL; holders; liquidity; multi-chain (ETH/Base/Arbitrum/OP/Polygon/BSC); GraphQL + Kafka/WS | Free key → $39/$79/mo | **On-chain feature modules (next)** — swap-level, holders, wallet flow |
| **Dune (SQL) / The Graph** | SQL / subgraph access to decoded events (Dune *Sim* API retired Aug 2026) | Free tiers + paid | Exploratory hypothesis validation |
| **DexScreener** | Live snapshots only — **no history** | Free | Live scanner only; **not** backtesting |
| **Managed RPC (Alchemy / QuickNode / Helius)** | Real-time state, event logs, mempool, contract calls | Free tiers + paid | **Live loop + execution (later)** |

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

### The Python original (archived)

The original Python CEX system is archived on the `v1-python` branch (tag `v1.0-python`);
`main` is Rust-only. It is a *reference*, not a dependency: its social-sentiment collectors
(Reddit/Twitter/Telegram) and CoinGecko fundamentals client are a spec to mine if we later
add a social/fundamental feature module. The Bybit/CEX clients and HTTP microservice mesh
are retired.

## 5. Phased plan

1. **Phase 0 — Data spike** ✅ *(done)*: pulled GeckoTerminal OHLCV for 15 liquid Base/Arbitrum
   pools into DuckDB/Parquet. **Result: data quality is pristine** (0 gaps / 0 dupes / 0 OHLC
   violations across 2,274 daily bars); **binding constraint is history depth ≈ 6 months on the
   keyless tier** — enough to build/validate the harness, too shallow for robust CV of an edge.
   Full findings: `research/spikes/gecko_ohlcv/README.md`.
2. **Phase 1 — Harness MVP** ✅ *(done)*: `core` + `storage` (DuckDB) +
   `providers/geckoterminal` (+ `UniverseSource`) + `features/technical` (mom/vol/rsi) +
   `strategy` (`CrossSectional`) + `backtest` (cost-aware, point-in-time) + `eval`
   (Sharpe/Sortino/maxDD/turnover/hit-rate + PSR). The `coinwatch` binary runs the full
   pipeline end-to-end on a live Base universe. **The demo backtest numbers are NOT an
   edge** — tiny universe, ~4 months, in-sample, bull-market window, 78% drawdown; their
   only purpose is to confirm the architecture (same `Strategy` will run live). Walk-forward
   train/test CV is deferred until a strategy carries fittable parameters; the single-pass
   run is the whole out-of-sample for a non-parametric rule.
3. **Phase 2 — Confirm modularity** ✅ *(done)*: added a second `FeatureModule`
   (`LiquidityFeatures` — dollar-volume momentum, Amihud illiquidity, volume z-score, derived
   from the `volume` already on each bar) plus a `combine()` frame-merge helper. **The engine
   crates (`core`/`storage`/`strategy`/`backtest`/`eval`) were not touched** — the same
   `CrossSectional` strategy now ranks by either module's feature via config only, and the two
   signals produce genuinely different backtests. Architecture confirmed: new edge hypotheses
   slot in at the feature layer alone.
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
- *Phase 1*: Harness MVP complete — 8 `cw-*` crates wired as a DAG; live GeckoTerminal ingestion
  - DuckDB store + technical features + cross-sectional strategy + cost-aware backtest + metrics;
  store-first ingestion makes reruns offline/instant. Demo results are architecture confirmation,
  not alpha.
- *Phase 2*: Modularity confirmed — second `FeatureModule` (`LiquidityFeatures`) + `combine()`
  added at the feature layer only; engine crates untouched; same strategy ranks by either
  module's feature via config. New edge hypotheses now slot in without engine changes.
- *Post-Phase 2*: Split the binary into `ingest` (networked, incremental, registers instruments;
  Base + Arbitrum) and `analyze` (offline; reconstructs the universe from the store). DuckStore
  gains an instrument registry (`register_instrument`/`list_instruments`). The wider cross-section
  already cut the demo momentum strategy's max drawdown ~79% → ~47% — concrete evidence that
  breadth, not cleverness, is the first lever against the in-sample problem.
- *Walk-forward CV*: new `cw-harness` crate (composes `backtest` + `eval`, which can't depend on
  each other) provides `walk_forward` — rolls train/test folds, sweeps candidate configs per train
  window, evaluates the in-train winner out-of-sample, and stitches a continuous OOS curve. `eval`
  gains the **Deflated Sharpe Ratio** (multiple-testing-aware, using the trial-Sharpe distribution)
  plus probit/returns/per-period-Sharpe helpers. The demo makes the point vividly: in-sample
  Sharpe 3.84 / PSR 98.6% collapses to a walk-forward **DSR of ~16%** once 16 trials are deflated —
  the overfitting guard is now a workflow, not a footnote.
- *Rust-only pivot*: `main` restructured to Rust-only; Python original archived on `v1-python`
  (tag `v1.0-python`), README rewritten, `.gitignore` slimmed. Decisions locked: self-hosted node
  dropped (managed RPC for live, analytical APIs for research); **Bitquery** chosen as the next
  on-chain data provider (free tier first) to build a swap-level/holder/wallet-flow feature module;
  eventual UI is a **TUI** introduced with paper trading. Direction: find → verify → execute.
- *On-chain ingestion*: verified Bitquery v2 (spike) — **free tier is realtime-only, ~4-day
  lookback**; historical needs a paid per-chain archive add-on (same query). Added `OnchainDaily`
  type, `onchain_daily` store table + registry `token` column, an `OnchainSource` seam +
  `Bitquery` adapter (Bearer auth, server-side `DEXTradeByTokens` aggregation), and wired `ingest`
  to forward-accumulate daily on-chain flow (trades / USD volume / unique buyers). First run
  stored 48 rows across 10 tokens. Feature module consuming it is next.
- *On-chain feature module*: widened `FeatureContext` with an `onchain` channel (feature layer
  only — engine untouched) and added `OnchainFeatures` — `oc_flow` (real DEX USD volume),
  `oc_buyers` (unique buyers), `oc_tpb` (trades/buyer). `oc_tpb` is genuinely new signal OHLCV
  can't give: e.g. ETH/USDC on Arbitrum ~275 trades/buyer (bot-churned) vs VELVET ~6.5 (organic).
  `analyze` combines all three modules and surfaces on-chain columns in the leaderboard; on-chain
  backtests await accumulated history (currently ~5 days).
- *Depth-aware slippage*: the backtester's slippage is now linear price impact scaled by trade
  notional vs. the pool's daily volume (`coeff × notional × min(notional/volume, 0.5)`), replacing the
  flat turnover haircut — small trades in deep pools cost ~nothing, large trades in thin pools cost
  more. Unit-tested (a thinner pool provably ends poorer). Survivorship bias remains the outstanding
  realism gap: the store only ever holds currently-liquid pools, so dead/rugged tokens are absent —
  a true fix needs historical universe membership (paid backfill or forward-accumulated snapshots).
- *IC screen (experiment layer)*: `cw-harness::feature_ic` computes each feature's time-averaged
  cross-sectional **rank Information Coefficient** vs. next-bar returns, with IC-IR and t-stat — the
  first-pass "which features actually predict?" screen, shown in `analyze` before any backtest. On
  the demo it's honest and useful: `dvol_z` is the strongest technical signal (IC 0.066, t≈1.6),
  `mom` (our default) is weak (IC 0.044, t≈0.8, matching its low DSR), `rsi`/`illiq`/`vol` are ~zero,
  and `oc_tpb` looks promising (IC 0.12) but has too few periods yet. Nothing clears t>2 — the tool
  measures the absence of edge instead of hiding it.
- *Order-flow imbalance*: extended the on-chain path with buy/sell USD split (verified Bitquery
  conditional aggregation) — `OnchainDaily` gains buy/sell volumes, storage migrates in two columns,
  and `OnchainFeatures` adds **`oc_netflow`** = (buy − sell)/(buy + sell) ∈ [−1, 1]. On the demo
  VELVET shows +0.89 (strongly net-bought, also top momentum), USDC/Base −0.16 (net sold). Still
  ~4 days of history so its IC isn't yet meaningful; densifies via forward-accumulation.
- *Strategy sweep*: `cw-harness::sweep` evaluates many configs each independently out-of-sample
  (its own walk-forward) and deflates every result by the full trial-Sharpe set → a ranked strategy
  leaderboard by DSR, shown in `analyze`. It surfaces two honest lessons on the demo: (1) `illiq@50%`
  tops the board (DSR 80%) despite ~zero IC — a top-quantile strategy can win by luck when the
  feature has no linear signal (overfitting red flag); (2) every config shows DSR 65–80% because
  *everything* works in a 4-month bull window — **DSR deflates for multiple-testing selection, not
  for single-regime/sample bias.** Reinforces that more data across regimes is the binding need.
- *Universe widened (Base + Arbitrum + Ethereum, ~44 pools, $300k floor)* + Bitquery retry/backoff.
  **The headline finding of the whole build:** on the wider 34-instrument cross-section the momentum
  "edge" *evaporates* — the strategy that showed +273% on 9 major-heavy pools now shows −25%
  (Sharpe −0.74), and **every** sweep config is negative-Sharpe (DSR ≤ 7%). The earlier numbers were
  a survivorship/selection artifact of a tiny majors-only universe. The IC screen now flags a
  plausible *real* lead — `vol` at **t=−2.5 over 89 periods** (the low-volatility anomaly). The
  platform killed a fake edge and pointed at a real one: exactly its job. Breadth was the single
  highest-leverage change.
- *PIVOT to Hyperliquid perps*: verified (spike) the free `info` API gives 232 perps, ~3y of clean
  daily candles, and hourly funding — solving the breadth/depth/regime constraints at zero cost, and
  enabling shorting (fixes the spot-only bull-market limitation). Added `providers/hyperliquid`
  (paced + retried; universe via `metaAndAssetCtxs`, candles via `candleSnapshot` as canonical
  `Bar`s) and an `hl-ingest` command (resumable, store-first). Landed **40 perps × 37,148 daily bars,
  2023-05 → today (3.3y), 24 coins with ≥3y each, 0 OHLC violations.** Next: funding store + perps-
  aware backtester (shorts + funding P&L) + long/short research on this universe.
- *HL funding (chunk 1b)*: added `FundingPoint` type, a `funding` store table + `upsert/load_funding`,
  and `Hyperliquid::fetch_funding` (paginated, deduped) wired into `hl-ingest` (resumable, 120-day
  window). Landed 75k+ hourly funding rows across 27/40 perps (rest fills on further runs). Bumped HL
  pacing to ~1.1s to stay under the info-API weight limit (500ms triggered 429 storms). Deep (3y)
  funding backfill deferred; 120d suffices to build the carry signal + verify the pipeline.
- *Consolidation on Hyperliquid*: the spot exploration built/validated the reusable engine, then we
  pivoted — so the spot-specific code was retired. **Archived on branch `v2-spot`**; removed from
  `main`: GeckoTerminal/Bitquery adapters, `OnchainFeatures`/`oc_*`, `onchain_daily` table, the
  `FeatureContext.onchain` channel, `UniverseSource`/`OnchainSource` traits, and dotenvy. Renamed
  `hl-ingest`→`ingest`; store cleaned to HL-only. Kept the asset-agnostic spine (features/strategy/
  backtest/eval/harness/storage) + `providers/hyperliquid`. On the now-clean **3+ year / 1,170-
  rebalance** HL cross-section two signals are **robustly significant**: `vol` IC **t=−5.45**
  (low-volatility anomaly) and `illiq` **t=+4.70** (illiquidity premium); `mom` is ~zero. Long-only
  strategies still blow up (they buy the high-vol pumped coins) — harvesting these needs **long/short**,
  which perps enable and `CrossSectional.allow_short` supports. This is exactly what the perps
  backtester (next) unlocks.
- *Perps backtester (chunk 2)*: the engine now models **funding P&L** (longs pay positive funding,
  shorts receive; daily-summed from the funding table) and **ruin** (equity floored at 0); shorts
  were already correct via signed weights. `CrossSectional` gains an **`invert`** flag (long the low
  end — e.g. long low-vol). `run`/`walk_forward`/`sweep` thread funding; the sweep now tests
  market-neutral long/short. **Honest finding:** even the L/S plays on the *significant* signals blow
  up (−99 to −100%) — including long-low-vol/short-high-vol. The IC (vol t=−5.4) is real but **small
  (~5%)**, and a naive equal-weight top-quantile L/S is swamped by crypto's fat tails
  (crashes/short-squeezes) and bled by high turnover with no risk management. The platform correctly
  refuses to bless a naive blowup: **a significant signal ≠ a profitable naive strategy.** Next
  research lever is *portfolio construction* — risk/vol-weighting, more diversification, lower
  turnover — not another signal.
- *Risk-aware sizing + a critical bug fix (chunk 3)*: added inverse-volatility position weighting
  (`CrossSectional.weight_feature`) and **portfolio vol-targeting** (`Backtester.with_vol_target`:
  scale gross exposure by target/trailing-vol, capped). **Neither rescued the L/S plays — which
  exposed the real problem:** HL candle volume `v` is in **base-coin units**, but the depth-aware
  slippage compared it to a USD notional → nonsensical slippage (≈25%/trade on BTC, ≈0 on DOGE),
  which had been corrupting *every* backtest. Fixed the adapter to store USD volume (`v × close`);
  dvol-momentum long-only went **−85% → +270%** on the fix alone. (The IC screen never used the
  backtester, so those signal readings stayed valid throughout.) **Post-fix finding:** market-neutral
  L/S on the low-vol/illiquidity signals *still* struggles — dominated by **crypto short-side tail
  risk** (shorting high-vol coins → short squeezes) while long-only is dominated by market beta. The
  signals are real; naive top/bottom-quantile L/S — especially the short leg — is the wrong vehicle.
  Next: long-only low-vol (no short) and/or hard short-risk controls.
- *Long-only + market-beta decomposition (chunk 4)*: added `eval::alpha_beta` (single-factor OLS of a
  strategy's returns on a benchmark → alpha, beta, R²) and a long-only section in `analyze` that
  decomposes each strategy vs an **equal-weight-universe benchmark** (+153%, Sharpe 0.77 — most
  long-only return is just **beta**), showing turnover too. **Two findings.** (1) **illiquidity is the
  first genuine edge candidate:** long-only `illiq@30%` returns **+195%** (beats the market), turnover
  a low **0.08**, and carries **+8.3%/yr alpha net of realistic costs** — it survives because illiquidity
  rankings are persistent. (2) **long-only low-vol ruins (−100%) purely from TURNOVER:** the low-vol
  majors have near-identical 14-day vol, so the ranking reshuffles daily → turnover **0.66** → cost
  bleed to ruin. Proven by a zero-cost run where the ruin vanishes (low-vol +165%, +3.6%/yr alpha).
  The signal is fine; the daily rebalance isn't. Next lever: **turnover control** (rebalance cadence /
  no-trade band / weight smoothing) — which would also lift every cost-sensitive strategy.
- *Turnover control (chunk 5)*: added a **rebalance cadence** to the backtester
  (`Backtester.with_rebalance_every(N)`): rebalance every N bars, holding weights between and paying
  no costs (unit-tested). Sweeping cadence d1/d7/d14 **cleanly separates two conclusions.** (1)
  **Low-vol long-only is NOT a real edge:** cadence cuts its turnover 0.66→0.05, but it stays negative
  net of costs (d14: −70%, alpha −58%/yr) — the signal doesn't survive as a long-only book. (2)
  **Illiquidity is the genuine edge, and it *improves* with less trading:** `illiq@30%` biweekly hits
  **+270%, Sharpe 0.92, +15.4%/yr alpha** net of realistic costs (turnover 0.03, R² 0.87). Less churn →
  more alpha. **Caveat: in-sample, full-period.** The honest next step is walk-forward (OOS) + DSR
  validation of the illiquidity alpha — does the +15%/yr survive out-of-sample and multiple-testing
  deflation?
- *OOS validation (chunk 6)*: threaded `rebalance_every` through `walk_forward`/`sweep` and reworked
  `analyze` into an out-of-sample validation — a long-only family (illiq/low-vol/mom/dvol_mom) run
  through walk-forward at biweekly cadence + DSR, plus an **OOS alpha** regression against an
  equal-weight-market walk-forward on the same folds. **The illiquidity premium survives OOS:**
  `illiq@30%` d14 shows OOS Sharpe **0.95**, OOS **+232%** (≈ its in-sample +270% → not overfit), and
  **OOS alpha +36.6%/yr** beyond market beta (≈1.04, R² 0.90) — top of the deflated board; `dvol_mom@30%`
  is a second liquidity-family lead (OOS Sharpe 0.93). **Low-vol is confirmed dead OOS** (negative
  alpha, DSR 0%). **Honest caveat:** the top DSR is only **17.6%** — after deflating for the 8 configs
  tried, this is a *strong lead* consistent with the documented academic illiquidity premium, **not a
  statistically overwhelming proven edge**; walk-forward re-enters positions per fold so the absolute
  alpha magnitude carries uncertainty, though the relative OOS outperformance is robust. Verdict: a
  genuine, literature-backed edge candidate worth **paper trading + further validation**, not yet a
  proven money-printer.
- *Funding signals + bidirectional/carry (chunk 7)*: added a `FundingFeatures` module (`fund_rate`,
  `fund_z`) on a new funding channel of `FeatureContext` (z-score uses an epsilon guard so a constant
  series gives z=0, not the fp-noise 1/√n); made `CrossSectional` hold cash (not error) when the rank
  feature is absent so sparse signals backtest cleanly; deep-funding backfill (~3y, resumable by
  depth); and added market-neutral illiquidity L/S + funding-carry candidates to the OOS validation.
  **Three findings (preliminary — funding only partially backfilled, 12/27 coins deep):** (1) **funding
  *anomaly* carry is the best edge yet** — `fundz carry` (market-neutral: short unusually-high funding
  vs the coin's own history, long unusually-low) posts **OOS Sharpe 1.37, DSR 56.8%, maxDD 24%**,
  ~2× the DSR of anything prior, and uses *both* shorting and funding. (2) **The illiquidity "edge"
  was mostly BETA** — market-neutral `illiq L/S` returns only +35.7% (Sharpe 0.66) vs +227% long-only,
  and even that residual is likely small-cap beta; the chunk-6 "+36%/yr alpha" was over-optimistic
  (an honest correction — the volume-units fix also flipped `illiq`'s IC negative: illiquid small-caps
  *underperform* risk-adjusted). (3) **Funding LEVEL fails, funding ANOMALY works** — raw-rate carry
  is −56%; the edge is the z-score. Next: complete the funding backfill and re-validate `fundz carry`
  over the full 3y before trusting it.
- *Walk-forward OOS re-entry fix (chunk 8)*: with 22/27 coins deep, re-validation showed the OOS
  market benchmark at an impossible **+3.6%** vs **+86%** for the *same* strategy run continuously
  over the *same* days. Root cause (traced step-by-step): each walk-forward fold's backtest
  **re-entered from cash**, paying a full entry cost *every* fold (35×) that a continuously-held OOS
  never pays — catastrophic in HL's **thin early-2023 market** (measured fold entry ≈ **6%**). Fix:
  `Backtester::run_stateful` seeds each fold from the prior fold's ending positions and returns its
  own ending positions, so walk-forward stitches a **continuously-held** OOS path (only the delta is
  traded when the chosen strategy changes). Market OOS → **+115%** (residual vs +86% is the skipped
  inter-fold boundary day, consistent across strategies). **The artifact had been understating the
  market-neutral strategies most** (they eat re-entry hardest). Corrected verdict: **illiquidity L/S
  (market-neutral) is a legitimate edge — OOS Sharpe 0.91, DSR 35.7%, maxDD 26%** (was 0.63/18%);
  `fundz carry` holds at DSR 24%; long-only illiq alpha is a more sober **+25.7%/yr** (the broken
  benchmark had inflated it). Pre-existing `dvol_z` epsilon bug (liquidity.rs) still flagged, unfixed.

## 8. Direction & signal roadmap

**Direction A (active): Hyperliquid perps.** Find durable long/short + funding-carry edges on 3y of
clean data, validate (walk-forward + DSR + realistic costs), then paper-trade and execute on HL.

**Signal sourcing (how a quant sequences it):**

- *Tier 1 (now — free, deep, directly about the traded instrument):* price signals (cross-sectional
  momentum, mean-reversion, **low-volatility**, illiquidity) + perp-native signals (**funding carry**,
  open interest / positioning, liquidation cascades). Start here; it's plenty to hunt a first edge.
- *Tier 2 (later — added one at a time, each IC-screened before trust):* orthogonal/alt-data — the
  *right* on-chain signals for large caps (exchange netflows, stablecoin flows, L1 activity/fees),
  CEX order-flow, social sentiment. **Signal source ≠ execution venue:** an alt-data signal can drive
  an HL trade. Don't hoard data — add a source only with a hypothesis, then IC-test it.

**Direction B (parked): new-token discovery.** Gaming newly-listed tokens is a *different* tool with
*different* signals (new-pool/launch detection, holder concentration, rug/LP-lock filters, first-hour
wallet intelligence — mostly new work, not what the retired spot code did). The idea and the old spot
adapters live on `v2-spot`; revive on the shared engine spine **if/when Direction A is working** and
there's bandwidth. One direction at a time.
