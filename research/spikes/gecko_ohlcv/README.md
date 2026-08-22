# Spike: GeckoTerminal free OHLCV for EVM-L2 backtesting

**Phase 0 gate question:** is GeckoTerminal's free on-chain OHLCV good enough to backtest on?

**Verdict: YES for building/validating the harness; history depth is the binding constraint for robust cross-validation.**

## Run

```bash
uv run research/spikes/gecko_ohlcv/spike.py
```

Outputs (gitignored) land in `out/`: `ohlcv_daily.parquet`, `spike.duckdb`, `quality_report.csv`.

## Findings (15 liquid pools, Base + Arbitrum, daily bars)

| Metric | Result |
| --- | --- |
| Pools with data | 15/15 |
| Total rows | 2,274 |
| Median history/pool | **181 days (~6 months)** |
| Missing days | 0 |
| Duplicate timestamps | 0 |
| OHLC-relationship violations | 0 |
| Zero-volume / zero-price bars | 0 |
| Date range | 2026-02-23 → 2026-08-22 |

**Data quality is pristine** on liquid pools: clean OHLC, no gaps, no dupes, no violations.
Younger pools (e.g. `Basecat/ETH` 8 rows, `halo/VIRTUAL` 17 rows) simply have shorter history
because the pool is newer — expected, not a defect.

## Constraints discovered (feed into Phase 1)

1. **History depth ≈ 6 months on the keyless tier** (median 181 daily bars = the cap). Enough to
   build and end-to-end-validate the backtest harness, but **too shallow for robust walk-forward /
   purged CV** of an actual edge. Mitigations:
   - CoinGecko paid on-chain tier advertises OHLCV back to 2021 → warranted once an edge is worth CV depth.
   - Run ingestion continuously to accumulate our own forward history from day one.
2. **Keyless rate limiting is aggressive/bursty** — the first run hit `429` almost immediately.
   Pacing (~6 s/call) + backoff works for a spike; get a free CoinGecko demo key early for higher
   limits, and expect to pace the Rust ingester.

## Implication for the architecture

The `providers/geckoterminal` adapter is viable for Phase 1. Design the `DataSource` trait so a
provider swap (keyless → keyed → paid, or → Bitquery for deeper/tick data) is a config change, not
a code change. The `storage` layer should be incremental/idempotent so continuous ingestion steadily
deepens local history beyond the 6-month API window.
