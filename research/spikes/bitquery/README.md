# Spike: Bitquery v2 auth + on-chain schema

**Question:** does the Bitquery OAuth token work, and what's the exact response shape for
per-day on-chain flow — and what does the free tier actually give us?

**Verdict:** auth + aggregation work; **the free tier is `realtime`-only (~4-day lookback)**.
Historical depth for backtesting needs a paid per-chain **archive add-on** (~$105–150/mo Base),
but the query is identical, so the Rust adapter works for both.

## Run

```bash
# put BITQUERY_ACCESS_TOKEN=ory_... in .env (repo root), then:
uv run research/spikes/bitquery/spike.py
```

## Findings (verified)

- **Endpoint:** `https://streaming.bitquery.io/graphql`; auth `Authorization: Bearer <ory_…>`.
- **Trade shape:** `Trade { Buy/Sell { Amount, Currency { Symbol, SmartContract } }, Dex { ProtocolName, ProtocolFamily } }`.
- **Per-day aggregation works** on `DEXTradeByTokens` with server-side metrics — the exact query
  the Rust adapter uses:
  - `trades: count`, `volumeUsd: sum(of: Trade_AmountInUSD)`, `buyers: uniq(of: Trade_Buyer)`,
    grouped by `Block { Date }`, ordered `ascendingByField: "day_Date"`.
  - Aggregate values come back as JSON **strings** (e.g. `"475533"`).
- **Free tier = `realtime` dataset only.** Requesting `dataset: combined` (archive) 403s:
  *"your plan only allows realtime"*. Probing 30 days back returned data only to ~4 days ago.
- **Volume is huge** (WETH/Base ≈ 475k trades, $306M/day) — server-side aggregation is mandatory;
  never pull raw trades.

## Decision (see DESIGN.md)

Build the adapter + on-chain feature pipeline on the **free tier now** and **forward-accumulate**
daily aggregates ($0). When historical depth is worth it, buy the Base archive add-on
month-to-month, deep-backfill into DuckDB (we own the data), then cancel.
