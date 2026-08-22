# Spike: Hyperliquid free API (the pivot)

**Question:** does Hyperliquid's free public API give us a clean universe, deep candle
history, and funding history — enough to pivot the project to perps?

**Verdict: YES, decisively.** It solves the breadth+depth constraints that were blocking the
spot-on-chain approach, at zero data cost.

## Run

```bash
uv run research/spikes/hyperliquid/spike.py
```

## Findings (verified)

- **Endpoint:** `POST https://api.hyperliquid.xyz/info` (no auth). Bodies:
  - `{"type":"metaAndAssetCtxs"}` → universe + per-asset funding / open interest / mark /
    day volume in one call (ideal for universe ranking).
  - `{"type":"candleSnapshot","req":{"coin","interval","startTime","endTime"}}` → OHLCV
    (`t,T,o,h,l,c,v,n`). Daily 3-year pull fits one call; hourly (>5000 pts) needs pagination.
  - `{"type":"fundingHistory","coin","startTime","endTime"}` → hourly funding + premium;
    ~500 points per call, so paginate (~21 days/call) for deep history.
- **Universe:** **232 perps** — established coins + liquid alts/memes; each carries maxLeverage.
- **Candle depth:** **~3 years of clean daily candles, 0 gaps** (BTC/ETH/SOL: 1096 bars,
  2023-08-23 → today). Newer listings shorter (HYPE ~626d). Multiple regimes covered.
- **Funding:** hourly, e.g. BTC ≈ +6.6%/yr; big harvestable spreads on some coins
  (FARTCOIN +173%/yr, AAVE +83%/yr, PUMP +75%/yr) — the carry-edge signal.

## Why this is the pivot

- **Shorting/leverage** (perps) fixes the spot-only bull-market limitation.
- **~3y × 232 coins, free** — vs. GeckoTerminal 6mo / Bitquery-free 4 days. Breadth + depth +
  regimes, the exact constraints that killed our fake momentum edge.
- **Funding + OI** = new, often-durable edge sources (carry, positioning) unavailable for spot.
- **Clean consolidated price** (oracle-anchored) — better than a single DEX pool.
- Our research spine (features/strategy/eval/harness) transfers; we add a Hyperliquid provider
  and extend the backtester for perps (shorts + funding P&L; leverage later).

## Implications for the Rust build

- New `providers/hyperliquid`: universe (`metaAndAssetCtxs`), candles (`candleSnapshot`),
  funding (`fundingHistory`, paginated). No auth for market data.
- `core`: a funding/OI data type; store gains a funding table.
- `backtest`: model shorts + per-period funding accrual (start leverage=1, no liquidation).
- New funding/OI feature modules; long/short strategies (CrossSectional already supports it).
- Execution (later) routes through `api.hyperliquid.xyz/exchange` with wallet signing — not RPC.
