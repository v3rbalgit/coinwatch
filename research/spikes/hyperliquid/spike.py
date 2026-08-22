# /// script
# requires-python = ">=3.11"
# dependencies = ["requests>=2.31"]
# ///
"""
Hyperliquid data spike: verify the free public API gives us what we need to
pivot — the perp universe, deep candle history, and funding-rate history.

No auth. Answers: how many perps? how deep is daily candle history? is funding
history available and how far back? what's the data shape (for the Rust provider)?

Run:  uv run research/spikes/hyperliquid/spike.py
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests

ENDPOINT = "https://api.hyperliquid.xyz/info"
DAY_MS = 86_400_000


def post(body: dict) -> Any:
    r = requests.post(ENDPOINT, json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def ymd(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def universe_snapshot() -> int:
    mc = post({"type": "metaAndAssetCtxs"})
    meta, ctxs = mc[0], mc[1]
    universe = meta["universe"]
    rows = []
    for u, c in zip(universe, ctxs):
        rows.append({
            "name": u["name"],
            "maxLev": u.get("maxLeverage"),
            "mark": float(c.get("markPx") or 0),
            "funding_hr": float(c.get("funding") or 0),
            "oi": float(c.get("openInterest") or 0),
            "day_vol_usd": float(c.get("dayNtlVlm") or 0),
        })
    rows.sort(key=lambda r: r["day_vol_usd"], reverse=True)
    print(f"UNIVERSE: {len(universe)} perps\n")
    print(f"  {'coin':<10}{'maxLev':>7}{'mark$':>14}{'fund/hr%':>10}{'fund/yr%':>10}{'OI(coins)':>14}{'dayVol$':>16}")
    for r in rows[:15]:
        print(f"  {r['name']:<10}{r['maxLev']:>7}{r['mark']:>14,.4f}"
              f"{r['funding_hr']*100:>10.4f}{r['funding_hr']*24*365*100:>10.1f}"
              f"{r['oi']:>14,.0f}{r['day_vol_usd']:>16,.0f}")
    return len(universe)


def candle_depth(coin: str, years: float = 3.0) -> None:
    now = int(time.time() * 1000)
    start = now - int(years * 365 * DAY_MS)
    candles = post({"type": "candleSnapshot",
                    "req": {"coin": coin, "interval": "1d", "startTime": start, "endTime": now}})
    if not candles:
        print(f"  {coin:<6} no candles")
        return
    ts = sorted(c["t"] for c in candles)
    span = (ts[-1] - ts[0]) // DAY_MS + 1
    gaps = max(span - len(candles), 0)
    c0 = candles[0]
    print(f"  {coin:<6} {len(candles):>5} daily candles  {ymd(ts[0])} -> {ymd(ts[-1])}"
          f"  ({span}d span, {gaps} gaps)  sample o/c={c0['o']}/{c0['c']} n_trades={c0.get('n')}")


def funding_depth(coin: str, days: int = 30) -> None:
    now = int(time.time() * 1000)
    fh = post({"type": "fundingHistory", "coin": coin, "startTime": now - days * DAY_MS, "endTime": now})
    if not fh:
        print(f"  {coin:<6} no funding history")
        return
    rates = [float(f["fundingRate"]) for f in fh]
    ts = [f["time"] for f in fh]
    mean_hr = sum(rates) / len(rates)
    print(f"  {coin:<6} {len(fh):>5} funding points  {ymd(min(ts))} -> {ymd(max(ts))}"
          f"  mean={mean_hr*100:.5f}%/hr ({mean_hr*24*365*100:+.1f}%/yr)  sample={fh[0]}")


def main() -> None:
    print("=" * 90)
    n = universe_snapshot()
    print("\n" + "=" * 90)
    print("CANDLE HISTORY DEPTH (daily, ask for 3 years)")
    for coin in ["BTC", "ETH", "SOL", "HYPE"]:
        candle_depth(coin)
    print("\n" + "=" * 90)
    print("FUNDING HISTORY (last 30 days, hourly)")
    for coin in ["BTC", "ETH", "SOL"]:
        funding_depth(coin)
    print("\n" + "=" * 90)
    print(f"VERDICT: {n} perps; daily candles + hourly funding both available from the free API.")


if __name__ == "__main__":
    main()
