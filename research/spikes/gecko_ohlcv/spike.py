# /// script
# requires-python = ">=3.11"
# dependencies = ["requests>=2.31", "duckdb>=1.1", "polars>=1.0"]
# ///
"""
Phase 0 data spike: validate GeckoTerminal free on-chain OHLCV for EVM-L2 backtesting.

Builds a small universe of liquid pools on Base + Arbitrum, pulls daily OHLCV for each,
lands it into Parquet + DuckDB, and prints a data-quality report (coverage, gaps,
OHLC sanity, zero-volume/zero-price days, duplicate timestamps).

Run:  uv run research/spikes/gecko_ohlcv/spike.py
Gate: is the free data good enough to backtest on? (see DESIGN.md Phase 0)

Resilient by design: keyless GeckoTerminal is aggressively rate-limited, so calls are
paced, 429s are backed off, and any pool that still fails is skipped rather than aborting
the whole run.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl
import requests

API = "https://api.geckoterminal.com/api/v2"
HEADERS = {"Accept": "application/json", "User-Agent": "coinwatch-spike/0.1"}
NETWORKS = ["base", "arbitrum"]
POOLS_PER_NETWORK = 12
MIN_RESERVE_USD = 250_000.0          # liquidity floor for a "backtestable" pool
DAILY_LIMIT = 1000                   # API cap per call; keyless history ~6 months
RATE_SLEEP_S = 6.0                   # keyless is strict; pace well under the limit
OUT = Path(__file__).parent / "out"

SEC_PER_DAY = 86_400


@dataclass
class Pool:
    network: str
    address: str
    name: str
    reserve_usd: float
    vol24h_usd: float


def _f(value: object, default: float = 0.0) -> float:
    """Best-effort float from messy API values."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _get(url: str, params: dict | None = None, tries: int = 5) -> dict | None:
    """GET JSON with 429/5xx backoff. Returns None after exhausting retries."""
    for attempt in range(tries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        except requests.RequestException:
            time.sleep(4 * (attempt + 1))
            continue
        if r.status_code == 429:
            time.sleep(15 * (attempt + 1))
            continue
        if r.ok:
            try:
                return r.json()
            except ValueError:
                return None
        if 400 <= r.status_code < 500:
            return None  # non-retryable client error
        time.sleep(4 * (attempt + 1))
    return None


def build_universe() -> list[Pool]:
    """Top pools per network, filtered by a liquidity floor, deduped by base token."""
    pools: list[Pool] = []
    for net in NETWORKS:
        seen_base: set[str] = set()
        added = 0
        resp = _get(f"{API}/networks/{net}/pools", {"page": 1})
        time.sleep(RATE_SLEEP_S)
        data = (resp or {}).get("data", []) or []
        for p in data:
            attr = p.get("attributes", {})
            rel = p.get("relationships", {})
            reserve = _f(attr.get("reserve_in_usd"))
            vol = _f((attr.get("volume_usd") or {}).get("h24"))
            base_id = (rel.get("base_token", {}).get("data") or {}).get("id") or attr.get("address", "")
            if reserve < MIN_RESERVE_USD or base_id in seen_base:
                continue
            seen_base.add(base_id)
            pools.append(Pool(net, attr["address"], attr.get("name", "?"), reserve, vol))
            added += 1
            if added >= POOLS_PER_NETWORK:
                break
        print(f"  {net:9s}: {added} pools (from {len(data)} listed)")
    return pools


def fetch_ohlcv(pool: Pool) -> pl.DataFrame:
    """Daily OHLCV for a pool. GeckoTerminal returns [ts, o, h, l, c, vol_usd]."""
    resp = _get(
        f"{API}/networks/{pool.network}/pools/{pool.address}/ohlcv/day",
        {"aggregate": 1, "limit": DAILY_LIMIT, "currency": "usd"},
    )
    rows = ((resp or {}).get("data", {}).get("attributes", {}) or {}).get("ohlcv_list", []) or []
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(
        rows, schema=["ts", "open", "high", "low", "close", "volume"], orient="row"
    ).with_columns(
        pl.lit(pool.network).alias("network"),
        pl.lit(pool.address).alias("pool"),
        pl.lit(pool.name).alias("name"),
    ).sort("ts")


def quality_report(df: pl.DataFrame) -> pl.DataFrame:
    """Per-pool data-quality metrics that decide the Phase-0 gate."""
    out = []
    for (net, _pool, name), g in df.group_by(["network", "pool", "name"], maintain_order=True):
        g = g.sort("ts")
        ts = g["ts"].to_list()
        span_days = (ts[-1] - ts[0]) // SEC_PER_DAY + 1 if len(ts) > 1 else len(ts)
        gaps = max(int(span_days) - g.height, 0)
        dupes = g.height - g["ts"].n_unique()
        # OHLC sanity: low <= min(open,close) and high >= max(open,close)
        bad_ohlc = g.filter(
            (pl.col("low") > pl.min_horizontal("open", "close"))
            | (pl.col("high") < pl.max_horizontal("open", "close"))
        ).height
        zero_vol = g.filter(pl.col("volume") <= 0).height
        zero_px = g.filter(pl.col("close") <= 0).height
        out.append(
            {
                "network": net, "name": str(name)[:22], "rows": g.height,
                "days_span": int(span_days), "missing_days": gaps,
                "dupes": dupes, "bad_ohlc": bad_ohlc,
                "zero_vol": zero_vol, "zero_px": zero_px,
                "first": time.strftime("%Y-%m-%d", time.gmtime(ts[0])),
                "last": time.strftime("%Y-%m-%d", time.gmtime(ts[-1])),
            }
        )
    return pl.DataFrame(out).sort(["network", "name"])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"Building universe ({', '.join(NETWORKS)}, reserve >= ${MIN_RESERVE_USD:,.0f}) ...")
    universe = build_universe()
    print(f"  -> {len(universe)} pools total\n")

    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    for i, pool in enumerate(universe, 1):
        df = fetch_ohlcv(pool)
        status = f"{df.height} rows" if df.height else "NO DATA"
        print(f"[{i:2d}/{len(universe)}] {pool.network:9s} {pool.name[:28]:28s} "
              f"reserve=${pool.reserve_usd:>12,.0f}  {status}")
        if df.height:
            frames.append(df)
        else:
            failed.append(f"{pool.network}:{pool.name}")
        time.sleep(RATE_SLEEP_S)

    if not frames:
        print("\nNO DATA RETURNED for any pool — investigate before proceeding.")
        return

    all_df = pl.concat(frames)
    pq = OUT / "ohlcv_daily.parquet"
    all_df.write_parquet(pq)

    # Land into DuckDB (the shared research artifact the Rust pipeline will also target).
    ddb = OUT / "spike.duckdb"
    con = duckdb.connect(str(ddb))
    con.execute("CREATE OR REPLACE TABLE ohlcv_daily AS SELECT * FROM read_parquet(?)", [str(pq)])
    con.close()

    report = quality_report(all_df)
    report.write_csv(OUT / "quality_report.csv")

    print("\n" + "=" * 96)
    print("DATA-QUALITY REPORT")
    print("=" * 96)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=30, tbl_width_chars=120):
        print(report)

    tot = all_df.height
    exp = tot + int(report["missing_days"].sum())
    print("\nSUMMARY")
    print(f"  pools with data : {all_df['pool'].n_unique()}/{len(universe)}"
          + (f"  (failed: {', '.join(failed)})" if failed else ""))
    print(f"  total rows      : {tot:,}")
    print(f"  median rows/pool: {int(report['rows'].median())}")
    print(f"  median span days: {int(report['days_span'].median())}")
    print(f"  total gaps      : {int(report['missing_days'].sum())} "
          f"({100 * report['missing_days'].sum() / max(exp, 1):.1f}% of expected)")
    print(f"  OHLC violations : {int(report['bad_ohlc'].sum())}")
    print(f"  dup timestamps  : {int(report['dupes'].sum())}")
    print(f"\nArtifacts:\n  {pq}\n  {ddb}\n  {OUT / 'quality_report.csv'}")


if __name__ == "__main__":
    main()
