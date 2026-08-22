# /// script
# requires-python = ">=3.11"
# dependencies = ["requests>=2.31", "python-dotenv>=1.0"]
# ///
"""
Bitquery v2 auth + schema spike.

Verifies the OAuth access token works and reveals the real DEXTrades / aggregated
response shapes on Base, so we can design the Rust adapter + on-chain feature module
against verified fields (not guesses).

Setup:  put BITQUERY_ACCESS_TOKEN=ory_... in .env (repo root), then:
Run:    uv run research/spikes/bitquery/spike.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ENDPOINT = "https://streaming.bitquery.io/graphql"
# WETH on Base — a liquid, always-active token to guarantee non-empty results.
WETH_BASE = "0x4200000000000000000000000000000000000006"

# 1) Minimal auth + shape check: a few recent Base DEX trades.
Q_TRADES = """
{
  EVM(network: base) {
    DEXTrades(limit: {count: 3}, orderBy: {descending: Block_Time}) {
      Block { Time }
      Trade {
        Dex { ProtocolName ProtocolFamily }
        Buy  { Amount Currency { Symbol SmartContract } }
        Sell { Amount Currency { Symbol SmartContract } }
      }
    }
  }
}
"""

# 2) Aggregation probe on the DEFAULT (realtime) dataset the free plan allows:
#    can we get per-day trade count / USD volume / unique traders for one token?
Q_DAILY = """
query ($token: String!, $since: DateTime!) {
  EVM(network: base) {
    DEXTradeByTokens(
      where: {
        Trade: {Currency: {SmartContract: {is: $token}}}
        Block: {Time: {since: $since}}
      }
      orderBy: {descendingByField: "day_Date"}
      limit: {count: 10}
    ) {
      day: Block { Date }
      trades: count
      volumeUsd: sum(of: Trade_AmountInUSD)
      buyers: uniq(of: Trade_Buyer)
    }
  }
}
"""

# 3) Lookback horizon probe: how far back does the realtime dataset reach? Count
#    WETH trades since N days ago; the earliest non-error N tells us the window.
Q_DEPTH = """
query ($token: String!, $since: DateTime!) {
  EVM(network: base) {
    DEXTradeByTokens(
      where: {
        Trade: {Currency: {SmartContract: {is: $token}}}
        Block: {Time: {since: $since}}
      }
    ) {
      trades: count
      earliest: Block { Time(minimum: Block_Time) }
    }
  }
}
"""


def run(token: str, query: str, variables: dict | None = None) -> dict:
    resp = requests.post(
        ENDPOINT,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        data=json.dumps({"query": query, "variables": variables or {}}),
        timeout=60,
    )
    print(f"  HTTP {resp.status_code}")
    try:
        return resp.json()
    except ValueError:
        return {"_raw": resp.text[:1000]}


def show(label: str, payload: dict) -> None:
    print(f"\n=== {label} ===")
    if payload.get("errors"):
        print("GraphQL errors:")
        print(json.dumps(payload["errors"], indent=2)[:1500])
    if payload.get("data"):
        print(json.dumps(payload["data"], indent=2)[:2500])
    if "_raw" in payload:
        print("non-JSON body:", payload["_raw"])


def main() -> None:
    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    token = os.environ.get("BITQUERY_ACCESS_TOKEN", "").strip()
    if not token:
        print("BITQUERY_ACCESS_TOKEN not set. Add it to .env (see .env.example).")
        sys.exit(1)
    print(f"token: {token[:8]}… (len {len(token)})  endpoint: {ENDPOINT}")

    print("\n[1/2] recent Base DEX trades (auth + shape)")
    show("DEXTrades", run(token, Q_TRADES))

    print("\n[2/3] per-day aggregation for WETH, last 3 days (realtime dataset)")
    show("DEXTradeByTokens daily", run(token, Q_DAILY, {"token": WETH_BASE, "since": "2026-08-19T00:00:00Z"}))

    print("\n[3/3] realtime lookback horizon (count + earliest since 30d ago)")
    show("lookback depth", run(token, Q_DEPTH, {"token": WETH_BASE, "since": "2026-07-23T00:00:00Z"}))


if __name__ == "__main__":
    main()
