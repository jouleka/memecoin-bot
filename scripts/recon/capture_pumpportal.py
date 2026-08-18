"""Capture PumpPortal data-WS frames to a JSONL fixture.

Subscribes to new-token + migration events; after seeing 5 new mints, also
subscribes to their trades so we capture the curve-trade schema.

NOTE: --out opens in append mode — re-running appends to the same file. Pass
a new --out (session2.jsonl, ...) or delete the old file first, or downstream
analysis will silently mix disjoint sessions.

Usage:
  ./.venv/bin/python scripts/recon/capture_pumpportal.py --minutes 10 \
      --out tests/fixtures/providers/pumpportal/session1.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import websockets

URI = "wss://pumpportal.fun/api/data"  # candidate — correct here + NOTES.md if docs disagree
TRADE_SUB_AFTER_MINTS = 5


async def capture(minutes: float, out: str) -> None:
    deadline = time.monotonic() + minutes * 60
    mints: list[str] = []
    trade_subscribed = False
    n = 0
    async with websockets.connect(URI) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        await ws.send(json.dumps({"method": "subscribeMigration"}))
        with open(out, "a", encoding="utf-8") as f:
            while time.monotonic() < deadline:
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=30)
                except TimeoutError:
                    print("WARN: no frame for 30s — note in NOTES.md")
                    continue
                f.write(json.dumps({"t_wall": time.time(), "raw": frame}) + "\n")
                n += 1
                if not trade_subscribed:
                    try:
                        payload = json.loads(frame)
                        mint = payload.get("mint")
                        if mint and mint not in mints:
                            mints.append(mint)
                    except (json.JSONDecodeError, AttributeError):
                        pass
                    if len(mints) >= TRADE_SUB_AFTER_MINTS:
                        await ws.send(
                            json.dumps({"method": "subscribeTokenTrade", "keys": mints})
                        )
                        trade_subscribed = True
                        print(f"subscribed to trades for {len(mints)} mints")
    print(f"captured {n} frames -> {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=10)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    asyncio.run(capture(a.minutes, a.out))
