"""Helius recon: the exact RPC calls the safety gate needs + program-logs WS capture.

Usage:
  ./.venv/bin/python scripts/recon/capture_helius.py rpc
  ./.venv/bin/python scripts/recon/capture_helius.py logs --minutes 5
Requires env MEMEBOT_HELIUS_API_KEY.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import time

import httpx
import websockets

KEY = os.environ["MEMEBOT_HELIUS_API_KEY"]
RPC = f"https://mainnet.helius-rpc.com/?api-key={KEY}"
WSS = f"wss://mainnet.helius-rpc.com/?api-key={KEY}"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"  # candidate — verify (A6)
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
OUT = pathlib.Path("tests/fixtures/providers/helius")

RPC_CALLS = [
    ("get_account_info_mint", {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
     "params": [BONK, {"encoding": "jsonParsed"}]}),  # → mintAuthority / freezeAuthority
    ("get_token_largest_accounts", {"jsonrpc": "2.0", "id": 1,
     "method": "getTokenLargestAccounts", "params": [BONK]}),  # → top-holder concentration
    ("get_token_supply", {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply", "params": [BONK]}),
]


async def rpc_probes() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=20) as client:
        for name, payload in RPC_CALLS:
            r = await client.post(RPC, json=payload)
            (OUT / f"{name}.json").write_text(
                json.dumps({"status": r.status_code, "body": r.json()}, indent=2)
            )
            print(name, r.status_code)


async def logs_capture(minutes: float) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    out = OUT / "logs_subscribe_pumpfun.jsonl"
    deadline = time.monotonic() + minutes * 60
    sub = {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
           "params": [{"mentions": [PUMPFUN_PROGRAM]}, {"commitment": "confirmed"}]}
    n = 0
    async with websockets.connect(WSS) as ws:
        await ws.send(json.dumps(sub))
        with open(out, "a", encoding="utf-8") as f:
            while time.monotonic() < deadline:
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=30)
                except TimeoutError:
                    print("WARN: no frame for 30s")
                    continue
                f.write(json.dumps({"t_wall": time.time(), "raw": frame}) + "\n")
                n += 1
    print(f"captured {n} frames -> {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["rpc", "logs"])
    p.add_argument("--minutes", type=float, default=5)
    a = p.parse_args()
    asyncio.run(rpc_probes() if a.mode == "rpc" else logs_capture(a.minutes))
