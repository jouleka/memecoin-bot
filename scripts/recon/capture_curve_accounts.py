"""Capture real pump.fun bonding-curve accounts into a fixture.

Pulls bondingCurveKey values from the A2 PumpPortal fixture, fetches the accounts
via one getMultipleAccounts call, saves base64 data for decode tests.
Usage: ./.venv/bin/python scripts/recon/capture_curve_accounts.py
"""
from __future__ import annotations

import json
import os
import pathlib

import httpx

KEY = os.environ["MEMEBOT_HELIUS_API_KEY"]
RPC = f"https://mainnet.helius-rpc.com/?api-key={KEY}"
FIXTURE_IN = pathlib.Path("tests/fixtures/providers/pumpportal/session1.jsonl")
FIXTURE_OUT = pathlib.Path("tests/fixtures/providers/helius/curve_accounts.json")


def main() -> None:
    keys: list[str] = []
    for line in FIXTURE_IN.read_text().splitlines():
        payload = json.loads(json.loads(line)["raw"])
        if payload.get("txType") == "create" and payload.get("bondingCurveKey"):
            keys.append(payload["bondingCurveKey"])
        if len(keys) >= 20:
            break
    body = {"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
            "params": [keys, {"encoding": "base64"}]}
    r = httpx.post(RPC, json=body, timeout=30)
    r.raise_for_status()
    accounts = r.json()["result"]["value"]
    # ~10% of live curve accounts return empty data (closed/reassigned) — skip inert entries
    out = [{"pubkey": k, "data_b64": a["data"][0], "lamports": a["lamports"]}
           for k, a in zip(keys, accounts) if a is not None and a["data"][0]]
    FIXTURE_OUT.write_text(json.dumps(out, indent=2))
    print(f"captured {len(out)}/{len(keys)} curve accounts -> {FIXTURE_OUT}")


if __name__ == "__main__":
    main()
