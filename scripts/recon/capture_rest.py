"""Capture REST responses (status + headers + body) to fixtures.

Usage:
  ./.venv/bin/python scripts/recon/capture_rest.py dexscreener
  ./.venv/bin/python scripts/recon/capture_rest.py --all
Env vars named in a target's "env" list are substituted into its URL as {VAR}.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib

import httpx

TARGETS_FILE = pathlib.Path(__file__).with_name("rest_targets.json")
FIXTURES = pathlib.Path("tests/fixtures/providers")


async def capture_provider(provider: str, targets: list[dict]) -> None:
    outdir = FIXTURES / provider
    outdir.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=20) as client:
        for t in targets:
            try:
                url = t["url"].format(**{v: os.environ[v] for v in t.get("env", [])})
                r = await client.get(url)
                record = {
                    "name": t["name"],
                    "url": t["url"],  # unexpanded — never write secrets to fixtures
                    "status": r.status_code,
                    "headers": {
                        k: v
                        for k, v in r.headers.items()
                        if k.lower().startswith(("x-rate", "ratelimit", "retry", "x-request"))
                    },
                    "body": r.json() if "json" in r.headers.get("content-type", "") else r.text,
                }
            except (httpx.HTTPError, KeyError) as e:
                record = {"name": t["name"], "url": t["url"], "error": repr(e)}
            path = outdir / f"{t['name']}.json"
            path.write_text(json.dumps(record, indent=2), encoding="utf-8")
            print(f"{provider}/{t['name']}: {record.get('status', record.get('error'))}")


async def main(only: str | None) -> None:
    targets = json.loads(TARGETS_FILE.read_text())
    for provider, tlist in targets.items():
        if only and provider != only:
            continue
        await capture_provider(provider, tlist)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("provider", nargs="?")
    p.add_argument("--all", action="store_true")
    a = p.parse_args()
    asyncio.run(main(None if a.all else a.provider))
