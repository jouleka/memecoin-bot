"""Budgeted bonding-curve poller (M2 delta 1: replaces the 21x-over-budget
program-log firehose). One getMultipleAccounts call reads up to batch_size curve
accounts; decoded offline; emits CurveProgress + TokenGraduated(complete-flag)."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from memebot.events import AdapterHealth, CurveProgress, TokenGraduated
from memebot.ingest.curve import decode_curve_account, progress_pct
from memebot.store import tracked_tokens

log = logging.getLogger("memebot.ingest.curvepoller")


class CurvePoller:
    def __init__(self, bus, conn, *, cfg: dict[str, Any], pumpfun: dict[str, Any],
                 rpc_url: str, client: httpx.AsyncClient,
                 source_boot_id: int) -> None:
        self._bus = bus
        self._conn = conn
        self._cfg = cfg
        self._pumpfun = pumpfun
        self._rpc_url = rpc_url
        self._client = client
        self._source_boot_id = source_boot_id
        self._source_seq = 0

    async def _health(self, status: str, detail: str) -> None:
        await self._bus.publish(AdapterHealth(
            t_wall=time.time(), t_mono=time.monotonic(),
            adapter="curvepoller", status=status, detail=detail))

    async def _poll_once(self) -> None:
        rows = tracked_tokens(self._conn, states=("FRESH", "CLIMBING"),
                              limit=self._cfg["max_tracked"])
        pollable = [r for r in rows if r["bonding_curve_key"]]
        for i in range(0, len(pollable), self._cfg["batch_size"]):
            chunk = pollable[i:i + self._cfg["batch_size"]]
            keys = [r["bonding_curve_key"] for r in chunk]
            body = {"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                    "params": [keys, {"encoding": "base64"}]}
            resp = await self._client.post(self._rpc_url, json=body, timeout=20)
            resp.raise_for_status()
            values = resp.json()["result"]["value"]
            if len(values) != len(keys):
                log.warning("rpc returned %d values for %d keys - skipping chunk",
                            len(values), len(keys),
                            extra={"extra_fields": {"adapter": "curvepoller"}})
                continue
            for row, account in zip(chunk, values):
                if account is None or not account["data"][0]:  # absent or empty-data (~10% live)
                    continue
                try:
                    state = decode_curve_account(account["data"][0])
                except ValueError:
                    log.warning("undecodable curve account",
                                extra={"extra_fields": {"mint": row["mint"]}})
                    continue
                if state.complete:
                    await self._bus.publish(TokenGraduated(
                        t_wall=time.time(), t_mono=time.monotonic(),
                        mint=row["mint"], pool="", dex="curve-complete", raw={}))
                    continue
                pct = progress_pct(
                    state,
                    sellable_supply_tokens=self._pumpfun["sellable_supply"],
                    token_decimals=self._pumpfun["token_decimals"])
                source_seq = 0
                if self._source_boot_id:
                    self._source_seq += 1
                    source_seq = self._source_seq
                await self._bus.publish(CurveProgress(
                    t_wall=time.time(), t_mono=time.monotonic(),
                    mint=row["mint"], progress_pct=pct,
                    virtual_sol_reserves=state.virtual_sol_reserves,
                    virtual_token_reserves=state.virtual_token_reserves,
                    real_sol_reserves=state.real_sol_reserves,
                    real_token_reserves=state.real_token_reserves,
                    source_boot_id=self._source_boot_id,
                    source_seq=source_seq))

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self._poll_once()
            except (httpx.HTTPError, KeyError, TypeError) as exc:
                await self._health("down", repr(exc))
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._cfg["interval_s"])
            except TimeoutError:
                continue
