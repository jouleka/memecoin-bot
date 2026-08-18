"""PumpPortal ingestion: pure frame parser plus websocket stream adapter.

Free tier delivers token creates + migrations ONLY (trade subscription is paywalled
— see docs/PROVIDERS.md). bonk-pool creates are skipped (n=1 provisional schema).
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any

import websockets

from memebot.events import AdapterHealth, Event, TokenCreated, TokenGraduated

log = logging.getLogger("memebot.ingest.pumpportal")

SUBSCRIPTIONS = ({"method": "subscribeNewToken"}, {"method": "subscribeMigration"})


def parse_frame(raw: str, *, t_wall: float, t_mono: float) -> Event | None:
    """Parse one PumpPortal frame into a typed event, or None (ack/garbage/skip)."""
    try:
        payload: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    tx_type = payload.get("txType")
    if tx_type == "create":
        if payload.get("pool") != "pump":  # bonk et al: provisional schema, skip (delta 5)
            return None
        if not payload.get("mint"):
            return None
        return TokenCreated(
            t_wall=t_wall, t_mono=t_mono,
            mint=payload["mint"],
            name=payload.get("name") or "",      # ~5% arrive metadata-less (delta 4)
            symbol=payload.get("symbol") or "",
            creator=payload.get("traderPublicKey") or "",
            raw=payload,
        )
    if tx_type == "migrate":
        if not payload.get("mint"):
            return None
        return TokenGraduated(
            t_wall=t_wall, t_mono=t_mono,
            mint=payload["mint"],
            pool="",                              # migrate frames carry no pool address
            dex=payload.get("pool") or "",
            raw=payload,
        )
    return None


class PumpPortalStream:
    """Websocket adapter: connect → subscribe → parse frames onto the bus.

    Fidelity rules (POL-3 lessons): jittered exponential backoff on reconnect,
    resubscribe on every (re)connect, staleness watchdog (no frame in
    stale_after_s → force reconnect), AdapterHealth on every transition.
    """

    def __init__(self, bus, *, uri: str, stale_after_s: float,
                 base_backoff_s: float = 1.0, max_backoff_s: float = 60.0) -> None:
        self._bus = bus
        self._uri = uri
        self._stale_after_s = stale_after_s
        self._base_backoff_s = base_backoff_s
        self._max_backoff_s = max_backoff_s

    async def _health(self, status: str, detail: str) -> None:
        await self._bus.publish(AdapterHealth(
            t_wall=time.time(), t_mono=time.monotonic(),
            adapter="pumpportal", status=status, detail=detail))

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            try:
                async with websockets.connect(self._uri) as ws:
                    for sub in SUBSCRIPTIONS:
                        await ws.send(json.dumps(sub))
                    await self._health("up", "connected+subscribed")
                    attempt = 0
                    while not stop.is_set():
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(), timeout=self._stale_after_s)
                        except TimeoutError:
                            await self._health("stale",
                                               f"no frame in {self._stale_after_s}s")
                            break  # force reconnect
                        # Deviation from plan (pre-approved, C1 review carry-forward):
                        # parse_frame can raise TypeError on exotic non-str/bytes
                        # input. Guard per-frame so a bad frame is logged+skipped
                        # rather than tearing down the whole connection.
                        try:
                            event = parse_frame(raw, t_wall=time.time(),
                                                t_mono=time.monotonic())
                        except Exception:
                            log.exception("unparseable frame skipped")
                            continue
                        if event is not None:
                            await self._bus.publish(event)
            except (OSError, websockets.WebSocketException) as exc:
                await self._health("down", repr(exc))
            if stop.is_set():
                return
            attempt += 1
            delay = min(self._max_backoff_s,
                        self._base_backoff_s * (2 ** min(attempt, 6)))
            delay *= 0.5 + random.random()  # jitter
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass
