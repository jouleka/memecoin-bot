"""Run an adapter coroutine forever: crash → AdapterHealth(down) + jittered
exponential-backoff restart; healthy run resets the backoff; stop exits promptly
(spec §7 adapter supervision)."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

from memebot.events import AdapterHealth

log = logging.getLogger("memebot.ingest.supervisor")

Adapter = Callable[[asyncio.Event], Awaitable[None]]


async def supervise(name: str, adapter: Adapter, bus, stop: asyncio.Event, *,
                    base_delay_s: float = 1.0, max_delay_s: float = 60.0,
                    healthy_reset_s: float = 300.0) -> None:
    failures = 0
    while not stop.is_set():
        started = time.monotonic()
        try:
            await adapter(stop)
            if stop.is_set():
                return
            log.warning("adapter returned unexpectedly",
                        extra={"extra_fields": {"adapter": name}})
        except Exception as exc:
            await bus.publish(AdapterHealth(
                t_wall=time.time(), t_mono=time.monotonic(),
                adapter=name, status="down", detail=repr(exc)))
            log.exception("adapter crashed", extra={"extra_fields": {"adapter": name}})
        if time.monotonic() - started > healthy_reset_s:
            failures = 0
        failures += 1
        delay = min(max_delay_s, base_delay_s * (2 ** min(failures, 6)))
        delay *= 0.5 + random.random()
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            continue
