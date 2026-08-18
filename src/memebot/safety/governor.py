"""Per-provider rate-limit governor: token-bucket limiter + circuit breaker (spec §7).

Makes "rate-limited" mean WAIT (acquire blocks for a refill), while genuine failures
trip a breaker so a dead provider fails fast instead of hammering. Injected clock/sleep
for deterministic tests.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


class CircuitOpen(Exception):
    pass


class Governor:
    def __init__(self, *, per_minute: float, clock: Callable[[], float] | None = None,
                 sleep: Callable[[float], Awaitable[None]] | None = None,
                 failure_threshold: int = 5, open_seconds: float = 30.0) -> None:
        if per_minute <= 0:
            raise ValueError("per_minute must be positive")
        import time
        self._rate = per_minute / 60.0          # tokens/sec
        self._capacity = max(1.0, per_minute / 20.0)  # ~3s burst
        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._tokens = self._capacity
        self._last = self._clock()
        self._failure_threshold = failure_threshold
        self._open_seconds = open_seconds
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
        self._last = now

    async def acquire(self) -> None:
        if self._opened_at is not None:
            if self._clock() - self._opened_at < self._open_seconds:
                raise CircuitOpen("provider circuit open")
            # half-open: allow one probe; stays "opened" until a success closes it
        self._refill()
        while self._tokens < 1.0:
            deficit = 1.0 - self._tokens
            await self._sleep(deficit / self._rate)
            self._refill()
        self._tokens -= 1.0

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold:
            self._opened_at = self._clock()
