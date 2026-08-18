"""Telegram ops channel (spec §5.9 subset; M3 delta 6): outbound alerts + read-only
/status, over a swappable transport. FakeTransport backs the tests; the real bot token
+ chat-id are deploy-time env vars. Rich state-changing commands (auth/nonce) are M6.
"""
from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from typing import Any, Protocol

import httpx

from memebot.redact import redact_secrets

log = logging.getLogger("memebot.telegram")

TELEGRAM_API_BASE = "https://api.telegram.org"


class Transport(Protocol):
    async def send(self, chat_id: str, text: str) -> None: ...
    async def get_updates(self) -> list[dict[str, Any]]: ...


class FakeTransport:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []
        self._updates: list[dict[str, Any]] = []

    async def send(self, chat_id: str, text: str) -> None:
        self.sent.append({"chat_id": chat_id, "text": text})

    async def get_updates(self) -> list[dict[str, Any]]:
        u, self._updates = self._updates, []
        return u

    def queue_update(self, *, chat_id: str, text: str) -> None:
        self._updates.append({"message": {"chat": {"id": chat_id}, "text": text}})


class HttpTransport:
    """Real Telegram Bot API transport (thin shim; TelegramOps logic is
    FakeTransport-tested). Uses an injected httpx.AsyncClient so lifetime/close
    is owned by the caller (main.py), matching the poller/ext/jup client pattern."""

    def __init__(self, *, token: str, client: httpx.AsyncClient,
                 api_base: str = TELEGRAM_API_BASE) -> None:
        self._base = f"{api_base}/bot{token}"
        self._client = client
        self._last_update_id: int | None = None

    async def send(self, chat_id: str, text: str) -> None:
        resp = await self._client.post(f"{self._base}/sendMessage",
                                       json={"chat_id": chat_id, "text": text}, timeout=20)
        resp.raise_for_status()

    async def get_updates(self) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"timeout": 0}
        if self._last_update_id is not None:
            params["offset"] = str(self._last_update_id + 1)
        resp = await self._client.get(f"{self._base}/getUpdates", params=params, timeout=20)
        resp.raise_for_status()
        updates = resp.json().get("result", [])
        for u in updates:
            uid = u.get("update_id")
            if isinstance(uid, int) and (self._last_update_id is None or uid > self._last_update_id):
                self._last_update_id = uid
        return updates


class _WatchReservation:
    """Opaque identity for one WATCH delivery attempt."""

    __slots__ = ("_sequence",)

    def __init__(self, sequence: int) -> None:
        self._sequence = sequence


class WatchLimiter:
    """In-memory WATCH-only rolling cap and mint+segment dedupe.

    State intentionally resets on restart. Persisting informational delivery would need
    reviewed append-only schema work; the unfinished v5/P3 schema is not modified by this
    immediate recovery slice. Exact boundaries reopen eligibility (age >= window).
    """

    def __init__(self, *, clock: Callable[[], float] = time.time,
                 max_per_hour: int = 15, dedupe_s: float = 86_400.0) -> None:
        self._clock = clock
        self._max = max_per_hour
        self._dedupe_s = dedupe_s
        self._send_times: list[float] = []
        self._seen: dict[tuple[str, str], tuple[float, _WatchReservation]] = {}
        self._next_reservation_id = 0

    def allow(self, mint: str, segment: str) -> _WatchReservation | None:
        """Reserve one attempt and return its opaque identity, or None if denied."""
        now = self._clock()
        if not math.isfinite(now):
            return None
        self._send_times = [t for t in self._send_times if now - t < 3600.0]
        self._seen = {
            key: reservation for key, reservation in self._seen.items()
            if now - reservation[0] < self._dedupe_s
        }
        key = (mint, segment)
        if key in self._seen or len(self._send_times) >= self._max:
            return None
        self._next_reservation_id += 1
        reservation = _WatchReservation(self._next_reservation_id)
        self._send_times.append(now)
        self._seen[key] = (now, reservation)
        return reservation

    def release_dedupe(self, mint: str, segment: str,
                       reservation: _WatchReservation) -> None:
        """Release one failed delivery reservation without refunding its attempt."""
        key = (mint, segment)
        current = self._seen.get(key)
        if current is not None and current[1] is reservation:
            self._seen.pop(key)


class TelegramOps:
    def __init__(self, transport: Transport, *, chat_id: str, max_alerts_per_hour: int,
                 clock: Callable[[], float] = time.time,
                 status_fn: Callable[[], str] | None = None,
                 watch_limiter: WatchLimiter | None = None) -> None:
        self._tp = transport
        self._chat_id = chat_id
        self._max = max_alerts_per_hour
        self._clock = clock
        self._status_fn = status_fn
        self._alert_times: list[float] = []
        self._watch_limiter = watch_limiter or WatchLimiter(clock=clock)

    async def alert(self, text: str) -> None:
        now = self._clock()
        if not math.isfinite(now):
            return
        self._alert_times = [t for t in self._alert_times if now - t < 3600.0]
        if len(self._alert_times) >= self._max:
            log.warning("telegram alert dropped (hourly cap)", extra={"extra_fields": {"text": text}})
            return
        self._alert_times.append(now)
        await self._send(text)

    async def watch(self, text: str, *, mint: str, segment: str) -> None:
        """Send informational WATCH without consuming BUY/SELL/ops alert capacity."""
        reservation = self._watch_limiter.allow(mint, segment)
        if reservation is None:
            log.info("telegram WATCH dropped (cap or dedupe)",
                     extra={"extra_fields": {"mint": mint, "segment": segment}})
            return
        if not await self._send(text):
            self._watch_limiter.release_dedupe(mint, segment, reservation)

    async def _send(self, text: str) -> bool:
        try:
            await self._tp.send(self._chat_id, text)
            return True
        except Exception as exc:
            # never let ops-paging break the caller; log.exception would dump a traceback
            # that can embed the bot token in the request URL (e.g. HTTPStatusError) -
            # redact and use warning (no traceback) instead.
            log.warning("telegram send failed: %s", redact_secrets(repr(exc)))
            return False

    async def poll_once(self) -> None:
        try:
            updates = await self._tp.get_updates()
        except Exception as exc:
            log.warning("telegram get_updates failed: %s", redact_secrets(repr(exc)))
            return
        for u in updates:
            try:
                msg = u.get("message") or {}
                chat = str((msg.get("chat") or {}).get("id", ""))
                if chat != self._chat_id:                # allowlist gate (read-only, no nonce needed)
                    continue
                if (msg.get("text") or "").strip() == "/status" and self._status_fn:
                    await self._send(self._status_fn())
            except Exception:
                log.exception("telegram update parse failed")
                continue


def format_buy_alert(ev) -> str:
    alert = (f"🟢 BUY {ev.segment} {ev.mint}\n"
             f"score {ev.score:.0f} · size {ev.size_sol:.3f} SOL · qty {ev.qty:,.0f}\n"
             f"fill {ev.fill_price:.3e} SOL · grade {ev.realism_grade}")
    canonical_mint = getattr(ev, "canonical_mint", None)
    resolver_version = getattr(ev, "canonical_resolver_version", None)
    recheck_hash = getattr(ev, "canonical_recheck_hash", None)
    proof_ids = (
        getattr(ev, "canonical_recheck_id", None),
        getattr(ev, "paper_trade_id", None),
        getattr(ev, "paper_entry_execution_id", None),
    )
    if (
        getattr(ev, "canonical_status", None) == "CANONICAL"
        and all(
            isinstance(value, str) and bool(value.strip())
            for value in (canonical_mint, resolver_version, recheck_hash)
        )
        and all(type(value) is int and value > 0 for value in proof_ids)
    ):
        return (f"{alert}\ncanonical confirmed {canonical_mint.strip()} · "
                f"{resolver_version.strip()} · recheck #{proof_ids[0]} · "
                f"trade #{proof_ids[1]} · execution #{proof_ids[2]}")
    return alert


def format_sell_alert(ev) -> str:
    sign = "🟢" if ev.pnl_sol >= 0 else "🔴"
    return (f"{sign} SELL {ev.segment} {ev.mint} ({ev.reason})\n"
            f"qty {ev.qty:,.0f} · fill {ev.fill_price:.3e} SOL · grade {ev.realism_grade}\n"
            f"P&L {ev.pnl_sol:+.3f} SOL")


def format_watch_alert(ev) -> str:
    """Pure informational candidate formatter; it never implies a trade action."""
    mint_value = getattr(ev, "mint", None)
    segment_value = getattr(ev, "segment", None)
    mint = mint_value.strip() if isinstance(mint_value, str) and mint_value.strip() else "UNKNOWN"
    segment = (segment_value.strip()
               if isinstance(segment_value, str) and segment_value.strip() else "UNKNOWN")
    score_value = getattr(ev, "score", None)
    spot_value = getattr(ev, "spot_price_sol", None)
    score = (f"{score_value:.1f}" if isinstance(score_value, (int, float))
             and not isinstance(score_value, bool) and math.isfinite(score_value) else "UNKNOWN")
    spot = (f"{spot_value:.3e} SOL/token" if isinstance(spot_value, (int, float))
            and not isinstance(spot_value, bool) and math.isfinite(spot_value) else "UNKNOWN")
    return ("👀 WATCH — NOT A BUY\n"
            f"Mint: {mint}\n"
            f"Segment: {segment}\n"
            f"Score: {score}\n"
            f"Spot: {spot}\n"
            f"Pump.fun: https://pump.fun/coin/{mint}\n"
            f"DexScreener: https://dexscreener.com/solana/{mint}\n"
            f"Solscan: https://solscan.io/token/{mint}")


class NullOps:
    """Stand-in for TelegramOps when telegram is disabled/unconfigured: alert() and
    poll_once() both no-op (debug-logged) so callers (GateRunner's consumer loop,
    main.py) never need a None-check on whether ops paging is wired up."""

    async def alert(self, text: str) -> None:
        log.debug("telegram disabled; alert dropped", extra={"extra_fields": {"text": text}})

    async def watch(self, text: str, *, mint: str, segment: str) -> None:
        log.debug("telegram disabled; WATCH dropped",
                  extra={"extra_fields": {"mint": mint, "segment": segment}})

    async def poll_once(self) -> None:
        log.debug("telegram disabled; poll_once no-op")
