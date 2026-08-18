"""Token lifecycle: pure transition rules + the bus-consuming tracker (spec §5.2).

M2 implements the data-driven transitions that HAVE producers: FRESH->CLIMBING
(curve progress), ->GRADUATED (either graduation signal, idempotent), ->DEAD
(stall/inactivity). TRENDING/ESTABLISHED need M4 market data; their thresholds
stay in config but nothing fires them yet.

Idle semantics: last_seen advances only on NEW information (progress change or
a state-changing event); equal-progress polls leave it alone so flat-liners can
go DEAD (otherwise every poll would refresh last_seen and the stall rule could
never fire).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import time
from collections.abc import Awaitable, Callable
from typing import Any

from memebot.events import (CurveProgress, LifecycleTransition, SafetyHardFail,
                            TokenCreated, TokenGraduated)
from memebot.store import (EvidenceIntegrityError, allocate_p3_causal_wall,
                           fence_p3_causal_wall, get_token,
                           set_terminal_state_with_reputation, set_token_state,
                           upsert_token_identity)

log = logging.getLogger("memebot.lifecycle")

TERMINAL = ("DEAD",)
_IDENTITY_FIELDS = (
    "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
)


def _validated_processing_wall(value: object) -> float:
    if (
        type(value) not in (int, float)
        or not 0.0 <= value <= 4_102_444_800.0
        or not math.isfinite(value)
    ):
        raise ValueError("invalid p3 causal wall")
    return float(value)


def _validated_terminal_creator(conn, row) -> tuple[object, bool]:
    def object_no_duplicates(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(token: str) -> object:
        raise ValueError(f"invalid JSON constant: {token}")

    try:
        raw_metadata = row["meta_json"]
        if type(raw_metadata) is not str:
            raise ValueError("invalid identity metadata affinity")
        metadata = json.loads(
            raw_metadata,
            object_pairs_hook=object_no_duplicates,
            parse_constant=reject_constant,
        )
        canonical_metadata = json.dumps(
            metadata,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if raw_metadata != canonical_metadata:
            raise ValueError("noncanonical identity metadata")
        if type(metadata) is not dict:
            raise ValueError("invalid identity metadata shape")
        observed_at = metadata.get("identity_observed_at")
        conflicts = metadata.get("identity_conflicts")
        conflict_observed_at = metadata.get("identity_conflict_observed_at")
        expected_keys = set(_IDENTITY_FIELDS) | {
            "identity_observed_at",
            "identity_conflicts",
            "identity_conflict_observed_at",
        }
        if (
            set(metadata) != expected_keys
            or any(type(metadata[field]) is not str for field in _IDENTITY_FIELDS)
            or type(observed_at) is not dict
            or type(conflicts) is not list
            or type(conflict_observed_at) is not dict
            or any(type(field) is not str for field in conflicts)
            or conflicts != sorted(set(conflicts))
            or any(field not in _IDENTITY_FIELDS for field in conflicts)
            or set(conflict_observed_at) != set(conflicts)
        ):
            raise ValueError("invalid identity metadata shape")
        expected_observed_fields = {
            field for field in _IDENTITY_FIELDS if metadata[field].strip()
        }
        if (
            set(observed_at) != expected_observed_fields
            or any(
                not metadata[field].strip() or field not in observed_at
                for field in conflicts
            )
        ):
            raise ValueError("invalid identity observation shape")
        clock_rows = conn.execute(
            "SELECT singleton,last_wall FROM p3_causal_clock LIMIT 2"
        ).fetchall()
        if (
            len(clock_rows) != 1
            or type(clock_rows[0][0]) is not int
            or clock_rows[0][0] != 1
        ):
            raise ValueError("invalid p3 causal clock")
        identity_t = _validated_processing_wall(row["p3_identity_ingested_at"])
        causal_wall = _validated_processing_wall(clock_rows[0][1])
        observation_times = {
            field: _validated_processing_wall(value)
            for field, value in observed_at.items()
        }
        conflict_times = {
            field: _validated_processing_wall(value)
            for field, value in conflict_observed_at.items()
        }
        if (
            identity_t > causal_wall
            or any(
                not identity_t <= value <= causal_wall
                for value in (*observation_times.values(), *conflict_times.values())
            )
            or any(
                conflict_times[field] <= observation_times[field]
                for field in conflicts
            )
        ):
            raise ValueError("invalid identity observation time")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid p3 token metadata") from exc
    return metadata["creator"], "creator" in conflicts


def decide(state: str, *, progress: float, age_s: float, idle_s: float,
           cfg: dict[str, Any]) -> str | None:
    """Pure state-transition rule. Returns the new state or None."""
    if state in TERMINAL or state == "GRADUATED":
        return None
    if state == "FRESH":
        if progress >= cfg["climbing_progress_pct"]:
            return "CLIMBING"
        if progress < cfg["stall_progress_pct"] and age_s >= cfg["dead_after_stalled_s"] \
                and idle_s >= cfg["dead_after_stalled_s"]:
            return "DEAD"
    if state == "CLIMBING" and idle_s >= cfg["dead_no_activity_s"]:
        return "DEAD"
    return None


class LifecycleTracker:
    def __init__(self, bus, conn, *, cfg: dict[str, Any],
                 runtime_boot_id: int,
                 runtime_causal_floor: float,
                 clock: Callable[[], float] = time.time,
                 retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 ) -> None:
        if type(runtime_boot_id) is not int or runtime_boot_id <= 0:
            raise ValueError("runtime boot ID must be a positive integer")
        self._bus = bus
        self._conn = conn
        self._cfg = cfg
        self._runtime_boot_id = runtime_boot_id
        self._runtime_causal_floor = _validated_processing_wall(
            runtime_causal_floor
        )
        self._clock = clock
        self._retry_sleep = retry_sleep
        self._queue = bus.subscribe(
            TokenCreated, CurveProgress, TokenGraduated, SafetyHardFail,
            critical=True,
        )

    def _persist_p3_curve_progress(
        self,
        event: CurveProgress,
        raw_processed_at: float,
    ) -> tuple[float, str, str, str] | None:
        source_wall = _validated_processing_wall(event.t_wall)
        if (
            type(event.source_boot_id) is not int
            or event.source_boot_id <= 0
            or event.source_boot_id != self._runtime_boot_id
        ):
            raise ValueError("invalid curve progress source boot")
        if (
            type(event.source_seq) is not int
            or event.source_seq <= 0
            or type(event.t_mono) not in (int, float)
            or not math.isfinite(event.t_mono)
            or type(event.progress_pct) not in (int, float)
            or not math.isfinite(event.progress_pct)
            or not 0.0 <= event.progress_pct <= 100.0
        ):
            raise ValueError("invalid P3 curve progress")
        reserves = (
            event.virtual_sol_reserves,
            event.virtual_token_reserves,
            event.real_sol_reserves,
            event.real_token_reserves,
        )
        if (
            any(type(value) is not int or value < 0 for value in reserves)
            or event.virtual_sol_reserves == 0
            or event.virtual_token_reserves == 0
        ):
            raise ValueError("invalid P3 curve progress")
        if self._conn.in_transaction:
            raise RuntimeError("P3 curve progress persistence owns its transaction")

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = get_token(self._conn, event.mint)
            if row is None:
                raise ValueError("missing token after identity adoption")
            state = row["state"]
            if state == "GRADUATED":
                self._conn.rollback()
                return None
            prior_boot_id = row["curve_progress_source_boot_id"]
            prior_seq = row["curve_progress_source_seq"]
            if (prior_boot_id is None) != (prior_seq is None):
                raise ValueError("invalid persisted curve progress sequence")
            if prior_boot_id is not None and (
                type(prior_boot_id) is not int
                or prior_boot_id <= 0
                or type(prior_seq) is not int
                or prior_seq <= 0
            ):
                raise ValueError("invalid persisted curve progress sequence")
            if prior_boot_id == event.source_boot_id and prior_seq is not None:
                if event.source_seq < prior_seq:
                    raise ValueError("invalid curve progress sequence")
                if event.source_seq == prior_seq:
                    prior_payload = (
                        row["curve_progress"],
                        row["curve_progress_source_wall"],
                        row["curve_progress_virtual_sol_reserves"],
                        row["curve_progress_virtual_token_reserves"],
                        row["curve_progress_real_sol_reserves"],
                        row["curve_progress_real_token_reserves"],
                    )
                    event_payload = (
                        event.progress_pct,
                        source_wall,
                        event.virtual_sol_reserves,
                        event.virtual_token_reserves,
                        event.real_sol_reserves,
                        event.real_token_reserves,
                    )
                    if prior_payload != event_payload:
                        raise ValueError("invalid curve progress sequence")
                    self._conn.rollback()
                    return None
            changed = event.progress_pct != row["curve_progress"]
            new = decide(
                state,
                progress=event.progress_pct,
                age_s=raw_processed_at - row["created_at"],
                idle_s=raw_processed_at - row["last_seen"],
                cfg=self._cfg,
            )
            causal_wall = max(raw_processed_at, source_wall)
            fence_p3_causal_wall(
                self._conn,
                observed_wall=causal_wall,
            )
            observed_at = allocate_p3_causal_wall(
                self._conn,
                raw_wall=causal_wall,
            )
            if observed_at <= self._runtime_causal_floor:
                raise ValueError("curve progress does not follow runtime causal floor")
            cursor = self._conn.execute(
                """UPDATE tokens
SET state=?,curve_progress=?,
    last_seen=CASE WHEN ? THEN ? ELSE last_seen END,
    curve_progress_virtual_sol_reserves=?,
    curve_progress_virtual_token_reserves=?,
    curve_progress_real_sol_reserves=?,
    curve_progress_real_token_reserves=?,
    curve_progress_observed_at=?,curve_progress_source_wall=?,
    curve_progress_source_boot_id=?,curve_progress_source_seq=?
WHERE mint=?""",
                (
                    new or state,
                    event.progress_pct,
                    changed or new is not None,
                    raw_processed_at,
                    event.virtual_sol_reserves,
                    event.virtual_token_reserves,
                    event.real_sol_reserves,
                    event.real_token_reserves,
                    observed_at,
                    source_wall,
                    event.source_boot_id,
                    event.source_seq,
                    event.mint,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("missing token during progress persistence")
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        if new is None:
            return None
        return observed_at, event.mint, state, new

    def _persist(
        self, event, raw_processed_at: float,
    ) -> tuple[float, str, str, str] | None:
        if isinstance(event, TokenCreated):
            upsert_token_identity(
                self._conn,
                mint=event.mint,
                raw_ingested_at=raw_processed_at,
                bonding_curve_key=event.raw.get("bondingCurveKey", ""),
                fields={
                    "creator": event.creator,
                    "name": event.name,
                    "symbol": event.symbol,
                    "uri": event.raw.get("uri"),
                    "website": event.raw.get("website"),
                    "twitter": event.raw.get("twitter"),
                    "telegram": event.raw.get("telegram"),
                },
            )
            row = get_token(self._conn, event.mint)
            if row["last_seen"] != raw_processed_at:
                set_token_state(
                    self._conn, event.mint, row["state"],
                    last_seen=raw_processed_at,
                )
            return None
        row = get_token(self._conn, event.mint)
        if row is None or row["p3_identity_ingested_at"] is None:
            raw = event.raw if isinstance(event, TokenGraduated) else {}
            upsert_token_identity(
                self._conn,
                mint=event.mint,
                raw_ingested_at=raw_processed_at,
                bonding_curve_key=raw.get("bondingCurveKey", ""),
                fields={
                    "creator": raw.get("traderPublicKey"),
                    "name": raw.get("name"),
                    "symbol": raw.get("symbol"),
                    "uri": raw.get("uri"),
                    "website": raw.get("website"),
                    "twitter": raw.get("twitter"),
                    "telegram": raw.get("telegram"),
                },
            )
            row = get_token(self._conn, event.mint)
        state = row["state"]
        if isinstance(event, (SafetyHardFail, TokenGraduated)):
            if isinstance(event, TokenGraduated) and row["rugged"]:
                return None
            creator, creator_conflicted = _validated_terminal_creator(
                self._conn, row,
            )
            result = set_terminal_state_with_reputation(
                self._conn,
                mint=event.mint,
                outcome=(
                    "RUGGED" if isinstance(event, SafetyHardFail) else "GRADUATED"
                ),
                raw_processed_at=raw_processed_at,
                creator=creator,
                creator_conflicted=creator_conflicted,
            )
            if state == result.state:
                return None
            if result.processed_at is None:
                raise ValueError("missing terminal processing wall")
            return (
                result.processed_at,
                event.mint,
                state,
                result.state,
            )
        # CurveProgress — Resolution 1+2: single write; last_seen only on new info
        if state == "GRADUATED":
            return None
        return self._persist_p3_curve_progress(event, raw_processed_at)

    async def _publish_transition(self, transition) -> None:
        if transition is None:
            return
        t_wall, mint, from_state, to_state = transition
        await self._bus.publish(LifecycleTransition(
            t_wall=t_wall,
            t_mono=time.monotonic(),
            mint=mint,
            from_state=from_state,
            to_state=to_state,
        ))

    async def _handle(self, event) -> None:
        raw_processed_at = _validated_processing_wall(self._clock())
        transition = self._persist(event, raw_processed_at)
        await self._publish_transition(transition)

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        self._queue.get(), timeout=0.5,
                    )
                except TimeoutError:
                    if stop.is_set():
                        break
                    continue
                raw_processed_at = _validated_processing_wall(self._clock())
                attempt = 1
                delay = 0.05
                while True:
                    try:
                        transition = self._persist(event, raw_processed_at)
                    except (sqlite3.Error, EvidenceIntegrityError):
                        log.exception(
                            "lifecycle persistence failed",
                            extra={"extra_fields": {
                                "mint": getattr(event, "mint", "?"),
                                "attempt": attempt,
                            }},
                        )
                        await self._retry_sleep(delay)
                        attempt += 1
                        delay = min(delay * 2.0, 2.0)
                        continue
                    break
                await self._publish_transition(transition)
                self._bus.critical_done(self._queue)
        finally:
            self._bus.unsubscribe(self._queue)
