"""ForwardReturnTracker (spec §5.7): counterfactual forward-returns at +1h/+6h/+24h for
EVERY scored candidate (traded or not). This is how the ledger proves out-of-sample whether
the score predicts profit, not merely graduation. Deterministic under an injected clock.
Maintains its own price cache from CurveProgress reserves (self-contained, race-free)."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from bisect import bisect_left, bisect_right
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field

import memebot.store as store
from memebot.events import (
    CandidateScored,
    CanonicalObservationStarted,
    CurveProgress,
    LifecycleTransition,
    event_from_dict,
)
from memebot.ingest.curve import CurveState, spot_price_sol_per_token
from memebot.journal import Journal, JournalReplayGap
from memebot.store import list_decisions_for_counterfactual, record_outcome

log = logging.getLogger("memebot.counterfactual")

DEFAULT_HORIZONS = (3600.0, 21600.0, 86400.0)   # +1h / +6h / +24h
DEFAULT_PRICE_HISTORY_RETENTION_S = 90000.0
DEFAULT_PRICE_HISTORY_MAX_SAMPLES_PER_MINT = 10000
DEFAULT_PRICE_HISTORY_MAX_MINTS = 1000
DEFAULT_MAX_IN_MEMORY_PENDING_OBSERVATIONS = 50000


@dataclass
class _Candidate:
    ref_kind: str
    ref_id: int
    mint: str
    t0: float
    price0_at: float
    price0: float
    pending: set = field(default_factory=set)   # horizons not yet recorded


class ForwardReturnTracker:
    def __init__(self, bus, conn, *, journal: Journal,
                 horizons: Sequence[float], token_decimals: int,
                 stale_price_after_s: float, reconcile_interval_s: float,
                 price_history_retention_s: float,
                 price_history_max_samples_per_mint: int,
                 price_history_max_mints: int,
                 max_in_memory_pending_observations: int,
                 clock: Callable[[], float] = time.time) -> None:
        self._bus = bus
        self._conn = conn
        self._clock = clock
        self._horizons = tuple(horizons)
        self._token_decimals = token_decimals
        self._stale_after_s = stale_price_after_s
        self._reconcile_interval_s = reconcile_interval_s
        self._price_history_retention_s = price_history_retention_s
        self._price_history_max_samples_per_mint = price_history_max_samples_per_mint
        self._price_history_max_mints = price_history_max_mints
        self._max_in_memory_pending_observations = max_in_memory_pending_observations
        self._candidates: list[_Candidate] = []
        self._prices: dict[str, float] = {}
        self._terminal: dict[str, str] = {}
        self._terminal_history: dict[str, list[tuple[float, str]]] = {}
        self._price_ts: dict[str, float] = {}
        self._price_history: dict[str, list[tuple[float, float]]] = {}
        self._price_overflow_intervals: dict[str, tuple[float, float]] = {}
        self._registered_mints: set[str] = set()
        self._journal = journal
        if bus is not None:
            self._q = bus.subscribe(
                CandidateScored,
                CanonicalObservationStarted,
                CurveProgress,
                LifecycleTransition,
            )

    def observe_price(self, ev: CurveProgress) -> None:
        if not ev.virtual_token_reserves:
            return
        tracked_mints = self._registered_mints | self._price_history.keys()
        if (
            ev.mint not in tracked_mints
            and len(tracked_mints) >= self._price_history_max_mints
        ):
            self._prune_price_history(self._clock())
            tracked_mints = self._registered_mints | self._price_history.keys()
            if (
                ev.mint not in tracked_mints
                and len(tracked_mints) >= self._price_history_max_mints
            ):
                return
        state = CurveState(virtual_token_reserves=ev.virtual_token_reserves,
                           virtual_sol_reserves=ev.virtual_sol_reserves,
                           real_token_reserves=ev.real_token_reserves,
                           real_sol_reserves=ev.real_sol_reserves,
                           token_total_supply=0, complete=False)
        price = spot_price_sol_per_token(state, token_decimals=self._token_decimals)
        self._prices[ev.mint] = price
        self._price_ts[ev.mint] = ev.t_wall
        history = self._price_history.setdefault(ev.mint, [])
        sample = (ev.t_wall, price)
        if not history or ev.t_wall >= history[-1][0]:
            history.append(sample)
        else:
            position = bisect_right(
                history, ev.t_wall, key=lambda item: item[0],
            )
            history.insert(position, sample)
        excess = len(history) - self._price_history_max_samples_per_mint
        if excess > 0:
            evicted_lower = history[0][0]
            evicted_upper = history[excess - 1][0]
            interval = self._price_overflow_intervals.get(ev.mint)
            if interval is None:
                self._price_overflow_intervals[ev.mint] = (
                    evicted_lower, evicted_upper,
                )
            else:
                self._price_overflow_intervals[ev.mint] = (
                    min(interval[0], evicted_lower),
                    max(interval[1], evicted_upper),
                )
            del history[:excess]

    def _drop_mint_memory(self, mint: str) -> None:
        self._prices.pop(mint, None)
        self._terminal.pop(mint, None)
        self._terminal_history.pop(mint, None)
        self._price_ts.pop(mint, None)
        self._price_history.pop(mint, None)
        self._price_overflow_intervals.pop(mint, None)
        self._registered_mints.discard(mint)

    def _prune_price_history(self, now: float) -> None:
        retention_cutoff = now - self._price_history_retention_s
        live_mints: set[str] = set()
        min_required_price_cutoff: dict[str, float] = {}
        for cand in self._candidates:
            if not cand.pending:
                continue
            live_mints.add(cand.mint)
            for horizon in cand.pending:
                absolute_horizon = cand.t0 + horizon
                transition = self._terminal_at_or_before(
                    cand.mint,
                    absolute_horizon,
                )
                if transition is not None and transition[1] == "DEAD":
                    continue
                required_cutoff = (
                    transition[0]
                    if transition is not None
                    else absolute_horizon
                )
                previous = min_required_price_cutoff.get(cand.mint)
                if previous is None or required_cutoff < previous:
                    min_required_price_cutoff[cand.mint] = required_cutoff

        reclaim: list[str] = []
        for mint, history in self._price_history.items():
            pending_cutoff = min_required_price_cutoff.get(mint)
            cutoff = (
                retention_cutoff
                if pending_cutoff is None
                else min(retention_cutoff, pending_cutoff)
            )
            prefix = bisect_left(history, cutoff, key=lambda sample: sample[0])
            if pending_cutoff is not None:
                anchor = bisect_right(
                    history,
                    pending_cutoff,
                    key=lambda sample: sample[0],
                ) - 1
                if anchor >= 0:
                    prefix = min(prefix, anchor)
            if prefix:
                del history[:prefix]
            if not history and mint not in live_mints:
                reclaim.append(mint)
        for mint in reclaim:
            self._drop_mint_memory(mint)

    def _release_completed_mints(self) -> None:
        live_mints = {cand.mint for cand in self._candidates}
        for mint in self._registered_mints - live_mints:
            self._drop_mint_memory(mint)
        self._registered_mints.intersection_update(live_mints)

    def _iter_replayed_overflow_evidence(
        self,
        mint: str,
    ) -> Iterator[tuple[float, float] | JournalReplayGap]:
        interval = self._price_overflow_intervals.get(mint)
        if interval is None:
            return
        if self._journal is None:
            raise RuntimeError("journal overflow evidence unavailable")
        found_lower = False
        found_upper = False
        try:
            for item in self._journal.iter_events(
                since_wall=interval[0],
                until_wall=interval[1],
            ):
                if isinstance(item, JournalReplayGap):
                    if item.mint is None or item.mint == mint:
                        if (
                            type(item.lower_wall) not in (int, float)
                            or type(item.upper_wall) not in (int, float)
                            or not math.isfinite(item.lower_wall)
                            or not math.isfinite(item.upper_wall)
                            or item.lower_wall > item.upper_wall
                        ):
                            raise RuntimeError(
                                "journal overflow evidence unavailable",
                            )
                        found_lower = found_lower or (
                            item.lower_wall <= interval[0] <= item.upper_wall
                        )
                        found_upper = found_upper or (
                            item.lower_wall <= interval[1] <= item.upper_wall
                        )
                        yield item
                    continue
                event = event_from_dict(item)
                if not isinstance(event, CurveProgress) or event.mint != mint:
                    continue
                if not event.virtual_token_reserves:
                    continue
                found_lower = found_lower or event.t_wall == interval[0]
                found_upper = found_upper or event.t_wall == interval[1]
                state = CurveState(
                    virtual_token_reserves=event.virtual_token_reserves,
                    virtual_sol_reserves=event.virtual_sol_reserves,
                    real_token_reserves=event.real_token_reserves,
                    real_sol_reserves=event.real_sol_reserves,
                    token_total_supply=0,
                    complete=False,
                )
                yield (
                    event.t_wall,
                    spot_price_sol_per_token(
                        state,
                        token_decimals=self._token_decimals,
                    ),
                )
        except RuntimeError:
            raise
        except (KeyError, OverflowError, TypeError, ValueError) as exc:
            raise RuntimeError("journal overflow evidence unavailable") from exc
        if not found_lower or not found_upper:
            raise RuntimeError("journal overflow evidence unavailable")

    def _iter_replayed_overflow_prices(
        self,
        mint: str,
    ) -> Iterator[tuple[float, float]]:
        for item in self._iter_replayed_overflow_evidence(mint):
            if isinstance(item, JournalReplayGap):
                raise RuntimeError("journal overflow evidence unavailable")
            yield item

    def latest_price(self, mint: str) -> float | None:
        return self._prices.get(mint)

    def on_transition(self, ev: LifecycleTransition) -> None:
        """DEAD/GRADUATED freeze pricing differently (C1): CurvePoller only polls
        FRESH/CLIMBING tokens, so _prices stops updating the instant a token leaves
        that set. Left alone, a DEAD (rugged) token's forward return would be computed
        against its stale-high last-seen price -- systematically overstating the edge
        for the ~99% of tokens that crash. DEAD is a total loss (price -> 0); GRADUATED
        is a real success the bonding curve can no longer price, so we keep the last
        curve price and just flag it so downstream analysis knows it's post-curve.
        Scoped to `ev.mint in self._prices` so unrelated tokens never grow either dict.
        """
        if ev.to_state not in ("DEAD", "GRADUATED"):
            return
        if ev.mint not in self._registered_mints and ev.mint not in self._prices:
            return
        if type(ev.t_wall) not in (int, float) or not math.isfinite(ev.t_wall):
            raise ValueError("invalid terminal transition time")
        history = self._terminal_history.setdefault(ev.mint, [])
        transition = (float(ev.t_wall), ev.to_state)
        if transition not in history:
            if len(history) >= 3:
                raise RuntimeError("terminal transition history exceeds lifecycle bound")
            history.append(transition)
            history.sort()
        if ev.to_state == "DEAD":
            self._prices[ev.mint] = 0.0
            self._terminal[ev.mint] = "dead"
        else:
            self._terminal[ev.mint] = "graduated"

    def _terminal_at_or_before(
        self,
        mint: str,
        cutoff: float,
    ) -> tuple[float, str] | None:
        history = self._terminal_history.get(mint, ())
        position = bisect_right(history, cutoff, key=lambda item: item[0])
        return history[position - 1] if position else None

    def register(self, ev: CandidateScored | CanonicalObservationStarted) -> None:
        if isinstance(ev, CandidateScored):
            ref_kind = "candidate"
            ref_id = ev.decision_id
            price0_at = ev.t_wall
            price0 = ev.spot_price_sol
            pending = set(self._horizons)
        else:
            ref_kind = "canonical_observation"
            ref_id = ev.observation_id
            price0_at = ev.price_observed_at
            price0 = ev.start_price_sol
        if any(
            c.ref_kind == ref_kind and c.ref_id == ref_id
            for c in self._candidates
        ):
            return
        if isinstance(ev, CanonicalObservationStarted):
            pending = {
                horizon
                for horizon in self._horizons
                if not store.canonical_outcome_exists(
                    self._conn,
                    observation_id=ev.observation_id,
                    horizon_s=horizon,
                )
            }
        if len(self._candidates) >= self._max_in_memory_pending_observations:
            return
        tracked_mints = self._registered_mints | self._price_history.keys()
        if (
            ev.mint not in tracked_mints
            and len(tracked_mints) >= self._price_history_max_mints
        ):
            return
        if pending and price0 and price0 > 0:
            self._candidates.append(_Candidate(
                ref_kind=ref_kind, ref_id=ref_id, mint=ev.mint, t0=ev.t_wall,
                price0_at=price0_at, price0=price0, pending=pending))
            self._registered_mints.add(ev.mint)

    def replay_journal(self, *, since_wall: float, until_wall: float) -> int:
        if self._journal is None:
            raise RuntimeError("journal replay unavailable")
        replayed = 0
        for item in self._journal.iter_events(
            since_wall=since_wall,
            until_wall=until_wall,
        ):
            if isinstance(item, JournalReplayGap):
                if item.mint is not None and item.mint not in self._registered_mints:
                    continue
                mints = (
                    self._registered_mints
                    if item.mint is None
                    else (item.mint,)
                )
                for mint in mints:
                    interval = self._price_overflow_intervals.get(mint)
                    if interval is None:
                        self._price_overflow_intervals[mint] = (
                            item.lower_wall,
                            item.upper_wall,
                        )
                    else:
                        self._price_overflow_intervals[mint] = (
                            min(interval[0], item.lower_wall),
                            max(interval[1], item.upper_wall),
                        )
                continue
            event = event_from_dict(item)
            if not isinstance(event, (CurveProgress, LifecycleTransition)):
                continue
            if event.mint not in self._registered_mints:
                continue
            if isinstance(event, CurveProgress):
                self.observe_price(event)
            else:
                self.on_transition(event)
            replayed += 1
        return replayed

    def _select_horizon_price(
        self,
        cand: _Candidate,
        *,
        cutoff: float,
        recovered: tuple[float, float] | None,
    ) -> float:
        sample = self._select_horizon_sample(
            cand,
            cutoff=cutoff,
            recovered=recovered,
        )
        return cand.price0 if sample is None else sample[1]

    def _select_horizon_sample(
        self,
        cand: _Candidate,
        *,
        cutoff: float,
        recovered: tuple[float, float] | None,
    ) -> tuple[float, float] | None:
        sample = (
            (cand.price0_at, cand.price0)
            if cand.price0_at <= cutoff
            else None
        )
        history = self._price_history.get(cand.mint, ())
        position = bisect_right(
            history,
            cutoff,
            key=lambda item: item[0],
        )
        if position and (
            sample is None or history[position - 1][0] >= sample[0]
        ):
            sample = history[position - 1]
        if recovered is not None and (
            sample is None or recovered[0] >= sample[0]
        ):
            sample = recovered
        return sample

    def check(
        self,
        now: float,
        *,
        ref_keys: set[tuple[str, int]] | None = None,
    ) -> int:
        self._prune_price_history(now)
        wrote = 0
        due_cutoffs: dict[str, set[float]] = {}
        due_horizon_cutoffs: dict[str, set[float]] = {}
        for cand in self._candidates:
            if (
                ref_keys is not None
                and (cand.ref_kind, cand.ref_id) not in ref_keys
            ):
                continue
            for horizon in cand.pending:
                if now - cand.t0 >= horizon:
                    cutoff = cand.t0 + horizon
                    due_cutoffs.setdefault(cand.mint, set()).add(cutoff)
                    due_horizon_cutoffs.setdefault(cand.mint, set()).add(cutoff)
                    due_cutoffs[cand.mint].update(
                        transition_at
                        for transition_at, _ in self._terminal_history.get(
                            cand.mint,
                            (),
                        )
                        if transition_at <= cutoff
                    )
        replayed_prices: dict[str, dict[float, tuple[float, float]]] = {}
        replayed_gap_cutoffs: dict[str, set[float]] = {}
        for cand in self._candidates:
            if (
                ref_keys is not None
                and (cand.ref_kind, cand.ref_id) not in ref_keys
            ):
                continue
            if not cand.pending:
                continue
            due = [h for h in sorted(cand.pending) if now - cand.t0 >= h]
            if not due:
                continue
            if cand.mint not in replayed_prices:
                cutoffs = sorted(due_cutoffs[cand.mint])
                best_at_start: list[tuple[float, float] | None] = [
                    None for _ in cutoffs
                ]
                recovered: dict[float, tuple[float, float]] = {}
                horizon_cutoffs = sorted(due_horizon_cutoffs[cand.mint])
                gap_cutoffs: set[float] = set()
                for evidence in self._iter_replayed_overflow_evidence(
                    cand.mint,
                ):
                    if isinstance(evidence, JournalReplayGap):
                        start = bisect_left(
                            horizon_cutoffs,
                            evidence.lower_wall,
                        )
                        stop = bisect_right(
                            horizon_cutoffs,
                            evidence.upper_wall + self._stale_after_s,
                        )
                        gap_cutoffs.update(horizon_cutoffs[start:stop])
                        continue
                    sample_at, sample_price = evidence
                    start = bisect_left(cutoffs, sample_at)
                    if start == len(cutoffs):
                        continue
                    previous = best_at_start[start]
                    if previous is None or sample_at >= previous[0]:
                        best_at_start[start] = (sample_at, sample_price)
                latest = None
                for cutoff, sample in zip(cutoffs, best_at_start, strict=True):
                    if sample is not None and (
                        latest is None or sample[0] >= latest[0]
                    ):
                        latest = sample
                    if latest is not None:
                        recovered[cutoff] = latest
                replayed_prices[cand.mint] = recovered
                replayed_gap_cutoffs[cand.mint] = gap_cutoffs
            for h in due:
                cutoff = cand.t0 + h
                transition = self._terminal_at_or_before(cand.mint, cutoff)
                unavailable_reason = ""
                if cutoff in replayed_gap_cutoffs[cand.mint]:
                    if cand.ref_kind != "canonical_observation":
                        raise RuntimeError(
                            "journal overflow evidence unavailable",
                        )
                    price_now = None
                    price_now_at = None
                    terminal = None
                    ret_pct = None
                    unavailable_reason = "journal_replay_gap"
                elif transition is not None and transition[1] == "DEAD":
                    price_now = 0.0
                    price_now_at = transition[0]
                    terminal = "DEAD"
                    ret_pct = -100.0
                elif transition is not None:
                    recovered = replayed_prices[cand.mint].get(transition[0])
                    sample = self._select_horizon_sample(
                        cand,
                        cutoff=transition[0],
                        recovered=recovered,
                    )
                    terminal = "GRADUATED"
                    if sample is None:
                        price_now = None
                        price_now_at = None
                        ret_pct = None
                        unavailable_reason = "graduated_no_price"
                    else:
                        price_now_at, price_now = sample
                        ret_pct = 100.0 * (
                            price_now - cand.price0
                        ) / cand.price0
                else:
                    recovered = replayed_prices[cand.mint].get(cutoff)
                    sample = self._select_horizon_sample(
                        cand,
                        cutoff=cutoff,
                        recovered=recovered,
                    )
                    if (
                        sample is None
                        or sample[0] < cutoff - self._stale_after_s
                    ):
                        price_now = 0.0
                        price_now_at = cutoff
                        ret_pct = -100.0
                        terminal = "STALE"
                    else:
                        price_now_at, price_now = sample
                        ret_pct = 100.0 * (
                            price_now - cand.price0
                        ) / cand.price0
                        terminal = None
                if cand.ref_kind == "canonical_observation":
                    store.record_canonical_observation_outcome(
                        self._conn,
                        raw_wall=now,
                        observation_id=cand.ref_id,
                        horizon_s=h,
                        forward_return_pct=ret_pct,
                        price0=cand.price0,
                        price0_observed_at=cand.price0_at,
                        price_now=price_now,
                        price_now_observed_at=price_now_at,
                        terminal=terminal,
                        unavailable_reason=unavailable_reason,
                    )
                else:
                    record_outcome(
                        self._conn,
                        at=now,
                        ref_kind=cand.ref_kind,
                        ref_id=cand.ref_id,
                        pnl_sol=0.0,
                        detail={
                            "horizon_s": h,
                            "forward_return_pct": ret_pct,
                            "price0": cand.price0,
                            "price_now": price_now,
                            "terminal": (
                                terminal.lower()
                                if terminal is not None
                                else None
                            ),
                        },
                    )
                cand.pending.discard(h)
                wrote += 1
        self._candidates = [c for c in self._candidates if c.pending]
        self._release_completed_mints()
        return wrote

    def resume_from_ledger(self, conn) -> int:
        """I3: after a restart, in-flight candidates only lived in this process's
        in-memory `_candidates` list -- a redeploy silently dropped forward-return
        tracking for every decision still mid-horizon. Rebuilds `_candidates` from
        the ledger, skipping (decision_id, horizon) pairs already written so a
        resume never re-records a horizon that's already in the outcomes table.
        """
        already_written: set[tuple[int, float]] = set()
        for row in conn.execute(
                "SELECT ref_id, detail_json FROM outcomes WHERE ref_kind='candidate'"):
            detail = json.loads(row["detail_json"])
            already_written.add((row["ref_id"], detail["horizon_s"]))

        registered = 0
        for row in list_decisions_for_counterfactual(conn):
            feature_vector = json.loads(row["feature_vector_json"])
            price0 = feature_vector.get("spot_price_sol")
            if not price0 or price0 <= 0:
                continue
            pending = {h for h in self._horizons if (row["id"], h) not in already_written}
            if not pending:
                continue
            if len(self._candidates) >= self._max_in_memory_pending_observations:
                continue
            tracked_mints = self._registered_mints | self._price_history.keys()
            if (
                row["mint"] not in tracked_mints
                and len(tracked_mints) >= self._price_history_max_mints
            ):
                continue
            self._candidates.append(_Candidate(
                ref_kind="candidate", ref_id=row["id"], mint=row["mint"], t0=row["at"],
                price0_at=row["at"], price0=price0, pending=pending))
            self._registered_mints.add(row["mint"])
            registered += 1
        log.info("counterfactual tracker resumed from ledger",
                 extra={"extra_fields": {"candidates_reregistered": registered}})
        self.reconcile_from_ledger(now=self._clock())
        return registered

    def reconcile_from_ledger(self, *, now: float) -> int:
        rows = store.list_pending_canonical_observations(
            self._conn,
            horizons=self._horizons,
            limit_plus_one=self._max_in_memory_pending_observations + 1,
        )
        existing = {
            (candidate.ref_kind, candidate.ref_id)
            for candidate in self._candidates
        }
        registered = 0
        earliest_t0 = None
        registered_keys: set[tuple[str, int]] = set()
        for row in rows:
            key = ("canonical_observation", row["id"])
            if key in existing:
                continue
            if len(self._candidates) >= self._max_in_memory_pending_observations:
                break
            tracked_mints = self._registered_mints | self._price_history.keys()
            if (
                row["mint"] not in tracked_mints
                and len(tracked_mints) >= self._price_history_max_mints
            ):
                continue
            pending = {
                horizon
                for horizon in self._horizons
                if not store.canonical_outcome_exists(
                    self._conn,
                    observation_id=row["id"],
                    horizon_s=horizon,
                )
            }
            if not pending:
                continue
            self._candidates.append(_Candidate(
                ref_kind="canonical_observation",
                ref_id=row["id"],
                mint=row["mint"],
                t0=row["observed_at"],
                price0_at=row["price_observed_at"],
                price0=row["start_price_sol"],
                pending=pending,
            ))
            existing.add(key)
            registered_keys.add(key)
            self._registered_mints.add(row["mint"])
            earliest_t0 = (
                row["observed_at"]
                if earliest_t0 is None
                else min(earliest_t0, row["observed_at"])
            )
            registered += 1
        if earliest_t0 is not None:
            self.replay_journal(
                since_wall=earliest_t0,
                until_wall=max(now, earliest_t0),
            )
            self.check(now, ref_keys=registered_keys)
        return registered

    async def run(self, stop: asyncio.Event) -> None:
        next_reconcile_at = self._clock()
        try:
            while not stop.is_set():
                now = self._clock()
                if now >= next_reconcile_at:
                    self.reconcile_from_ledger(now=now)
                    next_reconcile_at = now + self._reconcile_interval_s
                try:
                    ev = await asyncio.wait_for(
                        self._q.get(),
                        timeout=min(1.0, max(0.0, next_reconcile_at - now)),
                    )
                except TimeoutError:
                    self.check(self._clock())
                    continue
                if isinstance(ev, (CandidateScored, CanonicalObservationStarted)):
                    self.register(ev)
                elif isinstance(ev, CurveProgress):
                    self.observe_price(ev)
                elif isinstance(ev, LifecycleTransition):
                    self.on_transition(ev)
                self.check(self._clock())
        finally:
            self._bus.unsubscribe(self._q)
