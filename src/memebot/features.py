"""FeatureEngine (spec §5.4): per-mint bonding-curve reserve time-series -> CLIMBING
feature vector. Deterministic, no network. Velocity is the time-based proxy (spec §6):
SOL-locked gained per second over the buffered window."""
from __future__ import annotations

import asyncio
import logging
import math
from collections import OrderedDict, deque
from dataclasses import dataclass

from memebot.events import CurveProgress, LifecycleTransition
from memebot.ingest.curve import CurveState, LAMPORTS_PER_SOL, spot_price_sol_per_token

log = logging.getLogger("memebot.features")

TERMINAL_STATES = ("DEAD", "GRADUATED")
DEFAULT_MAX_MINTS = 4_096
MAX_UNIX_TS = 4_102_444_800.0
MAX_PRICE_SOL = 1e100


def _wall_is_orderable(value: object) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def curve_progress_is_finite(ev: CurveProgress) -> bool:
    """Reject malformed numeric snapshots before any feature or trading state changes."""
    try:
        return all(math.isfinite(value) for value in (
            ev.t_wall,
            ev.t_mono,
            ev.progress_pct,
            ev.virtual_sol_reserves,
            ev.virtual_token_reserves,
            ev.real_sol_reserves,
            ev.real_token_reserves,
        ))
    except (TypeError, ValueError, OverflowError):
        return False


@dataclass(frozen=True, slots=True)
class ClimbingFeatures:
    velocity_sol_per_s: float
    curve_progress_pct: float
    age_s: float
    risk_score: float
    spot_price_sol: float
    samples: int
    smart_money_count: int = 0
    smart_money_pnl_sol: float = 0.0


@dataclass(frozen=True, slots=True)
class CurveSnapshot:
    source_boot_id: int
    source_seq: int
    t_wall: float
    t_mono: float
    virtual_sol_reserves: int
    virtual_token_reserves: int
    real_sol_reserves: int
    real_token_reserves: int
    liquidity_sol: float
    spot_price_sol: float
    progress_pct: float

    def curve_state(self) -> CurveState:
        return CurveState(
            virtual_token_reserves=self.virtual_token_reserves,
            virtual_sol_reserves=self.virtual_sol_reserves,
            real_token_reserves=self.real_token_reserves,
            real_sol_reserves=self.real_sol_reserves,
            token_total_supply=0,
            complete=self.progress_pct >= 100.0,
        )


class FeatureEngine:
    def __init__(self, bus, *, maxlen: int = 64, max_mints: int = DEFAULT_MAX_MINTS,
                 max_terminal_mints: int = DEFAULT_MAX_MINTS,
                 max_feature_mints: int) -> None:
        if max_mints <= 0:
            raise ValueError("max_mints must be positive")
        if type(max_feature_mints) is not int or max_feature_mints <= 0:
            raise ValueError("max_feature_mints must be positive")
        if max_terminal_mints <= 0:
            raise ValueError("max_terminal_mints must be positive")
        self._bus = bus
        self._maxlen = maxlen
        self._max_mints = max_feature_mints
        self._max_feature_mints = max_feature_mints
        self._max_terminal_mints = max_terminal_mints
        self._series: dict[
            str, deque[tuple[float, float, float, float, CurveState, float]]
        ] = {}
        self._snapshot_series: dict[str, deque[tuple[CurveProgress, int]]] = {}
        self._arrival_seq = 0
        # each sample: (t_mono, sol_locked, spot_price, progress_pct, curve_state, t_wall)
        self._states: dict[str, CurveState] = {}
        self._active_mints: OrderedDict[str, None] = OrderedDict()
        # Exact lifetime terminal membership cannot be both bounded and in-memory exact.
        # This LRU suppresses retained terminals; after eviction, this feature cache may
        # accept a late curve. Feature eviction/cache is not lifecycle authority: Strategy
        # rechecks durable token state and the exact latest safety-report id before acting.
        self._terminal_mints: OrderedDict[str, None] = OrderedDict()
        if bus is not None:
            self._q = bus.subscribe(CurveProgress, LifecycleTransition)

    @staticmethod
    def _sample(
        ev: CurveProgress,
    ) -> tuple[float, float, float, float, CurveState, float]:
        sol_locked = ev.real_sol_reserves / LAMPORTS_PER_SOL
        state = CurveState(virtual_token_reserves=ev.virtual_token_reserves,
                           virtual_sol_reserves=ev.virtual_sol_reserves,
                           real_token_reserves=ev.real_token_reserves,
                           real_sol_reserves=ev.real_sol_reserves,
                           token_total_supply=0, complete=False)
        spot = spot_price_sol_per_token(state) if ev.virtual_token_reserves else 0.0
        return ev.t_mono, sol_locked, spot, ev.progress_pct, state, ev.t_wall

    @staticmethod
    def _snapshot(ev: CurveProgress) -> CurveSnapshot | None:
        if type(ev.t_wall) not in (int, float) or type(ev.t_mono) not in (int, float):
            return None
        try:
            timestamps_valid = (
                math.isfinite(ev.t_wall)
                and 0.0 <= ev.t_wall <= MAX_UNIX_TS
                and math.isfinite(ev.t_mono)
            )
        except (OverflowError, TypeError, ValueError):
            return None
        if (not timestamps_valid
                or type(ev.source_boot_id) is not int
                or ev.source_boot_id < 0
                or type(ev.source_seq) is not int
                or ev.source_seq < 0
                or type(ev.progress_pct) not in (int, float)
                or not math.isfinite(ev.progress_pct)
                or not 0.0 <= ev.progress_pct <= 100.0):
            return None
        reserves = (
            ev.virtual_sol_reserves,
            ev.virtual_token_reserves,
            ev.real_sol_reserves,
            ev.real_token_reserves,
        )
        if (any(type(value) is not int or value < 0 for value in reserves)
                or ev.virtual_sol_reserves == 0
                or ev.virtual_token_reserves == 0):
            return None
        state = CurveState(
            virtual_token_reserves=ev.virtual_token_reserves,
            virtual_sol_reserves=ev.virtual_sol_reserves,
            real_token_reserves=ev.real_token_reserves,
            real_sol_reserves=ev.real_sol_reserves,
            token_total_supply=0,
            complete=ev.progress_pct >= 100.0,
        )
        try:
            liquidity = ev.real_sol_reserves / LAMPORTS_PER_SOL
            spot = spot_price_sol_per_token(state)
        except (OverflowError, TypeError, ValueError, ZeroDivisionError):
            return None
        if (not math.isfinite(liquidity) or not 0.0 <= liquidity <= MAX_PRICE_SOL
                or not math.isfinite(spot) or not 0.0 < spot <= MAX_PRICE_SOL):
            return None
        return CurveSnapshot(
            source_boot_id=ev.source_boot_id,
            source_seq=ev.source_seq,
            t_wall=float(ev.t_wall),
            t_mono=float(ev.t_mono),
            virtual_sol_reserves=ev.virtual_sol_reserves,
            virtual_token_reserves=ev.virtual_token_reserves,
            real_sol_reserves=ev.real_sol_reserves,
            real_token_reserves=ev.real_token_reserves,
            liquidity_sol=liquidity,
            spot_price_sol=spot,
            progress_pct=float(ev.progress_pct),
        )

    def observe(self, ev: CurveProgress) -> None:
        if ev.mint in self._terminal_mints:
            return
        if ev.mint not in self._active_mints:
            if len(self._active_mints) >= self._max_feature_mints:
                self._drop_active(next(iter(self._active_mints)))
        else:
            self._active_mints.pop(ev.mint)
        self._active_mints[ev.mint] = None
        self._arrival_seq += 1
        history = self._snapshot_series.setdefault(
            ev.mint, deque(maxlen=self._maxlen),
        )
        history.append((ev, self._arrival_seq))
        if self._snapshot(ev) is None:
            return
        buf = self._series.get(ev.mint)
        if buf and ev.t_mono <= buf[-1][0]:
            return
        sample = self._sample(ev)
        if buf is None:
            buf = deque(maxlen=self._maxlen)
            self._series[ev.mint] = buf
        buf.append(sample)
        self._states[ev.mint] = sample[4]

    def _drop_active(self, mint: str) -> None:
        # Feature retention is intentionally isolated: strategy/broker position state and
        # the append-only decision/trade ledger have separate owners and are never evicted
        # merely because feature history reaches this local memory cap.
        self._series.pop(mint, None)
        self._states.pop(mint, None)
        self._snapshot_series.pop(mint, None)
        self._active_mints.pop(mint, None)

    def on_transition(self, ev: LifecycleTransition) -> None:
        if ev.to_state in TERMINAL_STATES:
            # Drop active feature state before recording terminality so no late curve can
            # observe a half-cleaned mint.
            self._drop_active(ev.mint)
            self._terminal_mints.pop(ev.mint, None)
            self._terminal_mints[ev.mint] = None
            if len(self._terminal_mints) > self._max_terminal_mints:
                self._terminal_mints.popitem(last=False)

    def latest_price(self, mint: str) -> float | None:
        buf = self._series.get(mint)
        return buf[-1][2] if buf else None

    def latest_state(self, mint: str) -> CurveState | None:
        return self._states.get(mint)

    def snapshot_at_or_before(self, mint: str, *, as_of: float) -> CurveSnapshot | None:
        if type(as_of) not in (int, float):
            return None
        try:
            as_of_valid = math.isfinite(as_of) and 0.0 <= as_of <= MAX_UNIX_TS
        except (OverflowError, TypeError, ValueError):
            return None
        if not as_of_valid:
            return None
        relevant: list[tuple[CurveSnapshot, int]] = []
        for event, arrival_seq in self._snapshot_series.get(mint, ()):
            if not _wall_is_orderable(event.t_wall):
                return None
            if event.t_wall > as_of:
                continue
            snapshot = self._snapshot(event)
            if snapshot is None:
                return None
            relevant.append((snapshot, arrival_seq))
        payload_by_time: dict[tuple[float, float], CurveSnapshot] = {}
        for snapshot, _ in relevant:
            key = (snapshot.t_wall, snapshot.t_mono)
            previous = payload_by_time.setdefault(key, snapshot)
            if previous != snapshot:
                return None
        if not relevant:
            return None
        return max(
            relevant,
            key=lambda item: (item[0].t_wall, item[0].t_mono, item[1]),
        )[0]

    def p3_snapshot_at_or_before(
        self,
        mint: str,
        *,
        as_of: float,
        durable_source_wall: float | None,
        durable_source_boot_id: int | None,
        durable_source_seq: int | None,
        durable_observed_at: float | None,
        runtime_boot_id: int,
        runtime_causal_floor: float,
    ) -> CurveSnapshot | None:
        if type(mint) is not str or not mint.strip():
            return None
        timestamps = (
            as_of,
            durable_source_wall,
            durable_observed_at,
            runtime_causal_floor,
        )
        if any(type(value) not in (int, float) for value in timestamps):
            return None
        try:
            if not all(
                math.isfinite(value) and 0.0 <= value <= MAX_UNIX_TS
                for value in timestamps
            ):
                return None
        except (OverflowError, TypeError, ValueError):
            return None
        if (
            type(runtime_boot_id) is not int
            or runtime_boot_id <= 0
            or type(durable_source_boot_id) is not int
            or durable_source_boot_id != runtime_boot_id
            or type(durable_source_seq) is not int
            or durable_source_seq <= 0
            or not durable_source_wall < durable_observed_at
            or not runtime_causal_floor < durable_observed_at <= as_of
        ):
            return None
        if self.snapshot_at_or_before(mint, as_of=as_of) is None:
            return None
        exact_matches: list[CurveSnapshot] = []
        for event, _ in self._snapshot_series.get(mint, ()):
            if type(event.source_boot_id) is not int:
                continue
            if event.source_boot_id != runtime_boot_id:
                continue
            if type(event.source_seq) is not int or event.source_seq <= 0:
                return None
            if event.source_seq > durable_source_seq:
                return None
            if (
                event.source_seq == durable_source_seq
                and event.t_wall == durable_source_wall
            ):
                snapshot = self._snapshot(event)
                if snapshot is None:
                    return None
                exact_matches.append(snapshot)
        if not exact_matches or any(
            snapshot != exact_matches[0] for snapshot in exact_matches[1:]
        ):
            return None
        return exact_matches[0]

    def state_as_of(self, mint: str, *, t_mono: float) -> CurveState | None:
        """Return the newest buffered curve state causally available at ``t_mono``."""
        if not math.isfinite(t_mono):
            return None
        return next(
            (sample[4] for sample in reversed(self._series.get(mint, ()))
             if sample[0] <= t_mono),
            None,
        )

    def features(
        self,
        mint: str,
        *,
        as_of: float,
        identity_ingested_at: float,
        risk_score: float,
        min_samples: int,
        max_latest_age_s: float,
    ) -> ClimbingFeatures | None:
        buf = self._series.get(mint)
        fresh_values = (as_of, identity_ingested_at, max_latest_age_s)
        if any(type(value) not in (int, float) for value in fresh_values):
            return None
        try:
            valid = (
                math.isfinite(as_of)
                and 0.0 <= as_of <= MAX_UNIX_TS
                and math.isfinite(identity_ingested_at)
                and 0.0 <= identity_ingested_at <= as_of
                and math.isfinite(max_latest_age_s)
                and max_latest_age_s > 0.0
            )
        except (OverflowError, TypeError, ValueError):
            return None
        if not valid:
            return None
        if self.snapshot_at_or_before(mint, as_of=as_of) is None:
            return None
        samples = [sample for sample in buf or () if sample[5] <= as_of]
        if (
            not samples
            or as_of - samples[-1][5] > max_latest_age_s
        ):
            return None
        return self._features_from_samples(
            samples, now=as_of, created_at=identity_ingested_at,
            risk_score=risk_score,
            min_samples=min_samples,
        )

    def features_as_of(self, mint: str, *, t_mono: float, now: float,
                       created_at: float, risk_score: float,
                       min_samples: int) -> ClimbingFeatures | None:
        """Evaluate only samples causally available at ``t_mono``."""
        if not math.isfinite(t_mono):
            return None
        samples = [sample for sample in self._series.get(mint, ())
                   if sample[0] <= t_mono]
        return self._features_from_samples(
            samples, now=now, created_at=created_at, risk_score=risk_score,
            min_samples=min_samples,
        )

    def features_including(self, ev: CurveProgress, *, now: float, created_at: float,
                           risk_score: float, min_samples: int) -> ClimbingFeatures | None:
        """Evaluate with ``ev`` exactly once, independent of consumer scheduling.

        Buffered samples at or after the event are excluded, then the event is appended
        exactly once. This prevents an ahead-of-strategy consumer from leaking future
        curve state into the decision while remaining independent of consumer scheduling.
        """
        if not curve_progress_is_finite(ev):
            return None
        if ev.mint in self._terminal_mints:
            return None
        samples = [sample for sample in self._series.get(ev.mint, ())
                   if sample[0] < ev.t_mono]
        samples.append(self._sample(ev))
        return self._features_from_samples(
            samples, now=now, created_at=created_at, risk_score=risk_score,
            min_samples=min_samples,
        )

    @staticmethod
    def _features_from_samples(samples, *, now: float, created_at: float,
                               risk_score: float,
                               min_samples: int) -> ClimbingFeatures | None:
        if len(samples) < min_samples:
            return None
        t0, sol0, _, _, _ = samples[0][:5]
        t1, sol1, spot1, prog1, _ = samples[-1][:5]
        dt = t1 - t0
        if dt <= 0:
            return None
        velocity = (sol1 - sol0) / dt
        age = now - created_at
        if not all(math.isfinite(value) for value in (
                velocity, prog1, age, risk_score, spot1)):
            return None
        return ClimbingFeatures(
            velocity_sol_per_s=velocity,
            curve_progress_pct=prog1,
            age_s=age,
            risk_score=risk_score,
            spot_price_sol=spot1,
            samples=len(samples))

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    ev = await asyncio.wait_for(self._q.get(), timeout=0.5)
                except TimeoutError:
                    continue
                if isinstance(ev, CurveProgress):
                    self.observe(ev)
                elif isinstance(ev, LifecycleTransition):
                    self.on_transition(ev)
        finally:
            self._bus.unsubscribe(self._q)
