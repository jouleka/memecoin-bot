"""ClimbingStrategy (spec §5.5): rug-passing CLIMBING candidates -> deterministic score ->
paper buy (curve-math fill) -> laddered/time/trailing exits. Records every SCORED candidate
as a decision (BUY or SKIP) so the counterfactual can measure whether the score predicts
profit. Entry honours the latency penalty (§5.6): decide now, fill against the next snapshot
>= T seconds later, using that event's own reserves (race-free vs the FeatureEngine). Positions
are in-memory; the append-only ledger is the source of truth and is reconciled on startup."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace

from memebot.events import (CandidateScored, CanonicalObservationStarted, CurveProgress,
                            LifecycleTransition, PaperEntry, PaperExit, SafetyHardFail,
                            SafetyPassed)
from memebot.features import CurveSnapshot, curve_progress_is_finite
from memebot.ingest.curve import CurveState, spot_price_sol_per_token
from memebot.store import (EvidenceIntegrityError, allocate_p3_causal_wall,
                           decision_exists, get_token,
                           latest_safety_report, p3_immediate_transaction,
                           pending_safety_passes_for_scoring,
                           record_canonical_paper_buy, record_canonical_paper_sell,
                           record_canonical_recheck, record_terminal_entry_execution,
                           record_decision, record_decision_with_canonical_observations,
                           record_outcome, record_paper_trade,
                           validated_early_buyer_for_report,
                           validated_latest_report_as_of)

log = logging.getLogger("memebot.strategy")
RECOVERY_SCAN_CAP = 64
_SCORE_FEATURE_KEYS = (
    "velocity_sol_per_s",
    "curve_progress_pct",
    "age_s",
    "risk_score",
    "spot_price_sol",
    "samples",
    "smart_money_count",
    "smart_money_pnl_sol",
)


class _StaleFillEvent(Exception):
    pass


def _state_from_event(ev: CurveProgress) -> CurveState:
    """Race-free fill/price state: build a CurveState from the event's own reserves rather
    than querying the FeatureEngine (a separate consumer whose buffer may lag this event)."""
    return CurveState(virtual_token_reserves=ev.virtual_token_reserves,
                      virtual_sol_reserves=ev.virtual_sol_reserves,
                      real_token_reserves=ev.real_token_reserves,
                      real_sol_reserves=ev.real_sol_reserves,
                      token_total_supply=0, complete=False)


def _snapshot_from_event(ev: CurveProgress) -> CurveSnapshot | None:
    """Build the exact resolver snapshot from the causative progress delivery."""
    if not curve_progress_is_finite(ev):
        return None
    if (
        type(ev.source_boot_id) is not int
        or ev.source_boot_id < 0
        or type(ev.source_seq) is not int
        or ev.source_seq < 0
        or type(ev.progress_pct) not in (int, float)
        or not 0.0 <= ev.progress_pct <= 100.0
    ):
        return None
    reserves = (
        ev.virtual_sol_reserves,
        ev.virtual_token_reserves,
        ev.real_sol_reserves,
        ev.real_token_reserves,
    )
    if (
        any(type(value) is not int or value < 0 for value in reserves)
        or ev.virtual_sol_reserves == 0
        or ev.virtual_token_reserves == 0
    ):
        return None
    state = _state_from_event(ev)
    try:
        liquidity = ev.real_sol_reserves / 1_000_000_000
        spot = spot_price_sol_per_token(state)
    except (OverflowError, TypeError, ValueError, ZeroDivisionError):
        return None
    if not all(math.isfinite(value) for value in (liquidity, spot)) or spot <= 0.0:
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


def _decision_snapshot(resolution, *, mint: str) -> CurveSnapshot:
    """Reload the immutable subject snapshot from the exact persisted payload shape."""
    inputs = resolution.verdict.ranking_inputs
    candidates = inputs.get("candidates") if isinstance(inputs, Mapping) else None
    if not isinstance(candidates, list):
        raise ValueError("canonical candidates are unavailable")
    matches = [candidate for candidate in candidates
               if isinstance(candidate, Mapping) and candidate.get("mint") == mint]
    if len(matches) != 1:
        raise ValueError("canonical subject snapshot is unavailable")
    raw = matches[0].get("raw")
    snapshot = raw.get("curve_snapshot") if isinstance(raw, Mapping) else None
    if not isinstance(snapshot, Mapping):
        raise ValueError("canonical subject snapshot is unavailable")
    return CurveSnapshot(
        source_boot_id=0,
        source_seq=0,
        t_wall=snapshot["t_wall"],
        t_mono=snapshot["t_mono"],
        virtual_sol_reserves=snapshot["virtual_sol_reserves"],
        virtual_token_reserves=snapshot["virtual_token_reserves"],
        real_sol_reserves=snapshot["real_sol_reserves"],
        real_token_reserves=snapshot["real_token_reserves"],
        liquidity_sol=raw["liquidity_sol"],
        spot_price_sol=snapshot["spot_price_sol"],
        progress_pct=raw["curve_progress_pct"],
    )


def _recheck_snapshot_payload(snapshot: CurveSnapshot) -> dict[str, object]:
    return {
        "t_wall": snapshot.t_wall,
        "t_mono": snapshot.t_mono,
        "virtual_sol_reserves": snapshot.virtual_sol_reserves,
        "virtual_token_reserves": snapshot.virtual_token_reserves,
        "real_sol_reserves": snapshot.real_sol_reserves,
        "real_token_reserves": snapshot.real_token_reserves,
        "liquidity_sol": snapshot.liquidity_sol,
        "spot_price_sol": snapshot.spot_price_sol,
        "progress_pct": snapshot.progress_pct,
    }


@dataclass
class Position:
    mint: str
    decision_id: int
    qty_remaining: float
    entry_price: float
    entry_at: float
    peak_price: float
    original_qty: float
    size_sol: float
    buy_notional: float = 0.0
    segment: str = "CLIMBING"
    is_p3: bool = False
    entry_latest_target_report_id: int | None = None
    ladder_hits: set = field(default_factory=set)


@dataclass
class PendingEntry:
    mint: str
    decision_id: int
    safety_report_id: int
    canonical_inputs_hash: str
    decision_snapshot: CurveSnapshot
    decision_at: float
    decision_mono: float
    size_sol: float
    score: float
    recheck_attempt: int = 1
    terminal_recheck_id: int | None = None
    terminal_reason: str | None = None


@dataclass(frozen=True, slots=True)
class P3EntryDraft:
    decision_id: int
    recheck_id: int
    raw_processed_at: float
    mint: str
    segment: str
    qty: float
    quote_price: float
    fill_price: float
    fees: tuple[tuple[str, float], ...]
    realism_grade: str
    planned_size_sol: float


@dataclass(frozen=True, slots=True)
class P3ExitDraft:
    decision_id: int
    raw_processed_at: float
    mint: str
    segment: str
    qty: float
    quote_price: float
    fill_price: float
    fees: tuple[tuple[str, float], ...]
    realism_grade: str
    exit_reason: str
    ladder_index: int | None


@dataclass(frozen=True, slots=True)
class PendingScore:
    safety_passed: SafetyPassed
    registered_at: float


class ClimbingStrategy:
    def __init__(self, bus, conn, *, feature_engine, scorer, broker,
                 canonical_resolver=None, strat_cfg, pumpfun_cfg, config_hash, fill_cfg,
                 exits_cfg=None, clock=time.time, mono_clock=time.monotonic,
                 stale_price_after_s: float = 300.0, smart_money_cfg: dict | None = None,
                 pending_score_capacity: int = 300,
                 curve_order_capacity: int | None = None,
                 fill_event_max_age_s: float = 30.0,
                 max_open_p3_positions: int = 100) -> None:
        if canonical_resolver is None and strat_cfg.get("entries_enabled") is not False:
            raise ValueError("canonical_resolver is required when entries are enabled")
        self._bus = bus
        self._conn = conn
        self._fe = feature_engine
        self._scorer = scorer
        self._broker = broker
        self._canonical_resolver = canonical_resolver
        self._cfg = strat_cfg
        self._pump = pumpfun_cfg
        self._fill_cfg = fill_cfg
        self._exits = exits_cfg or {}
        self._config_hash = config_hash
        self._clock = clock
        self._mono_clock = mono_clock
        if (
            type(fill_event_max_age_s) not in (int, float)
            or not math.isfinite(fill_event_max_age_s)
            or fill_event_max_age_s <= 0.0
        ):
            raise ValueError("fill_event_max_age_s must be finite and positive")
        if type(max_open_p3_positions) is not int or max_open_p3_positions <= 0:
            raise ValueError("max_open_p3_positions must be a positive integer")
        self._fill_event_max_age_s = float(fill_event_max_age_s)
        self._max_open_p3_positions = max_open_p3_positions
        self._smart_money_cfg = smart_money_cfg or {}
        self._pending_score_capacity = max(1, int(pending_score_capacity))
        self._recovery_scan_cap = RECOVERY_SCAN_CAP
        self._recovery_before_id: int | None = None
        self._recovery_started = False
        self._recovery_exhausted = False
        self._curve_order_capacity = max(1, int(
            self._pending_score_capacity if curve_order_capacity is None
            else curve_order_capacity
        ))
        self.positions: dict[str, Position] = {}
        self._restored_p3_latest_reports: dict[int, object] = {}
        self._pending_score: dict[str, PendingScore] = {}
        self._pending: dict[str, PendingEntry] = {}
        self._entry_times: list[float] = []
        # The strategy's OWN last-known curve state per mint, independent of the
        # FeatureEngine (which evicts on terminal transitions). Populated on every fill
        # and every price tick; consumed for the graduated force-close and the safety-flip
        # sell so a state the FeatureEngine already dropped is still available here.
        self._last_state: dict[str, CurveState] = {}
        # Wall-clock timestamp of each mint's last curve update, keyed alongside
        # _last_state. Drives sweep_stale's off-poll age-out (N2, strategy side): a mint
        # that stops producing fresh CurveProgress (fell off the top-max_tracked poll set)
        # is, per the base rate, dead -- without this a pending entry or open position in
        # such a mint leaks forever.
        self._last_state_ts: dict[str, float] = {}
        # Consumer-local monotonic order. It must not consult FeatureEngine because that
        # independent subscriber may legitimately be ahead or behind this one.
        self._curve_order: dict[str, float] = {}
        self._stale_after_s = stale_price_after_s
        if bus is not None:
            self._q = bus.subscribe(SafetyPassed, CurveProgress, SafetyHardFail,
                                    LifecycleTransition, critical=True)

    def reconcile(
        self, *, runtime_causal_floor: float | None = None,
        max_open_positions: int | None = None,
    ) -> int | tuple[int, ...]:
        """Rebuild in-memory positions from the append-only ledger after a restart. Restores
        the TRUE original_qty (sum of buys, not the shrunken remaining), buy_notional (full
        SOL cost incl. buy fees), and ladder_hits (which rungs already fired) -- without
        these, a restarted partially-laddered position double-ladders (I4: an already-hit
        rung fires again) and mis-sizes/mis-prices its exits."""
        from memebot.store import (list_open_p3_filled_positions,
                                   open_positions_from_ledger)
        restored = 0
        p3_decisions: set[int] = set()
        configured_ladders = len(self._exits.get("ladder_fractions") or [])
        restored_p3_ids: list[int] = []
        open_limit = (
            self._max_open_p3_positions
            if max_open_positions is None else max_open_positions
        )
        for p in list_open_p3_filled_positions(
            self._conn, max_open_positions=open_limit,
        ):
            if p.ladder_mask >> configured_ladders:
                raise ValueError("open P3 position has an unknown ladder bit")
            if p.qty_remaining <= 0.0 or p.mint in self.positions:
                raise ValueError("invalid duplicate open P3 position")
            if runtime_causal_floor is not None:
                latest = validated_latest_report_as_of(
                    self._conn, mint=p.mint, as_of=runtime_causal_floor,
                )
                if (
                    latest is None
                    or latest.holder_evidence_id is None
                    or latest.holder is None
                    or validated_early_buyer_for_report(
                        self._conn,
                        report_id=latest.safety_report_id,
                        expected_mint=p.mint,
                        as_of=runtime_causal_floor,
                    ) is None
                ):
                    raise EvidenceIntegrityError(
                        "restored P3 latest safety evidence is unavailable"
                    )
                self._restored_p3_latest_reports[p.decision_id] = latest
            ladder_hits = {
                index for index in range(configured_ladders)
                if p.ladder_mask & (1 << index)
            }
            self.positions[p.mint] = Position(
                mint=p.mint,
                decision_id=p.decision_id,
                qty_remaining=p.qty_remaining,
                entry_price=p.entry_price,
                entry_at=p.entry_at,
                peak_price=p.entry_price,
                original_qty=p.bought_qty,
                size_sol=self._cfg["position_size_sol"],
                buy_notional=p.buy_notional_sol,
                is_p3=True,
                entry_latest_target_report_id=p.entry_latest_target_report_id,
                ladder_hits=ladder_hits,
            )
            p3_decisions.add(p.decision_id)
            restored_p3_ids.append(p.decision_id)
            restored += 1
        for p in open_positions_from_ledger(self._conn, legacy_only=True):
            if p["decision_id"] in p3_decisions:
                continue
            if p["mint"] in self.positions:
                raise ValueError("duplicate open position mint")
            trades = self._conn.execute(
                "SELECT side, qty, fill_price, fees_json "
                "FROM paper_trades"
                " WHERE decision_id = ? ORDER BY at ASC", (p["decision_id"],)).fetchall()
            original_qty = sum(r["qty"] for r in trades if r["side"] == "buy")
            buy_notional = sum(
                r["qty"] * r["fill_price"] + sum(json.loads(r["fees_json"]).values())
                for r in trades if r["side"] == "buy")
            ladder_hits: set = set()
            fractions = self._exits.get("ladder_fractions") or []
            if fractions:
                for r in trades:
                    if r["side"] != "sell":
                        continue
                    for i, frac in enumerate(fractions):
                        if i in ladder_hits:
                            continue
                        expected = original_qty * frac
                        if abs(r["qty"] - expected) <= 1e-6 * max(1.0, original_qty):
                            ladder_hits.add(i)
                            break
            self.positions[p["mint"]] = Position(
                mint=p["mint"], decision_id=p["decision_id"], qty_remaining=p["qty_remaining"],
                entry_price=p["entry_price"], entry_at=p["entry_at"], peak_price=p["entry_price"],
                original_qty=original_qty, size_sol=self._cfg["position_size_sol"],
                buy_notional=buy_notional,
                is_p3=False,
                ladder_hits=ladder_hits)
            restored += 1
        if restored:
            log.info("reconciled open positions from ledger",
                     extra={"extra_fields": {"count": restored}})
        if runtime_causal_floor is not None or max_open_positions is not None:
            return tuple(restored_p3_ids)
        return restored

    def zero_close_restored_p3_position(
        self, *, decision_id: int, latest_report_id: int, raw_wall: float,
    ) -> tuple[int, int]:
        latest = self._restored_p3_latest_reports.get(decision_id)
        position = next(
            (
                candidate for candidate in self.positions.values()
                if candidate.decision_id == decision_id and candidate.is_p3
            ),
            None,
        )
        if (
            position is None
            or latest is None
            or latest.safety_report_id != latest_report_id
            or not latest.hard_fails
            or position.entry_latest_target_report_id is None
            or latest_report_id <= position.entry_latest_target_report_id
        ):
            raise EvidenceIntegrityError(
                "restored P3 hard-fail close proof is invalid"
            )
        sell_id, outcome_id = record_canonical_paper_sell(
            self._conn,
            decision_id=decision_id,
            raw_wall=raw_wall,
            mint=position.mint,
            segment=position.segment,
            qty=position.qty_remaining,
            quote_price=0.0,
            fill_price=0.0,
            fees={},
            realism_grade="F",
            exit_reason="restart_safety_hard_fail",
            ladder_index=None,
        )
        if outcome_id is None:
            raise EvidenceIntegrityError("restored P3 zero close was not terminal")
        self.positions.pop(position.mint, None)
        self._restored_p3_latest_reports.pop(decision_id, None)
        return sell_id, outcome_id

    def recover_pending_scores(self) -> int:
        """Recover one bounded raw page from existing append-only v5 evidence.

        No pending row is invented: the latest empty-hard-fail report is the durable
        compatibility seam. Its original checked_at remains the expiry origin, so a
        restart cannot reset TTL and revive old candidates indefinitely.
        """
        if self._recovery_exhausted:
            return 0
        now = self._clock()
        recovered = 0
        page = pending_safety_passes_for_scoring(
            self._conn, limit=self._recovery_scan_cap,
            scan_cap=self._recovery_scan_cap,
            before_id=self._recovery_before_id if self._recovery_started else None,
            now=now, stale_after_s=self._stale_after_s,
        )
        self._recovery_started = True
        self._recovery_before_id = page.next_before_id
        self._recovery_exhausted = page.exhausted
        for row in page.rows:
            if now - row["checked_at"] > self._stale_after_s:
                continue
            event = self._current_safety_pass(SafetyPassed(
                t_wall=row["checked_at"], t_mono=0.0, mint=row["mint"],
                segment="CLIMBING", safety_report_id=row["id"],
                risk_score=row["risk_score"],
            ))
            if (event is None
                    or decision_exists(self._conn, mint=row["mint"], segment="CLIMBING")
                    or row["mint"] in self.positions or row["mint"] in self._pending
                    or row["mint"] in self._pending_score):
                continue
            self._enqueue_pending_score(
                event,
                registered_at=row["checked_at"],
            )
            recovered += 1
        if recovered:
            log.info("recovered unscored candidates from safety evidence",
                     extra={"extra_fields": {"count": recovered}})
        return recovered

    @property
    def recovery_pending(self) -> bool:
        return self._recovery_started and not self._recovery_exhausted

    async def continue_pending_score_recovery(self, stop: asyncio.Event) -> None:
        """Yield between bounded keyset pages until history is exhausted or stopped."""
        try:
            while self.recovery_pending and not stop.is_set():
                await asyncio.sleep(0)
                if stop.is_set():
                    break
                self.recover_pending_scores()
        finally:
            if self.recovery_pending:
                log.warning(
                    "pending score recovery stopped with remaining pages",
                    extra={"extra_fields": {
                        "next_before_id": self._recovery_before_id,
                    }},
                )

    def _under_hourly_cap(self, now: float) -> bool:
        self._entry_times = [t for t in self._entry_times if now - t < 3600.0]
        return len(self._entry_times) < self._cfg["max_entries_per_hour"]

    def _accept_curve(self, ev: CurveProgress) -> bool:
        """Accept only finite, strictly newer snapshots seen by this consumer."""
        if not curve_progress_is_finite(ev):
            return False
        previous = self._curve_order.get(ev.mint)
        if previous is not None and ev.t_mono <= previous:
            return False
        if previous is not None:
            del self._curve_order[ev.mint]
        elif len(self._curve_order) >= self._curve_order_capacity:
            inactive = next((mint for mint in self._curve_order
                             if mint not in self.positions
                             and mint not in self._pending
                             and mint not in self._pending_score), None)
            if inactive is None:
                return False
            del self._curve_order[inactive]
            self._last_state.pop(inactive, None)
            self._last_state_ts.pop(inactive, None)
        self._curve_order[ev.mint] = ev.t_mono
        return True

    def _with_smart_money(self, mint: str, feats, *, before_at: float):
        """Add deterministic smart-money features from pre-decision ledger evidence only.

        No network calls here. The gate already decided whether the early-buyer read was
        available; strategy treats a missing read as a zero-valued ranking feature rather
        than an exception.
        """
        if not self._smart_money_cfg:
            return feats
        from memebot.store import latest_early_buyer_read, smart_wallets_snapshot
        read = latest_early_buyer_read(self._conn, mint)
        if read is None or read["unavailable_reason"]:
            return replace(feats, smart_money_count=0, smart_money_pnl_sol=0.0)
        buyers = tuple(json.loads(read["buyers_json"] or "[]"))
        smart = smart_wallets_snapshot(
            self._conn,
            before_at=before_at,
            min_events=int(self._smart_money_cfg["min_events"]),
            min_realized_pnl_sol=float(self._smart_money_cfg["min_realized_pnl_sol"]),
        )
        selected = [wallet for wallet in buyers if wallet in smart]
        pnl = sum(float(smart[wallet]["realized_pnl_sol"]) for wallet in selected)
        return replace(feats, smart_money_count=len(selected), smart_money_pnl_sol=pnl)

    def _decision_features(self, mint: str, row, *, decision_at: float,
                           risk_score: float):
        return self._fe.features(
            mint,
            as_of=decision_at,
            identity_ingested_at=row["p3_identity_ingested_at"],
            risk_score=risk_score,
            min_samples=self._cfg["min_samples"],
            max_latest_age_s=self._stale_after_s,
        )

    def _current_safety_pass(self, ev: SafetyPassed) -> SafetyPassed | None:
        """Return persisted current safety evidence, or fail closed."""
        if ev.segment != "CLIMBING":
            return None
        token = get_token(self._conn, ev.mint)
        if token is None or token["state"] != "CLIMBING" or token["rugged"] != 0:
            return None
        report = latest_safety_report(self._conn, ev.mint)
        if report is None or report["id"] != ev.safety_report_id:
            return None
        try:
            hard_fails = json.loads(report["hard_fails_json"])
        except (TypeError, ValueError):
            return None
        if hard_fails != []:
            return None
        return replace(ev, risk_score=float(report["risk_score"]))

    async def on_safety_passed(self, ev: SafetyPassed) -> None:
        ev = self._current_safety_pass(ev)
        if ev is None:
            return
        mint = ev.mint
        if mint in self.positions or mint in self._pending:
            return                                          # duplicate signal for an in-flight mint
        if decision_exists(self._conn, mint=mint, segment="CLIMBING"):
            return                                          # durable duplicate after score/restart
        pending = self._pending_score.get(mint)
        if (pending is not None
                and ev.safety_report_id <= pending.safety_passed.safety_report_id):
            return
        if not await self._try_score(ev, decision_t_mono=ev.t_mono):
            self._enqueue_pending_score(ev, registered_at=self._clock())

    def _enqueue_pending_score(self, ev: SafetyPassed, *, registered_at: float) -> None:
        if (ev.mint not in self._pending_score
                and len(self._pending_score) >= self._pending_score_capacity):
            evicted_mint = min(
                self._pending_score,
                key=lambda mint: (
                    self._pending_score[mint].registered_at,
                    self._pending_score[mint].safety_passed.safety_report_id,
                    mint,
                ),
            )
            del self._pending_score[evicted_mint]
            log.warning("unscored candidate superseded by capacity",
                        extra={"extra_fields": {"mint": evicted_mint}})
        self._pending_score[ev.mint] = PendingScore(
            safety_passed=ev, registered_at=registered_at,
        )

    async def _try_watch_only(self, ev: SafetyPassed) -> bool:
        """Score and publish a legacy SKIP when entries are explicitly disabled."""
        mint = ev.mint
        current_pass = self._current_safety_pass(ev)
        if current_pass is None:
            self._pending_score.pop(mint, None)
            return True
        if decision_exists(self._conn, mint=mint, segment="CLIMBING"):
            self._pending_score.pop(mint, None)
            return True
        row = get_token(self._conn, mint)
        if row is None:
            return True
        decision_at = self._clock()
        feats = self._decision_features(
            mint, row, decision_at=decision_at, risk_score=current_pass.risk_score,
        )
        if feats is None:
            return False
        feats = self._with_smart_money(mint, feats, before_at=decision_at)
        pinned = self._scorer.weights_version
        if type(pinned) is not str or not pinned:
            raise ValueError("scorer weights_version must be a non-empty string")
        try:
            score = self._scorer.score(feats, segment="CLIMBING")
            value = score.value
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not 0.0 <= value <= 100.0
                or score.weights_version != pinned
                or not isinstance(score.feature_vector, Mapping)
            ):
                raise ValueError("invalid watch-only score")
            score_value = float(value)
            score_vector = dict(score.feature_vector)
            if "canonical" in score_vector:
                raise ValueError("score feature_vector contains reserved key canonical")
            score_status = "VALID"
            unavailable_reason = ""
        except Exception:
            score_value = 0.0
            score_vector = dict.fromkeys(_SCORE_FEATURE_KEYS)
            score_status = "UNAVAILABLE"
            unavailable_reason = "score_exception"
        score_vector.update({
            "score_status": score_status,
            "score_weights_version": pinned,
            "score_unavailable_reason": unavailable_reason,
        })
        decision_id = record_decision(
            self._conn,
            at=decision_at,
            mint=mint,
            segment="CLIMBING",
            action="SKIP",
            score=score_value,
            feature_vector=score_vector,
            config_hash=self._config_hash,
            safety_report_id=current_pass.safety_report_id,
        )
        await self._bus.publish(CandidateScored(
            t_wall=decision_at,
            t_mono=self._mono_clock(),
            mint=mint,
            decision_id=decision_id,
            segment="CLIMBING",
            score=score_value,
            spot_price_sol=feats.spot_price_sol,
        ))
        self._pending_score.pop(mint, None)
        return True

    async def _try_score(self, ev: SafetyPassed, *, decision_t_mono: float,
                         current: CurveProgress | None = None) -> bool:
        if (self._canonical_resolver is None
                and self._cfg.get("entries_enabled") is not False):
            raise ValueError("canonical_resolver is required when entries are enabled")
        if self._canonical_resolver is None:
            return await self._try_watch_only(ev)
        mint = ev.mint
        raw_decision_at = self._clock()
        planned_size_sol = float(self._cfg["position_size_sol"])
        target_snapshot = None if current is None else _snapshot_from_event(current)

        class _NoDecision(Exception):
            pass

        try:
            with p3_immediate_transaction(self._conn):
                decision_at = allocate_p3_causal_wall(
                    self._conn, raw_wall=raw_decision_at,
                )
                current_pass = self._current_safety_pass(ev)
                if current_pass is None:
                    raise _NoDecision
                ev = current_pass
                if decision_exists(self._conn, mint=mint, segment="CLIMBING"):
                    raise _NoDecision
                row = get_token(self._conn, mint)
                if row is None:
                    raise _NoDecision
                feats = self._decision_features(
                    mint, row, decision_at=decision_at, risk_score=ev.risk_score,
                )
                if feats is None:
                    raise _NoDecision
                feats = self._with_smart_money(
                    mint, feats, before_at=decision_at,
                )
                pinned_weights_version = self._scorer.weights_version
                if type(pinned_weights_version) is not str or not pinned_weights_version:
                    raise ValueError("scorer weights_version must be a non-empty string")
                score_status = "VALID"
                score_unavailable_reason = ""
                try:
                    score = self._scorer.score(feats, segment="CLIMBING")
                    score_value = score.value
                    if (
                        type(score_value) not in (int, float)
                        or not math.isfinite(score_value)
                        or not 0.0 <= score_value <= 100.0
                    ):
                        score_status = "UNAVAILABLE"
                        score_unavailable_reason = "score_nonfinite"
                    else:
                        score_value = float(score_value)
                        if not isinstance(score.feature_vector, Mapping):
                            raise TypeError("score feature_vector must be a mapping")
                        score_vector = dict(score.feature_vector)
                        if "canonical" in score_vector:
                            raise ValueError(
                                "score feature_vector contains reserved key canonical"
                            )
                        if any(key not in score_vector for key in _SCORE_FEATURE_KEYS):
                            raise ValueError(
                                "score feature_vector is missing required keys"
                            )
                        for feature_key, feature_value in score_vector.items():
                            if (
                                type(feature_key) is not str
                                or type(feature_value) not in (int, float)
                            ):
                                raise TypeError(
                                    "score feature_vector values must be numeric"
                                )
                            if not math.isfinite(feature_value):
                                score_status = "UNAVAILABLE"
                                score_unavailable_reason = "score_nonfinite"
                                break
                        if score_status == "VALID":
                            if score.weights_version != pinned_weights_version:
                                raise ValueError(
                                    "score weights_version does not match scorer"
                                )
                            score_weights_version = pinned_weights_version
                except Exception:
                    score_status = "UNAVAILABLE"
                    score_unavailable_reason = "score_exception"
                if score_status == "UNAVAILABLE":
                    score_value = 0.0
                    score_vector = dict.fromkeys(_SCORE_FEATURE_KEYS)
                    score_weights_version = pinned_weights_version

                resolution = self._canonical_resolver.resolve(
                    mint,
                    decision_at=decision_at,
                    target_report_id=ev.safety_report_id,
                    target_snapshot=target_snapshot,
                )
                verdict = resolution.verdict
                ranked = sorted(
                    (
                        candidate for candidate in verdict.ranking_inputs.get(
                            "candidates", ()
                        )
                        if isinstance(candidate, Mapping)
                        and candidate.get("eligible") is True
                        and type(candidate.get("rank")) is int
                    ),
                    key=lambda candidate: candidate["rank"],
                )
                canonical_payload = asdict(verdict)
                canonical_payload.update({
                    "config_hash": self._config_hash,
                    "ranking_order": [candidate["mint"] for candidate in ranked],
                })
                score_vector.update({
                    "score_status": score_status,
                    "score_weights_version": score_weights_version,
                    "score_unavailable_reason": score_unavailable_reason,
                    "canonical": canonical_payload,
                })
                enter = (
                    verdict.status == "CANONICAL"
                    and score_status == "VALID"
                    and self._cfg["entries_enabled"]
                    and score_value >= self._cfg["score_threshold"]
                    and feats.age_s >= self._cfg["min_age_s"]
                    and (len(self.positions) + len(self._pending))
                    < self._cfg["max_concurrent_positions"]
                    and self._under_hourly_cap(decision_at)
                )
                action = "BUY" if enter else "SKIP"
                decision_id, observation_ids, _ = (
                    record_decision_with_canonical_observations(
                        self._conn,
                        at=decision_at,
                        mint=mint,
                        segment="CLIMBING",
                        action=action,
                        score=score_value,
                        feature_vector=score_vector,
                        safety_report_id=ev.safety_report_id,
                        config_hash=self._config_hash,
                        generation_hash=verdict.generation_hash,
                        observations=resolution.observations,
                        score_status=score_status,
                        score_weights_version=score_weights_version,
                        score_unavailable_reason=score_unavailable_reason,
                        planned_position_size_sol=planned_size_sol,
                    )
                )
        except _NoDecision:
            current_pass = self._current_safety_pass(ev)
            if current_pass is None or decision_exists(
                self._conn, mint=mint, segment="CLIMBING",
            ):
                self._pending_score.pop(mint, None)
                return True
            return False

        decision_mono = self._mono_clock()
        await self._bus.publish(CandidateScored(
            t_wall=decision_at,
            t_mono=decision_mono,
            mint=mint,
            decision_id=decision_id,
            segment="CLIMBING",
            score=score_value,
            spot_price_sol=feats.spot_price_sol,
        ))
        for observation_id, observation in zip(
            observation_ids, resolution.observations, strict=True,
        ):
            if observation.unavailable_reason:
                continue
            await self._bus.publish(CanonicalObservationStarted(
                t_wall=decision_at,
                t_mono=decision_mono,
                observation_id=observation_id,
                decision_id=decision_id,
                mint=observation.mint,
                start_price_sol=observation.start_price_sol,
                price_observed_at=observation.price_observed_at,
            ))
        self._pending_score.pop(mint, None)
        # t_wall uses the injected clock (not time.time()) so the counterfactual's horizon
        # math (now - t0) is correct under both the live clock and a mocked replay clock.
        if not enter:
            return True
        decision_snapshot = _decision_snapshot(resolution, mint=mint)
        self._pending[mint] = PendingEntry(
            mint=mint,
            decision_id=decision_id,
            safety_report_id=ev.safety_report_id,
            canonical_inputs_hash=verdict.inputs_hash,
            decision_snapshot=decision_snapshot,
            decision_at=decision_at,
            decision_mono=decision_mono,
            size_sol=planned_size_sol,
            score=score_value,
        )
        # Stamp freshness AT registration (not only on a later CurveProgress). A pending whose
        # mint falls off the poll set before any post-registration tick would otherwise never
        # appear in _last_state_ts, so sweep_stale (which iterates _last_state_ts) could never
        # age it out -- leaking a max_concurrent_positions slot forever (N2 residual).
        self._last_state_ts[mint] = decision_at
        return True

    async def _persist_entry_draft(self, draft: P3EntryDraft) -> tuple[int, int]:
        delay = 0.05
        while True:
            try:
                return record_canonical_paper_buy(
                    self._conn,
                    decision_id=draft.decision_id,
                    recheck_id=draft.recheck_id,
                    raw_wall=draft.raw_processed_at,
                    mint=draft.mint,
                    segment=draft.segment,
                    qty=draft.qty,
                    quote_price=draft.quote_price,
                    fill_price=draft.fill_price,
                    fees=dict(draft.fees),
                    realism_grade=draft.realism_grade,
                    planned_size_sol=draft.planned_size_sol,
                )
            except (EvidenceIntegrityError, ValueError, TypeError):
                raise
            except Exception:
                log.exception("P3 BUY persistence failed; retrying immutable draft")
                await asyncio.sleep(delay)
                delay = min(2.0, delay * 2.0)

    async def _persist_exit_draft(self, draft: P3ExitDraft) -> tuple[int, int | None]:
        delay = 0.05
        while True:
            try:
                return record_canonical_paper_sell(
                    self._conn,
                    decision_id=draft.decision_id,
                    raw_wall=draft.raw_processed_at,
                    mint=draft.mint,
                    segment=draft.segment,
                    qty=draft.qty,
                    quote_price=draft.quote_price,
                    fill_price=draft.fill_price,
                    fees=dict(draft.fees),
                    realism_grade=draft.realism_grade,
                    exit_reason=draft.exit_reason,
                    ladder_index=draft.ladder_index,
                )
            except (EvidenceIntegrityError, ValueError, TypeError):
                raise
            except Exception:
                log.exception("P3 SELL persistence failed; retrying immutable draft")
                await asyncio.sleep(delay)
                delay = min(2.0, delay * 2.0)

    async def _cancel_pending_entry(
        self,
        pending: PendingEntry,
        *,
        reason: str,
        trigger_report_id: int | None = None,
    ) -> None:
        if pending.terminal_recheck_id is None:
            latest = latest_safety_report(self._conn, pending.mint)
            if latest is None:
                raise ValueError("pending cancellation has no latest safety report")
            trigger_id = latest["id"] if trigger_report_id is None else trigger_report_id
            raw_rechecked_at = self._clock()
            with p3_immediate_transaction(self._conn):
                rechecked_at = allocate_p3_causal_wall(
                    self._conn, raw_wall=raw_rechecked_at,
                )
                payload = {
                    "decision_id": pending.decision_id,
                    "attempt": pending.recheck_attempt,
                    "trigger": "safety_hard_fail",
                    "trigger_report_id": trigger_id,
                    "rechecked_at": rechecked_at,
                    "fill_event_at": None,
                    "causal_target_report_id": pending.safety_report_id,
                    "latest_target_report_id": latest["id"],
                    "prior_inputs_hash": pending.canonical_inputs_hash,
                    "target_snapshot": None,
                    "verdict": {
                        "status": "UNRESOLVED",
                        "reason": reason,
                        "canonical_mint": None,
                        "inputs_hash": latest["inputs_hash"],
                    },
                }
                payload_json = json.dumps(
                    payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
                )
                recheck_hash = hashlib.sha256(
                    payload_json.encode("utf-8")
                ).hexdigest()
                recheck_id = record_canonical_recheck(
                    self._conn,
                    decision_id=pending.decision_id,
                    attempt=pending.recheck_attempt,
                    rechecked_at=rechecked_at,
                    causal_target_report_id=pending.safety_report_id,
                    latest_target_report_id=latest["id"],
                    status="CANCEL",
                    reason=reason,
                    canonical_mint=None,
                    prior_inputs_hash=pending.canonical_inputs_hash,
                    recheck_inputs_hash=recheck_hash,
                    payload=payload,
                )
            pending.terminal_recheck_id = recheck_id
            pending.terminal_reason = reason
        elif pending.terminal_reason is None:
            raise ValueError("durable pending CANCEL is missing its exact reason")
        else:
            reason = pending.terminal_reason

        delay = 0.05
        terminal_raw_wall = self._clock()
        while True:
            try:
                record_terminal_entry_execution(
                    self._conn,
                    decision_id=pending.decision_id,
                    raw_wall=terminal_raw_wall,
                    status="CANCELLED",
                    reason=reason,
                    recheck_id=pending.terminal_recheck_id,
                )
                break
            except Exception:
                log.exception(
                    "P3 CANCELLED persistence failed; retrying exact terminal link"
                )
                await asyncio.sleep(delay)
                delay = min(2.0, delay * 2.0)
        self._pending.pop(pending.mint, None)

    async def _fill_pending(self, ev: CurveProgress, *, _ordered: bool = False) -> None:
        if not _ordered and not self._accept_curve(ev):
            return
        pend = self._pending.get(ev.mint)
        if pend is None:
            return
        if pend.terminal_recheck_id is not None:
            await self._cancel_pending_entry(
                pend, reason=pend.terminal_reason or "canonical_internal_error",
            )
            return
        if ev.t_mono < pend.decision_mono + self._fill_cfg["latency_min_s"]:
            return                                          # latency penalty not yet satisfied
        target_snapshot = _snapshot_from_event(ev)
        if target_snapshot is None:
            return
        raw_rechecked_at = self._clock()
        try:
            with p3_immediate_transaction(self._conn):
                rechecked_at = allocate_p3_causal_wall(
                    self._conn, raw_wall=raw_rechecked_at,
                )
                if not pend.decision_at <= ev.t_wall <= rechecked_at:
                    raise ValueError("fill event is outside the causal decision window")
                if rechecked_at - ev.t_wall > self._fill_event_max_age_s:
                    raise _StaleFillEvent
                resolution = self._canonical_resolver.resolve(
                    ev.mint,
                    decision_at=rechecked_at,
                    target_report_id=pend.safety_report_id,
                    target_snapshot=target_snapshot,
                )
                verdict = resolution.verdict
                ranking_inputs = verdict.ranking_inputs
                latest_report_id = ranking_inputs.get("latest_target_report_id")
                passed = (
                    verdict.status == "CANONICAL"
                    and verdict.reason == "canonical_selected"
                    and verdict.canonical_mint == ev.mint
                    and latest_report_id == pend.safety_report_id
                )
                recheck_status = "PASS" if passed else "CANCEL"
                payload = {
                    "decision_id": pend.decision_id,
                    "attempt": pend.recheck_attempt,
                    "trigger": "curve_progress",
                    "trigger_report_id": None,
                    "rechecked_at": rechecked_at,
                    "fill_event_at": ev.t_wall,
                    "causal_target_report_id": pend.safety_report_id,
                    "latest_target_report_id": latest_report_id,
                    "prior_inputs_hash": pend.canonical_inputs_hash,
                    "target_snapshot": _recheck_snapshot_payload(target_snapshot),
                    "verdict": {
                        "status": verdict.status,
                        "reason": verdict.reason,
                        "canonical_mint": verdict.canonical_mint,
                        "inputs_hash": verdict.inputs_hash,
                    },
                }
                payload_json = json.dumps(
                    payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
                )
                recheck_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
                recheck_id = record_canonical_recheck(
                    self._conn,
                    decision_id=pend.decision_id,
                    attempt=pend.recheck_attempt,
                    rechecked_at=rechecked_at,
                    causal_target_report_id=pend.safety_report_id,
                    latest_target_report_id=latest_report_id,
                    status=recheck_status,
                    reason=verdict.reason,
                    canonical_mint=verdict.canonical_mint,
                    prior_inputs_hash=pend.canonical_inputs_hash,
                    recheck_inputs_hash=recheck_hash,
                    payload=payload,
                )
        except _StaleFillEvent:
            return
        if not passed:
            pend.terminal_recheck_id = recheck_id
            pend.terminal_reason = verdict.reason
            await self._cancel_pending_entry(pend, reason=verdict.reason)
            return

        fill_state = target_snapshot.curve_state()         # the >= T snapshot, race-free
        self._last_state[ev.mint] = fill_state
        self._last_state_ts[ev.mint] = ev.t_wall
        try:
            fill = self._broker.buy(
                pend.decision_snapshot.curve_state(), fill_state, sol_in=pend.size_sol,
            )
        except Exception:
            pend.recheck_attempt += 1
            log.exception("P3 broker BUY failed after durable PASS")
            return
        draft = P3EntryDraft(
            decision_id=pend.decision_id,
            recheck_id=recheck_id,
            raw_processed_at=self._clock(),
            mint=ev.mint,
            segment="CLIMBING",
            qty=fill.qty,
            quote_price=fill.quote_price,
            fill_price=fill.fill_price,
            fees=tuple(sorted(fill.fees.items())),
            realism_grade=fill.realism_grade,
            planned_size_sol=pend.size_sol,
        )
        paper_trade_id, execution_id = await self._persist_entry_draft(draft)
        entry_at = self._conn.execute(
            "SELECT at FROM paper_entry_executions WHERE id=?", (execution_id,),
        ).fetchone()["at"]
        self.positions[ev.mint] = Position(
            mint=ev.mint, decision_id=pend.decision_id, qty_remaining=fill.qty,
            entry_price=fill.fill_price, entry_at=entry_at, peak_price=fill.fill_price,
            original_qty=fill.qty, size_sol=pend.size_sol, buy_notional=fill.sol_notional,
            is_p3=True)
        self._entry_times.append(entry_at)
        del self._pending[ev.mint]
        await self._bus.publish(PaperEntry(
            t_wall=entry_at, t_mono=time.monotonic(), mint=ev.mint, segment="CLIMBING",
            qty=fill.qty, fill_price=fill.fill_price, size_sol=pend.size_sol,
            score=pend.score, realism_grade=fill.realism_grade,
            canonical_status=verdict.status,
            canonical_mint=verdict.canonical_mint,
            canonical_resolver_version=verdict.resolver_version,
            canonical_recheck_id=recheck_id,
            canonical_recheck_hash=recheck_hash,
            paper_trade_id=paper_trade_id,
            paper_entry_execution_id=execution_id,
        ))

    async def _sell(self, pos: Position, qty: float, reason: str, now: float,
                    state: CurveState) -> None:
        fill = self._broker.sell(state, state, tokens_in=qty)
        ladder_index = int(reason.removeprefix("ladder_")) if reason.startswith(
            "ladder_"
        ) else None
        if pos.is_p3:
            draft = P3ExitDraft(
                decision_id=pos.decision_id,
                raw_processed_at=now,
                mint=pos.mint,
                segment="CLIMBING",
                qty=qty,
                quote_price=fill.quote_price,
                fill_price=fill.fill_price,
                fees=tuple(sorted(fill.fees.items())),
                realism_grade=fill.realism_grade,
                exit_reason=reason,
                ladder_index=ladder_index,
            )
            sell_id, _ = await self._persist_exit_draft(draft)
            sell_at = self._conn.execute(
                "SELECT at FROM paper_trades WHERE id=?", (sell_id,),
            ).fetchone()["at"]
        else:
            record_paper_trade(
                self._conn, decision_id=pos.decision_id, at=now, mint=pos.mint,
                segment=pos.segment, side="sell", qty=qty,
                quote_price=fill.quote_price, fill_price=fill.fill_price,
                fees=fill.fees, realism_grade=fill.realism_grade,
            )
            sell_at = now
        pos.qty_remaining -= qty
        # proceeds - cost-basis fraction. buy_notional (post-fee SOL-in), not size_sol
        # (pre-fee), so sum(PaperExit.pnl_sol) over a full close equals outcomes.pnl_sol
        # exactly (M1) -- both sides then account for the buy fees identically.
        pnl = fill.sol_notional - (qty / pos.original_qty) * pos.buy_notional
        if pos.qty_remaining <= 1e-9:
            if not pos.is_p3:
                record_outcome(
                    self._conn, at=now, ref_kind="trade", ref_id=pos.decision_id,
                    pnl_sol=self._realized_pnl(pos),
                    detail={"reason": reason, "hold_s": now - pos.entry_at,
                            "grade": fill.realism_grade},
                )
            self.positions.pop(pos.mint, None)
        await self._bus.publish(PaperExit(
            t_wall=sell_at, t_mono=time.monotonic(), mint=pos.mint, segment="CLIMBING",
            qty=qty, fill_price=fill.fill_price, pnl_sol=pnl, reason=reason,
            realism_grade=fill.realism_grade))

    def _realized_pnl(self, pos: Position) -> float:
        """Realized SOL P&L for a fully-closed position: sum(sell notional) - sum(buy notional)."""
        rows = self._conn.execute(
            "SELECT side, qty, fill_price, fees_json FROM paper_trades WHERE decision_id=?",
            (pos.decision_id,)).fetchall()
        proceeds = cost = 0.0
        for r in rows:
            fees = sum(json.loads(r["fees_json"]).values())
            if r["side"] == "sell":
                proceeds += r["qty"] * r["fill_price"] - fees
            else:
                cost += r["qty"] * r["fill_price"] + fees
        return proceeds - cost

    async def on_price(self, ev: CurveProgress, *, _ordered: bool = False) -> None:
        if not _ordered and not self._accept_curve(ev):
            return
        pos = self.positions.get(ev.mint)
        if pos is None:
            return
        now = self._clock()
        state = _state_from_event(ev)                        # race-free price from THIS event
        self._last_state[ev.mint] = state
        self._last_state_ts[ev.mint] = ev.t_wall
        if not ev.virtual_token_reserves:
            return
        price = spot_price_sol_per_token(state, token_decimals=self._pump["token_decimals"])
        pos.peak_price = max(pos.peak_price, price)
        # 1) laddered take-profit
        for i, mult in enumerate(self._exits["ladder_multiples"]):
            if i not in pos.ladder_hits and price >= pos.entry_price * mult:
                qty = min(pos.qty_remaining, pos.original_qty * self._exits["ladder_fractions"][i])
                if qty > 1e-9:
                    await self._sell(pos, qty, f"ladder_{i}", now, state)
                    pos.ladder_hits.add(i)
                if ev.mint not in self.positions:
                    return
        # 2) hard time-stop
        if now - pos.entry_at >= self._exits["time_stop_s"]:
            await self._sell(pos, pos.qty_remaining, "time_stop", now, state)
            return
        # 3) trailing stop from peak
        if price <= pos.peak_price * (1 - self._exits["trailing_stop_pct"] / 100.0):
            await self._sell(pos, pos.qty_remaining, "trailing_stop", now, state)

    async def on_safety_flip(self, ev: SafetyHardFail) -> None:
        self._pending_score.pop(ev.mint, None)
        pending = self._pending.get(ev.mint)
        if pending is not None:
            trigger_report = None
            if type(ev.safety_report_id) is int and ev.safety_report_id > 0:
                trigger_report = self._conn.execute(
                    "SELECT id,mint FROM safety_reports WHERE id=?",
                    (ev.safety_report_id,),
                ).fetchone()
            valid_trigger = (
                trigger_report is not None and trigger_report["mint"] == ev.mint
            )
            cancel_reason = "safety_flip" if valid_trigger else "canonical_internal_error"
            await self._cancel_pending_entry(
                pending,
                reason=cancel_reason,
                trigger_report_id=ev.safety_report_id if valid_trigger else None,
            )
        self._curve_order.pop(ev.mint, None)
        pos = self.positions.get(ev.mint)
        if pos is None:
            self._last_state.pop(ev.mint, None)
            self._last_state_ts.pop(ev.mint, None)
            return
        # Minor (gate-rug-overstatement): a gate-detected rug can't actually be sold --
        # honeypot logic or frozen liquidity means there is no real exit -- so disposing at
        # the last cached LIVE curve price overstated rug outcomes. Dispose at 0 (total
        # loss), same treatment as a DEAD lifecycle transition. This also removes the
        # flip's dependency on any cached state.
        await self._close_at_zero(pos, "safety_flip", self._clock())

    async def on_transition(self, ev: LifecycleTransition) -> None:
        """I1/I2: a token that dies or graduates stops emitting CurveProgress forever, so
        neither _fill_pending nor on_price will ever run for it again. Without this handler
        a PendingEntry leaks (I1: needs a later CurveProgress that never comes) and a held
        moon-bag never closes (I2: no closing outcome, winners censored from expectancy) --
        both permanently consume a max_concurrent_positions slot."""
        if ev.to_state not in ("DEAD", "GRADUATED"):
            return
        self._pending_score.pop(ev.mint, None)
        self._curve_order.pop(ev.mint, None)
        pending = self._pending.get(ev.mint)
        if pending is not None:
            await self._cancel_pending_entry(
                pending, reason=ev.to_state.lower(),
            )
            log.info("pending entry cancelled on terminal transition",
                     extra={"extra_fields": {"mint": ev.mint, "to_state": ev.to_state}})
        pos = self.positions.get(ev.mint)
        if pos is not None:
            now = self._clock()
            last = self._last_state.get(ev.mint)
            if ev.to_state == "GRADUATED" and last is not None:
                # Real curve-math sell at the last observed curve price (owner's choice).
                await self._sell(pos, pos.qty_remaining, "graduated", now, last)
            else:
                # DEAD (rug: no live curve to sell into), or the rare GRADUATED-without-
                # cached-state case -- total-loss disposal at price 0.
                reason = "dead" if ev.to_state == "DEAD" else "graduated_no_price"
                await self._close_at_zero(pos, reason, now)
        self._last_state.pop(ev.mint, None)
        self._last_state_ts.pop(ev.mint, None)

    async def _close_at_zero(self, pos: Position, reason: str, now: float) -> None:
        """Total-loss disposal for a token with no live curve to sell into (rugged, or
        graduated with no cached price). Records the paper_trade + outcome directly,
        bypassing the broker (there is no quote to take)."""
        qty = pos.qty_remaining
        if pos.is_p3:
            draft = P3ExitDraft(
                decision_id=pos.decision_id,
                raw_processed_at=now,
                mint=pos.mint,
                segment="CLIMBING",
                qty=qty,
                quote_price=0.0,
                fill_price=0.0,
                fees=(),
                realism_grade="F",
                exit_reason=reason,
                ladder_index=None,
            )
            sell_id, _ = await self._persist_exit_draft(draft)
            sell_at = self._conn.execute(
                "SELECT at FROM paper_trades WHERE id=?", (sell_id,),
            ).fetchone()["at"]
        else:
            record_paper_trade(
                self._conn, decision_id=pos.decision_id, at=now, mint=pos.mint,
                segment=pos.segment, side="sell", qty=qty, quote_price=0.0,
                fill_price=0.0, fees={}, realism_grade="F",
            )
            record_outcome(
                self._conn, at=now, ref_kind="trade", ref_id=pos.decision_id,
                pnl_sol=self._realized_pnl(pos),
                detail={"reason": reason, "hold_s": now - pos.entry_at, "grade": "F"},
            )
            sell_at = now
        pos.qty_remaining = 0
        self.positions.pop(pos.mint, None)
        # Centralized stale-dict cleanup: every zero-disposal path (safety-flip, DEAD
        # transition, stale sweep) drops the mint's freshness state here, so callers stay
        # uniform (any redundant pops elsewhere are harmless no-ops).
        self._last_state.pop(pos.mint, None)
        self._last_state_ts.pop(pos.mint, None)
        self._curve_order.pop(pos.mint, None)
        await self._bus.publish(PaperExit(
            t_wall=sell_at, t_mono=time.monotonic(), mint=pos.mint, segment="CLIMBING",
            qty=qty, fill_price=0.0, pnl_sol=0.0 - (qty / pos.original_qty) * pos.buy_notional,
            reason=reason, realism_grade="F"))

    async def sweep_stale(self, now: float) -> None:
        """N2 (strategy side): age out mints whose last curve update is stale -- the same
        treatment the counterfactual already applies. CurvePoller tracks only the top
        max_tracked mints by curve progress, so a mint that stops producing fresh
        CurveProgress fell off that set; per the base rate (~99% of tokens die), it's dead.
        A pending entry there will never fill (evict it, freeing the concurrency slot); an
        open position there is a total loss (force-close via _close_at_zero, same as a rug).
        Iterates over a snapshot since both branches mutate _last_state/_last_state_ts."""
        expired_scores = [
            mint for mint, pending in self._pending_score.items()
            if now - pending.registered_at > self._stale_after_s
        ]
        for mint in expired_scores:
            del self._pending_score[mint]
            log.info("unscored candidate expired",
                     extra={"extra_fields": {"mint": mint}})

        stale_mints = [mint for mint, ts in list(self._last_state_ts.items())
                       if now - ts > self._stale_after_s]
        for mint in stale_mints:
            pending = self._pending.get(mint)
            if pending is not None:
                await self._cancel_pending_entry(pending, reason="stale")
                log.info("pending entry cancelled as stale (off-poll)",
                         extra={"extra_fields": {"mint": mint}})
            pos = self.positions.get(mint)
            if pos is not None:
                await self._close_at_zero(pos, "stale", now)
            self._last_state.pop(mint, None)
            self._last_state_ts.pop(mint, None)
            self._curve_order.pop(mint, None)

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    ev = await asyncio.wait_for(self._q.get(), timeout=0.5)
                except TimeoutError:
                    await self.sweep_stale(self._clock())
                    continue
                acknowledge = False
                try:
                    if isinstance(ev, CurveProgress) and not self._accept_curve(ev):
                        await self.sweep_stale(self._clock())
                        acknowledge = True
                        continue
                    if isinstance(ev, CurveProgress):
                        # Stamp before sweeping so the dequeued current tick cannot be
                        # mistaken for stale while unrelated work is expired.
                        self._last_state[ev.mint] = _state_from_event(ev)
                        self._last_state_ts[ev.mint] = ev.t_wall
                    # A continuously busy queue may never reach the timeout branch.
                    await self.sweep_stale(self._clock())
                    if isinstance(ev, SafetyPassed):
                        await self.on_safety_passed(ev)
                    elif isinstance(ev, CurveProgress):
                        pending_score = self._pending_score.get(ev.mint)
                        scored_now = False
                        if pending_score is not None:
                            scored_now = await self._try_score(
                                pending_score.safety_passed,
                                decision_t_mono=ev.t_mono,
                                current=ev,
                            )
                        if not scored_now:
                            await self._fill_pending(ev, _ordered=True)
                            await self.on_price(ev, _ordered=True)
                    elif isinstance(ev, SafetyHardFail):
                        await self.on_safety_flip(ev)
                    elif isinstance(ev, LifecycleTransition):
                        await self.on_transition(ev)
                    acknowledge = True
                except Exception:
                    log.exception(
                        "strategy handler failed",
                        extra={"extra_fields": {"mint": getattr(ev, "mint", "?")}},
                    )
                    raise
                finally:
                    if acknowledge:
                        self._bus.critical_done(self._q)
        finally:
            self._bus.unsubscribe(self._q)
