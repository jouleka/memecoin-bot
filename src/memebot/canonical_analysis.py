"""Read-only P3 canonical-resolver population and coverage metrics."""
from __future__ import annotations

import json
import math
import re
import sqlite3
from dataclasses import dataclass

from memebot.store import EvidenceIntegrityError

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_KEYS = frozenset({
    "resolver_version",
    "weights_version",
    "status",
    "reason",
    "resolved_at",
    "cluster_key",
    "cluster_size",
    "eligible_cluster_size",
    "canonical_mint",
    "rank",
    "rank_points",
    "generation_hash",
    "inputs_hash",
    "config_hash",
    "planned_size_sol",
    "ranking_order",
    "ranking_inputs",
})
_OUTCOME_KEYS = frozenset({
    "horizon_s",
    "forward_return_pct",
    "price0",
    "price0_observed_at",
    "price_now",
    "price_now_observed_at",
    "terminal",
    "unavailable_reason",
})
_UNRESOLVED_REASONS = frozenset({
    "canonical_target_not_live",
    "canonical_identity_unavailable",
    "canonical_identity_conflict",
    "canonical_cluster_too_large",
    "canonical_target_report_superseded",
    "canonical_holder_evidence_unavailable",
    "canonical_creator_history_overflow",
    "canonical_liquidity_unavailable",
    "canonical_internal_error",
})
_ZERO_CLUSTER_SUBJECT_ONLY_REASONS = frozenset({
    "canonical_target_not_live",
    "canonical_identity_unavailable",
    "canonical_identity_conflict",
})
_START_UNAVAILABLE_REASONS = frozenset({
    "start_price_missing",
    "start_price_stale",
    "start_price_malformed",
})


@dataclass(frozen=True, slots=True)
class CanonicalMetrics:
    horizon_s: float
    all_p3_decisions: int
    primary_decisions: int
    potential_pairs: int
    comparable_pairs: int
    pair_wins: int
    pair_losses: int
    pair_ties: int
    potential_clusters: int
    comparable_clusters: int
    cluster_wins: int
    cluster_losses: int
    cluster_ties: int
    harm_clusters: int
    unresolved_identity: int
    unresolved_holder: int
    unresolved_liquidity: int
    journal_gap_outcomes: int
    canonical_buy_decisions: int
    filled_entries: int
    cancelled_entries: int
    abandoned_entries: int
    pair_coverage: float | None
    cluster_coverage: float | None
    harm_rate: float | None
    unresolved_identity_rate: float | None
    unresolved_holder_rate: float | None
    unresolved_liquidity_rate: float | None
    terminal_coverage: float | None
    abandonment_rate: float | None


@dataclass(frozen=True, slots=True)
class _Decision:
    decision_id: int
    at: float
    mint: str
    action: str
    canonical: dict[str, object]
    horizons: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class _Observation:
    observation_id: int
    decision_id: int
    mint: str
    is_subject: bool
    is_canonical: bool
    eligible: bool
    observed_at: float
    start_price_sol: float | None
    price_observed_at: float | None
    unavailable_reason: str


@dataclass(frozen=True, slots=True)
class _Outcome:
    return_pct: float | None
    unavailable_reason: str


def _finite(value: object, *, minimum: float, maximum: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError
    numeric = float(value)
    if not minimum <= numeric <= maximum:
        raise ValueError
    return numeric


def _canonical_json(value: object) -> object:
    if type(value) is not str:
        raise ValueError
    decoded = json.loads(value, parse_constant=lambda _token: (_ for _ in ()).throw(ValueError()))
    if value != json.dumps(
        decoded, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ):
        raise ValueError
    return decoded


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _is_subject_only_unresolved(canonical: dict[str, object]) -> bool:
    reason = canonical["reason"]
    return (
        reason in _ZERO_CLUSTER_SUBJECT_ONLY_REASONS
        or reason == "canonical_cluster_too_large"
        or (
            reason == "canonical_internal_error"
            and canonical["cluster_size"] == 0
        )
    )


def _load_decisions(conn: sqlite3.Connection) -> dict[int, _Decision]:
    rows = conn.execute(
        "SELECT d.id,d.at,d.mint,d.action,d.feature_vector_json,d.config_hash "
        "FROM decisions AS d JOIN (SELECT DISTINCT decision_id "
        "FROM canonical_observations) AS population ON population.decision_id=d.id "
        "ORDER BY d.id",
    ).fetchall()
    decisions: dict[int, _Decision] = {}
    for row in rows:
        try:
            decision_id = row["id"]
            at = _finite(row["at"], minimum=0.0, maximum=4_102_444_800.0)
            mint = row["mint"]
            action = row["action"]
            feature = _canonical_json(row["feature_vector_json"])
            if (
                type(decision_id) is not int
                or decision_id <= 0
                or type(mint) is not str
                or not 1 <= len(mint.strip()) <= 128
                or action not in ("BUY", "SKIP")
                or type(feature) is not dict
            ):
                raise ValueError
            canonical = feature["canonical"]
            if type(canonical) is not dict or set(canonical) != _CANONICAL_KEYS:
                raise ValueError
            if (
                canonical["resolver_version"] != "canonical-v1"
                or type(canonical["weights_version"]) is not str
                or not canonical["weights_version"].strip()
                or canonical["resolved_at"] != at
                or type(canonical["cluster_key"]) is not str
                or type(canonical["cluster_size"]) is not int
                or canonical["cluster_size"] < 0
                or type(canonical["eligible_cluster_size"]) is not int
                or not 0 <= canonical["eligible_cluster_size"] <= canonical["cluster_size"]
                or not _HASH_RE.fullmatch(canonical["inputs_hash"])
                or not _HASH_RE.fullmatch(canonical["config_hash"])
                or canonical["config_hash"] != row["config_hash"]
                or type(canonical["ranking_order"]) is not list
            ):
                raise ValueError
            _finite(
                canonical["planned_size_sol"], minimum=math.nextafter(0.0, 1.0),
                maximum=1e100,
            )
            status = canonical["status"]
            reason = canonical["reason"]
            winner = canonical["canonical_mint"]
            generation_hash = canonical["generation_hash"]
            if status == "CANONICAL":
                valid_verdict = (
                    reason == "canonical_selected"
                    and canonical["cluster_size"] >= 1
                    and canonical["eligible_cluster_size"] >= 1
                    and winner == mint
                    and canonical["rank"] == 1
                    and type(canonical["rank_points"]) is int
                    and canonical["rank_points"] >= 0
                    and type(generation_hash) is str
                    and _HASH_RE.fullmatch(generation_hash) is not None
                )
            elif status == "SUPPRESSED":
                valid_verdict = (
                    reason == "copycat_cluster"
                    and canonical["cluster_size"] >= 1
                    and canonical["eligible_cluster_size"] >= 1
                    and type(winner) is str
                    and winner != mint
                    and type(canonical["rank"]) is int
                    and canonical["rank"] > 1
                    and type(canonical["rank_points"]) is int
                    and canonical["rank_points"] >= 0
                    and type(generation_hash) is str
                    and _HASH_RE.fullmatch(generation_hash) is not None
                    and action == "SKIP"
                )
            else:
                subject_only = _is_subject_only_unresolved(canonical)
                if winner is None:
                    valid_unresolved_winner = (
                        generation_hash is None
                        and canonical["eligible_cluster_size"] == 0
                        and canonical["ranking_order"] == []
                    )
                else:
                    valid_unresolved_winner = (
                        not subject_only
                        and type(winner) is str
                        and 1 <= len(winner.strip()) <= 128
                        and winner != mint
                        and type(generation_hash) is str
                        and _HASH_RE.fullmatch(generation_hash) is not None
                        and canonical["eligible_cluster_size"] >= 1
                        and bool(canonical["ranking_order"])
                        and canonical["ranking_order"][0] == winner
                    )
                valid_verdict = (
                    status == "UNRESOLVED"
                    and reason in _UNRESOLVED_REASONS
                    and canonical["rank"] is None
                    and canonical["rank_points"] is None
                    and action == "SKIP"
                    and valid_unresolved_winner
                    and (
                        (
                            subject_only
                            and canonical["eligible_cluster_size"] == 0
                            and (
                                (
                                    reason == "canonical_cluster_too_large"
                                    and canonical["cluster_size"] >= 1
                                )
                                or (
                                    reason != "canonical_cluster_too_large"
                                    and canonical["cluster_size"] == 0
                                )
                            )
                        )
                        or (
                            not subject_only
                            and canonical["cluster_size"] >= 1
                        )
                    )
                )
            if not valid_verdict:
                raise ValueError
            ranking_inputs = canonical["ranking_inputs"]
            if type(ranking_inputs) is not dict:
                raise ValueError
            raw_horizons = ranking_inputs["counterfactual_horizons_s"]
            if type(raw_horizons) is not list or not 1 <= len(raw_horizons) <= 32:
                raise ValueError
            horizons = tuple(
                _finite(item, minimum=math.nextafter(0.0, 1.0), maximum=1e100)
                for item in raw_horizons
            )
            if any(left >= right for left, right in zip(horizons, horizons[1:])):
                raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise EvidenceIntegrityError("malformed canonical decision") from exc
        decisions[decision_id] = _Decision(
            decision_id=decision_id,
            at=at,
            mint=mint,
            action=action,
            canonical=canonical,
            horizons=horizons,
        )
    return decisions


def _load_observations(
    conn: sqlite3.Connection, decisions: dict[int, _Decision],
) -> tuple[dict[int, _Observation], dict[int, list[_Observation]]]:
    by_id: dict[int, _Observation] = {}
    by_decision: dict[int, list[_Observation]] = {key: [] for key in decisions}
    rows = conn.execute(
        "SELECT id,decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason "
        "FROM canonical_observations ORDER BY id",
    ).fetchall()
    for row in rows:
        try:
            observation_id = row["id"]
            decision_id = row["decision_id"]
            decision = decisions[decision_id]
            observed_at = _finite(
                row["observed_at"], minimum=0.0, maximum=4_102_444_800.0,
            )
            mint = row["mint"]
            if (
                type(observation_id) is not int
                or observation_id <= 0
                or type(mint) is not str
                or not 1 <= len(mint.strip()) <= 128
                or observed_at != decision.at
                or any(type(row[name]) is not int or row[name] not in (0, 1)
                       for name in ("is_subject", "is_canonical", "eligible"))
                or type(row["unavailable_reason"]) is not str
            ):
                raise ValueError
            unavailable_reason = row["unavailable_reason"]
            if unavailable_reason == "":
                start_price = _finite(
                    row["start_price_sol"],
                    minimum=math.nextafter(0.0, 1.0), maximum=1e100,
                )
                price_at = _finite(
                    row["price_observed_at"], minimum=0.0, maximum=observed_at,
                )
                if row["price_source"] != "curve_snapshot":
                    raise ValueError
            else:
                if (
                    unavailable_reason not in _START_UNAVAILABLE_REASONS
                    or row["start_price_sol"] is not None
                    or row["price_observed_at"] is not None
                    or row["price_source"] != ""
                ):
                    raise ValueError
                start_price = None
                price_at = None
            observation = _Observation(
                observation_id=observation_id,
                decision_id=decision_id,
                mint=mint,
                is_subject=bool(row["is_subject"]),
                is_canonical=bool(row["is_canonical"]),
                eligible=bool(row["eligible"]),
                observed_at=observed_at,
                start_price_sol=start_price,
                price_observed_at=price_at,
                unavailable_reason=unavailable_reason,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise EvidenceIntegrityError("malformed canonical observation") from exc
        by_id[observation_id] = observation
        by_decision[decision_id].append(observation)

    for decision_id, observations in by_decision.items():
        decision = decisions[decision_id]
        subjects = [item for item in observations if item.is_subject]
        winners = [item for item in observations if item.is_canonical]
        canonical_mint = decision.canonical["canonical_mint"]
        if len(subjects) != 1 or subjects[0].mint != decision.mint:
            raise EvidenceIntegrityError("malformed canonical observation")
        subject_only = _is_subject_only_unresolved(decision.canonical)
        expected_observations = (
            1 if subject_only else decision.canonical["cluster_size"]
        )
        if (
            len(observations) != expected_observations
            or sum(item.eligible for item in observations)
            != decision.canonical["eligible_cluster_size"]
        ):
            raise EvidenceIntegrityError("malformed canonical observation")
        if canonical_mint is None:
            valid_winner = not winners
        else:
            valid_winner = (
                len(winners) == 1
                and winners[0].mint == canonical_mint
                and winners[0].eligible
            )
        if not valid_winner:
            raise EvidenceIntegrityError("malformed canonical observation")
    return by_id, by_decision


def _load_primary_ids(
    conn: sqlite3.Connection, decisions: dict[int, _Decision],
) -> tuple[int, ...]:
    primary: list[int] = []
    for row in conn.execute(
        "SELECT generation_hash,first_decision_id,created_at "
        "FROM canonical_generations ORDER BY generation_hash",
    ):
        try:
            generation_hash = row["generation_hash"]
            decision = decisions[row["first_decision_id"]]
            if (
                type(generation_hash) is not str
                or _HASH_RE.fullmatch(generation_hash) is None
                or decision.canonical["generation_hash"] != generation_hash
                or _finite(row["created_at"], minimum=0.0, maximum=4_102_444_800.0)
                != decision.at
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise EvidenceIntegrityError("malformed canonical generation") from exc
        primary.append(decision.decision_id)
    return tuple(primary)


def _load_outcomes(
    conn: sqlite3.Connection,
    *,
    decisions: dict[int, _Decision],
    observations: dict[int, _Observation],
    horizon_s: float,
) -> tuple[dict[int, _Outcome], dict[int, int]]:
    selected: dict[int, _Outcome] = {}
    gaps_by_decision: dict[int, int] = {}
    semantic_keys: set[tuple[int, float]] = set()
    for row in conn.execute(
        "SELECT id,at,ref_id,pnl_sol,detail_json FROM outcomes "
        "WHERE ref_kind='canonical_observation' ORDER BY id",
    ):
        try:
            observation = observations[row["ref_id"]]
            detail = _canonical_json(row["detail_json"])
            if type(detail) is not dict or set(detail) != _OUTCOME_KEYS:
                raise ValueError
            horizon = _finite(
                detail["horizon_s"],
                minimum=math.nextafter(0.0, 1.0), maximum=1e100,
            )
            semantic_key = (observation.observation_id, horizon)
            if semantic_key in semantic_keys or horizon not in decisions[
                observation.decision_id
            ].horizons:
                raise ValueError
            semantic_keys.add(semantic_key)
            outcome_at = _finite(
                row["at"], minimum=0.0, maximum=4_102_444_800.0,
            )
            if (
                outcome_at <= observation.observed_at
                or outcome_at < observation.observed_at + horizon
                or type(row["pnl_sol"]) not in (int, float)
                or not math.isfinite(row["pnl_sol"])
                or row["pnl_sol"] != 0.0
                or detail["price0"] != observation.start_price_sol
                or detail["price0_observed_at"] != observation.price_observed_at
            ):
                raise ValueError
            unavailable_reason = detail["unavailable_reason"]
            terminal = detail["terminal"]
            if unavailable_reason == "":
                measured_return = _finite(
                    detail["forward_return_pct"], minimum=-1e100, maximum=1e100,
                )
                price_now = _finite(
                    detail["price_now"], minimum=0.0, maximum=1e100,
                )
                price_now_at = _finite(
                    detail["price_now_observed_at"],
                    minimum=observation.observed_at,
                    maximum=observation.observed_at + horizon,
                )
                if terminal in ("DEAD", "STALE"):
                    if price_now != 0.0 or measured_return != -100.0:
                        raise ValueError
                elif terminal not in (None, "GRADUATED") or price_now <= 0.0:
                    raise ValueError
                else:
                    expected = 100.0 * (
                        price_now - observation.start_price_sol
                    ) / observation.start_price_sol
                    if not math.isclose(
                        measured_return, expected, rel_tol=1e-12, abs_tol=1e-12,
                    ):
                        raise ValueError
                del price_now_at
            elif unavailable_reason in ("journal_replay_gap", "graduated_no_price"):
                if (
                    detail["forward_return_pct"] is not None
                    or detail["price_now"] is not None
                    or detail["price_now_observed_at"] is not None
                    or (unavailable_reason == "journal_replay_gap" and terminal is not None)
                    or (unavailable_reason == "graduated_no_price" and terminal != "GRADUATED")
                ):
                    raise ValueError
                measured_return = None
            else:
                raise ValueError
        except (KeyError, TypeError, ValueError, ZeroDivisionError, json.JSONDecodeError) as exc:
            raise EvidenceIntegrityError("malformed canonical outcome") from exc
        if horizon == horizon_s:
            selected[observation.observation_id] = _Outcome(
                return_pct=measured_return,
                unavailable_reason=unavailable_reason,
            )
            if unavailable_reason == "journal_replay_gap":
                gaps_by_decision[observation.decision_id] = (
                    gaps_by_decision.get(observation.decision_id, 0) + 1
                )
    return selected, gaps_by_decision


def _execution_counts(
    conn: sqlite3.Connection, decisions: dict[int, _Decision],
) -> tuple[int, int, int, int]:
    canonical_buys = {
        decision_id
        for decision_id, decision in decisions.items()
        if decision.action == "BUY" and decision.canonical["status"] == "CANONICAL"
    }
    counts = {"FILLED": 0, "CANCELLED": 0, "ABANDONED": 0}
    for row in conn.execute(
        "SELECT id,decision_id,at,status,reason,planned_size_sol,"
        "canonical_recheck_id,paper_trade_id FROM paper_entry_executions ORDER BY id",
    ):
        try:
            decision = decisions[row["decision_id"]]
            at = _finite(row["at"], minimum=0.0, maximum=4_102_444_800.0)
            planned = _finite(
                row["planned_size_sol"],
                minimum=math.nextafter(0.0, 1.0), maximum=1e100,
            )
            status = row["status"]
            reason = row["reason"]
            recheck_id = row["canonical_recheck_id"]
            trade_id = row["paper_trade_id"]
            if (
                type(row["id"]) is not int
                or row["id"] <= 0
                or decision.decision_id not in canonical_buys
                or at <= decision.at
                or planned != decision.canonical["planned_size_sol"]
                or type(reason) is not str
                or not reason.strip()
            ):
                raise ValueError
            if status == "FILLED":
                valid_shape = reason == "filled" and type(recheck_id) is int and type(trade_id) is int
            elif status == "CANCELLED":
                valid_shape = type(recheck_id) is int and trade_id is None
            elif status == "ABANDONED":
                valid_shape = (
                    trade_id is None
                    and reason in ("restart_before_fill", "restart_after_pass")
                    and ((reason == "restart_before_fill") == (recheck_id is None))
                )
            else:
                valid_shape = False
            if not valid_shape:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise EvidenceIntegrityError("malformed canonical entry execution") from exc
        counts[status] += 1
    return (
        len(canonical_buys), counts["FILLED"], counts["CANCELLED"], counts["ABANDONED"],
    )


def compute_canonical_metrics(
    conn: sqlite3.Connection, *, horizon_s: float,
) -> CanonicalMetrics:
    """Compute the frozen P3 analysis population without mutating evidence."""
    try:
        horizon = _finite(
            horizon_s, minimum=math.nextafter(0.0, 1.0), maximum=1e100,
        )
    except ValueError as exc:
        raise ValueError("horizon_s must be finite and positive") from exc

    decisions = _load_decisions(conn)
    observations, by_decision = _load_observations(conn, decisions)
    primary_ids = _load_primary_ids(conn, decisions)
    outcomes, gaps_by_decision = _load_outcomes(
        conn,
        decisions=decisions,
        observations=observations,
        horizon_s=horizon,
    )

    potential_pairs = comparable_pairs = 0
    pair_wins = pair_losses = pair_ties = 0
    potential_clusters = comparable_clusters = 0
    cluster_wins = cluster_losses = cluster_ties = harm_clusters = 0
    journal_gap_outcomes = sum(gaps_by_decision.get(item, 0) for item in primary_ids)

    for decision_id in primary_ids:
        eligible = [item for item in by_decision[decision_id] if item.eligible]
        canonical = [item for item in eligible if item.is_canonical]
        if len(canonical) != 1:
            raise EvidenceIntegrityError("malformed canonical primary population")
        canonical_observation = canonical[0]
        peers = [item for item in eligible if not item.is_canonical]
        potential_pairs += len(peers)
        if not peers:
            continue
        potential_clusters += 1
        decision_has_gap = gaps_by_decision.get(decision_id, 0) > 0
        canonical_outcome = outcomes.get(canonical_observation.observation_id)
        comparable_returns: list[float] = []
        for peer in peers:
            peer_outcome = outcomes.get(peer.observation_id)
            if (
                decision_has_gap
                or canonical_observation.start_price_sol is None
                or peer.start_price_sol is None
                or canonical_outcome is None
                or peer_outcome is None
                or canonical_outcome.return_pct is None
                or peer_outcome.return_pct is None
            ):
                continue
            comparable_pairs += 1
            delta = canonical_outcome.return_pct - peer_outcome.return_pct
            if delta > 0.0:
                pair_wins += 1
            elif delta < 0.0:
                pair_losses += 1
            else:
                pair_ties += 1
            comparable_returns.append(peer_outcome.return_pct)
        if len(comparable_returns) != len(peers):
            continue
        comparable_clusters += 1
        best_peer = max(comparable_returns)
        cluster_delta = canonical_outcome.return_pct - best_peer
        if cluster_delta > 0.0:
            cluster_wins += 1
        elif cluster_delta < 0.0:
            cluster_losses += 1
        else:
            cluster_ties += 1
        if canonical_outcome.return_pct <= 0.0 < best_peer:
            harm_clusters += 1

    unresolved_identity = sum(
        decision.canonical["reason"] in (
            "canonical_identity_unavailable", "canonical_identity_conflict",
        )
        for decision in decisions.values()
    )
    unresolved_holder = sum(
        decision.canonical["reason"] == "canonical_holder_evidence_unavailable"
        for decision in decisions.values()
    )
    unresolved_liquidity = sum(
        decision.canonical["reason"] == "canonical_liquidity_unavailable"
        for decision in decisions.values()
    )
    canonical_buys, filled, cancelled, abandoned = _execution_counts(conn, decisions)
    terminal = filled + cancelled + abandoned
    all_decisions = len(decisions)
    return CanonicalMetrics(
        horizon_s=horizon,
        all_p3_decisions=all_decisions,
        primary_decisions=len(primary_ids),
        potential_pairs=potential_pairs,
        comparable_pairs=comparable_pairs,
        pair_wins=pair_wins,
        pair_losses=pair_losses,
        pair_ties=pair_ties,
        potential_clusters=potential_clusters,
        comparable_clusters=comparable_clusters,
        cluster_wins=cluster_wins,
        cluster_losses=cluster_losses,
        cluster_ties=cluster_ties,
        harm_clusters=harm_clusters,
        unresolved_identity=unresolved_identity,
        unresolved_holder=unresolved_holder,
        unresolved_liquidity=unresolved_liquidity,
        journal_gap_outcomes=journal_gap_outcomes,
        canonical_buy_decisions=canonical_buys,
        filled_entries=filled,
        cancelled_entries=cancelled,
        abandoned_entries=abandoned,
        pair_coverage=_ratio(comparable_pairs, potential_pairs),
        cluster_coverage=_ratio(comparable_clusters, potential_clusters),
        harm_rate=_ratio(harm_clusters, comparable_clusters),
        unresolved_identity_rate=_ratio(unresolved_identity, all_decisions),
        unresolved_holder_rate=_ratio(unresolved_holder, all_decisions),
        unresolved_liquidity_rate=_ratio(unresolved_liquidity, all_decisions),
        terminal_coverage=_ratio(terminal, canonical_buys),
        abandonment_rate=_ratio(abandoned, canonical_buys),
    )
