from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import unicodedata
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from memebot.features import CurveSnapshot, FeatureEngine
    from memebot.store import ValidatedSafetyHolder

_HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
_COMPONENT_NAMES = ("first_mover", "liquidity", "holder", "creator", "social")
_SOCIAL_FIELDS = ("uri", "website", "twitter", "telegram")
_MAX_UNIX_TS = Fraction(4_102_444_800)
MAX_PRICE_SOL = Decimal("1e100")
PPM = 1_000_000
WEIGHT_BPS = 10_000
RANK_POINTS_DIVISOR = 100_000_000


@dataclass(frozen=True, slots=True)
class RankedEligibleCandidate:
    mint: str
    rank_points: int
    rank: int


@dataclass(frozen=True, slots=True)
class TargetReportFence:
    causal_report: ValidatedSafetyHolder
    latest_report: ValidatedSafetyHolder | None

    @property
    def is_current(self) -> bool:
        return (
            self.latest_report is not None
            and self.latest_report.safety_report_id
            == self.causal_report.safety_report_id
        )


@dataclass(frozen=True, slots=True)
class CanonicalObservationDraft:
    mint: str
    is_subject: bool
    is_canonical: bool
    eligible: bool
    start_price_sol: float | None
    price_observed_at: float | None
    unavailable_reason: str


@dataclass(frozen=True, slots=True)
class CanonicalVerdict:
    resolver_version: str
    weights_version: str
    status: str
    reason: str
    resolved_at: float
    cluster_key: str
    cluster_size: int
    eligible_cluster_size: int
    canonical_mint: str | None
    rank: int | None
    rank_points: int | None
    generation_hash: str | None
    inputs_hash: str
    ranking_inputs: dict[str, object]


@dataclass(frozen=True, slots=True)
class CanonicalResolution:
    verdict: CanonicalVerdict
    observations: tuple[CanonicalObservationDraft, ...]


@dataclass(slots=True)
class _ResolverCandidate:
    mint: str
    state: str
    rugged: int
    identity_ingested_at: float
    metadata: dict[str, object]
    normalized_name: str
    normalized_symbol: str
    creator: str
    identity_observed_at: dict[str, float]
    identity_conflicts: tuple[str, ...]
    social_values: dict[str, str | None]
    social_metadata_conflicts: frozenset[str]
    durable_source_wall: float | None = None
    durable_source_boot_id: int | None = None
    durable_source_seq: int | None = None
    durable_observed_at: float | None = None
    eligible: bool = False
    ineligible_reason: str = ""
    report: ValidatedSafetyHolder | None = None
    snapshot: CurveSnapshot | None = None
    snapshot_unavailable_reason: str = "start_price_missing"
    creator_prior_successes: int = 0
    creator_prior_rugs: int = 0
    creator_event_ids: tuple[int, ...] = ()
    creator_reputation_eligible: bool = False
    components_ppm: dict[str, int] | None = None
    rank_points: int | None = None
    rank: int | None = None


def _resolver_number(
    value: object,
    *,
    name: str,
    lower: float,
    upper: float = float("inf"),
    strict_lower: bool = False,
) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    result = float(value)
    if (result <= lower if strict_lower else result < lower) or result > upper:
        raise ValueError(f"{name} is out of range")
    return result


def _resolver_positive_int(value: object, *, name: str, upper: int) -> int:
    if type(value) is not int or not 1 <= value <= upper:
        raise ValueError(f"{name} must be a positive bounded integer")
    return value


def _resolver_weight_bps(value: object, *, name: str) -> int:
    number = _decimal(value, name=name)
    if number < 0:
        raise ValueError(f"{name} must be nonnegative")
    scaled = number * WEIGHT_BPS
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{name} must be exact integral BPS")
    return int(scaled)


def _canonical_json_hash(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CanonicalResolver:
    def __init__(
        self,
        conn,
        *,
        feature_engine: FeatureEngine,
        canonical_cfg: Mapping[str, object],
        safety_cfg: Mapping[str, object],
        pumpfun_cfg: Mapping[str, object],
        config_hash: str,
        counterfactual_horizons: Sequence[float],
        runtime_boot_id: int,
        runtime_causal_floor: float,
    ) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise ValueError("conn must be a SQLite connection")
        if not isinstance(canonical_cfg, Mapping):
            raise ValueError("canonical_cfg must be a mapping")
        if not isinstance(safety_cfg, Mapping):
            raise ValueError("safety_cfg must be a mapping")
        if not isinstance(pumpfun_cfg, Mapping):
            raise ValueError("pumpfun_cfg must be a mapping")
        if (
            type(config_hash) is not str
            or re.fullmatch(r"[0-9a-f]{64}", config_hash) is None
        ):
            raise ValueError("config_hash must be lowercase 64-hex")
        if type(runtime_boot_id) is not int or runtime_boot_id <= 0:
            raise ValueError("runtime_boot_id must be positive")

        self._conn = conn
        self._feature_engine = feature_engine
        self._resolver_version = self._config_text(
            canonical_cfg, "resolver_version"
        )
        self._weights_version = self._config_text(
            canonical_cfg, "weights_version"
        )
        live_states = canonical_cfg.get("live_states")
        if (
            not isinstance(live_states, Sequence)
            or isinstance(live_states, (str, bytes))
            or tuple(live_states) != ("FRESH", "CLIMBING")
        ):
            raise ValueError("canonical live_states must be exact")
        self._live_states = tuple(live_states)
        self._max_cluster_candidates = _resolver_positive_int(
            canonical_cfg.get("max_cluster_candidates"),
            name="max_cluster_candidates",
            upper=500,
        )
        self._max_creator_history_mints = _resolver_positive_int(
            canonical_cfg.get("max_creator_history_mints"),
            name="max_creator_history_mints",
            upper=500,
        )
        self._liquidity_max_age_s = _resolver_number(
            canonical_cfg.get("liquidity_max_age_s"),
            name="liquidity_max_age_s",
            lower=0.0,
            strict_lower=True,
        )
        self._holder_max_age_s = _resolver_number(
            canonical_cfg.get("holder_max_age_s"),
            name="holder_max_age_s",
            lower=0.0,
            strict_lower=True,
        )
        self._comparison_price_max_age_s = _resolver_number(
            canonical_cfg.get("comparison_price_max_age_s"),
            name="comparison_price_max_age_s",
            lower=0.0,
            strict_lower=True,
        )
        self._weights_bps = {
            "first_mover": _resolver_weight_bps(
                canonical_cfg.get("w_first_mover"), name="w_first_mover"
            ),
            "liquidity": _resolver_weight_bps(
                canonical_cfg.get("w_liquidity"), name="w_liquidity"
            ),
            "holder": _resolver_weight_bps(
                canonical_cfg.get("w_holder"), name="w_holder"
            ),
            "creator": _resolver_weight_bps(
                canonical_cfg.get("w_creator"), name="w_creator"
            ),
            "social": _resolver_weight_bps(
                canonical_cfg.get("w_social"), name="w_social"
            ),
        }
        _basis_point_weights(
            self._weights_bps, expected_names=_COMPONENT_NAMES
        )
        social_cfg = canonical_cfg.get("social_weights")
        if not isinstance(social_cfg, Mapping):
            raise ValueError("canonical social_weights must be a mapping")
        self._social_weights_bps = {
            field: _resolver_weight_bps(
                social_cfg.get(field), name=f"social_weights.{field}"
            )
            for field in _SOCIAL_FIELDS
        }
        _basis_point_weights(
            self._social_weights_bps, expected_names=_SOCIAL_FIELDS
        )

        self._top10_holder_max_pct = _resolver_number(
            safety_cfg.get("top10_holder_max_pct"),
            name="top10_holder_max_pct",
            lower=0.0,
            upper=100.0,
            strict_lower=True,
        )
        early_buyers = safety_cfg.get("early_buyers")
        if not isinstance(early_buyers, Mapping):
            raise ValueError("safety.early_buyers must be a mapping")
        self._buyer_limit = _resolver_positive_int(
            early_buyers.get("buyer_limit"),
            name="buyer_limit",
            upper=1000,
        )
        self._graduation_sol = _resolver_number(
            pumpfun_cfg.get("graduation_sol"),
            name="graduation_sol",
            lower=0.0,
            strict_lower=True,
        )
        token_decimals = pumpfun_cfg.get("token_decimals")
        if type(token_decimals) is not int or token_decimals < 0:
            raise ValueError("token_decimals must be a nonnegative integer")
        self._token_decimals = token_decimals
        self._config_hash = config_hash
        self._counterfactual_horizons = self._validate_horizons(
            counterfactual_horizons
        )
        self._runtime_boot_id = runtime_boot_id
        self._runtime_causal_floor = _resolver_number(
            runtime_causal_floor,
            name="runtime_causal_floor",
            lower=0.0,
            upper=4_102_444_800.0,
        )
        self._conn.create_function(
            "_memebot_p3_cluster_key",
            2,
            identity_cluster_key,
            deterministic=True,
        )

    @staticmethod
    def _config_text(cfg: Mapping[str, object], key: str) -> str:
        value = cfg.get(key)
        if type(value) is not str or not value.strip():
            raise ValueError(f"{key} must be non-empty text")
        return value

    @staticmethod
    def _validate_horizons(values: Sequence[float]) -> tuple[float, ...]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise ValueError("counterfactual horizons must be a sequence")
        result = tuple(
            _resolver_number(
                value,
                name="counterfactual horizon",
                lower=0.0,
                strict_lower=True,
            )
            for value in values
        )
        if not 1 <= len(result) <= 32 or any(
            right <= left for left, right in zip(result, result[1:])
        ):
            raise ValueError("counterfactual horizons must be bounded and increasing")
        return result

    @staticmethod
    def _validate_decision_at(value: object) -> float:
        return _resolver_number(
            value,
            name="decision_at",
            lower=0.0,
            upper=4_102_444_800.0,
        )

    def _token_is_live(self, row: sqlite3.Row | None, *, at: float) -> bool:
        if row is None:
            return False
        identity_at = row["p3_identity_ingested_at"]
        return (
            type(row["mint"]) is str
            and 1 <= len(row["mint"].strip()) <= 128
            and row["state"] in self._live_states
            and row["rugged"] == 0
            and type(identity_at) in (int, float)
            and math.isfinite(identity_at)
            and 0.0 <= identity_at < at
        )

    @staticmethod
    def _load_metadata(
        row: sqlite3.Row, *, at: float
    ) -> tuple[
        dict[str, object],
        dict[str, float],
        tuple[str, ...],
        str,
        str,
        str,
        dict[str, str | None],
        frozenset[str],
    ]:
        raw = row["meta_json"]
        if type(raw) is not str or len(raw) > 65_536:
            raise ValueError("invalid P3 token metadata")

        def reject_constant(token: str) -> object:
            raise ValueError(f"invalid JSON constant: {token}")

        try:
            metadata = json.loads(raw, parse_constant=reject_constant)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("invalid P3 token metadata") from exc
        tracked = (
            "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
        )
        expected = set(tracked) | {
            "identity_observed_at",
            "identity_conflicts",
            "identity_conflict_observed_at",
        }
        observed_raw = metadata.get("identity_observed_at") if type(metadata) is dict else None
        conflicts_raw = metadata.get("identity_conflicts") if type(metadata) is dict else None
        conflict_times_raw = (
            metadata.get("identity_conflict_observed_at")
            if type(metadata) is dict
            else None
        )
        if (
            type(metadata) is not dict
            or set(metadata) != expected
            or any(type(metadata[field]) is not str for field in tracked)
            or type(observed_raw) is not dict
            or type(conflicts_raw) is not list
            or type(conflict_times_raw) is not dict
            or conflicts_raw != sorted(set(conflicts_raw))
            or any(field not in tracked for field in conflicts_raw)
            or set(conflict_times_raw) != set(conflicts_raw)
            or set(observed_raw)
            != {field for field in tracked if metadata[field].strip()}
        ):
            raise ValueError("invalid P3 token metadata")
        observed: dict[str, float] = {}
        for field, value in observed_raw.items():
            observed[field] = _resolver_number(
                value,
                name=f"identity_observed_at.{field}",
                lower=0.0,
                upper=4_102_444_800.0,
            )
        conflict_times: dict[str, float] = {}
        for field, value in conflict_times_raw.items():
            conflict_times[field] = _resolver_number(
                value,
                name=f"identity_conflict_observed_at.{field}",
                lower=0.0,
                upper=4_102_444_800.0,
            )
            if (
                field not in observed
                or conflict_times[field] <= observed[field]
            ):
                raise ValueError("invalid P3 token metadata")
        effective = {
            field: metadata[field] if observed.get(field, math.inf) <= at else ""
            for field in tracked
        }
        conflicts = tuple(
            field for field in conflicts_raw if conflict_times[field] <= at
        )
        normalized_name = normalize_identity(effective["name"])
        normalized_symbol = normalize_identity(effective["symbol"])
        social_values = {
            "uri": normalize_uri(effective["uri"]),
            "website": normalize_website(effective["website"]),
            "twitter": normalize_twitter(effective["twitter"]),
            "telegram": normalize_telegram(effective["telegram"]),
        }
        return (
            metadata,
            observed,
            conflicts,
            normalized_name,
            normalized_symbol,
            effective["creator"],
            social_values,
            frozenset(field for field in conflicts if field in _SOCIAL_FIELDS),
        )

    def _candidate_from_row(
        self, row: sqlite3.Row, *, at: float
    ) -> _ResolverCandidate:
        (
            metadata,
            observed,
            conflicts,
            normalized_name,
            normalized_symbol,
            creator,
            social_values,
            social_conflicts,
        ) = self._load_metadata(row, at=at)
        return _ResolverCandidate(
            mint=row["mint"],
            state=row["state"],
            rugged=row["rugged"],
            identity_ingested_at=float(row["p3_identity_ingested_at"]),
            metadata=metadata,
            normalized_name=normalized_name,
            normalized_symbol=normalized_symbol,
            creator=creator,
            identity_observed_at={
                field: value for field, value in observed.items() if value <= at
            },
            identity_conflicts=conflicts,
            social_values=social_values,
            social_metadata_conflicts=social_conflicts,
            durable_source_wall=row["curve_progress_source_wall"],
            durable_source_boot_id=row["curve_progress_source_boot_id"],
            durable_source_seq=row["curve_progress_source_seq"],
            durable_observed_at=row["curve_progress_observed_at"],
        )

    def _subject_observation(
        self, *, mint: str, at: float
    ) -> CanonicalObservationDraft:
        snapshot, reason = self._snapshot_for(
            mint=mint, at=at, target_snapshot=None
        )
        return CanonicalObservationDraft(
            mint=mint,
            is_subject=True,
            is_canonical=False,
            eligible=False,
            start_price_sol=None if snapshot is None else snapshot.spot_price_sol,
            price_observed_at=None if snapshot is None else snapshot.t_wall,
            unavailable_reason=reason,
        )

    def _subject_only(
        self,
        *,
        mint: str,
        at: float,
        target_report_id: int,
        reason: str,
        cluster_key: str = "",
        cluster_size: int = 0,
    ) -> CanonicalResolution:
        ranking_inputs = self._ranking_inputs(
            subject_mint=mint,
            target_report_id=target_report_id,
            latest_target_report_id=None,
            resolved_at=at,
            cluster_key=cluster_key,
            candidates=(),
        )
        inputs_hash = _canonical_json_hash(ranking_inputs)
        return CanonicalResolution(
            verdict=CanonicalVerdict(
                resolver_version=self._resolver_version,
                weights_version=self._weights_version,
                status="UNRESOLVED",
                reason=reason,
                resolved_at=at,
                cluster_key=cluster_key,
                cluster_size=cluster_size,
                eligible_cluster_size=0,
                canonical_mint=None,
                rank=None,
                rank_points=None,
                generation_hash=None,
                inputs_hash=inputs_hash,
                ranking_inputs=ranking_inputs,
            ),
            observations=(self._subject_observation(mint=mint, at=at),),
        )

    def _bounded_cluster(
        self, *, cluster_key: str, at: float
    ) -> list[_ResolverCandidate]:
        placeholders = ",".join("?" for _ in self._live_states)
        rows = self._conn.execute(
            "SELECT mint,state,rugged,p3_identity_ingested_at,meta_json,"  # nosec B608
            "curve_progress,curve_progress_observed_at,"
            "curve_progress_source_wall,curve_progress_source_boot_id,"
            "curve_progress_source_seq,"
            "curve_progress_virtual_sol_reserves,"
            "curve_progress_virtual_token_reserves,"
            "curve_progress_real_sol_reserves,"
            "curve_progress_real_token_reserves "
            "FROM tokens WHERE state IN ("
            f"{placeholders}"
            ") AND rugged=0 AND typeof(mint)='text' "
            "AND length(trim(mint)) BETWEEN 1 AND 128 "
            "AND typeof(p3_identity_ingested_at) IN ('integer','real') "
            "AND p3_identity_ingested_at>=0.0 AND p3_identity_ingested_at<? "
            "AND CASE WHEN json_valid(meta_json)=1 THEN "
            "_memebot_p3_cluster_key("
            "json_extract(meta_json,'$.name'),"
            "json_extract(meta_json,'$.symbol')) ELSE NULL END=? "
            "ORDER BY mint LIMIT ?",
            (
                *self._live_states,
                at,
                cluster_key,
                self._max_cluster_candidates + 1,
            ),
        ).fetchall()
        candidates: list[_ResolverCandidate] = []
        for row in rows:
            try:
                candidates.append(self._candidate_from_row(row, at=at))
            except (TypeError, ValueError):
                candidates.append(
                    _ResolverCandidate(
                        mint=row["mint"],
                        state=row["state"],
                        rugged=row["rugged"],
                        identity_ingested_at=float(row["p3_identity_ingested_at"]),
                        metadata={},
                        normalized_name="",
                        normalized_symbol="",
                        creator="",
                        identity_observed_at={},
                        identity_conflicts=(),
                        social_values={field: None for field in _SOCIAL_FIELDS},
                        social_metadata_conflicts=frozenset(),
                        ineligible_reason="canonical_internal_error",
                    )
                )
        return candidates

    @staticmethod
    def _snapshot_valid(snapshot: object, *, at: float) -> bool:
        from memebot.features import CurveSnapshot

        if not isinstance(snapshot, CurveSnapshot):
            return False
        timestamps = (snapshot.t_wall, snapshot.t_mono)
        numeric = (
            snapshot.liquidity_sol,
            snapshot.spot_price_sol,
            snapshot.progress_pct,
        )
        reserves = (
            snapshot.virtual_sol_reserves,
            snapshot.virtual_token_reserves,
            snapshot.real_sol_reserves,
            snapshot.real_token_reserves,
        )
        return (
            all(type(value) in (int, float) and math.isfinite(value) for value in timestamps)
            and 0.0 <= snapshot.t_wall <= at
            and all(type(value) in (int, float) and math.isfinite(value) for value in numeric)
            and 0.0 <= snapshot.liquidity_sol <= 1e100
            and 0.0 < snapshot.spot_price_sol <= 1e100
            and 0.0 <= snapshot.progress_pct <= 100.0
            and all(type(value) is int and value >= 0 for value in reserves)
            and snapshot.virtual_sol_reserves > 0
            and snapshot.virtual_token_reserves > 0
        )

    def _snapshot_for(
        self,
        *,
        mint: str,
        at: float,
        target_snapshot: CurveSnapshot | None,
        candidate: _ResolverCandidate | None = None,
    ) -> tuple[CurveSnapshot | None, str]:
        try:
            if target_snapshot is not None:
                snapshot = target_snapshot
            elif candidate is not None:
                snapshot = self._feature_engine.p3_snapshot_at_or_before(
                    mint,
                    as_of=at,
                    durable_source_wall=candidate.durable_source_wall,
                    durable_source_boot_id=candidate.durable_source_boot_id,
                    durable_source_seq=candidate.durable_source_seq,
                    durable_observed_at=candidate.durable_observed_at,
                    runtime_boot_id=self._runtime_boot_id,
                    runtime_causal_floor=self._runtime_causal_floor,
                )
            else:
                snapshot = self._feature_engine.snapshot_at_or_before(
                    mint, as_of=at
                )
        except Exception:
            return None, "start_price_malformed"
        if snapshot is None:
            return None, "start_price_missing"
        if not self._snapshot_valid(snapshot, at=at):
            return None, "start_price_malformed"
        if at - snapshot.t_wall > self._comparison_price_max_age_s:
            return None, "start_price_stale"
        return snapshot, ""

    def _holder_available(
        self, report: ValidatedSafetyHolder, *, at: float
    ) -> bool:
        return (
            report.holder is not None
            and report.holder_evidence_id is not None
            and not report.holder_unavailable_reason
            and not report.holder.unavailable_reason
            and at - report.holder.holder_observed_at <= self._holder_max_age_s
        )

    def _liquidity_available(
        self, candidate: _ResolverCandidate, *, at: float
    ) -> bool:
        return (
            candidate.snapshot is not None
            and at - candidate.snapshot.t_wall <= self._liquidity_max_age_s
        )

    def _target_early_buyer_available(
        self, *, report_id: int, mint: str, at: float
    ) -> bool:
        from memebot.store import validated_early_buyer_for_report

        selected = validated_early_buyer_for_report(
            self._conn,
            report_id=report_id,
            expected_mint=mint,
            as_of=at,
        )
        return selected is not None and len(selected[1].buyers) <= self._buyer_limit

    def _creator_evidence(
        self, candidate: _ResolverCandidate, *, at: float
    ) -> str:
        from memebot.store import (
            reputation_creator_eligible,
            validated_creator_reputation_current,
        )

        conflicted = "creator" in candidate.identity_conflicts
        candidate.creator_reputation_eligible = reputation_creator_eligible(
            candidate.creator,
            conflicted=conflicted,
        )
        if not candidate.creator_reputation_eligible:
            return ""
        try:
            result = validated_creator_reputation_current(
                self._conn,
                creator=candidate.creator,
                candidate_mint=candidate.mint,
                as_of=at,
                max_creator_history_mints=self._max_creator_history_mints,
            )
        except Exception:
            return "canonical_internal_error"
        if result.unavailable_reason == "creator_history_overflow":
            return "canonical_creator_history_overflow"
        if result.unavailable_reason:
            return "canonical_internal_error"
        candidate.creator_prior_successes = result.prior_successes
        candidate.creator_prior_rugs = result.prior_rugs
        candidate.creator_event_ids = result.selected_event_ids
        return ""

    def _mark_peer_evidence(
        self, candidate: _ResolverCandidate, *, at: float
    ) -> None:
        from memebot.store import validated_latest_report_as_of

        if (
            not candidate.normalized_name
            or not candidate.normalized_symbol
            or {"name", "symbol"} & set(candidate.identity_conflicts)
        ):
            candidate.ineligible_reason = "canonical_identity_conflict"
            return
        report = validated_latest_report_as_of(
            self._conn, mint=candidate.mint, as_of=at
        )
        if report is None:
            candidate.ineligible_reason = "canonical_safety_unavailable"
            return
        candidate.report = report
        if report.hard_fails:
            candidate.ineligible_reason = "canonical_safety_hard_fail"
            return
        if not self._holder_available(report, at=at):
            candidate.ineligible_reason = "canonical_holder_evidence_unavailable"
            return
        if not self._liquidity_available(candidate, at=at):
            candidate.ineligible_reason = "canonical_liquidity_unavailable"
            return
        creator_reason = self._creator_evidence(candidate, at=at)
        if creator_reason:
            candidate.ineligible_reason = creator_reason
            return
        candidate.eligible = True

    def _score_candidates(self, candidates: Sequence[_ResolverCandidate]) -> None:
        eligible = [candidate for candidate in candidates if candidate.eligible]
        eligible_pairs = tuple(
            (candidate.identity_ingested_at, candidate.mint)
            for candidate in eligible
        )
        social_rows = tuple(candidate.social_values for candidate in eligible)
        for candidate in eligible:
            holder = candidate.report.holder if candidate.report is not None else None
            if holder is None or candidate.snapshot is None:
                candidate.eligible = False
                candidate.ineligible_reason = "canonical_internal_error"
                continue
            components = {
                "first_mover": first_mover_component(
                    identity_ingested_at=candidate.identity_ingested_at,
                    mint=candidate.mint,
                    eligible_pairs=eligible_pairs,
                ),
                "liquidity": liquidity_component(
                    real_sol_locked=candidate.snapshot.liquidity_sol,
                    curve_progress_pct=candidate.snapshot.progress_pct,
                    graduation_sol=self._graduation_sol,
                ),
                "holder": holder_component(
                    distinct_non_curve_owners=holder.distinct_non_curve_owners,
                    top10_share_pct=holder.top10_non_curve_owner_share_pct,
                    top10_holder_max_pct=self._top10_holder_max_pct,
                ),
                "creator": creator_component(
                    creator=(
                        candidate.creator
                        if candidate.creator_reputation_eligible
                        else None
                    ),
                    creator_conflicted="creator" in candidate.identity_conflicts,
                    prior_successes=candidate.creator_prior_successes,
                    prior_rugs=candidate.creator_prior_rugs,
                ),
                "social": social_component(
                    candidate_values=candidate.social_values,
                    eligible_values=social_rows,
                    metadata_conflicts=candidate.social_metadata_conflicts,
                    social_weights_bps=self._social_weights_bps,
                ),
            }
            candidate.components_ppm = {
                name: quantize_component(value)
                for name, value in components.items()
            }
            candidate.rank_points = integer_rank_points(
                components=components, weights_bps=self._weights_bps
            )
        ranked = rank_eligible_candidates(
            tuple(
                {
                    "mint": candidate.mint,
                    "p3_identity_ingested_at": candidate.identity_ingested_at,
                    "rank_points": candidate.rank_points,
                }
                for candidate in candidates
                if candidate.eligible and candidate.rank_points is not None
            )
        )
        by_mint = {candidate.mint: candidate for candidate in candidates}
        for result in ranked:
            candidate = by_mint[result.mint]
            candidate.rank = result.rank
            candidate.rank_points = result.rank_points

    def _social_diagnostics(
        self,
        candidate: _ResolverCandidate,
        eligible: Sequence[_ResolverCandidate],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for field in _SOCIAL_FIELDS:
            value = candidate.social_values[field]
            values = [
                peer.social_values[field]
                for peer in eligible
                if peer.social_values[field] is not None
            ]
            result[field] = {
                "value": value,
                "present": value is not None,
                "reuse": value is not None and values.count(value) > 1,
                "cluster_conflict": len(set(values)) > 1,
                "metadata_conflict": field in candidate.social_metadata_conflicts,
            }
        return result

    @staticmethod
    def _snapshot_payload(snapshot: CurveSnapshot | None) -> dict[str, object] | None:
        if snapshot is None:
            return None
        return {
            "t_wall": snapshot.t_wall,
            "t_mono": snapshot.t_mono,
            "virtual_sol_reserves": snapshot.virtual_sol_reserves,
            "virtual_token_reserves": snapshot.virtual_token_reserves,
            "real_sol_reserves": snapshot.real_sol_reserves,
            "real_token_reserves": snapshot.real_token_reserves,
            "spot_price_sol": snapshot.spot_price_sol,
        }

    def _candidate_payload(
        self,
        candidate: _ResolverCandidate,
        *,
        eligible: Sequence[_ResolverCandidate],
    ) -> dict[str, object]:
        report = candidate.report if candidate.eligible else None
        holder = report.holder if report is not None else None
        snapshot = candidate.snapshot if candidate.eligible else None
        raw = (
            {
                "liquidity_sol": None,
                "curve_progress_pct": None,
                "curve_snapshot": None,
                "sampled_token_accounts": None,
                "distinct_non_curve_owners": None,
                "top10_non_curve_owner_share_pct": None,
                "creator_prior_successes": None,
                "creator_prior_rugs": None,
                "creator_reputation_event_ids": None,
                "social": None,
            }
            if not candidate.eligible
            else {
                "liquidity_sol": snapshot.liquidity_sol,
                "curve_progress_pct": snapshot.progress_pct,
                "curve_snapshot": self._snapshot_payload(snapshot),
                "sampled_token_accounts": holder.sampled_token_accounts,
                "distinct_non_curve_owners": holder.distinct_non_curve_owners,
                "top10_non_curve_owner_share_pct": (
                    holder.top10_non_curve_owner_share_pct
                ),
                "creator_prior_successes": candidate.creator_prior_successes,
                "creator_prior_rugs": candidate.creator_prior_rugs,
                "creator_reputation_event_ids": list(candidate.creator_event_ids),
                "social": self._social_diagnostics(candidate, eligible),
            }
        )
        return {
            "mint": candidate.mint,
            "p3_identity_ingested_at": candidate.identity_ingested_at,
            "state": candidate.state,
            "rugged": candidate.rugged,
            "normalized_name": candidate.normalized_name,
            "normalized_symbol": candidate.normalized_symbol,
            "creator": candidate.creator,
            "identity_observed_at": candidate.identity_observed_at,
            "identity_conflicts": list(candidate.identity_conflicts),
            "eligible": candidate.eligible,
            "ineligible_reason": candidate.ineligible_reason,
            "safety_report_id": None if report is None else report.safety_report_id,
            "safety_checked_at": None if report is None else report.checked_at,
            "safety_inputs_hash": None if report is None else report.safety_inputs_hash,
            "safety_hard_fails": (
                None if report is None else list(report.hard_fails)
            ),
            "safety_risk_score": None if report is None else report.risk_score,
            "holder_evidence_id": (
                None if report is None else report.holder_evidence_id
            ),
            "holder_inputs_hash": None if holder is None else holder.inputs_hash,
            "holder_observed_at": (
                None if holder is None else holder.holder_observed_at
            ),
            "liquidity_source": (
                "curve_snapshot" if snapshot is not None else None
            ),
            "liquidity_observed_at": (
                None if snapshot is None else snapshot.t_wall
            ),
            "raw": raw,
            "components_ppm": candidate.components_ppm or {},
            "rank_points": candidate.rank_points,
            "rank": candidate.rank,
        }

    def _ranking_inputs(
        self,
        *,
        subject_mint: str,
        target_report_id: int | None,
        latest_target_report_id: int | None,
        resolved_at: float,
        cluster_key: str,
        candidates: Sequence[_ResolverCandidate],
    ) -> dict[str, object]:
        eligible = tuple(candidate for candidate in candidates if candidate.eligible)
        return {
            "subject_mint": subject_mint,
            "target_report_id": target_report_id,
            "latest_target_report_id": latest_target_report_id,
            "resolved_at": resolved_at,
            "cluster_key": cluster_key,
            "resolver_version": self._resolver_version,
            "weights_version": self._weights_version,
            "config_hash": self._config_hash,
            "counterfactual_horizons_s": list(self._counterfactual_horizons),
            "limits": {
                "max_cluster_candidates": self._max_cluster_candidates,
                "liquidity_max_age_s": self._liquidity_max_age_s,
                "holder_max_age_s": self._holder_max_age_s,
                "comparison_price_max_age_s": self._comparison_price_max_age_s,
            },
            "component_parameters": {
                "graduation_sol": self._graduation_sol,
                "holder_owner_target": 20,
                "top10_holder_max_pct": self._top10_holder_max_pct,
                "token_decimals": self._token_decimals,
                "creator_reputation_as_of": resolved_at,
            },
            "weights_bps": dict(self._weights_bps),
            "social_weights_bps": dict(self._social_weights_bps),
            "candidates": [
                self._candidate_payload(candidate, eligible=eligible)
                for candidate in sorted(candidates, key=lambda item: item.mint)
            ],
        }

    def _observations(
        self,
        *,
        subject_mint: str,
        canonical_mint: str | None,
        candidates: Sequence[_ResolverCandidate],
    ) -> tuple[CanonicalObservationDraft, ...]:
        return tuple(
            CanonicalObservationDraft(
                mint=candidate.mint,
                is_subject=candidate.mint == subject_mint,
                is_canonical=candidate.mint == canonical_mint,
                eligible=candidate.eligible,
                start_price_sol=(
                    None
                    if candidate.snapshot is None
                    else candidate.snapshot.spot_price_sol
                ),
                price_observed_at=(
                    None if candidate.snapshot is None else candidate.snapshot.t_wall
                ),
                unavailable_reason=(
                    candidate.snapshot_unavailable_reason
                    if candidate.snapshot is None
                    else ""
                ),
            )
            for candidate in sorted(candidates, key=lambda item: item.mint)
        )

    def resolve(
        self,
        mint: str,
        *,
        decision_at: float,
        target_report_id: int,
        target_snapshot: CurveSnapshot | None = None,
    ) -> CanonicalResolution:
        if type(mint) is not str or not 1 <= len(mint.strip()) <= 128:
            raise ValueError("mint must be non-empty bounded text")
        at = self._validate_decision_at(decision_at)
        if type(target_report_id) is not int or target_report_id <= 0:
            raise ValueError("target_report_id must be a positive integer")

        target_row = self._conn.execute(
            "SELECT mint,state,rugged,p3_identity_ingested_at,meta_json,"
            "curve_progress,curve_progress_observed_at,"
            "curve_progress_source_wall,curve_progress_source_boot_id,"
            "curve_progress_source_seq,"
            "curve_progress_virtual_sol_reserves,"
            "curve_progress_virtual_token_reserves,"
            "curve_progress_real_sol_reserves,"
            "curve_progress_real_token_reserves "
            "FROM tokens WHERE mint=?",
            (mint,),
        ).fetchone()
        if not self._token_is_live(target_row, at=at):
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_target_not_live",
            )
        try:
            target = self._candidate_from_row(target_row, at=at)
        except (TypeError, ValueError):
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_internal_error",
            )
        if not target.normalized_name or not target.normalized_symbol:
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_identity_unavailable",
            )
        cluster_key = f"{target.normalized_name}:{target.normalized_symbol}"
        if {"name", "symbol"} & set(target.identity_conflicts):
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_identity_conflict",
                cluster_key=cluster_key,
            )
        try:
            candidates = self._bounded_cluster(cluster_key=cluster_key, at=at)
        except (sqlite3.Error, TypeError, ValueError):
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_internal_error",
                cluster_key=cluster_key,
            )
        if len(candidates) > self._max_cluster_candidates:
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_cluster_too_large",
                cluster_key=cluster_key,
                cluster_size=len(candidates),
            )
        by_mint = {candidate.mint: candidate for candidate in candidates}
        if mint not in by_mint:
            return self._subject_only(
                mint=mint,
                at=at,
                target_report_id=target_report_id,
                reason="canonical_internal_error",
                cluster_key=cluster_key,
            )
        target = by_mint[mint]

        target_failure = ""
        latest_target_report_id: int | None = None
        try:
            fence = target_report_fence(
                self._conn,
                mint=mint,
                decision_at=at,
                target_report_id=target_report_id,
            )
            target.report = fence.causal_report
            latest_target_report_id = (
                None
                if fence.latest_report is None
                else fence.latest_report.safety_report_id
            )
            if not fence.is_current:
                target_failure = "canonical_target_report_superseded"
            elif fence.causal_report.hard_fails:
                target_failure = "canonical_internal_error"
            elif not self._holder_available(fence.causal_report, at=at):
                target_failure = "canonical_holder_evidence_unavailable"
            elif not self._target_early_buyer_available(
                report_id=target_report_id, mint=mint, at=at
            ):
                target_failure = "canonical_internal_error"
        except Exception:
            target_failure = "canonical_internal_error"

        for candidate in candidates:
            snapshot, snapshot_reason = self._snapshot_for(
                mint=candidate.mint,
                at=at,
                target_snapshot=(
                    target_snapshot if candidate.mint == mint else None
                ),
                candidate=candidate,
            )
            candidate.snapshot = snapshot
            candidate.snapshot_unavailable_reason = snapshot_reason

        for candidate in candidates:
            if candidate.mint == mint:
                if target_failure:
                    candidate.ineligible_reason = target_failure
                    continue
                if not self._liquidity_available(candidate, at=at):
                    target_failure = (
                        "canonical_internal_error"
                        if candidate.snapshot_unavailable_reason
                        == "start_price_malformed"
                        else "canonical_liquidity_unavailable"
                    )
                    candidate.ineligible_reason = target_failure
                    continue
                creator_reason = self._creator_evidence(candidate, at=at)
                if creator_reason:
                    target_failure = creator_reason
                    candidate.ineligible_reason = creator_reason
                    continue
                candidate.eligible = True
            else:
                try:
                    self._mark_peer_evidence(candidate, at=at)
                except Exception:
                    candidate.eligible = False
                    candidate.ineligible_reason = "canonical_internal_error"

        try:
            self._score_candidates(candidates)
        except Exception:
            target_failure = "canonical_internal_error"
            target.eligible = False
            target.ineligible_reason = target_failure
            target.components_ppm = None
            target.rank_points = None
            target.rank = None
            for candidate in candidates:
                if candidate.mint != mint:
                    candidate.eligible = False
                    candidate.ineligible_reason = "canonical_internal_error"
                    candidate.components_ppm = None
                    candidate.rank_points = None
                    candidate.rank = None

        ranked = sorted(
            (
                candidate
                for candidate in candidates
                if candidate.eligible and candidate.rank is not None
            ),
            key=lambda candidate: candidate.rank,
        )
        canonical_mint = ranked[0].mint if ranked else None
        generation_hash = (
            None
            if canonical_mint is None
            else canonical_generation_hash(
                cluster_key=cluster_key,
                eligible=tuple(
                    {
                        "mint": candidate.mint,
                        "safety_report_id": candidate.report.safety_report_id,
                        "holder_evidence_id": candidate.report.holder_evidence_id,
                    }
                    for candidate in ranked
                ),
                canonical_mint=canonical_mint,
                resolver_version=self._resolver_version,
                weights_version=self._weights_version,
                config_hash=self._config_hash,
            )
        )
        ranking_inputs = self._ranking_inputs(
            subject_mint=mint,
            target_report_id=target_report_id,
            latest_target_report_id=latest_target_report_id,
            resolved_at=at,
            cluster_key=cluster_key,
            candidates=candidates,
        )
        inputs_hash = _canonical_json_hash(ranking_inputs)
        if target_failure:
            status = "UNRESOLVED"
            reason = target_failure
            target_rank = None
            target_points = None
        elif target.rank == 1:
            status = "CANONICAL"
            reason = "canonical_selected"
            target_rank = target.rank
            target_points = target.rank_points
        else:
            status = "SUPPRESSED"
            reason = "copycat_cluster"
            target_rank = target.rank
            target_points = target.rank_points
        return CanonicalResolution(
            verdict=CanonicalVerdict(
                resolver_version=self._resolver_version,
                weights_version=self._weights_version,
                status=status,
                reason=reason,
                resolved_at=at,
                cluster_key=cluster_key,
                cluster_size=len(candidates),
                eligible_cluster_size=len(ranked),
                canonical_mint=canonical_mint,
                rank=target_rank,
                rank_points=target_points,
                generation_hash=generation_hash,
                inputs_hash=inputs_hash,
                ranking_inputs=ranking_inputs,
            ),
            observations=self._observations(
                subject_mint=mint,
                canonical_mint=canonical_mint,
                candidates=candidates,
            ),
        )


def target_report_fence(
    conn: sqlite3.Connection,
    *,
    mint: str,
    decision_at: float,
    target_report_id: int,
) -> TargetReportFence:
    from memebot.store import (
        validated_latest_report_as_of,
        validated_report_by_id,
    )

    if (
        type(decision_at) not in (int, float)
        or not 0.0 <= decision_at <= 4_102_444_800.0
        or not math.isfinite(decision_at)
    ):
        raise ValueError("invalid p3 causal wall")
    causal_report = validated_report_by_id(
        conn,
        report_id=target_report_id,
        expected_mint=mint,
    )
    latest_report = validated_latest_report_as_of(
        conn,
        mint=mint,
        as_of=decision_at,
    )
    return TargetReportFence(
        causal_report=causal_report,
        latest_report=latest_report,
    )


def _decimal(value: object, *, name: str) -> Decimal:
    if isinstance(value, bool) or type(value) not in (int, float, Decimal):
        raise ValueError(f"{name} must be a finite number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a finite number") from None
    if not result.is_finite():
        raise ValueError(f"{name} must be a finite number")
    return result


def _fraction(value: object, *, name: str) -> Fraction:
    if isinstance(value, Fraction):
        return value
    return Fraction(_decimal(value, name=name))


def _unit_interval(value: Fraction) -> Fraction:
    return min(Fraction(1), max(Fraction(0), value))


def first_mover_component(
    *,
    identity_ingested_at: object,
    mint: str,
    eligible_pairs: Sequence[tuple[object, str]],
) -> Fraction:
    candidate_at = _fraction(identity_ingested_at, name="identity_ingested_at")
    if not Fraction(0) <= candidate_at <= _MAX_UNIX_TS:
        raise ValueError("identity_ingested_at is out of range")
    if type(mint) is not str or not mint.strip():
        raise ValueError("mint must be non-empty text")

    pairs: list[tuple[Fraction, str]] = []
    for raw_pair in eligible_pairs:
        if type(raw_pair) is not tuple or len(raw_pair) != 2:
            raise ValueError("eligible_pairs must contain timestamp/mint tuples")
        raw_at, raw_mint = raw_pair
        at = _fraction(raw_at, name="eligible identity_ingested_at")
        if not Fraction(0) <= at <= _MAX_UNIX_TS:
            raise ValueError("eligible identity_ingested_at is out of range")
        if type(raw_mint) is not str or not raw_mint.strip():
            raise ValueError("eligible mint must be non-empty text")
        pairs.append((at, raw_mint))

    candidate = (candidate_at, mint)
    if not pairs or candidate not in pairs:
        raise ValueError("candidate must be present in eligible_pairs")
    return Fraction(1) if candidate == min(pairs) else Fraction(0)


def liquidity_component(
    *,
    real_sol_locked: object | None,
    curve_progress_pct: object | None,
    graduation_sol: object,
) -> Fraction:
    graduation = _fraction(graduation_sol, name="graduation_sol")
    if graduation <= 0:
        raise ValueError("graduation_sol must be greater than zero")

    liquidity = None
    if real_sol_locked is not None:
        liquidity = _fraction(real_sol_locked, name="real_sol_locked")
        if not Fraction(0) <= liquidity <= Fraction(MAX_PRICE_SOL):
            raise ValueError("real_sol_locked must be between zero and 1e100")

    progress = None
    if curve_progress_pct is not None:
        progress = _fraction(curve_progress_pct, name="curve_progress_pct")
        if not Fraction(0) <= progress <= Fraction(100):
            raise ValueError("curve_progress_pct must be between zero and 100")

    if liquidity is not None:
        return _unit_interval(liquidity / graduation)
    if progress is not None:
        return _unit_interval(progress / Fraction(100))
    raise ValueError("liquidity evidence is unavailable")


def holder_component(
    *,
    distinct_non_curve_owners: object,
    top10_share_pct: object,
    top10_holder_max_pct: object,
) -> Fraction:
    if (
        type(distinct_non_curve_owners) is not int
        or distinct_non_curve_owners < 1
    ):
        raise ValueError("distinct_non_curve_owners must be a positive integer")
    top10_share = _fraction(top10_share_pct, name="top10_share_pct")
    top10_max = _fraction(top10_holder_max_pct, name="top10_holder_max_pct")
    if not Fraction(0) <= top10_share <= Fraction(100):
        raise ValueError("top10_share_pct must be between zero and 100")
    if not Fraction(0) < top10_max <= Fraction(100):
        raise ValueError("top10_holder_max_pct must be greater than zero and at most 100")

    owner_count = _unit_interval(Fraction(distinct_non_curve_owners, 20))
    concentration = _unit_interval((top10_max - top10_share) / top10_max)
    return (owner_count + concentration) / Fraction(2)


def creator_component(
    *,
    creator: object,
    creator_conflicted: bool,
    prior_successes: object,
    prior_rugs: object,
) -> Fraction:
    if type(creator_conflicted) is not bool:
        raise ValueError("creator_conflicted must be a boolean")
    if type(prior_successes) is not int or prior_successes < 0:
        raise ValueError("prior_successes must be a nonnegative integer")
    if type(prior_rugs) is not int or prior_rugs < 0:
        raise ValueError("prior_rugs must be a nonnegative integer")
    if type(creator) is not str or not creator.strip() or creator_conflicted:
        return Fraction(0)
    return Fraction(prior_successes + 1, prior_successes + prior_rugs + 2)


def _basis_point_weights(
    weights: Mapping[str, object], *, expected_names: tuple[str, ...]
) -> dict[str, int]:
    if set(weights) != set(expected_names):
        raise ValueError("weight names do not match the ranking contract")
    result: dict[str, int] = {}
    for name in expected_names:
        value = weights[name]
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} weight must be nonnegative integer BPS")
        result[name] = value
    if sum(result.values()) != WEIGHT_BPS:
        raise ValueError("weights must sum to exactly 10,000 BPS")
    return result


def social_component(
    *,
    candidate_values: Mapping[str, str | None],
    eligible_values: Sequence[Mapping[str, str | None]],
    metadata_conflicts: Collection[str],
    social_weights_bps: Mapping[str, object],
) -> Fraction:
    weights = _basis_point_weights(
        social_weights_bps, expected_names=_SOCIAL_FIELDS
    )
    if set(candidate_values) != set(_SOCIAL_FIELDS):
        raise ValueError("candidate social fields do not match the ranking contract")
    conflicts = set(metadata_conflicts)
    if any(type(field) is not str for field in conflicts) or not conflicts <= set(
        _SOCIAL_FIELDS
    ):
        raise ValueError("metadata conflicts contain an unknown social field")

    rows = tuple(eligible_values)
    for row in (candidate_values, *rows):
        if set(row) != set(_SOCIAL_FIELDS):
            raise ValueError("eligible social fields do not match the ranking contract")
        if any(value is not None and (type(value) is not str or not value) for value in row.values()):
            raise ValueError("social values must be normalized non-empty text or None")

    score = Fraction(0)
    for field in _SOCIAL_FIELDS:
        candidate_value = candidate_values[field]
        if candidate_value is None or field in conflicts:
            continue
        present = [row[field] for row in rows if row[field] is not None]
        if not present:
            continue
        score += Fraction(
            weights[field] * present.count(candidate_value),
            WEIGHT_BPS * len(present),
        )
    return score


def quantize_component(value: object) -> int:
    component = _fraction(value, name="component")
    if not Fraction(0) <= component <= Fraction(1):
        raise ValueError("component must be between zero and one")
    whole, remainder = divmod(component.numerator * PPM, component.denominator)
    return whole + int(2 * remainder >= component.denominator)


def integer_rank_points(
    *, components: Mapping[str, object], weights_bps: Mapping[str, object]
) -> int:
    weights = _basis_point_weights(weights_bps, expected_names=_COMPONENT_NAMES)
    if set(components) != set(_COMPONENT_NAMES):
        raise ValueError("component names do not match the ranking contract")
    return sum(
        weights[name] * quantize_component(components[name])
        for name in _COMPONENT_NAMES
    )


def rank_points_to_human(points: int) -> Decimal:
    if type(points) is not int or not 0 <= points <= WEIGHT_BPS * PPM:
        raise ValueError("rank points are out of range")
    whole, fractional = divmod(points, RANK_POINTS_DIVISOR)
    return Decimal(f"{whole}.{fractional:08d}")


def rank_eligible_candidates(
    candidates: Sequence[Mapping[str, object]],
) -> tuple[RankedEligibleCandidate, ...]:
    sortable: list[tuple[int, Fraction, str]] = []
    seen_mints: set[str] = set()
    for candidate in candidates:
        mint = candidate.get("mint")
        rank_points = candidate.get("rank_points")
        identity_ingested_at = _fraction(
            candidate.get("p3_identity_ingested_at"),
            name="p3_identity_ingested_at",
        )
        if type(mint) is not str or not mint.strip():
            raise ValueError("mint must be non-empty text")
        if mint in seen_mints:
            raise ValueError("eligible candidate mints must be unique")
        if (
            type(rank_points) is not int
            or not 0 <= rank_points <= WEIGHT_BPS * PPM
        ):
            raise ValueError("rank points are out of range")
        if not Fraction(0) <= identity_ingested_at <= _MAX_UNIX_TS:
            raise ValueError("p3_identity_ingested_at is out of range")
        seen_mints.add(mint)
        sortable.append((rank_points, identity_ingested_at, mint))

    sortable.sort(key=lambda item: (-item[0], item[1], item[2]))
    return tuple(
        RankedEligibleCandidate(mint=mint, rank_points=rank_points, rank=rank)
        for rank, (rank_points, _, mint) in enumerate(sortable, start=1)
    )


def canonical_generation_hash(
    *,
    cluster_key: str,
    eligible: Sequence[Mapping[str, object]],
    canonical_mint: str,
    resolver_version: str,
    weights_version: str,
    config_hash: str,
) -> str:
    for name, value in (
        ("cluster_key", cluster_key),
        ("canonical_mint", canonical_mint),
        ("resolver_version", resolver_version),
        ("weights_version", weights_version),
    ):
        if type(value) is not str or not value:
            raise ValueError(f"{name} must be non-empty text")
    if (
        type(config_hash) is not str
        or re.fullmatch(r"[0-9a-f]{64}", config_hash) is None
    ):
        raise ValueError("config_hash must be lowercase 64-hex")

    eligible_payload: list[dict[str, object]] = []
    seen_mints: set[str] = set()
    expected_keys = {"mint", "safety_report_id", "holder_evidence_id"}
    for candidate in eligible:
        if set(candidate) != expected_keys:
            raise ValueError("eligible generation evidence has unexpected keys")
        mint = candidate["mint"]
        safety_report_id = candidate["safety_report_id"]
        holder_evidence_id = candidate["holder_evidence_id"]
        if type(mint) is not str or not mint.strip() or mint in seen_mints:
            raise ValueError("eligible generation mints must be unique non-empty text")
        if type(safety_report_id) is not int or safety_report_id <= 0:
            raise ValueError("safety_report_id must be a positive integer")
        if type(holder_evidence_id) is not int or holder_evidence_id <= 0:
            raise ValueError("holder_evidence_id must be a positive integer")
        seen_mints.add(mint)
        eligible_payload.append(
            {
                "mint": mint,
                "safety_report_id": safety_report_id,
                "holder_evidence_id": holder_evidence_id,
            }
        )
    if canonical_mint not in seen_mints:
        raise ValueError("canonical_mint must identify an eligible candidate")

    payload = {
        "cluster_key": cluster_key,
        "eligible": sorted(eligible_payload, key=lambda candidate: candidate["mint"]),
        "canonical_mint": canonical_mint,
        "resolver_version": resolver_version,
        "weights_version": weights_version,
        "config_hash": config_hash,
    }
    serialized = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def normalize_identity(value: object) -> str:
    if not isinstance(value, str):
        return ""
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return "".join(character for character in normalized if character.isalnum())


def identity_cluster_key(name: object, symbol: object) -> str | None:
    normalized_name = normalize_identity(name)
    normalized_symbol = normalize_identity(symbol)
    if not normalized_name or not normalized_symbol:
        return None
    return f"{normalized_name}:{normalized_symbol}"


def _normalize_url(value: object, *, allowed_schemes: frozenset[str]) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw or any(
        character.isspace() or unicodedata.category(character) == "Cc" for character in raw
    ):
        return None

    try:
        parsed = urlsplit(raw)
        scheme = parsed.scheme.lower()
        if scheme not in allowed_schemes:
            return None
        if "@" in parsed.netloc:
            return None

        hostname = parsed.hostname
        if scheme in {"http", "https"} and not hostname:
            return None
        if parsed.netloc and not hostname:
            return None

        port = parsed.port
        _, _, host_and_port = parsed.netloc.rpartition("@")
        if host_and_port.endswith(":"):
            return None
    except ValueError:
        return None

    netloc = parsed.netloc
    if hostname is not None:
        normalized_hostname = hostname.lower()
        if host_and_port.startswith("[") or ":" in normalized_hostname:
            normalized_hostname = f"[{normalized_hostname}]"
        default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
        suffix = "" if port is None or default_port else f":{port}"
        netloc = f"{normalized_hostname}{suffix}"

    path = "" if parsed.path == "/" and not parsed.query else parsed.path
    if not netloc and not path:
        return None
    return urlunsplit((scheme, netloc, path, parsed.query, ""))


def normalize_uri(value: object) -> str | None:
    return _normalize_url(value, allowed_schemes=frozenset({"http", "https", "ipfs", "ar"}))


def normalize_website(value: object) -> str | None:
    return _normalize_url(value, allowed_schemes=frozenset({"http", "https"}))


def _normalize_handle(value: object, *, domains: frozenset[str]) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None

    candidate = raw[1:] if raw.startswith("@") else raw
    if _HANDLE_RE.fullmatch(candidate):
        return candidate.casefold()

    if any(
        character.isspace() or unicodedata.category(character) == "Cc" for character in raw
    ):
        return None
    if "?" in raw or "#" in raw:
        return None

    url = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlsplit(url)
        port = parsed.port
        _, _, host_and_port = parsed.netloc.rpartition("@")
    except ValueError:
        return None
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or parsed.hostname not in domains
        or port is not None
        or host_and_port.endswith(":")
        or "@" in parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path.count("/") != 1
    ):
        return None

    candidate = parsed.path[1:]
    if not _HANDLE_RE.fullmatch(candidate):
        return None
    return candidate.casefold()


def normalize_twitter(value: object) -> str | None:
    return _normalize_handle(value, domains=frozenset({"x.com", "twitter.com"}))


def normalize_telegram(value: object) -> str | None:
    return _normalize_handle(value, domains=frozenset({"t.me", "telegram.me"}))
