"""Typed bus events (spec §5.1). Frozen dataclasses + dict round-trip for the journal."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, ClassVar


@dataclass(frozen=True, slots=True)
class Event:
    kind: ClassVar[str] = "event"
    t_wall: float  # unix seconds, UTC
    t_mono: float  # monotonic seconds, in-process ordering (spec §7 clock discipline)


@dataclass(frozen=True, slots=True)
class TokenCreated(Event):
    kind: ClassVar[str] = "token_created"
    mint: str
    name: str
    symbol: str
    creator: str
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CurveTrade(Event):
    kind: ClassVar[str] = "curve_trade"
    mint: str
    side: str  # "buy" | "sell"
    sol_amount: float
    token_amount: float
    trader: str
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CurveProgress(Event):
    kind: ClassVar[str] = "curve_progress"
    mint: str
    progress_pct: float
    # P1: reserve snapshot for the velocity feature + curve-math fills. Defaulted so
    # pre-P1 journal rows (no reserve keys) still decode via event_from_dict.
    virtual_sol_reserves: int = 0
    virtual_token_reserves: int = 0
    real_sol_reserves: int = 0
    real_token_reserves: int = 0
    source_boot_id: int = 0
    source_seq: int = 0


@dataclass(frozen=True, slots=True)
class TokenGraduated(Event):
    kind: ClassVar[str] = "token_graduated"
    mint: str
    pool: str
    dex: str
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PairUpdate(Event):
    kind: ClassVar[str] = "pair_update"
    mint: str
    pair: str
    price_usd: float
    liquidity_usd: float
    volume_h24: float
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HolderSnapshot(Event):
    kind: ClassVar[str] = "holder_snapshot"
    mint: str
    holders: int
    top10_share: float


@dataclass(frozen=True, slots=True)
class MarketRegime(Event):
    kind: ClassVar[str] = "market_regime"
    state: str  # "normal" | "risk_off"
    inputs: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AdapterHealth(Event):
    kind: ClassVar[str] = "adapter_health"
    adapter: str
    status: str  # "up" | "stale" | "down" | "started"
    detail: str


@dataclass(frozen=True, slots=True)
class LifecycleTransition(Event):
    kind: ClassVar[str] = "lifecycle_transition"
    mint: str
    from_state: str
    to_state: str


@dataclass(frozen=True, slots=True)
class SafetyHardFail(Event):
    kind: ClassVar[str] = "safety_hard_fail"
    mint: str
    reasons: tuple[str, ...]
    safety_report_id: int | None = None


@dataclass(frozen=True, slots=True)
class SafetyPassed(Event):
    kind: ClassVar[str] = "safety_passed"
    mint: str
    segment: str
    safety_report_id: int
    risk_score: float


@dataclass(frozen=True, slots=True)
class CandidateScored(Event):
    kind: ClassVar[str] = "candidate_scored"
    mint: str
    decision_id: int
    segment: str
    score: float
    spot_price_sol: float


@dataclass(frozen=True, slots=True)
class CanonicalObservationStarted(Event):
    kind: ClassVar[str] = "canonical_observation_started"
    observation_id: int
    decision_id: int
    mint: str
    start_price_sol: float
    price_observed_at: float


@dataclass(frozen=True, slots=True)
class PaperEntry(Event):
    kind: ClassVar[str] = "paper_entry"
    mint: str
    segment: str
    qty: float
    fill_price: float
    size_sol: float
    score: float
    realism_grade: str
    canonical_status: str | None = None
    canonical_mint: str | None = None
    canonical_resolver_version: str | None = None
    canonical_recheck_id: int | None = None
    canonical_recheck_hash: str | None = None
    paper_trade_id: int | None = None
    paper_entry_execution_id: int | None = None


@dataclass(frozen=True, slots=True)
class PaperExit(Event):
    kind: ClassVar[str] = "paper_exit"
    mint: str
    segment: str
    qty: float
    fill_price: float
    pnl_sol: float
    reason: str
    realism_grade: str


EVENT_TYPES: dict[str, type[Event]] = {
    cls.kind: cls
    for cls in (TokenCreated, CurveTrade, CurveProgress, TokenGraduated,
                PairUpdate, HolderSnapshot, MarketRegime, AdapterHealth,
                LifecycleTransition, SafetyHardFail,
                SafetyPassed, CandidateScored, CanonicalObservationStarted,
                PaperEntry, PaperExit)
}


def event_to_dict(event: Event) -> dict[str, Any]:
    return {"kind": event.kind, **asdict(event)}


def event_from_dict(d: dict[str, Any]) -> Event:
    data = dict(d)
    cls = EVENT_TYPES[data.pop("kind")]
    return cls(**data)
