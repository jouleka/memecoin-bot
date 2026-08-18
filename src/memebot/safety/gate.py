"""SafetyGate: the fail-closed cascade (spec §5.3; M3 deltas 1/2/3).

Order: cheap free on-chain checks -> (if all pass) rate-limited external cross-check ->
(graduated only) honeypot. First hard-fail short-circuits, saving external budget. A
required check that's unavailable (errored after retries) is a hard-fail (fail-closed).
`probes` is an injected provider so the cascade is testable without real IO; D10 wires
the real on-chain/external/honeypot runners.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Protocol

from memebot.early_buyers import EarlyBuyerEvidenceDraft
from memebot.events import LifecycleTransition, SafetyHardFail, SafetyPassed
from memebot.redact import redact_secrets
from memebot.safety.checks import CheckResult, HolderEvidenceDraft
from memebot.store import (
    get_token,
    save_safety_report_with_p3_evidence,
)

log = logging.getLogger("memebot.safety.gate")

GATED_STATES = ("CLIMBING", "GRADUATED")

_EARLY_BUYER_EVIDENCE_KEYS = frozenset({
    "checked_at", "buyers", "unavailable_reason", "inputs_hash",
})
_EARLY_BUYER_EMBEDDED_REASONS = frozenset({
    "",
    "rpc_error",
    "no_signatures",
    "no_matching_buy_events",
})
_HOLDER_EVIDENCE_KEYS = frozenset({
    "sampled_token_accounts",
    "distinct_non_curve_owners",
    "top10_non_curve_owner_share_pct",
    "holder_observed_at",
    "unavailable_reason",
    "inputs_hash",
})
_HOLDER_EMBEDDED_REASONS = frozenset({
    "",
    "holder_mint_supply_unavailable",
    "holder_accounts_unavailable",
    "holder_accounts_empty",
    "holder_owner_resolution_unavailable",
    "holder_owner_resolution_incomplete",
    "holder_curve_owner_unavailable",
    "holder_non_curve_owners_empty",
})


def _synthetic_holder_evidence(reason: str) -> HolderEvidenceDraft:
    payload = {
        "sampled_token_accounts": None,
        "distinct_non_curve_owners": None,
        "top10_non_curve_owner_share_pct": None,
        "holder_observed_at": None,
        "unavailable_reason": reason,
    }
    inputs_hash = hashlib.sha256(json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()).hexdigest()
    return HolderEvidenceDraft(
        sampled_token_accounts=None,
        distinct_non_curve_owners=None,
        top10_non_curve_owner_share_pct=None,
        holder_observed_at=None,
        unavailable_reason=reason,
        inputs_hash=inputs_hash,
    )


def _extract_holder_evidence(results: Sequence[CheckResult]) -> HolderEvidenceDraft:
    malformed = _synthetic_holder_evidence("holder_evidence_malformed")
    if (
        not isinstance(results, Sequence)
        or isinstance(results, (str, bytes, bytearray))
        or any(not isinstance(result, CheckResult) for result in results)
    ):
        return malformed
    matching = [result for result in results if result.name == "holder_concentration"]
    if not matching:
        return _synthetic_holder_evidence("holder_check_not_run")
    if len(matching) != 1:
        return malformed
    detail = matching[0].detail
    if type(detail) is not dict:
        return malformed
    raw = detail.get("holder_evidence_v1")
    if type(raw) is not dict or set(raw) != _HOLDER_EVIDENCE_KEYS:
        return malformed
    sampled = raw["sampled_token_accounts"]
    owners = raw["distinct_non_curve_owners"]
    top10 = raw["top10_non_curve_owner_share_pct"]
    observed_at = raw["holder_observed_at"]
    unavailable_reason = raw["unavailable_reason"]
    inputs_hash = raw["inputs_hash"]
    if (
        type(observed_at) not in (int, float)
        or not math.isfinite(observed_at)
        or not 0.0 <= observed_at <= 4_102_444_800.0
        or type(unavailable_reason) is not str
        or unavailable_reason not in _HOLDER_EMBEDDED_REASONS
        or type(inputs_hash) is not str
        or len(inputs_hash) != 64
        or any(character not in "0123456789abcdef" for character in inputs_hash)
    ):
        return malformed
    if unavailable_reason:
        if any(value is not None for value in (sampled, owners, top10)):
            return malformed
    elif (
        type(sampled) is not int
        or sampled <= 0
        or type(owners) is not int
        or not 0 < owners <= sampled
        or type(top10) not in (int, float)
        or not math.isfinite(top10)
        or not 0.0 <= top10 <= 100.0
    ):
        return malformed
    return HolderEvidenceDraft(
        sampled_token_accounts=sampled,
        distinct_non_curve_owners=owners,
        top10_non_curve_owner_share_pct=(None if top10 is None else float(top10)),
        holder_observed_at=float(observed_at),
        unavailable_reason=unavailable_reason,
        inputs_hash=inputs_hash,
    )


def _synthetic_early_buyer_evidence(reason: str) -> EarlyBuyerEvidenceDraft:
    payload = {"checked_at": None, "buyers": (), "unavailable_reason": reason}
    inputs_hash = hashlib.sha256(json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()).hexdigest()
    return EarlyBuyerEvidenceDraft(
        checked_at=None,
        buyers=(),
        unavailable_reason=reason,
        inputs_hash=inputs_hash,
    )


def _extract_early_buyer_evidence(
    results: Sequence[CheckResult],
) -> EarlyBuyerEvidenceDraft:
    malformed = _synthetic_early_buyer_evidence("early_buyer_evidence_malformed")
    if (
        not isinstance(results, Sequence)
        or isinstance(results, (str, bytes, bytearray))
        or any(not isinstance(result, CheckResult) for result in results)
    ):
        return malformed
    matching = [result for result in results if result.name == "early_buyer_concentration"]
    if not matching:
        return _synthetic_early_buyer_evidence("early_buyer_check_not_run")
    if len(matching) != 1:
        return malformed

    detail = matching[0].detail
    if type(detail) is not dict:
        return malformed
    if "early_buyer_evidence_v1" not in detail:
        result = matching[0]
        if not (
            result.passed is False
            and result.hard is True
            and result.reason == "check_unavailable"
            and result.available is False
        ):
            return malformed
        reason = detail.get("reason")
        if (
            type(reason) is str
            and reason in {"missing_bonding_curve_key", "reader_unavailable"}
            and detail == {"reason": reason}
        ):
            return _synthetic_early_buyer_evidence(reason)
        if (
            reason == "owner_resolution_incomplete"
            and set(detail) == {"reason", "unresolved"}
        ):
            unresolved = detail["unresolved"]
            if (
                type(unresolved) is list
                and bool(unresolved)
                and all(type(address) is str and address.strip() for address in unresolved)
                and unresolved == sorted(set(unresolved))
            ):
                return _synthetic_early_buyer_evidence(reason)
        return malformed
    raw = detail.get("early_buyer_evidence_v1")
    if type(raw) is not dict or set(raw) != _EARLY_BUYER_EVIDENCE_KEYS:
        return malformed
    checked_at = raw["checked_at"]
    buyers = raw["buyers"]
    unavailable_reason = raw["unavailable_reason"]
    inputs_hash = raw["inputs_hash"]
    if (
        type(checked_at) not in (int, float)
        or not math.isfinite(checked_at)
        or not 0.0 <= checked_at <= 4_102_444_800.0
        or type(buyers) is not tuple
        or any(type(buyer) is not str or not buyer.strip() for buyer in buyers)
        or len(set(buyers)) != len(buyers)
        or type(unavailable_reason) is not str
        or unavailable_reason not in _EARLY_BUYER_EMBEDDED_REASONS
        or (unavailable_reason == "") != bool(buyers)
        or type(inputs_hash) is not str
        or len(inputs_hash) != 64
        or any(character not in "0123456789abcdef" for character in inputs_hash)
    ):
        return malformed
    result = matching[0]
    if unavailable_reason:
        status_valid = (
            result.passed is False
            and result.hard is True
            and result.reason == "check_unavailable"
            and result.available is False
        )
    else:
        status_valid = (
            result.hard is True
            and result.available is True
            and (
                (result.passed is True and result.reason == "")
                or (
                    result.passed is False
                    and result.reason == "early_buyer_concentration"
                )
            )
        )
    if not status_valid:
        return malformed
    return EarlyBuyerEvidenceDraft(
        checked_at=float(checked_at),
        buyers=buyers,
        unavailable_reason=unavailable_reason,
        inputs_hash=inputs_hash,
    )


@dataclass(frozen=True, slots=True)
class SafetyReport:
    mint: str
    checked_at: float
    segment: str
    hard_fails: tuple[str, ...]
    risk_score: float
    results: tuple[CheckResult, ...]
    inputs_hash: str
    report_id: int | None = None

    @property
    def passed(self) -> bool:
        return not self.hard_fails


@dataclass(frozen=True, slots=True)
class EvaluatedSafetyDraft:
    mint: str
    raw_completed_at: float
    segment: str
    hard_fails: tuple[str, ...]
    risk_score: float
    results_json: str
    safety_inputs_hash: str
    holder: HolderEvidenceDraft
    early_buyer: EarlyBuyerEvidenceDraft


def _hard_fails(results: list[CheckResult]) -> list[str]:
    out = []
    for r in results:
        if not r.available:
            out.append(f"check_unavailable:{r.name}")
        elif not r.passed and r.hard:
            out.append(r.reason or r.name)
    return out


def _risk_score(results: list[CheckResult]) -> float:
    # soft (non-hard) failures each add to the score; hard-fails dominate via the gate.
    soft_fails = sum(1 for r in results if r.available and not r.passed and not r.hard)
    return min(100.0, 20.0 * soft_fails)


class SafetyGate:
    def __init__(self, conn, *, probes, clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._probes = probes
        self._clock = clock

    async def evaluate_unpersisted(
        self, token: Mapping[str, object],
    ) -> EvaluatedSafetyDraft:
        if not isinstance(token, Mapping):
            raise ValueError("safety token must be a mapping")
        mint = token.get("mint")
        segment = token.get("state")
        if (
            type(mint) is not str
            or not mint.strip()
            or len(mint) > 128
            or type(segment) is not str
            or segment not in GATED_STATES
        ):
            raise ValueError("invalid safety token identity or state")

        results: list[CheckResult] = []
        try:
            onchain = await self._probes.onchain(token)
            results.extend(onchain)
            if not onchain:
                results.append(CheckResult(
                    "onchain_probe", passed=False, hard=True,
                    reason="no_checks_ran",
                ))
            if not _hard_fails(results):
                external = await self._probes.external(token)
                results.extend(external or [])
                if not external:
                    results.append(CheckResult(
                        "external_probe", passed=False, hard=True,
                        reason="no_checks_ran",
                    ))
            if not _hard_fails(results) and segment == "GRADUATED":
                honeypot = await self._probes.honeypot(token)
                results.extend(honeypot or [])
                if not honeypot:
                    results.append(CheckResult(
                        "honeypot_probe", passed=False, hard=True,
                        reason="no_checks_ran",
                    ))
        except Exception as exc:
            log.exception(
                "safety gate probe raised; failing closed",
                extra={"extra_fields": {"mint": mint}},
            )
            results.append(CheckResult(
                "gate_error", passed=False, hard=True,
                reason="gate_error",
                detail={"error": redact_secrets(repr(exc))},
                available=False,
            ))

        raw_completed_at = self._clock()
        if (
            type(raw_completed_at) not in (int, float)
            or not math.isfinite(raw_completed_at)
            or not 0.0 <= raw_completed_at <= 4_102_444_800.0
        ):
            raise ValueError("invalid safety completion time")
        hard = tuple(_hard_fails(results))
        safety_inputs_hash = hashlib.sha256(
            json.dumps(
                [(result.name, result.reason, result.detail) for result in results],
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()
        results_json = json.dumps(
            [asdict(result) for result in results],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return EvaluatedSafetyDraft(
            mint=mint,
            raw_completed_at=float(raw_completed_at),
            segment=segment,
            hard_fails=hard,
            risk_score=_risk_score(results),
            results_json=results_json,
            safety_inputs_hash=safety_inputs_hash,
            holder=_extract_holder_evidence(results),
            early_buyer=_extract_early_buyer_evidence(results),
        )

    def persist(self, draft: EvaluatedSafetyDraft) -> SafetyReport:
        report, _holder_id, _early_id = save_safety_report_with_p3_evidence(
            self._conn, draft=draft,
        )
        return report

class SafetyGatePort(Protocol):
    async def evaluate_unpersisted(
        self, token: Mapping[str, object],
    ) -> EvaluatedSafetyDraft: ...

    def persist(self, draft: EvaluatedSafetyDraft) -> SafetyReport: ...


class GateRunner:
    """Bus consumer: evaluates a token via the gate when it reaches CLIMBING/GRADUATED;
    publishes SafetyHardFail on a hard-fail (fail-closed -> the lifecycle tracker marks
    DEAD(rugged); the telegram consumer alerts). Does NOT alert directly — that's the
    telegram consumer's job, kept separate so the gate has no telegram dependency."""

    def __init__(
        self, bus, conn, gate: SafetyGatePort, *,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._bus = bus
        self._conn = conn
        self._gate = gate
        self._retry_sleep = retry_sleep
        self._q = bus.subscribe(LifecycleTransition, critical=True)

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    tr = await asyncio.wait_for(self._q.get(), timeout=0.5)
                except TimeoutError:
                    continue
                if tr.to_state not in GATED_STATES:
                    self._bus.critical_done(self._q)
                    continue
                row = get_token(self._conn, tr.mint)
                if row is None:
                    self._bus.critical_done(self._q)
                    continue
                draft = await self._gate.evaluate_unpersisted(dict(row))
                attempt = 1
                delay = 0.05
                while True:
                    try:
                        report = self._gate.persist(draft)
                    except Exception:
                        log.exception(
                            "safety gate persistence failed",
                            extra={"extra_fields": {
                                "mint": tr.mint,
                                "attempt": attempt,
                            }},
                        )
                        await self._retry_sleep(delay)
                        attempt += 1
                        delay = min(delay * 2.0, 2.0)
                        continue
                    break
                if not report.passed:
                    await self._bus.publish(SafetyHardFail(
                        t_wall=time.time(), t_mono=time.monotonic(),
                        mint=tr.mint, reasons=report.hard_fails,
                        safety_report_id=report.report_id))
                else:
                    await self._bus.publish(SafetyPassed(
                        t_wall=time.time(), t_mono=time.monotonic(),
                        mint=tr.mint, segment=report.segment,
                        safety_report_id=report.report_id,
                        risk_score=report.risk_score))
                self._bus.critical_done(self._q)
        finally:
            self._bus.unsubscribe(self._q)


class LiveProbes:
    """Real check runner: maps SafetyRpc / external / honeypot into CheckResult lists.
    On any RpcError it yields an available=False CheckResult (fail-closed, not a raise).

    Deferred (v1 follow-up): the bundled-snipe proxy (spec check 6) needs the create-frame
    initialBuy persisted (not in the token dict yet) — not implemented here.
    """
    def __init__(self, *, rpc_url, rpc_client, ext_client, jup_client, conn, cfg, governors,
                 early_buyer_reader=None, clock: Callable[[], float] = time.time):
        from memebot.early_buyers import EarlyBuyerReader
        from memebot.safety.rpc import SafetyRpc
        self._rpc = SafetyRpc(rpc_url, client=rpc_client)
        self._ext_client, self._jup_client = ext_client, jup_client
        self._conn, self._cfg, self._gov = conn, cfg, governors
        self._clock = clock
        early_cfg = cfg.get("early_buyers", {})
        self._early_buyer_reader = early_buyer_reader
        if self._early_buyer_reader is None and early_cfg.get("enabled", False):
            self._early_buyer_reader = EarlyBuyerReader(
                self._rpc,
                signature_limit=int(early_cfg.get("signature_limit", 25)),
                buyer_limit=int(early_cfg.get("buyer_limit", 20)),
            )

    async def onchain(self, token):
        import json as _json
        from memebot.safety.checks import (CheckResult, dev_wallet_check,
                                           derive_holder_evidence,
                                           early_buyer_concentration_check,
                                           freeze_authority_check, holder_concentration_check,
                                           mint_authority_check)
        from memebot.safety.rpc import RpcError
        from memebot.store import creator_rug_history
        mint = token["mint"]
        creator = _json.loads(token.get("meta_json") or "{}").get("creator", "")
        try:
            info = await self._rpc.mint_info(mint)
        except RpcError as e:
            return [CheckResult("mint_info", False, hard=True, reason="check_unavailable",
                                detail={"error": redact_secrets(repr(e))}, available=False)]
        curve = token.get("bonding_curve_key", "")

        def with_holder_evidence(result, *, holders, owners, holder_observed_at):
            result.detail["holder_evidence_v1"] = asdict(derive_holder_evidence(
                mint=mint,
                holders=holders,
                owners=owners,
                supply=info.supply,
                curve_owner=curve,
                holder_observed_at=holder_observed_at,
            ))
            return result

        out = [mint_authority_check(info), freeze_authority_check(info)]
        rug_prior = creator_rug_history(self._conn, creator)
        out.append(CheckResult("creator_rug_history", passed=(rug_prior == 0), hard=True,
                               reason="" if rug_prior == 0 else "creator_rug_history",
                               detail={"prior_rugs": rug_prior}))
        try:
            holders = await self._rpc.largest_accounts(mint)
        except RpcError as e:
            holder_observed_at = self._clock()
            out.append(with_holder_evidence(
                CheckResult("holder_concentration", False, hard=True,
                            reason="check_unavailable", detail={"error": redact_secrets(repr(e))},
                            available=False),
                holders=None,
                owners=None,
                holder_observed_at=holder_observed_at,
            ))
            return out
        if not holders:
            holder_observed_at = self._clock()
            # A valid (no-RpcError) but EMPTY holder list must not be read as "0% held,
            # perfectly distributed" - that defeats the primary rug filter. Fail-closed,
            # mirroring the supply=0 default in the pure check.
            out.append(with_holder_evidence(
                CheckResult("holder_concentration", False, hard=True,
                            reason="check_unavailable", detail={"reason": "no_holder_data"},
                            available=False),
                holders=holders,
                owners=None,
                holder_observed_at=holder_observed_at,
            ))
            return out
        # Resolve the on-chain OWNER of each top token account. getTokenLargestAccounts
        # returns token-ACCOUNT addresses; the bonding curve's own token account holds ~80%
        # of supply pre-graduation, so concentration (and dev share) must be reckoned by
        # owner -- NOT by matching the owner PDA against account addresses, the pre-fix bug
        # that hard-failed ~85% of climbing tokens. Owner resolution unavailable -> fail
        # closed, consistent with the rest of the cascade.
        try:
            owners = await self._rpc.account_owners([h.address for h in holders])
        except RpcError as e:
            holder_observed_at = self._clock()
            out.append(with_holder_evidence(
                CheckResult("holder_concentration", False, hard=True,
                            reason="check_unavailable", detail={"error": redact_secrets(repr(e))},
                            available=False),
                holders=holders,
                owners=None,
                holder_observed_at=holder_observed_at,
            ))
            return out
        holder_observed_at = self._clock()
        exclude = {addr for addr, owner in owners.items() if curve and owner == curve}
        if curve:
            exclude.add(curve)   # belt-and-suspenders: drop the owner PDA itself if it ever appears
        out.append(with_holder_evidence(
            holder_concentration_check(
                holders,
                supply=info.supply,
                exclude=exclude,
                max_pct=self._cfg["top10_holder_max_pct"],
            ),
            holders=holders,
            owners=owners,
            holder_observed_at=holder_observed_at,
        ))
        if creator:
            out.append(dev_wallet_check(holders, owners=owners, supply=info.supply, dev=creator,
                                        max_pct=self._cfg["dev_wallet_max_pct"]))
        else:
            # metadata-less create: can't evaluate dev share; holder-concentration covers the
            # concentration risk, so soft-flag (raises risk score) rather than hard-block.
            out.append(CheckResult("dev_wallet", passed=False, hard=False,
                                   reason="unknown_creator", detail={}, available=True))
        early_cfg = self._cfg.get("early_buyers", {})
        if early_cfg.get("enabled", False):
            if not curve:
                out.append(CheckResult("early_buyer_concentration", passed=False, hard=True,
                                       reason="check_unavailable",
                                       detail={"reason": "missing_bonding_curve_key"},
                                       available=False))
                return out
            unresolved = sorted(h.address for h in holders if h.address not in owners)
            if unresolved:
                out.append(CheckResult("early_buyer_concentration", passed=False, hard=True,
                                       reason="check_unavailable",
                                       detail={"reason": "owner_resolution_incomplete",
                                               "unresolved": unresolved},
                                       available=False))
                return out
            reader = self._early_buyer_reader
            if reader is None:
                out.append(CheckResult("early_buyer_concentration", passed=False, hard=True,
                                       reason="check_unavailable",
                                       detail={"reason": "reader_unavailable"},
                                       available=False))
                return out
            snapshot = await reader.read(mint=mint, bonding_curve_key=curve)
            early_buyer_checked_at = self._clock()
            early_buyer_payload = {
                "mint": mint,
                "buyers": snapshot.buyers,
                "unavailable_reason": snapshot.unavailable_reason,
                "signatures_scanned": snapshot.signatures_scanned,
                "transactions_scanned": snapshot.transactions_scanned,
            }
            early_buyer_inputs_hash = hashlib.sha256(json.dumps(
                early_buyer_payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()).hexdigest()
            early_buyer_evidence = EarlyBuyerEvidenceDraft(
                checked_at=early_buyer_checked_at,
                buyers=snapshot.buyers,
                unavailable_reason=snapshot.unavailable_reason,
                inputs_hash=early_buyer_inputs_hash,
            )
            if snapshot.unavailable_reason:
                result = CheckResult(
                    "early_buyer_concentration", passed=False, hard=True,
                    reason="check_unavailable",
                    detail={"reason": snapshot.unavailable_reason,
                            "signatures_scanned": snapshot.signatures_scanned,
                            "transactions_scanned": snapshot.transactions_scanned},
                    available=False,
                )
            else:
                result = early_buyer_concentration_check(
                    holders, owners=owners, supply=info.supply, early_buyers=snapshot.buyers,
                    max_pct=float(early_cfg["max_supply_pct"]))
                result.detail.update({"signatures_scanned": snapshot.signatures_scanned,
                                      "transactions_scanned": snapshot.transactions_scanned})
            result.detail["early_buyer_evidence_v1"] = asdict(early_buyer_evidence)
            out.append(result)
        return out

    async def external(self, token):
        from memebot.safety.external import goplus_check, rugcheck_check
        mint = token["mint"]
        return [
            await rugcheck_check(mint, client=self._ext_client, base_url=self._cfg["rugcheck_base"],
                                 governor=self._gov["rugcheck"]),
            await goplus_check(mint, client=self._ext_client, base_url=self._cfg["goplus_base"],
                               governor=self._gov["goplus"]),
        ]

    async def honeypot(self, token):
        from memebot.safety.honeypot import honeypot_check
        return [await honeypot_check(token["mint"], client=self._jup_client,
                                     base_url=self._cfg["jupiter_base"], governor=self._gov["jupiter"],
                                     max_impact_pct=self._cfg["honeypot_max_impact_pct"])]
