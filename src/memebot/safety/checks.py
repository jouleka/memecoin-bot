"""Pure safety-check functions over fetched on-chain data -> CheckResult (spec §5.3).

Pure: no IO, no clock. Each returns a CheckResult; the gate (D7) aggregates. `hard`
marks a failure as a non-overridable hard-fail; soft checks only feed the risk score.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from memebot.safety.rpc import Holder, MintInfo


@dataclass(frozen=True, slots=True)
class HolderEvidenceDraft:
    sampled_token_accounts: int | None
    distinct_non_curve_owners: int | None
    top10_non_curve_owner_share_pct: float | None
    holder_observed_at: float
    unavailable_reason: str
    inputs_hash: str


def derive_holder_evidence(
    *,
    mint: str,
    holders: Sequence[Holder] | None,
    owners: Mapping[str, str] | None,
    supply: int | None,
    curve_owner: str,
    holder_observed_at: float,
) -> HolderEvidenceDraft:
    supply_available = type(supply) is int and supply > 0
    accounts_available = (
        isinstance(holders, Sequence)
        and not isinstance(holders, (str, bytes, bytearray))
    )
    accounts: list[dict[str, object]] = []
    seen_addresses: set[str] = set()
    sampled_amount = 0
    if accounts_available:
        for holder in holders:
            if (
                not isinstance(holder, Holder)
                or type(holder.address) is not str
                or not holder.address.strip()
                or holder.address in seen_addresses
                or type(holder.amount) is not int
                or holder.amount < 0
            ):
                accounts_available = False
                accounts = []
                break
            seen_addresses.add(holder.address)
            sampled_amount += holder.amount
            accounts.append({
                "address": holder.address,
                "amount": holder.amount,
                "owner": None,
            })
    if accounts_available and supply_available and sampled_amount > supply:
        accounts_available = False
        accounts = []
    accounts.sort(key=lambda account: account["address"])

    owners_available = isinstance(owners, Mapping)
    owners_complete = owners_available
    if accounts_available and owners_available:
        for account in accounts:
            owner = owners.get(account["address"])
            if type(owner) is not str or not owner.strip():
                owners_complete = False
                continue
            account["owner"] = owner

    if not supply_available:
        unavailable_reason = "holder_mint_supply_unavailable"
    elif not accounts_available:
        unavailable_reason = "holder_accounts_unavailable"
    elif not accounts:
        unavailable_reason = "holder_accounts_empty"
    elif not owners_available:
        unavailable_reason = "holder_owner_resolution_unavailable"
    elif not owners_complete:
        unavailable_reason = "holder_owner_resolution_incomplete"
    elif type(curve_owner) is not str or not curve_owner.strip():
        unavailable_reason = "holder_curve_owner_unavailable"
    else:
        unavailable_reason = ""

    balances: dict[str, int] = {}
    if not unavailable_reason:
        for account in accounts:
            owner = account["owner"]
            if owner != curve_owner:
                balances[owner] = balances.get(owner, 0) + account["amount"]
        if not balances:
            unavailable_reason = "holder_non_curve_owners_empty"

    canonical_curve_owner = curve_owner if type(curve_owner) is str else None
    payload = {
        "mint": mint,
        "supply": supply if supply_available else None,
        "curve_owner": canonical_curve_owner,
        "holder_observed_at": holder_observed_at,
        "unavailable_reason": unavailable_reason,
        "accounts": accounts,
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    inputs_hash = hashlib.sha256(canonical.encode()).hexdigest()
    if unavailable_reason:
        return HolderEvidenceDraft(
            sampled_token_accounts=None,
            distinct_non_curve_owners=None,
            top10_non_curve_owner_share_pct=None,
            holder_observed_at=holder_observed_at,
            unavailable_reason=unavailable_reason,
            inputs_hash=inputs_hash,
        )
    return HolderEvidenceDraft(
        sampled_token_accounts=len(accounts),
        distinct_non_curve_owners=len(balances),
        top10_non_curve_owner_share_pct=(
            100.0 * sum(sorted(balances.values(), reverse=True)[:10]) / supply
        ),
        holder_observed_at=holder_observed_at,
        unavailable_reason="",
        inputs_hash=inputs_hash,
    )


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    passed: bool
    hard: bool
    reason: str = ""
    detail: dict[str, Any] = field(default_factory=dict)
    available: bool = True


def mint_authority_check(info: MintInfo) -> CheckResult:
    ok = info.mint_authority is None
    return CheckResult("mint_authority", ok, hard=True,
                       reason="" if ok else "mint_authority_active",
                       detail={"mint_authority": info.mint_authority})


def freeze_authority_check(info: MintInfo) -> CheckResult:
    ok = info.freeze_authority is None
    return CheckResult("freeze_authority", ok, hard=True,
                       reason="" if ok else "freeze_authority_active",
                       detail={"freeze_authority": info.freeze_authority})


def holder_concentration_check(holders: list[Holder], *, supply: int, exclude: set[str],
                               max_pct: float) -> CheckResult:
    relevant = [h for h in holders if h.address not in exclude]
    top10 = sorted(relevant, key=lambda h: h.amount, reverse=True)[:10]
    share = 100.0 * sum(h.amount for h in top10) / supply if supply else 100.0
    ok = share <= max_pct
    return CheckResult("holder_concentration", ok, hard=True,
                       reason="" if ok else "holder_concentration",
                       detail={"top10_share_pct": share, "excluded": sorted(exclude)})


def dev_wallet_check(holders: list[Holder], *, owners: dict[str, str], supply: int,
                     dev: str, max_pct: float) -> CheckResult:
    # `holders` are TOKEN-ACCOUNT addresses, `dev` is a WALLET pubkey — they never match
    # directly (the pre-fix `h.address == dev` was silently always-zero). Sum the dev's
    # share via the resolved owner map instead.
    #
    # SOFT (hard=False): a pump.fun creator's initial buy routinely exceeds dev_wallet_max_pct
    # early in the curve, so hard-blocking on it would reject most fresh tokens. It feeds the
    # risk score for now; promoting to hard + calibrating the threshold is an M4 (signals) task.
    held = sum(h.amount for h in holders if owners.get(h.address) == dev)
    share = 100.0 * held / supply if supply else 0.0
    ok = share <= max_pct
    return CheckResult("dev_wallet", ok, hard=False,
                       reason="" if ok else "dev_wallet_share",
                       detail={"dev_share_pct": share, "dev": dev})


def early_buyer_concentration_check(holders: list[Holder], *, owners: dict[str, str],
                                    supply: int, early_buyers: tuple[str, ...],
                                    max_pct: float) -> CheckResult:
    """Hard P2 rug-gate check: share currently held by first buyer wallets.

    `holders` are token-account addresses. `early_buyers` are wallet pubkeys, so the
    resolved `owners` mapping is required; matching addresses directly would repeat the
    dev-wallet pre-fix bug.
    """
    if not early_buyers:
        return CheckResult("early_buyer_concentration", passed=False, hard=True,
                           reason="check_unavailable",
                           detail={"reason": "no_early_buyers"}, available=False)
    early = set(early_buyers)
    held = sum(h.amount for h in holders if owners.get(h.address) in early)
    share = 100.0 * held / supply if supply else 100.0
    ok = share <= max_pct
    return CheckResult("early_buyer_concentration", ok, hard=True,
                       reason="" if ok else "early_buyer_concentration",
                       detail={"early_buyer_share_pct": share,
                               "early_buyers": sorted(early)})
