"""Candidate-only early-buyer reads for P2.

This module performs bounded Helius history reads for a single candidate token's bonding
curve account, decodes pump.fun TradeEvent logs locally, and returns the first unique buy
wallets. It does not decide pass/fail; the safety gate converts unavailable snapshots into
fail-closed CheckResults.
"""
from __future__ import annotations

from dataclasses import dataclass

from memebot.ingest.pumpfun_decode import trade_events_from_logs
from memebot.safety.rpc import RpcError


@dataclass(frozen=True, slots=True)
class EarlyBuyerSnapshot:
    mint: str
    buyers: tuple[str, ...]
    signatures_scanned: int
    transactions_scanned: int
    unavailable_reason: str = ""


@dataclass(frozen=True, slots=True)
class EarlyBuyerEvidenceDraft:
    checked_at: float | None
    buyers: tuple[str, ...]
    unavailable_reason: str
    inputs_hash: str


class EarlyBuyerReader:
    def __init__(self, rpc, *, signature_limit: int, buyer_limit: int) -> None:
        self._rpc = rpc
        self._signature_limit = signature_limit
        self._buyer_limit = buyer_limit

    async def read(self, *, mint: str, bonding_curve_key: str) -> EarlyBuyerSnapshot:
        try:
            sigs = await self._rpc.signatures_for_address(
                bonding_curve_key, limit=self._signature_limit)
        except RpcError:
            return EarlyBuyerSnapshot(mint=mint, buyers=(), signatures_scanned=0,
                                      transactions_scanned=0, unavailable_reason="rpc_error")
        if not sigs:
            return EarlyBuyerSnapshot(mint=mint, buyers=(), signatures_scanned=0,
                                      transactions_scanned=0,
                                      unavailable_reason="no_signatures")

        buyers: list[str] = []
        seen: set[str] = set()
        tx_count = 0
        # Solana getSignaturesForAddress returns newest first. The P2 gate wants the
        # earliest buyers within the bounded candidate window, so process that page
        # oldest-to-newest without expanding the RPC budget.
        for sig in reversed(sigs):
            if len(buyers) >= self._buyer_limit:
                break
            try:
                logs = await self._rpc.transaction_logs(sig.signature)
            except RpcError:
                return EarlyBuyerSnapshot(mint=mint, buyers=(), signatures_scanned=len(sigs),
                                          transactions_scanned=tx_count,
                                          unavailable_reason="rpc_error")
            tx_count += 1
            for event in trade_events_from_logs(logs):
                if event.mint != mint or not event.is_buy or event.user in seen:
                    continue
                buyers.append(event.user)
                seen.add(event.user)
                if len(buyers) >= self._buyer_limit:
                    break

        if not buyers:
            return EarlyBuyerSnapshot(mint=mint, buyers=(), signatures_scanned=len(sigs),
                                      transactions_scanned=tx_count,
                                      unavailable_reason="no_matching_buy_events")
        return EarlyBuyerSnapshot(mint=mint, buyers=tuple(buyers),
                                  signatures_scanned=len(sigs), transactions_scanned=tx_count)
