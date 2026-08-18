"""Honeypot probe (spec §5.3 check 9, graduated only): quote a buy (SOL->token) then a
sell (token->SOL) of the returned amount via Jupiter; no sell route or a large round-trip
loss => hard-fail. Simulated quotes only - never executes.

Breaker pattern (D4 fix): CircuitOpen is a rejected call, not a new failure - it does NOT
call record_failure(); only genuine httpx.HTTPError does.
"""
from __future__ import annotations

import httpx

from memebot.safety.checks import CheckResult
from memebot.safety.governor import CircuitOpen, Governor

WSOL = "So11111111111111111111111111111111111111112"
PROBE_LAMPORTS = 1_000_000_000  # 1 SOL notional probe


async def _quote(client, base_url, governor, **params) -> dict | None:
    await governor.acquire()
    resp = await client.get(f"{base_url}/quote", params=params, timeout=20)
    if resp.status_code >= 400:
        return None
    resp.raise_for_status()
    return resp.json()


async def honeypot_check(mint: str, *, client: httpx.AsyncClient, base_url: str,
                         governor: Governor, max_impact_pct: float) -> CheckResult:
    try:
        buy = await _quote(client, base_url, governor, inputMint=WSOL, outputMint=mint,
                           amount=PROBE_LAMPORTS, slippageBps=50)
        if buy is None:
            governor.record_success()
            return CheckResult("honeypot", False, hard=True, reason="no_buy_route",
                               detail={}, available=True)
        tokens = int(buy["outAmount"])
        if tokens == 0:
            # A quote that reports a successful buy route but returns zero tokens for a
            # positive SOL notional means the token isn't actually tradeable - hard-fail
            # explicitly rather than falling through to a sell-leg of 0 tokens (which
            # happens to compute a ~100% round-trip loss today by coincidence, not intent).
            governor.record_success()
            return CheckResult("honeypot", False, hard=True, reason="no_buy_route", detail={})
        sell = await _quote(client, base_url, governor, inputMint=mint, outputMint=WSOL,
                            amount=tokens, slippageBps=50)
        governor.record_success()
    except CircuitOpen as exc:
        return CheckResult("honeypot", False, hard=True, reason="check_unavailable",
                           detail={"error": repr(exc)}, available=False)   # no record_failure
    except (httpx.HTTPError, KeyError, ValueError) as exc:
        governor.record_failure()
        return CheckResult("honeypot", False, hard=True, reason="check_unavailable",
                           detail={"error": repr(exc)}, available=False)
    if sell is None:
        return CheckResult("honeypot", False, hard=True, reason="no_sell_route", detail={})
    returned = int(sell["outAmount"])
    # Round-trip loss = SOL-in vs SOL-out, both in LAMPORTS. We spent PROBE_LAMPORTS on the
    # buy leg (input mint = WSOL) and the sell leg's outAmount is also lamports (output mint =
    # WSOL), so this subtraction is dimensionally sound. Do NOT anchor on `tokens` (the buy's
    # outAmount): that is a TOKEN count in a different unit from `returned`, which makes
    # loss_pct meaningless and, for expensive tokens (few tokens bought, more lamport-units
    # back numerically), NEGATIVE -> the check would pass genuine honeypots.
    loss_pct = 100.0 * (PROBE_LAMPORTS - returned) / PROBE_LAMPORTS
    ok = loss_pct <= max_impact_pct
    return CheckResult("honeypot", ok, hard=True,
                       reason="" if ok else "round_trip_loss",
                       detail={"round_trip_loss_pct": loss_pct})
