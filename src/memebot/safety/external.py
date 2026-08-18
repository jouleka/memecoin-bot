"""RugCheck + GoPlus cross-check -> CheckResult (spec §5.3 check 8; M3 delta 5/8).

Both are rate-limited (RugCheck hard 15/min) and governed. A transport/HTTP error
after the governor's retries -> available=False (fail-closed). GoPlus has NO aggregate
verdict field - we combine raw fields ourselves.
"""
from __future__ import annotations

import httpx

from memebot.redact import redact_secrets
from memebot.safety.checks import CheckResult
from memebot.safety.governor import CircuitOpen, Governor

RUGCHECK_CRITICAL_LEVELS = {"critical", "danger"}


async def rugcheck_check(mint: str, *, client: httpx.AsyncClient, base_url: str,
                         governor: Governor) -> CheckResult:
    try:
        await governor.acquire()
        resp = await client.get(f"{base_url}/tokens/{mint}/report/summary", timeout=20)
        resp.raise_for_status()
        body = resp.json()
        governor.record_success()
    except CircuitOpen as exc:
        # breaker already open: a rejected call is NOT a new failure -- do not re-arm the window
        return CheckResult("rugcheck", passed=False, hard=True, reason="check_unavailable",
                           detail={"error": redact_secrets(repr(exc))}, available=False)
    except httpx.HTTPError as exc:
        governor.record_failure()
        return CheckResult("rugcheck", passed=False, hard=True, reason="check_unavailable",
                           detail={"error": redact_secrets(repr(exc))}, available=False)
    # A real RugCheck summary always carries score_normalised and risks; their absence ==
    # no report for this mint -> fail-closed (empty/malformed 200 must NOT pass a hard gate).
    if "score_normalised" not in body:
        return CheckResult("rugcheck", passed=False, hard=True, reason="check_unavailable",
                           detail={"reason": "empty_report"}, available=False)
    risks = body.get("risks")
    if not isinstance(risks, list):
        return CheckResult("rugcheck", passed=False, hard=True, reason="check_unavailable",
                           detail={"reason": "malformed_report"}, available=False)
    criticals = [r for r in risks
                 if r.get("level") in RUGCHECK_CRITICAL_LEVELS]
    score = float(body.get("score_normalised", 0))
    ok = not criticals
    return CheckResult("rugcheck", passed=ok, hard=True,
                       reason="" if ok else f"rugcheck_critical:{criticals[0]['name']}",
                       detail={"criticals": [r["name"] for r in criticals],
                               "score_normalised": score, "lp_locked_pct": body.get("lpLockedPct")})


async def goplus_check(mint: str, *, client: httpx.AsyncClient, base_url: str,
                       governor: Governor) -> CheckResult:
    try:
        await governor.acquire()
        resp = await client.get(f"{base_url}/solana/token_security",
                                params={"contract_addresses": mint}, timeout=20)
        resp.raise_for_status()
        body = resp.json()
        governor.record_success()
    except CircuitOpen as exc:
        # breaker already open: a rejected call is NOT a new failure -- do not re-arm the window
        return CheckResult("goplus", passed=False, hard=True, reason="check_unavailable",
                           detail={"error": redact_secrets(repr(exc))}, available=False)
    except httpx.HTTPError as exc:
        governor.record_failure()
        return CheckResult("goplus", passed=False, hard=True, reason="check_unavailable",
                           detail={"error": redact_secrets(repr(exc))}, available=False)
    # GoPlus keys results by lowercased mint; combine raw fields (no single verdict field).
    result = body.get("result") or {}
    row = result.get(mint) or result.get(mint.lower()) or {}
    # Our mint absent from result == no data for this token -> fail-closed (not a clean pass).
    if not row:
        return CheckResult("goplus", passed=False, hard=True, reason="check_unavailable",
                           detail={"reason": "mint_not_in_result"}, available=False)
    missing = [field for field in ("mintable", "freezable", "transfer_hook") if field not in row]
    if (missing or not isinstance(row.get("mintable"), dict)
            or not isinstance(row.get("freezable"), dict)
            or "status" not in row.get("mintable", {})
            or "status" not in row.get("freezable", {})):
        return CheckResult("goplus", passed=False, hard=True, reason="check_unavailable",
                           detail={"reason": "malformed_result_row",
                                   "missing_fields": missing}, available=False)
    mintable_status = row["mintable"]["status"]
    freezable_status = row["freezable"]["status"]
    if (str(mintable_status) not in {"0", "1"}
            or str(freezable_status) not in {"0", "1"}
            or not isinstance(row["transfer_hook"], list)):
        return CheckResult("goplus", passed=False, hard=True, reason="check_unavailable",
                           detail={"reason": "malformed_result_row",
                                   "missing_fields": missing}, available=False)
    criticals = []
    if str(mintable_status) == "1":
        criticals.append("mintable")
    if str(freezable_status) == "1":
        criticals.append("freezable")
    if row.get("transfer_hook"):
        criticals.append("transfer_hook")
    ok = not criticals
    return CheckResult("goplus", passed=ok, hard=True,
                       reason="" if ok else f"goplus_critical:{criticals[0]}",
                       detail={"criticals": criticals})
