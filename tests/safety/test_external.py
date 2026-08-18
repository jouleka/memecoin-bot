import json
from pathlib import Path

import httpx

from memebot.safety.external import goplus_check, rugcheck_check
from memebot.safety.governor import Governor

RUG_FIX = json.loads(Path("tests/fixtures/providers/rugcheck/summary_bonk.json").read_text())["body"]
GOPLUS_FIX = json.loads(Path("tests/fixtures/providers/goplus/token_security_bonk.json").read_text())["body"]
MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"


def gov():
    return Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0))


async def test_rugcheck_pass_on_clean_summary():
    t = httpx.MockTransport(lambda r: httpx.Response(200, json=RUG_FIX))
    r = await rugcheck_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://api.rugcheck.xyz/v1", governor=gov())
    assert r.name == "rugcheck" and r.available
    # BONK's real fixture: risks == [{"name": "Mutable metadata", "level": "warn", ...}]
    # only a warn-level risk, no "danger" -> no critical hard-fail -> passes.
    assert r.passed


async def test_rugcheck_hardfail_on_critical_risk():
    body = {"risks": [{"name": "Mint authority enabled", "level": "danger", "score": 5000}],
            "score_normalised": 80, "lpLockedPct": 0}
    t = httpx.MockTransport(lambda r: httpx.Response(200, json=body))
    r = await rugcheck_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://api.rugcheck.xyz/v1", governor=gov())
    assert not r.passed and r.hard and "critical" in r.reason


async def test_rugcheck_hardfail_on_provider_critical_level():
    body = {"risks": [{"name": "Provider critical risk", "level": "critical", "score": 5000}],
            "score_normalised": 80, "lpLockedPct": 0}
    t = httpx.MockTransport(lambda r: httpx.Response(200, json=body))
    r = await rugcheck_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://api.rugcheck.xyz/v1", governor=gov())
    assert not r.passed and r.hard and "Provider critical risk" in r.reason


async def test_rugcheck_unavailable_on_error():
    t = httpx.MockTransport(lambda r: httpx.Response(503, text="down"))
    r = await rugcheck_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://api.rugcheck.xyz/v1", governor=gov())
    assert not r.available and not r.passed   # fail-closed


async def test_goplus_combines_raw_fields_no_verdict_field():
    body = {"result": {MINT.lower(): {
        "mintable": {"status": "1", "authority": ["x"]},   # mintable -> critical
        "freezable": {"status": "0", "authority": []},
        "transfer_hook": [],
        "holders": []}}}
    t = httpx.MockTransport(lambda r: httpx.Response(200, json=body))
    r = await goplus_check(MINT, client=httpx.AsyncClient(transport=t),
                           base_url="https://api.gopluslabs.io/api/v1", governor=gov())
    assert not r.passed and r.hard and "mintable" in r.reason


async def test_goplus_pass_on_clean_fixture():
    # Real BONK GoPlus fixture: mintable.status == "0", freezable.status == "0",
    # transfer_hook == [] (falsy) -> no criticals -> passes. Also proves the result-key
    # lookup finds the row: GOPLUS_FIX keys the result by the ORIGINAL-CASE mint (not
    # lowercased), so this exercises `result.get(mint)` (the primary lookup branch),
    # not the `.lower()` fallback.
    t = httpx.MockTransport(lambda r: httpx.Response(200, json=GOPLUS_FIX))
    r = await goplus_check(MINT, client=httpx.AsyncClient(transport=t),
                           base_url="https://api.gopluslabs.io/api/v1", governor=gov())
    assert r.name == "goplus" and r.available and r.passed


async def test_rugcheck_breaker_recovers_after_open_window():
    # sustained rejected calls must NOT keep re-arming the open window; recovery must happen.
    import httpx
    from memebot.safety.governor import Governor
    now = [0.0]
    g = Governor(per_minute=600, clock=lambda: now[0], sleep=lambda s: __import__("asyncio").sleep(0),
                 failure_threshold=2, open_seconds=5.0)
    err = httpx.MockTransport(lambda r: httpx.Response(503, text="down"))
    ec = httpx.AsyncClient(transport=err)
    for _ in range(2):                                   # trip the breaker
        await rugcheck_check("M", client=ec, base_url="https://rc", governor=g)
    for _ in range(3):                                   # rejected calls at t=1,2,3 (within window)
        now[0] += 1.0
        r = await rugcheck_check("M", client=ec, base_url="https://rc", governor=g)
        assert not r.available                           # rejected via CircuitOpen
    # We are now at t=3.0. Jump to t=6.0: that is >= open_seconds (5.0) measured from the
    # ORIGINAL open at t=0, so a breaker that does NOT re-arm on rejected calls half-opens
    # and recovers. If rejected calls had re-armed _opened_at to t=3.0 (the bug), the window
    # would run to t=8.0 and this probe would still be rejected -> this assertion fails.
    # (t=6.0 discriminates; the coordinator's original +5.0 -> t=8.0 cleared even the
    # re-armed window and so passed with the bug present.)
    now[0] += 3.0                                        # -> t=6.0
    ok = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"score_normalised": 0, "risks": []})))
    r = await rugcheck_check("M", client=ok, base_url="https://rc", governor=g)
    assert r.available and r.passed                      # breaker half-opened and recovered


async def test_rugcheck_empty_body_is_unavailable():
    import httpx
    from memebot.safety.governor import Governor
    t = httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    r = await rugcheck_check("M", client=httpx.AsyncClient(transport=t), base_url="https://rc",
                             governor=Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0)))
    assert not r.available and not r.passed              # no report -> fail-closed


async def test_rugcheck_missing_risks_is_unavailable_not_clean_pass():
    import httpx
    from memebot.safety.governor import Governor
    t = httpx.MockTransport(lambda r: httpx.Response(200, json={"score_normalised": 0}))
    r = await rugcheck_check("M", client=httpx.AsyncClient(transport=t), base_url="https://rc",
                             governor=Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0)))
    assert not r.available and not r.passed
    assert r.reason == "check_unavailable"
    assert r.detail["reason"] == "malformed_report"


async def test_goplus_mint_absent_is_unavailable():
    import httpx
    from memebot.safety.governor import Governor
    t = httpx.MockTransport(lambda r: httpx.Response(200, json={"result": {}}))
    r = await goplus_check("M", client=httpx.AsyncClient(transport=t), base_url="https://gp",
                           governor=Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0)))
    assert not r.available and not r.passed


async def test_goplus_null_result_is_unavailable_not_exception():
    """Real GoPlus sometimes returns JSON with result=null for a mint.

    The safety gate must fail closed with a CheckResult, not raise AttributeError and
    force the outer gate_error fallback (which hides the provider-specific cause).
    """
    import httpx
    from memebot.safety.governor import Governor
    t = httpx.MockTransport(lambda r: httpx.Response(200, json={"result": None}))
    r = await goplus_check("M", client=httpx.AsyncClient(transport=t), base_url="https://gp",
                           governor=Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0)))
    assert r.name == "goplus"
    assert not r.available and not r.passed
    assert r.reason == "check_unavailable"
    assert r.detail["reason"] == "mint_not_in_result"


async def test_goplus_partial_row_is_unavailable_not_clean_pass():
    """A result row without the fields we consume is malformed provider data.

    Missing `mintable`/`freezable`/`transfer_hook` must not default to safe values and
    let a hard external rug gate pass.
    """
    import httpx
    from memebot.safety.governor import Governor
    t = httpx.MockTransport(lambda r: httpx.Response(200, json={"result": {"M": {"holders": []}}}))
    r = await goplus_check("M", client=httpx.AsyncClient(transport=t), base_url="https://gp",
                           governor=Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0)))
    assert r.name == "goplus"
    assert not r.available and not r.passed
    assert r.reason == "check_unavailable"
    assert r.detail["reason"] == "malformed_result_row"


async def test_goplus_malformed_required_values_are_unavailable_not_clean_pass():
    import httpx
    from memebot.safety.governor import Governor
    cases = [
        {"mintable": {"status": None}, "freezable": {"status": "0"}, "transfer_hook": []},
        {"mintable": {"status": "0"}, "freezable": {"status": "unknown"}, "transfer_hook": []},
        {"mintable": {"status": "0"}, "freezable": {"status": "0"}, "transfer_hook": None},
        {"mintable": {"status": "0"}, "freezable": {"status": "0"}, "transfer_hook": False},
        {"mintable": {"status": "0"}, "freezable": {"status": "0"}, "transfer_hook": ""},
        {"mintable": {"status": "0"}, "freezable": {"status": "0"}, "transfer_hook": {}},
    ]
    for row in cases:
        t = httpx.MockTransport(lambda r, row=row: httpx.Response(200, json={"result": {"M": row}}))
        r = await goplus_check("M", client=httpx.AsyncClient(transport=t), base_url="https://gp",
                               governor=Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0)))
        assert r.name == "goplus"
        assert not r.available and not r.passed, row
        assert r.reason == "check_unavailable"
        assert r.detail["reason"] == "malformed_result_row"
