import httpx

from memebot.safety.governor import Governor
from memebot.safety.honeypot import honeypot_check

MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
WSOL = "So11111111111111111111111111111111111111112"


def gov():
    return Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0))


def quote(out_amount, impact="0.5"):
    return {"outAmount": str(out_amount), "priceImpactPct": impact}


async def test_honeypot_pass_round_trip_ok():
    # Realistic lamport scale: buy spends PROBE_LAMPORTS (1e9 = 1 SOL); sell returns
    # 990_000_000 lamports (~0.99 SOL) => ~1% round-trip loss (passes under 30). The sell
    # leg's outAmount is LAMPORTS (output mint = WSOL), same unit as the 1e9 input, so the
    # SOL-in-vs-SOL-out loss is dimensionally sound.
    def handler(request):
        if request.url.params.get("inputMint") == WSOL:
            return httpx.Response(200, json=quote(1_000_000))       # buy -> tokens (count arbitrary)
        return httpx.Response(200, json=quote(990_000_000, "0.5"))  # sell -> ~0.99 SOL back
    t = httpx.MockTransport(handler)
    r = await honeypot_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://lite-api.jup.ag/swap/v1", governor=gov(),
                             max_impact_pct=30.0)
    assert r.passed and r.name == "honeypot"


async def test_honeypot_hardfail_no_sell_route():
    def handler(request):
        if request.url.params.get("inputMint") == WSOL:
            return httpx.Response(200, json=quote(1_000_000))
        return httpx.Response(400, json={"error": "no route found"})   # can't sell -> honeypot
    t = httpx.MockTransport(handler)
    r = await honeypot_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://lite-api.jup.ag/swap/v1", governor=gov(),
                             max_impact_pct=30.0)
    assert not r.passed and r.hard and r.reason == "no_sell_route"


async def test_honeypot_hardfail_high_round_trip_impact():
    def handler(request):
        if request.url.params.get("inputMint") == WSOL:
            return httpx.Response(200, json=quote(1_000_000))
        return httpx.Response(200, json=quote(500_000_000, "0.5"))   # ~0.5 SOL back = 50% loss -> tax/honeypot
    t = httpx.MockTransport(handler)
    r = await honeypot_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://lite-api.jup.ag/swap/v1", governor=gov(),
                             max_impact_pct=30.0)
    assert not r.passed and r.hard and r.reason == "round_trip_loss"


async def test_honeypot_circuit_open_does_not_rearm_and_recovers():
    # the D4 breaker-recovery pattern applied to honeypot: rejected calls must not re-arm
    # the window. NOTE (deviation from the plan's literal test): the trip must come from a
    # genuine httpx.HTTPError, not a clean 4xx/5xx status. `_quote` treats status >= 400 as
    # a graceful "no route" -> returns None WITHOUT raising, so it never reaches
    # governor.record_failure(). A transport that returns 500 would therefore never trip the
    # breaker in the corrected code, and this test would hang/fail for the wrong reason. Use
    # a transport that RAISES a transport-level httpx exception (ConnectError) so the
    # `except httpx.HTTPError` branch is actually exercised and record_failure() is reached.
    now = [0.0]
    g = Governor(per_minute=600, clock=lambda: now[0], sleep=lambda s: __import__("asyncio").sleep(0),
                 failure_threshold=2, open_seconds=5.0)

    def raise_handler(request):
        raise httpx.ConnectError("boom", request=request)

    err = httpx.AsyncClient(transport=httpx.MockTransport(raise_handler))
    for _ in range(2):                                   # trip breaker (2 real ConnectErrors on the buy quote)
        await honeypot_check(MINT, client=err, base_url="https://jup", governor=g, max_impact_pct=30.0)
    for _ in range(3):                                   # rejected calls within the window
        now[0] += 1.0
        r = await honeypot_check(MINT, client=err, base_url="https://jup", governor=g, max_impact_pct=30.0)
        assert not r.available
    now[0] += 3.0                                        # +3 past the ORIGINAL open (0.0) = t=6 >= 5
    ok = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json=quote(1_000_000) if r.url.params.get("inputMint") == WSOL else quote(990_000_000))))
    r = await honeypot_check(MINT, client=ok, base_url="https://jup", governor=g, max_impact_pct=30.0)
    assert r.available and r.passed                      # recovered (window measured from origin)


async def test_honeypot_zero_buy_amount_is_hardfail():
    def handler(request):
        return httpx.Response(200, json=quote(0))   # buy returns 0 tokens
    t = httpx.MockTransport(handler)
    r = await honeypot_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://lite-api.jup.ag/swap/v1", governor=gov(),
                             max_impact_pct=30.0)
    assert not r.passed and r.hard and r.reason == "no_buy_route"


async def test_honeypot_catches_expensive_token_honeypot():
    # Expensive token: 1 SOL buys few tokens, sell returns far less SOL than put in.
    # This is the case the token-anchored formula wrongly passed.
    def handler(request):
        if request.url.params.get("inputMint") == WSOL:
            return httpx.Response(200, json=quote(1000))          # buy 1 SOL -> 1000 tokens
        return httpx.Response(200, json=quote(1_000_000))         # sell -> 0.001 SOL back = 99.9% loss
    t = httpx.MockTransport(handler)
    r = await honeypot_check(MINT, client=httpx.AsyncClient(transport=t),
                             base_url="https://lite-api.jup.ag/swap/v1", governor=gov(),
                             max_impact_pct=30.0)
    assert not r.passed and r.hard and r.reason == "round_trip_loss"   # honeypot caught
    assert r.detail["round_trip_loss_pct"] > 99.0
