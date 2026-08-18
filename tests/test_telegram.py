import httpx
import pytest

from memebot.telegram import FakeTransport, HttpTransport, NullOps, TelegramOps


async def test_sends_alert_and_rate_limits():
    tp = FakeTransport()
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=2, clock=lambda: 0.0)
    await ops.alert("adapter down: pumpportal")
    await ops.alert("adapter down: curvepoller")
    await ops.alert("third alert - should be dropped by the hourly cap")
    assert len(tp.sent) == 2                       # rate-limited
    assert tp.sent[0]["chat_id"] == "42"
    assert "pumpportal" in tp.sent[0]["text"]


async def test_status_command_from_allowlisted_chat_only():
    tp = FakeTransport()
    tp.queue_update(chat_id="42", text="/status")     # allowlisted
    tp.queue_update(chat_id="999", text="/status")    # stranger - ignored
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=100, clock=lambda: 0.0,
                      status_fn=lambda: "adapters: 2 up | uptime 1h")
    await ops.poll_once()
    replies = [m for m in tp.sent if "adapters:" in m["text"]]
    assert len(replies) == 1 and replies[0]["chat_id"] == "42"   # only the allowlisted chat got a reply


class FailingTransport:
    """Transport whose send() always raises — proves ops-paging never breaks the caller."""

    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    async def send(self, chat_id: str, text: str) -> None:
        raise RuntimeError("network is down")

    async def get_updates(self) -> list[dict]:
        return []


async def test_alert_survives_transport_failure():
    tp = FailingTransport()
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=5, clock=lambda: 0.0)
    # Must not raise even though the transport's send() always blows up.
    await ops.alert("adapter down: pumpportal")


async def test_watch_budget_is_independent_of_buy_sell_alert_budget():
    from memebot.telegram import WatchLimiter

    tp = FakeTransport()
    limiter = WatchLimiter(clock=lambda: 0.0, max_per_hour=5)
    ops = TelegramOps(
        tp,
        chat_id="42",
        max_alerts_per_hour=1,
        clock=lambda: 0.0,
        watch_limiter=limiter,
    )

    for i in range(6):
        await ops.watch(f"WATCH {i}", mint=f"M{i}", segment="CLIMBING")
    await ops.alert("🟢 BUY remains deliverable")

    assert [message["text"] for message in tp.sent] == [
        "WATCH 0", "WATCH 1", "WATCH 2", "WATCH 3", "WATCH 4",
        "🟢 BUY remains deliverable",
    ]


async def test_trade_and_ops_alerts_cannot_mutate_or_replace_watch_limiter_state():
    from memebot.telegram import WatchLimiter

    tp = FakeTransport()
    limiter = WatchLimiter(clock=lambda: 0.0, max_per_hour=5)
    ops = TelegramOps(
        tp,
        chat_id="42",
        max_alerts_per_hour=100,
        clock=lambda: 0.0,
        watch_limiter=limiter,
    )
    for i in range(5):
        await ops.watch(f"WATCH {i}", mint=f"M{i}", segment="CLIMBING")
    send_times = list(limiter._send_times)
    seen = dict(limiter._seen)

    await ops.alert("🟢 BUY")
    await ops.alert("🔴 SELL")
    await ops.alert("adapter down")

    assert ops._watch_limiter is limiter
    assert limiter._send_times == send_times
    assert limiter._seen == seen
    await ops.watch("sixth WATCH", mint="M5", segment="CLIMBING")
    await ops.watch("duplicate WATCH", mint="M0", segment="CLIMBING")
    assert [message["text"] for message in tp.sent] == [
        "WATCH 0", "WATCH 1", "WATCH 2", "WATCH 3", "WATCH 4",
        "🟢 BUY", "🔴 SELL", "adapter down",
    ]


async def test_failed_watch_send_releases_dedupe_but_still_counts_attempt_cap():
    from memebot.telegram import WatchLimiter

    class FailOnceTransport(FakeTransport):
        def __init__(self):
            super().__init__()
            self.calls = []

        async def send(self, chat_id, text):
            self.calls.append(text)
            if len(self.calls) == 1:
                raise RuntimeError("transient network failure")
            await super().send(chat_id, text)

    tp = FailOnceTransport()
    limiter = WatchLimiter(clock=lambda: 0.0, max_per_hour=5)
    ops = TelegramOps(
        tp,
        chat_id="42",
        max_alerts_per_hour=100,
        clock=lambda: 0.0,
        watch_limiter=limiter,
    )

    await ops.watch("WATCH M", mint="M", segment="CLIMBING")       # attempt 1 fails
    await ops.watch("WATCH M", mint="M", segment="CLIMBING")       # same mint retries
    for i in range(2, 5):
        await ops.watch(f"WATCH M{i}", mint=f"M{i}", segment="CLIMBING")
    await ops.watch("WATCH M5", mint="M5", segment="CLIMBING")     # sixth attempt capped

    assert tp.calls == ["WATCH M", "WATCH M", "WATCH M2", "WATCH M3", "WATCH M4"]
    assert [message["text"] for message in tp.sent] == [
        "WATCH M", "WATCH M2", "WATCH M3", "WATCH M4",
    ]


async def test_concurrent_watch_calls_reserve_before_transport_await():
    import asyncio

    class BlockingTransport(FakeTransport):
        def __init__(self):
            super().__init__()
            self.calls = 0
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def send(self, chat_id, text):
            self.calls += 1
            self.entered.set()
            await self.release.wait()
            await super().send(chat_id, text)

    tp = BlockingTransport()
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=100, clock=lambda: 0.0)

    first = asyncio.create_task(ops.watch("first", mint="M", segment="CLIMBING"))
    second = None
    try:
        await asyncio.wait_for(tp.entered.wait(), 1.0)
        second = asyncio.create_task(ops.watch("second", mint="M", segment="CLIMBING"))
        for _ in range(3):
            await asyncio.sleep(0)

        assert second.done() is True
        assert second.result() is None
        assert tp.calls == 1
    finally:
        tp.release.set()
        await asyncio.gather(
            *(task for task in (first, second) if task is not None),
            return_exceptions=True,
        )


async def test_stale_failed_watch_cannot_release_newer_reservation_after_expiry():
    import asyncio

    class OrderedTransport(FakeTransport):
        def __init__(self):
            super().__init__()
            self.calls = []
            self.first_entered = asyncio.Event()
            self.second_entered = asyncio.Event()
            self.fail_first = asyncio.Event()
            self.release_second = asyncio.Event()

        async def send(self, chat_id, text):
            self.calls.append(text)
            if len(self.calls) == 1:
                self.first_entered.set()
                await self.fail_first.wait()
                raise RuntimeError("older delivery failed")
            if len(self.calls) == 2:
                self.second_entered.set()
                await self.release_second.wait()
            await super().send(chat_id, text)

    now = [0.0]
    tp = OrderedTransport()
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=100, clock=lambda: now[0])

    older = asyncio.create_task(ops.watch("older", mint="M", segment="CLIMBING"))
    await asyncio.wait_for(tp.first_entered.wait(), 1.0)
    now[0] = 86_400.0
    newer = asyncio.create_task(ops.watch("newer", mint="M", segment="CLIMBING"))
    await asyncio.wait_for(tp.second_entered.wait(), 1.0)

    tp.fail_first.set()
    await asyncio.wait_for(older, 1.0)
    await ops.watch("must remain deduped", mint="M", segment="CLIMBING")
    assert tp.calls == ["older", "newer"]

    tp.release_second.set()
    await asyncio.wait_for(newer, 1.0)


async def test_rate_limit_window_slides():
    tp = FakeTransport()
    now = [0.0]
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=1, clock=lambda: now[0])

    now[0] = 0.0
    await ops.alert("alert at t=0")          # sent (bucket empty -> allowed)

    now[0] = 100.0
    await ops.alert("alert at t=100")        # dropped (still within the 1h window)

    now[0] = 3700.0
    await ops.alert("alert at t=3700")       # sent again (window slid past 3600s)

    assert len(tp.sent) == 2
    assert "t=0" in tp.sent[0]["text"]
    assert "t=3700" in tp.sent[1]["text"]


async def test_alert_rejects_non_finite_clock_without_mutating_existing_cap_state():
    for invalid_now in (float("nan"), float("inf"), float("-inf")):
        now = [100.0]
        tp = FakeTransport()
        ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=1, clock=lambda: now[0])
        await ops.alert("existing alert")
        alert_times = ops._alert_times
        original_state = list(alert_times)

        now[0] = invalid_now
        await ops.alert("invalid-clock alert")
        assert len(tp.sent) == 1
        assert ops._alert_times is alert_times
        assert ops._alert_times == original_state

        now[0] = 200.0
        await ops.alert("finite alert remains capped")
        assert len(tp.sent) == 1
        assert ops._alert_times == original_state


async def test_alert_rejects_initial_non_finite_clock_without_mutating_cap_state():
    for invalid_now in (float("nan"), float("inf"), float("-inf")):
        now = [invalid_now]
        tp = FakeTransport()
        ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=1, clock=lambda: now[0])
        alert_times = ops._alert_times

        await ops.alert("invalid initial alert")
        assert tp.sent == []
        assert ops._alert_times is alert_times
        assert ops._alert_times == []

        now[0] = 100.0
        await ops.alert("first finite alert")
        assert [message["text"] for message in tp.sent] == ["first finite alert"]
        assert ops._alert_times == [100.0]


async def test_alert_backward_clock_does_not_expire_existing_cap_state():
    now = [100.0]
    tp = FakeTransport()
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=1, clock=lambda: now[0])
    await ops.alert("existing alert")
    original_state = list(ops._alert_times)

    now[0] = 0.0
    await ops.alert("backward-clock alert")

    assert [message["text"] for message in tp.sent] == ["existing alert"]
    assert ops._alert_times == original_state


def test_watch_limiter_default_cap_is_fifteen_per_rolling_hour():
    from memebot.telegram import WatchLimiter

    now = [0.0]
    limiter = WatchLimiter(clock=lambda: now[0])
    assert all(limiter.allow(f"M{i}", "CLIMBING") is not None for i in range(5))
    assert limiter.allow("M5", "CLIMBING") is not None
    assert all(limiter.allow(f"M{i}", "CLIMBING") is not None for i in range(6, 15))
    assert limiter.allow("M15", "CLIMBING") is None

    now[0] = 3600.0
    assert limiter.allow("M15", "CLIMBING") is not None


def test_watch_limiter_rolling_cap_and_exact_boundary():
    from memebot.telegram import WatchLimiter

    now = [0.0]
    limiter = WatchLimiter(clock=lambda: now[0], max_per_hour=5, dedupe_s=86_400.0)
    assert all(limiter.allow(f"M{i}", "CLIMBING") is not None for i in range(5))

    now[0] = 3599.999
    assert limiter.allow("M5", "CLIMBING") is None

    now[0] = 3600.0
    assert limiter.allow("M5", "CLIMBING") is not None


def test_watch_limiter_dedupes_mint_segment_until_exact_24h_boundary():
    from memebot.telegram import WatchLimiter

    now = [0.0]
    limiter = WatchLimiter(clock=lambda: now[0], max_per_hour=100, dedupe_s=86_400.0)
    assert limiter.allow("M", "CLIMBING") is not None
    assert limiter.allow("M", "CLIMBING") is None
    assert limiter.allow("M", "TRENDING") is not None  # segment is part of the key

    now[0] = 86_399.999
    assert limiter.allow("M", "CLIMBING") is None
    now[0] = 86_400.0
    assert limiter.allow("M", "CLIMBING") is not None


def test_watch_limiter_stale_release_cannot_delete_newer_reservation():
    from memebot.telegram import WatchLimiter

    now = [0.0]
    limiter = WatchLimiter(clock=lambda: now[0], max_per_hour=100, dedupe_s=86_400.0)
    older = limiter.allow("M", "CLIMBING")
    assert older is not None

    now[0] = 86_400.0
    newer = limiter.allow("M", "CLIMBING")
    assert newer is not None
    assert newer is not older

    limiter.release_dedupe("M", "CLIMBING", older)
    assert limiter.allow("M", "CLIMBING") is None


def test_watch_limiter_rejects_non_finite_clock_without_mutating_state():
    from memebot.telegram import WatchLimiter

    for invalid_now in (float("nan"), float("inf"), float("-inf")):
        now = [0.0]
        limiter = WatchLimiter(clock=lambda: now[0], max_per_hour=1, dedupe_s=86_400.0)
        assert limiter.allow("M", "CLIMBING") is not None
        send_times = list(limiter._send_times)
        seen = dict(limiter._seen)

        now[0] = invalid_now
        assert limiter.allow("OTHER", "CLIMBING") is None
        assert limiter._send_times == send_times
        assert limiter._seen == seen

        now[0] = 100.0
        assert limiter.allow("OTHER", "CLIMBING") is None  # original attempt still caps
        assert limiter.allow("M", "CLIMBING") is None      # original dedupe still holds


def test_watch_limiter_backward_clock_does_not_expire_existing_state():
    from memebot.telegram import WatchLimiter

    now = [100.0]
    limiter = WatchLimiter(clock=lambda: now[0], max_per_hour=1, dedupe_s=86_400.0)
    assert limiter.allow("M", "CLIMBING") is not None
    send_times = list(limiter._send_times)
    seen = dict(limiter._seen)

    now[0] = 0.0
    assert limiter.allow("OTHER", "CLIMBING") is None
    assert limiter._send_times == send_times
    assert limiter._seen == seen


async def test_malformed_update_does_not_block_valid_status():
    tp = FakeTransport()
    # a malformed update (null chat) queued BEFORE a valid owner /status
    tp._updates.append({"message": {"chat": None, "text": "/status"}})
    tp._updates.append({"message": None})
    tp.queue_update(chat_id="42", text="/status")
    ops = TelegramOps(tp, chat_id="42", max_alerts_per_hour=100, clock=lambda: 0.0,
                      status_fn=lambda: "ok status")
    await ops.poll_once()                                   # must NOT raise
    assert [m for m in tp.sent if m["text"] == "ok status"]  # the valid /status still got a reply


async def test_null_ops_alert_and_poll_are_noop():
    # Used when telegram is disabled: GateRunner/main call alert/poll_once unconditionally,
    # so NullOps must accept the same calls and simply do nothing (no exception, no send).
    ops = NullOps()
    await ops.alert("this should go nowhere")
    await ops.watch("this WATCH should go nowhere", mint="M", segment="CLIMBING")
    await ops.poll_once()   # must not raise even though there's no transport at all


async def test_http_transport_send_posts_to_telegram_api():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json as _json
        captured["url"] = str(request.url)
        captured["body"] = _json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "result": {}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    tp = HttpTransport(token="BOTTOKEN", client=client)
    await tp.send("42", "hello operator")

    assert captured["url"] == "https://api.telegram.org/botBOTTOKEN/sendMessage"
    assert captured["body"] == {"chat_id": "42", "text": "hello operator"}


async def test_alert_failure_does_not_log_token(caplog):
    import logging

    class TokenLeakTransport:
        async def send(self, chat_id, text):
            raise httpx.HTTPStatusError(
                "Client error '429' for url 'https://api.telegram.org/bot999:LEAKTOKEN/sendMessage'",
                request=httpx.Request("POST", "https://api.telegram.org/bot999:LEAKTOKEN/sendMessage"),
                response=httpx.Response(429))

        async def get_updates(self):
            return []

    ops = TelegramOps(TokenLeakTransport(), chat_id="1", max_alerts_per_hour=100, clock=lambda: 0.0)
    with caplog.at_level(logging.WARNING):
        await ops.alert("test")            # must not raise
        await ops.watch("watch", mint="M", segment="CLIMBING")  # must not raise either
    assert "LEAKTOKEN" not in caplog.text   # token never hits the log


async def test_status_send_failure_does_not_log_token(caplog):
    import logging

    token = "999:STATUSLEAKTOKEN"
    status_text = "adapters: 2 up | uptime 1h"

    class TokenLeakTransport:
        def __init__(self):
            self.send_calls = []

        async def send(self, chat_id, text):
            self.send_calls.append((chat_id, text))
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            raise httpx.HTTPStatusError(
                f"Client error '429' for url '{url}'",
                request=httpx.Request("POST", url),
                response=httpx.Response(429),
            )

        async def get_updates(self):
            return [
                {"message": {"chat": {"id": "999"}, "text": "/status"}},
                {"message": {"chat": {"id": "42"}, "text": "/status"}},
            ]

    transport = TokenLeakTransport()
    ops = TelegramOps(transport, chat_id="42", max_alerts_per_hour=100,
                      clock=lambda: 0.0, status_fn=lambda: status_text)

    with caplog.at_level(logging.WARNING):
        await ops.poll_once()

    assert transport.send_calls == [("42", status_text)]
    assert caplog.records
    for record in caplog.records:
        assert token not in record.getMessage()
        if record.exc_info:
            assert token not in repr(record.exc_info[1])
            assert token not in logging.Formatter().formatException(record.exc_info)
    assert token not in caplog.text


async def test_http_transport_get_updates_tracks_offset():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        if len(requests) == 1:
            return httpx.Response(200, json={"ok": True, "result": [
                {"update_id": 100, "message": {"chat": {"id": 42}, "text": "/status"}},
                {"update_id": 101, "message": {"chat": {"id": 42}, "text": "hi"}},
            ]})
        return httpx.Response(200, json={"ok": True, "result": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    tp = HttpTransport(token="BOTTOKEN", client=client)

    first = await tp.get_updates()
    assert len(first) == 2
    assert "offset" not in requests[0] or requests[0].get("offset") in (None, "0")

    await tp.get_updates()   # second call must request offset = last update_id + 1
    assert requests[1]["offset"] == "102"


def test_format_buy_and_sell_alert():
    from memebot.events import PaperEntry, PaperExit
    from memebot.telegram import format_buy_alert, format_sell_alert
    buy = format_buy_alert(PaperEntry(t_wall=1, t_mono=1, mint="MintABC", segment="CLIMBING",
                                      qty=1234.0, fill_price=2.5e-6, size_sol=0.2, score=77.0,
                                      realism_grade="B"))
    assert buy == (
        "🟢 BUY CLIMBING MintABC\n"
        "score 77 · size 0.200 SOL · qty 1,234\n"
        "fill 2.500e-06 SOL · grade B"
    )
    sell = format_sell_alert(PaperExit(t_wall=1, t_mono=1, mint="MintABC", segment="CLIMBING",
                                       qty=600.0, fill_price=5e-6, pnl_sol=0.31, reason="ladder_0",
                                       realism_grade="B"))
    assert sell == (
        "🟢 SELL CLIMBING MintABC (ladder_0)\n"
        "qty 600 · fill 5.000e-06 SOL · grade B\n"
        "P&L +0.310 SOL"
    )


def test_buy_alert_requires_explicit_canonical_recheck_proof():
    from dataclasses import replace

    from memebot.events import PaperEntry
    from memebot.telegram import format_buy_alert

    legacy = PaperEntry(
        t_wall=1.0,
        t_mono=2.0,
        mint="MintABC",
        segment="CLIMBING",
        qty=1234.0,
        fill_price=2.5e-6,
        size_sol=0.2,
        score=77.0,
        realism_grade="B",
    )
    proof = replace(
        legacy,
        canonical_status="CANONICAL",
        canonical_mint="MintABC",
        canonical_resolver_version="canonical-v1",
        canonical_recheck_id=11,
        canonical_recheck_hash="a" * 64,
        paper_trade_id=12,
        paper_entry_execution_id=13,
    )

    assert format_buy_alert(proof).endswith(
        "\ncanonical confirmed MintABC · canonical-v1 · "
        "recheck #11 · trade #12 · execution #13"
    )

    malformed = (
        legacy,
        replace(proof, canonical_status="canonical"),
        replace(proof, canonical_mint=""),
        replace(proof, canonical_mint="   "),
        replace(proof, canonical_resolver_version=""),
        replace(proof, canonical_recheck_hash=""),
        replace(proof, canonical_recheck_id=0),
        replace(proof, canonical_recheck_id=True),
        replace(proof, paper_trade_id=-1),
        replace(proof, paper_trade_id=1.0),
        replace(proof, paper_entry_execution_id=None),
        replace(proof, paper_entry_execution_id="13"),
    )
    assert all("canonical confirmed" not in format_buy_alert(event) for event in malformed)


def test_format_watch_alert_exact_and_unknown_safe():
    from types import SimpleNamespace

    from memebot.events import CandidateScored
    from memebot.telegram import format_watch_alert

    watch = format_watch_alert(CandidateScored(
        t_wall=1, t_mono=1, mint="MintABC", decision_id=7, segment="CLIMBING",
        score=12.34, spot_price_sol=2.5e-6,
    ))
    assert watch == (
        "👀 WATCH — NOT A BUY\n"
        "Mint: MintABC\n"
        "Segment: CLIMBING\n"
        "Score: 12.3\n"
        "Spot: 2.500e-06 SOL/token\n"
        "Pump.fun: https://pump.fun/coin/MintABC\n"
        "DexScreener: https://dexscreener.com/solana/MintABC\n"
        "Solscan: https://solscan.io/token/MintABC"
    )

    unknown = format_watch_alert(SimpleNamespace(
        mint=None, segment="", score=float("nan"), spot_price_sol=None,
    ))
    assert unknown == (
        "👀 WATCH — NOT A BUY\n"
        "Mint: UNKNOWN\n"
        "Segment: UNKNOWN\n"
        "Score: UNKNOWN\n"
        "Spot: UNKNOWN\n"
        "Pump.fun: https://pump.fun/coin/UNKNOWN\n"
        "DexScreener: https://dexscreener.com/solana/UNKNOWN\n"
        "Solscan: https://solscan.io/token/UNKNOWN"
    )


async def test_trade_alert_loop_forwards_to_ops():
    import asyncio
    from memebot.bus import EventBus
    from memebot.events import PaperEntry
    from memebot.main import _trade_alert_loop
    from memebot.telegram import FakeTransport, TelegramOps

    bus = EventBus()
    ops = TelegramOps(FakeTransport(), chat_id="C", max_alerts_per_hour=100)
    stop = asyncio.Event()
    task = asyncio.create_task(_trade_alert_loop(bus, ops, stop))
    await asyncio.sleep(0.05)   # let the loop reach bus.subscribe() before we publish
    await bus.publish(PaperEntry(t_wall=1, t_mono=1, mint="M", segment="CLIMBING", qty=10.0,
                                 fill_price=1e-6, size_sol=0.2, score=80.0, realism_grade="A"))
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert any("BUY" in s["text"] for s in ops._tp.sent)


async def test_watch_alert_loop_forwards_candidate_scored_to_fake_transport():
    import asyncio

    from memebot.bus import EventBus
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop
    from memebot.telegram import FakeTransport, TelegramOps

    bus = EventBus()
    transport = FakeTransport()
    ops = TelegramOps(transport, chat_id="C", max_alerts_per_hour=1, clock=lambda: 0.0)
    stop = asyncio.Event()
    task = asyncio.create_task(_watch_alert_loop(bus, ops, stop))
    await asyncio.sleep(0.05)
    await bus.publish(CandidateScored(
        t_wall=1, t_mono=1, mint="M", decision_id=1, segment="CLIMBING",
        score=9.0, spot_price_sol=1e-7,
    ))
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, 2)

    assert len(transport.sent) == 1
    assert transport.sent[0]["text"].startswith("👀 WATCH — NOT A BUY\n")


@pytest.mark.parametrize("exception_type", [RuntimeError, ValueError])
async def test_watch_alert_loop_continues_after_first_forwarding_exception(exception_type):
    import asyncio

    from memebot.bus import EventBus
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop

    forwarded_first = asyncio.Event()
    forwarded_second = asyncio.Event()

    class RaiseOnceOps:
        def __init__(self):
            self.calls = []

        async def watch(self, text, *, mint, segment):
            self.calls.append(mint)
            if len(self.calls) == 1:
                forwarded_first.set()
                raise exception_type("first forwarding failed")
            forwarded_second.set()

    async def reach(barrier):
        for _ in range(20):
            if barrier.is_set():
                return
            await asyncio.sleep(0)
        assert barrier.is_set() is True

    bus = EventBus()
    queue = bus.subscribe(CandidateScored)
    ops = RaiseOnceOps()
    stop = asyncio.Event()
    task = asyncio.create_task(_watch_alert_loop(bus, ops, stop, queue=queue))
    try:
        await bus.publish(CandidateScored(
            t_wall=1.0, t_mono=1.0, mint="FIRST", decision_id=1,
            segment="CLIMBING", score=9.0, spot_price_sol=1e-7,
        ))
        await reach(forwarded_first)
        for _ in range(6):
            await asyncio.sleep(0)

        assert task.done() is False

        await bus.publish(CandidateScored(
            t_wall=2.0, t_mono=2.0, mint="SECOND", decision_id=2,
            segment="CLIMBING", score=9.0, spot_price_sol=1e-7,
        ))
        await reach(forwarded_second)
        assert ops.calls == ["FIRST", "SECOND"]
        assert task.done() is False
    finally:
        stop.set()
        if not task.done():
            await asyncio.wait_for(task, 2)
        else:
            await asyncio.gather(task, return_exceptions=True)


async def test_watch_alert_loop_does_not_swallow_task_cancellation():
    import asyncio

    from memebot.bus import EventBus
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop

    forwarding = asyncio.Event()

    class BlockingOps:
        async def watch(self, text, *, mint, segment):
            forwarding.set()
            await asyncio.Event().wait()

    bus = EventBus()
    queue = bus.subscribe(CandidateScored)
    task = asyncio.create_task(
        _watch_alert_loop(bus, BlockingOps(), asyncio.Event(), queue=queue),
    )
    await bus.publish(CandidateScored(
        t_wall=1.0, t_mono=1.0, mint="FIRST", decision_id=1,
        segment="CLIMBING", score=9.0, spot_price_sol=1e-7,
    ))
    await asyncio.wait_for(forwarding.wait(), 1.0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert all(subscription.queue is not queue for subscription in bus._subs)


@pytest.mark.parametrize("exception_type", [SystemExit, KeyboardInterrupt])
async def test_watch_alert_loop_does_not_swallow_base_exceptions(exception_type):
    import asyncio

    from memebot.bus import EventBus
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop

    class RaisingOps:
        async def watch(self, text, *, mint, segment):
            raise exception_type("stop forwarding")

    bus = EventBus()
    queue = bus.subscribe(CandidateScored)
    await bus.publish(CandidateScored(
        t_wall=1.0, t_mono=1.0, mint="FIRST", decision_id=1,
        segment="CLIMBING", score=9.0, spot_price_sol=1e-7,
    ))

    with pytest.raises(exception_type, match="stop forwarding"):
        await _watch_alert_loop(bus, RaisingOps(), asyncio.Event(), queue=queue)

    assert all(subscription.queue is not queue for subscription in bus._subs)


async def test_watch_alert_loop_shutdown_releases_full_queue_publisher():
    import asyncio

    from memebot.bus import EventBus
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop

    forwarding_first = asyncio.Event()
    release_first = asyncio.Event()

    class BlockingOps:
        async def watch(self, text, *, mint, segment):
            forwarding_first.set()
            await release_first.wait()

    def scored(decision_id, mint):
        return CandidateScored(
            t_wall=float(decision_id), t_mono=float(decision_id), mint=mint,
            decision_id=decision_id, segment="CLIMBING", score=9.0,
            spot_price_sol=1e-7,
        )

    bus = EventBus(maxsize=1)
    queue = bus.subscribe(CandidateScored)
    stop = asyncio.Event()
    worker = asyncio.create_task(
        _watch_alert_loop(bus, BlockingOps(), stop, queue=queue),
    )
    third_publisher = None
    try:
        await bus.publish(scored(1, "FIRST"))
        await asyncio.wait_for(forwarding_first.wait(), 1.0)
        await bus.publish(scored(2, "SECOND"))
        third_publisher = asyncio.create_task(bus.publish(scored(3, "THIRD")))
        await asyncio.sleep(0)
        assert third_publisher.done() is False

        stop.set()
        release_first.set()
        await worker
        for _ in range(20):
            if third_publisher.done():
                break
            await asyncio.sleep(0)

        assert third_publisher.done() is True
        assert third_publisher.result() is None
        assert all(subscription.queue is not queue for subscription in bus._subs)
        assert (await queue.get()).mint == "SECOND"
        assert queue.empty()
    finally:
        stop.set()
        release_first.set()
        if not worker.done():
            worker.cancel()
        if third_publisher is not None and not third_publisher.done():
            third_publisher.cancel()
        await asyncio.gather(
            *(task for task in (worker, third_publisher) if task is not None),
            return_exceptions=True,
        )
