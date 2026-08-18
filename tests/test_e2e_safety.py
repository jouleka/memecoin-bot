"""D11: end-to-end safety cascade — a rug token flows through the REAL pipeline
(LifecycleTracker + GateRunner/SafetyGate/LiveProbes + a telegram-ops consumer)
to DEAD(rugged) + a Telegram alert. All IO mocked (httpx.MockTransport /
FakeTransport); nothing here is a stub of production logic — it is the real
classes wired together, proving the M3 safety gate integration end-to-end.
"""
from __future__ import annotations

import asyncio
import json

import httpx

from memebot.bus import EventBus
from memebot.events import CurveProgress, SafetyHardFail, TokenCreated
from memebot.lifecycle import LifecycleTracker
from memebot.safety.gate import GateRunner, LiveProbes, SafetyGate
from memebot.safety.governor import Governor
from memebot.store import get_token, latest_safety_report, open_db
from memebot.telegram import FakeTransport, TelegramOps

CFG = {"climbing_progress_pct": 10.0, "stall_progress_pct": 5.0,
       "dead_after_stalled_s": 7200.0, "dead_no_activity_s": 172800.0}
SAFETY_CFG = {"top10_holder_max_pct": 30.0, "dev_wallet_max_pct": 10.0,
              "honeypot_max_impact_pct": 30.0, "rugcheck_base": "https://rc",
              "goplus_base": "https://gp", "jupiter_base": "https://jup"}
RUNTIME_BOOT_ID = 1
RUNTIME_CAUSAL_FLOOR = 0.0


def gov() -> Governor:
    return Governor(per_minute=600, sleep=lambda s: asyncio.sleep(0))


def rug_rpc() -> httpx.MockTransport:
    def handler(request):
        body = json.loads(request.content)
        if body["method"] == "getAccountInfo":     # mint authority ACTIVE = rug
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": {"data": {
                "parsed": {"info": {"mintAuthority": "EVIL", "freezeAuthority": None,
                                    "supply": "1000", "decimals": 6}}}}}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": []}})
    return httpx.MockTransport(handler)


async def run_cascade(tmp_path, name: str):
    """Wires the real LifecycleTracker + GateRunner/SafetyGate/LiveProbes + a
    telegram-ops bus consumer, publishes a token birth + climb, and lets the
    rug verdict (mint authority active) cascade to DEAD + an alert."""
    conn = open_db(tmp_path / f"{name}.db")
    bus = EventBus()
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 100.0,
    )

    probes = LiveProbes(
        rpc_url="https://rpc.test",
        rpc_client=httpx.AsyncClient(transport=rug_rpc()),
        ext_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"score_normalised": 0, "risks": []}))),
        jup_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"outAmount": "1000000000", "priceImpactPct": "0"}))),
        conn=conn, cfg=SAFETY_CFG,
        governors={"rugcheck": gov(), "goplus": gov(), "jupiter": gov()})
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    gate_runner = GateRunner(bus, conn, gate)

    tp = FakeTransport()
    ops = TelegramOps(tp, chat_id="1", max_alerts_per_hour=100, clock=lambda: 100.0)
    hardfail_q = bus.subscribe(SafetyHardFail)

    async def alert_consumer(stop):
        # Minimal telegram-ops consumer: real TelegramOps.alert() over a
        # FakeTransport, subscribing to the same SafetyHardFail the lifecycle
        # tracker consumes -- proves the gate's verdict fans out to BOTH
        # sinks (store state + operator paging) from one published event.
        while not stop.is_set():
            try:
                ev = await asyncio.wait_for(hardfail_q.get(), 0.2)
            except TimeoutError:
                continue
            await ops.alert(f"RUG {ev.mint}: {','.join(ev.reasons)}")

    stop = asyncio.Event()
    tasks = [asyncio.create_task(tracker.run(stop)),
             asyncio.create_task(gate_runner.run(stop)),
             asyncio.create_task(alert_consumer(stop))]

    await bus.publish(TokenCreated(t_wall=100.0, t_mono=1.0, mint="RUG", name="", symbol="",
                                   creator="EVIL", raw={"bondingCurveKey": "B"}))
    await bus.publish(CurveProgress(
        t_wall=100.0, t_mono=2.0, mint="RUG", progress_pct=15.0,
        virtual_sol_reserves=70_000_000_000,
        virtual_token_reserves=70_000_000_000_000,
        real_sol_reserves=20_000_000_000,
        real_token_reserves=400_000_000_000_000,
        source_boot_id=RUNTIME_BOOT_ID,
        source_seq=1,
    ))
    await asyncio.sleep(0.5)
    stop.set()
    for t in tasks:
        await asyncio.wait_for(t, 5)

    return conn, tp


async def test_rug_token_flows_to_dead_and_alert(tmp_path):
    conn, tp = await run_cascade(tmp_path, "t")

    row = get_token(conn, "RUG")
    assert row["state"] == "DEAD" and row["rugged"] == 1
    assert any("RUG RUG" in m["text"] for m in tp.sent)
    assert latest_safety_report(conn, "RUG") is not None


async def test_rug_cascade_is_deterministic_3x(tmp_path):
    # Same scenario, 3 independent runs (separate DB files) -- final state and
    # alert outcome must be identical every time (D11 Step 2 requirement).
    outcomes = []
    for i in range(3):
        conn, tp = await run_cascade(tmp_path, f"run{i}")
        row = get_token(conn, "RUG")
        outcomes.append((row["state"], row["rugged"],
                         any("RUG RUG" in m["text"] for m in tp.sent)))
    assert outcomes == [("DEAD", 1, True)] * 3


def test_e2e_safety_lifecycle_supplies_runtime_boot_and_floor():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    tracker_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "LifecycleTracker"
    ]
    progress_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "CurveProgress"
    ]

    assert len(tracker_calls) == 1
    assert len(progress_calls) == 1
    tracker_keywords = {
        keyword.arg: keyword.value
        for keyword in tracker_calls[0].keywords
    }
    assert "runtime_boot_id" in tracker_keywords
    assert "runtime_causal_floor" in tracker_keywords

    progress_keywords = {
        keyword.arg: keyword.value
        for keyword in progress_calls[0].keywords
    }
    assert {
        "source_boot_id",
        "source_seq",
        "virtual_sol_reserves",
        "virtual_token_reserves",
        "real_sol_reserves",
        "real_token_reserves",
    } <= progress_keywords.keys()
