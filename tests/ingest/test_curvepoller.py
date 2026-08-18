import ast
import asyncio
import base64
import json
import struct
from pathlib import Path

import httpx

from memebot.bus import EventBus
from memebot.events import CurveProgress, TokenGraduated
from memebot.ingest.curve import _DISCRIMINATOR
from memebot.ingest.curvepoller import CurvePoller
from memebot.store import open_db, set_token_state, upsert_token

CFG = {"interval_s": 0.05, "batch_size": 2, "max_tracked": 10}
PUMPFUN = {"sellable_supply": 793100000.0, "token_decimals": 6}


def test_curvepoller_fixtures_supply_source_boot_id():
    tree = ast.parse(Path(__file__).read_text())
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "CurvePoller"
    ]

    assert len(calls) == 7
    for call in calls:
        source_boot_id = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "source_boot_id"),
            None,
        )
        assert (
            isinstance(source_boot_id, ast.Constant)
            and type(source_boot_id.value) is int
            and source_boot_id.value > 0
        )


def account_b64(real_token_reserves: int, complete: bool = False) -> str:
    raw = _DISCRIMINATOR + struct.pack("<QQQQQ?", 10**9, 30 * 10**9,
                                       real_token_reserves, 0, 10**15, complete)
    return base64.b64encode(raw).decode()


def rpc_transport(responses: dict[str, str]):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        keys = body["params"][0]
        value = [{"data": [responses[k], "base64"], "lamports": 1} if k in responses
                 else None for k in keys]
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"],
                                         "result": {"value": value}})

    return httpx.MockTransport(handler), calls


async def test_polls_tracked_and_emits_progress(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")
    upsert_token(conn, mint="M2", created_at=2.0, bonding_curve_key="B2")
    half = int(793_100_000 * 10**6 * 0.5)
    transport, calls = rpc_transport({"B1": account_b64(half),
                                      "B2": account_b64(0, complete=True)})
    bus = EventBus()
    progress_q = bus.subscribe(CurveProgress)
    grad_q = bus.subscribe(TokenGraduated)
    poller = CurvePoller(bus, conn, cfg=CFG, pumpfun=PUMPFUN,
                         rpc_url="https://rpc.test",
                         client=httpx.AsyncClient(transport=transport),
                         source_boot_id=73)
    stop = asyncio.Event()
    task = asyncio.create_task(poller.run(stop))
    p = await asyncio.wait_for(progress_q.get(), 5)
    g = await asyncio.wait_for(grad_q.get(), 5)
    assert p.mint == "M1" and 49.0 < p.progress_pct < 51.0
    assert g.mint == "M2" and g.dex == "curve-complete"
    assert all(len(c["params"][0]) <= CFG["batch_size"] for c in calls)  # batching respected
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_rpc_error_emits_health_not_crash(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")

    def handler(request):
        return httpx.Response(500, text="boom")

    from memebot.events import AdapterHealth
    bus = EventBus()
    health_q = bus.subscribe(AdapterHealth)
    poller = CurvePoller(bus, conn, cfg=CFG, pumpfun=PUMPFUN,
                         rpc_url="https://rpc.test",
                         client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
                         source_boot_id=73)
    stop = asyncio.Event()
    task = asyncio.create_task(poller.run(stop))
    h = await asyncio.wait_for(health_q.get(), 5)
    assert h.adapter == "curvepoller" and h.status in ("down", "stale")
    assert not task.done()   # loop survived
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_skips_tokens_without_curve_key(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M3", created_at=1.0, bonding_curve_key="")
    transport, calls = rpc_transport({})
    bus = EventBus()
    poller = CurvePoller(bus, conn, cfg=CFG, pumpfun=PUMPFUN,
                         rpc_url="https://rpc.test",
                         client=httpx.AsyncClient(transport=transport),
                         source_boot_id=73)
    stop = asyncio.Event()
    task = asyncio.create_task(poller.run(stop))
    await asyncio.sleep(0.2)
    assert calls == []       # nothing pollable → no RPC calls at all
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_length_mismatched_rpc_response_skipped_no_misattribution(tmp_path):
    """A malformed RPC response with fewer values than keys must not let zip()
    misattribute account state to the wrong mint. The chunk is skipped entirely
    (no CurveProgress/TokenGraduated for it), the loop stays alive, and a
    subsequent well-formed poll still emits normally."""
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")
    upsert_token(conn, mint="M2", created_at=2.0, bonding_curve_key="B2")
    half = int(793_100_000 * 10**6 * 0.5)
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        keys = body["params"][0]
        call_count["n"] += 1
        n = call_count["n"]
        if n == 1:
            # malformed: only 1 value for 2 requested keys. If zip() were used
            # directly, B1's value would be wrongly attributed to M1 (or M2,
            # depending on key order) instead of being rejected outright.
            value = [{"data": [account_b64(0, complete=True), "base64"], "lamports": 1}]
        else:
            value = [{"data": [account_b64(half), "base64"], "lamports": 1} if k == "B1"
                     else {"data": [account_b64(half), "base64"], "lamports": 1}
                     for k in keys]
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"],
                                         "result": {"value": value}})

    transport = httpx.MockTransport(handler)
    bus = EventBus()
    progress_q = bus.subscribe(CurveProgress)
    grad_q = bus.subscribe(TokenGraduated)
    poller = CurvePoller(bus, conn, cfg=CFG, pumpfun=PUMPFUN,
                         rpc_url="https://rpc.test",
                         client=httpx.AsyncClient(transport=transport),
                         source_boot_id=73)
    stop = asyncio.Event()
    task = asyncio.create_task(poller.run(stop))

    # poll 1 is malformed (1 value for 2 keys) -> must be skipped entirely, no
    # TokenGraduated emitted (that would prove misattribution of B1's "complete"
    # state onto whichever mint zip() paired it with).
    # poll 2 is well-formed -> both CurveProgress events must arrive.
    e1 = await asyncio.wait_for(progress_q.get(), 5)
    e2 = await asyncio.wait_for(progress_q.get(), 5)
    assert {e1.mint, e2.mint} == {"M1", "M2"}
    assert grad_q.empty()          # no misattributed TokenGraduated ever emitted
    assert call_count["n"] >= 2
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_empty_data_account_skipped_then_healthy_token_still_polls(tmp_path):
    """~10% of live curve accounts return non-null but EMPTY data (recon fact).
    A token whose account goes empty on poll 2 must be skipped without crashing
    or emitting, and polling must continue: a healthy sibling token still gets
    its CurveProgress on poll 3."""
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")
    half = int(793_100_000 * 10**6 * 0.5)
    # batch_size=2 with a single tracked token (M1/B1) means every getMultipleAccounts
    # call is its own poll pass. Count calls directly instead of guessing key order.
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        keys = body["params"][0]
        call_count["n"] += 1
        n = call_count["n"]
        value = []
        for k in keys:
            if k == "B1" and n == 2:
                value.append({"data": ["", "base64"], "lamports": 1})  # empty on poll 2
            elif k == "B1":
                value.append({"data": [account_b64(half), "base64"], "lamports": 1})
            else:
                value.append(None)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"],
                                         "result": {"value": value}})

    transport = httpx.MockTransport(handler)
    bus = EventBus()
    progress_q = bus.subscribe(CurveProgress)
    poller = CurvePoller(bus, conn, cfg=CFG, pumpfun=PUMPFUN,
                         rpc_url="https://rpc.test",
                         client=httpx.AsyncClient(transport=transport),
                         source_boot_id=73)
    stop = asyncio.Event()
    task = asyncio.create_task(poller.run(stop))

    # poll 1: normal CurveProgress. poll 2: empty data -> skipped, no event, no crash.
    # poll 3: normal again -> proves the loop kept going past the empty poll.
    e1 = await asyncio.wait_for(progress_q.get(), 5)
    assert e1.mint == "M1"
    e2 = await asyncio.wait_for(progress_q.get(), 5)
    assert e2.mint == "M1"
    assert call_count["n"] >= 3   # at least 3 polls happened to reach this 2nd event past poll 2
    assert not task.done()        # loop survived the empty-data poll without crashing
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_poll_publishes_reserves_on_curveprogress(tmp_path):
    # build a non-complete BondingCurve account (discriminator + borsh <QQQQQ?)
    disc = __import__("hashlib").sha256(b"account:BondingCurve").digest()[:8]
    body = struct.pack("<QQQQQ?", 900_000_000_000_000, 31_000_000_000,
                       700_000_000_000_000, 1_000_000_000, 1_000_000_000_000_000, False)
    data_b64 = base64.b64encode(disc + body).decode()

    def handler(req):
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1,
            "result": {"value": [{"data": [data_b64, "base64"]}]}})

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=1.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    bus = EventBus()
    q = bus.subscribe(CurveProgress)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CurvePoller(bus, conn, cfg={"max_tracked": 10, "batch_size": 10, "interval_s": 10},
                         pumpfun={"sellable_supply": 793_100_000.0, "token_decimals": 6},
                         rpc_url="http://x", client=client, source_boot_id=73)
    await poller._poll_once()
    ev = await q.get()
    assert ev.real_sol_reserves == 1_000_000_000
    assert ev.virtual_sol_reserves == 31_000_000_000
    await client.aclose()


async def test_curvepoller_stamps_boot_and_strict_publication_sequence(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")
    upsert_token(conn, mint="M2", created_at=2.0, bonding_curve_key="B2")
    half = int(793_100_000 * 10**6 * 0.5)
    transport, _ = rpc_transport({"B1": account_b64(half), "B2": account_b64(half)})
    bus = EventBus()
    progress_q = bus.subscribe(CurveProgress)
    client = httpx.AsyncClient(transport=transport)
    poller = CurvePoller(
        bus,
        conn,
        cfg=CFG,
        pumpfun=PUMPFUN,
        rpc_url="https://rpc.test",
        client=client,
        source_boot_id=73,
    )

    await poller._poll_once()
    await poller._poll_once()
    events = [await progress_q.get() for _ in range(4)]

    assert [event.source_boot_id for event in events] == [73, 73, 73, 73]
    assert [event.source_seq for event in events] == [1, 2, 3, 4]
    await client.aclose()
