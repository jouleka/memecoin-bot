import asyncio
import json

import websockets

from memebot.bus import EventBus
from memebot.events import AdapterHealth, TokenCreated
from memebot.ingest.pumpportal import PumpPortalStream

CREATE = json.dumps({"txType": "create", "pool": "pump", "mint": "M1",
                     "name": "N", "symbol": "S", "traderPublicKey": "C",
                     "bondingCurveKey": "B1", "vSolInBondingCurve": 30.0,
                     "vTokensInBondingCurve": 1073000000.0, "signature": "s1"})


async def _serve(handler):
    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}"


async def test_emits_events_and_subscribes():
    got_subs = []

    async def handler(ws):
        got_subs.append(await ws.recv())
        got_subs.append(await ws.recv())
        await ws.send(CREATE)
        await asyncio.sleep(1)

    server, uri = await _serve(handler)
    bus = EventBus()
    q = bus.subscribe(TokenCreated)
    stop = asyncio.Event()
    stream = PumpPortalStream(bus, uri=uri, stale_after_s=5.0)
    task = asyncio.create_task(stream.run(stop))
    e = await asyncio.wait_for(q.get(), 5)
    assert e.mint == "M1"
    assert any("subscribeNewToken" in s for s in got_subs)
    assert any("subscribeMigration" in s for s in got_subs)
    stop.set()
    await asyncio.wait_for(task, 5)
    server.close()


async def test_reconnects_after_drop_and_resubscribes():
    connections = []

    async def handler(ws):
        connections.append(1)
        await ws.recv()  # sub 1
        await ws.recv()  # sub 2
        if len(connections) == 1:
            await ws.close()      # first connection dies immediately after subs
        else:
            await ws.send(CREATE)
            await asyncio.sleep(1)

    server, uri = await _serve(handler)
    bus = EventBus()
    q = bus.subscribe(TokenCreated)
    health_q = bus.subscribe(AdapterHealth)
    stop = asyncio.Event()
    stream = PumpPortalStream(bus, uri=uri, stale_after_s=5.0, base_backoff_s=0.05)
    task = asyncio.create_task(stream.run(stop))
    e = await asyncio.wait_for(q.get(), 5)   # arrives on connection 2
    assert e.mint == "M1" and len(connections) == 2
    kinds = []
    while not health_q.empty():
        kinds.append(health_q.get_nowait().status)
    assert "up" in kinds and "down" in kinds
    stop.set()
    await asyncio.wait_for(task, 5)
    server.close()


async def test_staleness_forces_reconnect():
    connections = []

    async def handler(ws):
        connections.append(1)
        await ws.recv()
        await ws.recv()
        if len(connections) == 1:
            await asyncio.sleep(10)   # silent connection — must be declared stale
        else:
            await ws.send(CREATE)
            await asyncio.sleep(1)

    server, uri = await _serve(handler)
    bus = EventBus()
    q = bus.subscribe(TokenCreated)
    stop = asyncio.Event()
    stream = PumpPortalStream(bus, uri=uri, stale_after_s=0.2, base_backoff_s=0.05)
    task = asyncio.create_task(stream.run(stop))
    e = await asyncio.wait_for(q.get(), 5)
    assert e.mint == "M1" and len(connections) >= 2
    stop.set()
    await asyncio.wait_for(task, 5)
    server.close()
