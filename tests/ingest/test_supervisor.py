import asyncio

from memebot.bus import EventBus
from memebot.events import AdapterHealth
from memebot.ingest.supervisor import supervise


async def test_restarts_crashing_adapter_with_backoff():
    runs = []

    async def flaky(stop):
        runs.append(1)
        if len(runs) < 3:
            raise RuntimeError("boom")
        await stop.wait()

    bus = EventBus()
    health_q = bus.subscribe(AdapterHealth)
    stop = asyncio.Event()
    task = asyncio.create_task(
        supervise("flaky", flaky, bus, stop, base_delay_s=0.02, max_delay_s=0.1))
    await asyncio.sleep(0.5)
    assert len(runs) == 3          # crashed twice, then settled
    stop.set()
    await asyncio.wait_for(task, 5)
    statuses = []
    while not health_q.empty():
        statuses.append(health_q.get_nowait().status)
    assert statuses.count("down") == 2


async def test_supervisor_exits_promptly_on_stop():
    async def steady(stop):
        await stop.wait()

    bus = EventBus()
    stop = asyncio.Event()
    task = asyncio.create_task(supervise("steady", steady, bus, stop,
                                         base_delay_s=30.0, max_delay_s=60.0))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1)   # must not be stuck in a backoff sleep
