import asyncio

import pytest

from memebot.safety.governor import CircuitOpen, Governor


async def test_allows_up_to_rate_then_waits():
    now = [0.0]
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)
        now[0] += s

    g = Governor(per_minute=60, clock=lambda: now[0], sleep=fake_sleep)  # 1/sec, burst=capacity
    for _ in range(3):
        await g.acquire()          # burst tokens consumed, no wait
    assert sleeps == []
    await g.acquire()              # bucket empty -> must wait ~1s for a refill
    assert sleeps and sleeps[0] > 0


async def test_circuit_opens_after_consecutive_failures_and_half_opens():
    now = [0.0]
    g = Governor(per_minute=600, clock=lambda: now[0], sleep=lambda s: asyncio.sleep(0),
                 failure_threshold=3, open_seconds=5.0)
    for _ in range(3):
        g.record_failure()
    with pytest.raises(CircuitOpen):
        await g.acquire()          # open -> rejects immediately
    now[0] += 6.0                  # past open_seconds -> half-open probe allowed
    await g.acquire()              # half-open: one probe permitted
    g.record_success()            # closes the circuit
    await g.acquire()              # closed again


async def test_half_open_probe_failure_reopens_circuit():
    now = [0.0]
    g = Governor(per_minute=600, clock=lambda: now[0], sleep=lambda s: asyncio.sleep(0),
                 failure_threshold=3, open_seconds=5.0)
    for _ in range(3):
        g.record_failure()
    with pytest.raises(CircuitOpen):
        await g.acquire()          # confirm initially open

    now[0] += 6.0                  # past open_seconds -> half-open probe allowed
    await g.acquire()              # half-open probe permitted
    g.record_failure()             # ...but the probe FAILS -> must re-open, not stay half-open

    # Still within the fresh open window measured from the re-open moment (now[0]==6.0):
    # only 0.1s elapsed, nowhere near open_seconds=5.0 -> must reject immediately again.
    now[0] += 0.1
    with pytest.raises(CircuitOpen):
        await g.acquire()


async def test_per_minute_zero_rejected():
    import pytest
    from memebot.safety.governor import Governor
    with pytest.raises(ValueError):
        Governor(per_minute=0)
    with pytest.raises(ValueError):
        Governor(per_minute=-5)
