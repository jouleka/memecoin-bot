import asyncio
import json
from pathlib import Path

from memebot.bus import EventBus
from memebot.ingest.pumpportal import parse_frame
from memebot.lifecycle import LifecycleTracker
from memebot.store import open_db

FIXTURE = Path("tests/fixtures/providers/pumpportal/session1.jsonl")
CFG = {"climbing_progress_pct": 10.0, "stall_progress_pct": 5.0,
       "dead_after_stalled_s": 7200.0, "dead_no_activity_s": 172800.0}
RUNTIME_BOOT_ID = 1
RUNTIME_CAUSAL_FLOOR = 0.0


async def replay(tmp_path, name):
    conn = open_db(tmp_path / f"{name}.db")
    bus = EventBus()
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 10_000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    t = 0.0
    for line in FIXTURE.read_text().splitlines():
        raw = json.loads(line)["raw"]
        t += 1.0
        event = parse_frame(raw, t_wall=t, t_mono=t)
        if event is not None:
            await bus.publish(event)
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 5)
    rows = conn.execute(
        "SELECT mint, state FROM tokens ORDER BY mint").fetchall()
    return rows


async def test_replay_is_deterministic_and_counts_match(tmp_path):
    rows1 = await replay(tmp_path, "a")
    rows2 = await replay(tmp_path, "b")
    assert rows1 == rows2                       # deterministic
    states = [s for _, s in rows1]
    # Registry size ceiling, not a plain create count: the fixture's 197 pump
    # creates cover only 188 unique mints (one mint, the USDC-address-shaped
    # "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", repeats 10x — an upstream
    # fixture artifact) which correctly collapse to one row each under the
    # `tokens.mint` PRIMARY KEY (upsert idempotency, proven separately in
    # test_store.py). 5 of the fixture's 7 migrate frames name mints never seen
    # as a create in this same session and are adopted on graduation (the
    # "bot was down for the birth" path in LifecycleTracker._handle). So the
    # true registry size is 188 + 5 = 193, not >=197 as first assumed here —
    # confirmed by direct fixture inspection during C8 debugging.
    assert len(rows1) == 193
    assert states.count("GRADUATED") == 7       # all 7 migrate frames resolved: 2 seen-at-birth + 5 adopted
    assert all(s in ("FRESH", "GRADUATED") for s in states)  # no curve data in this replay


def test_e2e_replay_lifecycle_supplies_runtime_boot_and_floor():
    import ast

    tree = ast.parse(Path(__file__).read_text())
    tracker_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "LifecycleTracker"
    ]

    assert len(tracker_calls) == 1
    tracker_keywords = {
        keyword.arg: keyword.value
        for keyword in tracker_calls[0].keywords
    }
    assert "runtime_boot_id" in tracker_keywords
    assert "runtime_causal_floor" in tracker_keywords
