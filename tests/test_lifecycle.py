import asyncio
import json
import math
import sqlite3

import pytest

from memebot.bus import EventBus
from memebot.events import (CurveProgress, LifecycleTransition, TokenCreated,
                            TokenGraduated)
from memebot.lifecycle import LifecycleTracker, decide
from memebot.store import (get_token, open_db, set_token_state, upsert_token,
                           upsert_token_identity)

CFG = {"climbing_progress_pct": 10.0, "stall_progress_pct": 5.0,
       "dead_after_stalled_s": 7200.0, "dead_no_activity_s": 172800.0}
RUNTIME_BOOT_ID = 1
RUNTIME_CAUSAL_FLOOR = 0.0


def test_decide_fresh_to_climbing():
    assert decide("FRESH", progress=12.0, age_s=60, idle_s=5, cfg=CFG) == "CLIMBING"


def test_decide_fresh_stall_to_dead():
    assert decide("FRESH", progress=1.0, age_s=8000, idle_s=8000, cfg=CFG) == "DEAD"


def test_decide_climbing_inactivity_to_dead():
    assert decide("CLIMBING", progress=40.0, age_s=999999, idle_s=200000, cfg=CFG) == "DEAD"


def test_decide_no_change_returns_none():
    assert decide("FRESH", progress=1.0, age_s=60, idle_s=5, cfg=CFG) is None
    assert decide("GRADUATED", progress=100.0, age_s=1, idle_s=1, cfg=CFG) is None
    assert decide("DEAD", progress=99.0, age_s=1, idle_s=1, cfg=CFG) is None


async def test_tracker_end_to_end(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 1000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))

    await bus.publish(TokenCreated(t_wall=1000.0, t_mono=1.0, mint="M1", name="N",
                                   symbol="S", creator="C",
                                   raw={"bondingCurveKey": "B1"}))
    await bus.publish(CurveProgress(t_wall=1001.0, t_mono=2.0, mint="M1",
                                    progress_pct=15.0,
                                    virtual_sol_reserves=70_000_000_000,
                                    virtual_token_reserves=70_000_000_000_000,
                                    real_sol_reserves=20_000_000_000,
                                    real_token_reserves=400_000_000_000_000,
                                    source_boot_id=RUNTIME_BOOT_ID,
                                    source_seq=1))
    t1 = await asyncio.wait_for(transitions.get(), 5)
    assert (t1.mint, t1.from_state, t1.to_state) == ("M1", "FRESH", "CLIMBING")

    await bus.publish(TokenGraduated(t_wall=1002.0, t_mono=3.0, mint="M1",
                                     pool="", dex="pump-amm", raw={}))
    t2 = await asyncio.wait_for(transitions.get(), 5)
    assert t2.to_state == "GRADUATED"
    # duplicate graduation signal (curve complete-flag) must be idempotent:
    await bus.publish(TokenGraduated(t_wall=1003.0, t_mono=4.0, mint="M1",
                                     pool="", dex="curve-complete", raw={}))
    await asyncio.sleep(0.1)
    assert transitions.empty()

    row = get_token(conn, "M1")
    assert row["state"] == "GRADUATED" and row["bonding_curve_key"] == "B1"
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_flatlining_token_goes_dead(tmp_path):
    # Resolution 1 regression: equal-progress polls must not refresh last_seen.
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    now = [1000.0]
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: now[0],
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))

    await bus.publish(TokenCreated(t_wall=1000.0, t_mono=1.0, mint="M2", name="",
                                   symbol="", creator="C", raw={}))
    await bus.publish(CurveProgress(t_wall=1001.0, t_mono=2.0, mint="M2",
                                    progress_pct=1.0,
                                    virtual_sol_reserves=70_000_000_000,
                                    virtual_token_reserves=70_000_000_000_000,
                                    real_sol_reserves=20_000_000_000,
                                    real_token_reserves=400_000_000_000_000,
                                    source_boot_id=RUNTIME_BOOT_ID,
                                    source_seq=1))     # first observation
    for i in range(3):
        now[0] += 3000.0                                    # clock marches on
        await bus.publish(CurveProgress(t_wall=now[0], t_mono=3.0 + i, mint="M2",
                                        progress_pct=1.0,
                                        virtual_sol_reserves=70_000_000_000,
                                        virtual_token_reserves=70_000_000_000_000,
                                        real_sol_reserves=20_000_000_000,
                                        real_token_reserves=400_000_000_000_000,
                                        source_boot_id=RUNTIME_BOOT_ID,
                                        source_seq=i + 2))  # identical progress
    t = await asyncio.wait_for(transitions.get(), 5)
    assert (t.mint, t.to_state) == ("M2", "DEAD")           # stall rule fired
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_graduation_for_unknown_mint_adopts(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 1000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(TokenGraduated(t_wall=1000.0, t_mono=1.0, mint="M9",
                                     pool="", dex="pump-amm", raw={}))
    t = await asyncio.wait_for(transitions.get(), 5)
    assert (t.mint, t.to_state) == ("M9", "GRADUATED")
    assert get_token(conn, "M9") is not None
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_graduation_resurrects_dead_token(tmp_path):
    # Owner decision (C10): authoritative on-chain graduation overrides an inferred-DEAD state.
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="MD", created_at=1.0, bonding_curve_key="B")
    set_token_state(conn, "MD", "DEAD")               # abandoned early
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 2000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(TokenGraduated(t_wall=2000.0, t_mono=1.0, mint="MD",
                                     pool="", dex="pump-amm", raw={}))
    t = await asyncio.wait_for(transitions.get(), 5)
    assert (t.mint, t.from_state, t.to_state) == ("MD", "DEAD", "GRADUATED")
    assert get_token(conn, "MD")["state"] == "GRADUATED"
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_curveprogress_does_not_resurrect_dead(tmp_path):
    # Inferred signals must NOT revive DEAD — only authoritative graduation.
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="MD2", created_at=1.0, bonding_curve_key="B")
    set_token_state(conn, "MD2", "DEAD")
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 2000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(CurveProgress(t_wall=2000.0, t_mono=1.0, mint="MD2",
                                    progress_pct=80.0,
                                    virtual_sol_reserves=70_000_000_000,
                                    virtual_token_reserves=70_000_000_000_000,
                                    real_sol_reserves=20_000_000_000,
                                    real_token_reserves=400_000_000_000_000,
                                    source_boot_id=RUNTIME_BOOT_ID,
                                    source_seq=1))
    await asyncio.sleep(0.1)
    assert transitions.empty()                        # no revival from CurveProgress
    assert get_token(conn, "MD2")["state"] == "DEAD"
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_safety_hard_fail_marks_rugged_dead(tmp_path):
    from memebot.events import SafetyHardFail
    from memebot.store import get_token, set_token_state, upsert_token
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="RG", created_at=1.0, bonding_curve_key="B")
    set_token_state(conn, "RG", "CLIMBING", progress_pct=20.0)
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 5000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(SafetyHardFail(t_wall=5000.0, t_mono=1.0, mint="RG",
                                     reasons=("mint_authority_active",)))
    t = await asyncio.wait_for(transitions.get(), 5)
    assert (t.mint, t.to_state) == ("RG", "DEAD")
    row = get_token(conn, "RG")
    assert row["state"] == "DEAD" and row["rugged"] == 1
    stop.set()
    await asyncio.wait_for(task, 5)


def test_lifecycle_fixtures_use_terminal_reputation_writer():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    legacy_imports = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "memebot.store"
        and any(alias.name == "mark_rugged" for alias in node.names)
    ]
    legacy_names = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "mark_rugged"
    ]
    legacy_attributes = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "mark_rugged"
    ]
    target_functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "test_rugged_dead_not_resurrected_by_graduation"
    ]
    terminal_calls = [
        node.lineno
        for target in target_functions
        for node in ast.walk(target)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "set_terminal_state_with_reputation"
    ]

    assert legacy_imports == []
    assert legacy_names == []
    assert legacy_attributes == []
    assert len(target_functions) == 1
    assert terminal_calls


async def test_rugged_dead_not_resurrected_by_graduation(tmp_path):
    from memebot.store import (
        get_token,
        set_terminal_state_with_reputation,
        upsert_token,
    )
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="RG2", created_at=1.0)
    set_terminal_state_with_reputation(
        conn,
        mint="RG2",
        outcome="RUGGED",
        raw_processed_at=2.0,
        creator="",
        creator_conflicted=False,
    )                                               # DEAD + rugged
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 5000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(TokenGraduated(t_wall=5000.0, t_mono=1.0, mint="RG2",
                                     pool="", dex="pump-amm", raw={}))
    await asyncio.sleep(0.1)
    assert transitions.empty()                     # rugged stays dead (M3 delta 7)
    assert get_token(conn, "RG2")["state"] == "DEAD"
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_abandoned_dead_still_resurrects_by_graduation(tmp_path):
    # regression: the M2 C10 resurrection must still work for NON-rugged DEAD tokens.
    from memebot.store import set_token_state, upsert_token
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="AB", created_at=1.0)
    set_token_state(conn, "AB", "DEAD")            # abandoned-DEAD, rugged stays 0
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 5000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(TokenGraduated(t_wall=5000.0, t_mono=1.0, mint="AB",
                                     pool="", dex="pump-amm", raw={}))
    t = await asyncio.wait_for(transitions.get(), 5)
    assert (t.mint, t.to_state) == ("AB", "GRADUATED")   # non-rugged DEAD still resurrects
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_token_created_persists_creator(tmp_path):
    from memebot.store import get_token
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 5000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(TokenCreated(t_wall=5000.0, t_mono=1.0, mint="C1", name="", symbol="",
                                   creator="DEVX", raw={"bondingCurveKey": "B"}))
    await asyncio.sleep(0.1)
    import json
    meta = json.loads(get_token(conn, "C1")["meta_json"])
    assert meta.get("creator") == "DEVX"           # creator persisted for funding-graph (D10)
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_token_identity_insert_allocates_atomic_p3_ingestion_time(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    bus = EventBus()
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 50.0,
    )

    await tracker._handle(TokenCreated(
        t_wall=5000.0,
        t_mono=1.0,
        mint="P3IDENTITY",
        name="Meme Name",
        symbol="MEME",
        creator="Creator",
        raw={
            "bondingCurveKey": "Curve",
            "uri": "ipfs://metadata",
            "website": "https://example.com",
            "twitter": "@meme",
            "telegram": "https://t.me/meme",
        },
    ))

    row = get_token(conn, "P3IDENTITY")
    identity_t = math.nextafter(100.0, math.inf)
    assert row["p3_identity_ingested_at"] == identity_t
    assert row["p3_identity_ingested_at"] != row["created_at"]
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == identity_t
    assert json.loads(row["meta_json"]) == {
        "creator": "Creator",
        "name": "Meme Name",
        "symbol": "MEME",
        "uri": "ipfs://metadata",
        "website": "https://example.com",
        "twitter": "@meme",
        "telegram": "https://t.me/meme",
        "identity_observed_at": {
            "creator": identity_t,
            "name": identity_t,
            "symbol": identity_t,
            "uri": identity_t,
            "website": identity_t,
            "twitter": identity_t,
            "telegram": identity_t,
        },
        "identity_conflicts": [],
        "identity_conflict_observed_at": {},
    }

    clock_before_failed_insert = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    conn.execute(
        "CREATE TRIGGER reject_p3_identity_insert "
        "BEFORE INSERT ON tokens WHEN NEW.mint='REJECTED' BEGIN "
        "SELECT RAISE(ABORT, 'forced token persistence failure'); END"
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="forced token persistence failure"):
        await tracker._handle(TokenCreated(
            t_wall=5500.0,
            t_mono=1.5,
            mint="REJECTED",
            name="Rejected",
            symbol="NOPE",
            creator="Creator",
            raw={},
        ))
    assert get_token(conn, "REJECTED") is None
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before_failed_insert

    invalid = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: float("nan"),
    )
    with pytest.raises(ValueError, match="invalid p3 causal wall"):
        await invalid._handle(TokenCreated(
            t_wall=6000.0,
            t_mono=2.0,
            mint="INVALID",
            name="Invalid",
            symbol="BAD",
            creator="Creator",
            raw={},
        ))
    assert get_token(conn, "INVALID") is None


async def test_regressed_wall_preserves_durable_first_mover_order(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    bus = EventBus()
    raw_walls = iter((200.0, 50.0))
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: next(raw_walls),
    )
    first_event = TokenCreated(
        t_wall=9000.0,
        t_mono=1.0,
        mint="FIRST",
        name="Shared Name",
        symbol="SHARED",
        creator="Creator One",
        raw={},
    )
    second_event = TokenCreated(
        t_wall=1.0,
        t_mono=2.0,
        mint="SECOND",
        name="Shared Name",
        symbol="SHARED",
        creator="Creator Two",
        raw={},
    )

    await tracker._handle(first_event)
    await tracker._handle(second_event)

    first_t = get_token(conn, first_event.mint)["p3_identity_ingested_at"]
    second_t = get_token(conn, second_event.mint)["p3_identity_ingested_at"]
    assert second_event.t_wall < first_event.t_wall
    assert first_t == 200.0
    assert second_t == math.nextafter(first_t, math.inf)
    assert sorted(
        ((first_t, first_event.mint), (second_t, second_event.mint)),
    ) == [(first_t, first_event.mint), (second_t, second_event.mint)]
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == second_t


async def test_identity_exact_duplicate_preserves_fields_and_times(tmp_path):
    from memebot.store import allocate_p3_causal_wall, upsert_token_identity

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    bus = EventBus()
    raw_walls = iter((200.0, 50.0))
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: next(raw_walls),
    )
    event = TokenCreated(
        t_wall=9000.0,
        t_mono=1.0,
        mint="EXACT_DUPLICATE",
        name="Meme Name",
        symbol="MEME",
        creator="Creator",
        raw={
            "bondingCurveKey": "First Curve",
            "uri": "ipfs://metadata",
            "website": "https://example.com",
            "twitter": "@meme",
            "telegram": "https://t.me/meme",
        },
    )

    await tracker._handle(event)
    first = get_token(conn, event.mint)
    first_metadata = json.loads(first["meta_json"])
    first_identity_t = first["p3_identity_ingested_at"]
    conn.execute("BEGIN IMMEDIATE")
    conflict_t = allocate_p3_causal_wall(conn, raw_wall=50.0)
    first_metadata["identity_conflicts"] = ["website"]
    first_metadata["identity_conflict_observed_at"] = {"website": conflict_t}
    first_meta_json = json.dumps(
        first_metadata, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    conn.execute(
        "UPDATE tokens SET meta_json=? WHERE mint=?", (first_meta_json, event.mint),
    )
    conn.commit()
    first_clock = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    assert first_identity_t < conflict_t
    assert first_clock == conflict_t

    await tracker._handle(event)
    duplicate = get_token(conn, event.mint)
    assert duplicate["p3_identity_ingested_at"] == first_identity_t == 200.0
    assert duplicate["meta_json"] == first_meta_json
    assert first_metadata == {
        "creator": "Creator",
        "name": "Meme Name",
        "symbol": "MEME",
        "uri": "ipfs://metadata",
        "website": "https://example.com",
        "twitter": "@meme",
        "telegram": "https://t.me/meme",
        "identity_observed_at": {
            field: first_identity_t
            for field in (
                "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
            )
        },
        "identity_conflicts": ["website"],
        "identity_conflict_observed_at": {"website": conflict_t},
    }
    assert duplicate["bonding_curve_key"] == "First Curve"
    assert duplicate["last_seen"] == 50.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == first_clock

    upsert_token_identity(
        conn,
        mint=event.mint,
        raw_ingested_at=25.0,
        bonding_curve_key="Later Curve",
        fields={
            "creator": event.creator,
            "name": event.name,
            "symbol": event.symbol,
            "uri": event.raw["uri"],
            "website": event.raw["website"],
            "twitter": event.raw["twitter"],
            "telegram": event.raw["telegram"],
        },
    )
    helper_duplicate = get_token(conn, event.mint)
    assert helper_duplicate["p3_identity_ingested_at"] == first_identity_t
    assert helper_duplicate["meta_json"] == first_meta_json
    assert helper_duplicate["bonding_curve_key"] == "First Curve"
    assert helper_duplicate["last_seen"] == 25.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == first_clock


async def test_identity_empty_field_fill_allocates_processing_time(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    bus = EventBus()
    raw_walls = iter((200.0, 50.0))
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: next(raw_walls),
    )
    first_event = TokenCreated(
        t_wall=9000.0,
        t_mono=1.0,
        mint="EMPTY_FIELD_FILL",
        name="Meme Name",
        symbol="MEME",
        creator="Creator",
        raw={
            "bondingCurveKey": "First Curve",
            "twitter": "@meme",
        },
    )

    await tracker._handle(first_event)
    first = get_token(conn, first_event.mint)
    first_identity_t = first["p3_identity_ingested_at"]
    first_metadata = json.loads(first["meta_json"])

    await tracker._handle(TokenCreated(
        t_wall=1.0,
        t_mono=2.0,
        mint=first_event.mint,
        name=first_event.name,
        symbol=first_event.symbol,
        creator=first_event.creator,
        raw={
            "bondingCurveKey": "Later Curve",
            "uri": "ipfs://metadata",
            "website": "https://example.com",
            "twitter": first_event.raw["twitter"],
            "telegram": "not a valid handle!",
        },
    ))

    filled = get_token(conn, first_event.mint)
    filled_metadata = json.loads(filled["meta_json"])
    fill_t = math.nextafter(first_identity_t, math.inf)
    assert first_identity_t == 200.0
    assert fill_t > first_identity_t > 50.0
    assert filled["p3_identity_ingested_at"] == first_identity_t
    assert filled["bonding_curve_key"] == "First Curve"
    assert filled["last_seen"] == 50.0
    assert filled_metadata == {
        **first_metadata,
        "uri": "ipfs://metadata",
        "website": "https://example.com",
        "identity_observed_at": {
            **first_metadata["identity_observed_at"],
            "uri": fill_t,
            "website": fill_t,
        },
    }
    assert filled_metadata["identity_observed_at"]["creator"] == first_identity_t
    assert filled_metadata["identity_observed_at"]["name"] == first_identity_t
    assert filled_metadata["identity_observed_at"]["symbol"] == first_identity_t
    assert filled_metadata["identity_observed_at"]["twitter"] == first_identity_t
    assert filled_metadata["identity_conflicts"] == []
    assert filled_metadata["identity_conflict_observed_at"] == {}
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == fill_t

    valid_meta_json = filled["meta_json"]
    valid_last_seen = filled["last_seen"]
    identity_fields = {
        field: filled_metadata[field]
        for field in (
            "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
        )
    }
    corrupt_metadata_cases = (
        {
            **filled_metadata,
            "identity_conflicts": ["telegram"],
            "identity_conflict_observed_at": {"telegram": fill_t},
        },
        {
            **filled_metadata,
            "identity_conflicts": ["website"],
            "identity_conflict_observed_at": {"website": fill_t},
        },
    )
    for corrupt_metadata in corrupt_metadata_cases:
        corrupt_meta_json = json.dumps(
            corrupt_metadata, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        conn.execute(
            "UPDATE tokens SET meta_json=?,last_seen=? WHERE mint=?",
            (corrupt_meta_json, valid_last_seen, first_event.mint),
        )
        conn.commit()
        with pytest.raises(ValueError, match="invalid p3 token metadata"):
            upsert_token_identity(
                conn,
                mint=first_event.mint,
                raw_ingested_at=25.0,
                bonding_curve_key="Later Curve",
                fields=identity_fields,
            )
        corrupt = get_token(conn, first_event.mint)
        assert corrupt["meta_json"] == corrupt_meta_json
        assert corrupt["last_seen"] == valid_last_seen
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == fill_t
        assert not conn.in_transaction
        conn.execute(
            "UPDATE tokens SET meta_json=? WHERE mint=?",
            (valid_meta_json, first_event.mint),
        )
        conn.commit()


async def test_identity_conflict_allocates_processing_time(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    bus = EventBus()
    raw_walls = iter((200.0, 50.0, 25.0, 10.0))
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: next(raw_walls),
    )
    mint = "IDENTITY_CONFLICT"

    await tracker._handle(TokenCreated(
        t_wall=9000.0,
        t_mono=1.0,
        mint=mint,
        name="Meme Name",
        symbol="MEME",
        creator="Creator",
        raw={
            "bondingCurveKey": "First Curve",
            "uri": "https://EXAMPLE.com/Meta#first",
            "website": "https://Example.com/",
            "twitter": "@Meme",
        },
    ))
    first = get_token(conn, mint)
    first_identity_t = first["p3_identity_ingested_at"]
    first_metadata = json.loads(first["meta_json"])

    await tracker._handle(TokenCreated(
        t_wall=1.0,
        t_mono=2.0,
        mint=mint,
        name="Other Name",
        symbol="$MEME",
        creator=" Creator ",
        raw={
            "bondingCurveKey": "Later Curve",
            "uri": "https://example.com/Other",
            "website": "https://example.com",
            "twitter": "https://x.com/MEME",
            "telegram": "https://t.me/NewHandle",
        },
    ))
    conflicted = get_token(conn, mint)
    conflicted_metadata = json.loads(conflicted["meta_json"])
    conflict_t = math.nextafter(first_identity_t, math.inf)
    assert first_identity_t == 200.0
    assert conflict_t > first_identity_t > 50.0
    assert conflicted["p3_identity_ingested_at"] == first_identity_t
    assert conflicted["bonding_curve_key"] == "First Curve"
    assert conflicted["last_seen"] == 50.0
    assert conflicted_metadata == {
        **first_metadata,
        "telegram": "https://t.me/NewHandle",
        "identity_observed_at": {
            **first_metadata["identity_observed_at"],
            "telegram": conflict_t,
        },
        "identity_conflicts": ["name", "uri"],
        "identity_conflict_observed_at": {
            "name": conflict_t,
            "uri": conflict_t,
        },
    }
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == conflict_t

    await tracker._handle(TokenCreated(
        t_wall=2.0,
        t_mono=3.0,
        mint=mint,
        name="Third Name",
        symbol="OTHER",
        creator="Other Creator",
        raw={
            "uri": "https://",
            "website": "not a valid website",
            "twitter": "not a valid handle!",
            "telegram": "newhandle",
        },
    ))
    expanded = get_token(conn, mint)
    expanded_metadata = json.loads(expanded["meta_json"])
    second_conflict_t = math.nextafter(conflict_t, math.inf)
    assert expanded_metadata == {
        **conflicted_metadata,
        "identity_conflicts": ["creator", "name", "symbol", "uri"],
        "identity_conflict_observed_at": {
            **conflicted_metadata["identity_conflict_observed_at"],
            "creator": second_conflict_t,
            "symbol": second_conflict_t,
        },
    }
    assert expanded["p3_identity_ingested_at"] == first_identity_t
    assert expanded["bonding_curve_key"] == "First Curve"
    assert expanded["last_seen"] == 25.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == second_conflict_t

    await tracker._handle(TokenCreated(
        t_wall=3.0,
        t_mono=4.0,
        mint=mint,
        name="Fourth Name",
        symbol="AGAIN",
        creator="Fourth Creator",
        raw={
            "uri": "https://example.com/Fourth",
            "website": "https://EXAMPLE.com:443/",
            "twitter": "twitter.com/meme",
            "telegram": "@NEWHANDLE",
        },
    ))
    duplicate = get_token(conn, mint)
    assert duplicate["meta_json"] == expanded["meta_json"]
    assert duplicate["p3_identity_ingested_at"] == first_identity_t
    assert duplicate["bonding_curve_key"] == "First Curve"
    assert duplicate["last_seen"] == 10.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == second_conflict_t


async def test_missed_birth_adoption_uses_p3_identity_allocator(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token(conn, mint="LEGACY_BIRTH", created_at=2.0)
    upsert_token(conn, mint="LEGACY", created_at=1.0)
    upsert_token(
        conn,
        mint="LEGACY_GRADUATION",
        created_at=3.0,
        bonding_curve_key="Legacy Curve",
    )
    set_token_state(conn, "LEGACY", "CLIMBING", progress_pct=12.0)
    conn.execute(
        "UPDATE tokens SET meta_json=? WHERE mint=?",
        (json.dumps({
            "creator": "Legacy Creator",
            "name": "Legacy Name",
            "symbol": " ",
            "website": "https://legacy.example",
            "legacy_only": "discarded",
        }), "LEGACY"),
    )
    conn.execute(
        "UPDATE tokens SET meta_json=? WHERE mint=?",
        (json.dumps({
            "creator": "Birth Creator",
            "name": "Birth Name",
            "symbol": "BIRTH",
            "uri": "ipfs://birth-metadata",
            "website": " ",
            "twitter": "@birth",
            "telegram": "",
        }), "LEGACY_GRADUATION"),
    )
    invalid_legacy_metadata = (
        ("MALFORMED", "{broken"),
        ("LEGACY_LIST", "[]"),
        ("LEGACY_NULL", "null"),
        ("LEGACY_BLOB", sqlite3.Binary(b'{"creator":"Blob Creator"}')),
        ("LEGACY_DUPLICATE", '{"creator":"First","creator":"Second"}'),
        ("LEGACY_NAN", '{"legacy_only":NaN}'),
        ("LEGACY_INFINITY", '{"legacy_only":Infinity}'),
        ("LEGACY_NEG_INFINITY", '{"legacy_only":-Infinity}'),
        ("LEGACY_OVERFLOW", '{"legacy_only":{"nested":[1e999]}}'),
    )
    for mint, meta_json in invalid_legacy_metadata:
        upsert_token(conn, mint=mint, created_at=1.0)
        set_token_state(conn, mint, "CLIMBING", progress_pct=12.0)
        conn.execute(
            "UPDATE tokens SET meta_json=? WHERE mint=?", (meta_json, mint),
        )
    conn.commit()
    bus = EventBus()
    raw_walls = iter((
        50.0,
        125.0,
        *(150.0 + offset for offset in range(len(invalid_legacy_metadata))),
        200000.0,
        200001.0,
    ))
    clock_calls = 0

    def clock():
        nonlocal clock_calls
        clock_calls += 1
        return next(raw_walls)

    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=clock,
    )

    await tracker._handle(TokenGraduated(
        t_wall=9000.0,
        t_mono=1.0,
        mint="MISSED",
        pool="",
        dex="pump-amm",
        raw={
            "bondingCurveKey": "Curve",
            "traderPublicKey": "Creator",
            "name": "Meme Name",
            "symbol": "MEME",
            "uri": "ipfs://metadata",
            "website": "https://example.com",
            "twitter": "@meme",
            "telegram": "https://t.me/meme",
        },
    ))
    first = get_token(conn, "MISSED")
    first_identity_t = math.nextafter(100.0, math.inf)
    assert first["p3_identity_ingested_at"] == first_identity_t
    assert first["p3_identity_ingested_at"] != 9000.0
    assert first["state"] == "GRADUATED"
    first_meta = json.loads(first["meta_json"])
    assert first_meta["creator"] == "Creator"
    assert first_meta["name"] == "Meme Name"
    assert first["bonding_curve_key"] == "Curve"

    legacy_birth_before = get_token(conn, "LEGACY_BIRTH")
    assert legacy_birth_before["p3_identity_ingested_at"] is None
    assert legacy_birth_before["last_seen"] == 2.0
    await tracker._handle(TokenCreated(
        t_wall=9500.0,
        t_mono=1.25,
        mint="LEGACY_BIRTH",
        name="Legacy Birth",
        symbol="BIRTH",
        creator="Birth Creator",
        raw={},
    ))
    legacy_birth = get_token(conn, "LEGACY_BIRTH")
    assert legacy_birth["p3_identity_ingested_at"] == 125.0
    assert legacy_birth["last_seen"] == 125.0

    transitions = bus.subscribe(LifecycleTransition)
    for mint, _ in invalid_legacy_metadata:
        token_before = dict(get_token(conn, mint))
        clock_before = conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0]
        with pytest.raises(ValueError, match="legacy token metadata"):
            await tracker._handle(CurveProgress(
                t_wall=7000.0,
                t_mono=1.5,
                mint=mint,
                progress_pct=12.0,
                virtual_sol_reserves=70_000_000_000,
                virtual_token_reserves=70_000_000_000_000,
                real_sol_reserves=20_000_000_000,
                real_token_reserves=400_000_000_000_000,
                source_boot_id=RUNTIME_BOOT_ID,
                source_seq=1,
            ))
        assert dict(get_token(conn, mint)) == token_before
        assert get_token(conn, mint)["p3_identity_ingested_at"] is None
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == clock_before
        assert transitions.empty()

    await tracker._handle(CurveProgress(
        t_wall=8000.0,
        t_mono=2.0,
        mint="LEGACY",
        progress_pct=12.0,
        virtual_sol_reserves=70_000_000_000,
        virtual_token_reserves=70_000_000_000_000,
        real_sol_reserves=20_000_000_000,
        real_token_reserves=400_000_000_000_000,
        source_boot_id=RUNTIME_BOOT_ID,
        source_seq=1,
    ))
    refreshed = get_token(conn, "LEGACY")
    second_identity_t = 200000.0
    assert refreshed["p3_identity_ingested_at"] == second_identity_t
    assert refreshed["p3_identity_ingested_at"] != 8000.0
    assert refreshed["state"] == "DEAD"
    assert refreshed["curve_progress"] == 12.0
    assert json.loads(refreshed["meta_json"]) == {
        "creator": "Legacy Creator",
        "name": "Legacy Name",
        "symbol": "",
        "uri": "",
        "website": "https://legacy.example",
        "twitter": "",
        "telegram": "",
        "identity_observed_at": {
            "creator": second_identity_t,
            "name": second_identity_t,
            "website": second_identity_t,
        },
        "identity_conflicts": [],
        "identity_conflict_observed_at": {},
    }
    transition = transitions.get_nowait()
    assert (transition.mint, transition.from_state, transition.to_state) == (
        "LEGACY", "CLIMBING", "DEAD",
    )
    assert transitions.empty()

    await tracker._handle(TokenGraduated(
        t_wall=10000.0,
        t_mono=2.5,
        mint="LEGACY_GRADUATION",
        pool="",
        dex="pump-amm",
        raw={
            "bondingCurveKey": "Incoming Curve",
            "traderPublicKey": "Incoming Creator",
            "name": "Incoming Name",
            "symbol": "INCOMING",
            "uri": "ipfs://incoming-metadata",
            "website": "https://incoming.example",
            "twitter": "@incoming",
            "telegram": "https://t.me/incoming",
        },
    ))
    graduated = get_token(conn, "LEGACY_GRADUATION")
    third_identity_t = 200001.0
    assert graduated["p3_identity_ingested_at"] == third_identity_t
    assert graduated["state"] == "GRADUATED"
    assert graduated["bonding_curve_key"] == "Legacy Curve"
    assert json.loads(graduated["meta_json"]) == {
        "creator": "Birth Creator",
        "name": "Birth Name",
        "symbol": "BIRTH",
        "uri": "ipfs://birth-metadata",
        "website": "https://incoming.example",
        "twitter": "@birth",
        "telegram": "https://t.me/incoming",
        "identity_observed_at": {
            field: third_identity_t
            for field in (
                "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
            )
        },
        "identity_conflicts": [],
        "identity_conflict_observed_at": {},
    }
    transition = transitions.get_nowait()
    assert (transition.mint, transition.from_state, transition.to_state) == (
        "LEGACY_GRADUATION", "FRESH", "GRADUATED",
    )
    assert transitions.empty()
    assert clock_calls == len(invalid_legacy_metadata) + 4


async def test_abandoned_dead_then_hardfail_marks_rugged_no_transition(tmp_path):
    # D10b refinement: a hard-fail on an ALREADY-DEAD (abandoned, rugged=0) token must
    # still flip rugged=1 (funding-graph accuracy for later creator_rug_history checks),
    # but must NOT publish a spurious DEAD->DEAD LifecycleTransition (state didn't change).
    from memebot.events import SafetyHardFail
    from memebot.store import get_token, set_token_state, upsert_token
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="AB2", created_at=1.0, bonding_curve_key="B")
    set_token_state(conn, "AB2", "DEAD")           # abandoned-DEAD, rugged stays 0
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 5000.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(SafetyHardFail(t_wall=5000.0, t_mono=1.0, mint="AB2",
                                     reasons=("mint_authority_active",)))
    await asyncio.sleep(0.1)
    assert transitions.empty()                     # no spurious DEAD->DEAD transition
    row = get_token(conn, "AB2")
    assert row["state"] == "DEAD" and row["rugged"] == 1   # rugged flag now set
    stop.set()
    await asyncio.wait_for(task, 5)


def test_creator_reputation_first_terminal_evidence_is_atomic_and_append_only(
    tmp_path,
):
    from memebot.store import (
        TerminalReputationResult,
        reputation_creator_eligible,
        set_terminal_state_with_reputation,
    )

    assert reputation_creator_eligible("Creator", conflicted=False)
    assert reputation_creator_eligible("\tCreator\t", conflicted=False)
    assert reputation_creator_eligible("é" * 128, conflicted=False)
    assert not reputation_creator_eligible("Creator", conflicted=True)
    assert not reputation_creator_eligible("", conflicted=False)
    assert not reputation_creator_eligible(" Creator", conflicted=False)
    assert not reputation_creator_eligible("Creator ", conflicted=False)
    assert not reputation_creator_eligible("Creator\x00", conflicted=False)
    assert not reputation_creator_eligible("é" * 129, conflicted=False)
    assert not reputation_creator_eligible(True, conflicted=False)

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token(conn, mint="FIRST_GRADUATED", created_at=1.0)
    upsert_token(conn, mint="FIRST_RUGGED", created_at=2.0)
    upsert_token(conn, mint="ROLLBACK", created_at=3.0)

    graduated = set_terminal_state_with_reputation(
        conn,
        mint="FIRST_GRADUATED",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Creator One",
        creator_conflicted=False,
    )
    assert graduated == TerminalReputationResult(
        state="GRADUATED",
        rugged=False,
        processed_at=200.0,
        reputation_event_id=1,
        reputation_outcome="GRADUATED",
        event_inserted=True,
    )
    assert tuple(conn.execute(
        "SELECT creator,outcome,observed_at FROM creator_reputation_events "
        "WHERE id=?",
        (graduated.reputation_event_id,),
    ).fetchone()) == ("Creator One", "GRADUATED", 200.0)
    assert tuple(conn.execute(
        "SELECT creator,outcome,observed_at,event_id "
        "FROM creator_reputation_current WHERE mint='FIRST_GRADUATED'"
    ).fetchone()) == ("Creator One", "GRADUATED", 200.0, 1)
    assert get_token(conn, "FIRST_GRADUATED")["state"] == "GRADUATED"
    assert get_token(conn, "FIRST_GRADUATED")["rugged"] == 0

    rugged = set_terminal_state_with_reputation(
        conn,
        mint="FIRST_RUGGED",
        outcome="RUGGED",
        raw_processed_at=50.0,
        creator="Creator Two",
        creator_conflicted=False,
    )
    assert rugged == TerminalReputationResult(
        state="DEAD",
        rugged=True,
        processed_at=math.nextafter(200.0, math.inf),
        reputation_event_id=2,
        reputation_outcome="RUGGED",
        event_inserted=True,
    )
    assert tuple(conn.execute(
        "SELECT state,rugged FROM tokens WHERE mint='FIRST_RUGGED'"
    ).fetchone()) == ("DEAD", 1)

    with pytest.raises(sqlite3.IntegrityError, match="immutable evidence"):
        conn.execute(
            "UPDATE creator_reputation_events SET creator='Changed' WHERE id=1"
        )
    conn.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="immutable evidence"):
        conn.execute("DELETE FROM creator_reputation_events WHERE id=1")
    conn.rollback()

    clock_before = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    conn.execute(
        "CREATE TRIGGER reject_rollback_reputation "
        "BEFORE INSERT ON creator_reputation_events "
        "WHEN NEW.mint='ROLLBACK' BEGIN "
        "SELECT RAISE(ABORT,'forced reputation failure'); END"
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="forced reputation failure"):
        set_terminal_state_with_reputation(
            conn,
            mint="ROLLBACK",
            outcome="RUGGED",
            raw_processed_at=300.0,
            creator="Creator Three",
            creator_conflicted=False,
        )
    assert tuple(conn.execute(
        "SELECT state,rugged FROM tokens WHERE mint='ROLLBACK'"
    ).fetchone()) == ("FRESH", 0)
    assert conn.execute(
        "SELECT COUNT(*) FROM creator_reputation_events WHERE mint='ROLLBACK'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before
    assert not conn.in_transaction


def test_graduated_then_hardfail_appends_rug_and_sticks_dead(tmp_path):
    from memebot.store import (
        TerminalReputationResult,
        set_terminal_state_with_reputation,
    )

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token(conn, mint="GRADUATED_THEN_RUGGED", created_at=1.0)

    graduated = set_terminal_state_with_reputation(
        conn,
        mint="GRADUATED_THEN_RUGGED",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Stable Creator",
        creator_conflicted=False,
    )
    rugged = set_terminal_state_with_reputation(
        conn,
        mint="GRADUATED_THEN_RUGGED",
        outcome="RUGGED",
        raw_processed_at=300.0,
        creator=None,
        creator_conflicted=True,
    )

    assert rugged == TerminalReputationResult(
        state="DEAD",
        rugged=True,
        processed_at=300.0,
        reputation_event_id=2,
        reputation_outcome="RUGGED",
        event_inserted=True,
    )
    assert [tuple(row) for row in conn.execute(
        "SELECT id,creator,outcome,observed_at "
        "FROM creator_reputation_events WHERE mint=? ORDER BY id",
        ("GRADUATED_THEN_RUGGED",),
    )] == [
        (graduated.reputation_event_id, "Stable Creator", "GRADUATED", 200.0),
        (rugged.reputation_event_id, "Stable Creator", "RUGGED", 300.0),
    ]
    assert tuple(conn.execute(
        "SELECT creator,outcome,observed_at,event_id "
        "FROM creator_reputation_current WHERE mint=?",
        ("GRADUATED_THEN_RUGGED",),
    ).fetchone()) == ("Stable Creator", "RUGGED", 300.0, 2)
    assert tuple(conn.execute(
        "SELECT state,rugged FROM tokens WHERE mint=?",
        ("GRADUATED_THEN_RUGGED",),
    ).fetchone()) == ("DEAD", 1)


def test_regressed_terminal_wall_still_orders_rug_after_graduation(tmp_path):
    from memebot.store import (
        TerminalReputationResult,
        set_terminal_state_with_reputation,
    )

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    mint = "REGRESSED_TERMINAL_WALL"
    upsert_token(conn, mint=mint, created_at=1.0)

    graduated = set_terminal_state_with_reputation(
        conn,
        mint=mint,
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Stable Creator",
        creator_conflicted=False,
    )
    rugged = set_terminal_state_with_reputation(
        conn,
        mint=mint,
        outcome="RUGGED",
        raw_processed_at=50.0,
        creator=None,
        creator_conflicted=True,
    )

    rugged_at = math.nextafter(graduated.processed_at, math.inf)
    assert rugged == TerminalReputationResult(
        state="DEAD",
        rugged=True,
        processed_at=rugged_at,
        reputation_event_id=2,
        reputation_outcome="RUGGED",
        event_inserted=True,
    )
    assert [tuple(row) for row in conn.execute(
        "SELECT creator,outcome,observed_at "
        "FROM creator_reputation_events WHERE mint=? ORDER BY id",
        (mint,),
    )] == [
        ("Stable Creator", "GRADUATED", 200.0),
        ("Stable Creator", "RUGGED", rugged_at),
    ]
    assert tuple(conn.execute(
        "SELECT state,rugged FROM tokens WHERE mint=?", (mint,)
    ).fetchone()) == ("DEAD", 1)
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == rugged_at
    assert not conn.in_transaction


def test_creator_reputation_current_summary_supersedes_graduation(tmp_path):
    from memebot.store import set_terminal_state_with_reputation

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    mint = "CURRENT_SUMMARY_SUPERSEDES"
    creator = "Stable Creator"
    upsert_token(conn, mint=mint, created_at=1.0)

    graduated = set_terminal_state_with_reputation(
        conn,
        mint=mint,
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator=creator,
        creator_conflicted=False,
    )
    assert tuple(conn.execute(
        "SELECT creator,outcome,observed_at,event_id "
        "FROM creator_reputation_current WHERE mint=?",
        (mint,),
    ).fetchone()) == (creator, "GRADUATED", 200.0, graduated.reputation_event_id)

    rugged = set_terminal_state_with_reputation(
        conn,
        mint=mint,
        outcome="RUGGED",
        raw_processed_at=300.0,
        creator=None,
        creator_conflicted=True,
    )

    assert [tuple(row) for row in conn.execute(
        "SELECT creator,outcome,observed_at,event_id "
        "FROM creator_reputation_current WHERE mint=?",
        (mint,),
    )] == [(creator, "RUGGED", 300.0, rugged.reputation_event_id)]
    assert [tuple(row) for row in conn.execute(
        "SELECT outcome,observed_at FROM creator_reputation_events "
        "WHERE mint=? ORDER BY id",
        (mint,),
    )] == [("GRADUATED", 200.0), ("RUGGED", 300.0)]


def test_duplicate_terminal_delivery_returns_original_reputation_row(tmp_path):
    from memebot.store import (
        TerminalReputationResult,
        set_terminal_state_with_reputation,
    )

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    for index, mint in enumerate(
        ("DUP_GRADUATED", "DUP_RUGGED", "STATE_GRADUATED", "STATE_RUGGED"),
        start=1,
    ):
        upsert_token(conn, mint=mint, created_at=float(index))

    graduated = set_terminal_state_with_reputation(
        conn,
        mint="DUP_GRADUATED",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Original Graduate Creator",
        creator_conflicted=False,
    )
    rugged = set_terminal_state_with_reputation(
        conn,
        mint="DUP_RUGGED",
        outcome="RUGGED",
        raw_processed_at=300.0,
        creator="Original Rug Creator",
        creator_conflicted=False,
    )

    conn.execute(
        "UPDATE tokens SET state='TRENDING',rugged=0 WHERE mint='DUP_GRADUATED'"
    )
    conn.execute(
        "UPDATE tokens SET state='ESTABLISHED',rugged=0 WHERE mint='DUP_RUGGED'"
    )
    conn.execute(
        "UPDATE tokens SET state='GRADUATED',rugged=0 WHERE mint='STATE_GRADUATED'"
    )
    conn.execute(
        "UPDATE tokens SET state='DEAD',rugged=1 WHERE mint='STATE_RUGGED'"
    )
    conn.commit()

    duplicate_graduated = set_terminal_state_with_reputation(
        conn,
        mint="DUP_GRADUATED",
        outcome="GRADUATED",
        raw_processed_at=400.0,
        creator="Changed Creator",
        creator_conflicted=True,
    )
    duplicate_rugged = set_terminal_state_with_reputation(
        conn,
        mint="DUP_RUGGED",
        outcome="RUGGED",
        raw_processed_at=500.0,
        creator=None,
        creator_conflicted=True,
    )
    state_only_graduated = set_terminal_state_with_reputation(
        conn,
        mint="STATE_GRADUATED",
        outcome="GRADUATED",
        raw_processed_at=600.0,
        creator="Would Backfill",
        creator_conflicted=False,
    )
    state_only_rugged = set_terminal_state_with_reputation(
        conn,
        mint="STATE_RUGGED",
        outcome="RUGGED",
        raw_processed_at=700.0,
        creator="Would Backfill",
        creator_conflicted=False,
    )

    assert duplicate_graduated == TerminalReputationResult(
        state="GRADUATED",
        rugged=False,
        processed_at=graduated.processed_at,
        reputation_event_id=graduated.reputation_event_id,
        reputation_outcome="GRADUATED",
        event_inserted=False,
    )
    assert duplicate_rugged == TerminalReputationResult(
        state="DEAD",
        rugged=True,
        processed_at=rugged.processed_at,
        reputation_event_id=rugged.reputation_event_id,
        reputation_outcome="RUGGED",
        event_inserted=False,
    )
    for result, state, is_rugged in (
        (state_only_graduated, "GRADUATED", False),
        (state_only_rugged, "DEAD", True),
    ):
        assert result == TerminalReputationResult(
            state=state,
            rugged=is_rugged,
            processed_at=None,
            reputation_event_id=None,
            reputation_outcome=None,
            event_inserted=False,
        )

    assert [tuple(row) for row in conn.execute(
        "SELECT mint,creator,outcome,observed_at "
        "FROM creator_reputation_events ORDER BY id"
    )] == [
        ("DUP_GRADUATED", "Original Graduate Creator", "GRADUATED", 200.0),
        ("DUP_RUGGED", "Original Rug Creator", "RUGGED", 300.0),
    ]
    assert [tuple(row) for row in conn.execute(
        "SELECT mint,state,rugged FROM tokens ORDER BY mint"
    )] == [
        ("DUP_GRADUATED", "GRADUATED", 0),
        ("DUP_RUGGED", "DEAD", 1),
        ("STATE_GRADUATED", "GRADUATED", 0),
        ("STATE_RUGGED", "DEAD", 1),
    ]
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 300.0
    assert not conn.in_transaction

    corrupt_evidence = open_db(
        tmp_path / "corrupt-evidence.db", migration_clock=lambda: 100.0
    )
    upsert_token(corrupt_evidence, mint="CORRUPT_SIBLING", created_at=1.0)
    set_terminal_state_with_reputation(
        corrupt_evidence,
        mint="CORRUPT_SIBLING",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Stable Creator",
        creator_conflicted=False,
    )
    corrupt_evidence.execute(
        "UPDATE tokens SET state='FRESH',rugged=0 WHERE mint='CORRUPT_SIBLING'"
    )
    corrupt_evidence.execute("PRAGMA ignore_check_constraints=ON")
    corrupt_evidence.execute(
        "INSERT INTO creator_reputation_events("
        "mint,creator,outcome,observed_at) VALUES(?,?,?,?)",
        ("CORRUPT_SIBLING", "Stable Creator", "BROKEN", 250.0),
    )
    corrupt_evidence.commit()
    corrupt_evidence.execute("PRAGMA ignore_check_constraints=OFF")
    corrupt_evidence_before = (
        tuple(corrupt_evidence.execute(
            "SELECT state,rugged FROM tokens WHERE mint='CORRUPT_SIBLING'"
        ).fetchone()),
        [tuple(row) for row in corrupt_evidence.execute(
            "SELECT id,creator,outcome,observed_at "
            "FROM creator_reputation_events WHERE mint='CORRUPT_SIBLING' ORDER BY id"
        )],
        tuple(corrupt_evidence.execute(
            "SELECT creator,outcome,observed_at,event_id "
            "FROM creator_reputation_current WHERE mint='CORRUPT_SIBLING'"
        ).fetchone()),
        corrupt_evidence.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0],
    )
    with pytest.raises(ValueError, match="invalid terminal reputation evidence"):
        set_terminal_state_with_reputation(
            corrupt_evidence,
            mint="CORRUPT_SIBLING",
            outcome="GRADUATED",
            raw_processed_at=900.0,
            creator="Ignored Creator",
            creator_conflicted=False,
        )
    assert (
        tuple(corrupt_evidence.execute(
            "SELECT state,rugged FROM tokens WHERE mint='CORRUPT_SIBLING'"
        ).fetchone()),
        [tuple(row) for row in corrupt_evidence.execute(
            "SELECT id,creator,outcome,observed_at "
            "FROM creator_reputation_events WHERE mint='CORRUPT_SIBLING' ORDER BY id"
        )],
        tuple(corrupt_evidence.execute(
            "SELECT creator,outcome,observed_at,event_id "
            "FROM creator_reputation_current WHERE mint='CORRUPT_SIBLING'"
        ).fetchone()),
        corrupt_evidence.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0],
    ) == corrupt_evidence_before
    assert not corrupt_evidence.in_transaction

    corrupt_state = open_db(
        tmp_path / "corrupt-state.db", migration_clock=lambda: 100.0
    )
    upsert_token(corrupt_state, mint="CORRUPT_STATE", created_at=1.0)
    set_terminal_state_with_reputation(
        corrupt_state,
        mint="CORRUPT_STATE",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Stable Creator",
        creator_conflicted=False,
    )
    corrupt_state.execute(
        "UPDATE tokens SET state='CORRUPT',rugged=0 WHERE mint='CORRUPT_STATE'"
    )
    corrupt_state.commit()
    corrupt_state_before = (
        tuple(corrupt_state.execute(
            "SELECT state,rugged FROM tokens WHERE mint='CORRUPT_STATE'"
        ).fetchone()),
        [tuple(row) for row in corrupt_state.execute(
            "SELECT id,creator,outcome,observed_at "
            "FROM creator_reputation_events WHERE mint='CORRUPT_STATE' ORDER BY id"
        )],
        corrupt_state.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0],
    )
    with pytest.raises(ValueError, match="invalid token state"):
        set_terminal_state_with_reputation(
            corrupt_state,
            mint="CORRUPT_STATE",
            outcome="GRADUATED",
            raw_processed_at=900.0,
            creator="Ignored Creator",
            creator_conflicted=False,
        )
    assert (
        tuple(corrupt_state.execute(
            "SELECT state,rugged FROM tokens WHERE mint='CORRUPT_STATE'"
        ).fetchone()),
        [tuple(row) for row in corrupt_state.execute(
            "SELECT id,creator,outcome,observed_at "
            "FROM creator_reputation_events WHERE mint='CORRUPT_STATE' ORDER BY id"
        )],
        corrupt_state.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0],
    ) == corrupt_state_before
    assert not corrupt_state.in_transaction


def test_invalid_reputation_creator_cannot_rollback_terminal_state(tmp_path):
    from memebot.store import (
        TerminalReputationResult,
        set_terminal_state_with_reputation,
    )

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    cases = (
        ("GRADUATED", "", False),
        ("RUGGED", "", False),
        ("GRADUATED", " ", False),
        ("RUGGED", " ", False),
        ("GRADUATED", " Creator", False),
        ("RUGGED", " Creator", False),
        ("GRADUATED", "Creator ", False),
        ("RUGGED", "Creator ", False),
        ("GRADUATED", "Creator\x00", False),
        ("RUGGED", "Creator\x00", False),
        ("GRADUATED", "é" * 129, False),
        ("RUGGED", "é" * 129, False),
        ("GRADUATED", "Creator", True),
        ("RUGGED", "Creator", True),
        ("GRADUATED", True, False),
        ("RUGGED", True, False),
        ("GRADUATED", [], False),
        ("RUGGED", [], False),
    )
    for index, _ in enumerate(cases):
        upsert_token(conn, mint=f"INVALID_{index}", created_at=float(index + 1))

    conn.execute(
        "CREATE TRIGGER reject_invalid_creator_reputation "
        "BEFORE INSERT ON creator_reputation_events BEGIN "
        "SELECT RAISE(ABORT,'invalid creator reached SQL'); END"
    )
    conn.commit()

    expected_clock = 100.0
    for index, (outcome, creator, creator_conflicted) in enumerate(cases):
        mint = f"INVALID_{index}"
        raw_processed_at = 50.0
        result = set_terminal_state_with_reputation(
            conn,
            mint=mint,
            outcome=outcome,
            raw_processed_at=raw_processed_at,
            creator=creator,
            creator_conflicted=creator_conflicted,
        )
        expected_clock = math.nextafter(expected_clock, math.inf)
        state = "GRADUATED" if outcome == "GRADUATED" else "DEAD"
        rugged = outcome == "RUGGED"
        assert result == TerminalReputationResult(
            state=state,
            rugged=rugged,
            processed_at=expected_clock,
            reputation_event_id=None,
            reputation_outcome=None,
            event_inserted=False,
        )
        assert tuple(conn.execute(
            "SELECT state,rugged FROM tokens WHERE mint=?", (mint,)
        ).fetchone()) == (state, int(rugged))
        assert conn.execute(
            "SELECT COUNT(*) FROM creator_reputation_events WHERE mint=?", (mint,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM creator_reputation_current WHERE mint=?", (mint,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == expected_clock
        assert not conn.in_transaction


def test_post_rug_graduation_rejected_before_duplicate_or_state_mutation(tmp_path):
    import memebot.store as store

    def snapshot(conn, mint):
        return (
            tuple(conn.execute(
                "SELECT state,rugged FROM tokens WHERE mint=?", (mint,)
            ).fetchone()),
            [tuple(row) for row in conn.execute(
                "SELECT id,creator,outcome,observed_at "
                "FROM creator_reputation_events WHERE mint=? ORDER BY id",
                (mint,),
            )],
            [tuple(row) for row in conn.execute(
                "SELECT mint,creator,outcome,observed_at,event_id "
                "FROM creator_reputation_current WHERE mint=?",
                (mint,),
            )],
            conn.execute(
                "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
            ).fetchone()[0],
            conn.total_changes,
        )

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    for mint in ("BOTH", "FLAG_ONLY", "ROW_ONLY"):
        upsert_token(conn, mint=mint, created_at=1.0)

    store.set_terminal_state_with_reputation(
        conn,
        mint="BOTH",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Stable Creator",
        creator_conflicted=False,
    )
    store.set_terminal_state_with_reputation(
        conn,
        mint="BOTH",
        outcome="RUGGED",
        raw_processed_at=300.0,
        creator="Ignored Creator",
        creator_conflicted=True,
    )
    conn.execute(
        "UPDATE tokens SET state='DEAD',rugged=1 WHERE mint='FLAG_ONLY'"
    )
    conn.commit()
    store.set_terminal_state_with_reputation(
        conn,
        mint="ROW_ONLY",
        outcome="RUGGED",
        raw_processed_at=400.0,
        creator="Row Creator",
        creator_conflicted=False,
    )
    conn.execute(
        "UPDATE tokens SET state='FRESH',rugged=0 WHERE mint='ROW_ONLY'"
    )
    conn.commit()

    for mint in ("BOTH", "FLAG_ONLY", "ROW_ONLY"):
        before = snapshot(conn, mint)
        with pytest.raises(Exception) as raised:
            store.set_terminal_state_with_reputation(
                conn,
                mint=mint,
                outcome="GRADUATED",
                raw_processed_at=900.0,
                creator="Replacement Creator",
                creator_conflicted=False,
            )
        assert raised.type.__name__ == "EvidenceIntegrityError"
        assert raised.type is store.EvidenceIntegrityError
        assert snapshot(conn, mint) == before
        assert not conn.in_transaction

    malformed = open_db(
        tmp_path / "malformed.db", migration_clock=lambda: 100.0
    )
    upsert_token(malformed, mint="MALFORMED", created_at=1.0)
    store.set_terminal_state_with_reputation(
        malformed,
        mint="MALFORMED",
        outcome="RUGGED",
        raw_processed_at=200.0,
        creator="Stable Creator",
        creator_conflicted=False,
    )
    malformed.execute("PRAGMA ignore_check_constraints=ON")
    malformed.execute(
        "INSERT INTO creator_reputation_events("
        "mint,creator,outcome,observed_at) VALUES(?,?,?,?)",
        ("MALFORMED", "Stable Creator", "BROKEN", 300.0),
    )
    malformed.commit()
    malformed.execute("PRAGMA ignore_check_constraints=OFF")
    malformed_before = snapshot(malformed, "MALFORMED")

    with pytest.raises(
        ValueError, match="invalid terminal reputation evidence"
    ) as malformed_error:
        store.set_terminal_state_with_reputation(
            malformed,
            mint="MALFORMED",
            outcome="GRADUATED",
            raw_processed_at=900.0,
            creator="Replacement Creator",
            creator_conflicted=False,
        )
    assert malformed_error.type.__name__ != "EvidenceIntegrityError"
    assert snapshot(malformed, "MALFORMED") == malformed_before
    assert not malformed.in_transaction


async def test_lifecycle_persistence_fail_once_retries_same_event_before_next(
    tmp_path, monkeypatch, caplog,
):
    import memebot.lifecycle as lifecycle
    from memebot.store import set_terminal_state_with_reputation

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    for mint, creator in (("HELD", "Held Creator"), ("NEXT", "Next Creator")):
        upsert_token_identity(
            conn,
            mint=mint,
            raw_ingested_at=110.0,
            bonding_curve_key="Curve",
            fields={"creator": creator, "name": "Name", "symbol": "SYM"},
        )

    conn.execute(
        "CREATE TRIGGER fail_held_terminal_persistence "
        "BEFORE INSERT ON creator_reputation_events "
        "WHEN NEW.mint='HELD' BEGIN "
        "SELECT RAISE(ABORT,'fail once after rollback'); END"
    )
    conn.commit()
    persist_calls = []

    def fail_first_persistence(*args, **kwargs):
        persist_calls.append(dict(kwargs))
        return set_terminal_state_with_reputation(*args, **kwargs)

    monkeypatch.setattr(
        lifecycle, "set_terminal_state_with_reputation", fail_first_persistence,
        raising=False,
    )
    sleep_started = asyncio.Event()
    release_retry = asyncio.Event()
    retry_delays = []

    async def retry_sleep(delay):
        retry_delays.append(delay)
        sleep_started.set()
        await release_retry.wait()

    raw_walls = iter((200.0, 300.0))
    clock_calls = []

    def clock():
        value = next(raw_walls)
        clock_calls.append(value)
        return value

    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=clock, retry_sleep=retry_sleep,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    caplog.set_level("ERROR", logger="memebot.lifecycle")

    await bus.publish(TokenGraduated(
        t_wall=9000.0, t_mono=1.0, mint="HELD", pool="", dex="pump-amm", raw={},
    ))
    await bus.publish(TokenGraduated(
        t_wall=9001.0, t_mono=2.0, mint="NEXT", pool="", dex="pump-amm", raw={},
    ))
    await asyncio.wait_for(sleep_started.wait(), 5)

    assert get_token(conn, "HELD")["state"] == "FRESH"
    assert get_token(conn, "NEXT")["state"] == "FRESH"
    assert transitions.empty()
    assert tracker._queue.qsize() == 1
    assert clock_calls == [200.0]
    assert retry_delays == [0.05]
    assert [record.extra_fields for record in caplog.records] == [
        {"mint": "HELD", "attempt": 1},
    ]

    conn.execute("DROP TRIGGER fail_held_terminal_persistence")
    conn.commit()
    release_retry.set()
    first = await asyncio.wait_for(transitions.get(), 5)
    second = await asyncio.wait_for(transitions.get(), 5)
    assert [(first.mint, first.from_state, first.to_state),
            (second.mint, second.from_state, second.to_state)] == [
        ("HELD", "FRESH", "GRADUATED"),
        ("NEXT", "FRESH", "GRADUATED"),
    ]
    assert transitions.empty()
    assert clock_calls == [200.0, 300.0]
    assert [
        (call["mint"], call["outcome"], call["raw_processed_at"])
        for call in persist_calls
    ] == [
        ("HELD", "GRADUATED", 200.0),
        ("HELD", "GRADUATED", 200.0),
        ("NEXT", "GRADUATED", 300.0),
    ]
    assert [tuple(row) for row in conn.execute(
        "SELECT mint,COUNT(*) FROM creator_reputation_events "
        "GROUP BY mint ORDER BY mint"
    )] == [("HELD", 1), ("NEXT", 1)]

    stop.set()
    await asyncio.wait_for(task, 5)

    malformed_mints = (
        "MALFORMED_METADATA",
        "BLOB_METADATA",
        "NONCANONICAL_METADATA",
    )
    canonical_metadata = {}
    for offset, mint in enumerate(malformed_mints):
        upsert_token_identity(
            conn,
            mint=mint,
            raw_ingested_at=400.0 + offset,
            bonding_curve_key="Curve",
            fields={
                "creator": "Original Creator", "name": "Name", "symbol": "SYM",
            },
        )
        canonical_metadata[mint] = get_token(conn, mint)["meta_json"]
    malformed_values = {
        "MALFORMED_METADATA": (
            '{"creator":"Injected Creator","identity_conflicts":[]}'
        ),
        "BLOB_METADATA": sqlite3.Binary(
            canonical_metadata["BLOB_METADATA"].encode()
        ),
        "NONCANONICAL_METADATA": (
            " " + canonical_metadata["NONCANONICAL_METADATA"]
        ),
    }
    for mint, value in malformed_values.items():
        conn.execute(
            "UPDATE tokens SET meta_json=? WHERE mint=?", (value, mint),
        )
    conn.commit()
    assert [tuple(row) for row in conn.execute(
        "SELECT mint,typeof(meta_json) FROM tokens "
        "WHERE mint IN (?,?,?) ORDER BY mint",
        malformed_mints,
    )] == [
        ("BLOB_METADATA", "blob"),
        ("MALFORMED_METADATA", "text"),
        ("NONCANONICAL_METADATA", "text"),
    ]
    malformed_retry_delays = []

    async def malformed_retry_sleep(delay):
        malformed_retry_delays.append(delay)

    for offset, mint in enumerate(malformed_mints):
        malformed_bus = EventBus()
        malformed_tracker = LifecycleTracker(
            malformed_bus,
            conn,
            cfg=CFG,
            runtime_boot_id=RUNTIME_BOOT_ID,
            runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
            clock=lambda offset=offset: 500.0 + offset,
            retry_sleep=malformed_retry_sleep,
        )
        malformed_task = asyncio.create_task(
            malformed_tracker.run(asyncio.Event())
        )
        await malformed_bus.publish(TokenGraduated(
            t_wall=9002.0 + offset,
            t_mono=3.0 + offset,
            mint=mint,
            pool="",
            dex="pump-amm",
            raw={},
        ))
        with pytest.raises(ValueError, match="invalid p3 token metadata"):
            await asyncio.wait_for(malformed_task, 5)
    assert malformed_retry_delays == []
    assert all(get_token(conn, mint)["state"] == "FRESH" for mint in malformed_mints)
    assert conn.execute(
        "SELECT COUNT(*) FROM creator_reputation_events WHERE mint IN (?,?,?)",
        malformed_mints,
    ).fetchone()[0] == 0

    programming_retry_delays = []

    async def programming_retry_sleep(delay):
        programming_retry_delays.append(delay)

    programming_bus = EventBus()
    programming_tracker = LifecycleTracker(
        programming_bus,
        conn,
        cfg=CFG,
        runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: 600.0,
        retry_sleep=programming_retry_sleep,
    )

    def programming_failure(event, raw_processed_at):
        raise RuntimeError("programming failure")

    monkeypatch.setattr(programming_tracker, "_persist", programming_failure)
    programming_task = asyncio.create_task(
        programming_tracker.run(asyncio.Event())
    )
    await programming_bus.publish(TokenCreated(
        t_wall=9003.0,
        t_mono=4.0,
        mint="PROGRAMMING_FAILURE",
        name="Name",
        symbol="SYM",
        creator="Creator",
        raw={},
    ))
    with pytest.raises(RuntimeError, match="programming failure"):
        await asyncio.wait_for(programming_task, 5)
    assert programming_retry_delays == []

    schedule_retry_delays = []

    async def schedule_retry_sleep(delay):
        schedule_retry_delays.append(delay)

    schedule_bus = EventBus()
    schedule_stop = asyncio.Event()
    schedule_walls = iter((700.0, 800.0))
    schedule_tracker = LifecycleTracker(
        schedule_bus,
        conn,
        cfg=CFG,
        runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: next(schedule_walls),
        retry_sleep=schedule_retry_sleep,
    )
    schedule_attempts = {"MULTI_FAILURE": 0, "RESET_FAILURE": 0}
    schedule_raw_walls = {"MULTI_FAILURE": [], "RESET_FAILURE": []}

    def scheduled_persistence(event, raw_processed_at):
        schedule_attempts[event.mint] += 1
        schedule_raw_walls[event.mint].append(raw_processed_at)
        failure_count = 9 if event.mint == "MULTI_FAILURE" else 1
        if schedule_attempts[event.mint] <= failure_count:
            raise sqlite3.OperationalError("controlled persistence failure")
        if event.mint == "RESET_FAILURE":
            schedule_stop.set()
        return None

    monkeypatch.setattr(schedule_tracker, "_persist", scheduled_persistence)
    schedule_task = asyncio.create_task(schedule_tracker.run(schedule_stop))
    for offset, mint in enumerate(("MULTI_FAILURE", "RESET_FAILURE")):
        await schedule_bus.publish(TokenCreated(
            t_wall=9010.0 + offset,
            t_mono=10.0 + offset,
            mint=mint,
            name="Name",
            symbol="SYM",
            creator="Creator",
            raw={},
        ))
    await asyncio.wait_for(schedule_task, 5)
    assert schedule_retry_delays == [
        0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0, 2.0,
        0.05,
    ]
    assert schedule_attempts == {"MULTI_FAILURE": 10, "RESET_FAILURE": 2}
    assert schedule_raw_walls == {
        "MULTI_FAILURE": [700.0] * 10,
        "RESET_FAILURE": [800.0] * 2,
    }
    assert [record.extra_fields for record in caplog.records] == [
        {"mint": "HELD", "attempt": 1},
        *(
            {"mint": "MULTI_FAILURE", "attempt": attempt}
            for attempt in range(1, 10)
        ),
        {"mint": "RESET_FAILURE", "attempt": 1},
    ]


async def test_lifecycle_persistent_failure_blocks_next_and_shutdown_until_release(
    tmp_path, monkeypatch,
):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    for mint in ("HELD", "NEXT"):
        upsert_token_identity(
            conn,
            mint=mint,
            raw_ingested_at=110.0,
            bonding_curve_key="Curve",
            fields={"creator": f"{mint} Creator", "name": "Name", "symbol": "SYM"},
        )

    persistence_released = asyncio.Event()
    retry_started = asyncio.Event()
    persist_calls = []

    def controlled_persistence(event, raw_processed_at):
        persist_calls.append((event.mint, raw_processed_at))
        if event.mint == "HELD" and not persistence_released.is_set():
            raise sqlite3.OperationalError("persistent controlled failure")
        return (raw_processed_at, event.mint, "FRESH", "GRADUATED")

    async def retry_sleep(delay):
        assert delay == 0.05
        retry_started.set()
        await persistence_released.wait()

    raw_walls = iter((200.0, 300.0))
    clock_calls = []

    def clock():
        value = next(raw_walls)
        clock_calls.append(value)
        return value

    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=clock, retry_sleep=retry_sleep,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    monkeypatch.setattr(tracker, "_persist", controlled_persistence)

    await bus.publish(TokenGraduated(
        t_wall=9000.0, t_mono=1.0, mint="HELD", pool="", dex="pump-amm", raw={},
    ))
    await bus.publish(TokenGraduated(
        t_wall=9001.0, t_mono=2.0, mint="NEXT", pool="", dex="pump-amm", raw={},
    ))
    await asyncio.wait_for(retry_started.wait(), 5)
    stop.set()
    await asyncio.sleep(0)

    assert not task.done()
    assert transitions.empty()
    assert tracker._queue.qsize() == 1
    assert persist_calls == [("HELD", 200.0)]
    assert clock_calls == [200.0]

    persistence_released.set()
    await asyncio.wait_for(task, 5)

    assert persist_calls == [
        ("HELD", 200.0),
        ("HELD", 200.0),
        ("NEXT", 300.0),
    ]
    assert clock_calls == [200.0, 300.0]
    assert [transitions.get_nowait().mint, transitions.get_nowait().mint] == [
        "HELD", "NEXT",
    ]
    assert transitions.empty()


async def test_late_curve_progress_after_graduation_is_idempotent_noop(
    tmp_path, monkeypatch,
):
    import memebot.lifecycle as lifecycle
    from memebot.store import set_terminal_state_with_reputation

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token_identity(
        conn,
        mint="GRADUATED",
        raw_ingested_at=110.0,
        bonding_curve_key="Curve",
        fields={"creator": "Creator", "name": "Name", "symbol": "SYM"},
    )
    set_terminal_state_with_reputation(
        conn,
        mint="GRADUATED",
        outcome="GRADUATED",
        raw_processed_at=200.0,
        creator="Creator",
        creator_conflicted=False,
    )

    def snapshot():
        return (
            tuple(conn.execute(
                "SELECT * FROM tokens WHERE mint='GRADUATED'"
            ).fetchone()),
            [tuple(row) for row in conn.execute(
                "SELECT * FROM creator_reputation_events ORDER BY id"
            )],
            [tuple(row) for row in conn.execute(
                "SELECT * FROM creator_reputation_current ORDER BY mint"
            )],
            conn.execute(
                "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
            ).fetchone()[0],
            conn.total_changes,
        )

    before = snapshot()
    writer_calls = []
    for name in (
        "upsert_token_identity",
        "set_terminal_state_with_reputation",
        "set_token_state",
    ):
        original = getattr(lifecycle, name)

        def record_call(*args, _name=name, _original=original, **kwargs):
            writer_calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(lifecycle, name, record_call)

    retry_delays = []

    async def retry_sleep(delay):
        retry_delays.append(delay)

    clock_calls = []

    def clock():
        clock_calls.append(300.0)
        return 300.0

    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=clock, retry_sleep=retry_sleep,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    await bus.publish(CurveProgress(
        t_wall=250.0,
        t_mono=1.0,
        mint="GRADUATED",
        progress_pct=100.0,
        virtual_sol_reserves=70_000_000_000,
        virtual_token_reserves=70_000_000_000_000,
        real_sol_reserves=20_000_000_000,
        real_token_reserves=400_000_000_000_000,
        source_boot_id=RUNTIME_BOOT_ID,
        source_seq=1,
    ))
    stop.set()
    await asyncio.wait_for(task, 5)

    assert clock_calls == [300.0]
    assert writer_calls == []
    assert retry_delays == []
    assert transitions.empty()
    assert snapshot() == before


def test_authoritative_terminal_writers_have_no_legacy_bypass():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src" / "memebot").rglob("*.py"))
    paths += sorted((root / "tests").rglob("*.py"))
    trees = {
        path.relative_to(root).as_posix(): ast.parse(path.read_text())
        for path in paths
    }

    legacy_name = "mark_" + "rugged"
    legacy_surfaces = []
    for relative_path, tree in trees.items():
        for node in tree.body:
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == legacy_name
            ):
                legacy_surfaces.append(
                    (relative_path, node.lineno, "definition")
                )
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)) and any(
                alias.name == legacy_name for alias in node.names
            ):
                legacy_surfaces.append((relative_path, node.lineno, "import"))
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Name) and target.id == "__all__"
                    for target in node.targets
                )
                and any(
                    isinstance(value, ast.Constant)
                    and value.value == legacy_name
                    for value in ast.walk(node.value)
                )
            ):
                legacy_surfaces.append((relative_path, node.lineno, "export"))
            if isinstance(node, ast.Call) and (
                (
                    isinstance(node.func, ast.Name)
                    and node.func.id == legacy_name
                )
                or (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == legacy_name
                )
            ):
                legacy_surfaces.append((relative_path, node.lineno, "caller"))
    assert legacy_surfaces == []

    store_tree = trees["src/memebot/store.py"]
    state_writers = [
        node
        for node in store_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "set_token_state"
    ]
    assert len(state_writers) == 1
    guard = state_writers[0].body[0]
    assert isinstance(guard, ast.If)
    assert isinstance(guard.test, ast.Compare)
    assert (
        isinstance(guard.test.left, ast.Name)
        and guard.test.left.id == "state"
        and len(guard.test.ops) == 1
        and isinstance(guard.test.ops[0], ast.Eq)
        and len(guard.test.comparators) == 1
        and isinstance(guard.test.comparators[0], ast.Constant)
        and guard.test.comparators[0].value == "GRADUATED"
    )
    assert guard.orelse == []
    assert len(guard.body) == 1
    rejection = guard.body[0]
    assert isinstance(rejection, ast.Raise)
    assert isinstance(rejection.exc, ast.Call)
    assert isinstance(rejection.exc.func, ast.Name)
    assert rejection.exc.func.id == "ValueError"

    def dotted_name(node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = dotted_name(node.value)
            return None if base is None else f"{base}.{node.attr}"
        return None

    def simple_import_aliases(statements):
        direct_names = set()
        module_names = set()

        class ImportVisitor(ast.NodeVisitor):
            def visit_FunctionDef(self, node):
                pass

            visit_AsyncFunctionDef = visit_FunctionDef
            visit_ClassDef = visit_FunctionDef

            def visit_ImportFrom(self, node):
                if node.module == "memebot.store":
                    direct_names.update(
                        alias.asname or alias.name
                        for alias in node.names
                        if alias.name == "set_token_state"
                    )
                if node.module == "memebot":
                    module_names.update(
                        alias.asname or alias.name
                        for alias in node.names
                        if alias.name == "store"
                    )

            def visit_Import(self, node):
                module_names.update(
                    alias.asname
                    for alias in node.names
                    if alias.name == "memebot.store" and alias.asname is not None
                )

        visitor = ImportVisitor()
        for statement in statements:
            visitor.visit(statement)
        return direct_names, module_names

    generic_graduation_calls = []
    for relative_path, tree in trees.items():
        direct_names, module_names = simple_import_aliases(tree.body)
        if relative_path == "src/memebot/store.py":
            direct_names.add("set_token_state")

        class CallVisitor(ast.NodeVisitor):
            def __init__(self):
                self.owner = "<module>"
                self.direct_names = direct_names
                self.module_names = module_names

            def visit_FunctionDef(self, node):
                previous = self.owner, self.direct_names, self.module_names
                self.owner = node.name
                local_direct, local_modules = simple_import_aliases(node.body)
                self.direct_names = self.direct_names | local_direct
                self.module_names = self.module_names | local_modules
                for statement in node.body:
                    self.visit(statement)
                self.owner, self.direct_names, self.module_names = previous

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_Call(self, node):
                target = dotted_name(node.func)
                is_generic = (
                    isinstance(node.func, ast.Name)
                    and node.func.id in self.direct_names
                ) or (
                    target == "memebot.store.set_token_state"
                    or (
                        isinstance(node.func, ast.Attribute)
                        and isinstance(node.func.value, ast.Name)
                        and node.func.value.id in self.module_names
                        and node.func.attr == "set_token_state"
                    )
                )
                state_arg = node.args[2] if len(node.args) >= 3 else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "state"
                    ),
                    None,
                )
                literal_graduation = (
                    isinstance(state_arg, ast.Constant)
                    and state_arg.value == "GRADUATED"
                )
                allowed_rejection_proof = (
                    relative_path == "tests/test_store.py"
                    and self.owner
                    == "test_set_token_state_rejects_reserved_graduation"
                )
                if is_generic and literal_graduation and not allowed_rejection_proof:
                    generic_graduation_calls.append(
                        (relative_path, node.lineno, self.owner)
                    )
                self.generic_visit(node)

        CallVisitor().visit(tree)
    assert generic_graduation_calls == []

    lifecycle_tree = trees["src/memebot/lifecycle.py"]
    lifecycle_imports = {
        (node.module, alias.name, alias.asname)
        for node in lifecycle_tree.body
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert (
        "memebot.store",
        "set_terminal_state_with_reputation",
        None,
    ) in lifecycle_imports
    assert ("memebot.events", "SafetyHardFail", None) in lifecycle_imports
    assert ("memebot.events", "TokenGraduated", None) in lifecycle_imports

    tracker_classes = [
        node
        for node in lifecycle_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "LifecycleTracker"
    ]
    assert len(tracker_classes) == 1
    persist_methods = [
        node
        for node in tracker_classes[0].body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_persist"
    ]
    assert len(persist_methods) == 1
    persist = persist_methods[0]
    terminal_branches = [
        node
        for node in persist.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Call)
        and isinstance(node.test.func, ast.Name)
        and node.test.func.id == "isinstance"
        and len(node.test.args) == 2
        and isinstance(node.test.args[0], ast.Name)
        and node.test.args[0].id == "event"
        and isinstance(node.test.args[1], ast.Tuple)
    ]
    assert len(terminal_branches) == 1
    terminal_branch = terminal_branches[0]
    event_types = terminal_branch.test.args[1].elts
    assert len(event_types) == 2
    assert {
        node.id for node in event_types if isinstance(node, ast.Name)
    } == {"SafetyHardFail", "TokenGraduated"}

    terminal_calls = [
        node
        for statement in terminal_branch.body
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "set_terminal_state_with_reputation"
    ]
    assert len(terminal_calls) == 1

    outcomes = [
        keyword.value
        for keyword in terminal_calls[0].keywords
        if keyword.arg == "outcome"
    ]
    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert isinstance(outcome, ast.IfExp)
    assert isinstance(outcome.test, ast.Call)
    assert isinstance(outcome.test.func, ast.Name)
    assert outcome.test.func.id == "isinstance"
    assert len(outcome.test.args) == 2
    assert isinstance(outcome.test.args[0], ast.Name)
    assert outcome.test.args[0].id == "event"
    assert isinstance(outcome.test.args[1], ast.Name)
    assert outcome.test.args[1].id == "SafetyHardFail"
    assert isinstance(outcome.body, ast.Constant)
    assert outcome.body.value == "RUGGED"
    assert isinstance(outcome.orelse, ast.Constant)
    assert outcome.orelse.value == "GRADUATED"


async def test_lifecycle_critical_subscription_unsubscribes_in_finally(
    tmp_path, monkeypatch,
):
    conn = open_db(tmp_path / "normal.db", migration_clock=lambda: 100.0)
    upsert_token_identity(
        conn,
        mint="NORMAL",
        raw_ingested_at=110.0,
        bonding_curve_key="Curve",
        fields={"creator": "Creator", "name": "Name", "symbol": "SYM"},
    )

    bus = EventBus(maxsize=1)
    raw_walls = iter((200.0, 300.0))
    tracker = LifecycleTracker(
        bus, conn, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: next(raw_walls),
    )
    subscription = next(
        item for item in bus._subs if item.queue is tracker._queue
    )
    assert subscription.critical is True

    transitions = bus.subscribe(LifecycleTransition)
    transitions.put_nowait(LifecycleTransition(
        t_wall=0.0,
        t_mono=0.0,
        mint="BLOCKER",
        from_state="FRESH",
        to_state="CLIMBING",
    ))
    transition_publish_started = asyncio.Event()
    order = []
    original_publish = bus.publish

    async def observed_publish(event):
        if isinstance(event, LifecycleTransition):
            order.append("publish_started")
            transition_publish_started.set()
        await original_publish(event)
        if isinstance(event, LifecycleTransition):
            order.append("publish_finished")

    monkeypatch.setattr(bus, "publish", observed_publish)
    original_critical_done = bus.critical_done

    def observed_critical_done(queue):
        order.append("critical_done")
        original_critical_done(queue)

    monkeypatch.setattr(bus, "critical_done", observed_critical_done)
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))

    await bus.publish(TokenGraduated(
        t_wall=150.0,
        t_mono=1.0,
        mint="NORMAL",
        pool="",
        dex="pump-amm",
        raw={},
    ))
    await asyncio.wait_for(transition_publish_started.wait(), 5)

    assert get_token(conn, "NORMAL")["state"] == "GRADUATED"
    assert bus.critical_state() == (1, 1, False)
    assert order == ["publish_started"]

    blocker = transitions.get_nowait()
    assert blocker.mint == "BLOCKER"
    transitions.task_done()
    await asyncio.wait_for(tracker._queue.join(), 5)
    transition = await asyncio.wait_for(transitions.get(), 5)
    transitions.task_done()

    assert transition.mint == "NORMAL"
    assert order == ["publish_started", "publish_finished", "critical_done"]
    assert bus.critical_state() == (1, 0, False)

    await bus.publish(CurveProgress(
        t_wall=250.0,
        t_mono=2.0,
        mint="NORMAL",
        progress_pct=100.0,
        virtual_sol_reserves=70_000_000_000,
        virtual_token_reserves=70_000_000_000_000,
        real_sol_reserves=20_000_000_000,
        real_token_reserves=400_000_000_000_000,
        source_boot_id=RUNTIME_BOOT_ID,
        source_seq=1,
    ))
    await asyncio.wait_for(tracker._queue.join(), 5)

    assert order == [
        "publish_started", "publish_finished", "critical_done", "critical_done",
    ]
    assert bus.critical_state() == (2, 0, False)
    assert transitions.empty()

    stop.set()
    await asyncio.wait_for(task, 5)

    assert subscription.closed is True
    assert subscription.close_event.is_set() is True
    assert subscription not in bus._subs
    assert bus.critical_state() == (2, 0, False)

    failing_bus = EventBus()
    failing_tracker = LifecycleTracker(
        failing_bus, None, cfg=CFG, runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR, clock=lambda: 400.0,
    )
    failing_subscription = next(
        item for item in failing_bus._subs
        if item.queue is failing_tracker._queue
    )
    failing_acks = []
    original_failing_done = failing_bus.critical_done

    def observed_failing_done(queue):
        failing_acks.append(queue)
        original_failing_done(queue)

    monkeypatch.setattr(failing_bus, "critical_done", observed_failing_done)
    monkeypatch.setattr(
        failing_tracker,
        "_persist",
        lambda event, raw_processed_at: (
            raw_processed_at, event.mint, "FRESH", "GRADUATED",
        ),
    )

    async def fail_publication(transition):
        raise RuntimeError("controlled publication failure")

    monkeypatch.setattr(failing_tracker, "_publish_transition", fail_publication)
    failing_task = asyncio.create_task(
        failing_tracker.run(asyncio.Event())
    )
    await failing_bus.publish(TokenCreated(
        t_wall=350.0,
        t_mono=3.0,
        mint="PUBLISH_FAILURE",
        name="Name",
        symbol="SYM",
        creator="Creator",
        raw={},
    ))
    with pytest.raises(RuntimeError, match="controlled publication failure"):
        await asyncio.wait_for(failing_task, 5)

    assert failing_acks == []
    assert failing_tracker._queue.empty()
    assert failing_subscription.closed is True
    assert failing_subscription not in failing_bus._subs
    assert failing_bus.critical_state() == (1, 1, True)
    await asyncio.wait_for(failing_bus.wait_critical_idle_or_failed(), 1)

    held_bus = EventBus()
    retry_started = asyncio.Event()
    hold_retry = asyncio.Event()
    retry_delays = []

    async def held_retry_sleep(delay):
        retry_delays.append(delay)
        retry_started.set()
        await hold_retry.wait()

    held_tracker = LifecycleTracker(
        held_bus,
        None,
        cfg=CFG,
        runtime_boot_id=RUNTIME_BOOT_ID,
        runtime_causal_floor=RUNTIME_CAUSAL_FLOOR,
        clock=lambda: 500.0,
        retry_sleep=held_retry_sleep,
    )
    held_subscription = next(
        item for item in held_bus._subs if item.queue is held_tracker._queue
    )
    held_acks = []
    original_held_done = held_bus.critical_done

    def observed_held_done(queue):
        held_acks.append(queue)
        original_held_done(queue)

    monkeypatch.setattr(held_bus, "critical_done", observed_held_done)

    def fail_persistence(event, raw_processed_at):
        raise sqlite3.OperationalError("controlled persistence failure")

    monkeypatch.setattr(held_tracker, "_persist", fail_persistence)
    held_task = asyncio.create_task(held_tracker.run(asyncio.Event()))
    await held_bus.publish(TokenCreated(
        t_wall=450.0,
        t_mono=4.0,
        mint="HELD_FAILURE",
        name="Name",
        symbol="SYM",
        creator="Creator",
        raw={},
    ))
    await asyncio.wait_for(retry_started.wait(), 5)

    assert retry_delays == [0.05]
    assert held_acks == []
    assert held_tracker._queue.empty()
    assert held_bus.critical_state() == (1, 1, False)

    held_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(held_task, 5)

    assert held_acks == []
    assert held_subscription.closed is True
    assert held_subscription not in held_bus._subs
    assert held_bus.critical_state() == (1, 1, True)
    await asyncio.wait_for(held_bus.wait_critical_idle_or_failed(), 1)


async def test_curve_progress_identity_source_and_causal_times_are_atomic(
    tmp_path,
):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token_identity(
        conn,
        mint="PROGRESS",
        raw_ingested_at=110.0,
        bonding_curve_key="Curve",
        fields={"creator": "Creator", "name": "Name", "symbol": "SYM"},
    )
    bus = EventBus()
    transitions = bus.subscribe(LifecycleTransition)
    tracker = LifecycleTracker(
        bus,
        conn,
        cfg=CFG,
        runtime_boot_id=41,
        runtime_causal_floor=100.0,
        clock=lambda: 115.0,
    )

    await tracker._handle(CurveProgress(
        t_wall=120.0,
        t_mono=1.0,
        mint="PROGRESS",
        progress_pct=15.0,
        virtual_sol_reserves=70_000_000_000,
        virtual_token_reserves=70_000_000_000_000,
        real_sol_reserves=20_000_000_000,
        real_token_reserves=400_000_000_000_000,
        source_boot_id=41,
        source_seq=1,
    ))

    row = get_token(conn, "PROGRESS")
    observed_at = row["curve_progress_observed_at"]
    assert row["curve_progress"] == 15.0
    assert row["curve_progress_source_wall"] == 120.0
    assert row["curve_progress_source_boot_id"] == 41
    assert row["curve_progress_source_seq"] == 1
    reserve_columns = (
        "curve_progress_virtual_sol_reserves",
        "curve_progress_virtual_token_reserves",
        "curve_progress_real_sol_reserves",
        "curve_progress_real_token_reserves",
    )
    expected_reserves = (
        70_000_000_000,
        70_000_000_000_000,
        20_000_000_000,
        400_000_000_000_000,
    )
    assert tuple(row[column] for column in reserve_columns) == expected_reserves
    assert observed_at >= 120.0
    assert observed_at > 110.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == observed_at
    transition = transitions.get_nowait()
    assert (
        transition.mint,
        transition.from_state,
        transition.to_state,
        transition.t_wall,
    ) == ("PROGRESS", "FRESH", "CLIMBING", observed_at)

    before = dict(row)
    before_clock = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    conn.execute(
        """CREATE TRIGGER reject_second_progress
BEFORE UPDATE OF curve_progress_source_seq ON tokens
WHEN NEW.curve_progress_source_seq=2
BEGIN
  SELECT RAISE(ABORT,'controlled progress failure');
END"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="controlled progress failure"):
        await tracker._handle(CurveProgress(
            t_wall=121.0,
            t_mono=2.0,
            mint="PROGRESS",
            progress_pct=25.0,
            virtual_sol_reserves=71_000_000_000,
            virtual_token_reserves=69_000_000_000_000,
            real_sol_reserves=21_000_000_000,
            real_token_reserves=399_000_000_000_000,
            source_boot_id=41,
            source_seq=2,
        ))
    after = get_token(conn, "PROGRESS")
    assert tuple(after[column] for column in reserve_columns) == expected_reserves
    assert dict(after) == before
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == before_clock
    assert transitions.empty()


async def test_progress_write_advances_causal_clock_from_future_source(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token_identity(
        conn,
        mint="FUTURE_PROGRESS",
        raw_ingested_at=110.0,
        bonding_curve_key="Curve",
        fields={"creator": "Creator", "name": "Name", "symbol": "SYM"},
    )
    tracker = LifecycleTracker(
        EventBus(),
        conn,
        cfg=CFG,
        runtime_boot_id=42,
        runtime_causal_floor=100.0,
        clock=lambda: 150.0,
    )

    await tracker._handle(CurveProgress(
        t_wall=200.0,
        t_mono=1.0,
        mint="FUTURE_PROGRESS",
        progress_pct=15.0,
        virtual_sol_reserves=70_000_000_000,
        virtual_token_reserves=70_000_000_000_000,
        real_sol_reserves=20_000_000_000,
        real_token_reserves=400_000_000_000_000,
        source_boot_id=42,
        source_seq=1,
    ))

    row = get_token(conn, "FUTURE_PROGRESS")
    observed_at = row["curve_progress_observed_at"]
    assert row["curve_progress_source_wall"] == 200.0
    assert observed_at > 200.0
    assert observed_at > 150.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == observed_at


async def test_progress_duplicate_and_retrograde_sequence_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 100.0)
    upsert_token_identity(
        conn,
        mint="SEQUENCED_PROGRESS",
        raw_ingested_at=110.0,
        bonding_curve_key="Curve",
        fields={"creator": "Creator", "name": "Name", "symbol": "SYM"},
    )
    tracker = LifecycleTracker(
        EventBus(),
        conn,
        cfg=CFG,
        runtime_boot_id=51,
        runtime_causal_floor=100.0,
        clock=lambda: 120.0,
    )

    first_payload = {
        "t_wall": 121.0,
        "t_mono": 1.0,
        "mint": "SEQUENCED_PROGRESS",
        "progress_pct": 15.0,
        "virtual_sol_reserves": 70_000_000_000,
        "virtual_token_reserves": 70_000_000_000_000,
        "real_sol_reserves": 20_000_000_000,
        "real_token_reserves": 400_000_000_000_000,
        "source_boot_id": 51,
        "source_seq": 1,
    }
    first = CurveProgress(**first_payload)
    await tracker._handle(first)
    first_row = dict(get_token(conn, first.mint))
    first_clock = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]

    conflicting_duplicate = CurveProgress(
        **{
            **first_payload,
            "t_mono": 2.0,
            "progress_pct": 25.0,
        },
    )
    with pytest.raises(ValueError, match="curve progress sequence"):
        await tracker._handle(conflicting_duplicate)
    assert dict(get_token(conn, first.mint)) == first_row
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == first_clock

    await tracker._handle(first)
    assert dict(get_token(conn, first.mint)) == first_row
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == first_clock

    second = CurveProgress(
        **{
            **first_payload,
            "t_wall": 122.0,
            "t_mono": 3.0,
            "progress_pct": 25.0,
            "real_sol_reserves": 21_000_000_000,
            "real_token_reserves": 399_000_000_000_000,
            "source_seq": 2,
        },
    )
    await tracker._handle(second)
    second_row = dict(get_token(conn, first.mint))
    second_clock = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    assert second_row["curve_progress_source_boot_id"] == 51
    assert second_row["curve_progress_source_seq"] == 2
    assert second_row["curve_progress_source_wall"] == 122.0
    assert second_row["curve_progress"] == 25.0
    assert (
        second_row["curve_progress_virtual_sol_reserves"],
        second_row["curve_progress_virtual_token_reserves"],
        second_row["curve_progress_real_sol_reserves"],
        second_row["curve_progress_real_token_reserves"],
    ) == (
        70_000_000_000,
        70_000_000_000_000,
        21_000_000_000,
        399_000_000_000_000,
    )
    assert second_row["curve_progress_observed_at"] > first_clock
    assert second_row["curve_progress_observed_at"] > 122.0
    assert second_clock == second_row["curve_progress_observed_at"]

    with pytest.raises(ValueError, match="curve progress sequence"):
        await tracker._handle(first)
    with pytest.raises(ValueError, match="curve progress source boot"):
        await tracker._handle(CurveProgress(
            **{
                **first_payload,
                "t_wall": 122.0,
                "t_mono": 4.0,
                "progress_pct": 25.0,
                "real_sol_reserves": 21_000_000_000,
                "real_token_reserves": 399_000_000_000_000,
                "source_boot_id": 52,
                "source_seq": 3,
            },
        ))
    assert dict(get_token(conn, first.mint)) == second_row
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == second_clock
    restarted_tracker = LifecycleTracker(
        EventBus(),
        conn,
        cfg=CFG,
        runtime_boot_id=52,
        runtime_causal_floor=100.0,
        clock=lambda: 123.0,
    )
    restarted = CurveProgress(
        **{
            **first_payload,
            "t_wall": 124.0,
            "t_mono": 1.0,
            "progress_pct": 35.0,
            "virtual_sol_reserves": 72_000_000_000,
            "virtual_token_reserves": 68_000_000_000_000,
            "real_sol_reserves": 22_000_000_000,
            "real_token_reserves": 398_000_000_000_000,
            "source_boot_id": 52,
            "source_seq": 1,
        },
    )
    await restarted_tracker._handle(restarted)
    restarted_row = dict(get_token(conn, first.mint))
    restarted_clock = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    assert restarted_row["curve_progress_source_boot_id"] == 52
    assert restarted_row["curve_progress_source_seq"] == 1
    assert restarted_row["curve_progress_source_wall"] == 124.0
    assert restarted_row["curve_progress"] == 35.0
    assert (
        restarted_row["curve_progress_virtual_sol_reserves"],
        restarted_row["curve_progress_virtual_token_reserves"],
        restarted_row["curve_progress_real_sol_reserves"],
        restarted_row["curve_progress_real_token_reserves"],
    ) == (
        72_000_000_000,
        68_000_000_000_000,
        22_000_000_000,
        398_000_000_000_000,
    )
    assert restarted_row["curve_progress_observed_at"] > second_clock
    assert restarted_row["curve_progress_observed_at"] > 124.0
    assert restarted_clock == restarted_row["curve_progress_observed_at"]

    next_boot = LifecycleTracker(
        EventBus(),
        conn,
        cfg=CFG,
        runtime_boot_id=53,
        runtime_causal_floor=100.0,
        clock=lambda: 125.0,
    )
    next_event = CurveProgress(
        **{
            **first_payload,
            "t_wall": 126.0,
            "t_mono": 1.0,
            "progress_pct": 45.0,
            "virtual_sol_reserves": 73_000_000_000,
            "virtual_token_reserves": 67_000_000_000_000,
            "real_sol_reserves": 23_000_000_000,
            "real_token_reserves": 397_000_000_000_000,
            "source_boot_id": 53,
            "source_seq": 1,
        },
    )
    for malformed_boot, malformed_seq in (
        (None, 1),
        (52, None),
        ("bad-boot", 1),
        (52.5, 1),
        (0, 1),
        (52, "bad-sequence"),
        (52, 1.5),
        (52, 0),
    ):
        conn.execute(
            """UPDATE tokens
SET curve_progress_source_boot_id=?,curve_progress_source_seq=?
WHERE mint=?""",
            (malformed_boot, malformed_seq, first.mint),
        )
        conn.commit()
        malformed_row = dict(get_token(conn, first.mint))
        malformed_clock = conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0]
        with pytest.raises(
            ValueError,
            match="invalid persisted curve progress sequence",
        ):
            await next_boot._handle(next_event)
        assert dict(get_token(conn, first.mint)) == malformed_row
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == malformed_clock


def test_lifecycle_fixtures_supply_runtime_boot_and_floor():
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

    assert len(tracker_calls) == 31
    assert len(progress_calls) == 17
    for call in tracker_calls:
        keywords = {keyword.arg: keyword.value for keyword in call.keywords}
        assert "runtime_boot_id" in keywords, call.lineno
        assert "runtime_causal_floor" in keywords, call.lineno

    dict_bindings = {
        target.id: node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance((target := node.targets[0]), ast.Name)
        and isinstance(node.value, ast.Dict)
    }

    def resolve_mapping(node, resolving=frozenset()):
        if isinstance(node, ast.Name):
            assert node.id in dict_bindings, node.lineno
            assert node.id not in resolving, node.lineno
            return resolve_mapping(
                dict_bindings[node.id], resolving | {node.id},
            )
        assert isinstance(node, ast.Dict), node.lineno
        resolved = {}
        for key, value in zip(node.keys, node.values, strict=True):
            if key is None:
                resolved.update(resolve_mapping(value, resolving))
                continue
            assert isinstance(key, ast.Constant), key.lineno
            assert type(key.value) is str, key.lineno
            resolved[key.value] = value
        return resolved

    for call in progress_calls:
        keywords = {}
        for keyword in call.keywords:
            if keyword.arg is None:
                keywords.update(resolve_mapping(keyword.value))
            else:
                keywords[keyword.arg] = keyword.value
        assert "source_boot_id" in keywords, call.lineno
        assert "source_seq" in keywords, call.lineno
