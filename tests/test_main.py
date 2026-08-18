import asyncio
import json
import math
import socket
import sqlite3
from pathlib import Path

import httpx
import pytest

from memebot.main import run


@pytest.fixture(autouse=True)
def _isolate_runtime_config_validation(monkeypatch):
    """Keep legacy composition fixtures focused on their pre-existing concern."""
    from memebot import main as main_mod

    monkeypatch.setattr(main_mod, "validate_runtime_config", lambda _cfg: None)


def _closed_ephemeral_port() -> int:
    """Bind then immediately close a port: connecting to it afterwards fails
    fast with ECONNREFUSED (~10ms), unlike port 9 (discard) or port 1 which
    both hang until the websockets open_timeout (~10s) on this box — probed
    directly; see task report for the probe transcript."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

WATCH_ONLY_STRATEGY = """\
[strategy.climbing]
entries_enabled = false
"""

CONFIG = """
[storage]
data_dir = "{data}"
[log]
level = "INFO"
[ops]
heartbeat_interval_s = 1
[journal]
max_bytes = 1000000
retention_days = 30
disk_cap_bytes = 100000000
disk_alarm_fraction = 0.8
""" + WATCH_ONLY_STRATEGY

STRATEGY_RUNTIME_SECTIONS = """
[scorer.climbing]
[fill]
[counterfactual]
horizons_s = [10.0, 20.0]
stale_price_after_s = 300.0
price_history_retention_s = 400.0
price_history_max_samples_per_mint = 12
price_history_max_mints = 11
max_in_memory_pending_observations = 13
[exits.climbing]
[canonical]
max_feature_mints = 1000
reconcile_interval_s = 14.0
"""


async def test_runtime_validates_before_db_journal_clients_or_ready(
    tmp_path, monkeypatch,
):
    from memebot import main as main_mod
    from memebot.config import ConfigError, validate_runtime_config

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        Path("config.toml").read_text(encoding="utf-8").replace(
            "max_feature_mints = 1000",
            "max_feature_mints = 0",
        ),
        encoding="utf-8",
    )
    calls = []
    real_load_config = main_mod.load_config

    def load_config_spy(path):
        calls.append("load_config")
        return real_load_config(path)

    def validation_spy(cfg):
        calls.append("validate_runtime_config")
        validate_runtime_config(cfg)

    def forbidden_side_effect(name):
        def fail(*args, **kwargs):
            calls.append(name)
            raise AssertionError(
                f"startup side effect ran before runtime validation: {name}"
            )

        return fail

    monkeypatch.setattr(main_mod, "load_config", load_config_spy)
    monkeypatch.setattr(
        main_mod,
        "validate_runtime_config",
        validation_spy,
        raising=False,
    )
    monkeypatch.setattr(
        main_mod,
        "validate_watch_only_release",
        forbidden_side_effect("validate_watch_only_release"),
    )
    for name in (
        "setup_logging",
        "open_db",
        "record_boot",
        "Journal",
        "EventBus",
        "Heartbeat",
        "sd_notify",
    ):
        monkeypatch.setattr(main_mod, name, forbidden_side_effect(name))
    monkeypatch.setattr(
        type(tmp_path),
        "mkdir",
        forbidden_side_effect("data_dir.mkdir"),
    )
    monkeypatch.setattr(
        main_mod.httpx,
        "AsyncClient",
        forbidden_side_effect("httpx.AsyncClient"),
    )
    monkeypatch.setattr(
        main_mod.asyncio,
        "create_task",
        forbidden_side_effect("asyncio.create_task"),
    )

    with pytest.raises(
        ConfigError,
        match="canonical.max_feature_mints must be an integer from 1 to 10000",
    ):
        await run(cfg_path)

    assert calls == ["load_config", "validate_runtime_config"]


async def test_runtime_causal_floor_allocates_before_reconcile_tasks_and_ready(
    tmp_path, monkeypatch,
):
    from memebot import broker as broker_mod
    from memebot import counterfactual as counterfactual_mod
    from memebot import features as features_mod
    from memebot import main as main_mod
    from memebot import scoring as scoring_mod
    from memebot import strategy as strategy_mod
    from memebot.store import allocate_p3_causal_wall, open_db, upsert_token

    data_dir = tmp_path / "data"
    db_path = data_dir / "memebot.db"
    data_dir.mkdir()
    seeded = open_db(db_path)
    upsert_token(seeded, mint="PREBOOT", created_at=1.0)
    seeded.execute(
        """UPDATE tokens
SET curve_progress=50.0,curve_progress_observed_at=500.0,
    curve_progress_source_wall=499.0,curve_progress_source_boot_id=9,
    curve_progress_source_seq=1,
    curve_progress_virtual_sol_reserves=70000000000,
    curve_progress_virtual_token_reserves=70000000000000,
    curve_progress_real_sol_reserves=20000000000,
    curve_progress_real_token_reserves=400000000000000
WHERE mint='PREBOOT'"""
    )
    seeded.execute(
        "UPDATE p3_causal_clock SET last_wall=500.0 WHERE singleton=1"
    )
    seeded.commit()
    seeded.close()

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        CONFIG.format(data=data_dir) + STRATEGY_RUNTIME_SECTIONS + """
[providers.pumpportal]
ws_url = "ws://127.0.0.1:1"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_RUNTIME_FLOOR_RPC"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
token_decimals = 6
""",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "MEMEBOT_TEST_RUNTIME_FLOOR_RPC", "http://127.0.0.1:1"
    )

    order = []
    allocated = []
    live_conn = None
    real_open_db = main_mod.open_db
    real_create_task = asyncio.create_task

    def open_db_spy(path):
        nonlocal live_conn
        live_conn = real_open_db(path)
        return live_conn

    def audit(name):
        def run_audit(conn):
            assert conn is live_conn
            order.append(name)

        return run_audit

    def allocate_spy(conn, *, raw_wall):
        assert conn is live_conn
        assert conn.in_transaction is True
        order.append("allocate")
        floor = allocate_p3_causal_wall(conn, raw_wall=raw_wall)
        allocated.append((raw_wall, floor))
        return floor

    def assert_floor_committed(stage):
        assert allocated, f"runtime causal floor missing before {stage}"
        assert live_conn is not None and live_conn.in_transaction is False
        floor = allocated[0][1]
        assert floor > 500.0
        probe = sqlite3.connect(db_path)
        try:
            durable = probe.execute(
                "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
            ).fetchone()[0]
        finally:
            probe.close()
        assert durable == floor
        order.append(stage)

    def create_task_spy(coro, *args, **kwargs):
        try:
            assert_floor_committed("task")
        except BaseException:
            coro.close()
            raise
        return real_create_task(coro, *args, **kwargs)

    class IdleWorker:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, stop):
            await stop.wait()

    class FakeHeartbeat(IdleWorker):
        pass

    class RestoredPosition:
        decision_id = 1
        entry_latest_target_report_id = 1

    class HealthyLatestReport:
        safety_report_id = 1
        hard_fails = ()

    class FakeStrategy(IdleWorker):
        recovery_pending = False
        positions = {"M": RestoredPosition()}
        _restored_p3_latest_reports = {1: HealthyLatestReport()}

        def reconcile(self, *, runtime_causal_floor, max_open_positions):
            assert_floor_committed("reconcile")
            return (1,)

        def recover_pending_scores(self):
            return 0

    class FakeTracker(IdleWorker):
        def resume_from_ledger(self, conn):
            assert conn is live_conn
            assert_floor_committed("tracker_reconcile")
            return 0

        def replay_journal(self, *, since_wall, until_wall):
            assert since_wall == 0.0
            assert until_wall >= since_wall
            assert_floor_committed("tracker_replay")
            return 0

    class FakeClient:
        async def aclose(self):
            pass

    async def idle_supervise(name, run_adapter, bus, stop):
        await stop.wait()

    def ready_spy(message):
        if message == "READY=1":
            assert_floor_committed("ready")

    monkeypatch.setattr(main_mod, "open_db", open_db_spy)
    monkeypatch.setattr(
        main_mod,
        "assert_p3_buy_terminal_coverage",
        audit("terminal_audit"),
    )
    monkeypatch.setattr(
        main_mod,
        "assert_no_open_p3_positions",
        audit("open_position_audit"),
    )
    monkeypatch.setattr(
        main_mod,
        "allocate_p3_causal_wall",
        allocate_spy,
        raising=False,
    )
    monkeypatch.setattr(main_mod.time, "time", lambda: 100.0)
    monkeypatch.setattr(main_mod.asyncio, "create_task", create_task_spy)
    monkeypatch.setattr(main_mod, "Heartbeat", FakeHeartbeat)
    monkeypatch.setattr(main_mod, "LifecycleTracker", IdleWorker)
    monkeypatch.setattr(main_mod, "PumpPortalStream", IdleWorker)
    monkeypatch.setattr(main_mod, "CurvePoller", IdleWorker)
    monkeypatch.setattr(main_mod, "supervise", idle_supervise)
    monkeypatch.setattr(main_mod.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(main_mod, "sd_notify", ready_spy)
    monkeypatch.setattr(features_mod, "FeatureEngine", IdleWorker)
    monkeypatch.setattr(scoring_mod, "ConfluenceScorer", IdleWorker)
    monkeypatch.setattr(broker_mod, "PaperBroker", IdleWorker)
    monkeypatch.setattr(strategy_mod, "ClimbingStrategy", FakeStrategy)
    monkeypatch.setattr(counterfactual_mod, "ForwardReturnTracker", FakeTracker)

    stop = asyncio.Event()
    stop.set()
    await run(cfg_path, stop=stop)

    assert allocated == [(100.0, pytest.approx(math.nextafter(500.0, math.inf)))]
    assert order[:3] == ["allocate", "terminal_audit", "reconcile"]
    assert order.index("allocate") < order.index("task")
    assert order.index("allocate") < order.index("reconcile")
    assert order.index("terminal_audit") < order.index("reconcile")
    assert "open_position_audit" not in order
    assert order.index("reconcile") < order.index("task")
    assert order.index("allocate") < order.index("tracker_reconcile")
    assert "tracker_replay" not in order
    assert order.index("allocate") < order.index("ready")


def test_unmatched_p3_buys_reconcile_before_strategy_tasks_and_ready():
    source = Path("src/memebot/main.py").read_text(encoding="utf-8")
    source = source[source.index("async def run("):]
    reconcile = source.index("reconcile_unmatched_p3_buys(")
    coverage = source.index("assert_p3_buy_terminal_coverage(")
    first_task = source.index("asyncio.create_task(")
    ready = source.index('sd_notify("READY=1")')
    assert reconcile < coverage < first_task < ready


def test_open_p3_hardfails_reconcile_before_strategy_tasks_and_ready():
    source = Path("src/memebot/main.py").read_text(encoding="utf-8")
    source = source[source.index("async def run("):]
    restore = source.index("strategy.reconcile(")
    zero_close = source.index("strategy.zero_close_restored_p3_position(")
    first_task = source.index("asyncio.create_task(")
    ready = source.index('sd_notify("READY=1")')
    assert restore < zero_close < first_task < ready


def test_open_p3_malformed_latest_report_blocks_ready():
    import ast

    tree = ast.parse(Path("src/memebot/main.py").read_text(encoding="utf-8"))
    run_node = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "run"
    )
    calls = [
        node for node in ast.walk(run_node)
        if isinstance(node, ast.Call)
    ]
    reconcile_calls = [
        call for call in calls
        if isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "strategy"
        and call.func.attr == "reconcile"
    ]
    assert len(reconcile_calls) == 1
    assert {keyword.arg for keyword in reconcile_calls[0].keywords} == {
        "runtime_causal_floor", "max_open_positions",
    }


async def test_main_replays_journal_and_ledger_before_tracker_task_and_ready(
    tmp_path, monkeypatch,
):
    from memebot import broker as broker_mod
    from memebot import counterfactual as counterfactual_mod
    from memebot import features as features_mod
    from memebot import main as main_mod
    from memebot import scoring as scoring_mod
    from memebot import strategy as strategy_mod

    data_dir = tmp_path / "data"
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        CONFIG.format(data=data_dir) + STRATEGY_RUNTIME_SECTIONS + """
[providers.pumpportal]
ws_url = "ws://127.0.0.1:1"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_TRACKER_STARTUP_RPC"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
token_decimals = 6
""",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "MEMEBOT_TEST_TRACKER_STARTUP_RPC", "http://127.0.0.1:1"
    )

    calls = []
    tracker_kwargs = {}
    tracker_conn = None
    tracker_journal = None
    tracker_run_coro = None
    resumed_pending = 1
    real_create_task = asyncio.create_task

    class IdleWorker:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, stop):
            await stop.wait()

    class FakeStrategy(IdleWorker):
        recovery_pending = False

        def reconcile(self, *, runtime_causal_floor, max_open_positions):
            return ()

        def recover_pending_scores(self):
            return 0

    class FakeCandidate:
        def __init__(self, t0):
            self.t0 = t0

    class FakeTracker:
        def __init__(self, bus, conn, **kwargs):
            nonlocal tracker_conn, tracker_journal
            tracker_conn = conn
            tracker_journal = kwargs["journal"]
            assert tracker_journal is bus._journal
            tracker_kwargs.update(kwargs)
            self._candidates = []

        def resume_from_ledger(self, conn):
            assert conn is tracker_conn
            calls.append("ledger")
            if resumed_pending > 0:
                self._candidates = [
                    FakeCandidate(222.5),
                    FakeCandidate(111.25),
                ]
            return resumed_pending

        def replay_journal(self, *, since_wall, until_wall):
            assert resumed_pending > 0
            assert since_wall == 111.25
            assert until_wall == 1234.5
            calls.append("journal")
            return 0

        def run(self, stop):
            nonlocal tracker_run_coro

            async def wait_for_stop():
                await stop.wait()

            tracker_run_coro = wait_for_stop()
            return tracker_run_coro

    class FakeClient:
        async def aclose(self):
            pass

    async def idle_supervise(name, run_adapter, bus, stop):
        await stop.wait()

    def create_task_spy(coro, *args, **kwargs):
        if coro is tracker_run_coro:
            before_task = ["ledger"]
            if resumed_pending > 0:
                before_task.append("journal")
            assert calls == before_task
            calls.append("tracker_task")
        return real_create_task(coro, *args, **kwargs)

    def ready_spy(message):
        if message == "READY=1":
            before_ready = ["ledger"]
            if resumed_pending > 0:
                before_ready.append("journal")
            before_ready.append("tracker_task")
            assert calls == before_ready
            calls.append("ready")

    monkeypatch.setattr(main_mod, "Heartbeat", IdleWorker)
    monkeypatch.setattr(main_mod, "LifecycleTracker", IdleWorker)
    monkeypatch.setattr(main_mod, "PumpPortalStream", IdleWorker)
    monkeypatch.setattr(main_mod, "CurvePoller", IdleWorker)
    monkeypatch.setattr(main_mod, "supervise", idle_supervise)
    monkeypatch.setattr(main_mod.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(main_mod.asyncio, "create_task", create_task_spy)
    monkeypatch.setattr(main_mod.time, "time", lambda: 1234.5)
    monkeypatch.setattr(main_mod, "sd_notify", ready_spy)
    monkeypatch.setattr(features_mod, "FeatureEngine", IdleWorker)
    monkeypatch.setattr(scoring_mod, "ConfluenceScorer", IdleWorker)
    monkeypatch.setattr(broker_mod, "PaperBroker", IdleWorker)
    monkeypatch.setattr(strategy_mod, "ClimbingStrategy", FakeStrategy)
    monkeypatch.setattr(counterfactual_mod, "ForwardReturnTracker", FakeTracker)

    stop = asyncio.Event()
    stop.set()
    await run(cfg_path, stop=stop)

    assert calls == ["ledger", "journal", "tracker_task", "ready"]
    assert tracker_kwargs == {
        "journal": tracker_journal,
        "horizons": (10.0, 20.0),
        "token_decimals": 6,
        "stale_price_after_s": 300.0,
        "reconcile_interval_s": 14.0,
        "price_history_retention_s": 400.0,
        "price_history_max_samples_per_mint": 12,
        "price_history_max_mints": 11,
        "max_in_memory_pending_observations": 13,
    }
    assert tracker_journal is not None

    calls.clear()
    tracker_kwargs.clear()
    tracker_conn = None
    tracker_journal = None
    tracker_run_coro = None
    resumed_pending = 0
    await run(cfg_path, stop=stop)

    assert calls == ["ledger", "tracker_task", "ready"]
    assert tracker_journal is not None


async def test_runtime_boot_id_injected_into_progress_pipeline(
    tmp_path, monkeypatch,
):
    import secrets

    from memebot import main as main_mod

    data_dir = tmp_path / "data"
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        f"""
[storage]
data_dir = "{data_dir}"
[log]
level = "INFO"
[ops]
heartbeat_interval_s = 1
[journal]
max_bytes = 1000000
retention_days = 30
disk_cap_bytes = 100000000
disk_alarm_fraction = 0.8
[providers.pumpportal]
ws_url = "ws://127.0.0.1:1"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_RUNTIME_BOOT_RPC"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
token_decimals = 6
""",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "MEMEBOT_TEST_RUNTIME_BOOT_RPC", "http://127.0.0.1:1"
    )

    entropy_error = RuntimeError("entropy unavailable")
    allocated_floors = []
    committed_floors_at_entropy = []
    random_calls = []
    record_boot_calls = []
    task_calls = []
    injected = {}
    opened_connections = []
    real_allocate_p3_causal_wall = main_mod.allocate_p3_causal_wall
    real_open_db = main_mod.open_db
    real_record_boot = main_mod.record_boot
    real_create_task = asyncio.create_task

    def randbelow_spy(limit):
        assert len(opened_connections) == len(allocated_floors)
        live_connection = opened_connections[-1]
        assert live_connection.close_calls == 0
        assert live_connection.in_transaction is False
        independent = sqlite3.connect(data_dir / "memebot.db")
        try:
            committed_floor = independent.execute(
                "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
            ).fetchone()[0]
        finally:
            independent.close()
        assert committed_floor == allocated_floors[-1]
        committed_floors_at_entropy.append(committed_floor)
        random_calls.append(limit)
        if len(random_calls) == 1:
            raise entropy_error
        return 41

    class ConnectionSpy:
        def __init__(self, conn):
            self.conn = conn
            self.close_calls = 0

        def close(self):
            self.close_calls += 1
            return self.conn.close()

        def __getattr__(self, name):
            return getattr(self.conn, name)

    def open_db_spy(path):
        connection = ConnectionSpy(real_open_db(path))
        opened_connections.append(connection)
        return connection

    def allocate_spy(conn, *, raw_wall):
        floor = real_allocate_p3_causal_wall(conn, raw_wall=raw_wall)
        allocated_floors.append(floor)
        return floor

    def record_boot_spy(conn, config_hash):
        record_boot_calls.append((conn, config_hash))
        return real_record_boot(conn, config_hash)

    def create_task_spy(coro, *args, **kwargs):
        task_calls.append(coro)
        return real_create_task(coro, *args, **kwargs)

    class IdleWorker:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, stop):
            await stop.wait()

    class LifecycleSpy(IdleWorker):
        def __init__(self, *args, **kwargs):
            injected["lifecycle_boot"] = kwargs.get("runtime_boot_id")
            injected["lifecycle_floor"] = kwargs.get("runtime_causal_floor")

    class CurvePollerSpy(IdleWorker):
        def __init__(self, *args, **kwargs):
            injected["poller_boot"] = kwargs.get("source_boot_id")

    class FakeClient:
        async def aclose(self):
            pass

    async def idle_supervise(name, run_adapter, bus, stop):
        await stop.wait()

    monkeypatch.setattr(secrets, "randbelow", randbelow_spy)
    monkeypatch.setattr(main_mod, "allocate_p3_causal_wall", allocate_spy)
    monkeypatch.setattr(main_mod, "open_db", open_db_spy)
    monkeypatch.setattr(main_mod, "record_boot", record_boot_spy)
    monkeypatch.setattr(main_mod.asyncio, "create_task", create_task_spy)
    monkeypatch.setattr(main_mod, "Heartbeat", IdleWorker)
    monkeypatch.setattr(main_mod, "LifecycleTracker", LifecycleSpy)
    monkeypatch.setattr(main_mod, "PumpPortalStream", IdleWorker)
    monkeypatch.setattr(main_mod, "CurvePoller", CurvePollerSpy)
    monkeypatch.setattr(main_mod, "supervise", idle_supervise)
    monkeypatch.setattr(main_mod.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(main_mod, "validate_watch_only_release", lambda _cfg: None)

    stop = asyncio.Event()
    stop.set()
    with pytest.raises(RuntimeError) as exc_info:
        await run(cfg_path, stop=stop)

    assert exc_info.value is entropy_error
    assert committed_floors_at_entropy == allocated_floors
    assert random_calls == [2**63 - 1]
    assert record_boot_calls == []
    assert task_calls == []
    assert injected == {}
    assert len(opened_connections) == 1
    assert opened_connections[0].close_calls == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        opened_connections[0].execute("SELECT 1")
    failed_db = sqlite3.connect(data_dir / "memebot.db")
    try:
        assert failed_db.execute("SELECT COUNT(*) FROM boots").fetchone()[0] == 0
    finally:
        failed_db.close()

    await run(cfg_path, stop=stop)

    assert committed_floors_at_entropy == allocated_floors
    assert random_calls == [2**63 - 1, 2**63 - 1]
    assert len(record_boot_calls) == 1
    assert task_calls
    assert injected["lifecycle_boot"] == 42
    assert injected["poller_boot"] == 42
    assert type(injected["lifecycle_floor"]) is float

    conn = sqlite3.connect(data_dir / "memebot.db")
    try:
        operational_boot_id = conn.execute(
            "SELECT id FROM boots"
        ).fetchone()[0]
        causal_floor = conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0]
    finally:
        conn.close()
    assert operational_boot_id == 1
    assert injected["lifecycle_boot"] != operational_boot_id
    assert injected["lifecycle_floor"] == causal_floor
    assert len(opened_connections) == 2
    assert opened_connections[1].close_calls == 1


def test_all_progress_pipeline_callers_supply_boot_and_floor():
    import ast
    import subprocess

    repository = Path(__file__).parents[1]
    required_keywords = {
        "CurvePoller": {"source_boot_id"},
        "LifecycleTracker": {"runtime_boot_id", "runtime_causal_floor"},
    }
    expected_counts = {"CurvePoller": 8, "LifecycleTracker": 34}
    callers = {constructor: [] for constructor in required_keywords}
    omitted = []
    unsupported = []

    def terminal_name(node):
        return (
            node.id if isinstance(node, ast.Name)
            else node.attr if isinstance(node, ast.Attribute)
            else None
        )

    tracked = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.py"],
        cwd=repository,
        check=True,
        capture_output=True,
    ).stdout.decode().split("\0")
    assert any(tracked)

    for relative_path in sorted(filter(None, tracked)):
        tree = ast.parse((repository / relative_path).read_text())
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }

        def location(node):
            function = parents.get(node)
            while function is not None and not isinstance(
                function, (ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                function = parents.get(function)
            function_name = function.name if function is not None else "<module>"
            return (relative_path, function_name, node.lineno)

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name in required_keywords and alias.asname is not None:
                        unsupported.append(
                            (
                                relative_path,
                                node.lineno,
                                f"{alias.name} imported as {alias.asname}",
                            )
                        )

            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
                constructor = terminal_name(node.value)
                if constructor in required_keywords:
                    unsupported.append(
                        (
                            relative_path,
                            node.lineno,
                            f"{type(node).__name__} aliases {constructor}",
                        )
                    )

            if isinstance(node, ast.Call):
                constructor = terminal_name(node.func)
                if constructor not in required_keywords:
                    continue
                call_location = location(node)
                callers[constructor].append(call_location)
                present = {keyword.arg for keyword in node.keywords}
                missing = required_keywords[constructor] - present
                if missing:
                    omitted.append(
                        (*call_location, constructor, tuple(sorted(missing)))
                    )
                continue

            if not isinstance(node, (ast.Name, ast.Attribute)):
                continue
            constructor = terminal_name(node)
            if constructor not in required_keywords:
                continue
            parent = parents.get(node)
            if isinstance(parent, ast.Call) and parent.func is node:
                continue
            if (
                isinstance(parent, (ast.Assign, ast.AnnAssign, ast.NamedExpr))
                and parent.value is node
            ):
                continue
            unsupported.append(
                (
                    relative_path,
                    node.lineno,
                    f"unsupported {constructor} reference",
                )
            )

    assert unsupported == [], f"unsupported constructor forms: {unsupported}"
    assert omitted == [], f"constructor keywords omitted: {omitted}"
    observed_counts = {
        constructor: len(locations) for constructor, locations in callers.items()
    }
    assert observed_counts == expected_counts, (
        "constructor census changed: "
        f"expected={expected_counts}, observed={observed_counts}, "
        f"callers={callers}"
    )
    main_callers = {
        constructor
        for constructor, locations in callers.items()
        if any(path == "src/memebot/main.py" for path, _, _ in locations)
    }
    assert main_callers == set(required_keywords), (
        f"Main constructor coverage changed: {main_callers}"
    )


def test_curvepoller_rejects_removed_boot_seam():
    import inspect

    from memebot.ingest import curvepoller

    parameter = inspect.signature(
        getattr(curvepoller, "CurvePoller")
    ).parameters["source_boot_id"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is inspect.Parameter.empty


def test_lifecycle_rejects_removed_boot_seam():
    import inspect

    from memebot import lifecycle

    parameter = inspect.signature(
        getattr(lifecycle, "LifecycleTracker")
    ).parameters["runtime_boot_id"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is inspect.Parameter.empty


def test_lifecycle_rejects_removed_floor_seam():
    import inspect
    from typing import get_type_hints

    from memebot import lifecycle

    tracker = getattr(lifecycle, "LifecycleTracker")
    parameters = inspect.signature(tracker).parameters
    assert parameters["runtime_causal_floor"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["runtime_causal_floor"].default is inspect.Parameter.empty

    hints = get_type_hints(tracker.__init__)
    assert hints["runtime_boot_id"] is int
    assert hints["runtime_causal_floor"] is float

    class FakeBus:
        def subscribe(self, *args, **kwargs):
            return asyncio.Queue()

    conn = sqlite3.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="positive integer"):
            tracker(
                FakeBus(),
                conn,
                cfg={},
                runtime_boot_id=None,
                runtime_causal_floor=None,
            )
    finally:
        conn.close()


async def test_watch_only_validation_precedes_all_side_effects(tmp_path, monkeypatch):
    from memebot import main as main_mod
    from memebot.config import ConfigError, validate_watch_only_release

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        CONFIG.format(data=tmp_path / "data").replace(
            "entries_enabled = false", "entries_enabled = true",
        ),
        encoding="utf-8",
    )
    calls = []
    real_load_config = main_mod.load_config

    def load_config_spy(path):
        calls.append("load_config")
        return real_load_config(path)

    def validation_spy(cfg):
        calls.append("validate_watch_only_release")
        validate_watch_only_release(cfg)

    def forbidden_side_effect(name):
        def fail(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f"startup side effect ran before WATCH validation: {name}")

        return fail

    monkeypatch.setattr(main_mod, "load_config", load_config_spy)
    monkeypatch.setattr(
        main_mod, "validate_watch_only_release", validation_spy, raising=False,
    )
    for name in (
        "setup_logging", "open_db", "record_boot", "Journal", "EventBus",
        "Heartbeat", "sd_notify",
    ):
        monkeypatch.setattr(main_mod, name, forbidden_side_effect(name))
    monkeypatch.setattr(
        type(tmp_path), "mkdir", forbidden_side_effect("data_dir.mkdir"),
    )
    monkeypatch.setattr(
        main_mod.httpx, "AsyncClient", forbidden_side_effect("httpx.AsyncClient"),
    )
    monkeypatch.setattr(
        main_mod.asyncio, "create_task", forbidden_side_effect("asyncio.create_task"),
    )

    with pytest.raises(ConfigError, match="WATCH-only release"):
        await run(cfg_path)

    assert calls == ["load_config", "validate_watch_only_release"]
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize(
    ("failing_audit", "expected_calls", "message"),
    (
        (
            "assert_p3_buy_terminal_coverage",
            ["open_db", "assert_p3_buy_terminal_coverage"],
            "terminal audit failed",
        ),
        (
            "assert_no_open_p3_positions",
            [
                "open_db",
                "assert_p3_buy_terminal_coverage",
                "assert_no_open_p3_positions",
            ],
            "open-position audit failed",
        ),
    ),
)
async def test_watch_only_startup_audits_precede_boot_and_runtime_side_effects(
    tmp_path, monkeypatch, failing_audit, expected_calls, message,
):
    from memebot import main as main_mod

    cfg_path = tmp_path / "config.toml"
    data_dir = tmp_path / "data"
    db_path = data_dir / "memebot.db"
    cfg_path.write_text(CONFIG.format(data=data_dir), encoding="utf-8")
    calls = []
    audit_error = RuntimeError(message)
    real_open_db = main_mod.open_db

    class ConnectionSpy:
        def __init__(self, conn):
            self.conn = conn
            self.close_calls = 0

        def close(self):
            self.close_calls += 1
            return self.conn.close()

        def __getattr__(self, name):
            return getattr(self.conn, name)

    connection_spy = None

    def open_db_spy(path):
        nonlocal connection_spy
        calls.append("open_db")
        connection_spy = ConnectionSpy(real_open_db(path))
        return connection_spy

    def terminal_audit(conn):
        assert conn is connection_spy
        calls.append("assert_p3_buy_terminal_coverage")
        if failing_audit == "assert_p3_buy_terminal_coverage":
            raise audit_error

    def open_position_audit(conn):
        assert conn is connection_spy
        calls.append("assert_no_open_p3_positions")
        if failing_audit == "assert_no_open_p3_positions":
            raise audit_error

    def forbidden(name):
        def fail(*args, **kwargs):
            raise AssertionError(f"runtime side effect ran before WATCH audits: {name}")

        return fail

    monkeypatch.setattr(main_mod, "open_db", open_db_spy)
    monkeypatch.setattr(
        main_mod, "assert_p3_buy_terminal_coverage", terminal_audit, raising=False,
    )
    monkeypatch.setattr(
        main_mod, "assert_no_open_p3_positions", open_position_audit, raising=False,
    )
    monkeypatch.setattr(main_mod.EventBus, "subscribe", forbidden("bus.subscribe"))
    monkeypatch.setattr(main_mod.EventBus, "publish", forbidden("bus.publish"))
    for name in (
        "record_boot", "Journal", "EventBus", "Heartbeat", "LifecycleTracker",
        "PumpPortalStream", "CurvePoller", "GateRunner", "LiveProbes", "SafetyGate",
        "Governor", "HttpTransport", "NullOps", "TelegramOps", "supervise",
        "sd_notify", "mark_clean_shutdown",
    ):
        monkeypatch.setattr(main_mod, name, forbidden(name))
    monkeypatch.setattr(
        main_mod.httpx, "AsyncClient", forbidden("httpx.AsyncClient"),
    )
    monkeypatch.setattr(
        main_mod.asyncio, "create_task", forbidden("asyncio.create_task"),
    )

    with pytest.raises(RuntimeError) as exc_info:
        await run(cfg_path, stop=asyncio.Event())

    assert exc_info.value is audit_error
    assert calls == expected_calls
    assert connection_spy is not None
    assert connection_spy.close_calls == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connection_spy.execute("SELECT 1")
    persisted = sqlite3.connect(db_path)
    try:
        assert persisted.execute("SELECT COUNT(*) FROM boots").fetchone()[0] == 0
    finally:
        persisted.close()


async def test_watch_only_startup_audits_run_before_record_boot_on_success(
    tmp_path, monkeypatch,
):
    from memebot import main as main_mod

    cfg_path = tmp_path / "config.toml"
    data_dir = tmp_path / "data"
    cfg_path.write_text(CONFIG.format(data=data_dir), encoding="utf-8")
    calls = []
    record_boot_sentinel = RuntimeError("record_boot sentinel")
    real_open_db = main_mod.open_db

    class ConnectionSpy:
        def __init__(self, conn):
            self.conn = conn
            self.close_calls = 0

        def close(self):
            self.close_calls += 1
            return self.conn.close()

        def __getattr__(self, name):
            return getattr(self.conn, name)

    connection_spy = None

    def open_db_spy(path):
        nonlocal connection_spy
        calls.append("open_db")
        connection_spy = ConnectionSpy(real_open_db(path))
        return connection_spy

    def terminal_audit(conn):
        assert conn is connection_spy
        calls.append("assert_p3_buy_terminal_coverage")

    def open_position_audit(conn):
        assert conn is connection_spy
        calls.append("assert_no_open_p3_positions")

    def record_boot_spy(conn, config_hash):
        assert conn is connection_spy
        assert conn.execute("SELECT 1").fetchone()[0] == 1
        assert conn.close_calls == 0
        calls.append("record_boot")
        raise record_boot_sentinel

    def forbidden(name):
        def fail(*args, **kwargs):
            raise AssertionError(f"runtime side effect ran after record_boot sentinel: {name}")

        return fail

    monkeypatch.setattr(main_mod, "open_db", open_db_spy)
    monkeypatch.setattr(
        main_mod, "assert_p3_buy_terminal_coverage", terminal_audit, raising=False,
    )
    monkeypatch.setattr(
        main_mod, "assert_no_open_p3_positions", open_position_audit, raising=False,
    )
    monkeypatch.setattr(main_mod, "record_boot", record_boot_spy)
    monkeypatch.setattr(main_mod.EventBus, "subscribe", forbidden("bus.subscribe"))
    monkeypatch.setattr(main_mod.EventBus, "publish", forbidden("bus.publish"))
    for name in (
        "Journal", "EventBus", "Heartbeat", "LifecycleTracker", "PumpPortalStream",
        "CurvePoller", "GateRunner", "LiveProbes", "SafetyGate", "Governor",
        "HttpTransport", "NullOps", "TelegramOps", "supervise", "sd_notify",
        "mark_clean_shutdown",
    ):
        monkeypatch.setattr(main_mod, name, forbidden(name))
    monkeypatch.setattr(
        main_mod.httpx, "AsyncClient", forbidden("httpx.AsyncClient"),
    )
    monkeypatch.setattr(
        main_mod.asyncio, "create_task", forbidden("asyncio.create_task"),
    )

    try:
        with pytest.raises(RuntimeError) as exc_info:
            await run(cfg_path, stop=asyncio.Event())

        assert exc_info.value is record_boot_sentinel
        assert calls == [
            "open_db",
            "assert_p3_buy_terminal_coverage",
            "assert_no_open_p3_positions",
            "record_boot",
        ]
        assert connection_spy is not None
        assert connection_spy.close_calls == 0
        assert connection_spy.execute("SELECT 1").fetchone()[0] == 1
    finally:
        if connection_spy is not None and connection_spy.close_calls == 0:
            connection_spy.close()


async def test_boot_idle_clean_shutdown(tmp_path):
    cfg = tmp_path / "config.toml"
    cfg.write_text(CONFIG.format(data=tmp_path / "data"), encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)

    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    boots = conn.execute("SELECT config_hash, clean_shutdown FROM boots").fetchall()
    assert len(boots) == 1 and boots[0][1] == 1          # clean shutdown recorded
    assert (tmp_path / "data" / "heartbeat").exists()     # heartbeat ran
    journal_files = list((tmp_path / "data" / "journal").glob("events-*.jsonl"))
    lines = [json.loads(x) for f in journal_files for x in f.read_text().splitlines()]
    assert any(e["kind"] == "adapter_health" and e["status"] == "started" for e in lines)


async def test_watch_subscription_exists_before_ready(tmp_path, monkeypatch):
    from memebot import main as main_mod
    from memebot.bus import EventBus
    from memebot.events import CandidateScored

    state = {"watch_subscribed": False, "ready": False}

    class InspectBus(EventBus):
        def subscribe(self, *event_types, **kwargs):
            if CandidateScored in event_types:
                state["watch_subscribed"] = True
            return super().subscribe(*event_types, **kwargs)

    def inspect_notify(message):
        if message == "READY=1":
            assert state["watch_subscribed"] is True
            state["ready"] = True

    monkeypatch.setattr(main_mod, "EventBus", InspectBus)
    monkeypatch.setattr(main_mod, "sd_notify", inspect_notify)
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + """
[telegram]
enabled = true
watch_enabled = true
bot_token_env = "MEMEBOT_TEST_TG_TOKEN"
chat_id_env = "MEMEBOT_TEST_TG_CHAT"
max_alerts_per_hour = 30
""",
        encoding="utf-8",
    )
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)

    assert state["ready"] is True


async def test_watch_disabled_skips_subscription_and_loop_but_keeps_telegram_tasks(
    tmp_path, monkeypatch, caplog,
):
    from memebot import main as main_mod
    from memebot.bus import EventBus
    from memebot.events import CandidateScored

    state = {"subscriptions": [], "task_names_at_ready": set(), "watch_calls": []}

    class InspectBus(EventBus):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            state["bus"] = self

        def subscribe(self, *event_types, **kwargs):
            state["subscriptions"].append(event_types)
            return super().subscribe(*event_types, **kwargs)

    class WatchForbiddenOps:
        async def alert(self, text):
            pass

        async def watch(self, text, *, mint, segment):
            state["watch_calls"].append((text, mint, segment))

        async def poll_once(self):
            pass

    async def publish_candidate_then_wait(journal, interval_s, stop):
        await state["bus"].publish(CandidateScored(
            t_wall=1.0, t_mono=1.0, mint="NOISY", decision_id=1,
            segment="CLIMBING", score=9.0, spot_price_sol=1e-7,
        ))
        await stop.wait()

    def inspect_ready(message):
        if message == "READY=1":
            state["task_names_at_ready"] = {
                task.get_coro().__qualname__
                for task in asyncio.all_tasks()
                if task is not asyncio.current_task()
            }

    monkeypatch.setattr(main_mod, "EventBus", InspectBus)
    monkeypatch.setattr(main_mod, "NullOps", WatchForbiddenOps)
    monkeypatch.setattr(main_mod, "_periodic_prune", publish_candidate_then_wait)
    monkeypatch.setattr(main_mod, "sd_notify", inspect_ready)
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + """
[telegram]
enabled = true
watch_enabled = false
bot_token_env = "MEMEBOT_TEST_TG_TOKEN"
chat_id_env = "MEMEBOT_TEST_TG_CHAT"
max_alerts_per_hour = 30
""",
        encoding="utf-8",
    )
    stop = asyncio.Event()
    with caplog.at_level("INFO", logger="memebot.main"):
        task = asyncio.create_task(run(cfg, stop=stop))
        await asyncio.sleep(0.3)
        stop.set()
        await asyncio.wait_for(task, 5)

    assert (CandidateScored,) not in state["subscriptions"]
    assert "_watch_alert_loop" not in state["task_names_at_ready"]
    assert "_telegram_ops_loop" in state["task_names_at_ready"]
    assert "_trade_alert_loop" in state["task_names_at_ready"]
    assert state["watch_calls"] == []
    assert "telegram WATCH feed paused by config" in caplog.messages


async def test_watch_loop_stop_cancels_blocked_delivery_and_unsubscribes():
    from memebot.bus import EventBus
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop

    delivery_started = asyncio.Event()
    delivery_cancelled = asyncio.Event()
    delivery_exited = asyncio.Event()

    class BlockingOps:
        async def watch(self, text, *, mint, segment):
            delivery_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                delivery_cancelled.set()
                raise
            finally:
                delivery_exited.set()

    def scored(decision_id, mint):
        return CandidateScored(
            t_wall=float(decision_id), t_mono=float(decision_id), mint=mint,
            decision_id=decision_id, segment="CLIMBING", score=9.0,
            spot_price_sol=1e-7,
        )

    bus = EventBus(maxsize=1)
    queue = bus.subscribe(CandidateScored)
    third_put_started = asyncio.Event()
    original_put = queue.put

    async def observed_put(event):
        if event.mint == "THIRD":
            third_put_started.set()
        await original_put(event)

    queue.put = observed_put
    stop = asyncio.Event()
    worker = asyncio.create_task(
        _watch_alert_loop(bus, BlockingOps(), stop, queue=queue),
    )
    third_publisher = None
    try:
        await bus.publish(scored(1, "FIRST"))
        await asyncio.wait_for(delivery_started.wait(), 1.0)
        await bus.publish(scored(2, "SECOND"))
        third_publisher = asyncio.create_task(bus.publish(scored(3, "THIRD")))
        await asyncio.wait_for(third_put_started.wait(), 1.0)
        assert third_publisher.done() is False

        stop.set()
        await asyncio.wait_for(asyncio.shield(worker), 1.0)
        await asyncio.wait_for(asyncio.shield(third_publisher), 1.0)

        assert delivery_cancelled.is_set() is True
        assert delivery_exited.is_set() is True
        assert all(subscription.queue is not queue for subscription in bus._subs)
        assert (await queue.get()).mint == "SECOND"
        assert queue.empty()
        assert bus.critical_state() == (0, 0, False)
        assert worker.done() is True
        assert third_publisher.done() is True
    finally:
        stop.set()
        for task in (worker, third_publisher):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (worker, third_publisher) if task is not None),
            return_exceptions=True,
        )


async def test_main_noncritical_loops_unsubscribe_before_critical_drain():
    from memebot.bus import EventBus
    from memebot.events import AdapterHealth, PaperEntry
    from memebot.main import _telegram_ops_loop, _trade_alert_loop

    class QuietOps:
        async def alert(self, text):
            pass

        async def poll_once(self):
            pass

    bus = EventBus(maxsize=1)
    stop = asyncio.Event()
    workers = (
        asyncio.create_task(_telegram_ops_loop(bus, QuietOps(), stop)),
        asyncio.create_task(_trade_alert_loop(bus, QuietOps(), stop)),
    )
    try:
        for _ in range(100):
            if len(bus._subs) == 2:
                break
            await asyncio.sleep(0)
        assert len(bus._subs) == 2

        stop.set()
        await asyncio.wait_for(asyncio.gather(*workers), 2.0)
        assert bus._subs == []

        health = AdapterHealth(
            t_wall=1.0, t_mono=1.0, adapter="test", status="down", detail="x",
        )
        entry = PaperEntry(
            t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING",
            qty=1.0, fill_price=1e-6, size_sol=1e-6,
            score=90.0, realism_grade="A",
        )
        await asyncio.wait_for(bus.publish(health), 0.1)
        await asyncio.wait_for(bus.publish(health), 0.1)
        await asyncio.wait_for(bus.publish(entry), 0.1)
        await asyncio.wait_for(bus.publish(entry), 0.1)
    finally:
        stop.set()
        for worker in workers:
            if not worker.done():
                worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def test_shutdown_ack_barrier_waits_for_dequeued_persistence_failure():
    import inspect

    from memebot.bus import EventBus
    from memebot.events import AdapterHealth
    from memebot.main import _wait_for_critical_drain, run

    bus = EventBus()
    queue = bus.subscribe(AdapterHealth, critical=True)
    await bus.publish(AdapterHealth(
        t_wall=1.0, t_mono=1.0, adapter="producer", status="up", detail="held",
    ))
    held = await queue.get()
    assert held.detail == "held"
    assert queue.empty() is True
    assert bus.critical_state() == (1, 1, False)

    barrier = asyncio.create_task(_wait_for_critical_drain(bus))
    try:
        await asyncio.sleep(0)
        assert barrier.done() is False

        bus.critical_done(queue)
        assert await asyncio.wait_for(barrier, 0.5) is True
        assert bus.critical_state() == (1, 0, False)
    finally:
        if not barrier.done():
            barrier.cancel()
        await asyncio.gather(barrier, return_exceptions=True)
        bus.unsubscribe(queue)

    shutdown_source = inspect.getsource(run)
    noncritical_reap = shutdown_source.index("for t in noncritical_tasks:")
    barrier_call = shutdown_source.index(
        "critical_drained = await _wait_for_critical_drain(bus)"
    )
    critical_stop = shutdown_source.index("critical_stop.set()")
    critical_reap = shutdown_source.index("for t in critical_tasks:")
    clean_marker = shutdown_source.index("mark_clean_shutdown(conn, boot_id)")
    assert noncritical_reap < barrier_call < critical_stop < critical_reap < clean_marker
    assert "tracker.run(critical_stop)" in shutdown_source
    assert "gate_runner.run(critical_stop)" in shutdown_source
    assert "strategy.run(critical_stop)" in shutdown_source


def test_all_eventbus_consumers_close_subscriptions():
    import ast

    source_root = Path(__file__).parents[1] / "src" / "memebot"
    subscribe_owners = set()
    unsubscribe_args: dict[str, set[str]] = {}
    finally_unsubscribe_args: dict[str, set[str]] = {}
    delegated_watch_queue = False

    for path in source_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative = path.relative_to(source_root).as_posix()

        class Visitor(ast.NodeVisitor):
            def __init__(self):
                self.scope = []

            def visit_ClassDef(self, node):
                self.scope.append(node.name)
                self.generic_visit(node)
                self.scope.pop()

            def visit_FunctionDef(self, node):
                self.scope.append(node.name)
                self.generic_visit(node)
                self.scope.pop()

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_Call(self, node):
                nonlocal delegated_watch_queue
                owner = ".".join((relative, *self.scope))
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr == "subscribe":
                        subscribe_owners.add(owner)
                    elif node.func.attr == "unsubscribe" and node.args:
                        unsubscribe_args.setdefault(owner, set()).add(
                            ast.unparse(node.args[0])
                        )
                elif (
                    relative == "main.py"
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "_watch_alert_loop"
                    and any(
                        keyword.arg == "queue"
                        and isinstance(keyword.value, ast.Name)
                        and keyword.value.id == "watch_queue"
                        for keyword in node.keywords
                    )
                ):
                    delegated_watch_queue = True
                self.generic_visit(node)

            def visit_Try(self, node):
                owner = ".".join((relative, *self.scope))
                for final_node in node.finalbody:
                    for child in ast.walk(final_node):
                        if (
                            isinstance(child, ast.Call)
                            and isinstance(child.func, ast.Attribute)
                            and child.func.attr == "unsubscribe"
                            and child.args
                        ):
                            finally_unsubscribe_args.setdefault(owner, set()).add(
                                ast.unparse(child.args[0])
                            )
                self.generic_visit(node)

        Visitor().visit(tree)

    expected = {
        "counterfactual.py.ForwardReturnTracker.__init__",
        "features.py.FeatureEngine.__init__",
        "lifecycle.py.LifecycleTracker.__init__",
        "main.py._telegram_ops_loop",
        "main.py._trade_alert_loop",
        "main.py._watch_alert_loop",
        "main.py.run",
        "safety/gate.py.GateRunner.__init__",
        "strategy.py.ClimbingStrategy.__init__",
    }
    assert subscribe_owners == expected
    assert unsubscribe_args["counterfactual.py.ForwardReturnTracker.run"] == {"self._q"}
    assert unsubscribe_args["features.py.FeatureEngine.run"] == {"self._q"}
    assert unsubscribe_args["lifecycle.py.LifecycleTracker.run"] == {"self._queue"}
    assert unsubscribe_args["safety/gate.py.GateRunner.run"] == {"self._q"}
    assert unsubscribe_args["strategy.py.ClimbingStrategy.run"] == {"self._q"}
    assert unsubscribe_args["main.py._telegram_ops_loop"] == {"q"}
    assert unsubscribe_args["main.py._trade_alert_loop"] == {"q"}
    assert unsubscribe_args["main.py._watch_alert_loop"] == {"q"}
    assert finally_unsubscribe_args == {
        "counterfactual.py.ForwardReturnTracker.run": {"self._q"},
        "features.py.FeatureEngine.run": {"self._q"},
        "lifecycle.py.LifecycleTracker.run": {"self._queue"},
        "main.py._telegram_ops_loop": {"q"},
        "main.py._trade_alert_loop": {"q"},
        "main.py._watch_alert_loop": {"q"},
        "safety/gate.py.GateRunner.run": {"self._q"},
        "strategy.py.ClimbingStrategy.run": {"self._q"},
    }
    assert delegated_watch_queue is True


async def test_orderly_shutdown_releases_blocked_watch_publisher(tmp_path, monkeypatch):
    from memebot import main as main_mod
    from memebot.bus import EventBus
    from memebot.events import CandidateScored

    state = {}
    forwarding_first = asyncio.Event()
    delivery_cancelled = asyncio.Event()
    delivery_exited = asyncio.Event()
    third_publish_started = asyncio.Event()
    third_publish_done = asyncio.Event()

    class BoundedBus(EventBus):
        def __init__(self, *args, **kwargs):
            kwargs["maxsize"] = 1
            super().__init__(*args, **kwargs)
            state["bus"] = self

        def subscribe(self, *event_types, **kwargs):
            queue = super().subscribe(*event_types, **kwargs)
            if CandidateScored in event_types:
                state["watch_queue"] = queue
                original_put = queue.put

                async def observed_put(event):
                    if event.mint == "THIRD":
                        third_publish_started.set()
                    await original_put(event)

                queue.put = observed_put
            return queue

    class BlockingOps:
        async def alert(self, text):
            pass

        async def watch(self, text, *, mint, segment):
            forwarding_first.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                delivery_cancelled.set()
                raise
            finally:
                delivery_exited.set()

        async def poll_once(self):
            pass

    def scored(decision_id, mint):
        return CandidateScored(
            t_wall=float(decision_id), t_mono=float(decision_id), mint=mint,
            decision_id=decision_id, segment="CLIMBING", score=9.0,
            spot_price_sol=1e-7,
        )

    async def blocked_watch_publisher(journal, interval_s, producer_stop):
        bus = state["bus"]
        await bus.publish(scored(1, "FIRST"))
        await forwarding_first.wait()
        await bus.publish(scored(2, "SECOND"))
        await bus.publish(scored(3, "THIRD"))
        third_publish_done.set()

    monkeypatch.setattr(main_mod, "EventBus", BoundedBus)
    monkeypatch.setattr(main_mod, "NullOps", BlockingOps)
    monkeypatch.setattr(main_mod, "_periodic_prune", blocked_watch_publisher)
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + """
[telegram]
enabled = true
watch_enabled = true
bot_token_env = "MEMEBOT_TEST_TG_TOKEN"
chat_id_env = "MEMEBOT_TEST_TG_CHAT"
max_alerts_per_hour = 30
""",
        encoding="utf-8",
    )
    stop = asyncio.Event()
    main_task = asyncio.create_task(run(cfg, stop=stop))
    try:
        await asyncio.wait_for(third_publish_started.wait(), 1.0)
        await asyncio.sleep(0)
        assert third_publish_done.is_set() is False

        stop.set()
        await asyncio.wait_for(asyncio.shield(main_task), 2.0)

        queue = state["watch_queue"]
        assert delivery_cancelled.is_set() is True
        assert delivery_exited.is_set() is True
        assert third_publish_done.is_set() is True
        assert all(subscription.queue is not queue for subscription in state["bus"]._subs)
        assert (await queue.get()).mint == "SECOND"
        assert queue.empty()
        assert state["bus"].critical_state() == (0, 0, False)
        assert not any(
            task.get_coro().__qualname__ == "_watch_alert_loop"
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
        )
        conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
        assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
        conn.close()
    finally:
        stop.set()
        if not main_task.done():
            main_task.cancel()
        await asyncio.gather(main_task, return_exceptions=True)
        leaked_watch_tasks = [
            task for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_coro().__qualname__ == "_watch_alert_loop"
        ]
        for task in leaked_watch_tasks:
            task.cancel()
        await asyncio.gather(*leaked_watch_tasks, return_exceptions=True)


async def test_clean_shutdown_recorded_even_if_heartbeat_task_died(tmp_path, monkeypatch):
    # A non-OSError bug in the beat path must not erase clean-shutdown forensics.
    from memebot import ops

    def exploding_beat(self):
        raise RuntimeError("beat bug")

    monkeypatch.setattr(ops.Heartbeat, "beat", exploding_beat)
    cfg = tmp_path / "config.toml"
    cfg.write_text(CONFIG.format(data=tmp_path / "data"), encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)  # run() must not raise

    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    boots = conn.execute("SELECT clean_shutdown FROM boots").fetchall()
    assert boots == [(1,)]  # orderly stop still recorded clean


async def test_boot_with_adapters_still_clean(tmp_path):
    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_MISSING_RPC"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
sellable_supply = 793100000.0
token_decimals = 6
""", encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 10)
    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
    conn.close()


async def test_curvepoller_asyncclient_closed_on_shutdown(tmp_path, monkeypatch):
    """MB-4 C7-review follow-up: the poller-owned httpx.AsyncClient must be
    aclose()d on shutdown (previously leaked silently). rpc_url_env IS set
    (unlike the disabled-poller test above) so main.py takes the "construct a
    real AsyncClient" branch; the RPC url points at a closed local port so the
    poller's own polls fail fast and harmlessly via its existing httpx.HTTPError
    guard — proving the close path runs even though the poller was actively
    erroring, not idle."""
    close_calls = []
    original_aclose = httpx.AsyncClient.aclose

    async def counting_aclose(self):
        close_calls.append(1)
        await original_aclose(self)

    monkeypatch.setattr(httpx.AsyncClient, "aclose", counting_aclose)
    monkeypatch.setenv("MEMEBOT_HELIUS_RPC_URL", f"http://127.0.0.1:{_closed_ephemeral_port()}")

    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + STRATEGY_RUNTIME_SECTIONS + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_HELIUS_RPC_URL"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 0.05
batch_size = 100
max_tracked = 300
[pumpfun]
sellable_supply = 793100000.0
token_decimals = 6
""", encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)   # give the poller a few failed polls before stopping
    stop.set()
    await asyncio.wait_for(task, 10)

    assert close_calls == [1]   # aclose() called exactly once, not zero (leak) or many
    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
    conn.close()


async def test_periodic_prune_runs_and_exits(tmp_path):
    from memebot.main import _periodic_prune

    class FakeJournal:
        def __init__(self):
            self.prunes = 0

        def prune(self):
            self.prunes += 1
            return []

    j = FakeJournal()
    stop = asyncio.Event()
    task = asyncio.create_task(_periodic_prune(j, 0.03, stop))
    await asyncio.sleep(0.1)
    assert j.prunes >= 2                 # pruned multiple times
    stop.set()
    await asyncio.wait_for(task, 1)      # exits promptly on stop


SAFETY_TELEGRAM_SECTIONS = """
[safety]
top10_holder_max_pct = 30.0
dev_wallet_max_pct = 10.0
honeypot_max_impact_pct = 30.0
recheck_interval_s = 900
rugcheck_base = "https://rc.test"
goplus_base = "https://gp.test"
jupiter_base = "https://jup.test"
[governor.rugcheck]
per_minute = 15
[governor.goplus]
per_minute = 30
[governor.jupiter]
per_minute = 60
[telegram]
enabled = false
bot_token_env = "MEMEBOT_TEST_TG_TOKEN"
chat_id_env = "MEMEBOT_TEST_TG_CHAT"
max_alerts_per_hour = 30
"""


async def test_boot_with_gate_and_telegram_disabled_clean(tmp_path, monkeypatch):
    """D10b: boot with providers + [safety]/[governor.*]/[telegram enabled=false].
    Helius rpc env is UNSET, so the gate's LiveProbes/GateRunner path is skipped
    (mirrors the existing "curvepoller disabled" pattern) -- the process must still
    boot and shut down clean with the new sections present and unused."""
    monkeypatch.delenv("MEMEBOT_TEST_MISSING_RPC", raising=False)
    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_MISSING_RPC"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
sellable_supply = 793100000.0
token_decimals = 6
""" + SAFETY_TELEGRAM_SECTIONS, encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 10)
    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
    conn.close()


async def test_boot_with_gate_wired_closes_ext_jup_clients(tmp_path, monkeypatch):
    """D10b: when the helius rpc_url secret IS set, main.py must build the
    SafetyGate/GateRunner stack, including ext_client + jup_client httpx.AsyncClients
    -- and close both exactly once on shutdown (M2 C10 AsyncClient-leak fix, extended
    to the two new clients). rpc_url points at a closed port so the gate's own
    evaluate() calls fail fast and harmlessly, same pattern as the poller-close test."""
    close_calls = []
    original_aclose = httpx.AsyncClient.aclose

    async def counting_aclose(self):
        close_calls.append(1)
        await original_aclose(self)

    monkeypatch.setattr(httpx.AsyncClient, "aclose", counting_aclose)
    monkeypatch.setenv("MEMEBOT_HELIUS_RPC_URL", f"http://127.0.0.1:{_closed_ephemeral_port()}")

    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + STRATEGY_RUNTIME_SECTIONS + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_HELIUS_RPC_URL"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 0.05
batch_size = 100
max_tracked = 300
[pumpfun]
sellable_supply = 793100000.0
token_decimals = 6
""" + SAFETY_TELEGRAM_SECTIONS, encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 10)

    # poller_client + ext_client + jup_client == 3 AsyncClients to close (helius set).
    assert close_calls == [1, 1, 1]
    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
    conn.close()


async def test_boot_with_telegram_enabled_missing_chat_id_falls_back_to_nullops(tmp_path, monkeypatch):
    """D10b (D9-review note): telegram enabled=true but chat_id env unset must NOT
    construct a live TelegramOps with an empty chat_id (would alert into the void
    with no allowlist) -- falls back to NullOps, and boot still shuts down clean.

    Spies on memebot.main.NullOps/TelegramOps construction directly (rather than only
    asserting clean shutdown) so a regression that wrongly builds a live TelegramOps
    with chat_id="" is actually caught -- a clean-shutdown-only assertion passes
    whichever branch is taken, since nothing in this idle boot window ever calls
    ops.alert()."""
    from memebot import main as main_mod

    null_ops_calls = []
    telegram_ops_calls = []
    orig_null_ops = main_mod.NullOps
    orig_telegram_ops = main_mod.TelegramOps

    def spy_null_ops(*a, **kw):
        null_ops_calls.append((a, kw))
        return orig_null_ops(*a, **kw)

    def spy_telegram_ops(*a, **kw):
        telegram_ops_calls.append((a, kw))
        return orig_telegram_ops(*a, **kw)

    monkeypatch.setattr(main_mod, "NullOps", spy_null_ops)
    monkeypatch.setattr(main_mod, "TelegramOps", spy_telegram_ops)
    monkeypatch.setenv("MEMEBOT_TEST_TG_TOKEN2", "faketoken")
    monkeypatch.delenv("MEMEBOT_TEST_TG_CHAT2", raising=False)
    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_MISSING_RPC2"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
sellable_supply = 793100000.0
token_decimals = 6
[safety]
top10_holder_max_pct = 30.0
dev_wallet_max_pct = 10.0
honeypot_max_impact_pct = 30.0
recheck_interval_s = 900
rugcheck_base = "https://rc.test"
goplus_base = "https://gp.test"
jupiter_base = "https://jup.test"
[governor.rugcheck]
per_minute = 15
[governor.goplus]
per_minute = 30
[governor.jupiter]
per_minute = 60
[telegram]
enabled = true
watch_enabled = true
bot_token_env = "MEMEBOT_TEST_TG_TOKEN2"
chat_id_env = "MEMEBOT_TEST_TG_CHAT2"
max_alerts_per_hour = 30
""", encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 10)   # must not hang/crash despite enabled=true + no chat id

    assert len(null_ops_calls) == 1        # NullOps built exactly once
    assert telegram_ops_calls == []        # TelegramOps NEVER constructed
    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
    conn.close()


async def test_boot_with_telegram_enabled_empty_chat_id_falls_back_to_nullops(tmp_path, monkeypatch):
    """D10b (D9-review note, empty-string variant): the chat_id env var being SET but
    to an empty string is distinct from unset -- os.environ.get returns "" (falsy, but
    not None) rather than raising. Must still fall back to NullOps, not construct a
    TelegramOps with chat_id=''. Same construction-spy technique as the unset-chat_id
    test above (a clean-shutdown-only assertion doesn't distinguish the two branches)."""
    from memebot import main as main_mod

    null_ops_calls = []
    telegram_ops_calls = []
    orig_null_ops = main_mod.NullOps
    orig_telegram_ops = main_mod.TelegramOps

    def spy_null_ops(*a, **kw):
        null_ops_calls.append((a, kw))
        return orig_null_ops(*a, **kw)

    def spy_telegram_ops(*a, **kw):
        telegram_ops_calls.append((a, kw))
        return orig_telegram_ops(*a, **kw)

    monkeypatch.setattr(main_mod, "NullOps", spy_null_ops)
    monkeypatch.setattr(main_mod, "TelegramOps", spy_telegram_ops)
    monkeypatch.setenv("MEMEBOT_TEST_TG_TOKEN3", "faketoken")
    monkeypatch.setenv("MEMEBOT_TEST_TG_CHAT3", "")   # SET, but empty -- not unset
    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data") + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_TEST_MISSING_RPC3"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 10
batch_size = 100
max_tracked = 300
[pumpfun]
sellable_supply = 793100000.0
token_decimals = 6
[safety]
top10_holder_max_pct = 30.0
dev_wallet_max_pct = 10.0
honeypot_max_impact_pct = 30.0
recheck_interval_s = 900
rugcheck_base = "https://rc.test"
goplus_base = "https://gp.test"
jupiter_base = "https://jup.test"
[governor.rugcheck]
per_minute = 15
[governor.goplus]
per_minute = 30
[governor.jupiter]
per_minute = 60
[telegram]
enabled = true
watch_enabled = true
bot_token_env = "MEMEBOT_TEST_TG_TOKEN3"
chat_id_env = "MEMEBOT_TEST_TG_CHAT3"
max_alerts_per_hour = 30
""", encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 10)   # must not hang/crash despite enabled=true + empty chat id

    assert len(null_ops_calls) == 1        # NullOps built exactly once
    assert telegram_ops_calls == []        # TelegramOps NEVER constructed (chat_id="" rejected)
    conn = sqlite3.connect(tmp_path / "data" / "memebot.db")
    assert conn.execute("SELECT clean_shutdown FROM boots").fetchall() == [(1,)]
    conn.close()


async def test_telegram_ops_loop_suppresses_rug_by_default_but_pages_ops():
    # The owner wants telegram for the bot's buy/sell decisions + ops health, NOT per-rug
    # spam (on pump.fun most tokens are genuine rugs, so a per-rug ping is noise). Default
    # (alert_on_rug=False): SafetyHardFail is drained but NOT alerted; AdapterHealth still pages.
    from memebot.main import _telegram_ops_loop
    from memebot.bus import EventBus
    from memebot.events import AdapterHealth, SafetyHardFail
    from memebot.telegram import FakeTransport, TelegramOps

    bus = EventBus()
    tp = FakeTransport()
    ops = TelegramOps(tp, chat_id="1", max_alerts_per_hour=100, clock=lambda: 0.0)
    stop = asyncio.Event()
    task = asyncio.create_task(_telegram_ops_loop(bus, ops, stop))   # default alert_on_rug=False
    await asyncio.sleep(0.05)   # let the loop reach bus.subscribe() before we publish
    await bus.publish(SafetyHardFail(t_wall=0.0, t_mono=0.0, mint="RUGMINT",
                                     reasons=["holder_concentration"]))
    await bus.publish(AdapterHealth(t_wall=0.0, t_mono=0.0, adapter="pumpportal",
                                    status="down", detail="x"))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)
    texts = [m["text"] for m in tp.sent]
    assert not any("RUG" in t for t in texts)                       # rug spam suppressed
    assert any("adapter pumpportal down" in t for t in texts)       # ops paging preserved


async def test_telegram_ops_loop_alerts_rug_when_flag_enabled():
    # The debug escape hatch: alert_on_rug=True restores per-rug paging (for live diagnosis).
    from memebot.main import _telegram_ops_loop
    from memebot.bus import EventBus
    from memebot.events import SafetyHardFail
    from memebot.telegram import FakeTransport, TelegramOps

    bus = EventBus()
    tp = FakeTransport()
    ops = TelegramOps(tp, chat_id="1", max_alerts_per_hour=100, clock=lambda: 0.0)
    stop = asyncio.Event()
    task = asyncio.create_task(_telegram_ops_loop(bus, ops, stop, alert_on_rug=True))
    await asyncio.sleep(0.05)   # let the loop reach bus.subscribe() before we publish
    await bus.publish(SafetyHardFail(t_wall=0.0, t_mono=0.0, mint="RUGMINT",
                                     reasons=["holder_concentration"]))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert any("RUG RUGMINT" in m["text"] for m in tp.sent)


async def test_main_supplies_feature_mint_cap(tmp_path, monkeypatch):
    from memebot import features as features_mod
    from memebot import main as main_mod

    expected_cap = 17
    captured = []
    constructed = asyncio.Event()
    real_feature_engine = features_mod.FeatureEngine

    class CapturingFeatureEngine(real_feature_engine):
        def __init__(self, *args, **kwargs):
            captured.append(kwargs)
            super().__init__(*args, **kwargs)
            constructed.set()

    async def idle_supervise(name, run_adapter, bus, stop):
        await stop.wait()

    config_text = Path("config.toml").read_text(encoding="utf-8").replace(
        'data_dir = "data"', f'data_dir = "{tmp_path / "data"}"',
    ).replace(
        "max_feature_mints = 1000",
        f"max_feature_mints = {expected_cap}",
    )
    cfg = tmp_path / "config.toml"
    cfg.write_text(config_text, encoding="utf-8")

    monkeypatch.setattr(features_mod, "FeatureEngine", CapturingFeatureEngine)
    monkeypatch.setattr(main_mod, "supervise", idle_supervise)
    monkeypatch.setenv("MEMEBOT_HELIUS_RPC_URL", "http://127.0.0.1:1")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    try:
        await asyncio.wait_for(constructed.wait(), 2.0)
    finally:
        stop.set()
        await asyncio.wait_for(task, 5.0)

    assert captured == [{"max_feature_mints": expected_cap}]


async def test_main_constructs_strategy_with_canonical_resolver(
    tmp_path, monkeypatch,
):
    from memebot import canonical as canonical_mod
    from memebot import main as main_mod
    from memebot import strategy as strategy_mod
    from memebot.config import load_config, validate_runtime_config

    expected_horizons = (17.0, 31.0)
    calls = []
    resolver_constructions = []
    strategy_resolvers = []
    strategy_constructions = []
    allocated_floors = []
    boot_bounds = []
    real_create_task = asyncio.create_task
    real_allocate_causal_wall = main_mod.allocate_p3_causal_wall

    def validate_spy(cfg):
        validate_runtime_config(cfg)
        calls.append("validated")

    class CapturingResolver:
        def __init__(self, conn, **kwargs):
            calls.append("resolver")
            resolver_constructions.append((self, conn, kwargs))

    class FakeStrategy:
        recovery_pending = False

        def __init__(self, *args, **kwargs):
            calls.append("strategy")
            strategy_resolvers.append(kwargs.get("canonical_resolver"))
            strategy_constructions.append((args, kwargs))

        def reconcile(self, *, runtime_causal_floor, max_open_positions):
            return ()

        def recover_pending_scores(self):
            return 0

        async def run(self, stop):
            await stop.wait()

    async def idle_supervise(name, run_adapter, bus, stop):
        await stop.wait()

    def create_task_spy(coro, *args, **kwargs):
        calls.append("task")
        return real_create_task(coro, *args, **kwargs)

    def allocate_causal_wall_spy(conn, *, raw_wall):
        allocated = real_allocate_causal_wall(conn, raw_wall=raw_wall)
        allocated_floors.append(allocated)
        return allocated

    def randbelow_spy(upper):
        boot_bounds.append(upper)
        return 4241

    config_text = Path("config.toml").read_text(encoding="utf-8").replace(
        'data_dir = "data"', f'data_dir = "{tmp_path / "data"}"',
    ).replace(
        "horizons_s = [3600.0,21600.0,86400.0]",
        "horizons_s = [17.0,31.0]",
    )
    cfg = tmp_path / "config.toml"
    cfg.write_text(config_text, encoding="utf-8")
    expected_config_hash = load_config(cfg).resolved_hash

    monkeypatch.setattr(main_mod, "validate_runtime_config", validate_spy)
    monkeypatch.setattr(
        main_mod,
        "allocate_p3_causal_wall",
        allocate_causal_wall_spy,
    )
    monkeypatch.setattr(main_mod.secrets, "randbelow", randbelow_spy)
    monkeypatch.setattr(canonical_mod, "CanonicalResolver", CapturingResolver)
    monkeypatch.setattr(strategy_mod, "ClimbingStrategy", FakeStrategy)
    monkeypatch.setattr(main_mod, "supervise", idle_supervise)
    monkeypatch.setattr(main_mod.asyncio, "create_task", create_task_spy)
    monkeypatch.setenv("MEMEBOT_HELIUS_RPC_URL", "http://127.0.0.1:1")

    stop = asyncio.Event()
    stop.set()
    await run(cfg, stop=stop)

    assert calls[:2] == ["validated", "resolver"]
    assert calls.index("resolver") < calls.index("task")
    assert calls.index("resolver") < calls.index("strategy")
    assert len(resolver_constructions) == 1
    resolver, conn, resolver_kwargs = resolver_constructions[0]
    assert strategy_resolvers == [resolver]
    strategy_args, strategy_kwargs = strategy_constructions[0]
    assert conn is strategy_args[1]
    assert resolver_kwargs["feature_engine"] is strategy_kwargs["feature_engine"]
    assert resolver_kwargs["canonical_cfg"]["resolver_version"] == "canonical-v1"
    assert resolver_kwargs["safety_cfg"]["top10_holder_max_pct"] == 30.0
    assert resolver_kwargs["pumpfun_cfg"]["token_decimals"] == 6
    assert resolver_kwargs["config_hash"] == expected_config_hash
    assert resolver_kwargs["counterfactual_horizons"] == expected_horizons
    assert type(resolver_kwargs["counterfactual_horizons"]) is tuple
    assert boot_bounds == [2**63 - 1]
    assert resolver_kwargs["runtime_boot_id"] == 4242
    assert len(allocated_floors) == 1
    assert resolver_kwargs["runtime_causal_floor"] == allocated_floors[0]


async def test_main_wires_same_horizons_to_resolver_and_tracker(
    tmp_path, monkeypatch,
):
    from memebot import canonical as canonical_mod
    from memebot import counterfactual as counterfactual_mod
    from memebot import main as main_mod
    from memebot import strategy as strategy_mod

    expected_horizons = (17.0, 31.0)
    resolver_horizons = []
    tracker_horizons = []

    class CapturingResolver:
        def __init__(self, conn, **kwargs):
            resolver_horizons.append(kwargs["counterfactual_horizons"])

    class CapturingTracker:
        def __init__(self, *args, **kwargs):
            tracker_horizons.append(kwargs["horizons"])

        def resume_from_ledger(self, conn):
            return 0

        async def run(self, stop):
            await stop.wait()

    class FakeStrategy:
        recovery_pending = False

        def __init__(self, *args, **kwargs):
            pass

        def reconcile(self, *, runtime_causal_floor, max_open_positions):
            return ()

        def recover_pending_scores(self):
            return 0

        async def run(self, stop):
            await stop.wait()

    async def idle_supervise(name, run_adapter, bus, stop):
        await stop.wait()

    config_text = Path("config.toml").read_text(encoding="utf-8").replace(
        'data_dir = "data"', f'data_dir = "{tmp_path / "data"}"',
    ).replace(
        "horizons_s = [3600.0,21600.0,86400.0]",
        "horizons_s = [17.0,31.0]",
    )
    cfg = tmp_path / "config.toml"
    cfg.write_text(config_text, encoding="utf-8")

    monkeypatch.setattr(canonical_mod, "CanonicalResolver", CapturingResolver)
    monkeypatch.setattr(counterfactual_mod, "ForwardReturnTracker", CapturingTracker)
    monkeypatch.setattr(strategy_mod, "ClimbingStrategy", FakeStrategy)
    monkeypatch.setattr(main_mod, "supervise", idle_supervise)
    monkeypatch.setenv("MEMEBOT_HELIUS_RPC_URL", "http://127.0.0.1:1")

    stop = asyncio.Event()
    stop.set()
    await run(cfg, stop=stop)

    assert resolver_horizons == [expected_horizons]
    assert tracker_horizons == [expected_horizons]
    assert resolver_horizons[0] is tracker_horizons[0]


def test_all_entry_enabled_strategy_callers_supply_resolver():
    import ast
    import subprocess

    repository = Path(__file__).parents[1]
    required_callers = {
        ("src/memebot/main.py", "run"),
        (
            "tests/test_e2e_climbing.py",
            "test_e2e_climbing_buys_then_ladder_sells",
        ),
        (
            "tests/test_e2e_climbing.py",
            "test_incident_shaped_low_score_skip_emits_watch_without_broker_or_trades",
        ),
        (
            "tests/test_e2e_p2.py",
            "test_e2e_p2_strategy_scores_smart_money_from_preseeded_early_buyers",
        ),
        ("tests/test_strategy.py", "_make"),
        ("tests/test_strategy.py", "_make_with_exits"),
        ("tests/test_strategy.py", "_make_with_exits2"),
        (
            "tests/test_strategy.py",
            "test_feature_eviction_rewarms_before_decision_and_never_mutates_trade_state",
        ),
        (
            "tests/test_strategy.py",
            "test_evicted_terminal_feature_cache_cannot_revive_scoring_or_trading",
        ),
    }
    optional_callers = {
        (
            "tests/test_e2e_climbing.py",
            "test_watch_only_high_score_never_enters_but_existing_legacy_position_still_exits",
        ),
        (
            "tests/test_strategy.py",
            "test_strategy_accepts_optional_resolver_injection_seam",
        ),
    }
    observed = []
    omitted = []
    violations = []
    dynamic_keywords = []

    tracked = sorted(
        filter(
            None,
            subprocess.run(
                ["git", "ls-files", "-z", "--", "*.py"],
                cwd=repository,
                check=True,
                capture_output=True,
            ).stdout.decode().split("\0"),
        )
    )
    assert tracked

    for relative_path in tracked:
        tree = ast.parse((repository / relative_path).read_text())
        path_parts = Path(relative_path).parts
        memebot_relative_level = (
            len(path_parts) - 2
            if len(path_parts) >= 3
            and path_parts[:2] == ("src", "memebot")
            else None
        )
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }

        def lexical_scope(node):
            parent = parents.get(node)
            while parent is not None:
                if isinstance(
                    parent,
                    (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda),
                ):
                    return parent
                parent = parents.get(parent)
            return tree

        def syntactic_scope(node):
            parent = parents.get(node)
            while parent is not None:
                if isinstance(
                    parent,
                    (
                        ast.FunctionDef,
                        ast.AsyncFunctionDef,
                        ast.ClassDef,
                        ast.Lambda,
                    ),
                ):
                    return parent
                parent = parents.get(parent)
            return tree

        def location(node):
            scope = lexical_scope(node)
            return (
                relative_path,
                getattr(scope, "name", "<module>"),
                node.lineno,
            )

        def static_string(node):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                return node.value
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
                left = static_string(node.left)
                right = static_string(node.right)
                if left is not None and right is not None:
                    return left + right
            if isinstance(node, ast.JoinedStr):
                parts = []
                for value in node.values:
                    if isinstance(value, ast.FormattedValue):
                        if value.conversion != -1 or value.format_spec is not None:
                            return None
                        part = static_string(value.value)
                    else:
                        part = static_string(value)
                    if part is None:
                        return None
                    parts.append(part)
                return "".join(parts)
            return None

        canonical_imports = []
        reflective_helpers = {"getattr", "globals", "locals"}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                canonical_module = (
                    node.level == 0 and node.module == "memebot.strategy"
                    or memebot_relative_level is not None
                    and node.level == memebot_relative_level
                    and node.module == "strategy"
                )
                for alias in node.names:
                    bound_name = alias.asname or alias.name
                    if (
                        node.level == 0
                        and node.module == "builtins"
                        and alias.name in reflective_helpers
                        and alias.asname is not None
                    ):
                        violations.append(
                            (*location(node), "reflective helper import alias")
                        )
                    if canonical_module and alias.name == "*":
                        violations.append(
                            (*location(node), "wildcard constructor import")
                        )
                    canonical_constructor = (
                        canonical_module
                        and alias.name == "ClimbingStrategy"
                    )
                    if canonical_constructor and alias.asname is None:
                        canonical_imports.append(node)
                    elif canonical_constructor or bound_name == "ClimbingStrategy":
                        violations.append(
                            (*location(node), "noncanonical constructor import")
                        )
                continue
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bound_name = (
                        alias.asname
                        if alias.asname is not None
                        else alias.name.split(".", maxsplit=1)[0]
                    )
                    if (
                        bound_name == "ClimbingStrategy"
                        or alias.name == "memebot.strategy"
                    ):
                        violations.append(
                            (*location(node), "noncanonical constructor import")
                        )
                continue
            if (
                isinstance(node, ast.Name)
                and node.id == "ClimbingStrategy"
                and not isinstance(node.ctx, ast.Load)
                and not isinstance(syntactic_scope(node), ast.ClassDef)
            ):
                violations.append(
                    (*location(node), "constructor name rebound")
                )
            elif isinstance(node, ast.arg) and node.arg == "ClimbingStrategy":
                violations.append(
                    (*location(node), "constructor parameter shadows import")
                )
            elif (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                and node.name == "ClimbingStrategy"
                and not (
                    relative_path == "src/memebot/strategy.py"
                    and isinstance(node, ast.ClassDef)
                    and lexical_scope(node) is tree
                )
            ):
                violations.append(
                    (*location(node), "constructor definition shadows import")
                )
            elif (
                isinstance(node, ast.ExceptHandler)
                and node.name == "ClimbingStrategy"
            ):
                violations.append(
                    (*location(node), "constructor exception alias shadows import")
                )
            elif isinstance(node, (ast.Global, ast.Nonlocal)) and (
                "ClimbingStrategy" in node.names
            ):
                violations.append(
                    (*location(node), "constructor declaration is unauditable")
                )

        inspect_module_imports = []
        inspect_signature_imports = []
        inspect_shadows = {"inspect": [], "signature": []}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bound_name = alias.asname or alias.name.split(".", maxsplit=1)[0]
                    if alias.name == "inspect" and alias.asname is None:
                        inspect_module_imports.append(node)
                    elif bound_name in inspect_shadows:
                        inspect_shadows[bound_name].append(node)
                continue
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    bound_name = alias.asname or alias.name
                    if (
                        node.level == 0
                        and node.module == "inspect"
                        and alias.name == "signature"
                        and alias.asname is None
                    ):
                        inspect_signature_imports.append(node)
                    elif bound_name in inspect_shadows:
                        inspect_shadows[bound_name].append(node)
                continue
            shadowed_names = []
            if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
                shadowed_names.append(node.id)
            elif isinstance(node, ast.arg):
                shadowed_names.append(node.arg)
            elif isinstance(
                node,
                (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
            ):
                shadowed_names.append(node.name)
            elif isinstance(node, ast.ExceptHandler) and node.name is not None:
                shadowed_names.append(node.name)
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "signature"
                and isinstance(node.value, ast.Name)
                and node.value.id == "inspect"
                and not isinstance(node.ctx, ast.Load)
            ):
                inspect_shadows["inspect"].append(node)
            elif (
                isinstance(node, ast.Call)
                and (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "setattr"
                    or isinstance(node.func, ast.Attribute)
                    and node.func.attr == "setattr"
                )
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == "inspect"
                and static_string(node.args[1]) == "signature"
            ):
                inspect_shadows["inspect"].append(node)
            for name in shadowed_names:
                if name in inspect_shadows:
                    inspect_shadows[name].append(node)

        def visible_scopes(node):
            visible_scopes = {tree}
            scope = lexical_scope(node)
            while scope is not tree:
                visible_scopes.add(scope)
                scope = lexical_scope(scope)
            return visible_scopes

        def has_visible_import(node):
            scopes = visible_scopes(node)
            return any(
                canonical_import.lineno <= node.lineno
                and lexical_scope(canonical_import) in scopes
                for canonical_import in canonical_imports
            )

        def has_genuine_inspect_import(node, imports, name):
            scopes = visible_scopes(node)
            return any(
                imported.lineno <= node.lineno
                and lexical_scope(imported) in scopes
                for imported in imports
            ) and not any(
                lexical_scope(shadow) in scopes
                for shadow in inspect_shadows[name]
            )

        helper_shadows = {name: [] for name in reflective_helpers}
        builtins_module_imports = [
            (
                lexical_scope(node),
                alias.asname or alias.name,
                node,
            )
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
            if alias.name == "builtins"
        ]
        memebot_package_imports = [
            (
                lexical_scope(node),
                alias.asname or alias.name,
                node,
            )
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
            if alias.name == "memebot"
        ]
        builtins_module_shadows = {
            bound_name: []
            for _scope, bound_name, _node in builtins_module_imports
        }
        memebot_package_shadows = {
            bound_name: []
            for _scope, bound_name, _node in memebot_package_imports
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bound_name = alias.asname or alias.name.split(".", maxsplit=1)[0]
                    if alias.name != "builtins":
                        if bound_name in builtins_module_shadows:
                            builtins_module_shadows[bound_name].append(node)
                        if bound_name in helper_shadows:
                            helper_shadows[bound_name].append(node)
                    if (
                        alias.name != "memebot"
                        and bound_name in memebot_package_shadows
                    ):
                        memebot_package_shadows[bound_name].append(node)
                continue
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    bound_name = alias.asname or alias.name
                    genuine_helper = (
                        node.level == 0
                        and node.module == "builtins"
                        and alias.name in reflective_helpers
                        and alias.asname is None
                    )
                    if bound_name in helper_shadows and not genuine_helper:
                        helper_shadows[bound_name].append(node)
                    if bound_name in builtins_module_shadows:
                        builtins_module_shadows[bound_name].append(node)
                    if bound_name in memebot_package_shadows:
                        memebot_package_shadows[bound_name].append(node)
                continue
            names = []
            if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
                names.append(node.id)
            elif isinstance(node, ast.arg):
                names.append(node.arg)
            elif isinstance(
                node,
                (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
            ):
                names.append(node.name)
            elif isinstance(node, ast.ExceptHandler) and node.name is not None:
                names.append(node.name)
            for name in names:
                if name in helper_shadows:
                    helper_shadows[name].append(node)
                if name in builtins_module_shadows:
                    builtins_module_shadows[name].append(node)
                if name in memebot_package_shadows:
                    memebot_package_shadows[name].append(node)

        def real_builtin_helper(node):
            return (
                isinstance(node, ast.Name)
                and node.id in reflective_helpers
                and not any(
                    lexical_scope(shadow) in visible_scopes(node)
                    for shadow in helper_shadows[node.id]
                )
            )

        conditional_binding_parents = (
            ast.If,
            ast.For,
            ast.AsyncFor,
            ast.While,
            ast.Try,
            getattr(ast, "TryStar", ast.Try),
            ast.With,
            ast.AsyncWith,
            ast.Match,
            ast.ListComp,
            ast.SetComp,
            ast.DictComp,
            ast.GeneratorExp,
            ast.comprehension,
        )

        def is_conditional_binding(node):
            scope = lexical_scope(node)
            parent = parents.get(node)
            while parent is not None and parent is not scope:
                if isinstance(parent, conditional_binding_parents):
                    return True
                parent = parents.get(parent)
            return False

        def real_ordered_module(node, module_imports, module_shadows):
            if not isinstance(node, ast.Name):
                return False
            reference_position = (node.lineno, node.col_offset)
            scope = lexical_scope(node)
            while True:
                bindings = [
                    ((imported.lineno, imported.col_offset), True)
                    for import_scope, bound_name, imported in module_imports
                    if import_scope is scope
                    and bound_name == node.id
                    and (imported.lineno, imported.col_offset)
                    <= reference_position
                ]
                bindings.extend(
                    ((shadow.lineno, shadow.col_offset), False)
                    for shadow in module_shadows.get(node.id, [])
                    if lexical_scope(shadow) is scope
                    and not is_conditional_binding(shadow)
                    and (shadow.lineno, shadow.col_offset)
                    <= reference_position
                )
                if bindings:
                    return max(bindings)[1]
                if scope is tree:
                    return False
                scope = lexical_scope(scope)

        def real_builtins_module(node):
            return real_ordered_module(
                node,
                builtins_module_imports,
                builtins_module_shadows,
            )

        def real_memebot_package(node):
            return real_ordered_module(
                node,
                memebot_package_imports,
                memebot_package_shadows,
            )

        def reflective_lookup(call):
            if not isinstance(call, ast.Call):
                return False, None
            if (
                isinstance(call.func, ast.Name)
                and call.func.id == "getattr"
                or isinstance(call.func, ast.Attribute)
                and call.func.attr == "getattr"
            ) and len(call.args) >= 2:
                return True, static_string(call.args[1])
            if (
                isinstance(call.func, ast.Attribute)
                and call.func.attr == "get"
                and call.args
            ):
                return True, static_string(call.args[0])
            return False, None

        strategy_module_bindings = {
            (lexical_scope(node), alias.asname or alias.name)
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
            if alias.name == "strategy"
            and (
                node.level == 0
                and node.module == "memebot"
                or memebot_relative_level is not None
                and node.level == memebot_relative_level
                and node.module is None
            )
        } | {
            (
                lexical_scope(node),
                alias.asname or alias.name.split(".", maxsplit=1)[0],
            )
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
            if alias.name == "memebot.strategy"
        }

        def visible_strategy_module(node):
            return any(
                (scope, node.id) in strategy_module_bindings
                for scope in visible_scopes(node)
            )

        def canonical_module_expression(node, reference):
            if isinstance(node, ast.Name):
                return any(
                    (scope, node.id) in strategy_module_bindings
                    for scope in visible_scopes(reference)
                )
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "strategy"
                and real_memebot_package(node.value)
            ):
                return True
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "__dict__"
            ):
                return canonical_module_expression(node.value, reference)
            if isinstance(node, ast.Call):
                direct_import = (
                    isinstance(node.func, ast.Name)
                    and node.func.id in {"__import__", "import_module"}
                    or isinstance(node.func, ast.Attribute)
                    and node.func.attr == "import_module"
                )
                if direct_import and node.args:
                    return static_string(node.args[0]) == "memebot.strategy"
                if (
                    (
                        isinstance(node.func, ast.Name)
                        and node.func.id in {"globals", "locals"}
                        or isinstance(node.func, ast.Attribute)
                        and node.func.attr in {"globals", "locals"}
                        and real_builtins_module(node.func.value)
                    )
                    and not node.args
                    and not node.keywords
                ):
                    return has_visible_import(reference)
            return False

        def canonical_reflective_source(call):
            if (
                isinstance(call.func, ast.Name)
                and call.func.id == "getattr"
                or isinstance(call.func, ast.Attribute)
                and call.func.attr == "getattr"
            ):
                return canonical_module_expression(call.args[0], call)
            return canonical_module_expression(call.func.value, call)

        for node in ast.walk(tree):
            parent = parents.get(node)
            if (
                isinstance(node, ast.Name)
                and isinstance(node.ctx, ast.Load)
                and real_memebot_package(node)
                and not (
                    isinstance(parent, ast.Attribute)
                    and parent.value is node
                    or isinstance(parent, ast.Compare)
                )
            ):
                violations.append(
                    (*location(node), "canonical package reference escapes")
                )
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "strategy"
                and real_memebot_package(node.value)
            ):
                audited_monkeypatch = (
                    isinstance(parent, ast.Call)
                    and len(parent.args) >= 3
                    and parent.args[0] is node
                    and isinstance(parent.func, ast.Attribute)
                    and parent.func.attr == "setattr"
                    and isinstance(parent.func.value, ast.Name)
                    and parent.func.value.id == "monkeypatch"
                    and static_string(parent.args[1]) == "ClimbingStrategy"
                )
                benign_attribute = (
                    isinstance(parent, ast.Attribute)
                    and parent.value is node
                    and parent.attr != "ClimbingStrategy"
                )
                if not audited_monkeypatch and not benign_attribute:
                    violations.append(
                        (*location(node), "canonical strategy module reference escapes")
                    )
            if (
                isinstance(node, ast.Name)
                and isinstance(node.ctx, ast.Load)
                and visible_strategy_module(node)
            ):
                audited_monkeypatch = (
                    isinstance(parent, ast.Call)
                    and len(parent.args) >= 3
                    and parent.args[0] is node
                    and isinstance(parent.func, ast.Attribute)
                    and parent.func.attr == "setattr"
                    and isinstance(parent.func.value, ast.Name)
                    and parent.func.value.id == "monkeypatch"
                    and static_string(parent.args[1]) == "ClimbingStrategy"
                )
                benign_attribute = (
                    isinstance(parent, ast.Attribute)
                    and parent.value is node
                    and parent.attr != "ClimbingStrategy"
                )
                if not audited_monkeypatch and not benign_attribute:
                    violations.append(
                        (*location(node), "canonical strategy module reference escapes")
                    )
            if (
                real_builtin_helper(node)
                or isinstance(node, ast.Attribute)
                and node.attr in reflective_helpers
                and real_builtins_module(node.value)
            ) and not (
                isinstance(parent, ast.Call)
                and parent.func is node
            ):
                violations.append(
                    (*location(node), "reflective helper reference escapes direct call")
                )
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "ClimbingStrategy"
                and canonical_module_expression(node.value, node)
            ):
                violations.append(
                    (*location(node), "attribute constructor access")
                )
            if isinstance(node, ast.Call):
                reflective_access, reflective_name = reflective_lookup(node)
                if (
                    reflective_access
                    and reflective_name in {None, "ClimbingStrategy"}
                    and canonical_reflective_source(node)
                ):
                    violations.append(
                        (*location(node), "reflective constructor access")
                    )
            if (
                isinstance(node, ast.Subscript)
                and static_string(node.slice) == "ClimbingStrategy"
                and canonical_module_expression(node.value, node)
                and not isinstance(parent, ast.Compare)
            ):
                violations.append(
                    (*location(node), "subscript constructor access")
                )
            if not (
                isinstance(node, ast.Name)
                and node.id == "ClimbingStrategy"
                and isinstance(node.ctx, ast.Load)
            ):
                continue
            if not has_visible_import(node):
                violations.append(
                    (*location(node), "unresolved constructor reference")
                )
                continue
            if isinstance(parent, ast.Call) and parent.func is node:
                caller = location(parent)[:2]
                observed.append(caller)
                if any(keyword.arg is None for keyword in parent.keywords):
                    dynamic_keywords.append(caller)
                if (
                    caller in required_callers
                    and "canonical_resolver"
                    not in {keyword.arg for keyword in parent.keywords}
                ):
                    omitted.append(caller)
                continue
            genuine_signature = (
                isinstance(parent, ast.Call)
                and len(parent.args) == 1
                and parent.args[0] is node
                and not parent.keywords
                and (
                    isinstance(parent.func, ast.Attribute)
                    and parent.func.attr == "signature"
                    and isinstance(parent.func.value, ast.Name)
                    and parent.func.value.id == "inspect"
                    and has_genuine_inspect_import(
                        node,
                        inspect_module_imports,
                        "inspect",
                    )
                    or isinstance(parent.func, ast.Name)
                    and parent.func.id == "signature"
                    and has_genuine_inspect_import(
                        node,
                        inspect_signature_imports,
                        "signature",
                    )
                )
            )
            if not genuine_signature:
                violations.append(
                    (*location(node), "constructor reference escapes direct call")
                )

    assert violations == [], f"constructor policy violations: {violations}"
    assert dynamic_keywords == [], (
        f"dynamic strategy constructor keywords are not auditable: {dynamic_keywords}"
    )
    assert omitted == [], f"entry-enabled resolver keywords omitted: {omitted}"
    assert sorted(observed) == sorted(required_callers | optional_callers), (
        "strategy constructor census changed: "
        f"required={sorted(required_callers)}, optional={sorted(optional_callers)}, "
        f"observed={sorted(observed)}"
    )


async def test_main_passes_smart_money_config_to_strategy(tmp_path, monkeypatch):
    from memebot import strategy as strategy_mod

    captured = []
    recover_calls = []
    background_recover_calls = []
    ready = {"sent": False}

    class FakeStrategy:
        def __init__(self, *args, **kwargs):
            captured.append(kwargs)

        def reconcile(self, *, runtime_causal_floor, max_open_positions):
            return ()

        def recover_pending_scores(self):
            recover_calls.append(1)
            return 0

        @property
        def recovery_pending(self):
            return True

        async def continue_pending_score_recovery(self, stop):
            assert ready["sent"] is True
            background_recover_calls.append(1)
            await stop.wait()

        async def run(self, stop):
            await stop.wait()

    monkeypatch.setattr(strategy_mod, "ClimbingStrategy", FakeStrategy)
    monkeypatch.setattr(
        "memebot.main.sd_notify",
        lambda message: ready.__setitem__("sent", True) if message == "READY=1" else None,
    )
    monkeypatch.setenv("MEMEBOT_HELIUS_RPC_URL", f"http://127.0.0.1:{_closed_ephemeral_port()}")
    cfg = tmp_path / "config.toml"
    unroutable_port = _closed_ephemeral_port()
    cfg.write_text(
        CONFIG.format(data=tmp_path / "data").removesuffix(WATCH_ONLY_STRATEGY) + f"""
[providers.pumpportal]
ws_url = "ws://127.0.0.1:{unroutable_port}"
stale_after_s = 1
[providers.helius]
rpc_url_env = "MEMEBOT_HELIUS_RPC_URL"
ws_mode = "targeted"
[lifecycle]
climbing_progress_pct = 10.0
stall_progress_pct = 5.0
dead_after_stalled_s = 7200
dead_no_activity_s = 172800
[curvepoller]
interval_s = 0.05
batch_size = 100
max_tracked = 300
[pumpfun]
initial_v_sol = 30.0
initial_v_tokens = 1073000000.0
sellable_supply = 793100000.0
token_decimals = 6
protocol_fee_bps = 95
creator_fee_bps = 30
{{safety_and_telegram}}
[paper]
bankroll_sol = 10.0
[strategy.climbing]
entries_enabled = false
score_threshold = 70.0
position_size_sol = 0.2
max_concurrent_positions = 5
max_entries_per_hour = 10
min_samples = 3
min_age_s = 30.0
[scorer.climbing]
weights_version = "climbing-v1"
w_velocity = 0.40
w_progress = 0.20
w_age = 0.05
w_risk = 0.20
w_smart_money = 0.15
velocity_full_scale_sol_per_s = 0.05
progress_full_scale_pct = 80.0
age_full_scale_s = 600.0
smart_money_quality_full_scale_sol = 5.0
[smart_money]
min_events = 3
min_realized_pnl_sol = 1.0
quality_full_scale_sol = 5.0
[fill]
latency_min_s = 3.0
extra_slippage_bps = 50
priority_fee_sol = 0.0005
solana_base_fee_sol = 0.000005
grade_a_max_impact_pct = 2.0
grade_b_max_impact_pct = 5.0
grade_c_max_impact_pct = 10.0
[counterfactual]
horizons_s = [3600.0, 21600.0, 86400.0]
stale_price_after_s = 300.0
price_history_retention_s = 90000.0
price_history_max_samples_per_mint = 10000
price_history_max_mints = 1000
max_in_memory_pending_observations = 50000
[exits.climbing]
ladder_multiples = [1.5, 2.0]
ladder_fractions = [0.4, 0.3]
time_stop_s = 3600.0
trailing_stop_pct = 25.0
[canonical]
max_feature_mints = 1000
max_open_p3_positions = 37
fill_event_max_age_s = 19.0
reconcile_interval_s = 60.0
""".replace("{safety_and_telegram}", SAFETY_TELEGRAM_SECTIONS), encoding="utf-8")
    stop = asyncio.Event()
    task = asyncio.create_task(run(cfg, stop=stop))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, 10)

    assert captured
    assert captured[0]["smart_money_cfg"] == {
        "min_events": 3,
        "min_realized_pnl_sol": 1.0,
        "quality_full_scale_sol": 5.0,
    }
    assert captured[0]["pending_score_capacity"] == 300
    assert captured[0]["fill_event_max_age_s"] == 19.0
    assert captured[0]["max_open_p3_positions"] == 37
    assert recover_calls == [1]
    assert background_recover_calls == [1]


async def test_run_boots_and_stops_with_p1_config(tmp_path, monkeypatch):
    """With no RPC secret set, the strategy path is skipped but the bot still boots+stops
    cleanly on the real config.toml (P1 sections present)."""
    import asyncio
    from pathlib import Path
    from memebot.main import run

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.toml").write_text(Path(__file__).parent.parent.joinpath("config.toml").read_text())
    monkeypatch.delenv("MEMEBOT_HELIUS_RPC_URL", raising=False)
    monkeypatch.delenv("MEMEBOT_TELEGRAM_TOKEN", raising=False)
    stop = asyncio.Event()
    task = asyncio.create_task(run(Path("config.toml"), stop=stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)   # clean shutdown


def test_p3_main_wiring_is_paper_only():
    import ast

    tree = ast.parse(Path("src/memebot/main.py").read_text(encoding="utf-8"))
    run_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "run"
    )

    broker_imports = [
        alias.name
        for node in ast.walk(run_node)
        if isinstance(node, ast.ImportFrom) and node.module == "memebot.broker"
        for alias in node.names
    ]
    assert broker_imports == ["PaperBroker"]

    broker_assignments = [
        node
        for node in ast.walk(run_node)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "broker"
            for target in node.targets
        )
    ]
    assert len(broker_assignments) == 1
    broker_call = broker_assignments[0].value
    assert isinstance(broker_call, ast.Call)
    assert isinstance(broker_call.func, ast.Name)
    assert broker_call.func.id == "PaperBroker"

    strategy_calls = [
        node
        for node in ast.walk(run_node)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ClimbingStrategy"
    ]
    assert len(strategy_calls) == 1
    broker_keywords = [
        keyword.value
        for keyword in strategy_calls[0].keywords
        if keyword.arg == "broker"
    ]
    assert len(broker_keywords) == 1
    assert isinstance(broker_keywords[0], ast.Name)
    assert broker_keywords[0].id == "broker"

    forbidden_fragments = {
        "keypair",
        "live_broker",
        "livebroker",
        "place_order",
        "private_key",
        "seed_phrase",
        "send_raw_transaction",
        "send_transaction",
        "sign_transaction",
        "signer",
        "submit_order",
        "submit_transaction",
        "wallet",
    }
    composed_symbols = []
    for node in ast.walk(run_node):
        if isinstance(node, ast.Name):
            composed_symbols.append(node.id.casefold())
        elif isinstance(node, ast.Attribute):
            composed_symbols.append(node.attr.casefold())
        elif isinstance(node, ast.alias):
            composed_symbols.extend(
                item.casefold() for item in (node.name, node.asname) if item
            )
        elif isinstance(node, ast.keyword) and node.arg is not None:
            composed_symbols.append(node.arg.casefold())
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            composed_symbols.append(node.value.casefold())
    violations = sorted(
        (fragment, symbol)
        for fragment in forbidden_fragments
        for symbol in composed_symbols
        if fragment in symbol
    )
    assert violations == []
