import ast
import asyncio
import inspect
import json
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import get_type_hints

import pytest

from memebot.canonical_analysis import compute_canonical_metrics
from memebot.counterfactual import ForwardReturnTracker
from memebot.bus import EventBus
from memebot.events import (
    CandidateScored,
    CanonicalObservationStarted,
    CurveProgress,
    LifecycleTransition,
    event_to_dict,
)
from memebot.journal import Journal, JournalReplayGap
from memebot.store import (
    EvidenceIntegrityError,
    open_db,
    record_canonical_observation_outcome,
    record_decision,
    record_outcome,
)


class _EmptyReplayJournal:
    def iter_events(self, *, since_wall, until_wall):
        del since_wall, until_wall
        return iter(())


_EMPTY_REPLAY_JOURNAL = _EmptyReplayJournal()


def test_counterfactual_tracker_fixtures_supply_required_journal_and_bounds():
    tree = ast.parse(Path(__file__).read_text())
    calls = []
    null_journal_functions = []
    for function in (
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ):
        for node in ast.walk(function):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "ForwardReturnTracker"
            ):
                calls.append(node)
                journal = next(
                    keyword.value
                    for keyword in node.keywords
                    if keyword.arg == "journal"
                )
                if isinstance(journal, ast.Constant) and journal.value is None:
                    null_journal_functions.append(function.name)
    required = {
        "journal",
        "horizons",
        "token_decimals",
        "stale_price_after_s",
        "reconcile_interval_s",
        "price_history_retention_s",
        "price_history_max_samples_per_mint",
        "price_history_max_mints",
        "max_in_memory_pending_observations",
    }
    omitted = []
    for call in calls:
        keywords = {keyword.arg for keyword in call.keywords}
        missing = required - keywords
        if missing:
            omitted.append((call.lineno, sorted(missing)))

    assert len(calls) == 43
    assert omitted == []
    assert null_journal_functions == [
        "test_counterfactual_overflow_replays_journal",
        "test_horizon_terminal_dead_stale_and_graduated_contract",
    ]


def test_all_tracker_callers_supply_required_journal_and_bounds():
    import subprocess

    repository = Path(__file__).parents[1]
    required = {
        "journal",
        "horizons",
        "token_decimals",
        "stale_price_after_s",
        "reconcile_interval_s",
        "price_history_retention_s",
        "price_history_max_samples_per_mint",
        "price_history_max_mints",
        "max_in_memory_pending_observations",
    }
    callers = []
    omitted = []
    unsupported = []

    def dotted_name(node):
        parts = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if not isinstance(node, ast.Name):
            return None
        return ".".join((node.id, *reversed(parts)))

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
        constructor_names = set()
        module_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                canonical_module = (
                    node.module == "memebot.counterfactual"
                    or node.level == 1 and node.module == "counterfactual"
                )
                if canonical_module and any(
                    alias.name == "*" for alias in node.names
                ):
                    unsupported.append(
                        (relative_path, "<module>", node.lineno, "wildcard import")
                    )
                constructor_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "ForwardReturnTracker"
                )
                if node.module == "memebot":
                    module_names.update(
                        alias.asname or alias.name
                        for alias in node.names
                        if alias.name == "counterfactual"
                    )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "memebot.counterfactual":
                        module_names.add(alias.asname or alias.name)

        def is_constructor_reference(node):
            if isinstance(node, ast.Name):
                return (
                    node.id == "ForwardReturnTracker"
                    or node.id in constructor_names
                )
            if isinstance(node, ast.Attribute):
                return node.attr == "ForwardReturnTracker"
            if isinstance(node, ast.Call):
                return (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "getattr"
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Constant)
                    and node.args[1].value == "ForwardReturnTracker"
                )
            return (
                isinstance(node, ast.Subscript)
                and isinstance(node.slice, ast.Constant)
                and node.slice.value == "ForwardReturnTracker"
            )

        def is_module_reference(node):
            return dotted_name(node) in module_names

        changed = True
        while changed:
            changed = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    targets = node.targets
                    value = node.value
                elif isinstance(node, ast.AnnAssign):
                    targets = [node.target]
                    value = node.value
                elif isinstance(node, ast.NamedExpr):
                    targets = [node.target]
                    value = node.value
                else:
                    continue
                if value is None:
                    continue
                aliases = (
                    constructor_names if is_constructor_reference(value)
                    else module_names if is_module_reference(value)
                    else None
                )
                if aliases is None:
                    continue
                for target in targets:
                    if isinstance(target, ast.Name) and target.id not in aliases:
                        aliases.add(target.id)
                        changed = True

        def location(node):
            function = parents.get(node)
            while function is not None and not isinstance(
                function, (ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                function = parents.get(function)
            return (
                relative_path,
                function.name if function is not None else "<module>",
                node.lineno,
            )

        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and is_constructor_reference(node.func):
                call_location = location(node)
                callers.append(call_location)
                present = {keyword.arg for keyword in node.keywords}
                missing = required - present
                if missing or None in present:
                    omitted.append((*call_location, tuple(sorted(missing))))
                continue
            if not is_constructor_reference(node):
                if is_module_reference(node):
                    parent = parents.get(node)
                    if (
                        isinstance(parent, (ast.Assign, ast.AnnAssign, ast.NamedExpr))
                        and parent.value is node
                    ):
                        targets = (
                            parent.targets if isinstance(parent, ast.Assign)
                            else [parent.target]
                        )
                        if not all(isinstance(target, ast.Name) for target in targets):
                            unsupported.append(
                                (*location(node), type(parent).__name__)
                            )
                continue
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                continue
            parent = parents.get(node)
            if isinstance(parent, ast.Call) and parent.func is node:
                continue
            if (
                isinstance(parent, ast.Call)
                and dotted_name(parent.func) == "inspect.signature"
                and node in parent.args
            ):
                continue
            if (
                isinstance(parent, ast.Attribute)
                and parent.attr == "__init__"
                and isinstance(parents.get(parent), ast.Call)
                and dotted_name(parents[parent].func) == "get_type_hints"
                and parent in parents[parent].args
            ):
                continue
            if (
                isinstance(parent, (ast.Assign, ast.AnnAssign, ast.NamedExpr))
                and parent.value is node
            ):
                targets = (
                    parent.targets if isinstance(parent, ast.Assign)
                    else [parent.target]
                )
                if all(isinstance(target, ast.Name) for target in targets):
                    continue
            unsupported.append((*location(node), type(parent).__name__))

    assert unsupported == [], f"unsupported tracker references: {unsupported}"
    assert omitted == [], f"constructor keywords omitted: {omitted}"
    assert len(callers) == 46, f"tracker constructor census changed: {callers}"
    assert any(path == "src/memebot/main.py" for path, _, _ in callers)


def test_tracker_rejects_removed_optional_journal_seam():
    signature = inspect.signature(ForwardReturnTracker)
    required = (
        "journal",
        "horizons",
        "token_decimals",
        "stale_price_after_s",
        "reconcile_interval_s",
        "price_history_retention_s",
        "price_history_max_samples_per_mint",
        "price_history_max_mints",
        "max_in_memory_pending_observations",
    )

    assert [
        name
        for name in required
        if signature.parameters[name].default is not inspect.Parameter.empty
    ] == []
    assert list(signature.parameters) == [
        "bus",
        "conn",
        *required,
        "clock",
    ]
    assert all(
        signature.parameters[name].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for name in ("bus", "conn")
    )
    assert all(
        signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        for name in (*required, "clock")
    )
    assert signature.parameters["clock"].default is time.time
    assert get_type_hints(ForwardReturnTracker.__init__) == {
        "journal": Journal,
        "horizons": Sequence[float],
        "token_decimals": int,
        "stale_price_after_s": float,
        "reconcile_interval_s": float,
        "price_history_retention_s": float,
        "price_history_max_samples_per_mint": int,
        "price_history_max_mints": int,
        "max_in_memory_pending_observations": int,
        "clock": Callable[[], float],
        "return": type(None),
    }


def _price_event(mint, t, vsol, vtok=900_000_000_000_000):
    return CurveProgress(t_wall=t, t_mono=t, mint=mint, progress_pct=20.0,
                         virtual_sol_reserves=vsol, virtual_token_reserves=vtok,
                         real_sol_reserves=1_000_000_000, real_token_reserves=0)


def _register(trk, *, mint, decision_id, t, price):
    trk.register(CandidateScored(t_wall=t, t_mono=t, mint=mint, decision_id=decision_id,
                                 segment="CLIMBING", score=75.0, spot_price_sol=price))


def test_forward_return_recorded_after_horizon(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))     # price p0
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    trk.observe_price(_price_event("M", t=3600.0, vsol=60_000_000_000))  # 2x by +1h
    assert trk.check(now=3600.0) == 1
    row = conn.execute("SELECT ref_kind, ref_id, detail_json FROM outcomes").fetchone()
    assert row["ref_kind"] == "candidate" and row["ref_id"] == 5
    d = json.loads(row["detail_json"])
    assert d["horizon_s"] == 3600.0
    assert d["forward_return_pct"] == pytest.approx(100.0, rel=1e-3)   # +100%


def test_no_write_before_horizon(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    assert trk.check(now=1800.0) == 0                 # only 30 min elapsed


def test_each_horizon_written_once(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0,
        horizons=(3600.0, 21600.0), token_decimals=6,
        stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    trk.observe_price(_price_event("M", t=100000.0, vsol=45_000_000_000))
    assert trk.check(now=100000.0) == 2               # both horizons flush
    assert trk.check(now=100000.0) == 0               # idempotent — no duplicates


def _transition(mint, t, to_state, from_state="CLIMBING"):
    return LifecycleTransition(t_wall=t, t_mono=t, mint=mint,
                               from_state=from_state, to_state=to_state)


def test_dead_token_records_total_loss(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))   # price0
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    trk.on_transition(_transition("M", t=10.0, to_state="DEAD"))
    assert trk.check(now=3600.0) == 1
    row = conn.execute("SELECT detail_json FROM outcomes").fetchone()
    d = json.loads(row["detail_json"])
    assert d["forward_return_pct"] == pytest.approx(-100.0, rel=1e-3)
    assert d["terminal"] == "dead"


def test_graduated_token_records_at_last_price_flagged(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))   # price0
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    trk.observe_price(_price_event("M", t=5.0, vsol=60_000_000_000))   # 2x — last curve price
    trk.on_transition(_transition("M", t=10.0, to_state="GRADUATED"))
    assert trk.check(now=3600.0) == 1
    row = conn.execute("SELECT detail_json FROM outcomes").fetchone()
    d = json.loads(row["detail_json"])
    assert d["forward_return_pct"] == pytest.approx(100.0, rel=1e-3)
    assert d["terminal"] == "graduated"


def test_prices_pruned_for_terminal_mint_after_flush(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    trk.on_transition(_transition("M", t=10.0, to_state="DEAD"))
    assert trk.check(now=3600.0) == 1                 # flushes the only pending horizon
    assert "M" not in trk._prices
    assert "M" not in trk._terminal


def test_stale_price_treated_as_dead(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))   # price0 at t_wall=0
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    # No further observe_price -- mint falls off polling (top-300 dropout).
    # horizon elapsed (3901 >= 3600) AND price is stale (3901 - 0 = 3901 > 300).
    assert trk.check(now=3600.0 + 301) == 1
    row = conn.execute("SELECT detail_json FROM outcomes").fetchone()
    d = json.loads(row["detail_json"])
    assert d["forward_return_pct"] == pytest.approx(-100.0, rel=1e-3)
    assert d["terminal"] == "stale"


def test_fresh_price_not_stale(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))   # price0
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    trk.observe_price(_price_event("M", t=3600.0, vsol=60_000_000_000))  # 2x, fresh at check time
    assert trk.check(now=3600.0) == 1                 # now - price_ts == 0, well within bound
    row = conn.execute("SELECT detail_json FROM outcomes").fetchone()
    d = json.loads(row["detail_json"])
    assert d["forward_return_pct"] == pytest.approx(100.0, rel=1e-3)
    assert d["terminal"] is None


def test_stale_mint_pruned(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))
    assert trk.check(now=3600.0 + 301) == 1            # stale flush, as above
    assert "M" not in trk._prices
    assert "M" not in trk._price_ts
    assert "M" not in trk._terminal


def test_recovered_stale_mint_later_horizon_not_labeled_stale(tmp_path):
    # A token can go stale at an early horizon (fell off polling) yet later be freshly
    # priced again -- still on the bonding curve, no DEAD/GRADUATED transition. The
    # early horizon is legitimately "stale"/-100%, but the LATER horizon must flush
    # against the recovered price AND drop the stale label (it recovered): otherwise a
    # recovered-winner datapoint is mislabeled terminal="stale", corrupting the
    # categorical field P5 relies on. observe_price must clear a sticky "stale" marker.
    conn = open_db(tmp_path / "t.db")
    now = [0.0]
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: now[0],
        horizons=(3600.0, 21600.0), token_decimals=6,
        stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))   # price0 at t_wall=0
    _register(trk, mint="M", decision_id=5, t=0.0, price=trk.latest_price("M"))

    # 3600 horizon elapsed AND price stale (3901 - 0 = 3901 > 300) -> stale flush, ~-100%.
    now[0] = 3901.0
    assert trk.check(now=now[0]) == 1
    early = json.loads(conn.execute(
        "SELECT detail_json FROM outcomes ORDER BY id").fetchone()["detail_json"])
    assert early["horizon_s"] == 3600.0
    assert early["forward_return_pct"] == pytest.approx(-100.0, rel=1e-3)
    assert early["terminal"] == "stale"

    # Recovery: freshly priced again (2x) at t_wall=21600 -> clears the sticky marker.
    now[0] = 21600.0
    trk.observe_price(_price_event("M", t=21600.0, vsol=60_000_000_000))  # 2x
    assert trk.check(now=now[0]) == 1
    late = json.loads(conn.execute(
        "SELECT detail_json FROM outcomes WHERE json_extract(detail_json,'$.horizon_s')=21600.0"
        ).fetchone()["detail_json"])
    assert late["forward_return_pct"] == pytest.approx(100.0, rel=1e-3)
    assert late["terminal"] is None       # recovered -- must NOT stay tagged "stale"


def test_register_idempotent(tmp_path):
    conn = open_db(tmp_path / "t.db")
    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0, horizons=(3600.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    trk.observe_price(_price_event("M", t=0.0, vsol=30_000_000_000))
    p0 = trk.latest_price("M")
    _register(trk, mint="M", decision_id=5, t=0.0, price=p0)
    _register(trk, mint="M", decision_id=5, t=0.0, price=p0)   # duplicate registration
    trk.observe_price(_price_event("M", t=3600.0, vsol=60_000_000_000))
    assert trk.check(now=3600.0) == 1                  # exactly one outcome, not two
    rows = conn.execute("SELECT * FROM outcomes").fetchall()
    assert len(rows) == 1


def test_resume_from_ledger_reregisters_only_unwritten_horizons(tmp_path):
    conn = open_db(tmp_path / "t.db")
    p0 = 1.5e-6
    did = record_decision(conn, at=0.0, mint="M", segment="CLIMBING", action="BUY",
                          score=75.0, feature_vector={"spot_price_sol": p0},
                          config_hash="cfg")
    # Horizon 3600 already recorded in the ledger (e.g. from a prior process run).
    record_outcome(conn, at=3600.0, ref_kind="candidate", ref_id=did, pnl_sol=0.0,
                   detail={"horizon_s": 3600.0, "forward_return_pct": 0.0,
                           "price0": p0, "price_now": p0, "terminal": None})

    trk = ForwardReturnTracker(
        None, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: 0.0,
        horizons=(3600.0, 21600.0), token_decimals=6,
        stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    count = trk.resume_from_ledger(conn)
    assert count == 1

    # Observe a price and advance past BOTH horizons — only the still-pending
    # 21600s horizon should produce a new write (3600 was already in the ledger).
    trk.observe_price(_price_event("M", t=21600.0, vsol=45_000_000_000))
    wrote = trk.check(now=21600.0)
    assert wrote == 1
    rows = conn.execute(
        "SELECT detail_json FROM outcomes WHERE ref_id=? ORDER BY id", (did,)).fetchall()
    assert len(rows) == 2                              # original + the one new write
    horizons_written = sorted(json.loads(r["detail_json"])["horizon_s"] for r in rows)
    assert horizons_written == [3600.0, 21600.0]


@pytest.mark.asyncio
async def test_candidate_and_observation_same_numeric_id_do_not_collide(monkeypatch):
    recorded = []

    def capture_outcome(_conn, **values):
        recorded.append(values)
        return len(recorded)

    def capture_canonical_outcome(_conn, **values):
        recorded.append({
            **values,
            "ref_kind": "canonical_observation",
            "ref_id": values["observation_id"],
        })
        return len(recorded)

    monkeypatch.setattr("memebot.counterfactual.record_outcome", capture_outcome)
    monkeypatch.setattr(
        "memebot.store.record_canonical_observation_outcome",
        capture_canonical_outcome,
    )
    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        lambda _conn, *, horizons, limit_plus_one: [],
    )
    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        lambda _conn, *, observation_id, horizon_s: False,
    )
    now = [0.0]
    bus = EventBus()
    trk = ForwardReturnTracker(
        bus, object(), journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: now[0], horizons=(1.0,),
        token_decimals=6, stale_price_after_s=300.0, reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(trk.run(stop))

    candidate = CandidateScored(
        t_wall=0.0,
        t_mono=0.0,
        mint="CAND",
        decision_id=5,
        segment="CLIMBING",
        score=75.0,
        spot_price_sol=1.0,
    )
    observation = CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=5,
        decision_id=99,
        mint="OBS",
        start_price_sol=2.0,
        price_observed_at=0.0,
    )
    await bus.publish(candidate)
    await bus.publish(observation)
    now[0] = 1.0
    await bus.publish(_price_event("CAND", t=1.0, vsol=60_000_000_000))
    await bus.publish(_price_event("OBS", t=1.0, vsol=60_000_000_000))

    async def wait_for_outcomes():
        while len(recorded) < 2:
            await asyncio.sleep(0)

    try:
        await asyncio.wait_for(wait_for_outcomes(), timeout=1.0)
    finally:
        stop.set()
        await bus.publish(_price_event("WAKE", t=1.0, vsol=0))
        await asyncio.wait_for(task, timeout=1.0)

    assert [(row["ref_kind"], row["ref_id"]) for row in recorded] == [
        ("candidate", 5),
        ("canonical_observation", 5),
    ]


def test_counterfactual_history_caps_bound_memory(tmp_path, monkeypatch):
    class ReplayJournal:
        def __init__(self, *events):
            self.items = [event_to_dict(event) for event in events]

        def iter_events(self, *, since_wall, until_wall):
            yield from (
                item
                for item in self.items
                if since_wall <= item["t_wall"] <= until_wall
            )

    conn = open_db(tmp_path / "t.db")
    now = [0.0]
    bounded = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: now[0],
        horizons=(10.0, 20.0),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=0.5,
        price_history_max_samples_per_mint=1,
        price_history_max_mints=2,
        max_in_memory_pending_observations=2,
    )
    _register(bounded, mint="A", decision_id=10, t=0.0, price=1.0)
    _register(bounded, mint="A", decision_id=11, t=20.0, price=1.0)
    _register(bounded, mint="EXCESS", decision_id=12, t=0.0, price=1.0)
    assert [(item.mint, item.ref_id) for item in bounded._candidates] == [
        ("A", 10),
        ("A", 11),
    ]

    # A retention-old sample between the absolute horizons (10, 20, 30, 40)
    # remains: it is not older than *every* pending horizon.
    bounded.observe_price(_price_event("A", t=14.0, vsol=25_000_000_000))
    bounded.observe_price(_price_event("A", t=15.0, vsol=30_000_000_000))
    bounded._journal = ReplayJournal(
        _price_event("A", t=14.0, vsol=25_000_000_000),
    )
    assert bounded._price_overflow_intervals == {"A": (14.0, 14.0)}
    now[0] = 16.0
    bounded._prune_price_history(now[0])
    assert [sample[0] for sample in bounded._price_history["A"]] == [15.0]

    # Flushing only the first horizon retains the same observation, its later
    # horizon, the staggered observation, and the complete mint cache bundle.
    assert bounded.check(now=16.0) == 1
    assert [item.pending for item in bounded._candidates] == [
        {20.0},
        {10.0, 20.0},
    ]
    assert "A" in bounded._price_history
    assert "A" in bounded._prices
    assert "A" in bounded._price_ts
    assert "A" in bounded._registered_mints
    assert "A" in bounded._price_overflow_intervals

    # Completing the first same-mint observation still cannot release caches
    # needed by the staggered observation or either of its horizons.
    now[0] = 21.0
    assert bounded.check(now=21.0) == 1
    assert [(item.mint, item.ref_id) for item in bounded._candidates] == [("A", 11)]
    assert "A" in bounded._price_history
    assert "A" in bounded._prices

    # The second observation's early horizon also retains the mint bundle.
    now[0] = 30.0
    assert bounded.check(now=30.0) == 1
    assert len(bounded._candidates) == 1
    assert bounded._candidates[0].pending == {20.0}
    assert "A" in bounded._prices
    assert "A" in bounded._registered_mints

    now[0] = 40.0
    assert bounded.check(now=40.0) == 1
    assert bounded._candidates == []
    assert "A" not in bounded._price_history
    assert "A" not in bounded._prices
    assert "A" not in bounded._price_ts
    assert "A" not in bounded._registered_mints
    assert "A" not in bounded._price_overflow_intervals

    samples = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(100.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=1_000.0,
        price_history_max_samples_per_mint=2,
        price_history_max_mints=2,
        max_in_memory_pending_observations=2,
    )
    _register(samples, mint="SAMPLES", decision_id=20, t=0.0, price=1.0)
    samples.observe_price(_price_event("SAMPLES", t=10.0, vsol=30_000_000_000))
    samples.observe_price(_price_event("SAMPLES", t=20.0, vsol=40_000_000_000))
    samples.observe_price(_price_event("SAMPLES", t=30.0, vsol=50_000_000_000))
    assert samples._price_overflow_intervals == {"SAMPLES": (10.0, 10.0)}
    samples.observe_price(_price_event("SAMPLES", t=15.0, vsol=35_000_000_000))
    samples._journal = ReplayJournal(
        _price_event("SAMPLES", t=10.0, vsol=30_000_000_000),
        _price_event("SAMPLES", t=15.0, vsol=35_000_000_000),
    )
    assert [sample[0] for sample in samples._price_history["SAMPLES"]] == [20.0, 30.0]
    assert samples._price_overflow_intervals == {"SAMPLES": (10.0, 15.0)}
    assert samples.latest_price("SAMPLES") is not None
    assert samples._price_ts["SAMPLES"] == 15.0
    assert "SAMPLES" in samples._registered_mints
    assert samples.check(now=100.0) == 1
    assert "SAMPLES" not in samples._price_history
    assert samples.latest_price("SAMPLES") is None
    assert "SAMPLES" not in samples._price_ts
    assert "SAMPLES" not in samples._registered_mints
    assert "SAMPLES" not in samples._price_overflow_intervals

    # Retention-reclaimed, non-live mint state cannot permanently consume the
    # mint cap. Fresh pre-registration history is retained and blocks overflow.
    mint_cap_now = [0.0]
    mint_cap = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: mint_cap_now[0],
        horizons=(100.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=5.0,
        price_history_max_samples_per_mint=2,
        price_history_max_mints=1,
        max_in_memory_pending_observations=2,
    )
    mint_cap.observe_price(_price_event("OLD", t=0.0, vsol=30_000_000_000))
    mint_cap_now[0] = 10.0
    mint_cap.observe_price(_price_event("FRESH", t=10.0, vsol=40_000_000_000))
    assert tuple(mint_cap._price_history) == ("FRESH",)
    assert tuple(mint_cap._prices) == ("FRESH",)
    assert tuple(mint_cap._price_ts) == ("FRESH",)
    mint_cap.observe_price(_price_event("BLOCKED", t=10.0, vsol=50_000_000_000))
    assert tuple(mint_cap._price_history) == ("FRESH",)

    equality = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(100.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=5.0,
        price_history_max_samples_per_mint=2,
        price_history_max_mints=1,
        max_in_memory_pending_observations=1,
    )
    equality.observe_price(_price_event("EQUAL", t=5.0, vsol=30_000_000_000))
    equality._prune_price_history(now=10.0)
    assert [sample[0] for sample in equality._price_history["EQUAL"]] == [5.0]
    equality._prune_price_history(now=10.1)
    assert "EQUAL" not in equality._price_history
    assert "EQUAL" not in equality._prices
    assert "EQUAL" not in equality._price_ts

    class CountingCandidates(list):
        def __init__(self, values):
            super().__init__(values)
            self.iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    complexity = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0, 20.0),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=100.0,
        price_history_max_samples_per_mint=10,
        price_history_max_mints=3,
        max_in_memory_pending_observations=3,
    )
    _register(complexity, mint="SCAN-A", decision_id=30, t=0.0, price=1.0)
    _register(complexity, mint="SCAN-B", decision_id=31, t=0.0, price=1.0)
    complexity.observe_price(_price_event("SCAN-A", t=1.0, vsol=30_000_000_000))
    complexity.observe_price(_price_event("SCAN-B", t=1.0, vsol=30_000_000_000))
    counted = CountingCandidates(complexity._candidates)
    complexity._candidates = counted
    complexity._prune_price_history(now=1.0)
    assert counted.iterations == 1

    prune_calls = []
    monkeypatch.setattr(
        complexity,
        "_prune_price_history",
        lambda prune_now: prune_calls.append(prune_now),
    )
    complexity.observe_price(
        _price_event("SCAN-A", t=2.0, vsol=40_000_000_000),
    )
    assert prune_calls == []

    resume_conn = open_db(tmp_path / "resume.db")
    durable_ids = [
        record_decision(
            resume_conn,
            at=float(index),
            mint=f"DURABLE-{index}",
            segment="CLIMBING",
            action="BUY",
            score=75.0,
            feature_vector={"spot_price_sol": 1.0},
            config_hash="cfg",
        )
        for index in (1, 2, 3)
    ]
    resumed = ForwardReturnTracker(
        None,
        resume_conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=2,
    )
    assert resumed.resume_from_ledger(resume_conn) == 2
    assert [item.ref_id for item in resumed._candidates] == durable_ids[:2]
    assert [
        row["id"] for row in resume_conn.execute("SELECT id FROM decisions ORDER BY id")
    ] == durable_ids


def test_counterfactual_overflow_replays_journal(tmp_path):
    class ReplayJournal:
        def __init__(self):
            self.items = []
            self.calls = []

        def iter_events(self, *, since_wall, until_wall):
            self.calls.append((since_wall, until_wall))
            yield from self.items

    conn = open_db(tmp_path / "t.db")
    journal = ReplayJournal()
    tracker = ForwardReturnTracker(
        None,
        conn,
        journal=None,
        clock=lambda: 0.0,
        horizons=(10.0, 20.0),
        token_decimals=6,
        stale_price_after_s=100.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=100.0,
        price_history_max_samples_per_mint=1,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    _register(tracker, mint="M", decision_id=1, t=0.0, price=1.0)
    _register(tracker, mint="M", decision_id=2, t=0.0, price=1.0)
    evicted_lower = _price_event("M", t=5.0, vsol=30_000_000_000)
    evicted_upper = _price_event("M", t=6.0, vsol=45_000_000_000)
    retained = _price_event("M", t=7.0, vsol=60_000_000_000)
    tracker.observe_price(evicted_lower)
    tracker.observe_price(evicted_upper)
    tracker.observe_price(retained)
    assert tracker._price_overflow_intervals == {"M": (5.0, 6.0)}

    # Compatibility is private only: until the public constructor seam lands,
    # an overflow without an injected journal fails closed rather than bypassing
    # the authoritative evidence.
    with pytest.raises(RuntimeError, match="journal overflow evidence unavailable"):
        tracker.check(now=10.0)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
    assert tracker._price_overflow_intervals == {"M": (5.0, 6.0)}

    tracker._journal = journal
    unrelated_upper = _price_event("OTHER", t=6.0, vsol=90_000_000_000)
    unrelated_lower = _price_event("OTHER", t=5.0, vsol=75_000_000_000)

    def assert_replay_fails(items):
        journal.items = items
        with pytest.raises(RuntimeError, match="journal overflow evidence unavailable"):
            tracker.check(now=10.0)
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
        assert tracker._price_overflow_intervals == {"M": (5.0, 6.0)}

    assert_replay_fails([
        event_to_dict(evicted_lower),
        event_to_dict(unrelated_upper),
    ])
    assert_replay_fails([
        event_to_dict(unrelated_lower),
        event_to_dict(evicted_upper),
    ])
    assert_replay_fails([
        event_to_dict(evicted_lower),
        JournalReplayGap(
            mint="M", lower_wall=5.5, upper_wall=5.5,
            file_seq=1, line_number=2,
        ),
        event_to_dict(evicted_upper),
    ])
    assert_replay_fails([
        event_to_dict(evicted_lower),
        JournalReplayGap(
            mint=None, lower_wall=5.5, upper_wall=5.5,
            file_seq=1, line_number=2,
        ),
        event_to_dict(evicted_upper),
    ])
    assert_replay_fails([
        event_to_dict(evicted_lower),
        {"kind": "curve_progress"},
        event_to_dict(evicted_upper),
    ])

    journal.items = [
        event_to_dict(evicted_lower),
        JournalReplayGap(
            mint="OTHER", lower_wall=5.5, upper_wall=5.5,
            file_seq=1, line_number=2,
        ),
        event_to_dict(unrelated_upper),
        event_to_dict(evicted_upper),
    ]
    recovered = list(tracker._iter_replayed_overflow_prices("M"))
    expected_prices = [
        (30_000_000_000 / 1_000_000_000) / (900_000_000_000_000 / 1_000_000),
        (45_000_000_000 / 1_000_000_000) / (900_000_000_000_000 / 1_000_000),
    ]
    assert [timestamp for timestamp, _ in recovered] == [5.0, 6.0]
    assert [price for _, price in recovered] == pytest.approx(expected_prices)
    assert tracker.check(now=10.0) == 2
    assert tracker._price_overflow_intervals == {"M": (5.0, 6.0)}
    assert tracker.check(now=20.0) == 2
    assert tracker._price_overflow_intervals == {}
    # Two due candidates share one replay per check, not one replay per candidate.
    assert journal.calls == [(5.0, 6.0)] * 8


def test_horizon_never_uses_post_horizon_price(tmp_path):
    class ReplayJournal:
        def __init__(self, *events):
            self.items = [event_to_dict(event) for event in events]

        def iter_events(self, *, since_wall, until_wall):
            yield from (
                item
                for item in self.items
                if since_wall <= item["t_wall"] <= until_wall
            )

    conn = open_db(tmp_path / "t.db")
    tracker = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0, 20.0),
        token_decimals=6,
        stale_price_after_s=100.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=3,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    price0 = (
        (30_000_000_000 / 1_000_000_000)
        / (900_000_000_000_000 / 1_000_000)
    )
    _register(
        tracker,
        mint="M",
        decision_id=1,
        t=0.0,
        price=price0,
    )
    _register(
        tracker,
        mint="M",
        decision_id=2,
        t=5.0,
        price=price0,
    )
    before_horizon = _price_event("M", t=9.0, vsol=60_000_000_000)
    earlier = _price_event("M", t=8.0, vsol=45_000_000_000)
    between_horizons = _price_event("M", t=15.0, vsol=67_500_000_000)
    at_later_horizon = _price_event("M", t=20.0, vsol=90_000_000_000)
    retained_earlier = _price_event("M", t=18.0, vsol=75_000_000_000)
    tracker.observe_price(before_horizon)
    tracker.observe_price(earlier)
    tracker.observe_price(between_horizons)
    tracker.observe_price(at_later_horizon)
    tracker.observe_price(retained_earlier)
    tracker.observe_price(_price_event("M", t=21.0, vsol=120_000_000_000))
    assert tracker._price_overflow_intervals == {"M": (8.0, 15.0)}
    assert [sample[0] for sample in tracker._price_history["M"]] == [
        18.0,
        20.0,
        21.0,
    ]
    tracker._journal = ReplayJournal(between_horizons, earlier, before_horizon)

    assert tracker.check(now=21.0) == 3
    rows = [
        (row["ref_id"], json.loads(row["detail_json"]))
        for row in conn.execute("SELECT ref_id,detail_json FROM outcomes ORDER BY id")
    ]
    assert [(ref_id, detail["horizon_s"]) for ref_id, detail in rows] == [
        (1, 10.0),
        (1, 20.0),
        (2, 10.0),
    ]
    assert [detail["forward_return_pct"] for _, detail in rows] == pytest.approx(
        [100.0, 200.0, 125.0],
    )
    assert [detail["price_now"] for _, detail in rows] == pytest.approx(
        [2.0 * price0, 3.0 * price0, 2.25 * price0],
    )

    canonical_tracker = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    canonical_tracker.register(CanonicalObservationStarted(
        t_wall=10.0,
        t_mono=10.0,
        observation_id=3,
        decision_id=3,
        mint="CANON",
        start_price_sol=price0,
        price_observed_at=5.0,
    ))
    canonical_tracker.observe_price(
        _price_event("CANON", t=7.0, vsol=45_000_000_000),
    )
    canonical = canonical_tracker._candidates[0]
    assert canonical.price0_at == 5.0
    assert canonical_tracker._select_horizon_price(
        canonical,
        cutoff=20.0,
        recovered=None,
    ) == pytest.approx(1.5 * price0)

    live_tracker = ForwardReturnTracker(
        None,
        conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    live_tracker.register(CandidateScored(
        t_wall=5.0,
        t_mono=1.0,
        mint="LIVE-SOURCE",
        decision_id=4,
        segment="CLIMBING",
        score=75.0,
        spot_price_sol=price0,
    ))
    live_tracker.observe_price(
        _price_event("LIVE-SOURCE", t=3.0, vsol=45_000_000_000),
    )
    live_candidate = live_tracker._candidates[0]
    assert live_candidate.price0_at == 5.0
    assert live_tracker._select_horizon_price(
        live_candidate,
        cutoff=15.0,
        recovered=None,
    ) == pytest.approx(price0)
    assert live_tracker.check(now=15.0) == 1
    live_detail = json.loads(conn.execute(
        "SELECT detail_json FROM outcomes WHERE ref_kind='candidate' AND ref_id=4",
    ).fetchone()["detail_json"])
    assert live_detail["forward_return_pct"] == pytest.approx(0.0)
    assert live_detail["price_now"] == pytest.approx(price0)

    resume_conn = open_db(tmp_path / "resume-source.db")
    decision_id = record_decision(
        resume_conn,
        at=5.0,
        mint="RESUMED-SOURCE",
        segment="CLIMBING",
        action="BUY",
        score=75.0,
        feature_vector={"spot_price_sol": price0},
        config_hash="cfg",
    )
    resumed_tracker = ForwardReturnTracker(
        None,
        resume_conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=100.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    assert resumed_tracker.resume_from_ledger(resume_conn) == 1
    resumed_tracker.observe_price(
        _price_event("RESUMED-SOURCE", t=3.0, vsol=45_000_000_000),
    )
    assert resumed_tracker._candidates[0].price0_at == 5.0
    assert resumed_tracker.check(now=15.0) == 1
    resumed_detail = json.loads(resume_conn.execute(
        "SELECT detail_json FROM outcomes WHERE ref_kind='candidate' AND ref_id=?",
        (decision_id,),
    ).fetchone()["detail_json"])
    assert resumed_detail["forward_return_pct"] == pytest.approx(0.0)
    assert resumed_detail["price_now"] == pytest.approx(price0)

    delayed_conn = open_db(tmp_path / "delayed-source.db")
    delayed_tracker = ForwardReturnTracker(
        None,
        delayed_conn,
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=100.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=11.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    _register(
        delayed_tracker,
        mint="DELAYED",
        decision_id=5,
        t=0.0,
        price=price0,
    )
    delayed_tracker.observe_price(
        _price_event("DELAYED", t=9.0, vsol=60_000_000_000),
    )
    assert delayed_tracker.check(now=21.0) == 1
    delayed_detail = json.loads(
        delayed_conn.execute("SELECT detail_json FROM outcomes").fetchone()[
            "detail_json"
        ],
    )
    assert delayed_detail["forward_return_pct"] == pytest.approx(100.0)
    assert delayed_detail["price_now"] == pytest.approx(2.0 * price0)


def test_horizon_terminal_dead_stale_and_graduated_contract(monkeypatch):
    recorded = []

    def capture_canonical_outcome(_conn, **values):
        recorded.append(values)
        return len(recorded)

    def reject_generic_outcome(_conn, **_values):
        pytest.fail("canonical observations must use the strict outcome writer")

    monkeypatch.setattr(
        "memebot.counterfactual.record_outcome",
        reject_generic_outcome,
    )
    monkeypatch.setattr(
        "memebot.store.record_canonical_observation_outcome",
        capture_canonical_outcome,
    )
    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        lambda _conn, *, observation_id, horizon_s: False,
    )
    price0 = (
        (30_000_000_000 / 1_000_000_000)
        / (900_000_000_000_000 / 1_000_000)
    )

    tracker = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(15.0, 25.0, 35.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    tracker.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=1,
        decision_id=1,
        mint="TERMINAL",
        start_price_sol=price0,
        price_observed_at=0.0,
    ))
    tracker.observe_price(_price_event("TERMINAL", t=8.0, vsol=60_000_000_000))
    tracker.observe_price(_price_event("TERMINAL", t=12.0, vsol=90_000_000_000))
    tracker.observe_price(_price_event("TERMINAL", t=17.0, vsol=120_000_000_000))
    tracker.observe_price(_price_event("TERMINAL", t=22.0, vsol=150_000_000_000))
    tracker.observe_price(_price_event("TERMINAL", t=35.0, vsol=180_000_000_000))
    tracker.on_transition(_transition("TERMINAL", t=30.0, to_state="DEAD"))
    tracker.on_transition(_transition("TERMINAL", t=30.0, to_state="DEAD"))
    tracker.on_transition(_transition("TERMINAL", t=10.0, to_state="DEAD"))
    tracker.on_transition(_transition("TERMINAL", t=18.0, to_state="GRADUATED"))
    tracker.on_transition(_transition("TERMINAL", t=10.0, to_state="DEAD"))
    tracker.on_transition(_transition("TERMINAL", t=18.0, to_state="GRADUATED"))
    assert tracker._terminal_history == {
        "TERMINAL": [
            (10.0, "DEAD"),
            (18.0, "GRADUATED"),
            (30.0, "DEAD"),
        ],
    }

    assert tracker.check(now=35.0) == 3
    assert "TERMINAL" not in tracker._terminal_history
    terminal_rows = recorded[:3]
    assert [row["horizon_s"] for row in terminal_rows] == [15.0, 25.0, 35.0]
    assert [row["price_now"] for row in terminal_rows] == pytest.approx([
        0.0,
        4.0 * price0,
        0.0,
    ])
    assert [row["price_now_observed_at"] for row in terminal_rows] == [
        10.0,
        17.0,
        30.0,
    ]
    assert [row["forward_return_pct"] for row in terminal_rows] == pytest.approx([
        -100.0,
        300.0,
        -100.0,
    ])
    assert [row["terminal"] for row in terminal_rows] == [
        "DEAD",
        "GRADUATED",
        "DEAD",
    ]
    assert all(row["unavailable_reason"] == "" for row in terminal_rows)

    bounded = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    bounded.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=99,
        decision_id=99,
        mint="BOUNDED-TERMINALS",
        start_price_sol=price0,
        price_observed_at=0.0,
    ))
    bounded.on_transition(_transition(
        "BOUNDED-TERMINALS",
        t=10.0,
        to_state="DEAD",
    ))
    bounded.on_transition(_transition(
        "BOUNDED-TERMINALS",
        t=20.0,
        to_state="GRADUATED",
    ))
    bounded.on_transition(_transition(
        "BOUNDED-TERMINALS",
        t=30.0,
        to_state="DEAD",
    ))
    with pytest.raises(
        RuntimeError,
        match="terminal transition history exceeds lifecycle bound",
    ):
        bounded.on_transition(_transition(
            "BOUNDED-TERMINALS",
            t=40.0,
            to_state="GRADUATED",
        ))
    assert bounded._terminal_history == {
        "BOUNDED-TERMINALS": [
            (10.0, "DEAD"),
            (20.0, "GRADUATED"),
            (30.0, "DEAD"),
        ],
    }

    stale = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    stale.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=2,
        decision_id=2,
        mint="STALE",
        start_price_sol=price0,
        price_observed_at=0.0,
    ))
    stale.observe_price(_price_event("STALE", t=4.0, vsol=60_000_000_000))
    assert stale.check(now=10.0) == 1
    stale_row = recorded[3]
    assert stale_row["price_now"] == 0.0
    assert stale_row["price_now_observed_at"] == 10.0
    assert stale_row["forward_return_pct"] == -100.0
    assert stale_row["terminal"] == "STALE"
    assert stale_row["unavailable_reason"] == ""

    boundary = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    boundary.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=4,
        decision_id=4,
        mint="STALE-BOUNDARY",
        start_price_sol=price0,
        price_observed_at=0.0,
    ))
    boundary.observe_price(_price_event(
        "STALE-BOUNDARY",
        t=5.0,
        vsol=60_000_000_000,
    ))
    assert boundary.check(now=10.0) == 1
    boundary_row = recorded[4]
    assert boundary_row["price_now"] == pytest.approx(2.0 * price0)
    assert boundary_row["price_now_observed_at"] == 5.0
    assert boundary_row["forward_return_pct"] == pytest.approx(100.0)
    assert boundary_row["terminal"] is None
    assert boundary_row["unavailable_reason"] == ""

    no_price = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0,),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    no_price.register(CanonicalObservationStarted(
        t_wall=5.0,
        t_mono=5.0,
        observation_id=3,
        decision_id=3,
        mint="GRADUATED-NO-PRICE",
        start_price_sol=price0,
        price_observed_at=5.0,
    ))
    no_price.on_transition(_transition(
        "GRADUATED-NO-PRICE",
        t=2.0,
        to_state="GRADUATED",
    ))
    assert no_price.check(now=15.0) == 1
    no_price_row = recorded[5]
    assert no_price_row["forward_return_pct"] is None
    assert no_price_row["price_now"] is None
    assert no_price_row["price_now_observed_at"] is None
    assert no_price_row["terminal"] == "GRADUATED"
    assert no_price_row["unavailable_reason"] == "graduated_no_price"

    class ReplayJournal:
        def __init__(self, *events):
            self.items = [event_to_dict(event) for event in events]

        def iter_events(self, *, since_wall, until_wall):
            yield from (
                item
                for item in self.items
                if since_wall <= item["t_wall"] <= until_wall
            )

    capped = ForwardReturnTracker(
        None,
        object(),
        journal=None,
        clock=lambda: 0.0,
        horizons=(20.0,),
        token_decimals=6,
        stale_price_after_s=30.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=1,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    capped.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=5,
        decision_id=5,
        mint="CAPPED-GRADUATED",
        start_price_sol=price0,
        price_observed_at=0.0,
    ))
    pre_graduation = _price_event(
        "CAPPED-GRADUATED",
        t=8.0,
        vsol=60_000_000_000,
    )
    capped.observe_price(pre_graduation)
    capped.on_transition(_transition(
        "CAPPED-GRADUATED",
        t=10.0,
        to_state="GRADUATED",
    ))
    capped.observe_price(_price_event(
        "CAPPED-GRADUATED",
        t=12.0,
        vsol=90_000_000_000,
    ))
    assert capped._price_overflow_intervals == {
        "CAPPED-GRADUATED": (8.0, 8.0),
    }
    before_failed_replay = len(recorded)
    with pytest.raises(RuntimeError, match="journal overflow evidence unavailable"):
        capped.check(now=20.0)
    assert len(recorded) == before_failed_replay
    assert capped._candidates[0].pending == {20.0}

    capped._journal = ReplayJournal(pre_graduation)
    assert capped.check(now=20.0) == 1
    capped_row = recorded[6]
    assert capped_row["price_now"] == pytest.approx(2.0 * price0)
    assert capped_row["price_now_observed_at"] == 8.0
    assert capped_row["forward_return_pct"] == pytest.approx(100.0)
    assert capped_row["terminal"] == "GRADUATED"
    assert capped_row["unavailable_reason"] == ""

    delayed_graduation = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(20.0,),
        token_decimals=6,
        stale_price_after_s=200.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=21.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    delayed_graduation.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=6,
        decision_id=6,
        mint="DELAYED-GRADUATED",
        start_price_sol=price0,
        price_observed_at=0.0,
    ))
    delayed_graduation.observe_price(_price_event(
        "DELAYED-GRADUATED",
        t=8.0,
        vsol=60_000_000_000,
    ))
    delayed_graduation.on_transition(_transition(
        "DELAYED-GRADUATED",
        t=10.0,
        to_state="GRADUATED",
    ))
    delayed_graduation.observe_price(_price_event(
        "DELAYED-GRADUATED",
        t=12.0,
        vsol=90_000_000_000,
    ))
    assert delayed_graduation.check(now=100.0) == 1
    delayed_graduation_row = recorded[7]
    assert delayed_graduation_row["price_now"] == pytest.approx(2.0 * price0)
    assert delayed_graduation_row["price_now_observed_at"] == 8.0
    assert delayed_graduation_row["forward_return_pct"] == pytest.approx(100.0)
    assert delayed_graduation_row["terminal"] == "GRADUATED"
    assert delayed_graduation_row["unavailable_reason"] == ""

    expected_keys = {
        "raw_wall",
        "observation_id",
        "horizon_s",
        "forward_return_pct",
        "price0",
        "price0_observed_at",
        "price_now",
        "price_now_observed_at",
        "terminal",
        "unavailable_reason",
    }
    assert all(set(row) == expected_keys for row in recorded)
    assert [row["raw_wall"] for row in recorded] == [
        35.0,
        35.0,
        35.0,
        10.0,
        10.0,
        15.0,
        20.0,
        100.0,
    ]
    assert [row["observation_id"] for row in recorded] == [
        1,
        1,
        1,
        2,
        4,
        3,
        5,
        6,
    ]
    assert [row["horizon_s"] for row in recorded] == [
        15.0,
        25.0,
        35.0,
        10.0,
        10.0,
        10.0,
        20.0,
        20.0,
    ]
    assert all(row["price0"] == price0 for row in recorded)
    assert [row["price0_observed_at"] for row in recorded] == [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        5.0,
        0.0,
        0.0,
    ]


def test_horizon_gap_never_falls_back_to_older_favorable_price(monkeypatch):
    recorded = []

    def capture_canonical_outcome(_conn, **values):
        recorded.append(values)
        return len(recorded)

    def reject_generic_outcome(_conn, **_values):
        pytest.fail("canonical observations must use the strict outcome writer")

    monkeypatch.setattr(
        "memebot.store.record_canonical_observation_outcome",
        capture_canonical_outcome,
    )
    monkeypatch.setattr(
        "memebot.counterfactual.record_outcome",
        reject_generic_outcome,
    )
    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        lambda _conn, *, observation_id, horizon_s: False,
    )

    class ReplayJournal:
        def __init__(self, *items):
            self.items = list(items)
            self.calls = []

        def iter_events(self, *, since_wall, until_wall):
            self.calls.append((since_wall, until_wall))
            for item in self.items:
                if isinstance(item, JournalReplayGap):
                    if (
                        item.lower_wall <= until_wall
                        and item.upper_wall >= since_wall
                    ):
                        yield item
                elif since_wall <= item["t_wall"] <= until_wall:
                    yield item

    price0 = (
        (30_000_000_000 / 1_000_000_000)
        / (900_000_000_000_000 / 1_000_000)
    )

    def tracker_with_overflow(observation_id, mint):
        tracker = ForwardReturnTracker(
            None,
            object(),
            journal=_EMPTY_REPLAY_JOURNAL,
            clock=lambda: 0.0,
            horizons=(10.0,),
            token_decimals=6,
            stale_price_after_s=5.0,
            reconcile_interval_s=60.0,
            price_history_retention_s=90000.0,
            price_history_max_samples_per_mint=1,
            price_history_max_mints=1000,
            max_in_memory_pending_observations=50000,
        )
        tracker.register(CanonicalObservationStarted(
            t_wall=0.0,
            t_mono=0.0,
            observation_id=observation_id,
            decision_id=observation_id,
            mint=mint,
            start_price_sol=price0,
            price_observed_at=0.0,
        ))
        tracker.observe_price(_price_event(
            mint,
            t=4.0,
            vsol=60_000_000_000,
        ))
        tracker.observe_price(_price_event(
            mint,
            t=10.0,
            vsol=75_000_000_000,
        ))
        tracker.observe_price(_price_event(
            mint,
            t=11.0,
            vsol=90_000_000_000,
        ))
        assert tracker._price_overflow_intervals == {mint: (4.0, 10.0)}
        return tracker

    scoped = tracker_with_overflow(1, "SCOPED")
    scoped_journal = ReplayJournal(
        event_to_dict(_price_event("SCOPED", t=4.0, vsol=60_000_000_000)),
        JournalReplayGap(
            mint="SCOPED",
            lower_wall=5.0,
            upper_wall=5.0,
            file_seq=1,
            line_number=2,
        ),
        event_to_dict(_price_event("SCOPED", t=10.0, vsol=75_000_000_000)),
    )
    scoped._journal = scoped_journal
    assert scoped.check(now=10.0) == 1
    assert scoped_journal.calls == [(4.0, 10.0)]

    global_gap = tracker_with_overflow(2, "GLOBAL")
    global_journal = ReplayJournal(
        event_to_dict(_price_event("GLOBAL", t=4.0, vsol=60_000_000_000)),
        JournalReplayGap(
            mint=None,
            lower_wall=10.0,
            upper_wall=10.0,
            file_seq=1,
            line_number=5,
        ),
    )
    global_gap._journal = global_journal
    assert global_gap.check(now=10.0) == 1
    assert global_journal.calls == [(4.0, 10.0)]
    assert [row["observation_id"] for row in recorded] == [1, 2]
    assert all(row["horizon_s"] == 10.0 for row in recorded)
    assert all(row["forward_return_pct"] is None for row in recorded)
    assert all(row["price_now"] is None for row in recorded)
    assert all(row["price_now_observed_at"] is None for row in recorded)
    assert all(row["terminal"] is None for row in recorded)
    assert all(
        row["unavailable_reason"] == "journal_replay_gap"
        for row in recorded
    )

    safe = tracker_with_overflow(3, "SAFE")
    safe_journal = ReplayJournal(
        JournalReplayGap(
            mint="OTHER",
            lower_wall=5.0,
            upper_wall=5.0,
            file_seq=2,
            line_number=2,
        ),
        JournalReplayGap(
            mint="SAFE",
            lower_wall=4.0,
            upper_wall=4.0,
            file_seq=2,
            line_number=3,
        ),
        event_to_dict(_price_event("SAFE", t=10.0, vsol=75_000_000_000)),
    )
    safe._journal = safe_journal

    assert safe.check(now=10.0) == 1
    assert safe_journal.calls == [(4.0, 10.0)]
    safe_row = recorded[2]
    assert safe_row["forward_return_pct"] == pytest.approx(150.0)
    assert safe_row["price_now"] == pytest.approx(2.5 * price0)
    assert safe_row["price_now_observed_at"] == 10.0
    assert safe_row["terminal"] is None
    assert safe_row["unavailable_reason"] == ""

    shared = ForwardReturnTracker(
        None,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        clock=lambda: 0.0,
        horizons=(10.0, 20.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=2,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    for observation_id, t0 in ((4, 0.0), (5, 5.0)):
        shared.register(CanonicalObservationStarted(
            t_wall=t0,
            t_mono=t0,
            observation_id=observation_id,
            decision_id=observation_id,
            mint="SHARED",
            start_price_sol=price0,
            price_observed_at=t0,
        ))
    for event in (
        _price_event("SHARED", t=4.0, vsol=60_000_000_000),
        _price_event("SHARED", t=18.0, vsol=90_000_000_000),
        _price_event("SHARED", t=20.0, vsol=120_000_000_000),
        _price_event("SHARED", t=25.0, vsol=150_000_000_000),
    ):
        shared.observe_price(event)
    assert shared._price_overflow_intervals == {"SHARED": (4.0, 18.0)}
    shared_journal = ReplayJournal(
        event_to_dict(_price_event("SHARED", t=4.0, vsol=60_000_000_000)),
        JournalReplayGap(
            mint="SHARED",
            lower_wall=10.0,
            upper_wall=10.0,
            file_seq=3,
            line_number=2,
        ),
        event_to_dict(_price_event("SHARED", t=18.0, vsol=90_000_000_000)),
    )
    shared._journal = shared_journal

    assert shared.check(now=25.0) == 4
    assert shared_journal.calls == [(4.0, 18.0)]
    shared_rows = recorded[3:]
    assert [
        (row["observation_id"], row["horizon_s"])
        for row in shared_rows
    ] == [(4, 10.0), (4, 20.0), (5, 10.0), (5, 20.0)]
    assert [row["unavailable_reason"] for row in shared_rows] == [
        "journal_replay_gap",
        "",
        "journal_replay_gap",
        "",
    ]
    assert [row["forward_return_pct"] for row in shared_rows] == [
        None,
        pytest.approx(300.0),
        None,
        pytest.approx(400.0),
    ]
    assert [row["price_now_observed_at"] for row in shared_rows] == [
        None,
        20.0,
        None,
        25.0,
    ]
    assert [row["terminal"] for row in shared_rows] == [None, None, None, None]

    terminal_start = len(recorded)
    for observation_id, mint, to_state in (
        (6, "DEAD-GAP", "DEAD"),
        (7, "GRADUATED-GAP", "GRADUATED"),
    ):
        terminal_tracker = tracker_with_overflow(observation_id, mint)
        terminal_tracker.on_transition(_transition(
            mint,
            t=8.0,
            to_state=to_state,
        ))
        terminal_journal = ReplayJournal(
            event_to_dict(_price_event(mint, t=4.0, vsol=60_000_000_000)),
            JournalReplayGap(
                mint=mint,
                lower_wall=5.0,
                upper_wall=5.0,
                file_seq=4,
                line_number=2,
            ),
            event_to_dict(_price_event(mint, t=10.0, vsol=75_000_000_000)),
        )
        terminal_tracker._journal = terminal_journal
        assert terminal_tracker.check(now=10.0) == 1
        assert terminal_journal.calls == [(4.0, 10.0)]

    assert recorded[terminal_start:] == [
        {
            "raw_wall": 10.0,
            "observation_id": observation_id,
            "horizon_s": 10.0,
            "forward_return_pct": None,
            "price0": price0,
            "price0_observed_at": 0.0,
            "price_now": None,
            "price_now_observed_at": None,
            "terminal": None,
            "unavailable_reason": "journal_replay_gap",
        }
        for observation_id in (6, 7)
    ]
    expected_keys = {
        "raw_wall",
        "observation_id",
        "horizon_s",
        "forward_return_pct",
        "price0",
        "price0_observed_at",
        "price_now",
        "price_now_observed_at",
        "terminal",
        "unavailable_reason",
    }
    assert all(set(row) == expected_keys for row in recorded)


def test_startup_journal_replay_recovers_pending_horizon_state(monkeypatch):
    recorded = []

    def capture_canonical_outcome(_conn, **values):
        recorded.append(values)
        return len(recorded)

    monkeypatch.setattr(
        "memebot.store.record_canonical_observation_outcome",
        capture_canonical_outcome,
    )
    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        lambda _conn, *, observation_id, horizon_s: False,
    )

    class ReplayJournal:
        def __init__(self, *items):
            self.items = items
            self.calls = []

        def iter_events(self, *, since_wall, until_wall):
            self.calls.append((since_wall, until_wall))
            for item in self.items:
                if isinstance(item, JournalReplayGap):
                    if (
                        item.lower_wall <= until_wall
                        and item.upper_wall >= since_wall
                    ):
                        yield item
                elif since_wall <= item["t_wall"] <= until_wall:
                    yield item

    def make_tracker(
        journal,
        *,
        observation_id,
        horizons=(10.0,),
        until_wall=10.0,
    ):
        tracker = ForwardReturnTracker(
            None,
            object(),
            journal=journal,
            horizons=horizons,
            token_decimals=6,
            stale_price_after_s=5.0,
            reconcile_interval_s=1.25,
            price_history_retention_s=40.0,
            price_history_max_samples_per_mint=4,
            price_history_max_mints=1,
            max_in_memory_pending_observations=1,
            clock=lambda: until_wall,
        )
        tracker.register(CanonicalObservationStarted(
            t_wall=0.0,
            t_mono=0.0,
            observation_id=observation_id,
            decision_id=observation_id,
            mint="PENDING",
            start_price_sol=1.0,
            price_observed_at=0.0,
        ))
        return tracker

    terminal_journal = ReplayJournal(
        event_to_dict(_price_event("PENDING", t=5.0, vsol=60_000_000_000)),
        event_to_dict(_price_event("UNRELATED", t=6.0, vsol=90_000_000_000)),
        event_to_dict(_transition("PENDING", t=8.0, to_state="DEAD")),
    )
    terminal = make_tracker(terminal_journal, observation_id=1)
    assert terminal._reconcile_interval_s == 1.25
    assert terminal.replay_journal(since_wall=0.0, until_wall=10.0) == 2
    assert terminal_journal.calls == [(0.0, 10.0)]
    assert tuple(terminal._price_history) == ("PENDING",)
    assert terminal.check(now=10.0) == 1
    assert recorded.pop() == {
        "raw_wall": 10.0,
        "observation_id": 1,
        "horizon_s": 10.0,
        "forward_return_pct": -100.0,
        "price0": 1.0,
        "price0_observed_at": 0.0,
        "price_now": 0.0,
        "price_now_observed_at": 8.0,
        "terminal": "DEAD",
        "unavailable_reason": "",
    }

    for observation_id, gap in (
        (2, JournalReplayGap(
            mint="PENDING", lower_wall=9.0, upper_wall=9.0,
            file_seq=1, line_number=1,
        )),
        (3, JournalReplayGap(
            mint=None, lower_wall=9.0, upper_wall=9.0,
            file_seq=2, line_number=1,
        )),
    ):
        journal = ReplayJournal(
            gap,
            event_to_dict(_price_event(
                "PENDING", t=10.0, vsol=60_000_000_000,
            )),
        )
        tracker = make_tracker(journal, observation_id=observation_id)
        assert tracker.replay_journal(since_wall=0.0, until_wall=10.0) == 1
        assert tracker._price_overflow_intervals == {"PENDING": (9.0, 9.0)}
        assert tracker.check(now=10.0) == 1
        row = recorded.pop()
        assert row["observation_id"] == observation_id
        assert row["forward_return_pct"] is None
        assert row["price_now"] is None
        assert row["price_now_observed_at"] is None
        assert row["terminal"] is None
        assert row["unavailable_reason"] == "journal_replay_gap"

    unrelated_journal = ReplayJournal(
        JournalReplayGap(
            mint="UNRELATED", lower_wall=9.0, upper_wall=9.0,
            file_seq=3, line_number=1,
        ),
        event_to_dict(_price_event(
            "PENDING", t=10.0, vsol=60_000_000_000,
        )),
    )
    unrelated = make_tracker(unrelated_journal, observation_id=4)
    assert unrelated.replay_journal(since_wall=0.0, until_wall=10.0) == 1
    assert unrelated._price_overflow_intervals == {}
    assert unrelated.check(now=10.0) == 1
    unrelated_row = recorded.pop()
    assert unrelated_row["observation_id"] == 4
    assert unrelated_row["price_now"] is not None
    assert unrelated_row["price_now_observed_at"] == 10.0
    assert unrelated_row["terminal"] is None
    assert unrelated_row["unavailable_reason"] == ""

    disjoint_journal = ReplayJournal(
        JournalReplayGap(
            mint="PENDING", lower_wall=9.0, upper_wall=9.0,
            file_seq=4, line_number=1,
        ),
        event_to_dict(_price_event(
            "PENDING", t=10.0, vsol=60_000_000_000,
        )),
        event_to_dict(_price_event(
            "PENDING", t=20.0, vsol=75_000_000_000,
        )),
        JournalReplayGap(
            mint="PENDING", lower_wall=29.0, upper_wall=29.0,
            file_seq=4, line_number=4,
        ),
        event_to_dict(_price_event(
            "PENDING", t=30.0, vsol=90_000_000_000,
        )),
    )
    disjoint = make_tracker(
        disjoint_journal,
        observation_id=5,
        horizons=(10.0, 20.0, 30.0),
        until_wall=30.0,
    )
    assert disjoint.replay_journal(since_wall=0.0, until_wall=30.0) == 3
    assert disjoint._price_overflow_intervals == {"PENDING": (9.0, 29.0)}
    assert disjoint.check(now=30.0) == 3
    disjoint_rows = recorded[-3:]
    del recorded[-3:]
    assert [row["horizon_s"] for row in disjoint_rows] == [10.0, 20.0, 30.0]
    assert [row["unavailable_reason"] for row in disjoint_rows] == [
        "journal_replay_gap",
        "",
        "journal_replay_gap",
    ]
    assert disjoint_rows[1]["price_now_observed_at"] == 20.0
    assert disjoint_journal.calls == [(0.0, 30.0), (9.0, 29.0)]
    assert recorded == []


@pytest.mark.asyncio
async def test_periodic_reconcile_heals_commit_publish_and_capacity_gaps(
    monkeypatch,
):
    clock = [3.0]
    selector_calls = []
    existence_calls = []
    rows = [
        {
            "id": observation_id,
            "decision_id": observation_id + 1000,
            "mint": mint,
            "observed_at": observed_at,
            "start_price_sol": observation_id / 10.0,
            "price_observed_at": price_observed_at,
            "unavailable_reason": "",
            "horizons_json": "[1000.0,2000.0]",
            "full_mask": 3,
            "completed_mask": 0,
        }
        for observation_id, mint, observed_at, price_observed_at in (
            (101, "DB-LATE", 2.0, 1.0),
            (102, "DB-EARLY", 1.0, 0.5),
            (103, "DB-MID", 1.5, 1.25),
            (104, "CAP-EXCESS", 2.5, 2.0),
        )
    ]

    def list_pending(_conn, *, horizons, limit_plus_one):
        selector_calls.append((tuple(horizons), limit_plus_one))
        return rows[:limit_plus_one]

    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        list_pending,
    )

    def outcome_exists(_conn, *, observation_id, horizon_s):
        existence_calls.append((observation_id, horizon_s))
        return horizon_s == 2000.0

    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        outcome_exists,
    )

    class ReplayJournal:
        def __init__(self):
            self.calls = []
            self.events = [
                event_to_dict(_price_event(
                    "DB-LATE", t=2.5, vsol=40_000_000_000,
                )),
                event_to_dict(_price_event(
                    "DB-EARLY", t=1.5, vsol=45_000_000_000,
                )),
                event_to_dict(_price_event(
                    "DB-MID", t=1.75, vsol=47_000_000_000,
                )),
                event_to_dict(_price_event(
                    "CAP-EXCESS", t=2.75, vsol=50_000_000_000,
                )),
            ]

        def iter_events(self, *, since_wall, until_wall):
            self.calls.append((since_wall, until_wall))
            return iter(
                event
                for event in self.events
                if since_wall <= event["t_wall"] <= until_wall
            )

    journal = ReplayJournal()
    bus = EventBus()
    tracker = ForwardReturnTracker(
        bus,
        object(),
        journal=journal,
        clock=lambda: clock[0],
        horizons=(1000.0, 2000.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=7.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=4,
    )
    _register(tracker, mint="HELD", decision_id=101, t=0.0, price=1.0)
    reconcile_returns = []
    reconcile_from_ledger = tracker.reconcile_from_ledger

    def record_reconcile(*, now):
        result = reconcile_from_ledger(now=now)
        reconcile_returns.append(result)
        return result

    tracker.reconcile_from_ledger = record_reconcile
    stop = asyncio.Event()
    task = asyncio.create_task(tracker.run(stop))
    try:
        for _ in range(20):
            await asyncio.sleep(0)
            if len(tracker._candidates) == 4:
                break
        assert [(c.ref_kind, c.ref_id) for c in tracker._candidates] == [
            ("candidate", 101),
            ("canonical_observation", 101),
            ("canonical_observation", 102),
            ("canonical_observation", 103),
        ]
        db_only = tracker._candidates[1]
        assert (
            db_only.mint,
            db_only.t0,
            db_only.price0_at,
            db_only.price0,
            db_only.pending,
        ) == ("DB-LATE", 2.0, 1.0, 10.1, {1000.0})
        db_early = tracker._candidates[2]
        assert (
            db_early.mint,
            db_early.t0,
            db_early.price0_at,
            db_early.price0,
            db_early.pending,
        ) == ("DB-EARLY", 1.0, 0.5, 10.2, {1000.0})
        db_mid = tracker._candidates[3]
        assert (
            db_mid.mint,
            db_mid.t0,
            db_mid.price0_at,
            db_mid.price0,
            db_mid.pending,
        ) == ("DB-MID", 1.5, 1.25, 10.3, {1000.0})
        assert {"DB-LATE", "DB-EARLY", "DB-MID"} <= tracker._registered_mints
        assert tracker._price_history["DB-LATE"][0][0] == 2.5
        assert tracker._price_history["DB-EARLY"][0][0] == 1.5
        assert tracker._price_history["DB-MID"][0][0] == 1.75
        assert "CAP-EXCESS" not in tracker._price_history

        tracker._candidates = [
            candidate
            for candidate in tracker._candidates
            if candidate.ref_kind != "candidate"
        ]
        clock[0] = 9.0
        await bus.publish(_price_event("WAKE", t=9.0, vsol=30_000_000_000))
        for _ in range(20):
            await asyncio.sleep(0)
        assert not any(c.ref_id == 104 for c in tracker._candidates)
        assert selector_calls == [((1000.0, 2000.0), 5)]

        clock[0] = 10.0
        await bus.publish(_price_event("WAKE", t=10.0, vsol=30_000_000_000))
        for _ in range(20):
            await asyncio.sleep(0)
            if any(c.ref_id == 104 for c in tracker._candidates):
                break
        assert [(c.ref_kind, c.ref_id) for c in tracker._candidates] == [
            ("canonical_observation", 101),
            ("canonical_observation", 102),
            ("canonical_observation", 103),
            ("canonical_observation", 104),
        ]
        cap_excess = tracker._candidates[3]
        assert (
            cap_excess.mint,
            cap_excess.t0,
            cap_excess.price0_at,
            cap_excess.price0,
            cap_excess.pending,
        ) == ("CAP-EXCESS", 2.5, 2.0, 10.4, {1000.0})
        assert "CAP-EXCESS" in tracker._registered_mints
        assert tracker._price_history["CAP-EXCESS"][0][0] == 2.75
        assert selector_calls == [
            ((1000.0, 2000.0), 5),
            ((1000.0, 2000.0), 5),
        ]
        assert existence_calls == [
            (101, 1000.0),
            (101, 2000.0),
            (102, 1000.0),
            (102, 2000.0),
            (103, 1000.0),
            (103, 2000.0),
            (104, 1000.0),
            (104, 2000.0),
        ]
        assert journal.calls == [(1.0, 3.0), (2.5, 10.0)]
        assert reconcile_returns == [3, 1]
    finally:
        stop.set()
        await bus.publish(_price_event("WAKE", t=1.0, vsol=30_000_000_000))
        await task

    def fail_selector(_conn, *, horizons, limit_plus_one):
        raise RuntimeError("reconcile selector failed")

    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        fail_selector,
    )
    failing_tracker = ForwardReturnTracker(
        EventBus(),
        object(),
        journal=journal,
        clock=lambda: 10.0,
        horizons=(1000.0, 2000.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=7.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=3,
    )
    with pytest.raises(RuntimeError, match="reconcile selector failed"):
        await failing_tracker.run(asyncio.Event())

    timeouts = []

    async def capture_wait(awaitable, *, timeout):
        awaitable.close()
        timeouts.append(timeout)
        raise RuntimeError("wait captured")

    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        lambda _conn, *, horizons, limit_plus_one: [],
    )
    monkeypatch.setattr("memebot.counterfactual.asyncio.wait_for", capture_wait)
    interval_tracker = ForwardReturnTracker(
        EventBus(),
        object(),
        journal=journal,
        clock=lambda: 10.0,
        horizons=(1000.0, 2000.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=0.25,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=4,
    )
    with pytest.raises(RuntimeError, match="wait captured"):
        await interval_tracker.run(asyncio.Event())
    assert timeouts == [0.25]

    class EmptyResumeConnection:
        def execute(self, *_args, **_kwargs):
            return ()

    future_row = {
        "id": 201,
        "decision_id": 1201,
        "mint": "FUTURE-CAUSAL",
        "observed_at": 20.0,
        "start_price_sol": 12.5,
        "price_observed_at": 9.0,
    }
    future_journal = ReplayJournal()
    future_journal.events = []
    monkeypatch.setattr(
        "memebot.counterfactual.list_decisions_for_counterfactual",
        lambda _conn: [{
            "id": 200,
            "mint": "LEGACY-RESUMED",
            "at": 5.0,
            "feature_vector_json": '{"spot_price_sol":1.5}',
        }],
    )
    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        lambda _conn, *, horizons, limit_plus_one: [future_row],
    )
    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        lambda _conn, *, observation_id, horizon_s: False,
    )
    resume_conn = EmptyResumeConnection()
    startup_tracker = ForwardReturnTracker(
        EventBus(),
        resume_conn,
        journal=future_journal,
        clock=lambda: 10.0,
        horizons=(1000.0, 2000.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=7.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=4,
    )
    assert startup_tracker.resume_from_ledger(resume_conn) == 1
    assert [
        (candidate.ref_kind, candidate.ref_id)
        for candidate in startup_tracker._candidates
    ] == [("candidate", 200), ("canonical_observation", 201)]
    assert future_journal.calls == [(20.0, 20.0)]

    generic_rows = [
        {
            "id": decision_id,
            "mint": mint,
            "at": 5.0,
            "feature_vector_json": '{"spot_price_sol":1.5}',
        }
        for decision_id, mint in ((401, "GENERIC-A"), (402, "GENERIC-B"))
    ]
    monkeypatch.setattr(
        "memebot.counterfactual.list_decisions_for_counterfactual",
        lambda _conn: generic_rows,
    )
    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        lambda _conn, *, horizons, limit_plus_one: [],
    )
    startup_tracker._candidates.clear()
    startup_tracker._registered_mints.clear()
    startup_tracker._price_history.clear()
    startup_tracker._price_history_max_mints = 1
    assert startup_tracker.resume_from_ledger(resume_conn) == 1
    assert [candidate.ref_id for candidate in startup_tracker._candidates] == [401]
    assert startup_tracker._registered_mints == {"GENERIC-A"}

    startup_tracker._candidates.clear()
    startup_tracker._registered_mints.clear()
    startup_tracker._price_history["HISTORY-ONLY"] = [(4.0, 1.0)]
    assert startup_tracker.resume_from_ledger(resume_conn) == 0
    assert startup_tracker._candidates == []

    cap_rows = [
        {
            "id": observation_id,
            "decision_id": observation_id + 1000,
            "mint": mint,
            "observed_at": 1.0,
            "start_price_sol": 1.0,
            "price_observed_at": 1.0,
        }
        for observation_id, mint in ((301, "CAP-ONE"), (302, "CAP-TWO"))
    ]
    cap_journal = ReplayJournal()
    cap_journal.events = [
        event_to_dict(_price_event("CAP-ONE", t=2.0, vsol=40_000_000_000)),
        event_to_dict(_price_event("CAP-TWO", t=2.0, vsol=50_000_000_000)),
    ]
    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        lambda _conn, *, horizons, limit_plus_one: cap_rows,
    )
    cap_tracker = ForwardReturnTracker(
        EventBus(),
        object(),
        journal=cap_journal,
        clock=lambda: 3.0,
        horizons=(1000.0, 2000.0),
        token_decimals=6,
        stale_price_after_s=5.0,
        reconcile_interval_s=7.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1,
        max_in_memory_pending_observations=2,
    )
    assert cap_tracker.reconcile_from_ledger(now=3.0) == 1
    assert [candidate.ref_id for candidate in cap_tracker._candidates] == [301]
    assert cap_tracker._registered_mints == {"CAP-ONE"}
    assert set(cap_tracker._price_history) == {"CAP-ONE"}
    _register(cap_tracker, mint="CAP-TWO", decision_id=999, t=3.0, price=1.0)
    assert [candidate.ref_id for candidate in cap_tracker._candidates] == [301]

    cap_tracker._price_history.clear()
    _register(cap_tracker, mint="CAP-TWO", decision_id=998, t=3.0, price=1.0)
    assert [candidate.ref_id for candidate in cap_tracker._candidates] == [301]
    assert cap_tracker.reconcile_from_ledger(now=3.0) == 0

    cap_tracker._registered_mints.clear()
    cap_tracker._price_history["CAP-ONE"] = [(2.0, 1.0)]
    _register(cap_tracker, mint="CAP-TWO", decision_id=997, t=3.0, price=1.0)
    assert [candidate.ref_id for candidate in cap_tracker._candidates] == [301]
    assert cap_tracker.reconcile_from_ledger(now=3.0) == 0

    cap_tracker._price_history.clear()
    cap_tracker._registered_mints = {"CAP-ONE"}
    cap_tracker.observe_price(
        _price_event("CAP-TWO", t=4.0, vsol=50_000_000_000),
    )
    assert "CAP-TWO" not in cap_tracker._prices
    assert "CAP-TWO" not in cap_tracker._price_history
    cap_tracker.observe_price(
        _price_event("CAP-ONE", t=4.0, vsol=40_000_000_000),
    )
    assert set(cap_tracker._price_history) == {"CAP-ONE"}
    assert len(cap_tracker._registered_mints | cap_tracker._price_history.keys()) == 1


def test_due_resumed_observation_without_price_flushes_stale_once(monkeypatch):
    row = {
        "id": 501,
        "decision_id": 1501,
        "mint": "DUE-RESUMED",
        "observed_at": 8.0,
        "start_price_sol": 2.0,
        "price_observed_at": 8.0,
    }
    outcomes = set()
    writes = []

    class WindowJournal:
        def __init__(self):
            self.calls = []

        def iter_events(self, *, since_wall, until_wall):
            self.calls.append((since_wall, until_wall))
            event = event_to_dict(
                _price_event("OLDER-GENERIC", t=2.0, vsol=40_000_000_000),
            )
            return iter(
                [event]
                if since_wall <= event["t_wall"] <= until_wall
                else []
            )

    journal = WindowJournal()

    monkeypatch.setattr(
        "memebot.store.list_pending_canonical_observations",
        lambda _conn, *, horizons, limit_plus_one: [row],
    )
    monkeypatch.setattr(
        "memebot.store.canonical_outcome_exists",
        lambda _conn, *, observation_id, horizon_s: (
            observation_id,
            horizon_s,
        ) in outcomes,
    )

    def record_outcome(_conn, **kwargs):
        key = (kwargs["observation_id"], kwargs["horizon_s"])
        assert key not in outcomes
        outcomes.add(key)
        writes.append(kwargs)
        return len(writes)

    monkeypatch.setattr(
        "memebot.store.record_canonical_observation_outcome",
        record_outcome,
    )
    monkeypatch.setattr(
        "memebot.counterfactual.record_outcome",
        lambda *_args, **_kwargs: pytest.fail(
            "canonical reconcile must not flush older generic work",
        ),
    )
    tracker = ForwardReturnTracker(
        EventBus(),
        object(),
        journal=journal,
        horizons=(2.0,),
        token_decimals=6,
        stale_price_after_s=1.0,
        reconcile_interval_s=7.0,
        price_history_retention_s=10.0,
        price_history_max_samples_per_mint=10,
        price_history_max_mints=10,
        max_in_memory_pending_observations=10,
        clock=lambda: 12.0,
    )
    _register(
        tracker,
        mint="OLDER-GENERIC",
        decision_id=500,
        t=0.0,
        price=1.0,
    )
    tracker.register(CanonicalObservationStarted(
        t_wall=0.0,
        t_mono=0.0,
        observation_id=499,
        decision_id=1499,
        mint="OLDER-CANONICAL",
        start_price_sol=1.0,
        price_observed_at=0.0,
    ))

    assert tracker.reconcile_from_ledger(now=12.0) == 1
    assert writes == [{
        "raw_wall": 12.0,
        "observation_id": 501,
        "horizon_s": 2.0,
        "forward_return_pct": -100.0,
        "price0": 2.0,
        "price0_observed_at": 8.0,
        "price_now": 0.0,
        "price_now_observed_at": 10.0,
        "terminal": "STALE",
        "unavailable_reason": "",
    }]
    assert [
        (candidate.ref_kind, candidate.ref_id, candidate.pending)
        for candidate in tracker._candidates
    ] == [
        ("candidate", 500, {2.0}),
        ("canonical_observation", 499, {2.0}),
    ]
    assert journal.calls == [(8.0, 12.0)]

    assert tracker.reconcile_from_ledger(now=12.0) == 0
    tracker.register(CanonicalObservationStarted(
        t_wall=8.0,
        t_mono=8.0,
        observation_id=501,
        decision_id=1501,
        mint="DUE-RESUMED",
        start_price_sol=2.0,
        price_observed_at=8.0,
    ))
    assert not any(
        candidate.ref_kind == "canonical_observation"
        and candidate.ref_id == 501
        for candidate in tracker._candidates
    )
    assert len(writes) == 1

    outcomes.add((502, 2.0))
    partial_tracker = ForwardReturnTracker(
        EventBus(),
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        horizons=(2.0, 4.0),
        token_decimals=6,
        stale_price_after_s=1.0,
        reconcile_interval_s=7.0,
        price_history_retention_s=10.0,
        price_history_max_samples_per_mint=10,
        price_history_max_mints=10,
        max_in_memory_pending_observations=10,
        clock=lambda: 10.0,
    )
    partial_tracker.register(CanonicalObservationStarted(
        t_wall=1.0,
        t_mono=1.0,
        observation_id=502,
        decision_id=1502,
        mint="PARTIAL-LIVE",
        start_price_sol=3.0,
        price_observed_at=1.0,
    ))
    assert [
        (candidate.ref_kind, candidate.ref_id, candidate.pending)
        for candidate in partial_tracker._candidates
    ] == [("canonical_observation", 502, {4.0})]


@pytest.mark.asyncio
async def test_tracker_subscription_unsubscribes_in_finally():
    bus = EventBus()
    tracker = ForwardReturnTracker(
        bus,
        object(),
        journal=_EMPTY_REPLAY_JOURNAL,
        horizons=(3600.0,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000,
        price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
        clock=lambda: 0.0,
    )
    stop = asyncio.Event()
    stop.set()

    await tracker.run(stop)
    await bus.publish(_price_event("AFTER-EXIT", t=1.0, vsol=30_000_000_000))

    assert tracker._q.empty()


def test_canonical_analysis_population_formulas(tmp_path):
    horizon = 3600.0
    conn = open_db(tmp_path / "canonical-analysis.db")
    next_at = 100.0
    decisions = {}

    def canonical_json(*, label, at, status, reason, generation_hash, peers):
        zero_cluster_subject_only = reason in {
            "canonical_target_not_live",
            "canonical_identity_unavailable",
            "canonical_identity_conflict",
        }
        pre_snapshot_internal = (
            reason == "canonical_internal_error"
            and generation_hash is None
            and not peers
        )
        if status == "CANONICAL":
            eligible = 1 + sum(peer[1] for peer in peers)
            canonical_mint = f"{label}-CANON"
        elif generation_hash is not None:
            eligible = sum(peer[1] for peer in peers)
            canonical_mint = next(peer[0] for peer in peers if peer[1])
        else:
            eligible = 0
            canonical_mint = None
        return json.dumps(
            {
                "canonical": {
                    "canonical_mint": canonical_mint,
                    "cluster_key": label.casefold(),
                    "cluster_size": (
                        3
                        if reason == "canonical_cluster_too_large"
                        else 0
                        if zero_cluster_subject_only or pre_snapshot_internal
                        else 1 + len(peers)
                    ),
                    "config_hash": "c" * 64,
                    "eligible_cluster_size": eligible,
                    "generation_hash": generation_hash,
                    "inputs_hash": "a" * 64,
                    "planned_size_sol": 1.0,
                    "rank": 1 if status == "CANONICAL" else None,
                    "rank_points": 1 if status == "CANONICAL" else None,
                    "ranking_inputs": {
                        "counterfactual_horizons_s": [horizon],
                    },
                    "ranking_order": (
                        [
                            canonical_mint,
                            *[
                                peer[0]
                                for peer in peers
                                if peer[1] and peer[0] != canonical_mint
                            ],
                        ]
                        if status == "CANONICAL"
                        or canonical_mint is not None
                        else []
                    ),
                    "reason": reason,
                    "resolved_at": at,
                    "resolver_version": "canonical-v1",
                    "status": status,
                    "weights_version": "canonical-weighted-v1",
                },
                "score_status": "VALID",
                "score_unavailable_reason": "",
                "score_weights_version": "climbing-v1",
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    def add_decision(
        label,
        *,
        peers=(),
        reason="canonical_selected",
        status="CANONICAL",
        generation=None,
        action="SKIP",
        register_generation=True,
    ):
        nonlocal next_at
        at = next_at
        next_at += 10.0
        subject = f"{label}-CANON"
        all_mints = (subject, *(peer[0] for peer in peers))
        for mint in all_mints:
            conn.execute(
                "INSERT OR IGNORE INTO tokens(mint,created_at,state,curve_progress,last_seen) "
                "VALUES (?,?,?,?,?)",
                (mint, at - 2.0, "CLIMBING", 50.0, at - 1.0),
            )
        report_id = conn.execute(
            "INSERT INTO safety_reports(mint,checked_at,hard_fails_json,"
            "risk_score,inputs_hash) VALUES (?,?,?,?,?)",
            (subject, at - 1.0, "[]", 1.0, "b" * 64),
        ).lastrowid
        feature_json = canonical_json(
            label=label,
            at=at,
            status=status,
            reason=reason,
            generation_hash=generation,
            peers=peers,
        )
        canonical_mint = json.loads(feature_json)["canonical"]["canonical_mint"]
        decision_id = conn.execute(
            "INSERT INTO decisions(at,mint,segment,action,score,"
            "feature_vector_json,safety_report_id,config_hash) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (at, subject, "CLIMBING", action, 75.0, feature_json, report_id, "c" * 64),
        ).lastrowid
        observations = {}
        rows = ((subject, True, subject == canonical_mint, status == "CANONICAL", 1.0),)
        rows += tuple(
            (mint, False, mint == canonical_mint, eligible, start_price)
            for mint, eligible, start_price in peers
        )
        for mint, is_subject, is_canonical, eligible, start_price in rows:
            available = start_price is not None
            observations[mint] = conn.execute(
                "INSERT INTO canonical_observations("
                "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
                "start_price_sol,price_observed_at,price_source,unavailable_reason"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    decision_id,
                    mint,
                    at,
                    is_subject,
                    is_canonical,
                    eligible,
                    start_price,
                    at if available else None,
                    "curve_snapshot" if available else "",
                    "" if available else "start_price_missing",
                ),
            ).lastrowid
        if generation is not None and register_generation:
            conn.execute(
                "INSERT INTO canonical_generations("
                "generation_hash,first_decision_id,created_at) VALUES (?,?,?)",
                (generation, decision_id, at),
            )
        decisions[label] = {
            "id": decision_id,
            "at": at,
            "mint": subject,
            "report_id": report_id,
            "feature": json.loads(feature_json),
            "observations": observations,
        }
        return decisions[label]

    def add_outcome(observation_id, *, observed_at, price0=1.0, return_pct=None, gap=False):
        if gap:
            record_canonical_observation_outcome(
                conn,
                raw_wall=observed_at + horizon,
                observation_id=observation_id,
                horizon_s=horizon,
                forward_return_pct=None,
                price0=price0,
                price0_observed_at=observed_at,
                price_now=None,
                price_now_observed_at=None,
                terminal=None,
                unavailable_reason="journal_replay_gap",
            )
            return
        price_now = price0 * (1.0 + return_pct / 100.0)
        record_canonical_observation_outcome(
            conn,
            raw_wall=observed_at + horizon,
            observation_id=observation_id,
            horizon_s=horizon,
            forward_return_pct=return_pct,
            price0=price0,
            price0_observed_at=observed_at,
            price_now=price_now,
            price_now_observed_at=observed_at + horizon,
            terminal=None,
            unavailable_reason="",
        )

    win = add_decision(
        "WIN",
        peers=(("WIN-LOW", True, 1.0), ("WIN-HIGH", True, 1.0)),
        generation=f"{1:064x}",
        action="BUY",
    )
    tie = add_decision(
        "TIE", peers=(("TIE-PEER", True, 1.0),),
        generation=f"{2:064x}", action="BUY",
    )
    harm = add_decision(
        "HARM", peers=(("HARM-PEER", True, 1.0),),
        generation=f"{3:064x}", action="BUY",
    )
    incomplete = add_decision(
        "INCOMPLETE",
        peers=(("INCOMPLETE-ONE", True, 1.0), ("INCOMPLETE-MISSING", True, 1.0)),
        generation=f"{4:064x}",
    )
    missing_start = add_decision(
        "MISSING-START",
        peers=(("MISSING-START-PEER", True, None),),
        generation=f"{5:064x}",
    )
    ineligible = add_decision(
        "INELIGIBLE", peers=(("INELIGIBLE-PEER", False, 1.0),),
        generation=f"{6:064x}",
    )
    singleton = add_decision("SINGLETON", generation=f"{7:064x}")
    gap = add_decision(
        "GAP", peers=(("GAP-PEER", True, 1.0),),
        generation=f"{8:064x}", action="BUY",
    )
    changed = add_decision(
        "CHANGED", peers=(("CHANGED-PEER", True, 1.0),),
        generation=f"{9:064x}",
    )
    add_decision(
        "REPEATED",
        peers=(("REPEATED-PEER", True, 1.0),),
        generation=f"{1:064x}",
        register_generation=False,
    )
    unresolved_identity = add_decision(
        "UNRESOLVED-IDENTITY",
        status="UNRESOLVED",
        reason="canonical_identity_unavailable",
    )
    unresolved_conflict = add_decision(
        "UNRESOLVED-CONFLICT",
        status="UNRESOLVED",
        reason="canonical_identity_conflict",
    )
    unresolved_holder = add_decision(
        "UNRESOLVED-HOLDER",
        peers=(("UNRESOLVED-HOLDER-WINNER", True, 1.0),),
        status="UNRESOLVED",
        reason="canonical_holder_evidence_unavailable",
        generation=f"{10:064x}",
    )
    unresolved_liquidity = add_decision(
        "UNRESOLVED-LIQUIDITY",
        status="UNRESOLVED",
        reason="canonical_liquidity_unavailable",
    )
    cluster_too_large = add_decision(
        "CLUSTER-TOO-LARGE",
        status="UNRESOLVED",
        reason="canonical_cluster_too_large",
    )
    internal_pre_snapshot = add_decision(
        "INTERNAL-PRE",
        status="UNRESOLVED",
        reason="canonical_internal_error",
    )
    internal_post_snapshot = add_decision(
        "INTERNAL-POST",
        peers=(("INTERNAL-POST-WINNER", True, 1.0),),
        status="UNRESOLVED",
        reason="canonical_internal_error",
        generation=f"{11:064x}",
    )
    assert unresolved_identity["feature"]["canonical"]["cluster_size"] == 0
    assert unresolved_conflict["feature"]["canonical"]["cluster_size"] == 0
    assert unresolved_holder["feature"]["canonical"]["canonical_mint"] == (
        "UNRESOLVED-HOLDER-WINNER"
    )
    assert unresolved_holder["feature"]["canonical"]["rank"] is None
    assert unresolved_holder["feature"]["canonical"]["generation_hash"] == (
        f"{10:064x}"
    )
    assert cluster_too_large["feature"]["canonical"]["cluster_size"] == 3
    assert len(cluster_too_large["observations"]) == 1
    assert internal_pre_snapshot["feature"]["canonical"]["cluster_size"] == 0
    assert internal_pre_snapshot["feature"]["canonical"]["canonical_mint"] is None
    assert internal_post_snapshot["feature"]["canonical"]["cluster_size"] == 2
    assert internal_post_snapshot["feature"]["canonical"]["canonical_mint"] == (
        "INTERNAL-POST-WINNER"
    )
    assert internal_post_snapshot["feature"]["canonical"]["generation_hash"] == (
        f"{11:064x}"
    )
    del (
        ineligible,
        singleton,
        unresolved_identity,
        unresolved_conflict,
        unresolved_holder,
        unresolved_liquidity,
        cluster_too_large,
        internal_pre_snapshot,
        internal_post_snapshot,
    )
    conn.commit()

    for decision, returns in (
        (win, {"WIN-CANON": 50.0, "WIN-LOW": 10.0, "WIN-HIGH": 60.0}),
        (tie, {"TIE-CANON": 20.0, "TIE-PEER": 20.0}),
        (harm, {"HARM-CANON": -10.0, "HARM-PEER": 5.0}),
        (incomplete, {"INCOMPLETE-CANON": 30.0, "INCOMPLETE-ONE": 10.0}),
        (missing_start, {"MISSING-START-CANON": 10.0}),
        (gap, {"GAP-PEER": 10.0}),
        (changed, {"CHANGED-CANON": 40.0, "CHANGED-PEER": 20.0}),
    ):
        for mint, return_pct in returns.items():
            add_outcome(
                decision["observations"][mint],
                observed_at=decision["at"],
                return_pct=return_pct,
            )
    add_outcome(
        gap["observations"]["GAP-CANON"],
        observed_at=gap["at"],
        gap=True,
    )

    def add_recheck(decision, *, status):
        rechecked_at = decision["at"] + 1.0
        canonical_mint = decision["mint"] if status == "PASS" else None
        reason = "canonical_selected" if status == "PASS" else "canonical_internal_error"
        payload = {
            "attempt": 1,
            "causal_target_report_id": decision["report_id"],
            "decision_id": decision["id"],
            "fill_event_at": decision["at"] + 0.5,
            "latest_target_report_id": decision["report_id"],
            "prior_inputs_hash": "a" * 64,
            "rechecked_at": rechecked_at,
            "target_snapshot": None,
            "trigger": "curve_progress",
            "trigger_report_id": None,
            "verdict": {
                "canonical_mint": canonical_mint,
                "inputs_hash": "d" * 64,
                "reason": reason,
                "status": "CANONICAL" if status == "PASS" else "UNRESOLVED",
            },
        }
        return conn.execute(
            "INSERT INTO canonical_rechecks("
            "decision_id,attempt,rechecked_at,causal_target_report_id,"
            "latest_target_report_id,status,reason,canonical_mint,prior_inputs_hash,"
            "recheck_inputs_hash,payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                decision["id"],
                1,
                rechecked_at,
                decision["report_id"],
                decision["report_id"],
                status,
                reason,
                canonical_mint,
                "a" * 64,
                "d" * 64,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
            ),
        ).lastrowid

    pass_id = add_recheck(win, status="PASS")
    trade_at = win["at"] + 2.0
    trade_id = conn.execute(
        "INSERT INTO paper_trades("
        "decision_id,at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
        "realism_grade,canonical_recheck_id,canonical_proof_hash) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            win["id"], trade_at, win["mint"], "CLIMBING", "buy", 1.0, 1.0,
            1.0, "{}", "A", pass_id, "d" * 64,
        ),
    ).lastrowid
    conn.execute(
        "INSERT INTO paper_entry_executions("
        "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id,"
        "paper_trade_id) VALUES (?,?,?,?,?,?,?)",
        (win["id"], trade_at, "FILLED", "filled", 1.0, pass_id, trade_id),
    )
    cancel_id = add_recheck(tie, status="CANCEL")
    conn.execute(
        "INSERT INTO paper_entry_executions("
        "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id) "
        "VALUES (?,?,?,?,?,?)",
        (
            tie["id"], tie["at"] + 2.0, "CANCELLED", "canonical_internal_error",
            1.0, cancel_id,
        ),
    )
    conn.execute(
        "INSERT INTO paper_entry_executions("
        "decision_id,at,status,reason,planned_size_sol) VALUES (?,?,?,?,?)",
        (harm["id"], harm["at"] + 1.0, "ABANDONED", "restart_before_fill", 1.0),
    )
    conn.commit()

    metrics = compute_canonical_metrics(conn, horizon_s=horizon)
    assert metrics.all_p3_decisions == 17
    assert metrics.primary_decisions == 11
    assert (
        metrics.potential_pairs,
        metrics.comparable_pairs,
        metrics.pair_wins,
        metrics.pair_losses,
        metrics.pair_ties,
    ) == (9, 6, 3, 2, 1)
    assert (
        metrics.potential_clusters,
        metrics.comparable_clusters,
        metrics.cluster_wins,
        metrics.cluster_losses,
        metrics.cluster_ties,
        metrics.harm_clusters,
    ) == (7, 4, 1, 2, 1, 1)
    assert (
        metrics.unresolved_identity,
        metrics.unresolved_holder,
        metrics.unresolved_liquidity,
        metrics.journal_gap_outcomes,
    ) == (2, 1, 1, 1)
    assert (
        metrics.canonical_buy_decisions,
        metrics.filled_entries,
        metrics.cancelled_entries,
        metrics.abandoned_entries,
    ) == (4, 1, 1, 1)
    assert metrics.pair_coverage == pytest.approx(6 / 9)
    assert metrics.cluster_coverage == pytest.approx(4 / 7)
    assert metrics.harm_rate == pytest.approx(1 / 4)
    assert metrics.unresolved_identity_rate == pytest.approx(2 / 17)
    assert metrics.unresolved_holder_rate == pytest.approx(1 / 17)
    assert metrics.unresolved_liquidity_rate == pytest.approx(1 / 17)
    assert metrics.terminal_coverage == pytest.approx(3 / 4)
    assert metrics.abandonment_rate == pytest.approx(1 / 4)

    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,curve_progress,last_seen) "
        "VALUES ('MALFORMED',1.0,'CLIMBING',1.0,1.0)",
    )
    malformed_id = conn.execute(
        "INSERT INTO decisions(at,mint,segment,action,score,feature_vector_json,"
        "config_hash) VALUES (?,?,?,?,?,?,?)",
        (999.0, "MALFORMED", "CLIMBING", "SKIP", 0.0,
         '{"canonical":{"resolver_version":"wrong"}}', "c" * 64),
    ).lastrowid
    conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (malformed_id, "MALFORMED", 999.0, 1, 0, 0, None, None, "", "start_price_missing"),
    )
    conn.commit()
    with pytest.raises(EvidenceIntegrityError, match="malformed canonical decision"):
        compute_canonical_metrics(conn, horizon_s=horizon)
