import ast
import asyncio
import inspect
import math
from dataclasses import replace
from pathlib import Path

import pytest

from memebot.bus import EventBus
from memebot.events import CurveProgress, LifecycleTransition
from memebot.features import DEFAULT_MAX_MINTS, FeatureEngine


def test_feature_engine_rejects_removed_cap_seam():
    parameter = inspect.signature(FeatureEngine).parameters["max_feature_mints"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is inspect.Parameter.empty


def test_feature_fixtures_supply_mint_cap():
    tree = ast.parse(Path(__file__).read_text())
    omitted = []
    for function in (
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ):
        for call in ast.walk(function):
            if (
                not isinstance(call, ast.Call)
                or not isinstance(call.func, ast.Name)
                or call.func.id != "FeatureEngine"
            ):
                continue
            keywords = {keyword.arg: keyword.value for keyword in call.keywords}
            if "max_feature_mints" in keywords:
                continue
            omitted.append((function.name, call.lineno))

    assert omitted == []


def test_all_feature_callers_supply_mint_cap():
    repository = Path(__file__).parents[1]
    omitted = []
    callers = []

    def dotted_name(node):
        parts = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if not isinstance(node, ast.Name):
            return None
        return ".".join((node.id, *reversed(parts)))

    for path in sorted((
        *repository.joinpath("src").rglob("*.py"),
        *repository.joinpath("tests").rglob("*.py"),
    )):
        tree = ast.parse(path.read_text())
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        direct_names = set()
        qualified_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "memebot.features":
                direct_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "FeatureEngine"
                )
            elif isinstance(node, ast.ImportFrom) and node.module == "memebot":
                qualified_names.update(
                    f"{alias.asname or alias.name}.FeatureEngine"
                    for alias in node.names
                    if alias.name == "features"
                )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "memebot.features":
                        prefix = alias.asname or alias.name
                        qualified_names.add(f"{prefix}.FeatureEngine")

        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            is_constructor = (
                isinstance(call.func, ast.Name) and call.func.id in direct_names
            ) or dotted_name(call.func) in qualified_names
            if not is_constructor:
                continue

            relative_path = path.relative_to(repository).as_posix()
            function = parents.get(call)
            while function is not None and not isinstance(
                function, (ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                function = parents.get(function)
            function_name = function.name if function is not None else "<module>"
            callers.append((relative_path, function_name, call.lineno))
            keywords = {keyword.arg: keyword.value for keyword in call.keywords}
            if "max_feature_mints" in keywords:
                continue

            omitted.append((relative_path, function_name, call.lineno))

    assert callers
    assert omitted == []


def _cp(mint, t, real_sol_lamports, vsol=31_000_000_000, vtok=900_000_000_000_000, progress=12.0):
    return CurveProgress(t_wall=t, t_mono=t, mint=mint, progress_pct=progress,
                         virtual_sol_reserves=vsol, virtual_token_reserves=vtok,
                         real_sol_reserves=real_sol_lamports, real_token_reserves=0)


async def test_feature_subscription_unsubscribes_in_finally():
    bus = EventBus()
    engine = FeatureEngine(bus, max_feature_mints=DEFAULT_MAX_MINTS)
    subscription = next(item for item in bus._subs if item.queue is engine._q)
    assert subscription.critical is False

    task = asyncio.create_task(engine.run(asyncio.Event()))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert subscription.closed is True
    assert subscription.close_event.is_set() is True
    assert subscription not in bus._subs


def test_snapshot_at_or_before_excludes_future_and_conflicting_ties():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    selected = replace(
        _cp("M", t=10.0, real_sol_lamports=3_000_000_000,
            vsol=32_000_000_000, vtok=800_000_000_000_000, progress=40.0),
        real_token_reserves=700_000_000_000_000,
        source_boot_id=7,
        source_seq=1,
    )
    future = replace(
        selected,
        t_wall=30.0,
        t_mono=30.0,
        virtual_sol_reserves=99_000_000_000,
        real_sol_reserves=9_000_000_000,
        source_seq=2,
    )
    fe.observe(selected)
    fe.observe(future)

    snapshot = fe.snapshot_at_or_before("M", as_of=20.0)

    assert snapshot is not None
    assert snapshot.source_boot_id == 7
    assert snapshot.source_seq == 1
    assert snapshot.t_wall == 10.0
    assert snapshot.t_mono == 10.0
    assert snapshot.virtual_sol_reserves == 32_000_000_000
    assert snapshot.virtual_token_reserves == 800_000_000_000_000
    assert snapshot.real_sol_reserves == 3_000_000_000
    assert snapshot.real_token_reserves == 700_000_000_000_000
    assert snapshot.liquidity_sol == 3.0
    assert snapshot.spot_price_sol == pytest.approx(32.0 / (800_000_000_000_000 / 1e6))
    assert snapshot.progress_pct == 40.0
    assert snapshot.curve_state().virtual_sol_reserves == 32_000_000_000
    assert snapshot.curve_state().complete is False
    assert fe.latest_state("M").virtual_sol_reserves == 99_000_000_000
    for invalid_as_of in (True, -1.0, float("nan"), float("inf"), 10 ** 400):
        assert fe.snapshot_at_or_before("M", as_of=invalid_as_of) is None

    conflicting = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    conflicting.observe(selected)
    conflicting.observe(replace(selected, real_sol_reserves=4_000_000_000))
    assert conflicting.snapshot_at_or_before("M", as_of=20.0) is None

    for tied_real_sol in (selected.real_sol_reserves, 4_000_000_000):
        tied = FeatureEngine(bus=None, max_mints=2, max_feature_mints=2)
        tied_a = replace(selected, mint="tied-A")
        tied.observe(tied_a)
        tied.observe(replace(selected, mint="tied-B"))
        tied.observe(replace(tied_a, real_sol_reserves=tied_real_sol))
        assert tuple(tied._active_mints) == ("tied-B", "tied-A")
        tied.observe(replace(selected, mint="tied-C"))
        assert set(tied._series) == set(tied._snapshot_series) == {"tied-A", "tied-C"}

    bounded = FeatureEngine(bus=None, max_mints=2, max_feature_mints=2)
    mint_a = replace(selected, mint="A")
    mint_b = replace(selected, mint="B", t_wall=11.0, t_mono=11.0)
    bounded.observe(mint_a)
    bounded.observe(mint_b)
    bounded.observe(replace(mint_a, t_wall=12.0, t_mono=9.0, source_seq=2))
    assert tuple(bounded._active_mints) == ("B", "A")
    bounded.observe(replace(selected, mint="C", t_wall=13.0, t_mono=13.0))
    assert set(bounded._series) == set(bounded._snapshot_series) == {"A", "C"}
    assert bounded.snapshot_at_or_before("A", as_of=12.0).source_seq == 2

    for field, value in (
        ("virtual_sol_reserves", True),
        ("virtual_token_reserves", 1.5),
        ("virtual_sol_reserves", 0),
        ("real_sol_reserves", -1),
        ("real_token_reserves", -1),
    ):
        invalid = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
        invalid.observe(replace(selected, mint=f"invalid-{field}-{value}", **{field: value}))
        assert invalid.snapshot_at_or_before(
            f"invalid-{field}-{value}", as_of=20.0,
        ) is None


def test_snapshot_history_poison_blocks_older_favorable_fallback():
    valid = replace(
        _cp("M", t=10.0, real_sol_lamports=3_000_000_000,
            vsol=32_000_000_000, vtok=800_000_000_000_000, progress=40.0),
        real_token_reserves=700_000_000_000_000,
        source_boot_id=7,
        source_seq=1,
    )

    for bad_t_wall in (True, "15", float("nan"), float("inf")):
        poisoned = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
        poisoned.observe(valid)
        poisoned.observe(replace(valid, t_wall=bad_t_wall, t_mono=15.0, source_seq=2))
        assert poisoned.snapshot_at_or_before("M", as_of=20.0) is None

    malformed_relevant = (
        {"t_mono": True},
        {"t_mono": "15"},
        {"t_mono": float("nan")},
        {"source_boot_id": True},
        {"source_seq": -1},
        {"progress_pct": 101.0},
        {"virtual_sol_reserves": 0},
        {"virtual_token_reserves": 1.5},
        {"real_sol_reserves": -1},
        {"real_token_reserves": True},
    )
    for mutation in malformed_relevant:
        poisoned = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
        poisoned.observe(valid)
        ordinary_before = tuple(poisoned._series["M"])
        state_before = poisoned._states["M"]
        malformed_fields = {"t_wall": 15.0, "t_mono": 15.0, "source_seq": 2}
        malformed_fields.update(mutation)
        poisoned.observe(replace(valid, **malformed_fields))
        assert poisoned.snapshot_at_or_before("M", as_of=20.0) is None
        assert tuple(poisoned._series["M"]) == ordinary_before
        assert poisoned._states["M"] == state_before

    for mutation in (
        {"t_wall": True},
        {"source_boot_id": True},
        {"source_seq": -1},
        {"progress_pct": 101.0},
        {"virtual_sol_reserves": 0},
        {"real_sol_reserves": -1},
    ):
        strict_invalid = FeatureEngine(
            bus=None, max_mints=1, max_feature_mints=1,
        )
        malformed_fields = {
            "mint": "STRICT-INVALID",
            "t_wall": 5.0,
            "t_mono": 5.0,
            "source_seq": 1,
        }
        malformed_fields.update(mutation)
        strict_invalid.observe(replace(valid, **malformed_fields))
        assert tuple(strict_invalid._active_mints) == ("STRICT-INVALID",)
        assert tuple(strict_invalid._snapshot_series) == ("STRICT-INVALID",)
        assert "STRICT-INVALID" not in strict_invalid._series
        assert "STRICT-INVALID" not in strict_invalid._states
        assert strict_invalid.snapshot_at_or_before(
            "STRICT-INVALID", as_of=20.0,
        ) is None

    malformed_first = FeatureEngine(
        bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
    )
    malformed_first.observe(replace(
        valid,
        t_wall=5.0,
        t_mono=5.0,
        source_seq=0,
        real_sol_reserves=-1,
    ))
    malformed_first.observe(valid)
    assert malformed_first.snapshot_at_or_before("M", as_of=20.0) is None

    for bad_t_wall in (True, "15", float("nan"), float("inf")):
        poison_first = FeatureEngine(
            bus=None, maxlen=2, max_mints=1, max_feature_mints=1,
        )
        poison_first.observe(replace(
            valid,
            mint="POISON-FIRST",
            t_wall=bad_t_wall,
            t_mono=5.0,
            source_seq=0,
        ))
        assert tuple(poison_first._snapshot_series) == ("POISON-FIRST",)
        poison_first.observe(replace(valid, mint="POISON-FIRST"))
        assert poison_first.snapshot_at_or_before(
            "POISON-FIRST", as_of=20.0,
        ) is None
        poison_first.observe(replace(
            valid,
            mint="POISON-FIRST",
            t_wall=20.0,
            t_mono=20.0,
            source_seq=2,
        ))
        assert poison_first.snapshot_at_or_before(
            "POISON-FIRST", as_of=20.0,
        ).source_seq == 2

    for mutation in (
        {"t_mono": "5"},
        {"t_mono": float("nan")},
        {"progress_pct": "40"},
    ):
        poison_first = FeatureEngine(
            bus=None, maxlen=2, max_mints=1, max_feature_mints=1,
        )
        malformed_fields = {
            "mint": "PAYLOAD-POISON-FIRST",
            "t_wall": 5.0,
            "t_mono": 5.0,
            "source_seq": 0,
        }
        malformed_fields.update(mutation)
        poison_first.observe(replace(valid, **malformed_fields))
        assert tuple(poison_first._snapshot_series) == ("PAYLOAD-POISON-FIRST",)
        poison_first.observe(replace(valid, mint="PAYLOAD-POISON-FIRST"))
        assert poison_first.snapshot_at_or_before(
            "PAYLOAD-POISON-FIRST", as_of=20.0,
        ) is None
        poison_first.observe(replace(
            valid,
            mint="PAYLOAD-POISON-FIRST",
            t_wall=20.0,
            t_mono=20.0,
            source_seq=2,
        ))
        assert poison_first.snapshot_at_or_before(
            "PAYLOAD-POISON-FIRST", as_of=20.0,
        ).source_seq == 2

    mint_bounded = FeatureEngine(
        bus=None, max_mints=1, max_feature_mints=1,
    )
    for mint in ("POISON-A", "POISON-B"):
        mint_bounded.observe(replace(
            valid,
            mint=mint,
            t_wall=float("nan"),
            t_mono=5.0,
            source_seq=0,
        ))
    assert tuple(mint_bounded._active_mints) == ("POISON-B",)
    assert tuple(mint_bounded._snapshot_series) == ("POISON-B",)

    mixed_bounded = FeatureEngine(
        bus=None, max_mints=2, max_feature_mints=2,
    )
    mixed_bounded.observe(replace(valid, mint="MIXED-A"))
    mixed_bounded.observe(replace(valid, mint="MIXED-B"))
    mixed_bounded.observe(replace(
        valid,
        mint="MIXED-C",
        t_wall=15.0,
        t_mono="15",
        source_seq=2,
    ))
    retained_mints = (
        set(mixed_bounded._active_mints)
        | set(mixed_bounded._series)
        | set(mixed_bounded._states)
        | set(mixed_bounded._snapshot_series)
    )
    assert retained_mints == {"MIXED-B", "MIXED-C"}
    assert tuple(mixed_bounded._active_mints) == ("MIXED-B", "MIXED-C")
    assert "MIXED-A" not in mixed_bounded._series
    assert "MIXED-C" not in mixed_bounded._series

    terminal_poison = FeatureEngine(
        bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
    )
    terminal_poison.observe(replace(
        valid,
        mint="TERMINAL-POISON",
        t_wall=float("nan"),
        t_mono=5.0,
        source_seq=0,
    ))
    terminal_poison.on_transition(LifecycleTransition(
        t_wall=20.0,
        t_mono=20.0,
        mint="TERMINAL-POISON",
        from_state="CLIMBING",
        to_state="DEAD",
    ))
    assert "TERMINAL-POISON" not in terminal_poison._active_mints
    assert "TERMINAL-POISON" not in terminal_poison._snapshot_series

    future_poison = FeatureEngine(
        bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
    )
    future_poison.observe(valid)
    future_poison.observe(replace(
        valid,
        t_wall=30.0,
        t_mono=float("nan"),
        source_seq=2,
        virtual_sol_reserves=0,
    ))
    assert future_poison.snapshot_at_or_before("M", as_of=20.0).source_seq == 1
    assert future_poison.snapshot_at_or_before("M", as_of=40.0) is None

    bounded = FeatureEngine(
        bus=None, maxlen=2, max_feature_mints=DEFAULT_MAX_MINTS,
    )
    bounded.observe(valid)
    bounded.observe(replace(valid, t_wall=15.0, t_mono=15.0,
                            source_seq=2, real_sol_reserves=-1))
    bounded.observe(replace(valid, t_wall=20.0, t_mono=20.0, source_seq=3))
    assert bounded.snapshot_at_or_before("M", as_of=20.0) is None
    bounded.observe(replace(valid, t_wall=30.0, t_mono=30.0, source_seq=4))
    assert bounded.snapshot_at_or_before("M", as_of=30.0).source_seq == 4


def test_p3_snapshot_rejects_feature_first_lifecycle_lag():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    durable = replace(
        _cp("M", t=10.0, real_sol_lamports=8_000_000_000,
            vsol=39_000_000_000, vtok=800_000_000_000_000, progress=80.0),
        real_token_reserves=700_000_000_000_000,
        source_boot_id=7,
        source_seq=1,
    )
    fe.observe(durable)

    selected = fe.p3_snapshot_at_or_before(
        "M",
        as_of=20.0,
        durable_source_wall=10.0,
        durable_source_boot_id=7,
        durable_source_seq=1,
        durable_observed_at=12.0,
        runtime_boot_id=7,
        runtime_causal_floor=5.0,
    )
    assert selected is not None
    assert selected.source_boot_id == 7
    assert selected.source_seq == 1
    assert selected.t_wall == 10.0

    # FeatureEngine can consume the next poll before LifecycleTracker durably
    # acknowledges it. P3 must not reveal the older favorable durable sample.
    fe.observe(replace(
        durable,
        t_wall=15.0,
        t_mono=15.0,
        real_sol_reserves=1_000_000_000,
        progress_pct=10.0,
        source_seq=2,
    ))
    assert fe.snapshot_at_or_before("M", as_of=20.0).source_seq == 2
    assert fe.p3_snapshot_at_or_before(
        "M",
        as_of=20.0,
        durable_source_wall=10.0,
        durable_source_boot_id=7,
        durable_source_seq=1,
        durable_observed_at=12.0,
        runtime_boot_id=7,
        runtime_causal_floor=5.0,
    ) is None

    caught_up = fe.p3_snapshot_at_or_before(
        "M",
        as_of=20.0,
        durable_source_wall=15.0,
        durable_source_boot_id=7,
        durable_source_seq=2,
        durable_observed_at=16.0,
        runtime_boot_id=7,
        runtime_causal_floor=5.0,
    )
    assert caught_up is not None and caught_up.source_seq == 2

    valid = {
        "as_of": 20.0,
        "durable_source_wall": 15.0,
        "durable_source_boot_id": 7,
        "durable_source_seq": 2,
        "durable_observed_at": 16.0,
        "runtime_boot_id": 7,
        "runtime_causal_floor": 5.0,
    }
    for invalid_mint in ("", "   ", True, 7):
        invalid_mint_fe = FeatureEngine(
            bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
        )
        invalid_mint_fe.observe(replace(
            durable,
            mint=invalid_mint,
            t_wall=15.0,
            t_mono=15.0,
            source_seq=2,
        ))
        assert invalid_mint_fe.p3_snapshot_at_or_before(invalid_mint, **valid) is None

    for field, invalid_values in (
        (
            "durable_source_wall",
            (None, True, -1.0, 14.0, 16.0, float("nan"), float("inf")),
        ),
        ("durable_source_boot_id", (None, True, 0, 8)),
        ("durable_source_seq", (None, True, -1, 0, 1, 3)),
        (
            "durable_observed_at",
            (None, True, 5.0, 15.0, 21.0, float("nan"), float("inf")),
        ),
        ("runtime_boot_id", (True, 0, 8)),
        ("runtime_causal_floor", (True, -1.0, 16.0, float("nan"), float("inf"))),
    ):
        for value in invalid_values:
            kwargs = valid | {field: value}
            assert fe.p3_snapshot_at_or_before("M", **kwargs) is None


def test_p3_snapshot_rejects_newer_sequence_with_regressed_wall():
    def p3_snapshot(fe, *, source_wall=20.0, source_seq=1,
                    observed_at=25.0):
        return fe.p3_snapshot_at_or_before(
            "M",
            as_of=40.0,
            durable_source_wall=source_wall,
            durable_source_boot_id=7,
            durable_source_seq=source_seq,
            durable_observed_at=observed_at,
            runtime_boot_id=7,
            runtime_causal_floor=5.0,
        )

    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    durable = replace(
        _cp("M", t=20.0, real_sol_lamports=8_000_000_000,
            vsol=39_000_000_000, vtok=800_000_000_000_000, progress=80.0),
        real_token_reserves=700_000_000_000_000,
        source_boot_id=7,
        source_seq=1,
    )
    regressed = replace(
        durable,
        t_wall=10.0,
        t_mono=30.0,
        real_sol_reserves=1_000_000_000,
        progress_pct=10.0,
        source_seq=2,
    )
    fe.observe(durable)
    fe.observe(regressed)

    assert fe.snapshot_at_or_before("M", as_of=40.0).source_seq == 1
    assert p3_snapshot(fe) is None

    caught_up = p3_snapshot(
        fe, source_wall=10.0, source_seq=2, observed_at=35.0,
    )
    assert caught_up is not None
    assert caught_up.source_boot_id == 7
    assert caught_up.source_seq == 2
    assert caught_up.t_wall == 10.0

    conflicting = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    conflicting.observe(durable)
    conflicting.observe(replace(
        durable,
        t_mono=21.0,
        real_sol_reserves=7_000_000_000,
    ))
    assert p3_snapshot(conflicting) is None

    for invalid_source_seq in (0, True):
        invalid_identity = FeatureEngine(
            bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
        )
        invalid_identity.observe(durable)
        invalid_identity.observe(replace(
            durable,
            t_wall=50.0,
            t_mono=50.0,
            source_seq=invalid_source_seq,
        ))
        assert p3_snapshot(invalid_identity) is None

    wrong_boot_ahead = FeatureEngine(
        bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
    )
    wrong_boot_ahead.observe(durable)
    wrong_boot_ahead.observe(replace(
        durable,
        t_wall=10.0,
        t_mono=30.0,
        source_boot_id=8,
        source_seq=99,
    ))
    wrong_boot_result = p3_snapshot(wrong_boot_ahead)
    assert wrong_boot_result is not None
    assert wrong_boot_result.source_boot_id == 7
    assert wrong_boot_result.source_seq == 1

    wrong_boot_only = FeatureEngine(
        bus=None, max_feature_mints=DEFAULT_MAX_MINTS,
    )
    wrong_boot_only.observe(replace(durable, source_boot_id=8))
    assert p3_snapshot(wrong_boot_only) is None


def test_velocity_is_sol_locked_gained_per_second():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))   # 1.0 SOL
    fe.observe(_cp("M", t=10.0, real_sol_lamports=3_000_000_000))  # 3.0 SOL, +2 over 10s
    f = fe.features(
        "M",
        as_of=10.0,
        identity_ingested_at=0.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=1.0,
    )
    assert f is not None
    assert f.velocity_sol_per_s == pytest.approx(0.2)   # 2 SOL / 10 s
    assert f.age_s == pytest.approx(10.0)
    assert f.samples == 2


def test_features_support_fresh_asof_api_with_legacy_seam():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp(
        "M", t=10.0, real_sol_lamports=1_000_000_000, progress=10.0,
    ))
    fe.observe(_cp(
        "M", t=20.0, real_sol_lamports=3_000_000_000, progress=30.0,
    ))
    fe.observe(_cp(
        "M", t=30.0, real_sol_lamports=9_000_000_000, progress=90.0,
    ))

    fresh = fe.features(
        "M",
        as_of=25.0,
        identity_ingested_at=5.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=5.0,
    )

    assert fresh is not None
    assert fresh.velocity_sol_per_s == pytest.approx(0.2)
    assert fresh.curve_progress_pct == 30.0
    assert fresh.age_s == 20.0
    assert fresh.samples == 2
    assert fe.features(
        "M",
        as_of=25.0,
        identity_ingested_at=5.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=4.999,
    ) is None

    legacy = fe.features(
        "M",
        as_of=30.0,
        identity_ingested_at=5.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=1.0,
    )
    assert legacy is not None
    assert legacy.age_s == 25.0
    assert legacy.samples == 3

    poisoned = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    poisoned.observe(_cp(
        "POISONED", t=10.0, real_sol_lamports=1_000_000_000,
    ))
    poisoned.observe(_cp(
        "POISONED", t=20.0, real_sol_lamports=3_000_000_000,
    ))
    poisoned.observe(replace(
        _cp("POISONED", t=22.0, real_sol_lamports=4_000_000_000),
        real_sol_reserves=-1,
    ))
    assert poisoned.features(
        "POISONED",
        as_of=25.0,
        identity_ingested_at=5.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=5.0,
    ) is None

    conflicting = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    tied = _cp("CONFLICT", t=10.0, real_sol_lamports=1_000_000_000)
    conflicting.observe(tied)
    conflicting.observe(_cp(
        "CONFLICT", t=20.0, real_sol_lamports=3_000_000_000,
    ))
    conflicting.observe(replace(
        tied,
        real_sol_reserves=2_000_000_000,
    ))
    assert conflicting.features(
        "CONFLICT",
        as_of=25.0,
        identity_ingested_at=5.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=15.0,
    ) is None

    regressed = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    regressed.observe(_cp(
        "REGRESSED", t=10.0, real_sol_lamports=1_000_000_000,
    ))
    regressed.observe(replace(
        _cp("REGRESSED", t=20.0, real_sol_lamports=3_000_000_000),
        t_wall=24.0,
    ))
    regressed.observe(replace(
        _cp("REGRESSED", t=30.0, real_sol_lamports=9_000_000_000),
        t_wall=15.0,
    ))
    assert regressed.features(
        "REGRESSED",
        as_of=25.0,
        identity_ingested_at=5.0,
        risk_score=20.0,
        min_samples=2,
        max_latest_age_s=5.0,
    ) is None


def test_features_rejects_removed_now_keyword():
    parameters = inspect.signature(FeatureEngine.features).parameters
    assert tuple(parameters) == (
        "self",
        "mint",
        "as_of",
        "identity_ingested_at",
        "risk_score",
        "min_samples",
        "max_latest_age_s",
    )
    assert parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["mint"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    for name in (
        "as_of",
        "identity_ingested_at",
        "risk_score",
        "min_samples",
        "max_latest_age_s",
    ):
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[name].default is inspect.Parameter.empty
    assert "now" not in parameters
    assert "created_at" not in parameters

    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)

    with pytest.raises(TypeError, match="unexpected keyword argument 'now'"):
        fe.features(
            "M",
            as_of=1.0,
            identity_ingested_at=0.0,
            risk_score=20.0,
            min_samples=2,
            max_latest_age_s=1.0,
            now=1.0,
        )
    with pytest.raises(TypeError, match="unexpected keyword argument 'created_at'"):
        fe.features(
            "M",
            as_of=1.0,
            identity_ingested_at=0.0,
            risk_score=20.0,
            min_samples=2,
            max_latest_age_s=1.0,
            created_at=0.0,
        )


def test_features_none_below_min_samples():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    assert fe.features(
        "M",
        as_of=1.0,
        identity_ingested_at=0.0,
        risk_score=20.0,
        min_samples=3,
        max_latest_age_s=1.0,
    ) is None


def test_non_increasing_snapshots_never_warm_features_or_replace_latest_state():
    """Duplicate/reordered delivery is not a distinct market observation in either race order."""
    for feature_engine_first in (False, True):
        for non_increasing_t in (10.0, 5.0):
            fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
            fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000, vsol=31_000_000_000))
            fe.observe(_cp("M", t=10.0, real_sol_lamports=2_000_000_000,
                           vsol=32_000_000_000))
            stale = _cp("M", t=non_increasing_t, real_sol_lamports=9_000_000_000,
                        vsol=99_000_000_000)

            if feature_engine_first:
                fe.observe(stale)
            assert fe.features_including(
                stale, now=10.0, created_at=0.0, risk_score=0.0, min_samples=3,
            ) is None
            if not feature_engine_first:
                fe.observe(stale)

            assert fe.features(
                "M",
                as_of=10.0,
                identity_ingested_at=0.0,
                risk_score=0.0,
                min_samples=3,
                max_latest_age_s=1.0,
            ) is None
            assert fe.latest_state("M").virtual_sol_reserves == 32_000_000_000

            newer = _cp("M", t=20.0, real_sol_lamports=3_000_000_000,
                        vsol=33_000_000_000)
            if feature_engine_first:
                fe.observe(newer)
            qualified = fe.features_including(
                newer, now=20.0, created_at=0.0, risk_score=0.0, min_samples=3,
            )
            assert qualified is not None and qualified.samples == 3


def test_features_including_is_strict_as_of_event_with_buffered_future_tail():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000,
                   vsol=31_000_000_000, progress=10.0))
    fe.observe(_cp("M", t=20.0, real_sol_lamports=9_000_000_000,
                   vsol=99_000_000_000, progress=90.0))
    decision_tick = _cp("M", t=10.0, real_sol_lamports=3_000_000_000,
                        vsol=40_000_000_000, progress=40.0)

    feats = fe.features_including(
        decision_tick, now=10.0, created_at=0.0, risk_score=0.0, min_samples=2,
    )

    assert feats is not None
    assert feats.samples == 2
    assert feats.velocity_sol_per_s == pytest.approx(0.2)
    assert feats.curve_progress_pct == 40.0
    assert feats.spot_price_sol == pytest.approx(
        40.0 / (900_000_000_000_000 / 1e6)
    )


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize(
    "field",
    [
        "t_wall",
        "t_mono",
        "progress_pct",
        "virtual_sol_reserves",
        "virtual_token_reserves",
        "real_sol_reserves",
        "real_token_reserves",
    ],
)
def test_non_finite_curve_inputs_fail_closed_without_poisoning_features(field, non_finite):
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    first = _cp("M", t=0.0, real_sol_lamports=1_000_000_000,
                vsol=31_000_000_000, progress=10.0)
    second = _cp("M", t=10.0, real_sol_lamports=3_000_000_000,
                 vsol=33_000_000_000, progress=30.0)
    fe.observe(first)
    fe.observe(second)
    before_series = tuple(fe._series["M"])
    before_state = fe.latest_state("M")

    values = {
        "t_wall": 20.0,
        "t_mono": 20.0,
        "mint": "M",
        "progress_pct": 99.0,
        "virtual_sol_reserves": 99_000_000_000,
        "virtual_token_reserves": 900_000_000_000_000,
        "real_sol_reserves": 99_000_000_000,
        "real_token_reserves": 0,
    }
    values[field] = non_finite
    invalid = CurveProgress(**values)

    assert fe.features_including(
        invalid, now=20.0, created_at=0.0, risk_score=0.0, min_samples=2,
    ) is None
    fe.observe(invalid)
    assert tuple(fe._series["M"]) == before_series
    assert fe.latest_state("M") == before_state
    fe.observe(replace(invalid, mint="NEW"))
    assert "NEW" not in fe._series
    assert "NEW" not in fe._states

    assert fe.features(
        "M",
        as_of=20.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=10.0,
    ) is None
    features = fe.features_as_of(
        "M",
        t_mono=20.0,
        now=20.0,
        created_at=0.0,
        risk_score=0.0,
        min_samples=2,
    )
    assert features is not None
    assert all(math.isfinite(value) for value in (
        features.velocity_sol_per_s,
        features.curve_progress_pct,
        features.spot_price_sol,
    ))
    if field == "t_mono":
        assert fe.features_as_of(
            "M", t_mono=non_finite, now=20.0, created_at=0.0,
            risk_score=0.0, min_samples=2,
        ) is None
        assert fe.state_as_of("M", t_mono=non_finite) is None


def test_latest_price_tracks_last_snapshot():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    p = fe.latest_price("M")
    assert p == pytest.approx(31.0 / (900_000_000_000_000 / 1e6))  # spot from vSOL/vTok


def test_terminal_transition_evicts_buffer():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.on_transition(LifecycleTransition(t_wall=1.0, t_mono=1.0, mint="M",
                                         from_state="CLIMBING", to_state="DEAD"))
    assert fe.latest_price("M") is None


def test_terminal_tombstones_ignore_1000_late_mints_and_are_bounded_lru(monkeypatch):
    fe = FeatureEngine(
        bus=None,
        max_terminal_mints=1_000,
        max_feature_mints=DEFAULT_MAX_MINTS,
    )

    for index in range(1_000):
        mint = f"M{index}"
        observed_at = float(index * 2)
        fe.observe(_cp(mint, t=observed_at, real_sol_lamports=1_000_000_000))
        fe.on_transition(LifecycleTransition(
            t_wall=observed_at + 1.0,
            t_mono=observed_at + 1.0,
            mint=mint,
            from_state="CLIMBING",
            to_state="DEAD" if index % 2 == 0 else "GRADUATED",
        ))
        # Delivered after the terminal event, but timestamped before it.
        fe.observe(_cp(mint, t=observed_at + 0.5, real_sol_lamports=9_000_000_000))

    assert len(fe._terminal_mints) == 1_000
    assert fe._series == {}
    assert fe._states == {}
    assert fe._active_mints == {}

    bounded = FeatureEngine(
        bus=None,
        max_terminal_mints=2,
        max_feature_mints=DEFAULT_MAX_MINTS,
    )
    for mint in ("A", "B"):
        bounded.on_transition(LifecycleTransition(
            t_wall=1.0, t_mono=1.0, mint=mint,
            from_state="CLIMBING", to_state="DEAD",
        ))
    bounded.on_transition(LifecycleTransition(
        t_wall=2.0, t_mono=2.0, mint="A",
        from_state="CLIMBING", to_state="DEAD",
    ))
    bounded.on_transition(LifecycleTransition(
        t_wall=3.0, t_mono=3.0, mint="C",
        from_state="CLIMBING", to_state="GRADUATED",
    ))

    assert tuple(bounded._terminal_mints) == ("A", "C")
    retained_late = _cp("A", t=4.0, real_sol_lamports=9_000_000_000)
    bounded.observe(retained_late)
    monkeypatch.setattr(
        bounded,
        "_features_from_samples",
        lambda *args, **kwargs: pytest.fail("retained terminal reached feature evaluation"),
    )
    assert bounded.features_including(
        retained_late, now=4.0, created_at=0.0, risk_score=0.0, min_samples=1,
    ) is None
    bounded.observe(_cp("B", t=4.0, real_sol_lamports=9_000_000_000))
    assert "A" not in bounded._series
    assert set(bounded._series) == set(bounded._states) == {"B"}


def test_terminal_lru_overflow_does_not_disable_unseen_mints_at_production_defaults():
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)

    for index in range(DEFAULT_MAX_MINTS + 1):
        fe.on_transition(LifecycleTransition(
            t_wall=float(index),
            t_mono=float(index),
            mint=f"TERMINAL-{index}",
            from_state="CLIMBING",
            to_state="DEAD" if index % 2 == 0 else "GRADUATED",
        ))

    assert len(fe._terminal_mints) == DEFAULT_MAX_MINTS
    fe.observe(_cp("BRAND_NEW", t=1.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("BRAND_NEW", t=2.0, real_sol_lamports=2_000_000_000))

    features = fe.features(
        "BRAND_NEW",
        as_of=2.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=1.0,
    )
    assert features is not None
    assert features.samples == 2


def test_terminal_and_active_caps_do_not_resurrect_or_evict_each_other():
    fe = FeatureEngine(
        bus=None,
        max_mints=1,
        max_terminal_mints=1,
        max_feature_mints=1,
    )
    fe.observe(_cp("A", t=0.0, real_sol_lamports=1_000_000_000))
    fe.on_transition(LifecycleTransition(
        t_wall=1.0, t_mono=1.0, mint="A",
        from_state="CLIMBING", to_state="DEAD",
    ))
    fe.observe(_cp("B", t=2.0, real_sol_lamports=2_000_000_000))
    state_b = fe.latest_state("B")

    fe.observe(_cp("A", t=0.5, real_sol_lamports=9_000_000_000))

    assert tuple(fe._active_mints) == ("B",)
    assert tuple(fe._terminal_mints) == ("A",)
    assert fe.latest_state("B") == state_b
    assert fe.latest_state("A") is None

    fe.on_transition(LifecycleTransition(
        t_wall=3.0, t_mono=3.0, mint="B",
        from_state="CLIMBING", to_state="GRADUATED",
    ))
    fe.observe(_cp("B", t=2.5, real_sol_lamports=9_000_000_000))

    assert fe._active_mints == {}
    assert fe._series == {}
    assert fe._states == {}
    assert tuple(fe._terminal_mints) == ("B",)


def test_active_mint_cap_uses_observation_lru_and_eviction_requires_full_rewarm():
    with pytest.raises(ValueError, match="max_mints must be positive"):
        FeatureEngine(
            bus=None, max_mints=0, max_feature_mints=DEFAULT_MAX_MINTS,
        )

    fe = FeatureEngine(
        bus=None, maxlen=2, max_mints=2, max_feature_mints=2,
    )
    fe.observe(_cp("A", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("A", t=10.0, real_sol_lamports=2_000_000_000))
    fe.observe(_cp("B", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("A", t=20.0, real_sol_lamports=3_000_000_000))
    assert tuple(fe._active_mints) == ("B", "A")
    assert tuple(sample[0] for sample in fe._series["A"]) == (10.0, 20.0)

    # Every retained row refreshes sample recency, while malformed C never enters the
    # ordinary feature buffer.
    fe.observe(_cp("B", t=0.0, real_sol_lamports=9_000_000_000))
    fe.observe(replace(
        _cp("C", t=30.0, real_sol_lamports=9_000_000_000),
        virtual_sol_reserves=float("nan"),
    ))
    assert tuple(fe._active_mints) == ("B", "C")
    assert "A" not in fe._series
    assert "C" not in fe._series

    fe.observe(_cp("A", t=21.0, real_sol_lamports=3_100_000_000))
    fe.observe(_cp("C", t=30.0, real_sol_lamports=1_000_000_000))
    assert tuple(fe._active_mints) == ("A", "C")
    assert set(fe._series) == set(fe._states) == set(fe._active_mints) == {"A", "C"}
    assert max(len(fe._series), len(fe._states), len(fe._active_mints)) == 2

    fe.observe(_cp("B", t=40.0, real_sol_lamports=4_000_000_000))
    assert "A" not in fe._series and "A" not in fe._states
    assert fe.features(
        "B",
        as_of=40.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=1.0,
    ) is None

    fe.observe(_cp("B", t=50.0, real_sol_lamports=5_000_000_000))
    rewarmed = fe.features(
        "B",
        as_of=50.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=1.0,
    )
    assert rewarmed is not None and rewarmed.samples == 2


def test_feature_mint_lru_cap_bounds_lifetime_churn_fail_closed():
    for invalid_cap in (True, 1.0, 2.5, "2", [], 0, -1):
        with pytest.raises(ValueError, match="max_feature_mints must be positive"):
            FeatureEngine(bus=None, max_feature_mints=invalid_cap)

    fe = FeatureEngine(bus=None, maxlen=2, max_feature_mints=2)

    def observed(mint, *, t, source_seq, real_sol):
        fe.observe(replace(
            _cp(mint, t=t, real_sol_lamports=real_sol),
            source_boot_id=7,
            source_seq=source_seq,
        ))

    observed("A", t=10.0, source_seq=1, real_sol=1_000_000_000)
    observed("A", t=20.0, source_seq=2, real_sol=2_000_000_000)
    observed("B", t=10.0, source_seq=1, real_sol=1_000_000_000)
    observed("B", t=20.0, source_seq=2, real_sol=2_000_000_000)
    assert tuple(fe._active_mints) == ("A", "B")

    # Resolver and feature reads never refresh sample-order recency.
    assert fe.latest_price("A") is not None
    assert fe.latest_state("A") is not None
    assert fe.snapshot_at_or_before("A", as_of=20.0) is not None
    assert fe.features(
        "A",
        as_of=20.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=1.0,
    ) is not None
    assert tuple(fe._active_mints) == ("A", "B")

    observed("C", t=30.0, source_seq=1, real_sol=3_000_000_000)
    assert tuple(fe._active_mints) == ("B", "C")
    assert fe.latest_price("A") is None
    assert fe.latest_state("A") is None
    assert fe.snapshot_at_or_before("A", as_of=40.0) is None
    assert fe.p3_snapshot_at_or_before(
        "A",
        as_of=40.0,
        durable_source_wall=20.0,
        durable_source_boot_id=7,
        durable_source_seq=2,
        durable_observed_at=25.0,
        runtime_boot_id=7,
        runtime_causal_floor=5.0,
    ) is None

    # Reacquisition starts empty: old samples and their durable handshake cannot
    # reappear, and ordinary features must fully rewarm.
    observed("A", t=40.0, source_seq=3, real_sol=4_000_000_000)
    assert fe.features(
        "A",
        as_of=40.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=1.0,
    ) is None
    assert fe.p3_snapshot_at_or_before(
        "A",
        as_of=45.0,
        durable_source_wall=20.0,
        durable_source_boot_id=7,
        durable_source_seq=2,
        durable_observed_at=25.0,
        runtime_boot_id=7,
        runtime_causal_floor=5.0,
    ) is None
    assert fe.p3_snapshot_at_or_before(
        "A",
        as_of=45.0,
        durable_source_wall=40.0,
        durable_source_boot_id=7,
        durable_source_seq=3,
        durable_observed_at=42.0,
        runtime_boot_id=7,
        runtime_causal_floor=5.0,
    ) is not None

    observed("A", t=50.0, source_seq=4, real_sol=5_000_000_000)
    assert fe.features(
        "A",
        as_of=50.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=1.0,
    ) is not None

    for index in range(100):
        observed(
            f"CHURN-{index}",
            t=60.0 + index,
            source_seq=1,
            real_sol=6_000_000_000 + index,
        )
        retained = (
            set(fe._active_mints)
            | set(fe._series)
            | set(fe._states)
            | set(fe._snapshot_series)
        )
        assert retained == set(fe._active_mints)
        assert len(retained) <= 2


def test_latest_state_returns_curvestate():
    from memebot.features import FeatureEngine
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    st = fe.latest_state("M")
    assert st is not None and st.virtual_sol_reserves == 31_000_000_000
