import asyncio
from contextlib import contextmanager
from dataclasses import replace
import json
import math

import pytest

from memebot.broker import PaperBroker
from memebot.bus import EventBus
from memebot.canonical import (CanonicalObservationDraft, CanonicalResolution,
                               CanonicalVerdict)
from memebot.events import (CandidateScored, CanonicalObservationStarted, CurveProgress,
                            LifecycleTransition, PaperEntry, PaperExit, SafetyHardFail,
                            SafetyPassed)
from memebot.features import ClimbingFeatures, DEFAULT_MAX_MINTS, FeatureEngine
from memebot.scoring import ConfluenceScorer
from memebot.store import (open_db as _open_db, set_terminal_state_with_reputation,
                           set_token_state, upsert_token_identity)
from tests.test_e2e_climbing import (
    _DETERMINISTIC_CANONICAL_RESOLVER as _E2E_CANONICAL_RESOLVER,
    _E2E_CONFIG_HASH,
)

SCORER_CFG = {"weights_version": "climbing-v1", "w_velocity": 0.55, "w_progress": 0.20,
              "w_age": 0.05, "w_risk": 0.20, "velocity_full_scale_sol_per_s": 0.05,
              "progress_full_scale_pct": 80.0, "age_full_scale_s": 600.0}
SMART_SCORER_CFG = {**SCORER_CFG, "w_smart_money": 0.15,
                    "smart_money_quality_full_scale_sol": 5.0}
SMART_MONEY_CFG = {"min_events": 1, "min_realized_pnl_sol": 1.0}
STRAT_CFG = {"entries_enabled": True, "score_threshold": 40.0, "position_size_sol": 0.2,
             "max_concurrent_positions": 5, "max_entries_per_hour": 10, "min_samples": 2,
             "min_age_s": 0.0}
FILL_CFG = {"latency_min_s": 0.0, "extra_slippage_bps": 50, "priority_fee_sol": 0.0005,
            "solana_base_fee_sol": 0.000005, "grade_a_max_impact_pct": 2.0,
            "grade_b_max_impact_pct": 5.0, "grade_c_max_impact_pct": 10.0}
PUMP_CFG = {"protocol_fee_bps": 95, "creator_fee_bps": 30, "token_decimals": 6,
            "sellable_supply": 793_100_000.0}
_DETERMINISTIC_CANONICAL_RESOLVER = _E2E_CANONICAL_RESOLVER


def open_db(path):
    return _open_db(path, migration_clock=lambda: 0.0)


def upsert_token(conn, *, mint, created_at, bonding_curve_key=""):
    upsert_token_identity(
        conn,
        mint=mint,
        raw_ingested_at=created_at,
        bonding_curve_key=bonding_curve_key,
        fields={},
    )


def test_strategy_feature_fixtures_supply_mint_cap():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    omitted = []
    for function in (
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ):
        for call in ast.walk(function):
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "FeatureEngine"
                and "max_feature_mints"
                not in {keyword.arg for keyword in call.keywords}
            ):
                omitted.append((function.name, call.lineno))

    assert omitted == []


def test_strategy_uses_exact_asof_feature_call():
    import ast
    from pathlib import Path

    tree = ast.parse(Path("src/memebot/strategy.py").read_text())
    feature_calls = []
    legacy_calls = []
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        if not (
            isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Attribute)
            and isinstance(call.func.value.value, ast.Name)
            and call.func.value.value.id == "self"
            and call.func.value.attr == "_fe"
        ):
            continue
        if call.func.attr == "features":
            feature_calls.append(call)
        elif call.func.attr in {"features_as_of", "features_including"}:
            legacy_calls.append((call.func.attr, call.lineno))

    assert legacy_calls == []
    assert len(feature_calls) == 1
    call = feature_calls[0]
    keywords = {keyword.arg: keyword.value for keyword in call.keywords}
    assert set(keywords) == {
        "as_of",
        "identity_ingested_at",
        "risk_score",
        "min_samples",
        "max_latest_age_s",
    }
    as_of = keywords["as_of"]
    assert isinstance(as_of, ast.Name) and as_of.id == "decision_at"
    identity = keywords["identity_ingested_at"]
    assert (
        isinstance(identity, ast.Subscript)
        and isinstance(identity.value, ast.Name)
        and identity.value.id == "row"
        and isinstance(identity.slice, ast.Constant)
        and identity.slice.value == "p3_identity_ingested_at"
    )
    max_latest_age = keywords["max_latest_age_s"]
    assert (
        isinstance(max_latest_age, ast.Attribute)
        and isinstance(max_latest_age.value, ast.Name)
        and max_latest_age.value.id == "self"
        and max_latest_age.attr == "_stale_after_s"
    )


def _cp(mint, t, real_sol_lamports, vsol=31_000_000_000):
    return CurveProgress(t_wall=t, t_mono=t, mint=mint, progress_pct=20.0,
                         virtual_sol_reserves=vsol,
                         virtual_token_reserves=900_000_000_000_000,
                         real_sol_reserves=real_sol_lamports, real_token_reserves=0)


def _seed_report(conn):
    from memebot.store import save_safety_report
    return save_safety_report(conn, mint="M", raw_completed_at=1.0, segment="CLIMBING",
                              hard_fails=[], risk_score=0.0, results_json="[]", inputs_hash=(
                                  "de26c9b8f19b83a61809e4dffff12d3766678446fe061b0a7932319ed83b4a01"
                              ))


def _make(conn, bus, clock, *, scorer_cfg=None, smart_money_cfg=None, broker=None,
          canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER, strat_cfg=STRAT_CFG,
          mono_clock=lambda: 10.0):
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    from memebot.strategy import ClimbingStrategy
    return ClimbingStrategy(bus, conn, feature_engine=fe,
                            scorer=ConfluenceScorer(scorer_cfg or SCORER_CFG),
                            broker=broker or PaperBroker(FILL_CFG, PUMP_CFG),
                            canonical_resolver=canonical_resolver,
                            strat_cfg=strat_cfg, pumpfun_cfg=PUMP_CFG,
                            config_hash=_E2E_CONFIG_HASH, fill_cfg=FILL_CFG, clock=clock,
                            mono_clock=mono_clock,
                            smart_money_cfg=smart_money_cfg), fe


def test_strategy_accepts_optional_resolver_injection_seam():
    import inspect

    from memebot.strategy import ClimbingStrategy

    parameters = list(inspect.signature(ClimbingStrategy).parameters.values())
    names = [parameter.name for parameter in parameters]
    resolver_index = names.index("canonical_resolver")
    assert names[resolver_index - 1:resolver_index + 2] == [
        "broker",
        "canonical_resolver",
        "strat_cfg",
    ]
    assert parameters[resolver_index].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters[resolver_index].default is None

    resolver = object()
    strategy = ClimbingStrategy(
        None,
        object(),
        feature_engine=object(),
        scorer=object(),
        broker=object(),
        strat_cfg={},
        pumpfun_cfg={},
        config_hash=_E2E_CONFIG_HASH,
        fill_cfg={},
        canonical_resolver=resolver,
    )

    assert strategy._canonical_resolver is resolver


@pytest.mark.asyncio
async def test_entries_enabled_without_resolver_fails_closed():
    for unsafe_cfg in (
        {"entries_enabled": True},
        {"entries_enabled": None},
        {"entries_enabled": 0},
        {},
    ):
        with pytest.raises(
            ValueError,
            match="canonical_resolver is required when entries are enabled",
        ):
            _make(
                None,
                None,
                lambda: 0.0,
                canonical_resolver=None,
                strat_cfg=unsafe_cfg,
            )

    watch_cfg = {"entries_enabled": False}
    strategy, _ = _make(
        None,
        None,
        lambda: 0.0,
        canonical_resolver=None,
        strat_cfg=watch_cfg,
    )
    assert strategy._pending == {}
    assert strategy._pending_score == {}

    watch_cfg["entries_enabled"] = True
    with pytest.raises(
        ValueError,
        match="canonical_resolver is required when entries are enabled",
    ):
        await strategy._try_score(
            SafetyPassed(
                t_wall=1.0,
                t_mono=1.0,
                mint="M",
                segment="CLIMBING",
                safety_report_id=1,
                risk_score=0.0,
            ),
            decision_t_mono=1.0,
        )
    assert strategy._pending == {}
    assert strategy._pending_score == {}


def test_all_entry_enabled_strategy_fixtures_supply_resolver():
    import ast
    from pathlib import Path

    fixture_names = (
        "_make",
        "_make_with_exits",
        "_make_with_exits2",
        "test_feature_eviction_rewarms_before_decision_and_never_mutates_trade_state",
        "test_evicted_terminal_feature_cache_cannot_revive_scoring_or_trading",
    )
    tree = ast.parse(Path(__file__).read_text())
    fixture_calls = []
    omitted = []
    unexpected = []
    for function in (
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in fixture_names
    ):
        for call in ast.walk(function):
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "ClimbingStrategy"
            ):
                continue
            fixture_calls.append(function.name)
            resolver = next(
                (
                    keyword.value
                    for keyword in call.keywords
                    if keyword.arg == "canonical_resolver"
                ),
                None,
            )
            if resolver is None:
                omitted.append(function.name)
            elif function.name == "_make":
                defaults = {
                    arg.arg: default
                    for arg, default in zip(
                        function.args.kwonlyargs,
                        function.args.kw_defaults,
                    )
                }
                resolver_default = defaults["canonical_resolver"]
                if not (
                    isinstance(resolver, ast.Name)
                    and resolver.id == "canonical_resolver"
                    and isinstance(resolver_default, ast.Name)
                    and resolver_default.id == "_DETERMINISTIC_CANONICAL_RESOLVER"
                ):
                    unexpected.append(function.name)
            elif not (
                isinstance(resolver, ast.Name)
                and resolver.id == "_DETERMINISTIC_CANONICAL_RESOLVER"
            ):
                unexpected.append(function.name)

    assert sorted(fixture_calls) == sorted(fixture_names)
    assert omitted == []
    assert unexpected == []


class _FlowResolver:
    def __init__(self, *, status="CANONICAL", reason="canonical_selected",
                 expected_transaction=None):
        self.status = status
        self.reason = reason
        self.expected_transaction = expected_transaction
        self.calls = []

    def resolve(self, mint, *, decision_at, target_report_id, target_snapshot=None):
        if self.expected_transaction is not None:
            assert self.expected_transaction["active"] is True
        self.calls.append((mint, decision_at, target_report_id, target_snapshot))
        snapshot = {
            "t_wall": 10.0,
            "t_mono": 10.0,
            "virtual_sol_reserves": 31_000_000_000,
            "virtual_token_reserves": 900_000_000_000_000,
            "real_sol_reserves": 6_000_000_000,
            "real_token_reserves": 0,
            "spot_price_sol": 31_000_000_000 / 900_000_000_000_000 / 1_000_000,
        }
        candidate = {
            "mint": mint,
            "raw": {
                "curve_snapshot": snapshot,
                "curve_progress_pct": 20.0,
                "liquidity_sol": 6.0,
            },
        }
        canonical = self.status == "CANONICAL"
        return CanonicalResolution(
            verdict=CanonicalVerdict(
                resolver_version="fixture-v1",
                weights_version="fixture-v1",
                status=self.status,
                reason=self.reason,
                resolved_at=decision_at,
                cluster_key="fixture:fx",
                cluster_size=1,
                eligible_cluster_size=1,
                canonical_mint=mint if canonical else "PEER",
                rank=1 if canonical else 2,
                rank_points=1 if canonical else 0,
                generation_hash="a" * 64,
                inputs_hash="b" * 64,
                ranking_inputs={"candidates": [candidate]},
            ),
            observations=(CanonicalObservationDraft(
                mint=mint,
                is_subject=True,
                is_canonical=canonical,
                eligible=True,
                start_price_sol=snapshot["spot_price_sol"],
                price_observed_at=snapshot["t_wall"],
                unavailable_reason="",
            ),),
        )


def _atomic_flow_strategy(monkeypatch, *, bus, resolver, allocated_at=100.0,
                          mono_clock=lambda: 500.0, order=None):
    order = [] if order is None else order
    transaction = {"active": False}
    resolver.expected_transaction = transaction

    @contextmanager
    def immediate(_conn):
        assert transaction["active"] is False
        transaction["active"] = True
        order.append("begin")
        try:
            yield
        finally:
            transaction["active"] = False
            order.append("commit")

    def allocate(_conn, *, raw_wall):
        assert transaction["active"] is True
        order.append(("allocate", raw_wall))
        return allocated_at

    written = {}

    def write(_conn, **kwargs):
        assert transaction["active"] is True
        order.append("write")
        written.update(kwargs)
        return 41, (51,), True

    monkeypatch.setattr("memebot.strategy.p3_immediate_transaction", immediate)
    monkeypatch.setattr("memebot.strategy.allocate_p3_causal_wall", allocate)
    monkeypatch.setattr(
        "memebot.strategy.record_decision_with_canonical_observations", write,
    )
    monkeypatch.setattr(
        "memebot.strategy.decision_exists", lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        "memebot.strategy.get_token",
        lambda *_args, **_kwargs: {"p3_identity_ingested_at": 1.0},
    )

    class MonoClock:
        def __call__(self):
            order.append("mono")
            return mono_clock()

    strategy, feature_engine = _make(
        object(), bus, clock=lambda: 90.0, canonical_resolver=resolver,
    )
    strategy._mono_clock = MonoClock()
    strategy._current_safety_pass = lambda event: event
    feature_engine.features = lambda *_args, **_kwargs: ClimbingFeatures(
        velocity_sol_per_s=0.1,
        curve_progress_pct=20.0,
        age_s=99.0,
        risk_score=0.0,
        spot_price_sol=1e-9,
        samples=2,
    )
    return strategy, feature_engine, written, order, transaction


async def _score_atomic_flow(strategy, *, current=None):
    return await strategy._try_score(
        SafetyPassed(
            t_wall=1.0,
            t_mono=-1_000.0,
            mint="M",
            segment="CLIMBING",
            safety_report_id=7,
            risk_score=99.0,
        ),
        decision_t_mono=-1_000.0,
        current=current,
    )


async def test_strategy_atomic_skip_and_observation_publication(monkeypatch):
    bus = EventBus()
    scored = bus.subscribe(CandidateScored)
    observations = bus.subscribe(CanonicalObservationStarted)
    resolver = _FlowResolver(status="SUPPRESSED", reason="copycat_cluster")
    strategy, _, written, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=bus, resolver=resolver,
    )

    assert await _score_atomic_flow(strategy) is True
    assert written["action"] == "SKIP"
    assert written["planned_position_size_sol"] == STRAT_CFG["position_size_sol"]
    assert strategy._pending == {}
    assert (await observations.get()).observation_id == 51
    assert (await scored.get()).decision_id == 41


async def test_decision_quote_uses_persisted_asof_reserves_not_future_state(monkeypatch):
    bus = EventBus()
    resolver = _FlowResolver()
    strategy, feature_engine, _, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=bus, resolver=resolver,
    )
    feature_engine.state_as_of = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("future feature state must not set the decision quote")
    )

    assert await _score_atomic_flow(strategy) is True
    pending = strategy._pending["M"]
    assert pending.decision_snapshot.virtual_sol_reserves == 31_000_000_000
    assert pending.decision_snapshot.virtual_token_reserves == 900_000_000_000_000


async def test_latency_starts_after_commit_not_old_safety_event(monkeypatch):
    bus = EventBus()
    order = []
    strategy, _, _, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=bus, resolver=_FlowResolver(), order=order,
        mono_clock=lambda: 700.0,
    )

    assert await _score_atomic_flow(strategy) is True
    assert order.index("commit") < order.index("mono")
    assert strategy._pending["M"].decision_mono == 700.0
    assert strategy._pending["M"].decision_mono != -1_000.0


async def test_regressed_wall_after_rug_allocates_later_decision_time(monkeypatch):
    fence = 800.0
    strategy, _, written, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=EventBus(), resolver=_FlowResolver(),
        allocated_at=math.nextafter(fence, math.inf),
    )
    assert await _score_atomic_flow(strategy) is True
    assert written["at"] > fence
    assert strategy._canonical_resolver.calls[0][1] == written["at"]


async def test_regressed_wall_after_identity_conflict_keeps_conflict_visible(monkeypatch):
    fence = 810.0
    resolver = _FlowResolver(
        status="UNRESOLVED", reason="canonical_identity_conflict",
    )
    strategy, _, written, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=EventBus(), resolver=resolver,
        allocated_at=math.nextafter(fence, math.inf),
    )
    assert await _score_atomic_flow(strategy) is True
    assert resolver.calls[0][1] > fence
    assert written["action"] == "SKIP"


async def test_regressed_wall_after_newer_peer_safety_keeps_latest_report_visible(monkeypatch):
    fence = 820.0
    resolver = _FlowResolver(
        status="SUPPRESSED", reason="copycat_cluster",
    )
    strategy, _, written, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=EventBus(), resolver=resolver,
        allocated_at=math.nextafter(fence, math.inf),
    )
    assert await _score_atomic_flow(strategy) is True
    assert resolver.calls[0][1] > fence
    assert written["action"] == "SKIP"


async def test_regressed_wall_progress_sequence_never_reveals_old_favorable_sample(monkeypatch):
    resolver = _FlowResolver()
    strategy, _, _, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=EventBus(), resolver=resolver, allocated_at=900.0,
    )
    current = replace(
        _cp("M", t=10.0, real_sol_lamports=6_000_000_000),
        source_boot_id=17,
        source_seq=23,
    )
    assert await _score_atomic_flow(strategy, current=current) is True
    snapshot = resolver.calls[0][3]
    assert snapshot.source_boot_id == 17
    assert snapshot.source_seq == 23


async def test_regressed_wall_after_childless_report_keeps_integrity_fence_visible(monkeypatch):
    fence = 830.0
    resolver = _FlowResolver(
        status="UNRESOLVED", reason="canonical_target_report_superseded",
    )
    strategy, _, written, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=EventBus(), resolver=resolver,
        allocated_at=math.nextafter(fence, math.inf),
    )
    assert await _score_atomic_flow(strategy) is True
    assert resolver.calls[0][1] > fence
    assert written["action"] == "SKIP"


async def test_initial_resolution_and_decision_share_causal_transaction(monkeypatch):
    bus = EventBus()
    order = []
    resolver = _FlowResolver()
    original_resolve = resolver.resolve

    def resolve(*args, **kwargs):
        order.append("resolve")
        return original_resolve(*args, **kwargs)

    resolver.resolve = resolve
    strategy, _, _, _, _ = _atomic_flow_strategy(
        monkeypatch, bus=bus, resolver=resolver, order=order,
    )
    bus.publish = _ordered_publish(bus.publish, order)

    assert await _score_atomic_flow(strategy) is True
    assert order[:5] == ["begin", ("allocate", 90.0), "resolve", "write", "commit"]
    assert order.index("commit") < order.index("mono") < order.index("publish")


def _ordered_publish(publish, order):
    async def wrapped(event):
        order.append("publish")
        await publish(event)

    return wrapped


async def test_strategy_critical_subscription_acks_safety_and_unsubscribes(tmp_path):
    conn = open_db(tmp_path / "critical-strategy.db")
    bus = EventBus()
    strategy, _ = _make(
        conn,
        bus,
        clock=lambda: 100.0,
        canonical_resolver=None,
        strat_cfg={"entries_enabled": False},
    )
    stop = asyncio.Event()
    task = asyncio.create_task(strategy.run(stop))
    await bus.publish(SafetyHardFail(
        t_wall=1.0,
        t_mono=1.0,
        mint="IGNORED",
        reasons=("rugged",),
    ))
    await asyncio.wait_for(bus.wait_critical_idle_or_failed(), 1.0)
    assert bus.critical_state()[1:] == (0, False)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert strategy._q not in [subscription.queue for subscription in bus._subs]


def test_strategy_safety_and_early_buyer_fixture_hashes_are_v5_valid():
    import ast
    import re
    from pathlib import Path

    fixture_writers = {"record_early_buyer_read", "save_safety_report"}
    fixture_hashes = []
    tree = ast.parse(Path(__file__).read_text())
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        writer = call.func.id if isinstance(call.func, ast.Name) else None
        if writer not in fixture_writers:
            continue
        inputs_hash = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "inputs_hash"),
            None,
        )
        assert isinstance(inputs_hash, ast.Constant) and isinstance(inputs_hash.value, str), (
            f"{writer} fixture at line {call.lineno} must use a literal inputs_hash"
        )
        fixture_hashes.append((call.lineno, inputs_hash.value))

    assert fixture_hashes
    assert [
        (line, value)
        for line, value in fixture_hashes
        if re.fullmatch(r"[0-9a-f]{64}", value) is None
    ] == []


def test_strategy_safety_fixture_uses_raw_completed_at():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    legacy_calls = []
    missing_raw_calls = []
    raw_calls = []
    for call in (
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "save_safety_report"
    ):
        keywords = {keyword.arg for keyword in call.keywords}
        if "checked_at" in keywords:
            legacy_calls.append(call.lineno)
        if "raw_completed_at" in keywords:
            raw_calls.append(call.lineno)
        else:
            missing_raw_calls.append(call.lineno)

    assert legacy_calls == []
    assert missing_raw_calls == []
    assert raw_calls


async def test_high_score_pends_then_fills_on_next_snapshot(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    entries = bus.subscribe(PaperEntry)
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))   # +5 SOL/10s -> high velocity
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=0.0))
    # decision is BUY and PENDING — no position, no fill yet (latency penalty)
    assert "M" not in strat.positions and "M" in strat._pending
    assert conn.execute("SELECT action FROM decisions WHERE mint='M'").fetchone()["action"] == "BUY"
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
    # the NEXT snapshot (>= T later) fills the pending entry against ITS reserves
    later = replace(_cp("M", t=13.0, real_sol_lamports=7_000_000_000), t_wall=100.0)
    fe.observe(later)
    await strat._fill_pending(later)
    assert "M" in strat.positions and "M" not in strat._pending
    ev = await asyncio.wait_for(entries.get(), 2)
    assert ev.mint == "M" and ev.qty > 0
    assert conn.execute("SELECT side FROM paper_trades WHERE mint='M'").fetchone()["side"] == "buy"


async def test_feature_eviction_rewarms_before_decision_and_never_mutates_trade_state(tmp_path):
    conn = open_db(tmp_path / "feature-eviction.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    fe = FeatureEngine(bus=None, max_mints=1, max_feature_mints=1)
    from memebot.strategy import ClimbingStrategy
    strat = ClimbingStrategy(
        bus, conn, feature_engine=fe, scorer=ConfluenceScorer(SCORER_CFG),
        broker=PaperBroker(FILL_CFG, PUMP_CFG),
        canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER,
        strat_cfg=STRAT_CFG,
        pumpfun_cfg=PUMP_CFG, config_hash=_E2E_CONFIG_HASH, fill_cfg=FILL_CFG,
        clock=lambda: 100.0,
        mono_clock=lambda: 10.0,
    )

    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=2_000_000_000))
    fe.observe(_cp("EVICTOR", t=20.0, real_sol_lamports=1_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))

    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    first = _cp("M", t=40.0, real_sol_lamports=4_000_000_000)
    fe.observe(first)
    scored = await strat._try_score(
        strat._pending_score["M"].safety_passed,
        decision_t_mono=first.t_mono,
        current=first,
    )
    assert scored is False
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0

    second = _cp("M", t=50.0, real_sol_lamports=5_000_000_000)
    fe.observe(second)
    scored = await strat._try_score(
        strat._pending_score["M"].safety_passed,
        decision_t_mono=second.t_mono,
        current=second,
    )
    assert scored is True
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0

    fill = replace(_cp("M", t=60.0, real_sol_lamports=6_000_000_000), t_wall=100.0)
    fe.observe(fill)
    await strat._fill_pending(fill)
    position_before = replace(strat.positions["M"])
    ledger_before = (
        conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0],
    )

    # Feature history is a cache, not a lifecycle signal: eviction must not touch the
    # strategy-owned position or append-only decision/trade ledger.
    fe.observe(_cp("SECOND_EVICTOR", t=70.0, real_sol_lamports=1_000_000_000))

    assert "M" not in fe._series and "M" not in fe._states
    assert strat.positions["M"] == position_before
    assert (
        conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0],
    ) == ledger_before


async def test_evicted_terminal_feature_cache_cannot_revive_scoring_or_trading(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "terminal-cache-boundary.db")
    upsert_token(conn, mint="EVICTED_TERMINAL", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "EVICTED_TERMINAL", "CLIMBING")
    stale_pass_id = save_safety_report(
        conn, mint="EVICTED_TERMINAL", raw_completed_at=1.0, segment="CLIMBING",
        hard_fails=[], risk_score=0.0, results_json="[]",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )
    save_safety_report(
        conn, mint="EVICTED_TERMINAL", raw_completed_at=2.0, segment="CLIMBING",
        hard_fails=["terminal"], risk_score=100.0, results_json="[]",
        inputs_hash="2222222222222222222222222222222222222222222222222222222222222222",
    )
    set_token_state(conn, "EVICTED_TERMINAL", "DEAD")

    bus = EventBus()
    scored = bus.subscribe(CandidateScored)
    feature_engine = FeatureEngine(
        bus=None, max_mints=2, max_terminal_mints=2, max_feature_mints=2,
    )
    from memebot.strategy import ClimbingStrategy
    strategy = ClimbingStrategy(
        bus, conn, feature_engine=feature_engine, scorer=ConfluenceScorer(SCORER_CFG),
        broker=PaperBroker(FILL_CFG, PUMP_CFG),
        canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER,
        strat_cfg=STRAT_CFG,
        pumpfun_cfg=PUMP_CFG, config_hash=_E2E_CONFIG_HASH, fill_cfg=FILL_CFG,
        clock=lambda: 100.0,
        mono_clock=lambda: 10.0,
    )

    for index, mint in enumerate(("EVICTED_TERMINAL", "RETAINED_1", "RETAINED_2")):
        feature_engine.on_transition(_lt(mint, "DEAD", t=float(index + 1)))
    assert tuple(feature_engine._terminal_mints) == ("RETAINED_1", "RETAINED_2")

    feature_engine.observe(_cp(
        "EVICTED_TERMINAL", t=10.0, real_sol_lamports=1_000_000_000,
    ))
    feature_engine.observe(_cp(
        "EVICTED_TERMINAL", t=20.0, real_sol_lamports=6_000_000_000,
    ))
    assert feature_engine.features(
        "EVICTED_TERMINAL",
        as_of=100.0,
        identity_ingested_at=0.0,
        risk_score=0.0,
        min_samples=2,
        max_latest_age_s=300.0,
    ) is not None

    await strategy.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=20.0, mint="EVICTED_TERMINAL", segment="CLIMBING",
        safety_report_id=stale_pass_id, risk_score=0.0,
    ))

    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
    assert scored.empty()
    assert strategy._pending_score == {}
    assert strategy._pending == {}
    assert strategy.positions == {}
    assert set(feature_engine._series) == set(feature_engine._states)
    assert set(feature_engine._series) == set(feature_engine._active_mints)
    assert max(
        len(feature_engine._series),
        len(feature_engine._states),
        len(feature_engine._active_mints),
    ) <= feature_engine._max_mints


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
async def test_strategy_loop_ignores_non_finite_curve_inputs(tmp_path, field, non_finite):
    from memebot.strategy import Position

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()

    class CountingBroker:
        def __init__(self):
            self.calls = []
            self.delegate = PaperBroker(FILL_CFG, PUMP_CFG)

        def buy(self, *args, **kwargs):
            self.calls.append("buy")
            return self.delegate.buy(*args, **kwargs)

        def sell(self, *args, **kwargs):
            self.calls.append("sell")
            return self.delegate.sell(*args, **kwargs)

    broker = CountingBroker()
    strat, fe = _make(conn, bus, clock=lambda: 100.0, broker=broker)
    strat._exits = {
        "ladder_multiples": [],
        "ladder_fractions": [],
        "time_stop_s": 1_000.0,
        "trailing_stop_pct": 10.0,
    }
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))
    strat.positions["P"] = Position(
        mint="P", decision_id=999, qty_remaining=100.0, entry_price=1e-7,
        entry_at=100.0, peak_price=2e-7, original_qty=100.0, size_sol=0.2,
        buy_notional=0.2,
    )
    before_pending = dict(strat._pending)
    before_positions = {
        mint: replace(pos, ladder_hits=set(pos.ladder_hits))
        for mint, pos in strat.positions.items()
    }
    before_last_state = dict(strat._last_state)
    before_freshness = dict(strat._last_state_ts)
    before_series = tuple(fe._series["M"])
    before_trades = conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]

    invalid = replace(_cp("M", t=20.0, real_sol_lamports=9_000_000_000,
                          vsol=99_000_000_000), **{field: non_finite})
    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        await bus.publish(invalid)
        await bus.publish(replace(invalid, mint="P"))
        for _ in range(100):
            if strat._q.empty():
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.01)
    finally:
        stop.set()
        await bus.publish(LifecycleTransition(
            t_wall=21.0, t_mono=21.0, mint="WAKE", from_state="NEW",
            to_state="CLIMBING",
        ))
        await asyncio.wait_for(task, 2)

    assert strat._pending == before_pending
    assert strat.positions == before_positions
    assert strat._last_state == before_last_state
    assert strat._last_state_ts == before_freshness
    assert tuple(fe._series["M"]) == before_series
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == before_trades
    assert broker.calls == []
    assert not any(
        isinstance(value, float) and not math.isfinite(value)
        for state in strat._last_state.values()
        for value in (
            state.virtual_sol_reserves,
            state.virtual_token_reserves,
            state.real_sol_reserves,
            state.real_token_reserves,
        )
    )


async def test_fill_waits_until_latency_elapsed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    strat._fill_cfg = {**FILL_CFG, "latency_min_s": 5.0}     # require 5s after the decision
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=0.0))
    too_soon = _cp("M", t=12.0, real_sol_lamports=6_500_000_000)   # only 2s after decision (t_mono=10)
    fe.observe(too_soon)
    await strat._fill_pending(too_soon)
    assert "M" in strat._pending and "M" not in strat.positions   # not yet
    in_time = replace(
        _cp("M", t=16.0, real_sol_lamports=7_000_000_000), t_wall=100.0,
    )  # 6 monotonic seconds after -> fills
    fe.observe(in_time)
    await strat._fill_pending(in_time)
    assert "M" in strat.positions


async def test_low_score_records_skip_and_no_pending(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=1_000_000_100))   # ~0 velocity -> low score
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=90.0))
    assert "M" not in strat.positions and "M" not in strat._pending
    assert conn.execute("SELECT action FROM decisions WHERE mint='M'").fetchone()["action"] == "SKIP"


async def test_score_weights_and_unavailable_score_are_measurable(tmp_path):
    ordinary_keys = {
        "velocity_sol_per_s",
        "curve_progress_pct",
        "age_s",
        "risk_score",
        "spot_price_sol",
        "samples",
        "smart_money_count",
        "smart_money_pnl_sol",
    }

    pinned_cfg = dict(SCORER_CFG)
    pinned_scorer = ConfluenceScorer(pinned_cfg)
    pinned_cfg["weights_version"] = "mutated-after-construction"
    empty_config_scorer = ConfluenceScorer({})
    try:
        empty_config_version = empty_config_scorer.weights_version
    except Exception as exc:
        empty_config_version = type(exc).__name__
    observed = {
        "stable_version": (
            getattr(pinned_scorer, "weights_version", None),
            pinned_scorer.score(
                ClimbingFeatures(
                    velocity_sol_per_s=0.01,
                    curve_progress_pct=20.0,
                    age_s=10.0,
                    risk_score=5.0,
                    spot_price_sol=1e-6,
                    samples=2,
                )
            ).weights_version,
        ),
        "empty_config_version": empty_config_version,
    }

    class ScenarioScorer:
        weights_version = "climbing-v1"

        def __init__(
            self,
            value,
            *,
            returned_weights_version="climbing-v1",
            feature_change=None,
        ):
            self._value = value
            self._returned_weights_version = returned_weights_version
            self._feature_change = feature_change
            self._delegate = ConfluenceScorer(SCORER_CFG)

        def score(self, features, *, segment="CLIMBING"):
            if isinstance(self._value, Exception):
                raise self._value
            score = replace(
                self._delegate.score(features, segment=segment),
                value=self._value,
                weights_version=self._returned_weights_version,
            )
            if self._feature_change is None:
                return score
            operation, *args = self._feature_change
            if operation == "replace":
                vector = args[0]
            else:
                vector = dict(score.feature_vector)
                if operation == "set":
                    vector[args[0]] = args[1]
                else:
                    del vector[args[0]]
            return replace(score, feature_vector=vector)

    scenarios = {
        "valid": ConfluenceScorer(SCORER_CFG),
        "exception": ScenarioScorer(RuntimeError("scorer failed")),
        "nan": ScenarioScorer(float("nan")),
        "out_of_range": ScenarioScorer(101.0),
        "mismatched_version": ScenarioScorer(
            100.0,
            returned_weights_version="wrong",
        ),
        "empty_version": ScenarioScorer(
            100.0,
            returned_weights_version="",
        ),
        "feature_nan": ScenarioScorer(
            100.0,
            feature_change=("set", "velocity_sol_per_s", float("nan")),
        ),
        "feature_inf": ScenarioScorer(
            100.0,
            feature_change=("set", "spot_price_sol", float("inf")),
        ),
        "missing_feature": ScenarioScorer(
            100.0,
            feature_change=("drop", "samples"),
        ),
        "malformed_feature": ScenarioScorer(
            100.0,
            feature_change=("set", "samples", True),
        ),
        "nonmapping_vector": ScenarioScorer(
            100.0,
            feature_change=("replace", None),
        ),
        "extra_feature_nan": ScenarioScorer(
            100.0,
            feature_change=("set", "extra_signal", float("nan")),
        ),
        "extra_feature_inf": ScenarioScorer(
            100.0,
            feature_change=("set", "extra_signal", float("inf")),
        ),
        "nonstring_feature_key": ScenarioScorer(
            100.0,
            feature_change=("set", 1, 1.0),
        ),
        "reserved_canonical_key": ScenarioScorer(
            100.0,
            feature_change=("set", "canonical", 1.0),
        ),
    }
    for name, scorer in scenarios.items():
        conn = open_db(tmp_path / f"score-{name}.db")
        upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
        set_token_state(conn, "M", "CLIMBING")
        _seed_report(conn)
        bus = EventBus()
        scored_events = bus.subscribe(CandidateScored)
        strat, fe = _make(conn, bus, clock=lambda: 100.0)
        strat._scorer = scorer
        fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
        fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))

        error = None
        try:
            await strat.on_safety_passed(SafetyPassed(
                t_wall=1.0,
                t_mono=10.0,
                mint="M",
                segment="CLIMBING",
                safety_report_id=1,
                risk_score=0.0,
            ))
        except Exception as exc:
            error = type(exc).__name__
        row = conn.execute(
            "SELECT action,score,feature_vector_json FROM decisions WHERE mint='M'"
        ).fetchone()
        event = None if scored_events.empty() else scored_events.get_nowait()
        vector = {} if row is None else json.loads(row["feature_vector_json"])
        observed[name] = {
            "error": error,
            "action": None if row is None else row["action"],
            "score_zero": None if row is None else row["score"] == 0.0,
            "score_finite_in_range": (
                None if row is None
                else math.isfinite(row["score"]) and 0.0 <= row["score"] <= 100.0
            ),
            "score_status": vector.get("score_status"),
            "score_weights_version": vector.get("score_weights_version"),
            "score_unavailable_reason": vector.get("score_unavailable_reason"),
            "ordinary_keys_present": tuple(sorted(
                key for key in ordinary_keys if key in vector
            )),
            "null_ordinary_keys": tuple(sorted(
                key for key in ordinary_keys if key in vector and vector[key] is None
            )),
            "event_matches_persisted_score": (
                row is not None and event is not None and event.score == row["score"]
            ),
            "pending": "M" in strat._pending,
        }

    all_ordinary_keys = tuple(sorted(ordinary_keys))
    assert observed == {
        "stable_version": ("climbing-v1", "climbing-v1"),
        "empty_config_version": "ValueError",
        "valid": {
            "error": None,
            "action": "BUY",
            "score_zero": False,
            "score_finite_in_range": True,
            "score_status": "VALID",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": (),
            "event_matches_persisted_score": True,
            "pending": True,
        },
        "exception": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "nan": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_nonfinite",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "out_of_range": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_nonfinite",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "mismatched_version": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "empty_version": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "feature_nan": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_nonfinite",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "feature_inf": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_nonfinite",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "missing_feature": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "malformed_feature": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "nonmapping_vector": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "extra_feature_nan": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_nonfinite",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "extra_feature_inf": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_nonfinite",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "nonstring_feature_key": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
        "reserved_canonical_key": {
            "error": None,
            "action": "SKIP",
            "score_zero": True,
            "score_finite_in_range": True,
            "score_status": "UNAVAILABLE",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "score_exception",
            "ordinary_keys_present": all_ordinary_keys,
            "null_ordinary_keys": all_ordinary_keys,
            "event_matches_persisted_score": True,
            "pending": False,
        },
    }


async def test_smart_money_snapshot_uses_only_predecision_wallet_pnl(tmp_path):
    from memebot.store import (record_early_buyer_read, record_wallet_pnl_event)
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    record_early_buyer_read(conn, mint="M", checked_at=50.0, buyers=("SMART", "LOOKAHEAD"),
                            unavailable_reason="", inputs_hash=(
                                "f6864394f0ea78e87768505d83c1857ddd6887e35262232d59602a1616040bf2"
                            ))
    record_wallet_pnl_event(conn, at=90.0, wallet="SMART", mint="PRIOR",
                            realized_pnl_sol=1.5, source="test", detail={})
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0, scorer_cfg=SMART_SCORER_CFG,
                      smart_money_cfg=SMART_MONEY_CFG)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))

    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1,
                                              risk_score=0.0))
    record_wallet_pnl_event(conn, at=101.0, wallet="LOOKAHEAD", mint="FUTURE",
                            realized_pnl_sol=99.0, source="test", detail={})

    vector = json.loads(conn.execute(
        "SELECT feature_vector_json FROM decisions WHERE mint='M'").fetchone()[0])
    assert vector["smart_money_count"] == 1
    assert vector["smart_money_pnl_sol"] == 1.5


async def test_no_early_buyer_read_scores_with_zero_smart_money(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0, scorer_cfg=SMART_SCORER_CFG,
                      smart_money_cfg=SMART_MONEY_CFG)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))

    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1,
                                              risk_score=0.0))

    vector = json.loads(conn.execute(
        "SELECT feature_vector_json FROM decisions WHERE mint='M'").fetchone()[0])
    assert vector["smart_money_count"] == 0
    assert vector["smart_money_pnl_sol"] == 0.0


async def test_insufficient_history_records_no_decision(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))    # only 1 sample < min_samples=2
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=1, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=0.0))
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0


async def test_initial_safety_score_excludes_buffered_future_curve_samples(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 10.0)
    strat._cfg = {**STRAT_CFG, "min_samples": 3}
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=3_000_000_000))
    fe.observe(_cp("M", t=20.0, real_sol_lamports=9_000_000_000))

    await strat.on_safety_passed(SafetyPassed(
        t_wall=10.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))

    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert "M" in strat._pending_score
    assert "M" not in strat._pending


async def test_initial_safety_buy_uses_causal_curve_state(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    causal = _cp("M", t=10.0, real_sol_lamports=6_000_000_000,
                 vsol=40_000_000_000)
    fe.observe(causal)
    fe.observe(_cp("M", t=20.0, real_sol_lamports=9_000_000_000,
                   vsol=99_000_000_000))

    await strat.on_safety_passed(SafetyPassed(
        t_wall=10.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))

    state = strat._pending["M"].decision_snapshot.curve_state()
    payload = json.loads(conn.execute(
        "SELECT feature_vector_json FROM decisions WHERE mint='M'"
    ).fetchone()[0])
    persisted = payload["canonical"]["ranking_inputs"]["candidates"][0]["raw"][
        "curve_snapshot"
    ]
    assert (
        state.virtual_token_reserves,
        state.virtual_sol_reserves,
        state.real_token_reserves,
        state.real_sol_reserves,
    ) == (
        persisted["virtual_token_reserves"],
        persisted["virtual_sol_reserves"],
        persisted["real_token_reserves"],
        persisted["real_sol_reserves"],
    )


async def test_insufficient_history_retries_on_later_curve_progress_once(tmp_path):
    """A safety pass is durable work, not a one-shot scoring attempt.

    The incident path delivered SafetyPassed after only one feature sample.  A later
    CurveProgress made the candidate scoreable, but the strategy never retried it.
    Duplicate delivery of that recovery tick must still produce one decision only.
    """
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    first = _cp("M", t=0.0, real_sol_lamports=1_000_000_000)
    fe.observe(first)

    await strat.on_safety_passed(SafetyPassed(
        t_wall=1, t_mono=1, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=90.0,
    ))
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    later = _cp("M", t=10.0, real_sol_lamports=1_000_000_100)
    fe.observe(later)
    await bus.publish(later)
    await bus.publish(later)
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, 2)

    assert conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE mint='M'"
    ).fetchone()[0] == 1


async def test_retry_scores_when_strategy_sees_tick_before_feature_engine(tmp_path):
    """A feature-consumer lag defers scoring until independently observed history catches up."""
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1, t_mono=1, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=90.0,
    ))

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    decisive = _cp("M", t=10.0, real_sol_lamports=1_000_000_100)
    await bus.publish(decisive)  # FeatureEngine has deliberately not observed this tick.
    await asyncio.sleep(0.1)
    assert conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE mint='M'"
    ).fetchone()[0] == 0
    assert "M" in strat._pending_score

    fe.observe(decisive)
    await bus.publish(_cp("M", t=20.0, real_sol_lamports=1_000_000_200))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, 2)

    assert conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE mint='M'"
    ).fetchone()[0] == 1
    assert "M" not in strat._pending_score


async def test_high_score_recovery_decision_tick_cannot_also_fill(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        decisive = _cp("M", t=10.0, real_sol_lamports=6_000_000_000)
        fe.observe(decisive)
        await bus.publish(decisive)
        await asyncio.sleep(0.1)

        assert conn.execute(
            "SELECT action FROM decisions WHERE mint = 'M'"
        ).fetchone()["action"] == "BUY"
        assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
        assert strat.positions == {}
        assert "M" in strat._pending

        await bus.publish(replace(
            _cp("M", t=11.0, real_sol_lamports=6_500_000_000), t_wall=100.0,
        ))
        await asyncio.sleep(0.1)
        assert conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE mint = 'M' AND side = 'buy'"
        ).fetchone()[0] == 1
        assert "M" in strat.positions
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_duplicate_signal_while_pending_is_ignored(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    ev = SafetyPassed(t_wall=1, t_mono=10.0, mint="M", segment="CLIMBING",
                      safety_report_id=1, risk_score=0.0)
    await strat.on_safety_passed(ev)
    await strat.on_safety_passed(ev)              # already pending -> ignored, no 2nd decision
    assert conn.execute("SELECT COUNT(*) FROM decisions WHERE mint='M'").fetchone()[0] == 1
    later = replace(_cp("M", t=13.0, real_sol_lamports=7_000_000_000), t_wall=100.0)
    fe.observe(later)
    await strat._fill_pending(later)
    assert conn.execute("SELECT COUNT(*) FROM paper_trades WHERE side='buy'").fetchone()[0] == 1


async def test_duplicate_safety_pass_after_skip_does_not_duplicate_decision(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=1_000_000_100))
    passed = SafetyPassed(t_wall=1, t_mono=10.0, mint="M", segment="CLIMBING",
                          safety_report_id=1, risk_score=90.0)

    await strat.on_safety_passed(passed)
    await strat.on_safety_passed(passed)

    assert conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE mint='M'"
    ).fetchone()[0] == 1


async def test_safety_pass_must_match_current_climbing_token_and_latest_empty_report(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)

    invalid_events = []
    for mint, state, rugged, segment in (
        ("DEAD", "DEAD", 0, "CLIMBING"),
        ("GRAD", "GRADUATED", 0, "CLIMBING"),
        ("RUGGED", "CLIMBING", 1, "CLIMBING"),
        ("SEGMENT", "CLIMBING", 0, "TRENDING"),
    ):
        upsert_token(conn, mint=mint, created_at=0.0, bonding_curve_key=f"BC-{mint}")
        if state == "GRADUATED":
            set_terminal_state_with_reputation(
                conn, mint=mint, outcome="GRADUATED", raw_processed_at=2.0,
                creator=None, creator_conflicted=False,
            )
        else:
            set_token_state(conn, mint, state)
        if rugged:
            conn.execute("UPDATE tokens SET rugged = 1 WHERE mint = ?", (mint,))
            conn.commit()
        report_id = save_safety_report(
            conn, mint=mint, raw_completed_at=1.0, segment="CLIMBING", hard_fails=[],
            risk_score=0.0, results_json="[]",
            inputs_hash="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        fe.observe(_cp(mint, t=0.0, real_sol_lamports=1_000_000_000))
        fe.observe(_cp(mint, t=10.0, real_sol_lamports=6_000_000_000))
        invalid_events.append(SafetyPassed(
            t_wall=1.0, t_mono=1.0, mint=mint, segment=segment,
            safety_report_id=report_id, risk_score=0.0,
        ))

    upsert_token(conn, mint="STALE", created_at=0.0, bonding_curve_key="BC-STALE")
    set_token_state(conn, "STALE", "CLIMBING")
    old_report_id = save_safety_report(
        conn, mint="STALE", raw_completed_at=1.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    save_safety_report(
        conn, mint="STALE", raw_completed_at=2.0, segment="CLIMBING", hard_fails=["rug"],
        risk_score=100.0, results_json="[]",
        inputs_hash="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    )
    fe.observe(_cp("STALE", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("STALE", t=10.0, real_sol_lamports=6_000_000_000))
    invalid_events.append(SafetyPassed(
        t_wall=1.0, t_mono=1.0, mint="STALE", segment="CLIMBING",
        safety_report_id=old_report_id, risk_score=0.0,
    ))

    for ev in invalid_events:
        await strat.on_safety_passed(ev)

    assert strat._pending_score == {}
    assert strat._pending == {}
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0


async def test_stale_passing_safety_report_id_cannot_score_or_buy_against_newer_pass(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "stale-passing-report.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    report_1 = save_safety_report(
        conn, mint="M", raw_completed_at=1.0, segment="CLIMBING", hard_fails=[],
        risk_score=100.0, results_json="[]",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )
    report_2 = save_safety_report(
        conn, mint="M", raw_completed_at=2.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="2222222222222222222222222222222222222222222222222222222222222222",
    )
    assert (report_1, report_2) == (1, 2)

    bus = EventBus()
    scored_events = bus.subscribe(CandidateScored)
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    strat._cfg = {**STRAT_CFG, "score_threshold": 70.0}
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))

    # Both reports pass, but their persisted risks imply different actions at this
    # threshold: stale id=1 would SKIP while current id=2 is scoreable as a BUY.
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=report_1, risk_score=100.0,
    ))

    assert strat._pending_score == {}
    assert strat._pending == {}
    assert scored_events.empty()
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
    assert strat.positions == {}

    await strat.on_safety_passed(SafetyPassed(
        t_wall=2.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=report_2, risk_score=0.0,
    ))

    decision = conn.execute(
        "SELECT action, safety_report_id FROM decisions WHERE mint = 'M'"
    ).fetchone()
    assert (decision["action"], decision["safety_report_id"]) == ("BUY", report_2)
    assert "M" in strat._pending
    assert not scored_events.empty()
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
    assert strat.positions == {}


async def test_pending_score_revalidates_report_and_token_state_before_retry(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)

    for mint in ("HARD_FAIL", "DEAD", "GRADUATED"):
        upsert_token(conn, mint=mint, created_at=0.0, bonding_curve_key=f"BC-{mint}")
        set_token_state(conn, mint, "CLIMBING")
        report_id = save_safety_report(
            conn, mint=mint, raw_completed_at=1.0, segment="CLIMBING", hard_fails=[],
            risk_score=0.0, results_json="[]",
            inputs_hash="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        )
        fe.observe(_cp(mint, t=0.0, real_sol_lamports=1_000_000_000))
        await strat.on_safety_passed(SafetyPassed(
            t_wall=1.0, t_mono=1.0, mint=mint, segment="CLIMBING",
            safety_report_id=report_id, risk_score=0.0,
        ))
        assert mint in strat._pending_score

    save_safety_report(
        conn, mint="HARD_FAIL", raw_completed_at=2.0, segment="CLIMBING", hard_fails=["rug"],
        risk_score=100.0, results_json="[]",
        inputs_hash="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    )
    set_token_state(conn, "DEAD", "DEAD")
    set_terminal_state_with_reputation(
        conn, mint="GRADUATED", outcome="GRADUATED", raw_processed_at=3.0,
        creator=None, creator_conflicted=False,
    )

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    for mint in ("HARD_FAIL", "DEAD", "GRADUATED"):
        await bus.publish(_cp(mint, t=10.0, real_sol_lamports=6_000_000_000))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, 2)

    assert strat._pending_score == {}
    assert strat._pending == {}
    assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0


async def test_newer_safety_pass_supersedes_pending_risk_and_older_duplicates(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    report_1 = save_safety_report(
        conn, mint="M", raw_completed_at=1.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    strat._cfg = {**STRAT_CFG, "score_threshold": 70.0}
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    old_pass = SafetyPassed(
        t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING",
        safety_report_id=report_1, risk_score=0.0,
    )
    await strat.on_safety_passed(old_pass)

    report_2 = save_safety_report(
        conn, mint="M", raw_completed_at=2.0, segment="CLIMBING", hard_fails=[],
        risk_score=100.0, results_json="[]",
        inputs_hash="2222222222222222222222222222222222222222222222222222222222222222",
    )
    new_pass = SafetyPassed(
        t_wall=2.0, t_mono=2.0, mint="M", segment="CLIMBING",
        safety_report_id=report_2, risk_score=100.0,
    )
    await strat.on_safety_passed(new_pass)
    await strat.on_safety_passed(old_pass)
    await strat.on_safety_passed(SafetyPassed(
        t_wall=2.0, t_mono=2.0, mint="M", segment="CLIMBING",
        safety_report_id=report_2, risk_score=0.0,
    ))

    pending = strat._pending_score["M"].safety_passed
    assert pending.safety_report_id == report_2
    assert pending.risk_score == 100.0

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    current = _cp("M", t=10.0, real_sol_lamports=6_000_000_000)
    fe.observe(current)
    await bus.publish(current)
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, 2)

    decision = conn.execute(
        "SELECT action, safety_report_id FROM decisions WHERE mint = 'M'"
    ).fetchone()
    assert decision["action"] == "SKIP"
    assert decision["safety_report_id"] == report_2


async def test_hard_fail_cleans_pre_score_pending_work(tmp_path):
    conn = open_db(tmp_path / "pre-score.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, _ = _make(conn, bus, clock=lambda: 100.0)
    passed = SafetyPassed(t_wall=1, t_mono=1, mint="M", segment="CLIMBING",
                          safety_report_id=1, risk_score=0.0)
    failed = SafetyHardFail(t_wall=2, t_mono=2, mint="M", reasons=("rug",))

    await strat.on_safety_passed(passed)
    assert "M" in strat._pending_score
    await strat.on_safety_flip(failed)
    assert "M" not in strat._pending_score


async def test_hard_fail_cleans_pre_fill_pending_work(tmp_path):
    conn = open_db(tmp_path / "pre-fill.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    passed = SafetyPassed(t_wall=1, t_mono=10.0, mint="M", segment="CLIMBING",
                          safety_report_id=1, risk_score=0.0)
    failed = SafetyHardFail(t_wall=2, t_mono=2, mint="M", reasons=("rug",))

    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(passed)
    assert "M" in strat._pending
    await strat.on_safety_flip(failed)
    assert "M" not in strat._pending


async def _prepare_p3_pending(tmp_path, filename, *, clock):
    conn = open_db(tmp_path / filename)
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    report_id = _seed_report(conn)
    bus = EventBus()
    strategy, features = _make(conn, bus, clock=clock)
    features.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    features.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strategy.on_safety_passed(SafetyPassed(
        t_wall=1.0,
        t_mono=10.0,
        mint="M",
        segment="CLIMBING",
        safety_report_id=report_id,
        risk_score=0.0,
    ))
    assert strategy._pending["M"].recheck_attempt == 1
    return conn, bus, strategy, features


async def test_regressed_prefill_resolution_allocates_and_records_recheck_atomically(
    monkeypatch, tmp_path,
):
    from memebot.strategy import (
        allocate_p3_causal_wall as original_allocate,
        p3_immediate_transaction as original_transaction,
        record_canonical_recheck as original_recheck,
    )

    now = [100.0]
    conn, _, strategy, _ = await _prepare_p3_pending(
        tmp_path, "regressed-prefill.db", clock=lambda: now[0],
    )
    fence = 500.0
    conn.execute("UPDATE p3_causal_clock SET last_wall=? WHERE singleton=1", (fence,))
    conn.commit()
    now[0] = 50.0
    active = False
    order = []
    original_resolve = strategy._canonical_resolver.resolve

    @contextmanager
    def tracked_transaction(db):
        nonlocal active
        with original_transaction(db):
            active = True
            order.append("begin")
            try:
                yield
            finally:
                active = False
        order.append("commit")

    def tracked_allocate(db, *, raw_wall):
        assert active
        order.append(("allocate", raw_wall))
        return original_allocate(db, raw_wall=raw_wall)

    def tracked_resolve(*args, **kwargs):
        assert active
        order.append(("resolve", kwargs["decision_at"]))
        return original_resolve(*args, **kwargs)

    def tracked_recheck(db, **kwargs):
        assert active
        order.append(("recheck", kwargs["rechecked_at"]))
        return original_recheck(db, **kwargs)

    class FailingBroker:
        def buy(self, *_args, **_kwargs):
            assert not active
            order.append("broker")
            raise RuntimeError("stop after durable PASS")

    monkeypatch.setattr("memebot.strategy.p3_immediate_transaction", tracked_transaction)
    monkeypatch.setattr("memebot.strategy.allocate_p3_causal_wall", tracked_allocate)
    monkeypatch.setattr("memebot.strategy.record_canonical_recheck", tracked_recheck)
    monkeypatch.setattr(strategy._canonical_resolver, "resolve", tracked_resolve)
    strategy._broker = FailingBroker()
    fill = replace(
        _cp("M", t=20.0, real_sol_lamports=7_000_000_000),
        t_wall=fence,
    )

    await strategy._fill_pending(fill, _ordered=True)

    recheck = conn.execute("SELECT * FROM canonical_rechecks").fetchone()
    assert recheck["rechecked_at"] > fence
    assert order == [
        "begin",
        ("allocate", 50.0),
        ("resolve", recheck["rechecked_at"]),
        ("recheck", recheck["rechecked_at"]),
        "commit",
        "broker",
    ]


async def test_prefill_pass_persists_before_broker_and_fill(monkeypatch, tmp_path):
    from memebot.strategy import (
        p3_immediate_transaction as original_transaction,
        record_canonical_paper_buy as original_buy_store,
        record_canonical_recheck as original_recheck,
    )

    conn, _, strategy, _ = await _prepare_p3_pending(
        tmp_path, "prefill-pass-order.db", clock=lambda: 100.0,
    )
    order = []
    broker = strategy._broker

    @contextmanager
    def tracked_transaction(db):
        with original_transaction(db):
            order.append("begin")
            yield
        order.append("commit")

    def tracked_recheck(db, **kwargs):
        order.append("recheck")
        return original_recheck(db, **kwargs)

    class TrackedBroker:
        def buy(self, *args, **kwargs):
            order.append("broker")
            return broker.buy(*args, **kwargs)

    def tracked_buy_store(db, **kwargs):
        order.append("fill")
        return original_buy_store(db, **kwargs)

    monkeypatch.setattr("memebot.strategy.p3_immediate_transaction", tracked_transaction)
    monkeypatch.setattr("memebot.strategy.record_canonical_recheck", tracked_recheck)
    monkeypatch.setattr("memebot.strategy.record_canonical_paper_buy", tracked_buy_store)
    strategy._broker = TrackedBroker()
    fill = replace(
        _cp("M", t=20.0, real_sol_lamports=7_000_000_000),
        t_wall=100.0,
    )

    await strategy._fill_pending(fill, _ordered=True)

    assert order == ["begin", "recheck", "commit", "broker", "fill"]
    assert conn.execute("SELECT status FROM canonical_rechecks").fetchone()[0] == "PASS"
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 1


async def test_p3_buy_persistence_failure_never_rebrokers(monkeypatch, tmp_path):
    from memebot.strategy import record_canonical_paper_buy as original_store

    conn, _, strategy, _ = await _prepare_p3_pending(
        tmp_path, "buy-no-rebroker.db", clock=lambda: 100.0,
    )
    broker = strategy._broker
    broker_calls = 0
    store_calls = []

    class CountingBroker:
        def buy(self, *args, **kwargs):
            nonlocal broker_calls
            broker_calls += 1
            return broker.buy(*args, **kwargs)

    def fail_once_store(db, **kwargs):
        store_calls.append(kwargs)
        if len(store_calls) == 1:
            raise RuntimeError("transient BUY persistence failure")
        return original_store(db, **kwargs)

    async def no_delay(_seconds):
        return None

    strategy._broker = CountingBroker()
    monkeypatch.setattr("memebot.strategy.record_canonical_paper_buy", fail_once_store)
    monkeypatch.setattr("memebot.strategy.asyncio.sleep", no_delay)
    fill = replace(
        _cp("M", t=20.0, real_sol_lamports=7_000_000_000),
        t_wall=100.0,
    )

    await strategy._fill_pending(fill, _ordered=True)

    assert broker_calls == 1
    assert len(store_calls) == 2
    assert store_calls[0] == store_calls[1]
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 1


async def test_prefill_cancel_persists_before_pending_removal(monkeypatch, tmp_path):
    from memebot.strategy import (
        record_canonical_recheck as original_recheck,
        record_terminal_entry_execution as original_terminal,
    )

    conn, _, strategy, _ = await _prepare_p3_pending(
        tmp_path, "prefill-cancel-order.db", clock=lambda: 100.0,
    )
    original_resolve = strategy._canonical_resolver.resolve
    order = []

    def cancelling_resolve(*args, **kwargs):
        resolution = original_resolve(*args, **kwargs)
        return replace(
            resolution,
            verdict=replace(
                resolution.verdict,
                status="SUPPRESSED",
                reason="copycat_cluster",
                canonical_mint=None,
                rank=None,
            ),
        )

    def tracked_recheck(db, **kwargs):
        assert "M" in strategy._pending
        order.append("recheck")
        return original_recheck(db, **kwargs)

    def tracked_terminal(db, **kwargs):
        assert "M" in strategy._pending
        order.append("terminal")
        return original_terminal(db, **kwargs)

    class ForbiddenBroker:
        def buy(self, *_args, **_kwargs):
            raise AssertionError("CANCEL must not call broker")

    monkeypatch.setattr(strategy._canonical_resolver, "resolve", cancelling_resolve)
    monkeypatch.setattr("memebot.strategy.record_canonical_recheck", tracked_recheck)
    monkeypatch.setattr("memebot.strategy.record_terminal_entry_execution", tracked_terminal)
    strategy._broker = ForbiddenBroker()
    fill = replace(
        _cp("M", t=20.0, real_sol_lamports=7_000_000_000),
        t_wall=100.0,
    )

    await strategy._fill_pending(fill, _ordered=True)

    assert order == ["recheck", "terminal"]
    assert "M" not in strategy._pending
    assert tuple(conn.execute(
        "SELECT status,reason FROM canonical_rechecks"
    ).fetchone()) == ("CANCEL", "copycat_cluster")
    assert conn.execute(
        "SELECT status FROM paper_entry_executions"
    ).fetchone()[0] == "CANCELLED"


async def test_recheck_persistence_failure_retains_same_attempt(monkeypatch, tmp_path):
    conn, _, strategy, _ = await _prepare_p3_pending(
        tmp_path, "recheck-failure.db", clock=lambda: 100.0,
    )
    pending = strategy._pending["M"]
    broker_calls = 0

    def fail_recheck(_db, **_kwargs):
        raise RuntimeError("recheck store unavailable")

    class ForbiddenBroker:
        def buy(self, *_args, **_kwargs):
            nonlocal broker_calls
            broker_calls += 1
            raise AssertionError("broker must wait for durable recheck")

    monkeypatch.setattr("memebot.strategy.record_canonical_recheck", fail_recheck)
    strategy._broker = ForbiddenBroker()
    fill = replace(
        _cp("M", t=20.0, real_sol_lamports=7_000_000_000),
        t_wall=100.0,
    )

    with pytest.raises(RuntimeError, match="recheck store unavailable"):
        await strategy._fill_pending(fill, _ordered=True)

    assert strategy._pending["M"] is pending
    assert pending.recheck_attempt == 1
    assert broker_calls == 0
    assert conn.execute("SELECT COUNT(*) FROM canonical_rechecks").fetchone()[0] == 0


async def test_terminal_and_stale_pending_entries_cancel_exactly(tmp_path):
    terminal_conn, _, terminal_strategy, _ = await _prepare_p3_pending(
        tmp_path, "terminal-exact-cancel.db", clock=lambda: 100.0,
    )
    await terminal_strategy.on_transition(LifecycleTransition(
        t_wall=101.0,
        t_mono=101.0,
        mint="M",
        from_state="CLIMBING",
        to_state="DEAD",
    ))

    stale_conn, _, stale_strategy, _ = await _prepare_p3_pending(
        tmp_path, "stale-exact-cancel.db", clock=lambda: 100.0,
    )
    await stale_strategy.sweep_stale(401.0)

    assert "M" not in terminal_strategy._pending
    assert "M" not in stale_strategy._pending
    assert tuple(terminal_conn.execute(
        "SELECT cr.status,cr.reason,pe.status,pe.reason "
        "FROM canonical_rechecks cr JOIN paper_entry_executions pe "
        "ON pe.canonical_recheck_id=cr.id"
    ).fetchone()) == ("CANCEL", "dead", "CANCELLED", "dead")
    assert tuple(stale_conn.execute(
        "SELECT cr.status,cr.reason,pe.status,pe.reason "
        "FROM canonical_rechecks cr JOIN paper_entry_executions pe "
        "ON pe.canonical_recheck_id=cr.id"
    ).fetchone()) == ("CANCEL", "stale", "CANCELLED", "stale")


async def test_safety_hardfail_persists_cancel_before_eviction(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "durable-prefill-cancel.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    original_report_id = _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=original_report_id, risk_score=0.0,
    ))
    hard_fail_report_id = save_safety_report(
        conn,
        mint="M",
        raw_completed_at=101.0,
        segment="CLIMBING",
        hard_fails=["rug"],
        risk_score=100.0,
        results_json="[]",
        inputs_hash="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    )

    await strat.on_safety_flip(SafetyHardFail(
        t_wall=101.0,
        t_mono=101.0,
        mint="M",
        reasons=("rug",),
        safety_report_id=hard_fail_report_id,
    ))

    recheck = conn.execute("SELECT * FROM canonical_rechecks").fetchone()
    execution = conn.execute("SELECT * FROM paper_entry_executions").fetchone()
    assert (recheck["status"], recheck["reason"]) == ("CANCEL", "safety_flip")
    assert execution["status"] == "CANCELLED"
    assert execution["canonical_recheck_id"] == recheck["id"]
    assert "M" not in strat._pending


async def test_terminal_pending_entry_persists_exact_cancel_before_eviction(tmp_path):
    conn = open_db(tmp_path / "durable-terminal-cancel.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    report_id = _seed_report(conn)
    bus = EventBus()
    now = [100.0]
    strat, fe = _make(conn, bus, clock=lambda: now[0])
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=report_id, risk_score=0.0,
    ))

    await strat.on_transition(LifecycleTransition(
        t_wall=100.0,
        t_mono=100.0,
        mint="M",
        from_state="CLIMBING",
        to_state="DEAD",
    ))

    assert tuple(conn.execute(
        "SELECT status,reason FROM canonical_rechecks"
    ).fetchone()) == ("CANCEL", "dead")
    assert tuple(conn.execute(
        "SELECT status,reason FROM paper_entry_executions"
    ).fetchone()) == ("CANCELLED", "dead")
    assert "M" not in strat._pending


async def test_p3_buy_persistence_retry_preserves_exact_entry_draft(
    monkeypatch, tmp_path,
):
    from memebot.strategy import P3EntryDraft

    strategy, _ = _make(
        open_db(tmp_path / "draft-retry.db"), EventBus(), clock=lambda: 100.0,
    )
    buy_calls = []

    def buy_store(_conn, **kwargs):
        buy_calls.append(kwargs)
        if len(buy_calls) == 1:
            raise RuntimeError("transient BUY store failure")
        return 10, 20

    async def no_delay(_seconds):
        return None

    monkeypatch.setattr("memebot.strategy.record_canonical_paper_buy", buy_store)
    monkeypatch.setattr("memebot.strategy.asyncio.sleep", no_delay)
    entry = P3EntryDraft(
        decision_id=1, recheck_id=2, raw_processed_at=3.0, mint="M",
        segment="CLIMBING", qty=4.0, quote_price=5.0, fill_price=6.0,
        fees=(("base", 0.1),), realism_grade="B", planned_size_sol=24.0,
    )
    assert await strategy._persist_entry_draft(entry) == (10, 20)
    assert buy_calls[0] == buy_calls[1]


async def test_runtime_p3_sells_use_strict_exit_helper(monkeypatch, tmp_path):
    from memebot.strategy import record_canonical_paper_sell as original_strict

    conn = open_db(tmp_path / "runtime-strict-sell.db")
    strategy, features = _make_with_exits(
        conn, EventBus(), clock=lambda: 100.0,
    )
    await _open_position(conn, strategy._bus, strategy, features)
    assert strategy.positions["M"].is_p3 is True
    strict_calls = []

    def tracked_strict(db, **kwargs):
        strict_calls.append(kwargs)
        return original_strict(db, **kwargs)

    def forbidden_generic(*_args, **_kwargs):
        raise AssertionError("runtime P3 SELL used generic ledger writer")

    monkeypatch.setattr("memebot.strategy.record_canonical_paper_sell", tracked_strict)
    monkeypatch.setattr("memebot.strategy.record_paper_trade", forbidden_generic)
    pumped = _cp(
        "M", t=20.0, real_sol_lamports=6_000_000_000, vsol=77_500_000_000,
    )

    await strategy.on_price(pumped)

    assert len(strict_calls) == 1
    assert strict_calls[0]["exit_reason"] == "ladder_0"
    sell = conn.execute(
        "SELECT * FROM paper_trades WHERE side='sell'"
    ).fetchone()
    assert sell["p3_entry_execution_id"] is not None


async def test_p3_sell_persistence_failure_retries_same_fill_without_rebroker(
    monkeypatch, tmp_path,
):
    from memebot.strategy import record_canonical_paper_sell as original_store

    conn = open_db(tmp_path / "sell-no-rebroker.db")
    strategy, features = _make_with_exits(
        conn, EventBus(), clock=lambda: 100.0,
    )
    await _open_position(conn, strategy._bus, strategy, features)
    broker = strategy._broker
    broker_calls = 0
    store_calls = []

    class CountingBroker:
        def sell(self, *args, **kwargs):
            nonlocal broker_calls
            broker_calls += 1
            return broker.sell(*args, **kwargs)

    def fail_once_store(db, **kwargs):
        store_calls.append(kwargs)
        if len(store_calls) == 1:
            raise RuntimeError("transient SELL persistence failure")
        return original_store(db, **kwargs)

    async def no_delay(_seconds):
        return None

    strategy._broker = CountingBroker()
    monkeypatch.setattr("memebot.strategy.record_canonical_paper_sell", fail_once_store)
    monkeypatch.setattr("memebot.strategy.asyncio.sleep", no_delay)
    pumped = _cp(
        "M", t=20.0, real_sol_lamports=6_000_000_000, vsol=77_500_000_000,
    )

    await strategy.on_price(pumped)

    assert broker_calls == 1
    assert len(store_calls) == 2
    assert store_calls[0] == store_calls[1]
    assert conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE side='sell'"
    ).fetchone()[0] == 1


async def test_durable_cancel_terminal_failure_retries_same_cancelled_execution(
    monkeypatch, tmp_path,
):
    from types import SimpleNamespace

    strategy, _ = _make(
        open_db(tmp_path / "terminal-retry.db"), EventBus(), clock=lambda: 9.0,
    )
    pending = SimpleNamespace(
        decision_id=1, mint="M", terminal_recheck_id=7, terminal_reason="stale",
    )
    strategy._pending = {"M": pending}
    calls = []
    delays = []

    def terminal_store(_conn, **kwargs):
        calls.append(kwargs)
        if len(calls) < 3:
            raise RuntimeError("transient terminal failure")
        return 11

    async def capture_delay(seconds):
        delays.append(seconds)

    monkeypatch.setattr(
        "memebot.strategy.record_terminal_entry_execution", terminal_store,
    )
    monkeypatch.setattr("memebot.strategy.asyncio.sleep", capture_delay)

    await strategy._cancel_pending_entry(pending, reason="ignored_new_reason")

    assert calls[0] == calls[1] == calls[2] == {
        "decision_id": 1, "raw_wall": 9.0, "status": "CANCELLED",
        "reason": "stale", "recheck_id": 7,
    }
    assert delays == [0.05, 0.1]
    assert strategy._pending == {}


async def test_broker_failure_after_pass_increments_next_attempt(tmp_path):
    conn = open_db(tmp_path / "prefill-boundaries.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    report_id = _seed_report(conn)
    bus = EventBus()
    now = [100.0]
    strat, fe = _make(conn, bus, clock=lambda: now[0])
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=10.0, mint="M", segment="CLIMBING",
        safety_report_id=report_id, risk_score=0.0,
    ))
    strat._fill_event_max_age_s = 5.0
    conn.execute("UPDATE p3_causal_clock SET last_wall=110.0 WHERE singleton=1")
    conn.commit()
    stale = replace(_cp("M", t=20.0, real_sol_lamports=7_000_000_000), t_wall=100.0)
    clock_before = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    await strat._fill_pending(stale, _ordered=True)
    assert strat._pending["M"].recheck_attempt == 1
    assert conn.execute("SELECT count(*) FROM canonical_rechecks").fetchone()[0] == 0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before

    class FailingBroker:
        def buy(self, *_args, **_kwargs):
            raise RuntimeError("broker unavailable")

    strat._broker = FailingBroker()
    now[0] = 111.0
    fresh = replace(stale, t_wall=111.0, t_mono=30.0)
    await strat._fill_pending(fresh, _ordered=True)
    assert strat._pending["M"].recheck_attempt == 2
    assert tuple(conn.execute(
        "SELECT attempt,status FROM canonical_rechecks"
    ).fetchone()) == (1, "PASS")
    assert conn.execute("SELECT count(*) FROM paper_trades").fetchone()[0] == 0


async def test_ladder_hit_is_not_marked_before_durable_sell(tmp_path):
    from memebot.strategy import Position

    strategy, _ = _make_with_exits(
        open_db(tmp_path / "ladder-persist.db"), EventBus(), clock=lambda: 2.0,
    )
    strategy.positions = {"M": Position(
        mint="M", decision_id=1, qty_remaining=2.0, entry_price=1e-12,
        entry_at=1.0, peak_price=1e-12, original_qty=2.0, size_sol=1.0,
    )}
    strategy._clock = lambda: 2.0
    strategy._pump = PUMP_CFG
    strategy._exits = {
        "ladder_multiples": [2.0], "ladder_fractions": [0.5],
        "time_stop_s": 1000.0, "trailing_stop_pct": 99.0,
    }
    strategy._last_state = {}
    strategy._last_state_ts = {}

    async def fail_sell(*_args, **_kwargs):
        raise RuntimeError("durable sell failed")

    strategy._sell = fail_sell
    with pytest.raises(RuntimeError, match="durable sell failed"):
        await strategy.on_price(
            _cp("M", t=2.0, real_sol_lamports=6_000_000_000), _ordered=True,
        )
    assert strategy.positions["M"].ladder_hits == set()


EXITS_CFG = {"ladder_multiples": [2.0, 3.0], "ladder_fractions": [0.5, 0.3],
             "time_stop_s": 3600.0, "trailing_stop_pct": 25.0}


def _make_with_exits(conn, bus, clock):
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    from memebot.strategy import ClimbingStrategy
    strat = ClimbingStrategy(bus, conn, feature_engine=fe, scorer=ConfluenceScorer(SCORER_CFG),
                             broker=PaperBroker(FILL_CFG, PUMP_CFG),
                             canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER,
                             strat_cfg=STRAT_CFG,
                             pumpfun_cfg=PUMP_CFG, config_hash=_E2E_CONFIG_HASH,
                             fill_cfg=FILL_CFG,
                             exits_cfg=EXITS_CFG, clock=clock,
                             mono_clock=lambda: 10.0)
    return strat, fe


async def _open_position(conn, bus, strat, fe):
    """Drive the full two-phase entry so strat.positions['M'] exists (entry price ~ spot@31 vSOL)."""
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M", segment="CLIMBING",
                                              safety_report_id=1, risk_score=0.0))
    fill_snap = replace(
        _cp("M", t=13.0, real_sol_lamports=7_000_000_000), t_wall=100.0,
    )  # vSOL=31e9 -> entry ~ spot@31
    fe.observe(fill_snap)
    await strat._fill_pending(fill_snap)


async def test_ladder_take_profit_partial_sell(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    exits = bus.subscribe(PaperExit)
    now = [100.0]
    strat, fe = _make_with_exits(conn, bus, clock=lambda: now[0])
    await _open_position(conn, bus, strat, fe)
    pos = strat.positions["M"]
    # price ~2.5x entry -> only rung 0 (2.0x) fires; rung 1 (3.0x) does not. vSOL 77.5e9 = 2.5x spot@31
    pumped = _cp("M", t=20.0, real_sol_lamports=6_000_000_000, vsol=77_500_000_000)
    fe.observe(pumped)
    await strat.on_price(pumped)
    ev = await asyncio.wait_for(exits.get(), 2)
    assert ev.reason == "ladder_0"
    assert strat.positions["M"].qty_remaining == pytest.approx(pos.original_qty * 0.5, rel=1e-6)
    assert conn.execute("SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0] == 1


async def test_curve_order_guard_is_strategy_local_bounded_and_terminal_cleaned(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    strat._curve_order_capacity = 2
    strat._exits = {
        "ladder_multiples": [],
        "ladder_fractions": [],
        "time_stop_s": 3_600.0,
        "trailing_stop_pct": 25.0,
    }
    await _open_position(conn, bus, strat, fe)

    # The feature consumer may legitimately run ahead. Strategy must order only the
    # snapshots delivered to its own queue, rather than rejecting t=20 because FE saw t=30.
    fe.observe(_cp("M", t=30.0, real_sol_lamports=9_000_000_000,
                   vsol=80_000_000_000))
    high = _cp("M", t=20.0, real_sol_lamports=8_000_000_000,
               vsol=70_000_000_000)
    reordered_low = _cp("M", t=5.0, real_sol_lamports=2_000_000_000,
                        vsol=31_000_000_000)

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        await bus.publish(high)
        await bus.publish(reordered_low)
        await asyncio.sleep(0.1)

        # t=5 would trip the trailing stop after t=20's high watermark if Strategy
        # allowed the reordered event to reach position management.
        assert "M" in strat.positions
        assert conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE mint='M' AND side='sell'"
        ).fetchone()[0] == 0
        assert strat._last_state_ts["M"] == 20.0
        assert strat._curve_order["M"] == 20.0
        assert fe._series["M"][-1][0] == 30.0

        await bus.publish(_cp("OTHER-1", t=21.0, real_sol_lamports=1))
        await bus.publish(_cp("OTHER-2", t=22.0, real_sol_lamports=1))
        await asyncio.sleep(0.1)
        assert len(strat._curve_order) == 2
        assert list(strat._curve_order) == ["M", "OTHER-2"]

        await bus.publish(_lt("OTHER-2", "DEAD", t=23.0))
        await asyncio.sleep(0.1)
        assert "OTHER-2" not in strat._curve_order
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_equal_timestamp_duplicate_curve_is_rejected_before_any_strategy_mutation(tmp_path):
    from memebot.strategy import PendingScore

    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    now = [100.0]
    strat, fe = _make_with_exits(conn, bus, clock=lambda: now[0])
    strat._exits = {
        "ladder_multiples": [],
        "ladder_fractions": [],
        "time_stop_s": 3_600.0,
        "trailing_stop_pct": 25.0,
    }
    await _open_position(conn, bus, strat, fe)

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        high = _cp("M", t=100.0, real_sol_lamports=8_000_000_000,
                   vsol=70_000_000_000)
        await bus.publish(high)
        await asyncio.sleep(0.1)
        assert strat._curve_order["M"] == 100.0

        strat._pending_score["WAITING"] = PendingScore(
            safety_passed=SafetyPassed(
                t_wall=0.0, t_mono=0.0, mint="WAITING", segment="CLIMBING",
                safety_report_id=2, risk_score=0.0,
            ),
            registered_at=95.0,
        )
        strat._stale_after_s = 10.0
        before_positions = {
            mint: replace(pos, ladder_hits=set(pos.ladder_hits))
            for mint, pos in strat.positions.items()
        }
        before_pending = dict(strat._pending)
        before_pending_score = dict(strat._pending_score)
        before_entry_times = list(strat._entry_times)
        before_last_state = dict(strat._last_state)
        before_freshness = dict(strat._last_state_ts)
        before_order = dict(strat._curve_order)
        before_ledger = tuple(
            tuple(tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id"))
            for table in ("decisions", "paper_trades", "outcomes")
        )

        duplicate_low = _cp("M", t=100.0, real_sol_lamports=2_000_000_000,
                            vsol=31_000_000_000)
        await bus.publish(duplicate_low)
        await asyncio.sleep(0.1)

        assert strat.positions == before_positions
        assert strat._pending == before_pending
        assert strat._pending_score == before_pending_score
        assert strat._entry_times == before_entry_times
        assert strat._last_state == before_last_state
        assert strat._last_state_ts == before_freshness
        assert strat._curve_order == before_order
        assert tuple(
            tuple(tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id"))
            for table in ("decisions", "paper_trades", "outcomes")
        ) == before_ledger

        # A genuinely newer low tick still reaches expiry and position management.
        now[0] = 106.0
        await bus.publish(_cp("M", t=106.0, real_sol_lamports=2_000_000_000,
                              vsol=31_000_000_000))
        await asyncio.sleep(0.1)
        assert "WAITING" not in strat._pending_score
        assert "M" not in strat.positions
        assert conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE mint='M' AND side='sell'"
        ).fetchone()[0] == 1
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_curve_order_capacity_churn_preserves_open_position_watermark(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    strat._curve_order_capacity = 2
    strat._exits = {
        "ladder_multiples": [],
        "ladder_fractions": [],
        "time_stop_s": 3_600.0,
        "trailing_stop_pct": 25.0,
    }
    await _open_position(conn, bus, strat, fe)

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        await bus.publish(_cp("M", t=20.0, real_sol_lamports=8_000_000_000,
                              vsol=70_000_000_000))
        await bus.publish(_cp("OTHER-1", t=21.0, real_sol_lamports=1))
        await bus.publish(_cp("OTHER-2", t=22.0, real_sol_lamports=1))
        await bus.publish(_cp("M", t=5.0, real_sol_lamports=2_000_000_000,
                              vsol=31_000_000_000))
        await asyncio.sleep(0.1)

        assert "M" in strat.positions
        assert conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE mint='M' AND side='sell'"
        ).fetchone()[0] == 0
        assert strat._last_state_ts["M"] == 20.0
        assert strat._curve_order["M"] == 20.0
        assert len(strat._curve_order) == 2
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)

    from memebot.strategy import PendingEntry, PendingScore, _snapshot_from_event

    snapshot = _snapshot_from_event(
        _cp("PENDING", t=20.0, real_sol_lamports=1_000_000_000)
    )
    assert snapshot is not None
    strat._pending["PENDING"] = PendingEntry(
        mint="PENDING", decision_id=2, safety_report_id=1,
        canonical_inputs_hash="a" * 64, decision_snapshot=snapshot,
        decision_at=20.0, decision_mono=20.0, size_sol=0.2, score=80.0,
    )
    strat._pending_score["SCORE"] = PendingScore(
        safety_passed=SafetyPassed(
            t_wall=20.0, t_mono=20.0, mint="SCORE", segment="CLIMBING",
            safety_report_id=3, risk_score=0.0,
        ),
        registered_at=20.0,
    )
    strat._curve_order_capacity = 3
    strat._curve_order = {"M": 20.0, "PENDING": 20.0, "SCORE": 20.0}

    assert not strat._accept_curve(_cp("UNTRACKED", t=21.0, real_sol_lamports=1))
    assert strat._curve_order == {"M": 20.0, "PENDING": 20.0, "SCORE": 20.0}
    assert not strat._accept_curve(_cp("PENDING", t=5.0, real_sol_lamports=1))
    assert not strat._accept_curve(_cp("SCORE", t=5.0, real_sol_lamports=1))


async def test_curve_order_eviction_bounds_causal_state_and_reobserves_from_current_tick(tmp_path):
    async def wait_until(predicate):
        while not predicate():
            await asyncio.sleep(0)

    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    strat, _ = _make_with_exits(conn, bus, clock=lambda: 0.0)
    strat._curve_order_capacity = 2

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        for index in range(1_000):
            await bus.publish(_cp(
                f"M{index}", t=1_000.0 + index,
                real_sol_lamports=index + 1,
                vsol=31_000_000_000 + index,
            ))
        await asyncio.wait_for(wait_until(lambda: "M999" in strat._last_state), 2)

        expected_keys = {"M998", "M999"}
        assert len(strat._curve_order) <= 2
        assert len(strat._last_state) <= 2
        assert len(strat._last_state_ts) <= 2
        assert set(strat._curve_order) == expected_keys
        assert set(strat._last_state) == expected_keys
        assert set(strat._last_state_ts) == expected_keys

        # M0's old high-timestamp/high-price state was evicted. Re-observation starts a
        # fresh causal history from this valid event alone; no stale quote survives.
        current = _cp(
            "M0", t=1.0, real_sol_lamports=7,
            vsol=41_000_000_000,
        )
        await bus.publish(current)
        await asyncio.wait_for(
            wait_until(lambda: strat._curve_order.get("M0") == current.t_mono), 2,
        )

        assert set(strat._curve_order) == {"M999", "M0"}
        assert set(strat._last_state) == {"M999", "M0"}
        assert set(strat._last_state_ts) == {"M999", "M0"}
        assert strat._last_state["M0"].virtual_sol_reserves == current.virtual_sol_reserves
        assert strat._last_state["M0"].real_sol_reserves == current.real_sol_reserves
        assert strat._last_state_ts["M0"] == current.t_wall

        await bus.publish(_lt("M0", "DEAD", t=2.0))
        await asyncio.wait_for(wait_until(lambda: "M0" not in strat._curve_order), 2)
        assert "M0" not in strat._last_state
        assert "M0" not in strat._last_state_ts
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_time_stop_closes_and_writes_outcome(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    now = [100.0]
    strat, fe = _make_with_exits(conn, bus, clock=lambda: now[0])
    await _open_position(conn, bus, strat, fe)
    now[0] = 100.0 + EXITS_CFG["time_stop_s"] + 1
    tick = _cp("M", t=20.0, real_sol_lamports=6_000_000_000)     # vSOL=31e9 -> ~entry price, no ladder
    fe.observe(tick)
    await strat.on_price(tick)
    assert "M" not in strat.positions                    # fully closed by the time-stop
    o = p3_outcome_for_decision(conn, strat_decision_id(conn))
    assert o["ref_kind"] == "trade"


async def test_safety_flip_force_exits(tmp_path):
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    now = [100.0]
    strat, fe = _make_with_exits(conn, bus, clock=lambda: now[0])
    await _open_position(conn, bus, strat, fe)
    await strat.on_safety_flip(SafetyHardFail(t_wall=1, t_mono=1, mint="M",
                                              reasons=("mint_authority_active",)))
    assert "M" not in strat.positions
    # paper_trades has no `reason` column — reason travels on PaperExit + outcome.detail_json
    assert conn.execute("SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0] >= 1


def strat_decision_id(conn):
    return conn.execute("SELECT id FROM decisions WHERE mint='M'").fetchone()["id"]


def p3_outcome_for_decision(conn, decision_id):
    return conn.execute(
        "SELECT o.* FROM outcomes o JOIN paper_trades pt ON pt.id=o.ref_id "
        "WHERE o.ref_kind='trade' AND o.p3_exit_trade_id=pt.id "
        "AND pt.decision_id=?",
        (decision_id,),
    ).fetchone()


async def test_reconcile_restores_open_positions_from_ledger(tmp_path):
    from memebot.store import record_decision, record_paper_trade
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M", segment="CLIMBING", action="BUY",
                          score=80.0, feature_vector={}, config_hash="cfg")
    record_paper_trade(conn, decision_id=did, at=2.0, mint="M", segment="CLIMBING",
                       side="buy", qty=1000.0, quote_price=1e-6, fill_price=1e-6,
                       fees={}, realism_grade="B")
    bus = EventBus()
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    restored = strat.reconcile()
    assert restored == 1
    assert strat.positions["M"].qty_remaining == pytest.approx(1000.0)
    assert strat.positions["M"].decision_id == did
    # buy-only position: original_qty collapses to qty_remaining (no sells to distinguish
    # them), ladder_hits stays empty (nothing fired), buy_notional is computed (not the
    # old defaulted 0.0) -- I4 fix must not regress this ledger-only reconcile path.
    assert strat.positions["M"].original_qty == pytest.approx(1000.0)
    assert strat.positions["M"].ladder_hits == set()
    assert strat.positions["M"].buy_notional == pytest.approx(1000.0 * 1e-6)


def test_reconcile_restores_p3_position_from_bounded_summary(tmp_path):
    from memebot.store import record_canonical_paper_sell
    from tests.test_store import _fill_strict_p3_buy, _seed_strict_p3_buy

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "restore-p3-summary.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    record_canonical_paper_sell(
        conn, decision_id=decision_id, raw_wall=5.0, mint="M",
        segment="CLIMBING", qty=0.5, quote_price=5.0, fill_price=4.5,
        fees={}, realism_grade="B", exit_reason="ladder_0", ladder_index=0,
    )
    strat, _ = _make_with_exits2(conn, EventBus(), clock=lambda: 10.0)

    assert strat.reconcile() == 1
    restored = strat.positions["M"]
    assert restored.is_p3 is True
    assert restored.decision_id == decision_id
    assert restored.original_qty == 2.0
    assert restored.qty_remaining == 1.5
    assert restored.ladder_hits == {0}
    assert restored.buy_notional == 10.01
    conn.close()


def test_restored_open_p3_hardfail_zero_closes_with_strict_exit(tmp_path):
    from tests.test_store import (_add_valid_p3_safety_children,
                                  _fill_strict_p3_buy, _seed_strict_p3_buy)
    from memebot.store import record_canonical_paper_sell, save_safety_report

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "restore-hardfail-zero.db",
    )
    _add_valid_p3_safety_children(conn, report_id)
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    hard_fail_id = save_safety_report(
        conn, mint="M", raw_completed_at=5.0, segment="CLIMBING",
        hard_fails=("rug",), risk_score=100.0, results_json="[]",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )
    _add_valid_p3_safety_children(conn, hard_fail_id)
    strat, _ = _make_with_exits2(conn, EventBus(), clock=lambda: 10.0)
    assert strat.reconcile(runtime_causal_floor=10.0, max_open_positions=5) == (
        decision_id,
    )

    committed_ids = record_canonical_paper_sell(
        conn, decision_id=decision_id, raw_wall=11.0, mint="M",
        segment="CLIMBING", qty=2.0, quote_price=0.0, fill_price=0.0,
        fees={}, realism_grade="F", exit_reason="restart_safety_hard_fail",
        ladder_index=None,
    )
    sell_id, outcome_id = strat.zero_close_restored_p3_position(
        decision_id=decision_id, latest_report_id=hard_fail_id, raw_wall=12.0,
    )

    assert (sell_id, outcome_id) == committed_ids
    sell = conn.execute("SELECT * FROM paper_trades WHERE id=?", (sell_id,)).fetchone()
    outcome = conn.execute("SELECT * FROM outcomes WHERE id=?", (outcome_id,)).fetchone()
    assert sell["fill_price"] == 0.0
    assert sell["p3_entry_execution_id"] is not None
    assert outcome["p3_exit_trade_id"] == sell_id
    assert "M" not in strat.positions
    conn.close()


async def test_recover_pending_scores_from_latest_safety_evidence(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    report_id = _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)

    assert strat.recover_pending_scores() == 1
    recovered = strat._pending_score["M"].safety_passed
    assert recovered.safety_report_id == report_id
    assert recovered.risk_score == 0.0

    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    current = _cp("M", t=10.0, real_sol_lamports=1_000_000_100)
    fe.observe(current)
    await bus.publish(current)
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, 2)

    assert conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE mint='M'"
    ).fetchone()[0] == 1


async def test_background_recovery_reaches_older_eligible_candidate_once(tmp_path):
    from memebot.store import record_decision, save_safety_report

    conn = open_db(tmp_path / "paged-recovery.db")
    upsert_token(conn, mint="OLDER", created_at=0.0, bonding_curve_key="BC-OLDER")
    set_token_state(conn, "OLDER", "CLIMBING")
    older_report_id = save_safety_report(
        conn, mint="OLDER", raw_completed_at=95.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="8888888888888888888888888888888888888888888888888888888888888888",
    )
    for index in range(6):
        mint = f"RECENT_{index}"
        upsert_token(conn, mint=mint, created_at=0.0, bonding_curve_key=f"BC-{index}")
        set_token_state(conn, mint, "CLIMBING")
        save_safety_report(
            conn, mint=mint, raw_completed_at=99.0, segment="CLIMBING", hard_fails=[],
            risk_score=0.0, results_json="[]",
            inputs_hash="9999999999999999999999999999999999999999999999999999999999999999",
        )
        if index % 2:
            set_terminal_state_with_reputation(
                conn, mint=mint, outcome="GRADUATED",
                raw_processed_at=100.0 + index,
                creator=None, creator_conflicted=False,
            )
        else:
            record_decision(
                conn, at=99.0, mint=mint, segment="CLIMBING", action="SKIP",
                score=1.0, feature_vector={}, config_hash="cfg",
            )

    bus = EventBus()
    scored = bus.subscribe(CandidateScored)
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    strat._recovery_scan_cap = 2

    assert strat.recover_pending_scores() == 0
    assert strat.recovery_pending is True
    assert "OLDER" not in strat._pending_score

    stop = asyncio.Event()
    strategy_task = asyncio.create_task(strat.run(stop))
    recovery_task = asyncio.create_task(strat.continue_pending_score_recovery(stop))
    try:
        await asyncio.wait_for(recovery_task, 2.0)
        recovered = strat._pending_score["OLDER"].safety_passed
        assert recovered.safety_report_id == older_report_id
        assert strat.recovery_pending is False

        fe.observe(_cp("OLDER", t=0.0, real_sol_lamports=1_000_000_000))
        current = _cp("OLDER", t=10.0, real_sol_lamports=1_000_000_100)
        fe.observe(current)
        await bus.publish(current)
        event = await asyncio.wait_for(scored.get(), 2.0)
        assert event.mint == "OLDER"

        await strat.continue_pending_score_recovery(stop)
        await bus.publish(current)
        await asyncio.sleep(0.05)
        assert conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE mint='OLDER'"
        ).fetchone()[0] == 1
    finally:
        stop.set()
        await asyncio.wait_for(strategy_task, 2.0)


async def test_background_recovery_warns_when_stop_leaves_pages(tmp_path, caplog):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "stopped-recovery.db")
    for index in range(3):
        mint = f"TERMINAL_{index}"
        upsert_token(conn, mint=mint, created_at=0.0, bonding_curve_key=f"BC-{index}")
        set_terminal_state_with_reputation(
            conn, mint=mint, outcome="GRADUATED",
            raw_processed_at=float(index + 1),
            creator=None, creator_conflicted=False,
        )
        save_safety_report(
            conn, mint=mint, raw_completed_at=99.0, segment="CLIMBING", hard_fails=[],
            risk_score=0.0, results_json="[]",
            inputs_hash="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )

    strat, _ = _make(conn, EventBus(), clock=lambda: 100.0)
    strat._recovery_scan_cap = 1
    assert strat.recover_pending_scores() == 0
    assert strat.recovery_pending is True

    caplog.set_level("WARNING", logger="memebot.strategy")
    stop = asyncio.Event()
    stop.set()
    await strat.continue_pending_score_recovery(stop)

    assert "pending score recovery stopped with remaining pages" in caplog.text


# I4 (final-review): reconcile() must restore full ladder state, not just qty_remaining --
# original_qty=qty_remaining (shrunken), empty ladder_hits, and buy_notional=0.0 all cause
# a restarted position to double-ladder (re-fire an already-hit rung) and mis-price PnL.
EXITS_CFG_I4 = {"ladder_multiples": [2.0, 3.0], "ladder_fractions": [0.4, 0.3],
               "time_stop_s": 999_999.0, "trailing_stop_pct": 90.0}


def _make_with_exits2(conn, bus, clock):
    fe = FeatureEngine(bus=None, max_feature_mints=DEFAULT_MAX_MINTS)
    from memebot.strategy import ClimbingStrategy
    strat = ClimbingStrategy(bus, conn, feature_engine=fe, scorer=ConfluenceScorer(SCORER_CFG),
                             broker=PaperBroker(FILL_CFG, PUMP_CFG),
                             canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER,
                             strat_cfg=STRAT_CFG,
                             pumpfun_cfg=PUMP_CFG, config_hash=_E2E_CONFIG_HASH,
                             fill_cfg=FILL_CFG,
                             exits_cfg=EXITS_CFG_I4, clock=clock,
                             mono_clock=lambda: 10.0)
    return strat, fe


async def test_reconcile_restores_ladder_state(tmp_path):
    from memebot.store import record_decision, record_paper_trade
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M", segment="CLIMBING", action="BUY",
                          score=80.0, feature_vector={}, config_hash="cfg")
    fees = {"protocol": 0.001, "priority": 0.0005}
    fill_price = 1e-6
    record_paper_trade(conn, decision_id=did, at=2.0, mint="M", segment="CLIMBING",
                       side="buy", qty=1000.0, quote_price=fill_price, fill_price=fill_price,
                       fees=fees, realism_grade="B")
    # ladder sell of qty=400.0 == 0.4 * 1000.0 == original_qty * ladder_fractions[0]
    record_paper_trade(conn, decision_id=did, at=5.0, mint="M", segment="CLIMBING",
                       side="sell", qty=400.0, quote_price=2e-6, fill_price=2e-6,
                       fees={}, realism_grade="B")
    bus = EventBus()
    strat, fe = _make_with_exits2(conn, bus, clock=lambda: 100.0)
    restored = strat.reconcile()
    assert restored == 1
    pos = strat.positions["M"]
    assert pos.original_qty == pytest.approx(1000.0)          # true original, NOT 600 remaining
    assert pos.ladder_hits == {0}                              # rung 0 already fired
    assert pos.qty_remaining == pytest.approx(600.0)
    assert pos.buy_notional == pytest.approx(1000.0 * fill_price + sum(fees.values()))


# N1 (final-review, latent ledger-corruption trap): reconcile() rebuilds original_qty/
# buy_notional/ladder_hits by re-querying paper_trades WHERE decision_id = <the id
# open_positions_from_ledger reported>. If that id came from a closed EARLIER decision
# (mint-keyed aggregation bug) while qty_remaining came from a later OPEN decision, the
# restored Position would reconstruct the wrong cycle's original_qty/buy_notional/
# ladder_hits against the right qty_remaining -- every subsequent exit then writes under
# the wrong (closed) decision_id, corrupting _realized_pnl. Confirms the fix flows through
# reconcile: the restored position must be decision B's (the open cycle), not A's (closed).
async def test_reconcile_restores_open_cycle_not_closed_cycle(tmp_path):
    from memebot.store import record_decision, record_paper_trade
    conn = open_db(tmp_path / "t.db")
    # decision A on mint "M": buy 1000, sell 1000 -> fully closed
    did_a = record_decision(conn, at=1.0, mint="M", segment="CLIMBING", action="BUY",
                            score=80.0, feature_vector={}, config_hash="cfg")
    record_paper_trade(conn, decision_id=did_a, at=2.0, mint="M", segment="CLIMBING",
                       side="buy", qty=1000.0, quote_price=1e-6, fill_price=1e-6,
                       fees={}, realism_grade="B")
    record_paper_trade(conn, decision_id=did_a, at=3.0, mint="M", segment="CLIMBING",
                       side="sell", qty=1000.0, quote_price=2e-6, fill_price=2e-6,
                       fees={}, realism_grade="B")
    # decision B on the SAME mint "M", entered LATER: buy 500, sell 200 -> net 300 open
    did_b = record_decision(conn, at=10.0, mint="M", segment="CLIMBING", action="BUY",
                            score=85.0, feature_vector={}, config_hash="cfg")
    record_paper_trade(conn, decision_id=did_b, at=11.0, mint="M", segment="CLIMBING",
                       side="buy", qty=500.0, quote_price=5e-6, fill_price=5e-6,
                       fees={}, realism_grade="B")
    record_paper_trade(conn, decision_id=did_b, at=12.0, mint="M", segment="CLIMBING",
                       side="sell", qty=200.0, quote_price=6e-6, fill_price=6e-6,
                       fees={}, realism_grade="B")
    bus = EventBus()
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    restored = strat.reconcile()
    assert restored == 1
    pos = strat.positions["M"]
    assert pos.decision_id == did_b                             # B's id, never A's
    assert pos.original_qty == pytest.approx(500.0)              # B's original (500), NOT A's (1000)
    assert pos.qty_remaining == pytest.approx(300.0)


async def test_reconcile_does_not_refire_hit_ladder_rung(tmp_path):
    from memebot.store import record_decision, record_paper_trade
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M", segment="CLIMBING", action="BUY",
                          score=80.0, feature_vector={}, config_hash="cfg")
    fees = {"protocol": 0.001, "priority": 0.0005}
    # entry fill_price matches _cp's spot price at vsol=31_000_000_000 (spot@31, per _cp's
    # fixed virtual_token_reserves of 900_000_000_000_000 and PUMP_CFG token_decimals=6)
    # so the later "pumped" vsol below is a real, comparable price increase relative to
    # entry -- not an arbitrary unrelated number. Same formula as spot_price_sol_per_token.
    fill_price = (31_000_000_000 / 1_000_000_000) / (900_000_000_000_000 / 10 ** 6)
    record_paper_trade(conn, decision_id=did, at=2.0, mint="M", segment="CLIMBING",
                       side="buy", qty=1000.0, quote_price=fill_price, fill_price=fill_price,
                       fees=fees, realism_grade="B")
    record_paper_trade(conn, decision_id=did, at=5.0, mint="M", segment="CLIMBING",
                       side="sell", qty=400.0, quote_price=fill_price * 2.0,
                       fill_price=fill_price * 2.0, fees={}, realism_grade="B")
    bus = EventBus()
    strat, fe = _make_with_exits2(conn, bus, clock=lambda: 100.0)
    strat.reconcile()
    sells_before = conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0]
    assert sells_before == 1
    # price comfortably above entry_price * ladder_multiples[0] (2.0x) but BELOW
    # ladder_multiples[1] (3.0x) -> rung 0 WOULD fire again if ladder_hits weren't
    # restored, and rung 1 must not fire either (isolates the assertion to rung 0's
    # refire behavior). entry_price == spot@31 vSOL; vsol=77_500_000_000 -> 2.5x that,
    # clearing rung 0 with margin while staying under rung 1 (same ratio as
    # test_ladder_take_profit_partial_sell). time_stop_s/trailing_stop_pct are
    # large/loose in EXITS_CFG_I4 so only the ladder branch is exercised.
    pumped = _cp("M", t=20.0, real_sol_lamports=6_000_000_000, vsol=77_500_000_000)
    fe.observe(pumped)
    await strat.on_price(pumped)
    sells_after = conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0]
    assert sells_after == 1                                    # rung 0 did NOT re-fire
    assert 1 not in strat.positions["M"].ladder_hits            # rung 1 correctly not hit
    assert strat.positions["M"].ladder_hits == {0}


def _lt(mint, to_state, from_state="CLIMBING", t=100.0):
    return LifecycleTransition(t_wall=t, t_mono=t, mint=mint,
                               from_state=from_state, to_state=to_state)


async def test_pending_evicted_on_terminal_transition(tmp_path):
    # I1: a PendingEntry for a token that dies/graduates before the fill snapshot arrives
    # must be evicted by the lifecycle transition itself, not left to leak a
    # max_concurrent_positions slot forever (the fill-trigger snapshot never comes).
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))   # high velocity -> BUY
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=0.0))
    assert "M" in strat._pending                          # registered, but NOT yet filled
    await strat.on_transition(_lt("M", "DEAD"))
    assert "M" not in strat._pending                       # slot freed
    assert conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE mint='M' AND side='buy'").fetchone()[0] == 0


async def test_pre_score_pending_evicted_on_terminal_transition(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, _ = _make(conn, bus, clock=lambda: 100.0)
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1, t_mono=1, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))
    assert "M" in strat._pending_score

    await strat.on_transition(_lt("M", "GRADUATED"))

    assert "M" not in strat._pending_score


async def test_held_position_force_closed_on_graduation(tmp_path):
    # I2: a held moon-bag whose token GRADUATES must be force-closed using the strategy's
    # own cached last curve state (self._last_state), since no further CurveProgress will
    # ever arrive to drive on_price's exits.
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    exits = bus.subscribe(PaperExit)
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    await _open_position(conn, bus, strat, fe)
    assert "M" in strat.positions
    assert "M" in strat._last_state                        # cached by _fill_pending (req. A)
    await strat.on_transition(_lt("M", "GRADUATED"))
    assert "M" not in strat.positions
    assert conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE mint='M' AND side='sell'").fetchone()[0] == 1
    o = p3_outcome_for_decision(conn, strat_decision_id(conn))
    assert o is not None
    ev = await asyncio.wait_for(exits.get(), 2)
    assert ev.reason == "graduated"
    assert "M" not in strat._last_state                     # popped after handling


async def test_held_position_total_loss_on_dead(tmp_path):
    # I2 (rug case): a held position whose token goes DEAD has no live curve to sell into
    # -> total-loss disposal at price 0, still recording a closing outcome so winners/losers
    # aren't censored from the expectancy stat.
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    exits = bus.subscribe(PaperExit)
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    await _open_position(conn, bus, strat, fe)
    assert "M" in strat.positions
    await strat.on_transition(_lt("M", "DEAD"))
    assert "M" not in strat.positions
    o = p3_outcome_for_decision(conn, strat_decision_id(conn))
    assert o is not None and o["pnl_sol"] < 0                # total loss
    pos_buy_notional = strat.positions.get("M")               # already popped; sanity only
    assert pos_buy_notional is None
    row = conn.execute(
        "SELECT fill_price, realism_grade FROM paper_trades"
        " WHERE mint='M' AND side='sell' ORDER BY id DESC LIMIT 1").fetchone()
    assert row["fill_price"] == 0.0
    assert row["realism_grade"] == "F"
    ev = await asyncio.wait_for(exits.get(), 2)
    assert ev.reason == "dead"
    assert ev.fill_price == 0.0


async def test_sweep_evicts_stale_pending(tmp_path):
    # N2 (strategy side): a pending entry whose mint fell off the top-max_tracked poll set
    # (no fresh CurveProgress within stale_price_after_s) must be evicted -- it will never
    # fill, and left alone leaks a max_concurrent_positions slot forever.
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))   # high velocity -> BUY
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=0.0))
    assert "M" in strat._pending                          # registered, but NOT yet filled
    strat._last_state_ts["M"] = 0.0                        # last curve update was long ago
    await strat.sweep_stale(now=1000.0)                    # 1000 - 0 > stale_after_s (default 300)
    assert "M" not in strat._pending                       # slot freed
    assert conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE mint='M' AND side='buy'").fetchone()[0] == 0


async def test_pre_score_pending_expires_only_after_stale_boundary(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, _ = _make(conn, bus, clock=lambda: 100.0)
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1, t_mono=1, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))

    await strat.sweep_stale(now=100.0 + strat._stale_after_s)
    assert "M" in strat._pending_score  # exact boundary remains eligible

    await strat.sweep_stale(now=100.0 + strat._stale_after_s + 0.001)
    assert "M" not in strat._pending_score


async def test_busy_curve_stream_still_expires_other_unscored_candidates(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    now = [100.0]
    strat, _ = _make(conn, bus, clock=lambda: now[0])
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1, t_mono=1, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))
    assert "M" in strat._pending_score

    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    now[0] = 100.0 + strat._stale_after_s + 1.0
    try:
        for i in range(10):
            await bus.publish(_cp("OTHER", t=now[0] + i, real_sol_lamports=1_000_000_000))
            await asyncio.sleep(0.02)  # continuously reset run()'s 0.5s idle timeout
        assert "M" not in strat._pending_score
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_busy_rejected_curve_stream_still_expires_other_unscored_candidates(tmp_path):
    from memebot.strategy import PendingScore

    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    now = [100.0]
    strat, _ = _make(conn, bus, clock=lambda: now[0])
    strat._stale_after_s = 10.0

    accepted_consumed = asyncio.Event()
    rejected_consumed = asyncio.Event()
    rejected_count = 0
    original_accept_curve = strat._accept_curve

    def counting_accept_curve(ev):
        nonlocal rejected_count
        accepted = original_accept_curve(ev)
        if ev.mint == "STREAM":
            if accepted:
                accepted_consumed.set()
            else:
                rejected_count += 1
                if rejected_count == 100:
                    rejected_consumed.set()
        return accepted

    strat._accept_curve = counting_accept_curve
    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    try:
        accepted = _cp("STREAM", t=100.0, real_sol_lamports=1_000_000_000)
        await bus.publish(accepted)
        await asyncio.wait_for(accepted_consumed.wait(), 2)
        assert strat._curve_order["STREAM"] == 100.0

        strat._pending_score["WAITING"] = PendingScore(
            safety_passed=SafetyPassed(
                t_wall=95.0, t_mono=95.0, mint="WAITING", segment="CLIMBING",
                safety_report_id=1, risk_score=0.0,
            ),
            registered_at=95.0,
        )
        before_positions = dict(strat.positions)
        before_pending = dict(strat._pending)
        before_entry_times = list(strat._entry_times)
        before_last_state = dict(strat._last_state)
        before_freshness = dict(strat._last_state_ts)
        before_order = dict(strat._curve_order)
        before_ledger = tuple(
            tuple(tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id"))
            for table in ("decisions", "paper_trades", "outcomes")
        )

        now[0] = 106.0
        for _ in range(100):
            await bus.publish(accepted)
        await asyncio.wait_for(rejected_consumed.wait(), 2)

        assert rejected_count == 100
        assert "WAITING" not in strat._pending_score
        assert strat.positions == before_positions
        assert strat._pending == before_pending
        assert strat._entry_times == before_entry_times
        assert strat._last_state == before_last_state
        assert strat._last_state_ts == before_freshness
        assert strat._curve_order == before_order
        assert tuple(
            tuple(tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id"))
            for table in ("decisions", "paper_trades", "outcomes")
        ) == before_ledger
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_busy_duplicate_safety_stream_still_expires_other_unscored_candidates(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    upsert_token(conn, mint="OTHER", created_at=0.0, bonding_curve_key="BC-OTHER")
    set_token_state(conn, "OTHER", "CLIMBING")
    other_report = save_safety_report(
        conn, mint="OTHER", raw_completed_at=1.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="3333333333333333333333333333333333333333333333333333333333333333",
    )
    bus = EventBus()
    now = [100.0]
    strat, _ = _make(conn, bus, clock=lambda: now[0])
    await strat.on_safety_passed(SafetyPassed(
        t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING",
        safety_report_id=1, risk_score=0.0,
    ))
    assert "M" in strat._pending_score

    duplicate = SafetyPassed(
        t_wall=1.0, t_mono=1.0, mint="OTHER", segment="CLIMBING",
        safety_report_id=other_report, risk_score=0.0,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(strat.run(stop))
    now[0] = 100.0 + strat._stale_after_s + 1.0
    try:
        for _ in range(10):
            await bus.publish(duplicate)
            await asyncio.sleep(0.02)  # continuously reset run()'s 0.5s idle timeout
        assert "M" not in strat._pending_score
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_pre_score_pending_capacity_evicts_oldest_stably(tmp_path):
    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    now = [100.0]
    strat, _ = _make(conn, bus, clock=lambda: now[0])
    strat._pending_score_capacity = 2

    for i in range(3):
        mint = f"M{i}"
        now[0] = 100.0 + i
        upsert_token(conn, mint=mint, created_at=0.0, bonding_curve_key=f"BC{i}")
        set_token_state(conn, mint, "CLIMBING")
        report_id = save_safety_report(
            conn, mint=mint, raw_completed_at=now[0], segment="CLIMBING", hard_fails=[],
            risk_score=0.0, results_json="[]",
            inputs_hash="dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        )
        await strat.on_safety_passed(SafetyPassed(
            t_wall=now[0], t_mono=now[0], mint=mint, segment="CLIMBING",
            safety_report_id=report_id, risk_score=0.0,
        ))

    assert list(strat._pending_score) == ["M1", "M2"]


async def test_sweep_evicts_never_ticked_pending(tmp_path):
    # N2 residual (FT5 follow-up): a PendingEntry registered in on_safety_passed has NO
    # _last_state_ts until a later CurveProgress arrives. If the mint falls off the poll set
    # before any post-registration tick, sweep_stale (which only iterates _last_state_ts)
    # would never see it -> pending leaks a max_concurrent_positions slot forever. Fix stamps
    # _last_state_ts[mint] = now at registration so a never-ticked pending still ages out.
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=0.0, bonding_curve_key="BC")
    set_token_state(conn, "M", "CLIMBING")
    _seed_report(conn)
    bus = EventBus()
    strat, fe = _make(conn, bus, clock=lambda: 100.0)          # decision now == 100.0
    fe.observe(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    fe.observe(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))   # high velocity -> BUY
    await strat.on_safety_passed(SafetyPassed(t_wall=1, t_mono=10.0, mint="M",
                                              segment="CLIMBING", safety_report_id=1, risk_score=0.0))
    # NO fill-trigger CurveProgress observed, and we do NOT touch _last_state_ts by hand:
    assert "M" in strat._pending                               # registered, never filled
    assert "M" in strat._last_state_ts                         # stamped AT registration (FIX 1)
    await strat.sweep_stale(now=100.0 + strat._stale_after_s + 1.0)   # past the staleness bound
    assert "M" not in strat._pending                           # never-ticked pending swept, slot freed
    assert "M" not in strat._last_state_ts                     # cleaned up


async def test_sweep_closes_stale_held_position_as_dead(tmp_path):
    # N2 (strategy side): an open position whose mint fell off the poll set is dead per the
    # base rate (an off-poll token has no fresh price -> treat as rugged) -- force-close as a
    # total loss so it doesn't sit open forever, censored from the outcomes stat.
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    await _open_position(conn, bus, strat, fe)
    assert "M" in strat.positions
    strat._last_state_ts["M"] = 0.0
    await strat.sweep_stale(now=strat._stale_after_s + 1.0)
    assert "M" not in strat.positions
    o = p3_outcome_for_decision(conn, strat_decision_id(conn))
    assert o is not None and o["pnl_sol"] < 0                # total loss
    row = conn.execute(
        "SELECT fill_price, realism_grade FROM paper_trades"
        " WHERE mint='M' AND side='sell' ORDER BY id DESC LIMIT 1").fetchone()
    assert row["fill_price"] == 0.0
    assert row["realism_grade"] == "F"


async def test_fresh_position_not_swept(tmp_path):
    # A position whose mint is still being freshly polled must NOT be swept even when
    # sweep_stale runs -- only staleness (no fresh price within the bound) triggers eviction.
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    now = [100.0]
    strat, fe = _make_with_exits(conn, bus, clock=lambda: now[0])
    await _open_position(conn, bus, strat, fe)
    assert "M" in strat.positions
    strat._last_state_ts["M"] = now[0]                      # fresh, set by _fill_pending (req. A)
    sells_before = conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0]
    await strat.sweep_stale(now=now[0] + 1.0)               # well within stale_after_s
    assert "M" in strat.positions                           # still open
    sells_after = conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0]
    assert sells_after == sells_before                       # no new sell


async def test_safety_flip_disposes_at_zero(tmp_path):
    # Minor (gate-rug-overstatement): a gate-detected rug can't actually be sold (honeypot/
    # frozen liquidity) -- on_safety_flip must dispose the moon-bag at price 0 (total loss),
    # not at the last cached LIVE curve price, which overstates rug outcomes.
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    exits = bus.subscribe(PaperExit)
    strat, fe = _make_with_exits(conn, bus, clock=lambda: 100.0)
    await _open_position(conn, bus, strat, fe)
    assert "M" in strat.positions
    await strat.on_safety_flip(SafetyHardFail(t_wall=1, t_mono=1, mint="M",
                                              reasons=("mint_authority_active",)))
    assert "M" not in strat.positions
    row = conn.execute(
        "SELECT fill_price, realism_grade FROM paper_trades"
        " WHERE mint='M' AND side='sell' ORDER BY id DESC LIMIT 1").fetchone()
    assert row["fill_price"] == 0.0
    assert row["realism_grade"] == "F"
    o = p3_outcome_for_decision(conn, strat_decision_id(conn))
    assert o is not None and o["pnl_sol"] < 0                # total loss
    ev = await asyncio.wait_for(exits.get(), 2)
    assert ev.fill_price == 0.0


async def test_paperexit_pnl_sums_to_outcome(tmp_path):
    # M1 invariant: sum(PaperExit.pnl_sol) over a full close must equal the authoritative
    # outcomes.pnl_sol (both use buy_notional, the post-fee cost basis, so the ladder rung's
    # per-event pnl and the time-stop's final pnl reconcile with _realized_pnl exactly).
    conn = open_db(tmp_path / "t.db")
    bus = EventBus()
    exits = bus.subscribe(PaperExit)
    now = [100.0]
    strat, fe = _make_with_exits(conn, bus, clock=lambda: now[0])
    await _open_position(conn, bus, strat, fe)
    # ladder rung 0 (2.0x) fires and sells 50%
    pumped = _cp("M", t=20.0, real_sol_lamports=6_000_000_000, vsol=77_500_000_000)
    fe.observe(pumped)
    await strat.on_price(pumped)
    assert "M" in strat.positions                          # only partially closed
    # then the hard time-stop closes the remainder
    now[0] = 100.0 + EXITS_CFG["time_stop_s"] + 1
    tick = _cp("M", t=30.0, real_sol_lamports=6_000_000_000, vsol=77_500_000_000)
    fe.observe(tick)
    await strat.on_price(tick)
    assert "M" not in strat.positions                       # fully closed now
    did = strat_decision_id(conn)
    collected = []
    while not exits.empty():
        collected.append(exits.get_nowait())
    assert len(collected) == 2                              # ladder_0 + time_stop
    total_pnl = sum(ev.pnl_sol for ev in collected)
    o = p3_outcome_for_decision(conn, did)
    assert total_pnl == pytest.approx(o["pnl_sol"], abs=1e-9)
