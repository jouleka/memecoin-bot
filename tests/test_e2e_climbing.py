"""End-to-end CLIMBING slice, offline + deterministic: TokenCreated -> rising curve reserves
-> SafetyPassed -> score >= threshold -> paper buy -> price doubles -> ladder sell -> ledger +
alert + counterfactual. Drives components directly on one EventBus (no network, injected clock)."""
import asyncio
import hashlib
import json
import math
from dataclasses import asdict, replace

from memebot.broker import PaperBroker
from memebot.bus import EventBus
from memebot.canonical import (CanonicalObservationDraft, CanonicalResolution,
                               CanonicalVerdict, canonical_generation_hash,
                               creator_component, first_mover_component,
                               holder_component, integer_rank_points,
                               liquidity_component, quantize_component,
                               social_component)
from memebot.counterfactual import ForwardReturnTracker
from memebot.events import CurveProgress, PaperEntry, PaperExit, SafetyPassed
from memebot.features import DEFAULT_MAX_MINTS, FeatureEngine
from memebot.scoring import ConfluenceScorer
from memebot.store import (open_db, save_safety_report, set_token_state,
                           upsert_token_identity)
from memebot.strategy import ClimbingStrategy

SCORER_CFG = {"weights_version": "climbing-v1", "w_velocity": 0.55, "w_progress": 0.20,
              "w_age": 0.05, "w_risk": 0.20, "velocity_full_scale_sol_per_s": 0.05,
              "progress_full_scale_pct": 80.0, "age_full_scale_s": 600.0}
STRAT_CFG = {"entries_enabled": True, "score_threshold": 40.0, "position_size_sol": 0.2,
             "max_concurrent_positions": 5, "max_entries_per_hour": 10, "min_samples": 2,
             "min_age_s": 0.0}
FILL_CFG = {"latency_min_s": 0.0, "extra_slippage_bps": 50, "priority_fee_sol": 0.0005,
            "solana_base_fee_sol": 0.000005, "grade_a_max_impact_pct": 2.0,
            "grade_b_max_impact_pct": 5.0, "grade_c_max_impact_pct": 10.0}
EXITS_CFG = {"ladder_multiples": [2.0], "ladder_fractions": [0.5], "time_stop_s": 999999.0,
             "trailing_stop_pct": 99.0}
_E2E_TOKEN_DECIMALS = 6
PUMP_CFG = {"protocol_fee_bps": 95, "creator_fee_bps": 30,
            "token_decimals": _E2E_TOKEN_DECIMALS,
            "sellable_supply": 793_100_000.0}
_E2E_CONFIG_HASH = "d" * 64
_E2E_SAFETY_INPUTS_HASH = "e" * 64
_E2E_HOLDER_INPUTS_HASH = "f" * 64
_E2E_HORIZONS = (3600.0,)
_E2E_RESOLVER_VERSION = "e2e-deterministic-v1"
_E2E_WEIGHTS_VERSION = "e2e-deterministic-v1"
_E2E_WEIGHTS_BPS = {
    "first_mover": 2_000,
    "liquidity": 2_000,
    "holder": 2_000,
    "creator": 2_000,
    "social": 2_000,
}
_E2E_SOCIAL_WEIGHTS_BPS = {
    "uri": 2_500,
    "website": 2_500,
    "twitter": 2_500,
    "telegram": 2_500,
}


class _DeterministicCanonicalResolver:
    def resolve(
        self,
        mint,
        *,
        decision_at,
        target_report_id,
        target_snapshot=None,
    ):
        cluster_key = "fixture:fx"
        identity_at = math.nextafter(0.0, math.inf)
        safety_checked_at = math.nextafter(1.0, math.inf)
        safety_risk_score = 90.0 if mint == "INCIDENT" else 0.0
        social_values = {
            "uri": None,
            "website": None,
            "twitter": None,
            "telegram": None,
        }
        social_diagnostics = {
            field: {
                "value": None,
                "present": False,
                "reuse": False,
                "cluster_conflict": False,
                "metadata_conflict": False,
            }
            for field in social_values
        }
        if target_snapshot is None:
            snapshot = {
                "t_wall": decision_at,
                "t_mono": decision_at,
                "virtual_sol_reserves": 31_000_000_000,
                "virtual_token_reserves": 900_000_000_000_000,
                "real_sol_reserves": 1_000_000_000,
                "real_token_reserves": 0,
                "spot_price_sol": 1e-9,
            }
            curve_progress_pct = 25.0
        else:
            snapshot = {
                "t_wall": target_snapshot.t_wall,
                "t_mono": target_snapshot.t_mono,
                "virtual_sol_reserves": target_snapshot.virtual_sol_reserves,
                "virtual_token_reserves": target_snapshot.virtual_token_reserves,
                "real_sol_reserves": target_snapshot.real_sol_reserves,
                "real_token_reserves": target_snapshot.real_token_reserves,
                "spot_price_sol": target_snapshot.spot_price_sol,
            }
            curve_progress_pct = target_snapshot.progress_pct
        components = {
            "first_mover": first_mover_component(
                identity_ingested_at=identity_at,
                mint=mint,
                eligible_pairs=((identity_at, mint),),
            ),
            "liquidity": liquidity_component(
                real_sol_locked=snapshot["real_sol_reserves"] / 1_000_000_000,
                curve_progress_pct=curve_progress_pct,
                graduation_sol=85.0,
            ),
            "holder": holder_component(
                distinct_non_curve_owners=2,
                top10_share_pct=40.0,
                top10_holder_max_pct=60.0,
            ),
            "creator": creator_component(
                creator="",
                creator_conflicted=False,
                prior_successes=0,
                prior_rugs=0,
            ),
            "social": social_component(
                candidate_values=social_values,
                eligible_values=(social_values,),
                metadata_conflicts=(),
                social_weights_bps=_E2E_SOCIAL_WEIGHTS_BPS,
            ),
        }
        components_ppm = {
            name: quantize_component(value) for name, value in components.items()
        }
        rank_points = integer_rank_points(
            components=components, weights_bps=_E2E_WEIGHTS_BPS,
        )
        candidate = {
            "mint": mint,
            "p3_identity_ingested_at": identity_at,
            "state": "CLIMBING",
            "rugged": 0,
            "normalized_name": "fixture",
            "normalized_symbol": "fx",
            "creator": "",
            "identity_observed_at": {
                "name": identity_at,
                "symbol": identity_at,
            },
            "identity_conflicts": [],
            "eligible": True,
            "ineligible_reason": "",
            "safety_report_id": target_report_id,
            "safety_checked_at": safety_checked_at,
            "safety_inputs_hash": _E2E_SAFETY_INPUTS_HASH,
            "safety_hard_fails": [],
            "safety_risk_score": safety_risk_score,
            "holder_evidence_id": 1,
            "holder_inputs_hash": _E2E_HOLDER_INPUTS_HASH,
            "holder_observed_at": 1.0,
            "liquidity_source": "curve_snapshot",
            "liquidity_observed_at": snapshot["t_wall"],
            "raw": {
                "liquidity_sol": snapshot["real_sol_reserves"] / 1_000_000_000,
                "curve_progress_pct": curve_progress_pct,
                "curve_snapshot": snapshot,
                "sampled_token_accounts": 3,
                "distinct_non_curve_owners": 2,
                "top10_non_curve_owner_share_pct": 40.0,
                "creator_prior_successes": 0,
                "creator_prior_rugs": 0,
                "creator_reputation_event_ids": [],
                "social": social_diagnostics,
            },
            "components_ppm": components_ppm,
            "rank_points": rank_points,
            "rank": 1,
        }
        ranking_inputs = {
            "subject_mint": mint,
            "target_report_id": target_report_id,
            "latest_target_report_id": target_report_id,
            "resolved_at": decision_at,
            "cluster_key": cluster_key,
            "resolver_version": _E2E_RESOLVER_VERSION,
            "weights_version": _E2E_WEIGHTS_VERSION,
            "config_hash": _E2E_CONFIG_HASH,
            "counterfactual_horizons_s": list(_E2E_HORIZONS),
            "limits": {
                "max_cluster_candidates": 8,
                "liquidity_max_age_s": 300.0,
                "holder_max_age_s": 300.0,
                "comparison_price_max_age_s": 300.0,
            },
            "component_parameters": {
                "graduation_sol": 85.0,
                "holder_owner_target": 20,
                "top10_holder_max_pct": 60.0,
                "token_decimals": _E2E_TOKEN_DECIMALS,
                "creator_reputation_as_of": decision_at,
            },
            "weights_bps": dict(_E2E_WEIGHTS_BPS),
            "social_weights_bps": dict(_E2E_SOCIAL_WEIGHTS_BPS),
            "candidates": [candidate],
        }
        inputs_hash = hashlib.sha256(json.dumps(
            ranking_inputs, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode()).hexdigest()
        generation_hash = canonical_generation_hash(
            cluster_key=cluster_key,
            eligible=({
                "mint": mint,
                "safety_report_id": target_report_id,
                "holder_evidence_id": 1,
            },),
            canonical_mint=mint,
            resolver_version=_E2E_RESOLVER_VERSION,
            weights_version=_E2E_WEIGHTS_VERSION,
            config_hash=_E2E_CONFIG_HASH,
        )
        return CanonicalResolution(
            verdict=CanonicalVerdict(
                resolver_version=_E2E_RESOLVER_VERSION,
                weights_version=_E2E_WEIGHTS_VERSION,
                status="CANONICAL",
                reason="canonical_selected",
                resolved_at=decision_at,
                cluster_key=cluster_key,
                cluster_size=1,
                eligible_cluster_size=1,
                canonical_mint=mint,
                rank=1,
                rank_points=rank_points,
                generation_hash=generation_hash,
                inputs_hash=inputs_hash,
                ranking_inputs=ranking_inputs,
            ),
            observations=(CanonicalObservationDraft(
                mint=mint,
                is_subject=True,
                is_canonical=True,
                eligible=True,
                start_price_sol=snapshot["spot_price_sol"],
                price_observed_at=snapshot["t_wall"],
                unavailable_reason="",
            ),),
        )


_DETERMINISTIC_CANONICAL_RESOLVER = _DeterministicCanonicalResolver()


def _save_e2e_safety_evidence(conn, *, mint, risk_score):
    dummy_report_id = save_safety_report(
        conn,
        mint=mint,
        raw_completed_at=0.5,
        segment="CLIMBING",
        hard_fails=["dummy_superseded_report"],
        risk_score=100.0,
        results_json="[]",
        inputs_hash="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    report_id = save_safety_report(
        conn,
        mint=mint,
        raw_completed_at=1.0,
        segment="CLIMBING",
        hard_fails=[],
        risk_score=risk_score,
        results_json="[]",
        inputs_hash="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    )
    holder_id = conn.execute(
        "INSERT INTO holder_evidence("
        "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
        "top10_non_curve_owner_share_pct,holder_observed_at,unavailable_reason,"
        "inputs_hash) VALUES (?,?,?,?,?,?,?)",
        (report_id, 3, 2, 40.0, 1.0, "", _E2E_HOLDER_INPUTS_HASH),
    ).lastrowid
    conn.commit()
    assert dummy_report_id == holder_id == 1
    assert report_id == 2
    assert report_id != holder_id
    return report_id


class _EmptyReplayJournal:
    def iter_events(self, *, since_wall, until_wall):
        del since_wall, until_wall
        return iter(())


_EMPTY_REPLAY_JOURNAL = _EmptyReplayJournal()


def test_e2e_tracker_fixture_supplies_required_journal_and_bounds():
    import ast
    from pathlib import Path

    assert vars(_EMPTY_REPLAY_JOURNAL) == {}
    assert list(
        _EMPTY_REPLAY_JOURNAL.iter_events(since_wall=1.0, until_wall=2.0)
    ) == []

    tree = ast.parse(Path(__file__).read_text())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ForwardReturnTracker"
    ]
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
    null_journals = []
    for call in calls:
        keywords = {keyword.arg for keyword in call.keywords}
        missing = required - keywords
        if missing:
            omitted.append((call.lineno, sorted(missing)))
        journal = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "journal"),
            None,
        )
        if journal is None or (
            isinstance(journal, ast.Constant) and journal.value is None
        ):
            null_journals.append(call.lineno)

    assert len(calls) == 1
    assert omitted == []
    assert null_journals == []
    keywords = {keyword.arg: keyword.value for keyword in calls[0].keywords}
    expected = {
        "journal": "_EMPTY_REPLAY_JOURNAL",
        "clock": "lambda: now[0]",
        "horizons": "_E2E_HORIZONS",
        "token_decimals": "_E2E_TOKEN_DECIMALS",
        "stale_price_after_s": "300.0",
        "reconcile_interval_s": "60.0",
        "price_history_retention_s": "90000.0",
        "price_history_max_samples_per_mint": "10000",
        "price_history_max_mints": "1000",
        "max_in_memory_pending_observations": "50000",
    }
    assert set(keywords) == set(expected)
    assert {
        name: ast.dump(expression, include_attributes=False)
        for name, expression in keywords.items()
    } == {
        name: ast.dump(ast.parse(source, mode="eval").body, include_attributes=False)
        for name, source in expected.items()
    }


def test_climbing_feature_fixture_supplies_mint_cap():
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


def test_climbing_fixture_supplies_canonical_resolver(tmp_path):
    import ast
    import inspect
    import symtable
    from pathlib import Path

    from memebot.store import (allocate_p3_causal_wall, p3_immediate_transaction,
                               record_decision_with_canonical_observations)

    fixture_names = (
        "test_e2e_climbing_buys_then_ladder_sells",
        "test_incident_shaped_low_score_skip_emits_watch_without_broker_or_trades",
    )
    source = Path(__file__).read_text()
    tree = ast.parse(source)
    fixture_calls = []
    omitted = []
    unexpected = []
    unexpected_config_hashes = []
    shadowed = []
    fixture_functions = tuple(
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in fixture_names
    )
    for function in fixture_functions:
        shadowed.extend(
            (function.name, node.lineno)
            for node in ast.walk(function)
            if (
                isinstance(node, ast.Name)
                and node.id == "_DETERMINISTIC_CANONICAL_RESOLVER"
                and isinstance(node.ctx, ast.Store)
            )
            or (
                isinstance(node, ast.arg)
                and node.arg == "_DETERMINISTIC_CANONICAL_RESOLVER"
            )
        )
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
            elif not (
                isinstance(resolver, ast.Name)
                and resolver.id == "_DETERMINISTIC_CANONICAL_RESOLVER"
            ):
                unexpected.append(function.name)
            config_hash = next(
                (
                    keyword.value
                    for keyword in call.keywords
                    if keyword.arg == "config_hash"
                ),
                None,
            )
            if not (
                isinstance(config_hash, ast.Name)
                and config_hash.id == "_E2E_CONFIG_HASH"
            ):
                unexpected_config_hashes.append(function.name)

    assert sorted(fixture_calls) == sorted(fixture_names)
    assert omitted == []
    assert unexpected == []
    assert unexpected_config_hashes == []
    assert shadowed == []
    lexical_functions = {
        table.get_name(): table
        for table in symtable.symtable(source, __file__, "exec").get_children()
        if table.get_name() in fixture_names
    }
    assert sorted(lexical_functions) == sorted(fixture_names)
    for table in lexical_functions.values():
        resolver_symbol = table.lookup("_DETERMINISTIC_CANONICAL_RESOLVER")
        assert resolver_symbol.is_global()
        assert not resolver_symbol.is_local()

    tracker_calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ForwardReturnTracker"
    ]
    assert len(tracker_calls) == 1
    tracker_horizons = next(
        keyword.value
        for keyword in tracker_calls[0].keywords
        if keyword.arg == "horizons"
    )
    assert isinstance(tracker_horizons, ast.Name)
    assert tracker_horizons.id == "_E2E_HORIZONS"
    tracker_token_decimals = next(
        keyword.value
        for keyword in tracker_calls[0].keywords
        if keyword.arg == "token_decimals"
    )
    assert isinstance(tracker_token_decimals, ast.Name)
    assert tracker_token_decimals.id == "_E2E_TOKEN_DECIMALS"

    parameters = inspect.signature(
        _DETERMINISTIC_CANONICAL_RESOLVER.resolve
    ).parameters
    assert tuple(parameters) == (
        "mint",
        "decision_at",
        "target_report_id",
        "target_snapshot",
    )
    assert parameters["decision_at"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["target_report_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["target_snapshot"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["target_snapshot"].default is None

    conn = open_db(tmp_path / "resolver-shape.db", migration_clock=lambda: 0.0)
    upsert_token_identity(
        conn,
        mint="M",
        raw_ingested_at=0.0,
        bonding_curve_key="BC",
        fields={"name": "Fixture", "symbol": "FX"},
    )
    set_token_state(conn, "M", "CLIMBING")
    report_id = _save_e2e_safety_evidence(conn, mint="M", risk_score=0.0)
    identity = conn.execute(
        "SELECT mint,state,rugged,p3_identity_ingested_at,meta_json "
        "FROM tokens WHERE mint='M'",
    ).fetchone()
    assert identity["state"] == "CLIMBING"
    assert identity["rugged"] == 0
    assert identity["p3_identity_ingested_at"] == math.nextafter(0.0, math.inf)
    identity_meta = json.loads(identity["meta_json"])
    assert identity_meta["name"] == "Fixture"
    assert identity_meta["symbol"] == "FX"
    assert identity_meta["identity_observed_at"] == {
        "name": math.nextafter(0.0, math.inf),
        "symbol": math.nextafter(0.0, math.inf),
    }
    safety = conn.execute(
        "SELECT id,checked_at,hard_fails_json,risk_score,inputs_hash "
        "FROM safety_reports WHERE id=?",
        (report_id,),
    ).fetchone()
    holder = conn.execute(
        "SELECT id,safety_report_id,sampled_token_accounts,"
        "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
        "holder_observed_at,unavailable_reason,inputs_hash "
        "FROM holder_evidence WHERE safety_report_id=?",
        (report_id,),
    ).fetchone()
    assert safety["id"] == report_id == 2
    assert holder["id"] == 1
    assert holder["id"] != report_id
    latest_report_id = conn.execute(
        "SELECT id FROM safety_reports WHERE mint='M' ORDER BY id DESC LIMIT 1",
    ).fetchone()["id"]
    assert latest_report_id == report_id

    from memebot.features import CurveSnapshot

    target_snapshot = CurveSnapshot(
        source_boot_id=7,
        source_seq=11,
        t_wall=1.75,
        t_mono=12.5,
        virtual_sol_reserves=32_000_000_000,
        virtual_token_reserves=800_000_000_000_000,
        real_sol_reserves=2_000_000_000,
        real_token_reserves=3,
        liquidity_sol=2.0,
        spot_price_sol=4e-8,
        progress_pct=35.0,
    )

    from dataclasses import fields, is_dataclass

    def mutable_container_ids(value):
        mutable_ids = set()
        visited = set()

        def visit(item):
            identity_id = id(item)
            if identity_id in visited:
                return
            visited.add(identity_id)
            if isinstance(item, dict):
                mutable_ids.add(identity_id)
                for key, nested in item.items():
                    visit(key)
                    visit(nested)
            elif isinstance(item, (list, set, bytearray)):
                mutable_ids.add(identity_id)
                for nested in item:
                    visit(nested)
            elif is_dataclass(item) and not isinstance(item, type):
                for field in fields(item):
                    visit(getattr(item, field.name))
            elif isinstance(item, (tuple, frozenset)):
                for nested in item:
                    visit(nested)

        visit(value)
        return mutable_ids

    resolver_class_fields = {
        name for name in vars(_DeterministicCanonicalResolver)
        if not name.startswith("__")
    }
    assert resolver_class_fields == {"resolve"}

    with p3_immediate_transaction(conn):
        decision_at = allocate_p3_causal_wall(conn, raw_wall=2.0)
        assert vars(_DETERMINISTIC_CANONICAL_RESOLVER) == {}
        results = tuple(
            _DETERMINISTIC_CANONICAL_RESOLVER.resolve(
                "M",
                decision_at=decision_at,
                target_report_id=report_id,
                target_snapshot=target_snapshot,
            )
            for _ in range(5)
        )
        assert all(result == results[0] for result in results[1:])
        assert vars(_DETERMINISTIC_CANONICAL_RESOLVER) == {}
        mutable_graphs = tuple(mutable_container_ids(result) for result in results)
        assert all(mutable_graphs)
        for index, graph in enumerate(mutable_graphs):
            assert all(
                graph.isdisjoint(other)
                for other in mutable_graphs[index + 1:]
            )

        pristine_third = results[2]
        results[0].verdict.ranking_inputs["weights_bps"]["first_mover"] = 0
        results[0].verdict.ranking_inputs["counterfactual_horizons_s"].append(7200.0)
        results[0].verdict.ranking_inputs["candidates"][0]["raw"]["social"]["uri"][
            "present"
        ] = True
        assert pristine_third.verdict.ranking_inputs["weights_bps"] == (
            _E2E_WEIGHTS_BPS
        )
        assert pristine_third.verdict.ranking_inputs[
            "counterfactual_horizons_s"
        ] == list(_E2E_HORIZONS)
        assert not pristine_third.verdict.ranking_inputs["candidates"][0]["raw"][
            "social"
        ]["uri"]["present"]
        first = _DETERMINISTIC_CANONICAL_RESOLVER.resolve(
            "M",
            decision_at=decision_at,
            target_report_id=report_id,
            target_snapshot=target_snapshot,
        )
        assert first == pristine_third
        assert vars(_DETERMINISTIC_CANONICAL_RESOLVER) == {}
        assert {
            name for name in vars(_DeterministicCanonicalResolver)
            if not name.startswith("__")
        } == {"resolve"}
        incident = _DETERMINISTIC_CANONICAL_RESOLVER.resolve(
            "INCIDENT",
            decision_at=decision_at,
            target_report_id=report_id,
            target_snapshot=target_snapshot,
        )
        ranking_inputs = first.verdict.ranking_inputs
        candidate = ranking_inputs["candidates"][0]
        assert ranking_inputs["subject_mint"] == identity["mint"]
        assert ranking_inputs["target_report_id"] == safety["id"]
        assert ranking_inputs["latest_target_report_id"] == latest_report_id
        assert ranking_inputs["resolved_at"] == decision_at
        assert ranking_inputs["cluster_key"] == first.verdict.cluster_key == (
            f"{identity_meta['name'].casefold()}:{identity_meta['symbol'].casefold()}"
        )
        assert ranking_inputs["resolver_version"] == (
            first.verdict.resolver_version
        ) == _E2E_RESOLVER_VERSION
        assert ranking_inputs["weights_version"] == (
            first.verdict.weights_version
        ) == _E2E_WEIGHTS_VERSION
        assert ranking_inputs["config_hash"] == _E2E_CONFIG_HASH
        assert ranking_inputs["weights_bps"] == _E2E_WEIGHTS_BPS
        assert ranking_inputs["social_weights_bps"] == _E2E_SOCIAL_WEIGHTS_BPS
        assert ranking_inputs["component_parameters"]["token_decimals"] == (
            PUMP_CFG["token_decimals"]
        ) == _E2E_TOKEN_DECIMALS
        assert ranking_inputs["component_parameters"][
            "creator_reputation_as_of"
        ] == decision_at
        assert candidate["mint"] == identity["mint"]
        assert candidate["state"] == identity["state"]
        assert candidate["rugged"] == identity["rugged"]
        assert candidate["normalized_name"] == identity_meta["name"].casefold()
        assert candidate["normalized_symbol"] == identity_meta["symbol"].casefold()
        assert candidate["creator"] == identity_meta.get("creator", "")
        assert candidate["identity_conflicts"] == identity_meta.get(
            "identity_conflicts", []
        )
        assert candidate["eligible"] is True
        assert candidate["ineligible_reason"] == ""
        assert candidate["p3_identity_ingested_at"] == identity[
            "p3_identity_ingested_at"
        ]
        assert candidate["identity_observed_at"] == identity_meta[
            "identity_observed_at"
        ]
        assert candidate["safety_report_id"] == safety["id"]
        assert candidate["safety_checked_at"] == safety["checked_at"]
        assert candidate["safety_inputs_hash"] == safety["inputs_hash"]
        assert candidate["safety_hard_fails"] == json.loads(
            safety["hard_fails_json"]
        )
        assert candidate["safety_risk_score"] == safety["risk_score"]
        assert candidate["holder_evidence_id"] == holder["id"]
        assert candidate["holder_inputs_hash"] == holder["inputs_hash"]
        assert candidate["holder_observed_at"] == holder["holder_observed_at"]
        assert candidate["raw"]["sampled_token_accounts"] == holder[
            "sampled_token_accounts"
        ]
        assert candidate["raw"]["distinct_non_curve_owners"] == holder[
            "distinct_non_curve_owners"
        ]
        assert candidate["raw"]["top10_non_curve_owner_share_pct"] == holder[
            "top10_non_curve_owner_share_pct"
        ]
        reputation_rows = conn.execute(
            "SELECT id,outcome FROM creator_reputation_events WHERE mint='M'",
        ).fetchall()
        assert reputation_rows == []
        assert candidate["raw"]["creator_prior_successes"] == 0
        assert candidate["raw"]["creator_prior_rugs"] == 0
        assert candidate["raw"]["creator_reputation_event_ids"] == []
        for field in ("uri", "website", "twitter", "telegram"):
            diagnostic = candidate["raw"]["social"][field]
            assert identity_meta.get(field) == ""
            assert diagnostic["value"] is None
            assert diagnostic["present"] is False
            assert diagnostic["reuse"] is False
            assert diagnostic["cluster_conflict"] is False
            assert diagnostic["metadata_conflict"] is False
        assert holder["safety_report_id"] == safety["id"]
        assert holder["unavailable_reason"] == ""
        assert ranking_inputs["counterfactual_horizons_s"] == list(_E2E_HORIZONS)
        expected_snapshot = {
            "t_wall": target_snapshot.t_wall,
            "t_mono": target_snapshot.t_mono,
            "virtual_sol_reserves": target_snapshot.virtual_sol_reserves,
            "virtual_token_reserves": target_snapshot.virtual_token_reserves,
            "real_sol_reserves": target_snapshot.real_sol_reserves,
            "real_token_reserves": target_snapshot.real_token_reserves,
            "spot_price_sol": target_snapshot.spot_price_sol,
        }
        assert candidate["raw"]["curve_snapshot"] == expected_snapshot
        assert candidate["liquidity_observed_at"] == target_snapshot.t_wall
        assert candidate["raw"]["liquidity_sol"] == target_snapshot.liquidity_sol
        assert candidate["raw"]["curve_progress_pct"] == target_snapshot.progress_pct
        assert first.observations[0].start_price_sol == target_snapshot.spot_price_sol
        assert first.observations[0].price_observed_at == target_snapshot.t_wall
        assert incident.verdict.ranking_inputs["candidates"][0][
            "safety_risk_score"
        ] == 90.0
        canonical = {
            **asdict(first.verdict),
            "config_hash": _E2E_CONFIG_HASH,
            "ranking_order": ["M"],
        }
        decision_id, observation_ids, analysis_primary = (
            record_decision_with_canonical_observations(
                conn,
                at=decision_at,
                mint="M",
                segment="CLIMBING",
                action="BUY",
                score=80.0,
                feature_vector={"canonical": canonical},
                safety_report_id=report_id,
                config_hash=_E2E_CONFIG_HASH,
                generation_hash=first.verdict.generation_hash,
                observations=first.observations,
                score_status="VALID",
                score_weights_version="climbing-v1",
                score_unavailable_reason="",
                planned_position_size_sol=0.2,
            )
        )

    assert decision_id == 1
    assert observation_ids == (1,)
    assert analysis_primary
    assert conn.execute(
        "SELECT config_hash FROM decisions WHERE id=?", (decision_id,),
    ).fetchone()["config_hash"] == _E2E_CONFIG_HASH
    persisted = conn.execute(
        "SELECT feature_vector_json FROM decisions WHERE id=?", (decision_id,),
    ).fetchone()["feature_vector_json"]
    persisted_canonical = json.loads(persisted)["canonical"]
    assert persisted_canonical["inputs_hash"] == first.verdict.inputs_hash
    assert persisted_canonical["ranking_inputs"]["candidates"][0]["raw"][
        "curve_snapshot"
    ] == expected_snapshot
    persisted_observation = conn.execute(
        "SELECT start_price_sol,price_observed_at FROM canonical_observations "
        "WHERE id=?",
        (observation_ids[0],),
    ).fetchone()
    assert persisted_observation["start_price_sol"] == target_snapshot.spot_price_sol
    assert persisted_observation["price_observed_at"] == target_snapshot.t_wall


def test_climbing_safety_fixture_hashes_are_v5_valid():
    import ast
    import re
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    fixture_calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "save_safety_report"
    ]

    assert fixture_calls
    for call in fixture_calls:
        inputs_hash = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "inputs_hash"),
            None,
        )
        assert isinstance(inputs_hash, ast.Constant)
        assert isinstance(inputs_hash.value, str)
        assert re.fullmatch(r"[0-9a-f]{64}", inputs_hash.value)


def test_climbing_safety_fixture_uses_raw_completed_at():
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


def _cp(mint, t, vsol, real_sol, vtok=900_000_000_000_000):
    return CurveProgress(t_wall=t, t_mono=t, mint=mint, progress_pct=25.0,
                         virtual_sol_reserves=vsol, virtual_token_reserves=vtok,
                         real_sol_reserves=real_sol, real_token_reserves=0)


async def test_e2e_climbing_buys_then_ladder_sells(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 0.0)
    upsert_token_identity(
        conn, mint="M", raw_ingested_at=0.0, bonding_curve_key="BC",
        fields={"name": "Fixture", "symbol": "FX"},
    )
    set_token_state(conn, "M", "CLIMBING")
    report_id = _save_e2e_safety_evidence(conn, mint="M", risk_score=0.0)
    now = [100.0]
    bus = EventBus()
    entries = bus.subscribe(PaperEntry)
    exits = bus.subscribe(PaperExit)

    fe = FeatureEngine(bus, max_feature_mints=DEFAULT_MAX_MINTS)
    strat = ClimbingStrategy(bus, conn, feature_engine=fe, scorer=ConfluenceScorer(SCORER_CFG),
                             broker=PaperBroker(FILL_CFG, PUMP_CFG),
                             canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER,
                             strat_cfg=STRAT_CFG,
                             pumpfun_cfg=PUMP_CFG, config_hash=_E2E_CONFIG_HASH,
                             fill_cfg=FILL_CFG,
                             exits_cfg=EXITS_CFG, clock=lambda: now[0],
                             mono_clock=lambda: 10.0)
    ctf = ForwardReturnTracker(
        bus, conn, journal=_EMPTY_REPLAY_JOURNAL, clock=lambda: now[0],
        horizons=_E2E_HORIZONS, token_decimals=_E2E_TOKEN_DECIMALS,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0, price_history_retention_s=90000.0,
        price_history_max_samples_per_mint=10000, price_history_max_mints=1000,
        max_in_memory_pending_observations=50000,
    )
    stop = asyncio.Event()
    tasks = [asyncio.create_task(c.run(stop)) for c in (fe, strat, ctf)]

    # rising accumulation -> high velocity
    await bus.publish(_cp("M", t=0.0, vsol=31_000_000_000, real_sol=1_000_000_000))
    await bus.publish(_cp("M", t=10.0, vsol=31_000_000_000, real_sol=6_000_000_000))
    await asyncio.sleep(0.1)
    # decision made now (BUY + pending); NO fill yet (latency penalty)
    await bus.publish(SafetyPassed(t_wall=1, t_mono=10.0, mint="M", segment="CLIMBING",
                                   safety_report_id=report_id, risk_score=0.0))
    await asyncio.sleep(0.1)
    # the NEXT snapshot (>= T later; t_mono 13 >= 10) fills the pending entry
    await bus.publish(replace(
        _cp("M", t=13.0, vsol=31_000_000_000, real_sol=6_500_000_000),
        t_wall=100.0,
    ))
    entry = await asyncio.wait_for(entries.get(), 2)
    assert entry.mint == "M"

    # price triples -> ladder rung 0 (2.0x) sells 50%
    await bus.publish(_cp("M", t=20.0, vsol=int(31_000_000_000 * 3),
                          real_sol=6_000_000_000))
    ex = await asyncio.wait_for(exits.get(), 2)
    assert ex.reason == "ladder_0"

    # +1h later, counterfactual flushes for the decision
    now[0] = 100.0 + 3600.0
    await bus.publish(_cp("M", t=3620.0, vsol=int(31_000_000_000 * 3), real_sol=6_000_000_000))
    await asyncio.sleep(0.2)

    stop.set()
    await asyncio.gather(*tasks)

    assert conn.execute("SELECT COUNT(*) FROM decisions WHERE action='BUY'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM paper_trades WHERE side='buy'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM paper_trades WHERE side='sell'").fetchone()[0] >= 1
    assert conn.execute(
        "SELECT COUNT(*) FROM outcomes WHERE ref_kind='candidate'").fetchone()[0] >= 1


async def test_incident_shaped_low_score_skip_emits_watch_without_broker_or_trades(tmp_path):
    from memebot.events import CandidateScored, PaperEntry, PaperExit
    from memebot.main import _watch_alert_loop
    from memebot.telegram import FakeTransport, TelegramOps

    class NoTradeBroker:
        def __init__(self):
            self.calls = []

        def buy(self, *args, **kwargs):
            self.calls.append(("buy", args, kwargs))
            raise AssertionError("WATCH/SKIP must never call PaperBroker.buy")

        def sell(self, *args, **kwargs):
            self.calls.append(("sell", args, kwargs))
            raise AssertionError("WATCH/SKIP must never call PaperBroker.sell")

    conn = open_db(tmp_path / "watch.db", migration_clock=lambda: 0.0)
    upsert_token_identity(
        conn, mint="INCIDENT", raw_ingested_at=0.0, bonding_curve_key="BC",
        fields={"name": "Fixture", "symbol": "FX"},
    )
    set_token_state(conn, "INCIDENT", "CLIMBING")
    report_id = _save_e2e_safety_evidence(
        conn, mint="INCIDENT", risk_score=90.0,
    )
    bus = EventBus()
    entries = bus.subscribe(PaperEntry)
    exits = bus.subscribe(PaperExit)
    watch_queue = bus.subscribe(CandidateScored)
    feature_engine = FeatureEngine(bus, max_feature_mints=DEFAULT_MAX_MINTS)
    broker = NoTradeBroker()
    strategy = ClimbingStrategy(
        bus, conn, feature_engine=feature_engine, scorer=ConfluenceScorer(SCORER_CFG),
        broker=broker, canonical_resolver=_DETERMINISTIC_CANONICAL_RESOLVER,
        strat_cfg=STRAT_CFG, pumpfun_cfg=PUMP_CFG, config_hash=_E2E_CONFIG_HASH,
        fill_cfg=FILL_CFG, exits_cfg=EXITS_CFG, clock=lambda: 100.0,
    )
    transport = FakeTransport()
    ops = TelegramOps(transport, chat_id="C", max_alerts_per_hour=1, clock=lambda: 100.0)
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(feature_engine.run(stop)),
        asyncio.create_task(strategy.run(stop)),
        asyncio.create_task(_watch_alert_loop(bus, ops, stop, queue=watch_queue)),
    ]

    # Incident order: safety passes before FeatureEngine reaches min_samples.
    await bus.publish(SafetyPassed(
        t_wall=1.0, t_mono=1.0, mint="INCIDENT", segment="CLIMBING",
        safety_report_id=report_id, risk_score=90.0,
    ))
    await bus.publish(_cp("INCIDENT", t=2.0, vsol=31_000_000_000,
                          real_sol=1_000_000_000))
    await bus.publish(_cp("INCIDENT", t=12.0, vsol=31_000_000_000,
                          real_sol=1_000_000_100))

    for _ in range(50):
        if transport.sent:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.gather(*tasks)

    assert len(transport.sent) == 1
    assert transport.sent[0]["text"].startswith("👀 WATCH — NOT A BUY\n")
    assert conn.execute(
        "SELECT action FROM decisions WHERE mint='INCIDENT'"
    ).fetchone()["action"] == "SKIP"
    assert conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
    assert entries.empty() and exits.empty()
    assert broker.calls == []


async def test_watch_only_high_score_never_enters_but_existing_legacy_position_still_exits(
    tmp_path,
):
    from memebot.events import CandidateScored
    from memebot.main import _watch_alert_loop
    from memebot.store import record_decision, record_paper_trade
    from memebot.strategy import PendingEntry
    from memebot.telegram import FakeTransport, TelegramOps

    class WatchOnlyBroker:
        def __init__(self):
            self.real = PaperBroker(FILL_CFG, PUMP_CFG)
            self.buy_calls = 0
            self.sell_calls = 0

        def buy(self, *args, **kwargs):
            self.buy_calls += 1
            raise AssertionError("WATCH-only high score must never call PaperBroker.buy")

        def sell(self, *args, **kwargs):
            self.sell_calls += 1
            return self.real.sell(*args, **kwargs)

    async def wait_for_state(predicate, tasks, message):
        for _ in range(1_000):
            if predicate():
                return
            for task in tasks:
                if task.done():
                    exception = task.exception()
                    assert exception is None, f"{task.get_name()} failed: {exception!r}"
                    raise AssertionError(f"{task.get_name()} stopped before {message}")
            await asyncio.sleep(0)
        assert predicate(), message

    conn = open_db(
        tmp_path / "watch-high-and-legacy.db", migration_clock=lambda: 0.0,
    )
    for mint in ("WATCH_HIGH", "LEGACY"):
        upsert_token_identity(
            conn,
            mint=mint,
            raw_ingested_at=0.0,
            bonding_curve_key=f"{mint}_BC",
            fields={},
        )
        set_token_state(conn, mint, "CLIMBING")
    report_id = save_safety_report(
        conn, mint="WATCH_HIGH", raw_completed_at=11.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="261e89d2eb0d1d77eea65ac44bd88488e75d8ee29fc68ac54e8b630b5239e391",
    )
    legacy_decision_id = record_decision(
        conn, at=10.0, mint="LEGACY", segment="CLIMBING", action="BUY", score=50.0,
        feature_vector={"spot_price_sol": 1e-9}, config_hash="legacy-cfg",
    )
    record_paper_trade(
        conn, decision_id=legacy_decision_id, at=20.0, mint="LEGACY",
        segment="CLIMBING", side="buy", qty=1_000.0, quote_price=1e-9,
        fill_price=1e-9, fees={}, realism_grade="A",
    )
    legacy_buy = conn.execute(
        "SELECT canonical_recheck_id, canonical_proof_hash, p3_entry_execution_id "
        "FROM paper_trades WHERE decision_id=? AND side='buy'",
        (legacy_decision_id,),
    ).fetchone()
    assert tuple(legacy_buy) == (None, None, None)

    now = [100.0]
    bus = EventBus()
    scored_events = bus.subscribe(CandidateScored)
    watch_queue = bus.subscribe(CandidateScored)
    pending_entries = bus.subscribe(PendingEntry)
    paper_entries = bus.subscribe(PaperEntry)
    paper_exits = bus.subscribe(PaperExit)
    feature_engine = FeatureEngine(bus, max_feature_mints=DEFAULT_MAX_MINTS)
    broker = WatchOnlyBroker()
    strat_cfg = {**STRAT_CFG, "entries_enabled": False, "score_threshold": 40.0}
    exits_cfg = {**EXITS_CFG, "ladder_fractions": [1.0]}
    strategy = ClimbingStrategy(
        bus, conn, feature_engine=feature_engine, scorer=ConfluenceScorer(SCORER_CFG),
        broker=broker, strat_cfg=strat_cfg, pumpfun_cfg=PUMP_CFG, config_hash="watch-cfg",
        fill_cfg=FILL_CFG, exits_cfg=exits_cfg, clock=lambda: now[0],
    )
    assert strategy.reconcile() == 1
    assert set(strategy.positions) == {"LEGACY"}

    transport = FakeTransport()
    ops = TelegramOps(transport, chat_id="C", max_alerts_per_hour=1,
                      clock=lambda: now[0])
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(feature_engine.run(stop), name="W5 FeatureEngine"),
        asyncio.create_task(strategy.run(stop), name="W5 ClimbingStrategy"),
        asyncio.create_task(
            _watch_alert_loop(bus, ops, stop, queue=watch_queue), name="W5 WATCH alerts",
        ),
    ]

    try:
        await bus.publish(_cp("WATCH_HIGH", t=1.0, vsol=31_000_000_000,
                              real_sol=1_000_000_000))
        await bus.publish(_cp("WATCH_HIGH", t=11.0, vsol=31_000_000_000,
                              real_sol=6_000_000_000))
        await wait_for_state(
            lambda: feature_engine.features_as_of(
                "WATCH_HIGH", t_mono=11.0, now=now[0], created_at=0.0,
                risk_score=0.0, min_samples=2,
            ) is not None,
            tasks, "causal rising samples were not observed",
        )
        await bus.publish(SafetyPassed(
            t_wall=11.0, t_mono=11.0, mint="WATCH_HIGH", segment="CLIMBING",
            safety_report_id=report_id, risk_score=0.0,
        ))
        await wait_for_state(
            lambda: not scored_events.empty() and bool(transport.sent),
            tasks, "high-score WATCH decision and alert were not delivered",
        )

        # This later event is an explosive BUY witness for an entries_enabled bypass mutant.
        await bus.publish(_cp("WATCH_HIGH", t=13.0, vsol=31_000_000_000,
                              real_sol=6_500_000_000))
        await wait_for_state(
            lambda: strategy._curve_order.get("WATCH_HIGH") == 13.0,
            tasks, "post-decision BUY witness was not processed",
        )

        # The same WATCH-only strategy must continue managing a reconciled legacy position.
        await bus.publish(_cp("LEGACY", t=30.0, vsol=31_000_000_000,
                              real_sol=6_000_000_000))
        await wait_for_state(
            lambda: broker.sell_calls == 1 and not paper_exits.empty(),
            tasks, "legacy exit did not complete while entries were disabled",
        )
    finally:
        stop.set()
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

    assert task_results == [None, None, None]
    assert all(task.done() and not task.cancelled() and task.exception() is None for task in tasks)

    scored = scored_events.get_nowait()
    assert scored.mint == "WATCH_HIGH"
    assert scored.score >= strat_cfg["score_threshold"]
    assert scored_events.empty()
    assert len(transport.sent) == 1
    assert transport.sent[0]["text"].startswith((
        "WATCH — NOT A BUY", "👀 WATCH — NOT A BUY",
    ))
    assert broker.buy_calls == 0
    watch_decision = conn.execute(
        "SELECT id, action, score FROM decisions WHERE mint='WATCH_HIGH'",
    ).fetchone()
    assert watch_decision["action"] == "SKIP"
    assert watch_decision["score"] >= strat_cfg["score_threshold"]
    assert "WATCH_HIGH" not in strategy._pending
    assert "WATCH_HIGH" not in strategy._pending_score
    assert pending_entries.empty()
    assert paper_entries.empty()
    assert conn.execute(
        "SELECT COUNT(*) FROM paper_trades WHERE mint='WATCH_HIGH' AND side='buy'",
    ).fetchone()[0] == 0

    assert broker.sell_calls == 1
    paper_exit = paper_exits.get_nowait()
    assert paper_exit.mint == "LEGACY"
    assert paper_exit.reason == "ladder_0"
    assert paper_exits.empty()
    assert "LEGACY" not in strategy.positions
    legacy_sell = conn.execute(
        "SELECT id, canonical_recheck_id, canonical_proof_hash, p3_entry_execution_id "
        "FROM paper_trades WHERE decision_id=? AND side='sell'",
        (legacy_decision_id,),
    ).fetchone()
    assert tuple(legacy_sell)[1:] == (None, None, None)
    legacy_outcome = conn.execute(
        "SELECT ref_kind, ref_id, p3_exit_trade_id FROM outcomes WHERE ref_kind='trade'",
    ).fetchone()
    assert tuple(legacy_outcome) == ("trade", legacy_decision_id, None)
