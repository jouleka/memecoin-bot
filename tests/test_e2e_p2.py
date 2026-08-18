import asyncio
import hashlib
import json
import math
from dataclasses import asdict, replace
from fractions import Fraction

import httpx

from memebot.broker import PaperBroker
from memebot.bus import EventBus
from memebot.canonical import (canonical_generation_hash, creator_component,
                               holder_component, integer_rank_points,
                               quantize_component)
from memebot.early_buyers import EarlyBuyerSnapshot
from memebot.events import CurveProgress, LifecycleTransition
from memebot.features import (ClimbingFeatures, CurveSnapshot, DEFAULT_MAX_MINTS,
                              FeatureEngine)
from memebot.safety.gate import GateRunner, LiveProbes, SafetyGate
from memebot.safety.governor import Governor
from memebot.scoring import ConfluenceScorer
from memebot.store import (allocate_p3_causal_wall, open_db,
                           p3_immediate_transaction,
                           record_decision_with_canonical_observations,
                           record_early_buyer_read, record_wallet_pnl_event,
                           set_token_state, upsert_token_identity)
from memebot.strategy import ClimbingStrategy
from tests.test_e2e_climbing import (
    _DETERMINISTIC_CANONICAL_RESOLVER as _BASE_CANONICAL_RESOLVER,
    _save_e2e_safety_evidence,
)

SCORER_CFG = {"weights_version": "climbing-v1", "w_velocity": 0.40, "w_progress": 0.20,
              "w_age": 0.05, "w_risk": 0.20, "w_smart_money": 0.15,
              "velocity_full_scale_sol_per_s": 0.05, "progress_full_scale_pct": 80.0,
              "age_full_scale_s": 600.0, "smart_money_quality_full_scale_sol": 5.0}
SMART_CFG = {"min_events": 1, "min_realized_pnl_sol": 1.0, "quality_full_scale_sol": 5.0}
STRAT_CFG = {"entries_enabled": True, "score_threshold": 40.0, "position_size_sol": 0.2,
             "max_concurrent_positions": 5, "max_entries_per_hour": 10, "min_samples": 2,
             "min_age_s": 0.0}
FILL_CFG = {"latency_min_s": 0.0, "extra_slippage_bps": 50, "priority_fee_sol": 0.0005,
            "solana_base_fee_sol": 0.000005, "grade_a_max_impact_pct": 2.0,
            "grade_b_max_impact_pct": 5.0, "grade_c_max_impact_pct": 10.0}
_P2_TOKEN_DECIMALS = 6
PUMP_CFG = {"protocol_fee_bps": 95, "creator_fee_bps": 30,
            "token_decimals": _P2_TOKEN_DECIMALS,
            "sellable_supply": 793_100_000.0}
_P2_CONFIG_HASH = "d" * 64
_P2_HORIZONS = (3600.0,)
_P2_RESOLVER_VERSION = "e2e-p2-deterministic-v1"
_P2_WEIGHTS_VERSION = "e2e-p2-deterministic-v1"
_P2_WEIGHTS_BPS = {
    "first_mover": 2_000,
    "liquidity": 2_000,
    "holder": 2_000,
    "creator": 2_000,
    "social": 2_000,
}
_P2_SOCIAL_WEIGHTS_BPS = {
    "uri": 2_500,
    "website": 2_500,
    "twitter": 2_500,
    "telegram": 2_500,
}


class _P2CanonicalResolver:
    """Deterministic row-33d resolver output adapted to exact P2 fixture evidence."""

    __slots__ = ("_conn",)

    def __init__(self, conn):
        self._conn = conn

    def resolve(
        self,
        mint,
        *,
        decision_at,
        target_report_id,
        target_snapshot=None,
    ):
        resolution = _BASE_CANONICAL_RESOLVER.resolve(
            mint,
            decision_at=decision_at,
            target_report_id=target_report_id,
            target_snapshot=target_snapshot,
        )
        token = self._conn.execute(
            "SELECT mint,state,rugged,p3_identity_ingested_at,meta_json "
            "FROM tokens WHERE mint=?",
            (mint,),
        ).fetchone()
        latest_safety = self._conn.execute(
            "SELECT id,checked_at,hard_fails_json,risk_score,inputs_hash "
            "FROM safety_reports WHERE mint=? AND checked_at<? "
            "ORDER BY checked_at DESC,id DESC LIMIT 1",
            (mint, decision_at),
        ).fetchone()
        if latest_safety is None or latest_safety["id"] != target_report_id:
            raise ValueError("P2 target safety report is not latest as-of decision")
        safety = latest_safety
        holder = self._conn.execute(
            "SELECT id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,inputs_hash "
            "FROM holder_evidence WHERE safety_report_id=?",
            (target_report_id,),
        ).fetchone()
        if token is None or safety is None or holder is None:
            raise ValueError("P2 canonical fixture evidence is incomplete")

        metadata = json.loads(token["meta_json"])
        ranking_inputs = json.loads(json.dumps(
            resolution.verdict.ranking_inputs,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ))
        candidate = ranking_inputs["candidates"][0]
        cluster_key = (
            f"{metadata['name'].casefold()}:{metadata['symbol'].casefold()}"
        )
        candidate.update({
            "mint": token["mint"],
            "p3_identity_ingested_at": token["p3_identity_ingested_at"],
            "state": token["state"],
            "rugged": token["rugged"],
            "normalized_name": metadata["name"].casefold(),
            "normalized_symbol": metadata["symbol"].casefold(),
            "creator": metadata.get("creator", ""),
            "identity_observed_at": metadata["identity_observed_at"],
            "identity_conflicts": metadata.get("identity_conflicts", []),
            "safety_report_id": safety["id"],
            "safety_checked_at": safety["checked_at"],
            "safety_inputs_hash": safety["inputs_hash"],
            "safety_hard_fails": json.loads(safety["hard_fails_json"]),
            "safety_risk_score": safety["risk_score"],
            "holder_evidence_id": holder["id"],
            "holder_inputs_hash": holder["inputs_hash"],
            "holder_observed_at": holder["holder_observed_at"],
        })
        candidate["raw"].update({
            "sampled_token_accounts": holder["sampled_token_accounts"],
            "distinct_non_curve_owners": holder["distinct_non_curve_owners"],
            "top10_non_curve_owner_share_pct": holder[
                "top10_non_curve_owner_share_pct"
            ],
        })
        ranking_inputs.update({
            "subject_mint": mint,
            "target_report_id": safety["id"],
            "latest_target_report_id": latest_safety["id"],
            "resolved_at": decision_at,
            "cluster_key": cluster_key,
            "resolver_version": _P2_RESOLVER_VERSION,
            "weights_version": _P2_WEIGHTS_VERSION,
            "config_hash": _P2_CONFIG_HASH,
            "counterfactual_horizons_s": list(_P2_HORIZONS),
            "weights_bps": dict(_P2_WEIGHTS_BPS),
            "social_weights_bps": dict(_P2_SOCIAL_WEIGHTS_BPS),
        })
        ranking_inputs["component_parameters"]["token_decimals"] = (
            _P2_TOKEN_DECIMALS
        )
        candidate["components_ppm"]["holder"] = quantize_component(
            holder_component(
                distinct_non_curve_owners=holder["distinct_non_curve_owners"],
                top10_share_pct=holder["top10_non_curve_owner_share_pct"],
                top10_holder_max_pct=ranking_inputs["component_parameters"][
                    "top10_holder_max_pct"
                ],
            )
        )
        candidate["components_ppm"]["creator"] = quantize_component(
            creator_component(
                creator=candidate["creator"],
                creator_conflicted=False,
                prior_successes=0,
                prior_rugs=0,
            )
        )
        candidate["rank_points"] = integer_rank_points(
            components={
                name: Fraction(value, 1_000_000)
                for name, value in candidate["components_ppm"].items()
            },
            weights_bps=ranking_inputs["weights_bps"],
        )
        inputs_hash = hashlib.sha256(json.dumps(
            ranking_inputs,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()).hexdigest()
        generation_hash = canonical_generation_hash(
            cluster_key=cluster_key,
            eligible=({
                "mint": mint,
                "safety_report_id": safety["id"],
                "holder_evidence_id": holder["id"],
            },),
            canonical_mint=mint,
            resolver_version=_P2_RESOLVER_VERSION,
            weights_version=_P2_WEIGHTS_VERSION,
            config_hash=_P2_CONFIG_HASH,
        )
        return replace(
            resolution,
            verdict=replace(
                resolution.verdict,
                resolver_version=_P2_RESOLVER_VERSION,
                weights_version=_P2_WEIGHTS_VERSION,
                cluster_key=cluster_key,
                rank_points=candidate["rank_points"],
                generation_hash=generation_hash,
                inputs_hash=inputs_hash,
                ranking_inputs=ranking_inputs,
            ),
        )


def _assert_p2_resolution_provenance(
    conn,
    resolution,
    *,
    mint,
    decision_at,
    target_report_id,
    target_snapshot,
):
    token = conn.execute(
        "SELECT mint,state,rugged,p3_identity_ingested_at,meta_json "
        "FROM tokens WHERE mint=?",
        (mint,),
    ).fetchone()
    safety = conn.execute(
        "SELECT id,checked_at,hard_fails_json,risk_score,inputs_hash "
        "FROM safety_reports WHERE id=?",
        (target_report_id,),
    ).fetchone()
    holder = conn.execute(
        "SELECT id,safety_report_id,sampled_token_accounts,"
        "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
        "holder_observed_at,inputs_hash FROM holder_evidence "
        "WHERE safety_report_id=?",
        (target_report_id,),
    ).fetchone()
    latest = conn.execute(
        "SELECT id FROM safety_reports WHERE mint=? AND checked_at<? "
        "ORDER BY checked_at DESC,id DESC LIMIT 1",
        (mint, decision_at),
    ).fetchone()
    assert token is not None and safety is not None and holder is not None
    assert latest["id"] == target_report_id

    metadata = json.loads(token["meta_json"])
    verdict = resolution.verdict
    ranking_inputs = verdict.ranking_inputs
    candidate = ranking_inputs["candidates"][0]
    assert verdict.resolver_version == _P2_RESOLVER_VERSION
    assert verdict.weights_version == _P2_WEIGHTS_VERSION
    assert verdict.resolved_at == decision_at
    assert ranking_inputs["subject_mint"] == mint
    assert ranking_inputs["target_report_id"] == target_report_id
    assert ranking_inputs["latest_target_report_id"] == latest["id"]
    assert ranking_inputs["resolved_at"] == decision_at
    assert ranking_inputs["resolver_version"] == _P2_RESOLVER_VERSION
    assert ranking_inputs["weights_version"] == _P2_WEIGHTS_VERSION
    assert ranking_inputs["config_hash"] == _P2_CONFIG_HASH
    assert ranking_inputs["counterfactual_horizons_s"] == list(_P2_HORIZONS)
    assert ranking_inputs["weights_bps"] == _P2_WEIGHTS_BPS
    assert ranking_inputs["social_weights_bps"] == _P2_SOCIAL_WEIGHTS_BPS
    assert ranking_inputs["component_parameters"]["token_decimals"] == (
        PUMP_CFG["token_decimals"]
    ) == _P2_TOKEN_DECIMALS

    assert candidate["mint"] == token["mint"]
    assert candidate["state"] == token["state"]
    assert candidate["rugged"] == token["rugged"]
    assert candidate["p3_identity_ingested_at"] == token[
        "p3_identity_ingested_at"
    ]
    assert candidate["normalized_name"] == metadata["name"].casefold()
    assert candidate["normalized_symbol"] == metadata["symbol"].casefold()
    assert candidate["creator"] == metadata.get("creator", "")
    assert candidate["identity_observed_at"] == metadata[
        "identity_observed_at"
    ]
    assert candidate["identity_conflicts"] == metadata.get(
        "identity_conflicts", []
    )
    assert candidate["safety_report_id"] == safety["id"]
    assert candidate["safety_checked_at"] == safety["checked_at"]
    assert candidate["safety_inputs_hash"] == safety["inputs_hash"]
    assert candidate["safety_hard_fails"] == json.loads(
        safety["hard_fails_json"]
    )
    assert candidate["safety_risk_score"] == safety["risk_score"]
    assert candidate["holder_evidence_id"] == holder["id"]
    assert holder["safety_report_id"] == safety["id"]
    assert candidate["holder_inputs_hash"] == holder["inputs_hash"]
    assert candidate["holder_observed_at"] == holder["holder_observed_at"]
    for field in (
        "sampled_token_accounts",
        "distinct_non_curve_owners",
        "top10_non_curve_owner_share_pct",
    ):
        assert candidate["raw"][field] == holder[field]
    assert candidate["raw"]["curve_progress_pct"] == target_snapshot.progress_pct
    assert candidate["raw"]["liquidity_sol"] == target_snapshot.liquidity_sol
    assert candidate["liquidity_observed_at"] == target_snapshot.t_wall
    assert candidate["raw"]["curve_snapshot"] == {
        "t_wall": target_snapshot.t_wall,
        "t_mono": target_snapshot.t_mono,
        "virtual_sol_reserves": target_snapshot.virtual_sol_reserves,
        "virtual_token_reserves": target_snapshot.virtual_token_reserves,
        "real_sol_reserves": target_snapshot.real_sol_reserves,
        "real_token_reserves": target_snapshot.real_token_reserves,
        "spot_price_sol": target_snapshot.spot_price_sol,
    }
    assert resolution.observations[0].start_price_sol == (
        target_snapshot.spot_price_sol
    )
    assert resolution.observations[0].price_observed_at == target_snapshot.t_wall
    return ranking_inputs


def test_p2_feature_fixture_supplies_mint_cap():
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


def test_p2_fixture_supplies_canonical_resolver(tmp_path):
    import ast
    import inspect
    import symtable
    from pathlib import Path

    import pytest

    source = Path(__file__).read_text()
    tree = ast.parse(source)
    fixtures = [
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name
        == "test_e2e_p2_strategy_scores_smart_money_from_preseeded_early_buyers"
    ]
    assert len(fixtures) == 1
    fixture = fixtures[0]
    calls = [
        call
        for call in ast.walk(fixture)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "ClimbingStrategy"
    ]
    assert len(calls) == 1
    keywords = {keyword.arg: keyword.value for keyword in calls[0].keywords}
    resolver_expression = keywords["canonical_resolver"]
    assert isinstance(resolver_expression, ast.Name)
    assert resolver_expression.id == "canonical_resolver"
    resolver_bindings = [
        node.value
        for node in fixture.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "canonical_resolver"
    ]
    assert len(resolver_bindings) == 1
    binding = resolver_bindings[0]
    assert isinstance(binding, ast.Call)
    assert isinstance(binding.func, ast.Name)
    assert binding.func.id == "_P2CanonicalResolver"
    assert len(binding.args) == 1
    assert isinstance(binding.args[0], ast.Name)
    assert binding.args[0].id == "conn"
    assert binding.keywords == []
    config_hash = keywords["config_hash"]
    assert isinstance(config_hash, ast.Name)
    assert config_hash.id == "_P2_CONFIG_HASH"
    protected = {
        "ClimbingStrategy",
        "_P2CanonicalResolver",
        "_P2_CONFIG_HASH",
        "globals",
        "type",
    }
    shadowed = [
        (node.id, node.lineno)
        for node in ast.walk(fixture)
        if isinstance(node, ast.Name)
        and node.id in protected
        and isinstance(node.ctx, ast.Store)
    ] + [
        (argument.arg, argument.lineno)
        for node in ast.walk(fixture)
        if isinstance(node, ast.arguments)
        for argument in (*node.posonlyargs, *node.args, *node.kwonlyargs)
        if argument.arg in protected
    ]
    assert shadowed == []
    lexical_fixture = next(
        table
        for table in symtable.symtable(source, __file__, "exec").get_children()
        if table.get_name() == fixture.name
    )
    for name in protected:
        symbol = lexical_fixture.lookup(name)
        assert symbol.is_global()
        assert not symbol.is_local()
    resolver_symbol = lexical_fixture.lookup("canonical_resolver")
    assert resolver_symbol.is_local()
    assert resolver_symbol.is_assigned()
    expected_runtime_guards = ast.parse(
        "assert type(strat) is globals()['ClimbingStrategy']\n"
        "assert strat._canonical_resolver is canonical_resolver"
    ).body
    fixture_asserts = [
        node for node in fixture.body if isinstance(node, ast.Assert)
    ]
    assert all(
        any(
            ast.dump(actual, include_attributes=False)
            == ast.dump(expected, include_attributes=False)
            for actual in fixture_asserts
        )
        for expected in expected_runtime_guards
    )

    parameters = inspect.signature(_P2CanonicalResolver.resolve).parameters
    assert tuple(parameters) == (
        "self",
        "mint",
        "decision_at",
        "target_report_id",
        "target_snapshot",
    )
    for name in ("decision_at", "target_report_id", "target_snapshot"):
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["target_snapshot"].default is None

    conn = open_db(tmp_path / "resolver.db", migration_clock=lambda: 0.0)
    upsert_token_identity(
        conn,
        mint="M",
        raw_ingested_at=0.0,
        bonding_curve_key="CURVE",
        fields={"name": "Fixture", "symbol": "FX", "creator": "DEV"},
    )
    set_token_state(conn, "M", "CLIMBING")
    report_id = _save_e2e_safety_evidence(conn, mint="M", risk_score=0.0)
    snapshot = CurveSnapshot(
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
    resolver = _P2CanonicalResolver(conn)
    assert _P2CanonicalResolver.__slots__ == ("_conn",)
    assert not hasattr(resolver, "__dict__")
    assert resolver._conn is conn
    with p3_immediate_transaction(conn):
        decision_at = allocate_p3_causal_wall(conn, raw_wall=2.0)
        with pytest.raises(
            ValueError,
            match="target safety report is not latest as-of decision",
        ):
            resolver.resolve(
                "M",
                decision_at=decision_at,
                target_report_id=report_id - 1,
                target_snapshot=snapshot,
            )
        changes_before_resolve = conn.total_changes
        results = tuple(
            resolver.resolve(
                "M",
                decision_at=decision_at,
                target_report_id=report_id,
                target_snapshot=snapshot,
            )
            for _ in range(3)
        )
        assert conn.total_changes == changes_before_resolve
        assert resolver._conn is conn
        assert all(result == results[0] for result in results[1:])
        assert results[0].verdict.ranking_inputs is not (
            results[1].verdict.ranking_inputs
        )
        assert results[0].verdict.ranking_inputs["candidates"] is not (
            results[1].verdict.ranking_inputs["candidates"]
        )
        results[0].verdict.ranking_inputs["weights_bps"]["holder"] = 0
        results[0].verdict.ranking_inputs["counterfactual_horizons_s"].append(
            7200.0
        )
        results[0].verdict.ranking_inputs["candidates"][0]["raw"]["social"][
            "uri"
        ]["present"] = True
        pristine = results[2]
        assert pristine.verdict.ranking_inputs["weights_bps"]["holder"] == 2000
        assert pristine.verdict.ranking_inputs[
            "counterfactual_horizons_s"
        ] == list(_P2_HORIZONS)
        assert not pristine.verdict.ranking_inputs["candidates"][0]["raw"][
            "social"
        ]["uri"]["present"]
        ranking_inputs = _assert_p2_resolution_provenance(
            conn,
            pristine,
            mint="M",
            decision_at=decision_at,
            target_report_id=report_id,
            target_snapshot=snapshot,
        )

        canonical = {
            **asdict(pristine.verdict),
            "config_hash": _P2_CONFIG_HASH,
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
                config_hash=_P2_CONFIG_HASH,
                generation_hash=pristine.verdict.generation_hash,
                observations=pristine.observations,
                score_status="VALID",
                score_weights_version="climbing-v1",
                score_unavailable_reason="",
                planned_position_size_sol=0.2,
            )
        )

    assert (decision_id, observation_ids, analysis_primary) == (1, (1,), True)
    persisted = conn.execute(
        "SELECT config_hash,feature_vector_json FROM decisions WHERE id=?",
        (decision_id,),
    ).fetchone()
    assert persisted["config_hash"] == _P2_CONFIG_HASH
    persisted_canonical = json.loads(persisted["feature_vector_json"])[
        "canonical"
    ]
    assert persisted_canonical["inputs_hash"] == pristine.verdict.inputs_hash
    assert persisted_canonical["ranking_inputs"] == ranking_inputs
    observation = conn.execute(
        "SELECT mint,start_price_sol,price_observed_at "
        "FROM canonical_observations WHERE id=?",
        (observation_ids[0],),
    ).fetchone()
    assert tuple(observation) == ("M", snapshot.spot_price_sol, snapshot.t_wall)


def _cp(mint, t, real_sol_lamports, progress=20.0):
    return CurveProgress(t_wall=t, t_mono=t, mint=mint, progress_pct=progress,
                         virtual_sol_reserves=31_000_000_000,
                         virtual_token_reserves=900_000_000_000_000,
                         real_sol_reserves=real_sol_lamports, real_token_reserves=0)


class FakeEarlyBuyerReader:
    def __init__(self):
        self.calls = []

    async def read(self, *, mint, bonding_curve_key):
        self.calls.append((mint, bonding_curve_key))
        return EarlyBuyerSnapshot(mint=mint, buyers=("SMART1", "W2"),
                                  signatures_scanned=2, transactions_scanned=2)


def rpc_transport():
    def handler(request):
        body = json.loads(request.content)
        method = body["method"]
        if method == "getAccountInfo":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": {"data": {
                "parsed": {"info": {"mintAuthority": None, "freezeAuthority": None,
                                    "supply": "1000", "decimals": 6}}}}}})
        if method == "getTokenLargestAccounts":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": [
                {"address": "SMART1_ATA", "amount": "100", "decimals": 6},
                {"address": "W2_ATA", "amount": "50", "decimals": 6},
                {"address": "OTHER_ATA", "amount": "850", "decimals": 6},
            ]}})
        if method == "getMultipleAccounts":
            owners = {"SMART1_ATA": "SMART1", "W2_ATA": "W2", "OTHER_ATA": "CURVE"}
            addrs = body["params"][0]
            value = [{"data": {"parsed": {"info": {"owner": owners[a]}}}} for a in addrs]
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": value}})
        raise AssertionError(method)
    return httpx.MockTransport(handler)


async def test_e2e_p2_strategy_scores_smart_money_from_preseeded_early_buyers(tmp_path):
    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 0.0)
    row = None
    upsert_token_identity(
        conn,
        mint="M",
        raw_ingested_at=0.0,
        bonding_curve_key="CURVE",
        fields={"name": "Fixture", "symbol": "FX", "creator": "DEV"},
    )
    set_token_state(conn, "M", "CLIMBING")
    record_wallet_pnl_event(conn, at=50.0, wallet="SMART1", mint="PRIOR",
                            realized_pnl_sol=2.0, source="test", detail={})
    record_early_buyer_read(
        conn,
        mint="M",
        checked_at=60.0,
        buyers=("SMART1", "W2"),
        unavailable_reason="",
        inputs_hash="b8501d647a40b05fb628092f29b2bf2e49c10575aec0cda8ee8c0c10f0bf7f0c",
    )

    bus = EventBus()
    fe = FeatureEngine(bus, max_feature_mints=DEFAULT_MAX_MINTS)
    reader = FakeEarlyBuyerReader()
    def ext_handler(request):
        if "token_security" in str(request.url):
            return httpx.Response(200, json={"result": {"M": {"mintable": {"status": "0"},
                                                                "freezable": {"status": "0"},
                                                                "transfer_hook": []}}})
        return httpx.Response(200, json={"score_normalised": 0, "risks": []})

    probes = LiveProbes(
        rpc_url="https://rpc.test",
        rpc_client=httpx.AsyncClient(transport=rpc_transport()),
        ext_client=httpx.AsyncClient(transport=httpx.MockTransport(ext_handler)),
        jup_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"outAmount": "1000000000", "priceImpactPct": "0"}))),
        conn=conn,
        cfg={"top10_holder_max_pct": 90.0, "dev_wallet_max_pct": 10.0,
             "honeypot_max_impact_pct": 30.0, "rugcheck_base": "https://rc",
             "goplus_base": "https://gp", "jupiter_base": "https://jup",
             "early_buyers": {"enabled": True, "signature_limit": 25, "buyer_limit": 20,
                                "max_supply_pct": 25.0}},
            governors={"rugcheck": Governor(per_minute=600, sleep=lambda s: asyncio.sleep(0)),
                       "goplus": Governor(per_minute=600, sleep=lambda s: asyncio.sleep(0)),
                       "jupiter": Governor(per_minute=600, sleep=lambda s: asyncio.sleep(0))},
            early_buyer_reader=reader, clock=lambda: 98.0)
    gate = SafetyGate(conn, probes=probes, clock=lambda: 99.0)
    gate_runner = GateRunner(bus, conn, gate)
    canonical_resolver = _P2CanonicalResolver(conn)
    strat = ClimbingStrategy(bus, conn, feature_engine=fe, scorer=ConfluenceScorer(SCORER_CFG),
                             broker=PaperBroker(FILL_CFG, PUMP_CFG),
                             canonical_resolver=canonical_resolver, strat_cfg=STRAT_CFG,
                             pumpfun_cfg=PUMP_CFG, config_hash=_P2_CONFIG_HASH, fill_cfg=FILL_CFG,
                             exits_cfg={"ladder_multiples": [2.0], "ladder_fractions": [0.5],
                                        "time_stop_s": 999999.0, "trailing_stop_pct": 99.0},
                             clock=lambda: 100.0, smart_money_cfg=SMART_CFG)
    assert type(strat) is globals()["ClimbingStrategy"]
    assert strat._canonical_resolver is canonical_resolver
    stop = asyncio.Event()
    tasks = [asyncio.create_task(c.run(stop)) for c in (fe, gate_runner, strat)]
    await asyncio.sleep(0.05)   # let consumers subscribe before publishing the replay frames

    await bus.publish(_cp("M", t=0.0, real_sol_lamports=1_000_000_000))
    await bus.publish(_cp("M", t=10.0, real_sol_lamports=6_000_000_000))
    await asyncio.sleep(0.1)
    await bus.publish(LifecycleTransition(t_wall=11.0, t_mono=11.0, mint="M",
                                          from_state="FRESH", to_state="CLIMBING"))
    for _ in range(30):
        row = conn.execute("SELECT feature_vector_json, score FROM decisions WHERE mint='M'").fetchone()
        if row is not None:
            break
        await asyncio.sleep(0.1)
    stop.set()
    await asyncio.gather(*tasks)

    assert reader.calls == [("M", "CURVE")]
    assert row is not None
    vector = json.loads(row["feature_vector_json"])
    assert vector["smart_money_count"] == 1
    assert vector["smart_money_pnl_sol"] == 2.0
    baseline = ConfluenceScorer(SCORER_CFG).score(ClimbingFeatures(
        velocity_sol_per_s=vector["velocity_sol_per_s"],
        curve_progress_pct=vector["curve_progress_pct"],
        age_s=vector["age_s"],
        risk_score=vector["risk_score"],
        spot_price_sol=vector["spot_price_sol"],
        samples=vector["samples"],
    ))
    assert row["score"] > baseline.value

    latest_report = conn.execute(
        "SELECT id,checked_at FROM safety_reports WHERE mint='M' "
        "ORDER BY checked_at DESC,id DESC LIMIT 1",
    ).fetchone()
    assert latest_report is not None
    actual_decision_at = math.nextafter(latest_report["checked_at"], math.inf)
    actual_snapshot = fe.snapshot_at_or_before("M", as_of=actual_decision_at)
    assert actual_snapshot is not None
    changes_before_resolve = conn.total_changes
    actual_resolution = canonical_resolver.resolve(
        "M",
        decision_at=actual_decision_at,
        target_report_id=latest_report["id"],
        target_snapshot=actual_snapshot,
    )
    assert conn.total_changes == changes_before_resolve
    _assert_p2_resolution_provenance(
        conn,
        actual_resolution,
        mint="M",
        decision_at=actual_decision_at,
        target_report_id=latest_report["id"],
        target_snapshot=actual_snapshot,
    )
