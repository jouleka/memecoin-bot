import hashlib
import json
from dataclasses import asdict, replace

from memebot.canonical import CanonicalResolver
from memebot.canonical_analysis import compute_canonical_metrics
from memebot.counterfactual import ForwardReturnTracker
from memebot.events import CanonicalObservationStarted, CurveProgress
from memebot.features import CurveSnapshot
from memebot.store import (
    allocate_p3_causal_wall,
    open_db,
    p3_immediate_transaction,
    reconcile_unmatched_p3_buys,
    record_canonical_recheck,
    record_decision_with_canonical_observations,
    set_token_state,
    upsert_token_identity,
)


class _EmptyJournal:
    def iter_events(self, *, since_wall, until_wall):
        del since_wall, until_wall
        return iter(())


class _Snapshots:
    def __init__(self):
        self.by_mint = {}

    def snapshot_at_or_before(self, mint, *, as_of):
        snapshot = self.by_mint.get(mint)
        return snapshot if snapshot is not None and snapshot.t_wall <= as_of else None

    def p3_snapshot_at_or_before(self, mint, *, as_of, **_durable):
        return self.snapshot_at_or_before(mint, as_of=as_of)


class _P3Fixture:
    horizon = 60.0
    config_hash = "c" * 64

    def __init__(self):
        self.conn = open_db(":memory:", migration_clock=lambda: 1.0)
        self.snapshots = _Snapshots()
        self.report_ids = {}
        self.resolver = CanonicalResolver(
            self.conn,
            feature_engine=self.snapshots,
            canonical_cfg={
                "enabled": True,
                "resolver_version": "canonical-v1",
                "weights_version": "canonical-weighted-v1",
                "live_states": ["FRESH", "CLIMBING"],
                "max_cluster_candidates": 10,
                "max_creator_history_mints": 10,
                "max_feature_mints": 10,
                "max_open_p3_positions": 10,
                "liquidity_max_age_s": 300.0,
                "holder_max_age_s": 900.0,
                "comparison_price_max_age_s": 300.0,
                "fill_event_max_age_s": 30.0,
                "reconcile_interval_s": 60.0,
                "w_first_mover": 0.35,
                "w_liquidity": 0.25,
                "w_holder": 0.20,
                "w_creator": 0.15,
                "w_social": 0.05,
                "social_weights": {
                    "uri": 0.25,
                    "website": 0.25,
                    "twitter": 0.25,
                    "telegram": 0.25,
                },
            },
            safety_cfg={
                "top10_holder_max_pct": 30.0,
                "early_buyers": {"buyer_limit": 10},
            },
            pumpfun_cfg={"graduation_sol": 85.0, "token_decimals": 6},
            config_hash=self.config_hash,
            counterfactual_horizons=(self.horizon,),
            runtime_boot_id=7,
            runtime_causal_floor=1.0,
        )

    @staticmethod
    def _price(virtual_sol_reserves, virtual_token_reserves=900_000_000_000_000):
        return (virtual_sol_reserves / 1_000_000_000) / (
            virtual_token_reserves / 1_000_000
        )

    def add_token(
        self,
        mint,
        *,
        ingested_at,
        name="Pepe",
        symbol="PEPE",
        real_sol=40.0,
        virtual_sol=40_000_000_000,
    ):
        upsert_token_identity(
            self.conn,
            mint=mint,
            raw_ingested_at=ingested_at,
            bonding_curve_key=f"curve-{mint}",
            fields={
                "creator": f"creator-{mint}",
                "name": name,
                "symbol": symbol,
                "uri": "ipfs://shared",
                "website": "",
                "twitter": "@shared",
                "telegram": "",
            },
        )
        set_token_state(
            self.conn,
            mint,
            "CLIMBING",
            progress_pct=100.0 * real_sol / 85.0,
            last_seen=20.0,
        )
        report_id = self.conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES (?,20.0,'[]',5.0,?)",
            (mint, hashlib.sha256(f"safety:{mint}".encode()).hexdigest()),
        ).lastrowid
        self.conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,"
            "unavailable_reason,inputs_hash) VALUES (?,20,12,20.0,19.0,'',?)",
            (report_id, hashlib.sha256(f"holders:{mint}".encode()).hexdigest()),
        )
        self.conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id) VALUES (?,19.5,?,'',?,?)",
            (
                mint,
                json.dumps([f"buyer-{mint}"], separators=(",", ":")),
                hashlib.sha256(f"buyers:{mint}".encode()).hexdigest(),
                report_id,
            ),
        )
        self.conn.commit()
        self.report_ids[mint] = report_id
        self.snapshots.by_mint[mint] = CurveSnapshot(
            source_boot_id=7,
            source_seq=len(self.snapshots.by_mint) + 1,
            t_wall=30.0,
            t_mono=30.0,
            virtual_sol_reserves=virtual_sol,
            virtual_token_reserves=900_000_000_000_000,
            real_sol_reserves=int(real_sol * 1_000_000_000),
            real_token_reserves=400_000_000_000_000,
            liquidity_sol=real_sol,
            spot_price_sol=self._price(virtual_sol),
            progress_pct=100.0 * real_sol / 85.0,
        )

    def record_resolution(self, mint, *, raw_at, action):
        with p3_immediate_transaction(self.conn):
            at = allocate_p3_causal_wall(self.conn, raw_wall=raw_at)
            resolution = self.resolver.resolve(
                mint,
                decision_at=at,
                target_report_id=self.report_ids[mint],
            )
            ranked = sorted(
                (
                    candidate
                    for candidate in resolution.verdict.ranking_inputs["candidates"]
                    if candidate["eligible"] is True
                ),
                key=lambda candidate: candidate["rank"],
            )
            canonical = asdict(resolution.verdict)
            canonical.update(
                {
                    "config_hash": self.config_hash,
                    "ranking_order": [candidate["mint"] for candidate in ranked],
                }
            )
            decision_id, observation_ids, primary = (
                record_decision_with_canonical_observations(
                    self.conn,
                    at=at,
                    mint=mint,
                    segment="CLIMBING",
                    action=action,
                    score=80.0,
                    feature_vector={"canonical": canonical},
                    safety_report_id=self.report_ids[mint],
                    config_hash=self.config_hash,
                    generation_hash=resolution.verdict.generation_hash,
                    observations=resolution.observations,
                    score_status="VALID",
                    score_weights_version="climbing-v1",
                    score_unavailable_reason="",
                    planned_position_size_sol=0.1,
                )
            )
        return at, decision_id, observation_ids, primary, resolution

    def record_pass_recheck(self, *, decision_id, resolution, raw_at):
        prior_hash = resolution.verdict.inputs_hash
        target = replace(
            self.snapshots.by_mint[resolution.verdict.canonical_mint],
            t_wall=raw_at - 1.0,
            t_mono=raw_at - 1.0,
        )
        with p3_immediate_transaction(self.conn):
            at = allocate_p3_causal_wall(self.conn, raw_wall=raw_at)
            rechecked = self.resolver.resolve(
                resolution.verdict.canonical_mint,
                decision_at=at,
                target_report_id=self.report_ids[resolution.verdict.canonical_mint],
                target_snapshot=target,
            )
            payload = {
                "decision_id": decision_id,
                "attempt": 1,
                "trigger": "curve_progress",
                "trigger_report_id": None,
                "rechecked_at": at,
                "fill_event_at": target.t_wall,
                "causal_target_report_id": self.report_ids[
                    resolution.verdict.canonical_mint
                ],
                "latest_target_report_id": rechecked.verdict.ranking_inputs[
                    "latest_target_report_id"
                ],
                "prior_inputs_hash": prior_hash,
                "target_snapshot": {
                    "t_wall": target.t_wall,
                    "t_mono": target.t_mono,
                    "virtual_sol_reserves": target.virtual_sol_reserves,
                    "virtual_token_reserves": target.virtual_token_reserves,
                    "real_sol_reserves": target.real_sol_reserves,
                    "real_token_reserves": target.real_token_reserves,
                    "liquidity_sol": target.liquidity_sol,
                    "spot_price_sol": target.spot_price_sol,
                    "progress_pct": target.progress_pct,
                },
                "verdict": {
                    "status": rechecked.verdict.status,
                    "reason": rechecked.verdict.reason,
                    "canonical_mint": rechecked.verdict.canonical_mint,
                    "inputs_hash": rechecked.verdict.inputs_hash,
                },
            }
            encoded = json.dumps(
                payload, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            recheck_id = record_canonical_recheck(
                self.conn,
                decision_id=decision_id,
                attempt=1,
                rechecked_at=at,
                causal_target_report_id=self.report_ids[
                    resolution.verdict.canonical_mint
                ],
                latest_target_report_id=self.report_ids[
                    resolution.verdict.canonical_mint
                ],
                status="PASS",
                reason="canonical_selected",
                canonical_mint=resolution.verdict.canonical_mint,
                prior_inputs_hash=prior_hash,
                recheck_inputs_hash=hashlib.sha256(encoded.encode()).hexdigest(),
                payload=payload,
            )
        return recheck_id, rechecked


def _curve_event(mint, *, at, virtual_sol):
    return CurveProgress(
        t_wall=at,
        t_mono=at,
        mint=mint,
        progress_pct=50.0,
        virtual_sol_reserves=virtual_sol,
        virtual_token_reserves=900_000_000_000_000,
        real_sol_reserves=40_000_000_000,
        real_token_reserves=400_000_000_000_000,
    )


def test_p3_clone_suppression_recheck_and_paired_returns():
    fixture = _P3Fixture()
    fixture.add_token(
        "CANON", ingested_at=10.0, real_sol=60.0, virtual_sol=60_000_000_000
    )
    fixture.add_token(
        "CLONE", ingested_at=11.0, real_sol=20.0, virtual_sol=30_000_000_000
    )

    at, decision_id, observation_ids, primary, selected = fixture.record_resolution(
        "CANON", raw_at=100.0, action="BUY"
    )
    _, clone_decision_id, _, clone_primary, suppressed = fixture.record_resolution(
        "CLONE", raw_at=101.0, action="SKIP"
    )
    assert (selected.verdict.status, selected.verdict.canonical_mint) == (
        "CANONICAL",
        "CANON",
    )
    assert (suppressed.verdict.status, suppressed.verdict.canonical_mint) == (
        "SUPPRESSED",
        "CANON",
    )
    assert selected.verdict.generation_hash == suppressed.verdict.generation_hash
    assert primary is True and clone_primary is False

    recheck_id, rechecked = fixture.record_pass_recheck(
        decision_id=decision_id,
        resolution=selected,
        raw_at=110.0,
    )
    assert rechecked.verdict.status == "CANONICAL"
    assert tuple(
        fixture.conn.execute(
            "SELECT status,canonical_mint FROM canonical_rechecks WHERE id=?",
            (recheck_id,),
        ).fetchone()
    ) == ("PASS", "CANON")
    assert fixture.conn.execute(
        "SELECT action FROM decisions WHERE id=?", (clone_decision_id,)
    ).fetchone()[0] == "SKIP"
    assert fixture.conn.execute(
        "SELECT count(*) FROM canonical_rechecks WHERE decision_id=?",
        (clone_decision_id,),
    ).fetchone()[0] == 0

    tracker = ForwardReturnTracker(
        None,
        fixture.conn,
        journal=_EmptyJournal(),
        horizons=(fixture.horizon,),
        token_decimals=6,
        stale_price_after_s=300.0,
        reconcile_interval_s=60.0,
        price_history_retention_s=1_000.0,
        price_history_max_samples_per_mint=20,
        price_history_max_mints=10,
        max_in_memory_pending_observations=20,
        clock=lambda: at + fixture.horizon,
    )
    observation_rows = fixture.conn.execute(
        "SELECT id,mint,start_price_sol,price_observed_at "
        "FROM canonical_observations WHERE decision_id=? ORDER BY id",
        (decision_id,),
    ).fetchall()
    assert tuple(row["id"] for row in observation_rows) == observation_ids
    for row in observation_rows:
        tracker.register(
            CanonicalObservationStarted(
                t_wall=at,
                t_mono=at,
                observation_id=row["id"],
                decision_id=decision_id,
                mint=row["mint"],
                start_price_sol=row["start_price_sol"],
                price_observed_at=row["price_observed_at"],
            )
        )
    tracker.observe_price(
        _curve_event("CANON", at=at + fixture.horizon, virtual_sol=90_000_000_000)
    )
    tracker.observe_price(
        _curve_event("CLONE", at=at + fixture.horizon, virtual_sol=27_000_000_000)
    )
    assert tracker.check(now=at + fixture.horizon) == 2

    metrics = compute_canonical_metrics(fixture.conn, horizon_s=fixture.horizon)
    assert (metrics.potential_pairs, metrics.comparable_pairs) == (1, 1)
    assert (metrics.pair_wins, metrics.cluster_wins, metrics.harm_clusters) == (
        1,
        1,
        0,
    )
    assert metrics.pair_coverage == metrics.cluster_coverage == 1.0


def test_p3_unresolved_restart_and_generation_dedup():
    fixture = _P3Fixture()
    fixture.add_token("UNRESOLVED", ingested_at=10.0, symbol="")
    _, unresolved_id, unresolved_observations, unresolved_primary, unresolved = (
        fixture.record_resolution("UNRESOLVED", raw_at=100.0, action="SKIP")
    )
    assert (unresolved.verdict.status, unresolved.verdict.reason) == (
        "UNRESOLVED",
        "canonical_identity_unavailable",
    )
    assert unresolved.verdict.generation_hash is None
    assert unresolved_primary is False and len(unresolved_observations) == 1

    fixture.add_token("RESTART", ingested_at=20.0, name="Doge", symbol="DOGE")
    _, buy_id, _, first_primary, first = fixture.record_resolution(
        "RESTART", raw_at=110.0, action="BUY"
    )
    _, repeat_id, _, repeat_primary, repeated = fixture.record_resolution(
        "RESTART", raw_at=120.0, action="SKIP"
    )
    assert first.verdict.generation_hash == repeated.verdict.generation_hash
    assert first_primary is True and repeat_primary is False
    assert fixture.conn.execute(
        "SELECT first_decision_id FROM canonical_generations WHERE generation_hash=?",
        (first.verdict.generation_hash,),
    ).fetchone()[0] == buy_id
    assert fixture.conn.execute(
        "SELECT count(*) FROM canonical_generations WHERE generation_hash=?",
        (first.verdict.generation_hash,),
    ).fetchone()[0] == 1

    assert reconcile_unmatched_p3_buys(fixture.conn, raw_wall=130.0) == 1
    terminal = tuple(
        fixture.conn.execute(
            "SELECT status,reason,canonical_recheck_id,paper_trade_id "
            "FROM paper_entry_executions WHERE decision_id=?",
            (buy_id,),
        ).fetchone()
    )
    assert terminal == ("ABANDONED", "restart_before_fill", None, None)
    assert reconcile_unmatched_p3_buys(fixture.conn, raw_wall=140.0) == 0

    metrics = compute_canonical_metrics(fixture.conn, horizon_s=fixture.horizon)
    assert metrics.all_p3_decisions == 3
    assert metrics.primary_decisions == 1
    assert metrics.unresolved_identity == 1
    assert metrics.canonical_buy_decisions == metrics.abandoned_entries == 1
    assert metrics.terminal_coverage == metrics.abandonment_rate == 1.0
    assert {
        row["id"] for row in fixture.conn.execute("SELECT id FROM decisions")
    } == {unresolved_id, buy_id, repeat_id}
