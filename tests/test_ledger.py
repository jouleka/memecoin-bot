import sqlite3

import pytest

from memebot.store import (open_db, open_positions_from_ledger, record_decision,
                           record_outcome, record_paper_trade)


def test_record_decision_roundtrips_feature_vector_and_returns_id(tmp_path):
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M1", segment="CLIMBING", action="BUY",
                          score=72.0, feature_vector={"velocity_sol_per_s": 0.03},
                          config_hash="cfg", safety_report_id=None)
    assert isinstance(did, int) and did > 0
    row = conn.execute("SELECT * FROM decisions WHERE id=?", (did,)).fetchone()
    assert row["action"] == "BUY" and row["score"] == 72.0
    assert '"velocity_sol_per_s"' in row["feature_vector_json"]


def test_record_paper_trade_and_outcome(tmp_path):
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M1", segment="CLIMBING", action="BUY",
                          score=72.0, feature_vector={}, config_hash="cfg")
    tid = record_paper_trade(conn, decision_id=did, at=2.0, mint="M1", segment="CLIMBING",
                             side="buy", qty=1000.0, quote_price=1e-6, fill_price=1.1e-6,
                             fees={"protocol_sol": 0.001}, realism_grade="B")
    assert isinstance(tid, int) and tid > 0
    oid = record_outcome(conn, at=3.0, ref_kind="trade", ref_id=did, pnl_sol=0.5,
                         detail={"hold_s": 120})
    assert isinstance(oid, int) and oid > 0
    assert conn.execute("SELECT pnl_sol FROM outcomes WHERE id=?", (oid,)).fetchone()[0] == 0.5


def test_ledger_writes_are_append_only(tmp_path):
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M1", segment="CLIMBING", action="SKIP",
                          score=10.0, feature_vector={}, config_hash="cfg")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute("UPDATE decisions SET score=1 WHERE id=?", (did,))


def test_open_positions_from_ledger_nets_buys_minus_sells(tmp_path):
    conn = open_db(tmp_path / "t.db")
    did = record_decision(conn, at=1.0, mint="M1", segment="CLIMBING", action="BUY",
                          score=80.0, feature_vector={}, config_hash="cfg")
    record_paper_trade(conn, decision_id=did, at=2.0, mint="M1", segment="CLIMBING",
                       side="buy", qty=1000.0, quote_price=1e-6, fill_price=1e-6,
                       fees={}, realism_grade="B")
    record_paper_trade(conn, decision_id=did, at=3.0, mint="M1", segment="CLIMBING",
                       side="sell", qty=400.0, quote_price=2e-6, fill_price=2e-6,
                       fees={}, realism_grade="B")
    # a fully-closed token must NOT appear
    did2 = record_decision(conn, at=1.0, mint="M2", segment="CLIMBING", action="BUY",
                           score=80.0, feature_vector={}, config_hash="cfg")
    record_paper_trade(conn, decision_id=did2, at=2.0, mint="M2", segment="CLIMBING",
                       side="buy", qty=500.0, quote_price=1e-6, fill_price=1e-6,
                       fees={}, realism_grade="B")
    record_paper_trade(conn, decision_id=did2, at=3.0, mint="M2", segment="CLIMBING",
                       side="sell", qty=500.0, quote_price=2e-6, fill_price=2e-6,
                       fees={}, realism_grade="B")
    open_pos = open_positions_from_ledger(conn)
    assert len(open_pos) == 1
    assert open_pos[0]["mint"] == "M1"
    assert open_pos[0]["qty_remaining"] == pytest.approx(600.0)
    assert open_pos[0]["decision_id"] == did


# N1 (final-review, latent ledger-corruption trap): open_positions_from_ledger used to
# aggregate by MINT, netting qty_remaining across ALL of a mint's entry cycles while
# taking decision_id/entry_price from the FIRST buy only. A mint with a fully-closed
# earlier decision (A) and a still-open later decision (B) at restart would return ONE
# row mixing A's decision_id/entry_price with B's net qty_remaining -- every subsequent
# exit then writes under decision A, silently corrupting _realized_pnl. Not reachable in
# shipped P1 (nothing re-enters a token) but live in P4 (re-enters graduated tokens), so
# the trap is closed now: aggregate by decision_id, not mint.
def test_open_positions_grouped_by_decision_not_mint(tmp_path):
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
    open_pos = open_positions_from_ledger(conn)
    assert len(open_pos) == 1                                  # exactly one row, not zero/two
    row = open_pos[0]
    assert row["decision_id"] == did_b                          # B's id, never A's
    assert row["qty_remaining"] == pytest.approx(300.0)         # B's net only, not mixed with A
    assert row["mint"] == "M"
    assert row["entry_price"] == pytest.approx(5e-6)            # B's buy fill_price, NOT A's (1e-6)


def test_canonical_payload_roundtrip_and_hash(tmp_path):
    import hashlib
    import json
    from copy import deepcopy

    from memebot.canonical import (
        CanonicalObservationDraft,
        canonical_generation_hash,
    )
    from memebot.store import (
        allocate_p3_causal_wall,
        p3_immediate_transaction,
        record_decision_with_canonical_observations,
        save_safety_report,
    )

    conn = open_db(tmp_path / "canonical-payload.db", migration_clock=lambda: 1.0)
    for mint in ("mintA", "mintB"):
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
            "VALUES (?,0.5,'CLIMBING',0.5,'{}')",
            (mint,),
        )
    conn.commit()

    report_ids = {}
    holder_ids = {}
    safety_times = {}
    for index, mint in enumerate(("mintA", "mintB"), start=2):
        report_ids[mint] = save_safety_report(
            conn,
            mint=mint,
            raw_completed_at=float(index),
            segment="CLIMBING",
            hard_fails=(),
            risk_score=10.0 if mint == "mintA" else 12.0,
            results_json="[]",
            inputs_hash="2" * 64,
        )
        safety_times[mint] = conn.execute(
            "SELECT checked_at FROM safety_reports WHERE id=?",
            (report_ids[mint],),
        ).fetchone()[0]
        holder_ids[mint] = conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,"
            "unavailable_reason,inputs_hash) VALUES (?,?,?,?,1.5,'',?)",
            (
                report_ids[mint],
                20,
                15 if mint == "mintA" else 10,
                20.0 if mint == "mintA" else 15.0,
                ("4" if mint == "mintA" else "5") * 64,
            ),
        ).lastrowid
        conn.commit()

    def social(value):
        return {
            "value": value,
            "present": value is not None,
            "reuse": value is not None,
            "cluster_conflict": False,
            "metadata_conflict": False,
        }

    def candidate(
        *, mint, identity_at, liquidity_sol, progress_pct, owners,
        top10_share, creator, successes, event_ids, components, points, rank,
    ):
        return {
            "mint": mint,
            "p3_identity_ingested_at": identity_at,
            "state": "CLIMBING",
            "rugged": 0,
            "normalized_name": "pepe",
            "normalized_symbol": "pepe",
            "creator": creator,
            "identity_observed_at": {"name": identity_at, "symbol": identity_at},
            "identity_conflicts": [],
            "eligible": True,
            "ineligible_reason": "",
            "safety_report_id": report_ids[mint],
            "safety_checked_at": safety_times[mint],
            "safety_inputs_hash": "2" * 64,
            "safety_hard_fails": [],
            "safety_risk_score": 10.0 if mint == "mintA" else 12.0,
            "holder_evidence_id": holder_ids[mint],
            "holder_inputs_hash": ("4" if mint == "mintA" else "5") * 64,
            "holder_observed_at": 1.5,
            "liquidity_source": "curve_snapshot",
            "liquidity_observed_at": 9.0,
            "raw": {
                "liquidity_sol": liquidity_sol,
                "curve_progress_pct": progress_pct,
                "curve_snapshot": {
                    "t_wall": 9.0,
                    "t_mono": 8.0,
                    "virtual_sol_reserves": 70_000_000_000,
                    "virtual_token_reserves": 70_000_000_000_000,
                    "real_sol_reserves": int(liquidity_sol * 1_000_000_000),
                    "real_token_reserves": 400_000_000_000_000,
                    "spot_price_sol": 0.000001,
                },
                "sampled_token_accounts": 20,
                "distinct_non_curve_owners": owners,
                "top10_non_curve_owner_share_pct": top10_share,
                "creator_prior_successes": successes,
                "creator_prior_rugs": 0,
                "creator_reputation_event_ids": event_ids,
                "social": {
                    "uri": social("ipfs://x"),
                    "website": social(None),
                    "twitter": social(None),
                    "telegram": social(None),
                },
            },
            "components_ppm": components,
            "rank_points": points,
            "rank": rank,
        }

    config_hash = "a" * 64
    candidates = [
        candidate(
            mint="mintA",
            identity_at=0.5,
            liquidity_sol=42.5,
            progress_pct=50.0,
            owners=15,
            top10_share=20.0,
            creator="creatorA",
            successes=1,
            event_ids=[31],
            components={
                "first_mover": 1_000_000,
                "liquidity": 500_000,
                "holder": 541_667,
                "creator": 666_667,
                "social": 250_000,
            },
            points=6_958_334_500,
            rank=1,
        ),
        candidate(
            mint="mintB",
            identity_at=0.6,
            liquidity_sol=34.0,
            progress_pct=40.0,
            owners=10,
            top10_share=15.0,
            creator="creatorB",
            successes=0,
            event_ids=[],
            components={
                "first_mover": 0,
                "liquidity": 400_000,
                "holder": 500_000,
                "creator": 500_000,
                "social": 250_000,
            },
            points=2_875_000_000,
            rank=2,
        ),
    ]
    generation_hash = canonical_generation_hash(
        cluster_key="pepe:pepe",
        eligible=tuple(
            {
                "mint": item["mint"],
                "safety_report_id": item["safety_report_id"],
                "holder_evidence_id": item["holder_evidence_id"],
            }
            for item in candidates
        ),
        canonical_mint="mintA",
        resolver_version="canonical-v1",
        weights_version="canonical-weighted-v1",
        config_hash=config_hash,
    )

    with p3_immediate_transaction(conn):
        decision_at = allocate_p3_causal_wall(conn, raw_wall=10.0)
        ranking_inputs = {
            "subject_mint": "mintA",
            "target_report_id": report_ids["mintA"],
            "latest_target_report_id": report_ids["mintA"],
            "resolved_at": decision_at,
            "cluster_key": "pepe:pepe",
            "resolver_version": "canonical-v1",
            "weights_version": "canonical-weighted-v1",
            "config_hash": config_hash,
            "counterfactual_horizons_s": [3600.0, 21600.0, 86400.0],
            "limits": {
                "max_cluster_candidates": 50,
                "liquidity_max_age_s": 30.0,
                "holder_max_age_s": 900.0,
                "comparison_price_max_age_s": 300.0,
            },
            "component_parameters": {
                "graduation_sol": 85.0,
                "holder_owner_target": 20,
                "top10_holder_max_pct": 30.0,
                "token_decimals": 6,
                "creator_reputation_as_of": decision_at,
            },
            "weights_bps": {
                "first_mover": 3500,
                "liquidity": 2500,
                "holder": 2000,
                "creator": 1500,
                "social": 500,
            },
            "social_weights_bps": {
                "uri": 2500,
                "website": 2500,
                "twitter": 2500,
                "telegram": 2500,
            },
            "candidates": candidates,
        }
        ranking_json = json.dumps(
            ranking_inputs,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        inputs_hash = hashlib.sha256(ranking_json.encode("utf-8")).hexdigest()
        feature_vector = {
            "velocity_sol_per_s": 0.03,
            "ordinary_diagnostics": {"sample_count": 4},
            "canonical": {
                "resolver_version": "canonical-v1",
                "weights_version": "canonical-weighted-v1",
                "status": "CANONICAL",
                "reason": "canonical_selected",
                "resolved_at": decision_at,
                "cluster_key": "pepe:pepe",
                "cluster_size": 2,
                "eligible_cluster_size": 2,
                "canonical_mint": "mintA",
                "rank": 1,
                "rank_points": 6_958_334_500,
                "generation_hash": generation_hash,
                "inputs_hash": inputs_hash,
                "config_hash": config_hash,
                "ranking_order": ["mintA", "mintB"],
                "ranking_inputs": ranking_inputs,
            },
        }
        decision_id, observation_ids, analysis_primary = (
            record_decision_with_canonical_observations(
                conn,
                at=decision_at,
                mint="mintA",
                segment="CLIMBING",
                action="BUY",
                score=90.0,
                feature_vector=feature_vector,
                safety_report_id=report_ids["mintA"],
                config_hash=config_hash,
                generation_hash=generation_hash,
                observations=(
                    CanonicalObservationDraft(
                        mint="mintA",
                        is_subject=True,
                        is_canonical=True,
                        eligible=True,
                        start_price_sol=0.000001,
                        price_observed_at=9.0,
                        unavailable_reason="",
                    ),
                    CanonicalObservationDraft(
                        mint="mintB",
                        is_subject=False,
                        is_canonical=False,
                        eligible=True,
                        start_price_sol=0.000001,
                        price_observed_at=9.0,
                        unavailable_reason="",
                    ),
                ),
                score_status="VALID",
                score_weights_version="climbing-v1",
                score_unavailable_reason="",
                planned_position_size_sol=0.1,
            )
        )

    expected = deepcopy(feature_vector)
    expected.update(
        {
            "score_status": "VALID",
            "score_weights_version": "climbing-v1",
            "score_unavailable_reason": "",
        }
    )
    expected["canonical"]["planned_size_sol"] = 0.1
    expected_json = json.dumps(
        expected, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    stored_json = conn.execute(
        "SELECT feature_vector_json FROM decisions WHERE id=?", (decision_id,)
    ).fetchone()[0]
    assert stored_json == expected_json
    stored = json.loads(stored_json)
    assert stored == expected
    assert stored["velocity_sol_per_s"] == 0.03
    assert stored["ordinary_diagnostics"] == {"sample_count": 4}

    canonical = stored["canonical"]
    assert canonical["ranking_inputs"] == ranking_inputs
    assert canonical["ranking_inputs"]["counterfactual_horizons_s"] == [
        3600.0,
        21600.0,
        86400.0,
    ]
    assert [item["mint"] for item in canonical["ranking_inputs"]["candidates"]] == [
        "mintA",
        "mintB",
    ]
    assert canonical["cluster_size"] == len(
        canonical["ranking_inputs"]["candidates"]
    )
    assert canonical["ranking_order"] == ["mintA", "mintB"]
    assert canonical["ranking_order"] == [
        item["mint"]
        for item in sorted(
            canonical["ranking_inputs"]["candidates"],
            key=lambda item: item["rank"],
        )
        if item["eligible"]
    ]
    assert canonical["inputs_hash"] == hashlib.sha256(
        json.dumps(
            canonical["ranking_inputs"],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert canonical["generation_hash"] == generation_hash
    assert canonical["generation_hash"] != canonical["inputs_hash"]

    omitted_member = deepcopy(canonical["ranking_inputs"])
    omitted_member["candidates"].pop()
    omitted_horizons = deepcopy(canonical["ranking_inputs"])
    omitted_horizons.pop("counterfactual_horizons_s")
    for mutated_inputs in (omitted_member, omitted_horizons):
        mutated_json = json.dumps(
            mutated_inputs,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        assert hashlib.sha256(mutated_json.encode("utf-8")).hexdigest() != canonical[
            "inputs_hash"
        ]

    assert len(observation_ids) == 2
    assert analysis_primary is True
    conn.close()
