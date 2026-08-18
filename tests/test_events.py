from memebot.events import (
    EVENT_TYPES,
    AdapterHealth,
    TokenCreated,
    event_from_dict,
    event_to_dict,
)


def test_round_trip():
    e = TokenCreated(t_wall=1.0, t_mono=2.0, mint="M", name="N", symbol="S",
                     creator="C", raw={"x": 1})
    d = event_to_dict(e)
    assert d["kind"] == "token_created"
    assert event_from_dict(d) == e


def test_registry_covers_all_kinds():
    assert set(EVENT_TYPES) == {
        "token_created", "curve_trade", "curve_progress", "token_graduated",
        "pair_update", "holder_snapshot", "market_regime", "adapter_health",
        "lifecycle_transition", "safety_hard_fail",
        "safety_passed", "candidate_scored", "canonical_observation_started",
        "paper_entry", "paper_exit",
    }


def test_adapter_health_round_trip():
    e = AdapterHealth(t_wall=1.0, t_mono=2.0, adapter="pumpportal",
                      status="stale", detail="no frame 30s")
    assert event_from_dict(event_to_dict(e)) == e


def test_lifecycle_transition_round_trip():
    from memebot.events import LifecycleTransition
    e = LifecycleTransition(t_wall=1.0, t_mono=2.0, mint="M",
                            from_state="FRESH", to_state="CLIMBING")
    assert event_from_dict(event_to_dict(e)) == e


def test_safety_hard_fail_round_trip():
    from memebot.events import SafetyHardFail
    e = SafetyHardFail(t_wall=1.0, t_mono=2.0, mint="M", reasons=("mint_authority_active",))
    assert event_from_dict(event_to_dict(e)) == e


def test_curveprogress_carries_reserves_and_roundtrips_without_them():
    from memebot.events import CurveProgress, event_from_dict, event_to_dict
    ev = CurveProgress(t_wall=1.0, t_mono=1.0, mint="M", progress_pct=12.0,
                       virtual_sol_reserves=31_000_000_000, virtual_token_reserves=900_000_000_000_000,
                       real_sol_reserves=1_000_000_000, real_token_reserves=700_000_000_000_000)
    assert event_from_dict(event_to_dict(ev)) == ev
    # an OLD journal row (pre-P1) has no reserve keys -> must still decode with defaults
    legacy = {"kind": "curve_progress", "t_wall": 1.0, "t_mono": 1.0, "mint": "M", "progress_pct": 12.0}
    decoded = event_from_dict(legacy)
    assert decoded.real_sol_reserves == 0 and decoded.progress_pct == 12.0


def test_curve_progress_boot_sequence_defaults_and_roundtrip():
    from memebot.events import CurveProgress, event_from_dict, event_to_dict

    legacy = {
        "kind": "curve_progress",
        "t_wall": 1.0,
        "t_mono": 2.0,
        "mint": "M",
        "progress_pct": 12.0,
    }
    decoded = event_from_dict(legacy)
    assert decoded.source_boot_id == 0
    assert decoded.source_seq == 0
    assert type(decoded.source_boot_id) is int
    assert type(decoded.source_seq) is int

    event = CurveProgress(
        t_wall=3.0,
        t_mono=4.0,
        mint="M",
        progress_pct=34.0,
        source_boot_id=17,
        source_seq=23,
    )
    encoded = event_to_dict(event)
    assert encoded["source_boot_id"] == 17
    assert encoded["source_seq"] == 23
    assert event_from_dict(encoded) == event


def test_p1_events_register_and_roundtrip():
    from memebot.events import (CandidateScored, PaperEntry, PaperExit, SafetyPassed,
                                EVENT_TYPES, event_from_dict, event_to_dict)
    for kind in ("safety_passed", "candidate_scored", "paper_entry", "paper_exit"):
        assert kind in EVENT_TYPES
    evs = [
        SafetyPassed(t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING",
                     safety_report_id=7, risk_score=20.0),
        CandidateScored(t_wall=1.0, t_mono=1.0, mint="M", decision_id=3, segment="CLIMBING",
                        score=75.0, spot_price_sol=1e-6),
        PaperEntry(t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING", qty=1000.0,
                   fill_price=1.1e-6, size_sol=0.2, score=75.0, realism_grade="B"),
        PaperExit(t_wall=1.0, t_mono=1.0, mint="M", segment="CLIMBING", qty=400.0,
                  fill_price=2e-6, pnl_sol=0.3, reason="ladder_1", realism_grade="B"),
    ]
    for ev in evs:
        assert event_from_dict(event_to_dict(ev)) == ev


def test_p3_events_and_legacy_paperentry_decode():
    from memebot.events import CanonicalObservationStarted, PaperEntry

    observation = CanonicalObservationStarted(
        t_wall=10.0,
        t_mono=11.0,
        observation_id=12,
        decision_id=13,
        mint="M",
        start_price_sol=1e-6,
        price_observed_at=9.5,
    )
    assert EVENT_TYPES["canonical_observation_started"] is CanonicalObservationStarted
    assert event_from_dict(event_to_dict(observation)) == observation

    legacy = event_from_dict({
        "kind": "paper_entry",
        "t_wall": 1.0,
        "t_mono": 2.0,
        "mint": "M",
        "segment": "CLIMBING",
        "qty": 1000.0,
        "fill_price": 1.1e-6,
        "size_sol": 0.2,
        "score": 75.0,
        "realism_grade": "B",
    })
    assert isinstance(legacy, PaperEntry)
    assert (
        legacy.canonical_status,
        legacy.canonical_mint,
        legacy.canonical_resolver_version,
        legacy.canonical_recheck_id,
        legacy.canonical_recheck_hash,
        legacy.paper_trade_id,
        legacy.paper_entry_execution_id,
    ) == (None,) * 7

    proof = PaperEntry(
        t_wall=3.0,
        t_mono=4.0,
        mint="M",
        segment="CLIMBING",
        qty=1000.0,
        fill_price=1.1e-6,
        size_sol=0.2,
        score=75.0,
        realism_grade="B",
        canonical_status="CANONICAL",
        canonical_mint="M",
        canonical_resolver_version="canonical-v1",
        canonical_recheck_id=14,
        canonical_recheck_hash="a" * 64,
        paper_trade_id=15,
        paper_entry_execution_id=16,
    )
    assert event_from_dict(event_to_dict(proof)) == proof
