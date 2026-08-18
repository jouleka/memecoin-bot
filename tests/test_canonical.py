import hashlib
import json
from decimal import Decimal, Inexact, ROUND_DOWN, localcontext
from fractions import Fraction
from itertools import permutations

import pytest

from memebot.canonical import (
    TargetReportFence,
    canonical_generation_hash,
    creator_component,
    first_mover_component,
    holder_component,
    identity_cluster_key,
    integer_rank_points,
    liquidity_component,
    normalize_identity,
    normalize_telegram,
    normalize_twitter,
    normalize_uri,
    normalize_website,
    quantize_component,
    rank_eligible_candidates,
    rank_points_to_human,
    social_component,
    target_report_fence,
)


def test_identity_and_social_normalization_contract():
    assert normalize_identity(123) == ""
    assert normalize_identity("") == ""
    assert normalize_identity(" \t\n") == ""
    assert normalize_identity("ＰＥＰＥ coin™") == "pepecointm"
    assert normalize_identity("ÉλБ中_-") == "éλб中"
    assert normalize_identity("а") == "а"
    assert normalize_identity("α") == "α"
    assert normalize_identity("а") != normalize_identity("a")
    assert normalize_identity("é") != normalize_identity("e")

    assert identity_cluster_key(" ＰÉΠÉ ", " $Pepe_2 ") == "péπé:pepe2"
    assert identity_cluster_key("", "PEPE") is None
    assert identity_cluster_key("Pepe", None) is None
    assert identity_cluster_key([], "PEPE") is None

    assert normalize_uri(" HTTPS://Example.COM:443/Path/File?Q=MiXeD#section ") == (
        "https://example.com/Path/File?Q=MiXeD"
    )
    assert normalize_uri("http://EXAMPLE.com:80/") == "http://example.com"
    assert normalize_uri("https://EXAMPLE.com/?Q=X") == "https://example.com/?Q=X"
    assert normalize_uri("https://EXAMPLE.com/a/") == "https://example.com/a/"
    assert normalize_uri("ipfs://BafyCID/Metadata.JSON#fragment") == (
        "ipfs://bafycid/Metadata.JSON"
    )
    assert normalize_uri("AR://TransactionID/Meta?Key=Value") == (
        "ar://transactionid/Meta?Key=Value"
    )
    assert normalize_uri("ipfs:BafyCID/Meta?Q=X#fragment") == (
        "ipfs:BafyCID/Meta?Q=X"
    )
    assert normalize_uri("ar:TransactionID#fragment") == "ar:TransactionID"
    assert normalize_uri("https://EXAMPLE.com:8443/Path?Q=X#fragment") == (
        "https://example.com:8443/Path?Q=X"
    )
    assert normalize_uri("https://[2001:DB8::1]:443/Path") == (
        "https://[2001:db8::1]/Path"
    )
    assert normalize_uri("https://[v1.FOO]/Path") == "https://[v1.foo]/Path"
    assert normalize_uri("https://v1.FOO/Path") == "https://v1.foo/Path"
    assert normalize_uri("https://[v1.FOO]/Path") != normalize_uri(
        "https://v1.FOO/Path"
    )
    assert normalize_website("HTTP://Example.COM:80/") == "http://example.com"
    assert normalize_website("https://Example.COM/A?B=C#ignored") == (
        "https://example.com/A?B=C"
    )

    for invalid in (
        None,
        7,
        "",
        "   ",
        "example.com",
        "ftp://example.com/file",
        "https:///missing-host",
        "https://example.com:",
        "https://example.com:bad/path",
        "https://user:secret@example.com/path",
        "https://exa mple.com/path",
        "https://example.com/pa\nth",
        "https://example.com/pa\x00th",
        "https://[not-an-ipv6]/",
        "ipfs:///",
        "ar:/",
    ):
        assert normalize_uri(invalid) is None

    assert normalize_website("ipfs://bafy/path") is None
    assert normalize_website("ar://transaction") is None
    assert normalize_website("https://user:secret@example.com/path") is None

    twitter_cases = {
        "@Meme_Coin": "meme_coin",
        "MemeCoin32": "memecoin32",
        " AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA ": "a" * 32,
        "x.com/MemeCoin": "memecoin",
        "twitter.com/Meme_Coin": "meme_coin",
        "https://x.com/MEME": "meme",
        "http://twitter.com/Meme123": "meme123",
    }
    for raw, expected in twitter_cases.items():
        assert normalize_twitter(raw) == expected

    telegram_cases = {
        "@Meme_Coin": "meme_coin",
        "MemeCoin32": "memecoin32",
        " AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA ": "a" * 32,
        "t.me/MemeCoin": "memecoin",
        "telegram.me/Meme_Coin": "meme_coin",
        "https://t.me/MEME": "meme",
        "http://telegram.me/Meme123": "meme123",
    }
    for raw, expected in telegram_cases.items():
        assert normalize_telegram(raw) == expected

    invalid_handles = (
        None,
        7,
        "",
        "@",
        "has-hyphen",
        "é",
        "a" * 33,
        "x.com/name/extra",
        "x.com/name/",
        "x.com/name?query=1",
        "x.com/name#fragment",
        "https://x.com/name/extra",
        "https://x.com/name/",
        "https://x.com/name?query=1",
        "https://x.com/name#fragment",
        "https://x.com:/name",
        "https://x.com/na me",
        "https://example.com/name",
        "https://t.me/name",
    )
    for invalid in invalid_handles:
        assert normalize_twitter(invalid) is None

    for invalid in (
        *invalid_handles[:-1],
        "https://twitter.com/name",
        "t.me/name/extra",
        "https://telegram.me/name/",
        "https://telegram.me/name?query=1",
        "https://telegram.me/name#fragment",
        "https://t.me:/name",
    ):
        assert normalize_telegram(invalid) is None


def test_integer_component_and_rank_points_contract():
    components = {
        "first_mover": first_mover_component(
            identity_ingested_at=0.5,
            mint="mintA",
            eligible_pairs=((0.6, "mintB"), (0.5, "mintA")),
        ),
        "liquidity": liquidity_component(
            real_sol_locked=42.5,
            curve_progress_pct=99.0,
            graduation_sol=85.0,
        ),
        "holder": holder_component(
            distinct_non_curve_owners=15,
            top10_share_pct=20.0,
            top10_holder_max_pct=30.0,
        ),
        "creator": creator_component(
            creator="creatorA",
            creator_conflicted=False,
            prior_successes=1,
            prior_rugs=0,
        ),
        "social": social_component(
            candidate_values={
                "uri": "ipfs://x",
                "website": None,
                "twitter": None,
                "telegram": None,
            },
            eligible_values=(
                {
                    "uri": "ipfs://x",
                    "website": None,
                    "twitter": None,
                    "telegram": None,
                },
                {
                    "uri": "ipfs://x",
                    "website": None,
                    "twitter": None,
                    "telegram": None,
                },
            ),
            metadata_conflicts=frozenset(),
            social_weights_bps={
                "uri": 2500,
                "website": 2500,
                "twitter": 2500,
                "telegram": 2500,
            },
        ),
    }
    assert {name: quantize_component(value) for name, value in components.items()} == {
        "first_mover": 1_000_000,
        "liquidity": 500_000,
        "holder": 541_667,
        "creator": 666_667,
        "social": 250_000,
    }

    points = integer_rank_points(
        components=components,
        weights_bps={
            "first_mover": 3500,
            "liquidity": 2500,
            "holder": 2000,
            "creator": 1500,
            "social": 500,
        },
    )
    assert type(points) is int
    assert points == 6_958_334_500
    assert rank_points_to_human(points) == Decimal("69.583345")

    assert (
        quantize_component(
            liquidity_component(
                real_sol_locked=None,
                curve_progress_pct=40.0,
                graduation_sol=85.0,
            )
        )
        == 400_000
    )
    assert (
        quantize_component(
            liquidity_component(
                real_sol_locked=Decimal("1e100"),
                curve_progress_pct=None,
                graduation_sol=85.0,
            )
        )
        == 1_000_000
    )
    assert quantize_component(0.0000005) == 1
    assert quantize_component(0.5416665) == 541_667
    below_half_ppm = Decimal("0.0000004" + "9" * 80)
    assert below_half_ppm < Decimal("0.0000005")
    assert quantize_component(below_half_ppm) == 0
    assert creator_component(
        creator=None,
        creator_conflicted=False,
        prior_successes=0,
        prior_rugs=0,
    ) == Decimal(0)
    assert creator_component(
        creator="creatorA",
        creator_conflicted=True,
        prior_successes=1,
        prior_rugs=0,
    ) == Decimal(0)
    assert social_component(
        candidate_values={
            "uri": None,
            "website": None,
            "twitter": "alpha",
            "telegram": None,
        },
        eligible_values=(
            {
                "uri": None,
                "website": None,
                "twitter": "alpha",
                "telegram": None,
            },
            {
                "uri": None,
                "website": None,
                "twitter": "beta",
                "telegram": None,
            },
        ),
        metadata_conflicts=frozenset(),
        social_weights_bps={
            "uri": 2500,
            "website": 2500,
            "twitter": 2500,
            "telegram": 2500,
        },
    ) == Decimal("0.125")

    sparse_social = social_component(
        candidate_values={
            "uri": "candidate",
            "website": None,
            "twitter": None,
            "telegram": None,
        },
        eligible_values=tuple(
            {
                "uri": "candidate" if index < 5 else f"peer-{index}",
                "website": None,
                "twitter": None,
                "telegram": None,
            }
            for index in range(24)
        ),
        metadata_conflicts=frozenset(),
        social_weights_bps={
            "uri": 3,
            "website": 9997,
            "twitter": 0,
            "telegram": 0,
        },
    )
    assert sparse_social == Decimal("0.0000625")
    assert quantize_component(sparse_social) == 63

    mixed_social = social_component(
        candidate_values={
            "uri": "uri-target",
            "website": "website-target",
            "twitter": "twitter-target",
            "telegram": "telegram-target",
        },
        eligible_values=tuple(
            {
                "uri": "uri-target" if index < 22 else f"uri-peer-{index}",
                "website": (
                    "website-target"
                    if index < 28
                    else f"website-peer-{index}"
                    if index < 42
                    else None
                ),
                "twitter": "twitter-target" if index < 18 else None,
                "telegram": (
                    "telegram-target"
                    if index < 5
                    else "telegram-peer"
                    if index == 5
                    else None
                ),
            }
            for index in range(48)
        ),
        metadata_conflicts=frozenset(),
        social_weights_bps={
            "uri": 4333,
            "website": 1013,
            "twitter": 864,
            "telegram": 3790,
        },
    )
    assert quantize_component(mixed_social) == 668_363
    assert mixed_social == Fraction(53_469, 80_000)
    assert mixed_social == Decimal("0.6683625")

    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        context.traps[Inexact] = True
        low_precision_components = {
            "first_mover": first_mover_component(
                identity_ingested_at=0.5,
                mint="mintA",
                eligible_pairs=((0.6, "mintB"), (0.5, "mintA")),
            ),
            "liquidity": liquidity_component(
                real_sol_locked=42.5,
                curve_progress_pct=99.0,
                graduation_sol=85.0,
            ),
            "holder": holder_component(
                distinct_non_curve_owners=15,
                top10_share_pct=20.0,
                top10_holder_max_pct=30.0,
            ),
            "creator": creator_component(
                creator="creatorA",
                creator_conflicted=False,
                prior_successes=1,
                prior_rugs=0,
            ),
            "social": social_component(
                candidate_values={
                    "uri": "ipfs://x",
                    "website": None,
                    "twitter": None,
                    "telegram": None,
                },
                eligible_values=(
                    {
                        "uri": "ipfs://x",
                        "website": None,
                        "twitter": None,
                        "telegram": None,
                    },
                    {
                        "uri": "ipfs://x",
                        "website": None,
                        "twitter": None,
                        "telegram": None,
                    },
                ),
                metadata_conflicts=frozenset(),
                social_weights_bps={
                    "uri": 2500,
                    "website": 2500,
                    "twitter": 2500,
                    "telegram": 2500,
                },
            ),
        }
        assert {
            name: quantize_component(value)
            for name, value in low_precision_components.items()
        } == {
            "first_mover": 1_000_000,
            "liquidity": 500_000,
            "holder": 541_667,
            "creator": 666_667,
            "social": 250_000,
        }
        low_precision_points = integer_rank_points(
            components=low_precision_components,
            weights_bps={
                "first_mover": 3500,
                "liquidity": 2500,
                "holder": 2000,
                "creator": 1500,
                "social": 500,
            },
        )
        assert low_precision_points == 6_958_334_500
        assert rank_points_to_human(low_precision_points) == Decimal("69.583345")

    for kwargs in (
        {
            "real_sol_locked": -1.0,
            "curve_progress_pct": None,
            "graduation_sol": 85.0,
        },
        {
            "real_sol_locked": float("nan"),
            "curve_progress_pct": None,
            "graduation_sol": 85.0,
        },
        {
            "real_sol_locked": Decimal("1e101"),
            "curve_progress_pct": None,
            "graduation_sol": 85.0,
        },
        {
            "real_sol_locked": None,
            "curve_progress_pct": 100.1,
            "graduation_sol": 85.0,
        },
    ):
        with pytest.raises(ValueError):
            liquidity_component(**kwargs)

    for kwargs in (
        {
            "distinct_non_curve_owners": True,
            "top10_share_pct": 20.0,
            "top10_holder_max_pct": 30.0,
        },
        {
            "distinct_non_curve_owners": 15,
            "top10_share_pct": float("inf"),
            "top10_holder_max_pct": 30.0,
        },
        {
            "distinct_non_curve_owners": 15,
            "top10_share_pct": 101.0,
            "top10_holder_max_pct": 30.0,
        },
    ):
        with pytest.raises(ValueError):
            holder_component(**kwargs)

    with pytest.raises(ValueError):
        quantize_component(1.000001)


def test_ranking_permutation_ties_and_generation_hash():
    candidates = (
        {
            "mint": "mint-c",
            "p3_identity_ingested_at": 2.0,
            "rank_points": 900,
        },
        {
            "mint": "mint-b",
            "p3_identity_ingested_at": 1.0,
            "rank_points": 900,
        },
        {
            "mint": "mint-a",
            "p3_identity_ingested_at": 2.0,
            "rank_points": 900,
        },
        {
            "mint": "mint-z",
            "p3_identity_ingested_at": 0.5,
            "rank_points": 901,
        },
    )
    expected_ranking = (
        ("mint-z", 901, 1),
        ("mint-b", 900, 2),
        ("mint-a", 900, 3),
        ("mint-c", 900, 4),
    )
    for permutation in permutations(candidates):
        ranked = rank_eligible_candidates(permutation)
        assert tuple(
            (candidate.mint, candidate.rank_points, candidate.rank)
            for candidate in ranked
        ) == expected_ranking
        assert [candidate.rank for candidate in ranked] == [1, 2, 3, 4]
        assert sum(candidate.rank == 1 for candidate in ranked) == 1

    eligible = (
        {
            "mint": "mint-b",
            "safety_report_id": 12,
            "holder_evidence_id": 22,
        },
        {
            "mint": "mint-a",
            "safety_report_id": 11,
            "holder_evidence_id": 21,
        },
    )
    generation_args = {
        "cluster_key": "pepe:pepe",
        "eligible": eligible,
        "canonical_mint": "mint-a",
        "resolver_version": "canonical-v1",
        "weights_version": "canonical-weighted-v1",
        "config_hash": "a" * 64,
    }
    expected_payload = {
        "cluster_key": "pepe:pepe",
        "eligible": [
            {
                "mint": "mint-a",
                "safety_report_id": 11,
                "holder_evidence_id": 21,
            },
            {
                "mint": "mint-b",
                "safety_report_id": 12,
                "holder_evidence_id": 22,
            },
        ],
        "canonical_mint": "mint-a",
        "resolver_version": "canonical-v1",
        "weights_version": "canonical-weighted-v1",
        "config_hash": "a" * 64,
    }
    expected_hash = hashlib.sha256(
        json.dumps(
            expected_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    assert canonical_generation_hash(**generation_args) == expected_hash
    assert canonical_generation_hash(
        **{**generation_args, "eligible": tuple(reversed(eligible))}
    ) == expected_hash

    changes = (
        {"eligible": eligible[1:]},
        {
            "eligible": (
                {**eligible[0], "safety_report_id": 99},
                eligible[1],
            )
        },
        {
            "eligible": (
                {**eligible[0], "holder_evidence_id": 99},
                eligible[1],
            )
        },
        {"canonical_mint": "mint-b"},
        {"resolver_version": "canonical-v2"},
        {"weights_version": "canonical-weighted-v2"},
        {"config_hash": "b" * 64},
    )
    for change in changes:
        assert (
            canonical_generation_hash(**{**generation_args, **change})
            != expected_hash
        )


def test_target_exact_and_latest_report_fence(tmp_path):
    from memebot.store import EvidenceIntegrityError, open_db

    conn = open_db(tmp_path / "target-report-fence.db", migration_clock=lambda: 1.0)

    def insert_report(
        *,
        checked_at,
        hard_fails_json="[]",
        holder_shape="available",
    ):
        report_id = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
            ") VALUES ('MINT',?,?,5.0,?)",
            (checked_at, hard_fails_json, "a" * 64),
        ).lastrowid
        if holder_shape == "available":
            conn.execute(
                "INSERT INTO holder_evidence("
                "safety_report_id,sampled_token_accounts,"
                "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
                "holder_observed_at,unavailable_reason,inputs_hash"
                ") VALUES (?,20,12,25.0,?,'',?)",
                (report_id, checked_at - 0.5, "b" * 64),
            )
        elif holder_shape == "unavailable":
            conn.execute(
                "INSERT INTO holder_evidence("
                "safety_report_id,sampled_token_accounts,"
                "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
                "holder_observed_at,unavailable_reason,inputs_hash"
                ") VALUES (?,NULL,NULL,NULL,?,'holder_accounts_unavailable',?)",
                (report_id, checked_at - 0.5, "b" * 64),
            )
        elif holder_shape == "malformed":
            conn.execute(
                "INSERT INTO holder_evidence("
                "safety_report_id,sampled_token_accounts,"
                "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
                "holder_observed_at,unavailable_reason,inputs_hash"
                ") VALUES (?,20,12,25.0,?,'',?)",
                (report_id, checked_at + 0.5, "b" * 64),
            )
        elif holder_shape != "childless":
            raise AssertionError(holder_shape)
        conn.commit()
        return report_id

    causal_id = insert_report(checked_at=10.0)
    current = target_report_fence(
        conn,
        mint="MINT",
        decision_at=30.0,
        target_report_id=causal_id,
    )
    assert current == TargetReportFence(
        causal_report=current.causal_report,
        latest_report=current.causal_report,
    )
    assert current.causal_report.safety_report_id == causal_id
    assert current.is_current

    newer_cases = (
        {"checked_at": 10.0},
        {"checked_at": 11.0},
        {"checked_at": 12.0, "hard_fails_json": '["hard_fail"]'},
        {"checked_at": 13.0, "holder_shape": "unavailable"},
        {"checked_at": 14.0, "holder_shape": "childless"},
        {"checked_at": 15.0, "holder_shape": "malformed"},
    )
    for case in newer_cases:
        newest_id = insert_report(**case)
        superseded = target_report_fence(
            conn,
            mint="MINT",
            decision_at=30.0,
            target_report_id=causal_id,
        )
        assert superseded.causal_report.safety_report_id == causal_id
        assert superseded.latest_report is not None
        assert superseded.latest_report.safety_report_id == newest_id
        assert not superseded.is_current

    equal_time_id = insert_report(checked_at=30.0)
    equal_time = target_report_fence(
        conn,
        mint="MINT",
        decision_at=30.0,
        target_report_id=causal_id,
    )
    assert equal_time.causal_report.safety_report_id == causal_id
    assert equal_time.latest_report is None
    assert not equal_time.is_current
    assert conn.execute(
        "SELECT id FROM safety_reports WHERE mint='MINT' ORDER BY id DESC LIMIT 1"
    ).fetchone()[0] == equal_time_id

    for invalid_report_id in (0, True):
        with pytest.raises(ValueError, match="invalid safety report ID"):
            target_report_fence(
                conn,
                mint="MINT",
                decision_at=30.0,
                target_report_id=invalid_report_id,
            )
    with pytest.raises(EvidenceIntegrityError, match="safety report mint mismatch"):
        target_report_fence(
            conn,
            mint="OTHER",
            decision_at=30.0,
            target_report_id=causal_id,
        )
    with pytest.raises(ValueError, match="invalid p3 causal wall"):
        target_report_fence(
            conn,
            mint="MINT",
            decision_at=True,
            target_report_id=causal_id,
        )
    with pytest.raises(ValueError, match="invalid p3 causal wall"):
        target_report_fence(
            conn,
            mint="MINT",
            decision_at=True,
            target_report_id=0,
        )
    conn.close()


def test_resolver_verdict_observation_matrix(tmp_path):
    import memebot.store as store_module
    from memebot.canonical import (
        CanonicalObservationDraft,
        CanonicalResolution,
        CanonicalResolver,
        CanonicalVerdict,
    )
    from memebot.features import CurveSnapshot
    from memebot.store import (
        CreatorReputationResult,
        open_db,
        upsert_token_identity,
    )

    class SnapshotSource:
        def __init__(self):
            self.snapshots = {}
            self.fail = set()

        def snapshot_at_or_before(self, mint, *, as_of):
            if mint in self.fail:
                raise RuntimeError("snapshot provider failed")
            snapshot = self.snapshots.get(mint)
            if snapshot is None or snapshot.t_wall > as_of:
                return None
            return snapshot

        def p3_snapshot_at_or_before(self, mint, *, as_of, **_kwargs):
            return self.snapshot_at_or_before(mint, as_of=as_of)

    canonical_cfg = {
        "enabled": True,
        "resolver_version": "canonical-v1",
        "weights_version": "canonical-weighted-v1",
        "live_states": ["FRESH", "CLIMBING"],
        "max_cluster_candidates": 2,
        "max_creator_history_mints": 10,
        "max_feature_mints": 100,
        "max_open_p3_positions": 10,
        "liquidity_max_age_s": 60.0,
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
    }
    safety_cfg = {
        "top10_holder_max_pct": 30.0,
        "early_buyers": {"buyer_limit": 2},
    }
    pumpfun_cfg = {"graduation_sol": 85.0, "token_decimals": 6}
    config_hash = "c" * 64
    snapshots = SnapshotSource()
    conn = open_db(tmp_path / "resolver-matrix.db", migration_clock=lambda: 1.0)
    report_ids = {}

    def add_token(
        mint,
        *,
        name,
        symbol,
        ingested_at,
        state="CLIMBING",
        creator=None,
        liquidity_sol=42.5,
        holder_shape="available",
        buyers=("buyer-a",),
    ):
        upsert_token_identity(
            conn,
            mint=mint,
            raw_ingested_at=ingested_at,
            bonding_curve_key=f"curve-{mint}",
            fields={
                "creator": creator or f"creator-{mint}",
                "name": name,
                "symbol": symbol,
                "uri": "ipfs://shared",
                "website": "",
                "twitter": "@shared",
                "telegram": "",
            },
        )
        conn.execute(
            "UPDATE tokens SET state=?,rugged=? WHERE mint=?",
            (state, int(state == "DEAD"), mint),
        )
        report_id = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
            ") VALUES (?,40.0,'[]',5.0,?)",
            (mint, (mint.encode().hex() + "0" * 64)[:64]),
        ).lastrowid
        if holder_shape == "available":
            conn.execute(
                "INSERT INTO holder_evidence("
                "safety_report_id,sampled_token_accounts,"
                "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
                "holder_observed_at,unavailable_reason,inputs_hash"
                ") VALUES (?,20,12,20.0,39.0,'',?)",
                (report_id, (mint.encode().hex() + "1" * 64)[:64]),
            )
        elif holder_shape == "unavailable":
            conn.execute(
                "INSERT INTO holder_evidence("
                "safety_report_id,sampled_token_accounts,"
                "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
                "holder_observed_at,unavailable_reason,inputs_hash"
                ") VALUES (?,NULL,NULL,NULL,39.0,"
                "'holder_accounts_unavailable',?)",
                (report_id, (mint.encode().hex() + "1" * 64)[:64]),
            )
        else:
            raise AssertionError(holder_shape)
        conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id) VALUES (?,39.5,?,'',?,?)",
            (
                mint,
                json.dumps(list(buyers), separators=(",", ":")),
                (mint.encode().hex() + "2" * 64)[:64],
                report_id,
            ),
        )
        conn.commit()
        report_ids[mint] = report_id
        if liquidity_sol is not None:
            snapshots.snapshots[mint] = CurveSnapshot(
                source_boot_id=7,
                source_seq=len(snapshots.snapshots) + 1,
                t_wall=50.0,
                t_mono=5.0,
                virtual_sol_reserves=70_000_000_000,
                virtual_token_reserves=70_000_000_000_000,
                real_sol_reserves=int(liquidity_sol * 1_000_000_000),
                real_token_reserves=400_000_000_000_000,
                liquidity_sol=liquidity_sol,
                spot_price_sol=0.000001,
                progress_pct=min(100.0, 100.0 * liquidity_sol / 85.0),
            )
        return report_id

    def resolver(**canonical_overrides):
        return CanonicalResolver(
            conn,
            feature_engine=snapshots,
            canonical_cfg={**canonical_cfg, **canonical_overrides},
            safety_cfg=safety_cfg,
            pumpfun_cfg=pumpfun_cfg,
            config_hash=config_hash,
            counterfactual_horizons=(3600.0, 21600.0, 86400.0),
            runtime_boot_id=7,
            runtime_causal_floor=1.0,
        )

    def resolve(mint, *, report_id=None, instance=None):
        return (instance or resolver()).resolve(
            mint,
            decision_at=100.0,
            target_report_id=report_id or report_ids[mint],
        )

    def assert_subject_only(result, *, mint, reason):
        assert isinstance(result, CanonicalResolution)
        assert isinstance(result.verdict, CanonicalVerdict)
        assert result.verdict.status == "UNRESOLVED"
        assert result.verdict.reason == reason
        assert result.verdict.rank is None
        assert result.verdict.rank_points is None
        assert result.verdict.canonical_mint is None
        assert result.verdict.generation_hash is None
        assert (
            len(result.verdict.inputs_hash) == 64
            and set(result.verdict.inputs_hash) <= set("0123456789abcdef")
        )
        assert len(result.observations) == 1
        assert result.observations == (
            CanonicalObservationDraft(
                mint=mint,
                is_subject=True,
                is_canonical=False,
                eligible=False,
                start_price_sol=0.000001,
                price_observed_at=50.0,
                unavailable_reason="",
            ),
        )

    def assert_full_cluster(
        result,
        *,
        subject_mint,
        canonical_mint,
        eligible_mints,
        cluster_mints,
    ):
        assert tuple(row.mint for row in result.observations) == tuple(
            sorted(cluster_mints)
        )
        assert {
            row.mint: (row.is_subject, row.is_canonical, row.eligible)
            for row in result.observations
        } == {
            member: (
                member == subject_mint,
                member == canonical_mint,
                member in eligible_mints,
            )
            for member in cluster_mints
        }
        assert sum(row.is_subject for row in result.observations) == 1
        assert sum(row.is_canonical for row in result.observations) == (
            0 if canonical_mint is None else 1
        )
        assert result.verdict.cluster_size == len(cluster_mints)
        assert result.verdict.eligible_cluster_size == len(eligible_mints)
        assert result.verdict.canonical_mint == canonical_mint
        assert (result.verdict.generation_hash is not None) == (
            canonical_mint is not None
        )
        assert (
            len(result.verdict.inputs_hash) == 64
            and set(result.verdict.inputs_hash) <= set("0123456789abcdef")
        )

    add_token("WIN", name="Pepe", symbol="PEPE", ingested_at=2.0, liquidity_sol=50.0)
    add_token("LOSE", name="Ｐｅｐｅ", symbol="pepe", ingested_at=3.0, liquidity_sol=20.0)
    selected = resolve("WIN")
    suppressed = resolve("LOSE")
    assert selected.verdict.status == "CANONICAL"
    assert selected.verdict.reason == "canonical_selected"
    assert selected.verdict.rank == 1
    assert selected.verdict.canonical_mint == "WIN"
    assert (
        selected.verdict.generation_hash is not None
        and len(selected.verdict.generation_hash) == 64
        and set(selected.verdict.generation_hash) <= set("0123456789abcdef")
    )
    assert (
        len(selected.verdict.inputs_hash) == 64
        and set(selected.verdict.inputs_hash) <= set("0123456789abcdef")
    )
    assert selected.verdict.cluster_size == 2
    assert selected.verdict.eligible_cluster_size == 2
    assert suppressed.verdict.status == "SUPPRESSED"
    assert suppressed.verdict.reason == "copycat_cluster"
    assert suppressed.verdict.rank == 2
    assert suppressed.verdict.canonical_mint == "WIN"
    assert suppressed.verdict.generation_hash == selected.verdict.generation_hash
    assert selected.observations == (
        CanonicalObservationDraft(
            mint="LOSE",
            is_subject=False,
            is_canonical=False,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=50.0,
            unavailable_reason="",
        ),
        CanonicalObservationDraft(
            mint="WIN",
            is_subject=True,
            is_canonical=True,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=50.0,
            unavailable_reason="",
        ),
    )
    assert suppressed.observations == (
        CanonicalObservationDraft(
            mint="LOSE",
            is_subject=True,
            is_canonical=False,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=50.0,
            unavailable_reason="",
        ),
        CanonicalObservationDraft(
            mint="WIN",
            is_subject=False,
            is_canonical=True,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=50.0,
            unavailable_reason="",
        ),
    )
    assert [
        candidate["mint"]
        for candidate in sorted(
            selected.verdict.ranking_inputs["candidates"],
            key=lambda candidate: candidate["rank"],
        )
    ] == ["WIN", "LOSE"]
    assert_full_cluster(
        selected,
        subject_mint="WIN",
        canonical_mint="WIN",
        eligible_mints={"LOSE", "WIN"},
        cluster_mints={"LOSE", "WIN"},
    )
    assert_full_cluster(
        suppressed,
        subject_mint="LOSE",
        canonical_mint="WIN",
        eligible_mints={"LOSE", "WIN"},
        cluster_mints={"LOSE", "WIN"},
    )

    add_token(
        "NOT-LIVE",
        name="Dead",
        symbol="DEAD",
        ingested_at=4.0,
        state="DEAD",
    )
    assert_subject_only(
        resolve("NOT-LIVE"),
        mint="NOT-LIVE",
        reason="canonical_target_not_live",
    )

    add_token("NO-NAME", name="", symbol="NONE", ingested_at=5.0)
    assert_subject_only(
        resolve("NO-NAME"),
        mint="NO-NAME",
        reason="canonical_identity_unavailable",
    )

    add_token("CONFLICT", name="Alpha", symbol="ALPHA", ingested_at=6.0)
    upsert_token_identity(
        conn,
        mint="CONFLICT",
        raw_ingested_at=7.0,
        bonding_curve_key="curve-CONFLICT",
        fields={"name": "Beta"},
    )
    assert_subject_only(
        resolve("CONFLICT"),
        mint="CONFLICT",
        reason="canonical_identity_conflict",
    )

    for index in range(3):
        add_token(
            f"OVERSIZE-{index}",
            name="Oversized",
            symbol="BIG",
            ingested_at=10.0 + index,
        )
    oversized = resolve("OVERSIZE-0")
    assert_subject_only(
        oversized,
        mint="OVERSIZE-0",
        reason="canonical_cluster_too_large",
    )
    assert oversized.verdict.cluster_size == 3

    old_superseded = add_token(
        "SUPERSEDED",
        name="Fence",
        symbol="FENCE",
        ingested_at=20.0,
        liquidity_sol=60.0,
    )
    add_token(
        "FENCE-PEER",
        name="Fence",
        symbol="FENCE",
        ingested_at=21.0,
        liquidity_sol=10.0,
    )
    add_token(
        "SUPERSEDED",
        name="Fence",
        symbol="FENCE",
        ingested_at=22.0,
        liquidity_sol=60.0,
    )
    superseded = resolve("SUPERSEDED", report_id=old_superseded)
    assert superseded.verdict.status == "UNRESOLVED"
    assert superseded.verdict.reason == "canonical_target_report_superseded"
    assert superseded.verdict.canonical_mint == "FENCE-PEER"
    assert superseded.verdict.generation_hash is not None
    assert superseded.verdict.eligible_cluster_size == 1
    assert superseded.observations == (
        CanonicalObservationDraft(
            mint="FENCE-PEER",
            is_subject=False,
            is_canonical=True,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=50.0,
            unavailable_reason="",
        ),
        CanonicalObservationDraft(
            mint="SUPERSEDED",
            is_subject=True,
            is_canonical=False,
            eligible=False,
            start_price_sol=0.000001,
            price_observed_at=50.0,
            unavailable_reason="",
        ),
    )
    superseded_payload = {
        candidate["mint"]: candidate
        for candidate in superseded.verdict.ranking_inputs["candidates"]
    }["SUPERSEDED"]
    assert superseded_payload["ineligible_reason"] == (
        "canonical_target_report_superseded"
    )
    assert {
        field: superseded_payload[field]
        for field in (
            "safety_report_id",
            "safety_checked_at",
            "safety_inputs_hash",
            "safety_hard_fails",
            "safety_risk_score",
            "holder_evidence_id",
            "holder_inputs_hash",
            "holder_observed_at",
            "liquidity_source",
            "liquidity_observed_at",
        )
    } == {
        "safety_report_id": None,
        "safety_checked_at": None,
        "safety_inputs_hash": None,
        "safety_hard_fails": None,
        "safety_risk_score": None,
        "holder_evidence_id": None,
        "holder_inputs_hash": None,
        "holder_observed_at": None,
        "liquidity_source": None,
        "liquidity_observed_at": None,
    }
    assert superseded_payload["raw"] == {
        "liquidity_sol": None,
        "curve_progress_pct": None,
        "curve_snapshot": None,
        "sampled_token_accounts": None,
        "distinct_non_curve_owners": None,
        "top10_non_curve_owner_share_pct": None,
        "creator_prior_successes": None,
        "creator_prior_rugs": None,
        "creator_reputation_event_ids": None,
        "social": None,
    }
    assert superseded_payload["components_ppm"] == {}
    assert superseded_payload["rank_points"] is None
    assert superseded_payload["rank"] is None
    assert_full_cluster(
        superseded,
        subject_mint="SUPERSEDED",
        canonical_mint="FENCE-PEER",
        eligible_mints={"FENCE-PEER"},
        cluster_mints={"FENCE-PEER", "SUPERSEDED"},
    )

    add_token(
        "NO-HOLDER",
        name="Holder",
        symbol="HOLD",
        ingested_at=30.0,
        liquidity_sol=60.0,
        holder_shape="unavailable",
    )
    add_token(
        "HOLDER-PEER",
        name="Holder",
        symbol="HOLD",
        ingested_at=31.0,
        liquidity_sol=10.0,
    )
    no_holder = resolve("NO-HOLDER")
    assert no_holder.verdict.status == "UNRESOLVED"
    assert no_holder.verdict.reason == "canonical_holder_evidence_unavailable"
    assert no_holder.verdict.canonical_mint == "HOLDER-PEER"
    assert no_holder.verdict.generation_hash is not None
    assert len(no_holder.observations) == 2
    assert_full_cluster(
        no_holder,
        subject_mint="NO-HOLDER",
        canonical_mint="HOLDER-PEER",
        eligible_mints={"HOLDER-PEER"},
        cluster_mints={"HOLDER-PEER", "NO-HOLDER"},
    )

    add_token(
        "NO-LIQUIDITY",
        name="Liquidity",
        symbol="LIQ",
        ingested_at=32.0,
        liquidity_sol=None,
    )
    add_token(
        "LIQUIDITY-PEER",
        name="Liquidity",
        symbol="LIQ",
        ingested_at=33.0,
        liquidity_sol=10.0,
    )
    no_liquidity = resolve("NO-LIQUIDITY")
    assert no_liquidity.verdict.status == "UNRESOLVED"
    assert no_liquidity.verdict.reason == "canonical_liquidity_unavailable"
    assert no_liquidity.verdict.canonical_mint == "LIQUIDITY-PEER"
    assert no_liquidity.verdict.generation_hash is not None
    assert len(no_liquidity.observations) == 2
    subject = next(row for row in no_liquidity.observations if row.is_subject)
    assert subject == CanonicalObservationDraft(
        mint="NO-LIQUIDITY",
        is_subject=True,
        is_canonical=False,
        eligible=False,
        start_price_sol=None,
        price_observed_at=None,
        unavailable_reason="start_price_missing",
    )
    assert_full_cluster(
        no_liquidity,
        subject_mint="NO-LIQUIDITY",
        canonical_mint="LIQUIDITY-PEER",
        eligible_mints={"LIQUIDITY-PEER"},
        cluster_mints={"LIQUIDITY-PEER", "NO-LIQUIDITY"},
    )

    add_token(
        "TOO-MANY-BUYERS",
        name="BuyerLimit",
        symbol="BUY",
        ingested_at=34.0,
        buyers=("buyer-a", "buyer-b", "buyer-c"),
    )
    add_token(
        "BUYER-PEER",
        name="BuyerLimit",
        symbol="BUY",
        ingested_at=35.0,
    )
    buyer_limit = resolve("TOO-MANY-BUYERS")
    assert buyer_limit.verdict.status == "UNRESOLVED"
    assert buyer_limit.verdict.reason == "canonical_internal_error"
    assert buyer_limit.verdict.canonical_mint == "BUYER-PEER"
    assert buyer_limit.verdict.generation_hash is not None
    assert len(buyer_limit.observations) == 2
    assert not next(
        row for row in buyer_limit.observations if row.is_subject
    ).eligible
    assert_full_cluster(
        buyer_limit,
        subject_mint="TOO-MANY-BUYERS",
        canonical_mint="BUYER-PEER",
        eligible_mints={"BUYER-PEER"},
        cluster_mints={"BUYER-PEER", "TOO-MANY-BUYERS"},
    )

    add_token(
        "CREATOR-OVERFLOW",
        name="Creator",
        symbol="HISTORY",
        ingested_at=35.5,
        creator="shared-history",
    )
    add_token(
        "CREATOR-PEER",
        name="Creator",
        symbol="HISTORY",
        ingested_at=35.75,
    )
    for index in range(2):
        history_mint = f"HISTORY-{index}"
        add_token(
            history_mint,
            name=f"History {index}",
            symbol=f"H{index}",
            ingested_at=35.8 + index / 10,
            creator="shared-history",
        )
        conn.execute(
            "INSERT INTO creator_reputation_events("
            "mint,creator,outcome,observed_at"
            ") VALUES (?,?,'GRADUATED',?)",
            (history_mint, "shared-history", 60.0 + index),
        )
        conn.commit()
    creator_overflow = resolve(
        "CREATOR-OVERFLOW",
        instance=resolver(max_creator_history_mints=1),
    )
    assert creator_overflow.verdict.status == "UNRESOLVED"
    assert creator_overflow.verdict.reason == "canonical_creator_history_overflow"
    assert creator_overflow.verdict.canonical_mint == "CREATOR-PEER"
    assert creator_overflow.verdict.generation_hash is not None
    assert len(creator_overflow.observations) == 2
    assert_full_cluster(
        creator_overflow,
        subject_mint="CREATOR-OVERFLOW",
        canonical_mint="CREATOR-PEER",
        eligible_mints={"CREATOR-PEER"},
        cluster_mints={"CREATOR-OVERFLOW", "CREATOR-PEER"},
    )

    invalid_creators = {
        "CREATOR-EDGE-SPACE": " creator-edge-space ",
        "CREATOR-NUL": "\x00creator-nul",
        "CREATOR-TOO-LONG": "x" * 129,
        "CREATOR-CONFLICT": "creator-conflict-first",
    }
    for index, (invalid_mint, invalid_creator) in enumerate(
        invalid_creators.items()
    ):
        add_token(
            invalid_mint,
            name=f"Invalid Creator {index}",
            symbol=f"IC{index}",
            ingested_at=36.0 + index / 10,
            creator=invalid_creator,
        )
    upsert_token_identity(
        conn,
        mint="CREATOR-CONFLICT",
        raw_ingested_at=36.5,
        bonding_curve_key="curve-CREATOR-CONFLICT",
        fields={"creator": "creator-conflict-second"},
    )
    conn.commit()

    reputation_calls = []
    original_creator_reputation = (
        store_module.validated_creator_reputation_current
    )

    def record_creator_reputation(*args, **kwargs):
        reputation_calls.append(kwargs["creator"])
        return original_creator_reputation(*args, **kwargs)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            store_module,
            "validated_creator_reputation_current",
            record_creator_reputation,
        )
        invalid_creator_results = {
            invalid_mint: resolve(invalid_mint)
            for invalid_mint in invalid_creators
        }
    assert reputation_calls == []
    for invalid_mint, invalid_result in invalid_creator_results.items():
        assert invalid_result.verdict.status == "CANONICAL"
        assert invalid_result.verdict.reason == "canonical_selected"
        invalid_payload = invalid_result.verdict.ranking_inputs["candidates"][0]
        assert invalid_payload["mint"] == invalid_mint
        assert invalid_payload["creator"] == invalid_creators[invalid_mint]
        assert invalid_payload["components_ppm"]["creator"] == 0
        assert invalid_payload["raw"]["creator_prior_successes"] == 0
        assert invalid_payload["raw"]["creator_prior_rugs"] == 0
    conflict_payload = invalid_creator_results[
        "CREATOR-CONFLICT"
    ].verdict.ranking_inputs["candidates"][0]
    assert conflict_payload["identity_conflicts"] == ["creator"]

    add_token(
        "CREATOR-UNAVAILABLE",
        name="Creator Unavailable",
        symbol="CREATOR-UNAVAILABLE",
        ingested_at=36.6,
        creator="creator-unavailable",
    )
    add_token(
        "CREATOR-UNAVAILABLE-PEER",
        name="Creator Unavailable",
        symbol="CREATOR-UNAVAILABLE",
        ingested_at=36.7,
    )
    add_token(
        "CREATOR-VALUE-ERROR",
        name="Creator Value Error",
        symbol="CREATOR-VALUE-ERROR",
        ingested_at=36.8,
        creator="creator-value-error",
    )
    add_token(
        "CREATOR-VALUE-ERROR-PEER",
        name="Creator Value Error",
        symbol="CREATOR-VALUE-ERROR",
        ingested_at=36.9,
    )

    def unavailable_creator_reputation(*args, **kwargs):
        if kwargs["creator"] == "creator-unavailable":
            return CreatorReputationResult(
                prior_successes=0,
                prior_rugs=0,
                selected_event_ids=(),
                as_of=kwargs["as_of"],
                unavailable_reason="creator_reputation_unavailable",
            )
        if kwargs["creator"] == "creator-value-error":
            raise ValueError("invalid creator evidence")
        return original_creator_reputation(*args, **kwargs)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            store_module,
            "validated_creator_reputation_current",
            unavailable_creator_reputation,
        )
        creator_unavailable = resolve("CREATOR-UNAVAILABLE")
        creator_value_error = resolve("CREATOR-VALUE-ERROR")
    for result, subject_mint, peer_mint in (
        (
            creator_unavailable,
            "CREATOR-UNAVAILABLE",
            "CREATOR-UNAVAILABLE-PEER",
        ),
        (
            creator_value_error,
            "CREATOR-VALUE-ERROR",
            "CREATOR-VALUE-ERROR-PEER",
        ),
    ):
        assert result.verdict.status == "UNRESOLVED"
        assert result.verdict.reason == "canonical_internal_error"
        assert_full_cluster(
            result,
            subject_mint=subject_mint,
            canonical_mint=peer_mint,
            eligible_mints={peer_mint},
            cluster_mints={subject_mint, peer_mint},
        )

    add_token("SNAP-FAIL", name="Failure", symbol="FAIL", ingested_at=36.0)
    add_token("FAIL-PEER", name="Failure", symbol="FAIL", ingested_at=37.0)
    snapshots.fail.add("SNAP-FAIL")
    snapshot_failure = resolve("SNAP-FAIL")
    assert snapshot_failure.verdict.status == "UNRESOLVED"
    assert snapshot_failure.verdict.reason == "canonical_internal_error"
    assert snapshot_failure.verdict.canonical_mint == "FAIL-PEER"
    assert snapshot_failure.verdict.generation_hash is not None
    assert sum(row.is_canonical for row in snapshot_failure.observations) == 1
    assert len(snapshot_failure.observations) == 2
    assert snapshot_failure.observations[1] == CanonicalObservationDraft(
        mint="SNAP-FAIL",
        is_subject=True,
        is_canonical=False,
        eligible=False,
        start_price_sol=None,
        price_observed_at=None,
        unavailable_reason="start_price_malformed",
    )
    assert_full_cluster(
        snapshot_failure,
        subject_mint="SNAP-FAIL",
        canonical_mint="FAIL-PEER",
        eligible_mints={"FAIL-PEER"},
        cluster_mints={"FAIL-PEER", "SNAP-FAIL"},
    )

    add_token("BAD-META", name="Bad", symbol="META", ingested_at=38.0)
    conn.execute("UPDATE tokens SET meta_json='[]' WHERE mint='BAD-META'")
    conn.commit()
    assert_subject_only(
        resolve("BAD-META"),
        mint="BAD-META",
        reason="canonical_internal_error",
    )

    conn.close()


def test_resolver_asof_freshness_and_peer_eligibility(tmp_path):
    from memebot.canonical import CanonicalResolver
    from memebot.events import CurveProgress
    from memebot.features import CurveSnapshot, FeatureEngine
    from memebot.store import open_db, upsert_token_identity

    decision_at = 100.0
    runtime_boot_id = 7

    real_source = FeatureEngine(bus=None, max_feature_mints=4)
    real_source.observe(
        CurveProgress(
            t_wall=90.0,
            t_mono=90.0,
            mint="REAL-ASOF-FENCE",
            progress_pct=50.0,
            virtual_sol_reserves=70_000_000_000,
            virtual_token_reserves=70_000_000_000_000,
            real_sol_reserves=10_000_000_000,
            real_token_reserves=400_000_000_000_000,
            source_boot_id=runtime_boot_id,
            source_seq=2,
        )
    )
    real_source.observe(
        CurveProgress(
            t_wall=decision_at + 1.0,
            t_mono=decision_at + 1.0,
            mint="REAL-ASOF-FENCE",
            progress_pct=50.0,
            virtual_sol_reserves=70_000_000_000,
            virtual_token_reserves=70_000_000_000_000,
            real_sol_reserves=-1,
            real_token_reserves=400_000_000_000_000,
            source_boot_id=runtime_boot_id,
            source_seq=1,
        )
    )
    strict_snapshot = real_source.p3_snapshot_at_or_before(
        "REAL-ASOF-FENCE",
        as_of=decision_at,
        durable_source_wall=90.0,
        durable_source_boot_id=runtime_boot_id,
        durable_source_seq=2,
        durable_observed_at=90.5,
        runtime_boot_id=runtime_boot_id,
        runtime_causal_floor=1.0,
    )
    assert strict_snapshot is not None
    assert (strict_snapshot.t_wall, strict_snapshot.source_seq) == (90.0, 2)

    class SnapshotSource:
        def __init__(self):
            self.history = {}
            self.strict_calls = []

        def snapshot_at_or_before(self, mint, *, as_of):
            eligible = [
                snapshot
                for snapshot in self.history.get(mint, ())
                if snapshot.t_wall <= as_of
            ]
            return max(eligible, key=lambda snapshot: snapshot.t_wall, default=None)

        def p3_snapshot_at_or_before(
            self,
            mint,
            *,
            as_of,
            durable_source_wall,
            durable_source_boot_id,
            durable_source_seq,
            durable_observed_at,
            runtime_boot_id,
            runtime_causal_floor,
        ):
            self.strict_calls.append(
                (
                    mint,
                    as_of,
                    durable_source_wall,
                    durable_source_boot_id,
                    durable_source_seq,
                    durable_observed_at,
                    runtime_boot_id,
                    runtime_causal_floor,
                )
            )
            current_boot = [
                snapshot
                for snapshot in self.history.get(mint, ())
                if snapshot.source_boot_id == runtime_boot_id
            ]
            if (
                durable_source_boot_id != runtime_boot_id
                or durable_source_seq is None
                or durable_source_wall is None
                or durable_observed_at is None
                or not runtime_causal_floor < durable_observed_at <= as_of
                or any(
                    snapshot.source_seq > durable_source_seq
                    for snapshot in current_boot
                )
            ):
                return None
            matches = [
                snapshot
                for snapshot in current_boot
                if snapshot.source_seq == durable_source_seq
                and snapshot.t_wall == durable_source_wall
                and snapshot.t_wall <= as_of
            ]
            return matches[0] if len(matches) == 1 else None

    canonical_cfg = {
        "enabled": True,
        "resolver_version": "canonical-v1",
        "weights_version": "canonical-weighted-v1",
        "live_states": ["FRESH", "CLIMBING"],
        "max_cluster_candidates": 10,
        "max_creator_history_mints": 10,
        "max_feature_mints": 100,
        "max_open_p3_positions": 10,
        "liquidity_max_age_s": 10.0,
        "holder_max_age_s": 20.0,
        "comparison_price_max_age_s": 30.0,
        "fill_event_max_age_s": 30.0,
        "reconcile_interval_s": 60.0,
        "w_first_mover": 0.0,
        "w_liquidity": 1.0,
        "w_holder": 0.0,
        "w_creator": 0.0,
        "w_social": 0.0,
        "social_weights": {
            "uri": 0.25,
            "website": 0.25,
            "twitter": 0.25,
            "telegram": 0.25,
        },
    }
    safety_cfg = {
        "top10_holder_max_pct": 30.0,
        "early_buyers": {"buyer_limit": 2},
    }
    pumpfun_cfg = {"graduation_sol": 85.0, "token_decimals": 6}
    source = SnapshotSource()
    conn = open_db(tmp_path / "resolver-freshness.db", migration_clock=lambda: 1.0)
    report_ids = {}

    def evidence_hash(label):
        return hashlib.sha256(label.encode()).hexdigest()

    def add_report(
        mint,
        *,
        checked_at,
        holder_observed_at,
        hard_fails=(),
    ):
        report_id = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
            ") VALUES (?,?,?,?,?)",
            (
                mint,
                checked_at,
                json.dumps(list(hard_fails), separators=(",", ":")),
                5.0,
                evidence_hash(f"report:{mint}:{checked_at}:{hard_fails}"),
            ),
        ).lastrowid
        conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,"
            "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
            "holder_observed_at,unavailable_reason,inputs_hash"
            ") VALUES (?,20,12,20.0,?,'',?)",
            (
                report_id,
                holder_observed_at,
                evidence_hash(f"holder:{report_id}"),
            ),
        )
        conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id) VALUES (?,?,?,'',?,?)",
            (
                mint,
                min(checked_at, decision_at) - 0.25,
                '["buyer-a"]',
                evidence_hash(f"buyers:{report_id}"),
                report_id,
            ),
        )
        conn.commit()
        return report_id

    def snapshot(mint, *, source_seq, source_wall, liquidity_sol):
        return CurveSnapshot(
            source_boot_id=runtime_boot_id,
            source_seq=source_seq,
            t_wall=source_wall,
            t_mono=source_wall,
            virtual_sol_reserves=70_000_000_000,
            virtual_token_reserves=70_000_000_000_000,
            real_sol_reserves=int(liquidity_sol * 1_000_000_000),
            real_token_reserves=400_000_000_000_000,
            liquidity_sol=liquidity_sol,
            spot_price_sol=0.000001,
            progress_pct=min(100.0, 100.0 * liquidity_sol / 85.0),
        )

    def add_token(
        mint,
        *,
        name,
        symbol,
        ingested_at,
        liquidity_sol,
        source_wall,
        holder_observed_at,
        checked_at=99.0,
        later_snapshots=(),
    ):
        upsert_token_identity(
            conn,
            mint=mint,
            raw_ingested_at=ingested_at,
            bonding_curve_key=f"curve-{mint}",
            fields={
                "creator": f"creator-{mint}",
                "name": name,
                "symbol": symbol,
                "uri": "",
                "website": "",
                "twitter": "",
                "telegram": "",
            },
        )
        acknowledged = snapshot(
            mint,
            source_seq=1,
            source_wall=source_wall,
            liquidity_sol=liquidity_sol,
        )
        source.history[mint] = [acknowledged, *later_snapshots]
        conn.execute(
            "UPDATE tokens SET state='CLIMBING',curve_progress=?,"
            "curve_progress_observed_at=?,curve_progress_source_wall=?,"
            "curve_progress_source_boot_id=?,curve_progress_source_seq=?,"
            "curve_progress_virtual_sol_reserves=?,"
            "curve_progress_virtual_token_reserves=?,"
            "curve_progress_real_sol_reserves=?,"
            "curve_progress_real_token_reserves=? WHERE mint=?",
            (
                acknowledged.progress_pct,
                source_wall + 0.5,
                source_wall,
                runtime_boot_id,
                acknowledged.source_seq,
                acknowledged.virtual_sol_reserves,
                acknowledged.virtual_token_reserves,
                acknowledged.real_sol_reserves,
                acknowledged.real_token_reserves,
                mint,
            ),
        )
        conn.commit()
        report_ids[mint] = add_report(
            mint,
            checked_at=checked_at,
            holder_observed_at=holder_observed_at,
        )

    add_token(
        "DELAYED-TARGET",
        name="Delayed",
        symbol="DELAY",
        ingested_at=2.0,
        liquidity_sol=80.0,
        source_wall=90.0,
        holder_observed_at=79.999,
    )
    add_token(
        "DELAYED-WINNER",
        name="Delayed",
        symbol="DELAY",
        ingested_at=3.0,
        liquidity_sol=10.0,
        source_wall=90.0,
        holder_observed_at=80.0,
    )

    future_snapshot = snapshot(
        "FUTURE-PEER",
        source_seq=2,
        source_wall=101.0,
        liquidity_sol=82.0,
    )
    add_token(
        "GOOD-TARGET",
        name="Fresh",
        symbol="FRESH",
        ingested_at=10.0,
        liquidity_sol=10.0,
        source_wall=90.0,
        holder_observed_at=80.0,
    )
    add_token(
        "FUTURE-PEER",
        name="Fresh",
        symbol="FRESH",
        ingested_at=11.0,
        liquidity_sol=82.0,
        source_wall=95.0,
        holder_observed_at=80.0,
        later_snapshots=(future_snapshot,),
    )
    add_token(
        "STALE-PEER",
        name="Fresh",
        symbol="FRESH",
        ingested_at=12.0,
        liquidity_sol=83.0,
        source_wall=89.999,
        holder_observed_at=80.0,
    )
    add_token(
        "COMPARISON-STALE-PEER",
        name="Fresh",
        symbol="FRESH",
        ingested_at=12.5,
        liquidity_sol=83.5,
        source_wall=69.999,
        holder_observed_at=80.0,
    )
    add_token(
        "DELAYED-PEER",
        name="Fresh",
        symbol="FRESH",
        ingested_at=13.0,
        liquidity_sol=84.0,
        source_wall=90.0,
        holder_observed_at=79.999,
    )
    add_token(
        "HARDFAIL-PEER",
        name="Fresh",
        symbol="FRESH",
        ingested_at=14.0,
        liquidity_sol=85.0,
        source_wall=90.0,
        holder_observed_at=80.0,
        checked_at=98.0,
    )
    add_report(
        "HARDFAIL-PEER",
        checked_at=99.0,
        holder_observed_at=80.0,
        hard_fails=("newest_bad",),
    )
    add_token(
        "EQUAL-REPORT-PEER",
        name="Fresh",
        symbol="FRESH",
        ingested_at=15.0,
        liquidity_sol=85.0,
        source_wall=90.0,
        holder_observed_at=80.0,
        checked_at=98.0,
    )
    add_report(
        "EQUAL-REPORT-PEER",
        checked_at=decision_at,
        holder_observed_at=80.0,
    )
    add_token(
        "DIRECT-TARGET",
        name="Direct",
        symbol="DIRECT",
        ingested_at=16.0,
        liquidity_sol=10.0,
        source_wall=90.0,
        holder_observed_at=80.0,
    )

    resolver = CanonicalResolver(
        conn,
        feature_engine=source,
        canonical_cfg=canonical_cfg,
        safety_cfg=safety_cfg,
        pumpfun_cfg=pumpfun_cfg,
        config_hash="c" * 64,
        counterfactual_horizons=(3600.0, 21600.0, 86400.0),
        runtime_boot_id=runtime_boot_id,
        runtime_causal_floor=1.0,
    )
    delayed = resolver.resolve(
        "DELAYED-TARGET",
        decision_at=decision_at,
        target_report_id=report_ids["DELAYED-TARGET"],
    )
    peers = resolver.resolve(
        "GOOD-TARGET",
        decision_at=decision_at,
        target_report_id=report_ids["GOOD-TARGET"],
    )
    repeated_peers = resolver.resolve(
        "GOOD-TARGET",
        decision_at=decision_at,
        target_report_id=report_ids["GOOD-TARGET"],
    )
    assert repeated_peers.verdict.ranking_inputs == peers.verdict.ranking_inputs
    assert repeated_peers.verdict.inputs_hash == peers.verdict.inputs_hash
    assert repeated_peers.verdict.generation_hash == peers.verdict.generation_hash

    direct_call_count = len(source.strict_calls)
    direct_stale = resolver.resolve(
        "DIRECT-TARGET",
        decision_at=decision_at,
        target_report_id=report_ids["DIRECT-TARGET"],
        target_snapshot=snapshot(
            "DIRECT-TARGET",
            source_seq=2,
            source_wall=89.999,
            liquidity_sol=85.0,
        ),
    )
    direct_future = resolver.resolve(
        "DIRECT-TARGET",
        decision_at=decision_at,
        target_report_id=report_ids["DIRECT-TARGET"],
        target_snapshot=snapshot(
            "DIRECT-TARGET",
            source_seq=3,
            source_wall=100.001,
            liquidity_sol=85.0,
        ),
    )
    assert len(source.strict_calls) == direct_call_count
    assert (
        direct_stale.verdict.status,
        direct_stale.verdict.reason,
        direct_stale.verdict.canonical_mint,
        tuple(
            (
                row.mint,
                row.eligible,
                row.start_price_sol,
                row.price_observed_at,
                row.unavailable_reason,
            )
            for row in direct_stale.observations
        ),
    ) == (
        "UNRESOLVED",
        "canonical_liquidity_unavailable",
        None,
        (("DIRECT-TARGET", False, 0.000001, 89.999, ""),),
    )
    assert (
        direct_future.verdict.status,
        direct_future.verdict.reason,
        tuple(
            (
                row.mint,
                row.start_price_sol,
                row.price_observed_at,
                row.unavailable_reason,
            )
            for row in direct_future.observations
        ),
    ) == (
        "UNRESOLVED",
        "canonical_internal_error",
        (("DIRECT-TARGET", None, None, "start_price_malformed"),),
    )
    peer_payloads = {
        candidate["mint"]: candidate
        for candidate in peers.verdict.ranking_inputs["candidates"]
    }

    assert {
        "delayed": (
            delayed.verdict.status,
            delayed.verdict.reason,
            delayed.verdict.canonical_mint,
            tuple(row.mint for row in delayed.observations if row.eligible),
        ),
        "peers": (
            peers.verdict.status,
            peers.verdict.reason,
            peers.verdict.canonical_mint,
            tuple(row.mint for row in peers.observations if row.eligible),
        ),
        "peer_reasons": {
            mint: peer_payloads[mint]["ineligible_reason"]
            for mint in (
                "FUTURE-PEER",
                "STALE-PEER",
                "COMPARISON-STALE-PEER",
                "DELAYED-PEER",
                "HARDFAIL-PEER",
                "EQUAL-REPORT-PEER",
            )
        },
    } == {
        "delayed": (
            "UNRESOLVED",
            "canonical_holder_evidence_unavailable",
            "DELAYED-WINNER",
            ("DELAYED-WINNER",),
        ),
        "peers": (
            "CANONICAL",
            "canonical_selected",
            "GOOD-TARGET",
            ("GOOD-TARGET",),
        ),
        "peer_reasons": {
            "FUTURE-PEER": "canonical_liquidity_unavailable",
            "STALE-PEER": "canonical_liquidity_unavailable",
            "COMPARISON-STALE-PEER": "canonical_liquidity_unavailable",
            "DELAYED-PEER": "canonical_holder_evidence_unavailable",
            "HARDFAIL-PEER": "canonical_safety_hard_fail",
            "EQUAL-REPORT-PEER": "canonical_safety_unavailable",
        },
    }
    comparison_observation = next(
        row for row in peers.observations if row.mint == "COMPARISON-STALE-PEER"
    )
    assert (
        comparison_observation.eligible,
        comparison_observation.start_price_sol,
        comparison_observation.price_observed_at,
        comparison_observation.unavailable_reason,
    ) == (False, None, None, "start_price_stale")

    expected_durable_sources = {
        "DELAYED-TARGET": (90.0, 90.5),
        "DELAYED-WINNER": (90.0, 90.5),
        "GOOD-TARGET": (90.0, 90.5),
        "FUTURE-PEER": (95.0, 95.5),
        "STALE-PEER": (89.999, 90.499),
        "COMPARISON-STALE-PEER": (69.999, 70.499),
        "DELAYED-PEER": (90.0, 90.5),
        "HARDFAIL-PEER": (90.0, 90.5),
        "EQUAL-REPORT-PEER": (90.0, 90.5),
    }
    assert {call[0] for call in source.strict_calls} == set(
        expected_durable_sources
    )
    for (
        called_mint,
        called_as_of,
        called_source_wall,
        called_source_boot,
        called_source_seq,
        called_observed_at,
        called_runtime_boot,
        called_runtime_floor,
    ) in source.strict_calls:
        expected_source_wall, expected_observed_at = expected_durable_sources[
            called_mint
        ]
        assert (
            called_as_of,
            called_source_wall,
            called_source_boot,
            called_source_seq,
            called_observed_at,
            called_runtime_boot,
            called_runtime_floor,
        ) == (
            decision_at,
            expected_source_wall,
            runtime_boot_id,
            1,
            expected_observed_at,
            runtime_boot_id,
            1.0,
        )

    class GenericOnlySnapshotSource:
        def __init__(self):
            self.calls = 0

        def snapshot_at_or_before(self, mint, *, as_of):
            self.calls += 1
            return source.snapshot_at_or_before(mint, as_of=as_of)

    generic_only = GenericOnlySnapshotSource()
    nonconforming = CanonicalResolver(
        conn,
        feature_engine=generic_only,
        canonical_cfg=canonical_cfg,
        safety_cfg=safety_cfg,
        pumpfun_cfg=pumpfun_cfg,
        config_hash="c" * 64,
        counterfactual_horizons=(3600.0, 21600.0, 86400.0),
        runtime_boot_id=runtime_boot_id,
        runtime_causal_floor=1.0,
    ).resolve(
        "GOOD-TARGET",
        decision_at=decision_at,
        target_report_id=report_ids["GOOD-TARGET"],
    )
    assert (
        nonconforming.verdict.status,
        nonconforming.verdict.reason,
        nonconforming.verdict.canonical_mint,
        tuple(row.mint for row in nonconforming.observations if row.eligible),
        generic_only.calls,
    ) == (
        "UNRESOLVED",
        "canonical_internal_error",
        None,
        (),
        0,
    )
    conn.close()
