from memebot.safety.checks import (
    HolderEvidenceDraft, derive_holder_evidence, dev_wallet_check,
    early_buyer_concentration_check, freeze_authority_check,
    holder_concentration_check, mint_authority_check,
)
from memebot.safety.rpc import Holder, MintInfo


def info(mint_auth=None, freeze_auth=None, supply=1_000_000_000_000_000):
    return MintInfo(mint_authority=mint_auth, freeze_authority=freeze_auth,
                    supply=supply, decimals=6)


def test_mint_authority_pass_when_revoked():
    r = mint_authority_check(info(mint_auth=None))
    assert r.passed and r.hard and r.name == "mint_authority"


def test_mint_authority_hardfail_when_present():
    r = mint_authority_check(info(mint_auth="KEY"))
    assert not r.passed and r.hard and r.reason == "mint_authority_active"


def test_freeze_authority_hardfail_when_present():
    assert not freeze_authority_check(info(freeze_auth="F")).passed
    assert freeze_authority_check(info(freeze_auth=None)).passed


def test_holder_concentration_excludes_known_addresses():
    supply = 1000
    holders = [Holder("CURVE", 600), Holder("A", 200), Holder("B", 100), Holder("C", 100)]
    # exclude CURVE (bonding curve) -> top-10 of the rest = 400/1000 = 40% > 30 -> hard-fail
    r = holder_concentration_check(holders, supply=supply, exclude={"CURVE"}, max_pct=30.0)
    assert not r.passed and r.hard and r.reason == "holder_concentration"
    assert abs(r.detail["top10_share_pct"] - 40.0) < 0.01


def test_holder_concentration_pass_under_threshold():
    holders = [Holder("A", 100), Holder("B", 100)]
    r = holder_concentration_check(holders, supply=1000, exclude=set(), max_pct=30.0)
    assert r.passed and abs(r.detail["top10_share_pct"] - 20.0) < 0.01


def test_holder_concentration_zero_supply_is_hard_fail_not_zerodiv():
    # supply=0 is unknowable/degenerate -> must fail-closed (100% share), never raise.
    holders = [Holder("A", 100)]
    r = holder_concentration_check(holders, supply=0, exclude=set(), max_pct=30.0)
    assert not r.passed and r.hard and r.reason == "holder_concentration"
    assert abs(r.detail["top10_share_pct"] - 100.0) < 0.01


def test_dev_wallet_check():
    # holders are token-ACCOUNT addresses; the owner map ties DEV_ATA back to wallet "DEV".
    holders = [Holder("DEV_ATA", 150), Holder("A_ATA", 50)]
    owners = {"DEV_ATA": "DEV", "A_ATA": "A"}
    r = dev_wallet_check(holders, owners=owners, supply=1000, dev="DEV", max_pct=10.0)
    # dev holds 150/1000 = 15% > 10% -> flags, but SOFT (raises risk score, never a hard block)
    assert not r.passed and not r.hard and abs(r.detail["dev_share_pct"] - 15.0) < 0.01
    # a wallet that owns none of the top accounts holds 0 -> passes
    assert dev_wallet_check(holders, owners=owners, supply=1000, dev="NOBODY", max_pct=10.0).passed


def test_early_buyer_concentration_uses_owner_wallets_and_hardfails_over_threshold():
    holders = [Holder("A_ATA", 250), Holder("B_ATA", 50), Holder("OTHER_ATA", 700)]
    owners = {"A_ATA": "EARLY_A", "B_ATA": "EARLY_B", "OTHER_ATA": "OTHER"}
    r = early_buyer_concentration_check(
        holders, owners=owners, supply=1000, early_buyers=("EARLY_A", "EARLY_B"),
        max_pct=25.0)
    assert not r.passed and r.hard and r.reason == "early_buyer_concentration"
    assert r.detail["early_buyer_share_pct"] == 30.0


def test_early_buyer_concentration_passes_under_threshold():
    holders = [Holder("A_ATA", 100), Holder("OTHER_ATA", 900)]
    owners = {"A_ATA": "EARLY_A", "OTHER_ATA": "OTHER"}
    r = early_buyer_concentration_check(
        holders, owners=owners, supply=1000, early_buyers=("EARLY_A",), max_pct=25.0)
    assert r.passed and r.detail["early_buyer_share_pct"] == 10.0


def test_early_buyer_concentration_empty_buyers_is_unavailable():
    r = early_buyer_concentration_check(
        [Holder("A", 100)], owners={"A": "W"}, supply=1000, early_buyers=(), max_pct=25.0)
    assert not r.passed and r.hard and not r.available
    assert r.reason == "check_unavailable"


def test_holder_evidence_groups_owners_and_excludes_curve():
    holders = [
        Holder("z_curve", 500),
        Holder("c_alice", 100),
        Holder("a_alice", 100),
        Holder("b_alice", 100),
        *(Holder(f"peer_{index}", 50) for index in range(10)),
    ]
    owners = {
        "z_curve": "CURVE_OWNER",
        "a_alice": "ALICE",
        "b_alice": "ALICE",
        "c_alice": "ALICE",
        **{f"peer_{index}": f"OWNER_{index}" for index in range(10)},
    }

    draft = derive_holder_evidence(
        mint="RawMint",
        holders=holders,
        owners=owners,
        supply=2_000,
        curve_owner="CURVE_OWNER",
        holder_observed_at=123.5,
    )
    reordered = derive_holder_evidence(
        mint="RawMint",
        holders=list(reversed(holders)),
        owners=dict(reversed(tuple(owners.items()))),
        supply=2_000,
        curve_owner="CURVE_OWNER",
        holder_observed_at=123.5,
    )

    assert draft == HolderEvidenceDraft(
        sampled_token_accounts=14,
        distinct_non_curve_owners=11,
        top10_non_curve_owner_share_pct=37.5,
        holder_observed_at=123.5,
        unavailable_reason="",
        inputs_hash="87265185c210c057e69ddcc68baa08b6c7bcce7c9f93a43c1160bb1f4daf1e33",
    )
    assert reordered == draft


def test_holder_evidence_unavailable_reason_matrix():
    holders = [Holder("curve_ata", 500), Holder("alice_ata", 100)]
    owners = {"curve_ata": "CURVE", "alice_ata": "ALICE"}
    defaults = {
        "mint": "RawMint",
        "holders": holders,
        "owners": owners,
        "supply": 1_000,
        "curve_owner": "CURVE",
        "holder_observed_at": 12.5,
    }

    def derive(**overrides):
        return derive_holder_evidence(**(defaults | overrides))

    cases = [
        (
            {"supply": None},
            "holder_mint_supply_unavailable",
            "0a6b7051b9da09e3e2b620ea4be0f0d0084014883db22dad6648ee0ffc8d53cb",
        ),
        (
            {"holders": None, "owners": None, "curve_owner": ""},
            "holder_accounts_unavailable",
            "d415494728beeb47e0edbac859d3dce83078cfe646ff2ce73a822b90e7b6f7d4",
        ),
        (
            {"holders": [], "owners": None, "curve_owner": ""},
            "holder_accounts_empty",
            "8d790a6525342846325a08a4cfb872aae54986498e29904d6b6d27412857cff9",
        ),
        (
            {"owners": None, "curve_owner": ""},
            "holder_owner_resolution_unavailable",
            "c4f4ae1949b194456cdc8aeb4066de11b5a1fd639344166194f63d6cd5c2a6c2",
        ),
        (
            {"owners": {"curve_ata": "CURVE"}, "curve_owner": ""},
            "holder_owner_resolution_incomplete",
            "3cc1721a69caaa7313d2ee035db3b3cc74d55d382beea0b308fe3a4b76c37179",
        ),
        (
            {"curve_owner": ""},
            "holder_curve_owner_unavailable",
            "c9790a95d35e9458d9b63069aa0a0b853a3cf2be22225c2ea083cc558b31389c",
        ),
        (
            {"curve_owner": b"CURVE"},
            "holder_curve_owner_unavailable",
            "fb27d48089fff8918771d253fa1a4e4251ace8618b897998b63916d6b72f8995",
        ),
        (
            {"supply": None, "curve_owner": b"CURVE"},
            "holder_mint_supply_unavailable",
            "3537a93f5f07f4b7b67d584fb630376613d467d51e6cc5a4697d9a5813f55bcb",
        ),
        (
            {
                "holders": [Holder("curve_ata", 500)],
                "owners": {"curve_ata": "CURVE"},
            },
            "holder_non_curve_owners_empty",
            "ec696fdd4fc0673874892b8657cbabc38e507f472ef33fda57e04534245c9889",
        ),
    ]
    for overrides, reason, inputs_hash in cases:
        draft = derive(**overrides)
        assert draft == HolderEvidenceDraft(
            sampled_token_accounts=None,
            distinct_non_curve_owners=None,
            top10_non_curve_owner_share_pct=None,
            holder_observed_at=12.5,
            unavailable_reason=reason,
            inputs_hash=inputs_hash,
        )

    malformed_accounts = [
        [object()],
        [Holder("", 1)],
        [Holder(" ", 1)],
        [Holder("dup", 1), Holder("dup", 2)],
        [Holder("negative", -1)],
        [Holder("float", 1.5)],
        [Holder("boolean", True)],
        [Holder("alice_ata", 1_001)],
        [Holder("curve_ata", 600), Holder("alice_ata", 500)],
    ]
    for malformed in malformed_accounts:
        assert derive(holders=malformed) == HolderEvidenceDraft(
            sampled_token_accounts=None,
            distinct_non_curve_owners=None,
            top10_non_curve_owner_share_pct=None,
            holder_observed_at=12.5,
            unavailable_reason="holder_accounts_unavailable",
            inputs_hash="52863081bee60e13ba3da10d0439c4ecf794f939e58092a4096c6fd808d75912",
        )

    for bad_supply in (None, 0, -1, True, 1.5):
        assert derive(supply=bad_supply) == HolderEvidenceDraft(
            sampled_token_accounts=None,
            distinct_non_curve_owners=None,
            top10_non_curve_owner_share_pct=None,
            holder_observed_at=12.5,
            unavailable_reason="holder_mint_supply_unavailable",
            inputs_hash="0a6b7051b9da09e3e2b620ea4be0f0d0084014883db22dad6648ee0ffc8d53cb",
        )

    for incomplete_owners in (
        {"curve_ata": "CURVE"},
        {"curve_ata": "CURVE", "alice_ata": ""},
        {"curve_ata": "CURVE", "alice_ata": " "},
        {"curve_ata": "CURVE", "alice_ata": 7},
    ):
        assert derive(owners=incomplete_owners) == HolderEvidenceDraft(
            sampled_token_accounts=None,
            distinct_non_curve_owners=None,
            top10_non_curve_owner_share_pct=None,
            holder_observed_at=12.5,
            unavailable_reason="holder_owner_resolution_incomplete",
            inputs_hash="a9fc54c0118c86bb0bacfda347015f80706bc127afc52f1f5cdb48e17959c07f",
        )

    retained_inputs = derive(supply=None)
    for changed in (
        {"mint": "OtherRawMint"},
        {"holders": [Holder("curve_ata", 500), Holder("alice_ata", 101)]},
        {"owners": {"curve_ata": "CURVE", "alice_ata": "OTHER_OWNER"}},
        {"curve_owner": "OTHER_CURVE"},
        {"holder_observed_at": 12.75},
    ):
        changed_draft = derive(supply=None, **changed)
        assert changed_draft.unavailable_reason == "holder_mint_supply_unavailable"
        assert changed_draft.inputs_hash != retained_inputs.inputs_hash
