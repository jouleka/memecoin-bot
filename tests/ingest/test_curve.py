import json
from pathlib import Path

import pytest

from memebot.ingest.curve import CurveState, decode_curve_account, progress_pct

FIXTURE = Path("tests/fixtures/providers/helius/curve_accounts.json")


def test_decodes_all_fixture_accounts():
    accounts = json.loads(FIXTURE.read_text())
    assert len(accounts) >= 5
    for a in accounts:
        st = decode_curve_account(a["data_b64"])
        assert isinstance(st, CurveState)
        assert 0 <= st.real_token_reserves <= st.token_total_supply
        # A fully-graduated curve legitimately zeroes its reserves (complete=True);
        # every still-active curve must show positive virtual reserves.
        assert st.virtual_token_reserves > 0 or st.complete
        assert isinstance(st.complete, bool)


def test_progress_math():
    st = CurveState(virtual_token_reserves=1, virtual_sol_reserves=1,
                    real_token_reserves=793_100_000 * 10**6 // 2,  # half sold (6 decimals)
                    real_sol_reserves=0, token_total_supply=10**15, complete=False)
    p = progress_pct(st, sellable_supply_tokens=793_100_000, token_decimals=6)
    assert 49.0 < p < 51.0


def test_progress_clamps():
    st = CurveState(1, 1, 0, 0, 10**15, True)  # fully depleted
    assert progress_pct(st, sellable_supply_tokens=793_100_000, token_decimals=6) == 100.0


def test_bad_discriminator_raises():
    with pytest.raises(ValueError, match="discriminator"):
        decode_curve_account("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")


def test_progress_zero_supply_raises():
    st = CurveState(1, 1, 0, 0, 10**15, False)
    with pytest.raises(ValueError):
        progress_pct(st, sellable_supply_tokens=0)
