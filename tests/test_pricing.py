import pytest

from memebot.ingest.curve import (CurveState, buy_quote, sell_quote,
                                   spot_price_sol_per_token)

# pump.fun launch reserves: 30 vSOL, 1.073e9 tokens (config: initial_v_sol / initial_v_tokens)
def _fresh_state() -> CurveState:
    return CurveState(
        virtual_token_reserves=1_073_000_000 * 10**6,  # base units (6 decimals)
        virtual_sol_reserves=30 * 10**9,               # lamports
        real_token_reserves=793_100_000 * 10**6,
        real_sol_reserves=0,
        token_total_supply=1_000_000_000 * 10**6,
        complete=False,
    )


def test_spot_price_matches_reserve_ratio():
    s = _fresh_state()
    # price = (vSOL / 1e9) / (vTok / 1e6) = 30 / 1.073e9
    assert spot_price_sol_per_token(s) == pytest.approx(30.0 / 1_073_000_000)


def test_buy_quote_avg_price_is_worse_than_spot_and_conserves_k():
    s = _fresh_state()
    spot = spot_price_sol_per_token(s)
    tokens_out, avg_price = buy_quote(s, sol_in=1.0)
    assert tokens_out > 0
    assert avg_price > spot                       # buying moves price up (impact >= 0)
    assert avg_price == pytest.approx(1.0 / tokens_out)  # avg price = sol_in / tokens_out


def test_buy_quote_tiny_size_approaches_spot():
    s = _fresh_state()
    spot = spot_price_sol_per_token(s)
    _, avg_price = buy_quote(s, sol_in=1e-6)
    assert avg_price == pytest.approx(spot, rel=1e-4)


def test_sell_quote_avg_price_is_worse_than_spot():
    s = _fresh_state()
    spot = spot_price_sol_per_token(s)
    sol_out, avg_price = sell_quote(s, tokens_in=1_000_000.0)
    assert sol_out > 0
    assert avg_price < spot                       # selling moves price down
    assert avg_price == pytest.approx(sol_out / 1_000_000.0)


def test_buy_then_sell_same_size_loses_to_impact():
    # round-trip with no fees still loses SOL to price impact (constant product)
    s = _fresh_state()
    tokens_out, _ = buy_quote(s, sol_in=1.0)
    sol_back, _ = sell_quote(s, tokens_in=tokens_out)
    assert sol_back < 1.0
