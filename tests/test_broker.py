import pytest

from memebot.broker import PaperBroker
from memebot.ingest.curve import CurveState

FILL_CFG = {"latency_min_s": 3.0, "extra_slippage_bps": 50, "priority_fee_sol": 0.0005,
            "solana_base_fee_sol": 0.000005, "grade_a_max_impact_pct": 2.0,
            "grade_b_max_impact_pct": 5.0, "grade_c_max_impact_pct": 10.0}
PUMP_CFG = {"protocol_fee_bps": 95, "creator_fee_bps": 30, "token_decimals": 6}


def _state(vsol_lamports, vtok_base):
    return CurveState(virtual_token_reserves=vtok_base, virtual_sol_reserves=vsol_lamports,
                      real_token_reserves=0, real_sol_reserves=0, token_total_supply=0,
                      complete=False)


def test_buy_fill_price_never_better_than_quote_even_if_price_dropped():
    # fill_state cheaper than decision_state (price fell in the latency gap) -> clamp to quote
    broker = PaperBroker(FILL_CFG, PUMP_CFG)
    decision = _state(31_000_000_000, 900_000_000_000_000)
    cheaper = _state(30_000_000_000, 950_000_000_000_000)   # lower price
    fill = broker.buy(decision, cheaper, sol_in=0.2)
    assert fill.fill_price >= fill.quote_price      # invariant: never better than quote


def test_buy_fill_worse_when_price_rose():
    broker = PaperBroker(FILL_CFG, PUMP_CFG)
    decision = _state(31_000_000_000, 900_000_000_000_000)
    pricier = _state(35_000_000_000, 800_000_000_000_000)   # higher price
    fill = broker.buy(decision, pricier, sol_in=0.2)
    assert fill.fill_price > fill.quote_price
    assert fill.qty > 0
    assert fill.fees["protocol_sol"] == pytest.approx(0.2 * 95 / 10000)
    assert fill.sol_notional > 0.2                  # notional includes fees


def test_sell_fill_price_never_better_than_quote():
    broker = PaperBroker(FILL_CFG, PUMP_CFG)
    decision = _state(31_000_000_000, 900_000_000_000_000)
    pricier = _state(35_000_000_000, 800_000_000_000_000)   # selling into a higher price is "better"
    fill = broker.sell(decision, pricier, tokens_in=1_000_000.0)
    assert fill.fill_price <= fill.quote_price      # invariant: sell never better than quote


def test_grade_by_impact():
    assert PaperBroker.grade_for_impact(1.0, FILL_CFG) == "A"
    assert PaperBroker.grade_for_impact(4.0, FILL_CFG) == "B"
    assert PaperBroker.grade_for_impact(8.0, FILL_CFG) == "C"
    assert PaperBroker.grade_for_impact(25.0, FILL_CFG) == "F"
