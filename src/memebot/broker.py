"""PaperBroker + fill model (spec §5.6 — the honesty core). Curve-math quote at decision
time; realistic fill from a LATER snapshot (>= T seconds) + extra slippage + fees; graded
A/B/C/F by price impact. Invariant (property-tested): a paper fill is never better than its
decision-time quote (buy price >= quote; sell price <= quote), so paper never flatters
itself when the market happens to move favorably in the latency gap."""
from __future__ import annotations

from dataclasses import dataclass

from memebot.ingest.curve import (CurveState, buy_quote, sell_quote,
                                   spot_price_sol_per_token)


@dataclass(frozen=True, slots=True)
class Fill:
    side: str
    qty: float
    quote_price: float
    fill_price: float
    fees: dict
    realism_grade: str
    sol_notional: float


class PaperBroker:
    def __init__(self, fill_cfg: dict, pumpfun_cfg: dict) -> None:
        self._f = fill_cfg
        self._p = pumpfun_cfg

    @staticmethod
    def grade_for_impact(impact_pct: float, cfg: dict) -> str:
        if impact_pct <= cfg["grade_a_max_impact_pct"]:
            return "A"
        if impact_pct <= cfg["grade_b_max_impact_pct"]:
            return "B"
        if impact_pct <= cfg["grade_c_max_impact_pct"]:
            return "C"
        return "F"

    def _fee_sol(self, sol_amount: float) -> dict:
        protocol = sol_amount * self._p["protocol_fee_bps"] / 10000
        creator = sol_amount * self._p["creator_fee_bps"] / 10000
        return {"protocol_sol": protocol, "creator_sol": creator,
                "priority_sol": self._f["priority_fee_sol"],
                "base_sol": self._f["solana_base_fee_sol"]}

    def buy(self, decision_state: CurveState, fill_state: CurveState, *, sol_in: float) -> Fill:
        dec = self._p["token_decimals"]
        _, quote_price = buy_quote(decision_state, sol_in, token_decimals=dec)
        _, raw_fill_price = buy_quote(fill_state, sol_in, token_decimals=dec)
        slipped = raw_fill_price * (1 + self._f["extra_slippage_bps"] / 10000)
        fill_price = max(slipped, quote_price)               # never better than quote
        qty = sol_in / fill_price
        spot = spot_price_sol_per_token(fill_state, token_decimals=dec)
        impact = 100.0 * (fill_price - spot) / spot
        fees = self._fee_sol(sol_in)
        notional = sol_in + sum(fees.values())
        return Fill(side="buy", qty=qty, quote_price=quote_price, fill_price=fill_price,
                    fees=fees, realism_grade=self.grade_for_impact(impact, self._f),
                    sol_notional=notional)

    def sell(self, decision_state: CurveState, fill_state: CurveState, *, tokens_in: float) -> Fill:
        dec = self._p["token_decimals"]
        _, quote_price = sell_quote(decision_state, tokens_in, token_decimals=dec)
        _, raw_fill_price = sell_quote(fill_state, tokens_in, token_decimals=dec)
        slipped = raw_fill_price * (1 - self._f["extra_slippage_bps"] / 10000)
        fill_price = min(slipped, quote_price)               # never better than quote
        sol_out_gross = tokens_in * fill_price
        spot = spot_price_sol_per_token(fill_state, token_decimals=dec)
        impact = 100.0 * (spot - fill_price) / spot
        fees = self._fee_sol(sol_out_gross)
        notional = sol_out_gross - sum(fees.values())
        return Fill(side="sell", qty=tokens_in, quote_price=quote_price, fill_price=fill_price,
                    fees=fees, realism_grade=self.grade_for_impact(impact, self._f),
                    sol_notional=notional)
