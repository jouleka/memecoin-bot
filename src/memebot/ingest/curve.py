"""pump.fun BondingCurve account decode + curve-progress math (pure; M2 delta 1).

Anchor account layout (verified against the fixture in the C3 capture):
8-byte discriminator sha256("account:BondingCurve")[:8], then borsh:
u64 virtual_token_reserves, u64 virtual_sol_reserves, u64 real_token_reserves,
u64 real_sol_reserves, u64 token_total_supply, bool complete. Trailing bytes
(creator pubkey in newer curves) are ignored.
"""
from __future__ import annotations

import base64
import hashlib
import struct
from dataclasses import dataclass

_DISCRIMINATOR = hashlib.sha256(b"account:BondingCurve").digest()[:8]
_LAYOUT = struct.Struct("<QQQQQ?")


@dataclass(frozen=True, slots=True)
class CurveState:
    virtual_token_reserves: int
    virtual_sol_reserves: int
    real_token_reserves: int
    real_sol_reserves: int
    token_total_supply: int
    complete: bool


def decode_curve_account(data_b64: str) -> CurveState:
    raw = base64.b64decode(data_b64)
    if raw[:8] != _DISCRIMINATOR:
        raise ValueError(f"not a BondingCurve account (bad discriminator: {raw[:8].hex()})")
    fields = _LAYOUT.unpack_from(raw, 8)
    return CurveState(*fields)


def progress_pct(state: CurveState, *, sellable_supply_tokens: float,
                 token_decimals: int = 6) -> float:
    """% of the sellable supply sold off the curve. 100.0 once complete."""
    if state.complete:
        return 100.0
    if sellable_supply_tokens <= 0:
        raise ValueError("sellable_supply_tokens must be positive")
    initial = sellable_supply_tokens * (10 ** token_decimals)
    sold = max(0.0, initial - state.real_token_reserves)
    return max(0.0, min(100.0, 100.0 * sold / initial))


LAMPORTS_PER_SOL = 1_000_000_000


def spot_price_sol_per_token(state: CurveState, *, token_decimals: int = 6) -> float:
    """Marginal price (SOL per whole token) from the virtual reserves."""
    sol = state.virtual_sol_reserves / LAMPORTS_PER_SOL
    tokens = state.virtual_token_reserves / (10 ** token_decimals)
    return sol / tokens


def buy_quote(state: CurveState, sol_in: float, *,
              token_decimals: int = 6) -> tuple[float, float]:
    """Constant-product buy: spend `sol_in` SOL, receive tokens. Returns
    (tokens_out, avg_price_sol_per_token). Fees are applied by the caller."""
    if sol_in <= 0:
        raise ValueError("sol_in must be positive")
    sol = state.virtual_sol_reserves / LAMPORTS_PER_SOL
    tokens = state.virtual_token_reserves / (10 ** token_decimals)
    k = sol * tokens
    new_tokens = k / (sol + sol_in)
    tokens_out = tokens - new_tokens
    return tokens_out, sol_in / tokens_out


def sell_quote(state: CurveState, tokens_in: float, *,
               token_decimals: int = 6) -> tuple[float, float]:
    """Constant-product sell: sell `tokens_in` whole tokens, receive SOL. Returns
    (sol_out, avg_price_sol_per_token). Fees are applied by the caller."""
    if tokens_in <= 0:
        raise ValueError("tokens_in must be positive")
    sol = state.virtual_sol_reserves / LAMPORTS_PER_SOL
    tokens = state.virtual_token_reserves / (10 ** token_decimals)
    k = sol * tokens
    new_sol = k / (tokens + tokens_in)
    sol_out = sol - new_sol
    return sol_out, sol_out / tokens_in
