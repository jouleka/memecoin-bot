"""Pure pump.fun Anchor event decoding used by P2 early-buyer reads.

The layout is validated in `tests/fixtures/providers/pumpfun/NOTES.md`: pump.fun emits
Anchor `Program data:` logs containing an 8-byte discriminator followed by borsh fields.
Only the stable TradeEvent prefix is decoded here. Malformed or non-TradeEvent lines return
None so callers can make the fail-closed policy decision at the gate/reader layer.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import struct
from dataclasses import dataclass

_PROGRAM_DATA_PREFIX = "Program data: "
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _event_discriminator(name: str) -> bytes:
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


DISC_TRADE = _event_discriminator("TradeEvent")


def _b58encode(raw: bytes) -> str:
    num = int.from_bytes(raw, "big")
    encoded = ""
    while num > 0:
        num, rem = divmod(num, 58)
        encoded = _B58_ALPHABET[rem] + encoded
    n_pad = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * n_pad + encoded


class _Cursor:
    def __init__(self, buf: bytes, offset: int = 0) -> None:
        self.buf = buf
        self.offset = offset

    def u8(self) -> int:
        (v,) = struct.unpack_from("<B", self.buf, self.offset)
        self.offset += 1
        return v

    def u64(self) -> int:
        (v,) = struct.unpack_from("<Q", self.buf, self.offset)
        self.offset += 8
        return v

    def i64(self) -> int:
        (v,) = struct.unpack_from("<q", self.buf, self.offset)
        self.offset += 8
        return v

    def bool(self) -> bool:
        return self.u8() != 0

    def pubkey(self) -> str:
        raw = self.buf[self.offset:self.offset + 32]
        if len(raw) != 32:
            raise struct.error("short pubkey")
        self.offset += 32
        return _b58encode(raw)


@dataclass(frozen=True, slots=True)
class PumpfunTradeEvent:
    mint: str
    sol_amount: int
    token_amount: int
    is_buy: bool
    user: str
    timestamp: int
    virtual_sol_reserves: int
    virtual_token_reserves: int
    real_sol_reserves: int
    real_token_reserves: int
    creator: str


def _decode_trade_payload(payload: bytes) -> PumpfunTradeEvent | None:
    if len(payload) < 8 or payload[:8] != DISC_TRADE:
        return None
    try:
        c = _Cursor(payload, 8)
        mint = c.pubkey()
        sol_amount = c.u64()
        token_amount = c.u64()
        is_buy = c.bool()
        user = c.pubkey()
        timestamp = c.i64()
        virtual_sol_reserves = c.u64()
        virtual_token_reserves = c.u64()
        real_sol_reserves = c.u64()
        real_token_reserves = c.u64()
        _fee_recipient = c.pubkey()
        _fee_basis_points = c.u64()
        _fee = c.u64()
        creator = c.pubkey()
        _creator_fee_basis_points = c.u64()
        _creator_fee = c.u64()
    except (struct.error, ValueError, IndexError):
        return None
    return PumpfunTradeEvent(
        mint=mint,
        sol_amount=sol_amount,
        token_amount=token_amount,
        is_buy=is_buy,
        user=user,
        timestamp=timestamp,
        virtual_sol_reserves=virtual_sol_reserves,
        virtual_token_reserves=virtual_token_reserves,
        real_sol_reserves=real_sol_reserves,
        real_token_reserves=real_token_reserves,
        creator=creator,
    )


def decode_trade_event_from_program_data(line: str) -> PumpfunTradeEvent | None:
    if not line.startswith(_PROGRAM_DATA_PREFIX):
        return None
    try:
        payload = base64.b64decode(line[len(_PROGRAM_DATA_PREFIX):], validate=True)
    except (binascii.Error, ValueError):
        return None
    return _decode_trade_payload(payload)


def trade_events_from_logs(logs: list[str]) -> list[PumpfunTradeEvent]:
    events: list[PumpfunTradeEvent] = []
    for line in logs:
        event = decode_trade_event_from_program_data(line)
        if event is not None:
            events.append(event)
    return events
