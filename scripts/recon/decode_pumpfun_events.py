"""Offline decoder for pump.fun Anchor `Program data:` event blobs (Task A6 / MB-2).

Decodes the base64 `Program data: ...` lines emitted by the pump.fun program
(`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`) in Solana transaction logs,
without any extra RPC call. This settles the M2 open question from Task A4:
whether trade size/price/mint are recoverable from `logsSubscribe` output
alone (yes) or require a per-trade `getTransaction` call (no, not needed).

Layout source: the official Anchor IDL published at
https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json
(fetched to /tmp/pump_idl.json during this recon pass; not vendored here to
avoid drift — re-fetch if the layout ever needs re-validation).

Anchor event-log encoding, confirmed against our fixture data in this pass:
  1. The program CPIs into itself (`self_cpi_log` in newer Anchor) which is
     why every `Program data:` line is preceded by a nested
     "Program 6EF8rr... invoke [N+1]" / "... success" pair in the logs.
  2. The base64 payload is: 8-byte discriminator + borsh-serialized event
     struct fields, in IDL declaration order.
  3. Discriminator = first 8 bytes of sha256(f"event:{EventName}") — this is
     the standard Anchor convention (independently verified: sha256
     digests computed in this recon match the IDL's own `discriminator`
     array for every event we checked, including CreateEvent and
     TradeEvent).

Only stdlib (base64, struct, hashlib) is used — no anchorpy/borsh dependency.

Usage:
  ./.venv/bin/python scripts/recon/decode_pumpfun_events.py <path-to-jsonl>

Input format expected: one JSON object per line, `{"t_wall": ..., "raw":
"<json-encoded RPC notification>"}` (the shape produced by
capture_helius.py's `logs` subcommand). Prints one decoded event per
recognized `Program data:` line found in the frame's `logs` array, plus a
summary of discriminators seen and any undecodable blobs.
"""
from __future__ import annotations

import base64
import hashlib
import json
import struct
import sys
from dataclasses import dataclass

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# --- Anchor discriminators (sha256(f"event:{Name}")[:8]) --------------------
# Cross-checked against the pump.fun IDL's own `discriminator` field for each
# event during this recon pass -- both derivations agree byte-for-byte.


def _event_discriminator(name: str) -> bytes:
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


DISC_TRADE = _event_discriminator("TradeEvent")
DISC_CREATE = _event_discriminator("CreateEvent")
DISC_COMPLETE = _event_discriminator("CompleteEvent")
DISC_COMPLETE_MIGRATION = _event_discriminator("CompletePumpAmmMigrationEvent")

KNOWN_DISCRIMINATORS = {
    DISC_TRADE: "TradeEvent",
    DISC_CREATE: "CreateEvent",
    DISC_COMPLETE: "CompleteEvent",
    DISC_COMPLETE_MIGRATION: "CompletePumpAmmMigrationEvent",
}


class Cursor:
    """Tiny borsh-ish reader over a bytes buffer (little-endian, as Solana/Anchor use)."""

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
        self.offset += 32
        # base58-encode without extra deps (stdlib-only constraint)
        return _b58encode(raw)

    def string(self) -> str:
        (length,) = struct.unpack_from("<I", self.buf, self.offset)
        self.offset += 4
        s = self.buf[self.offset:self.offset + length].decode("utf-8", errors="replace")
        self.offset += length
        return s

    def remaining(self) -> int:
        return len(self.buf) - self.offset


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(raw: bytes) -> str:
    num = int.from_bytes(raw, "big")
    encoded = ""
    while num > 0:
        num, rem = divmod(num, 58)
        encoded = _B58_ALPHABET[rem] + encoded
    n_pad = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * n_pad + encoded


@dataclass
class TradeEvent:
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
    fee_recipient: str
    fee_basis_points: int
    fee: int
    creator: str
    creator_fee_basis_points: int
    creator_fee: int
    # NOTE: the live/current IDL's TradeEvent has many more trailing fields
    # (track_volume, total_unclaimed_tokens, ..., ix_name, mayhem_mode,
    # cashback/buyback fields, shareholders vec, quote_mint, quote_amount,
    # virtual_quote_reserves, real_quote_reserves) added by later program
    # upgrades. Our captured fixture predates some of these -- see NOTES.md
    # "version skew" section. We decode only the stable, always-present
    # prefix (through creator_fee) which is what's actually validated below;
    # anything after that is read speculatively and reported but not
    # asserted on.
    trailing_raw_len: int = 0


@dataclass
class CreateEvent:
    name: str
    symbol: str
    uri: str
    mint: str
    bonding_curve: str
    user: str
    creator: str
    timestamp: int
    virtual_token_reserves: int
    virtual_sol_reserves: int
    real_token_reserves: int
    token_total_supply: int


def decode_trade_event(payload: bytes) -> TradeEvent:
    c = Cursor(payload, 8)  # skip discriminator
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
    fee_recipient = c.pubkey()
    fee_basis_points = c.u64()
    fee = c.u64()
    creator = c.pubkey()
    creator_fee_basis_points = c.u64()
    creator_fee = c.u64()
    return TradeEvent(
        mint=mint, sol_amount=sol_amount, token_amount=token_amount, is_buy=is_buy,
        user=user, timestamp=timestamp, virtual_sol_reserves=virtual_sol_reserves,
        virtual_token_reserves=virtual_token_reserves, real_sol_reserves=real_sol_reserves,
        real_token_reserves=real_token_reserves, fee_recipient=fee_recipient,
        fee_basis_points=fee_basis_points, fee=fee, creator=creator,
        creator_fee_basis_points=creator_fee_basis_points, creator_fee=creator_fee,
        trailing_raw_len=c.remaining(),
    )


def decode_create_event(payload: bytes) -> CreateEvent:
    c = Cursor(payload, 8)  # skip discriminator
    name = c.string()
    symbol = c.string()
    uri = c.string()
    mint = c.pubkey()
    bonding_curve = c.pubkey()
    user = c.pubkey()
    creator = c.pubkey()
    timestamp = c.i64()
    virtual_token_reserves = c.u64()
    virtual_sol_reserves = c.u64()
    real_token_reserves = c.u64()
    token_total_supply = c.u64()
    return CreateEvent(
        name=name, symbol=symbol, uri=uri, mint=mint, bonding_curve=bonding_curve,
        user=user, creator=creator, timestamp=timestamp,
        virtual_token_reserves=virtual_token_reserves, virtual_sol_reserves=virtual_sol_reserves,
        real_token_reserves=real_token_reserves, token_total_supply=token_total_supply,
    )


def find_program_data_lines(logs: list[str]) -> list[str]:
    return [line[len("Program data: "):] for line in logs if line.startswith("Program data: ")]


def instruction_names(logs: list[str]) -> list[str]:
    names = []
    for line in logs:
        prefix = "Program log: Instruction: "
        if line.startswith(prefix):
            names.append(line[len(prefix):])
    return names


def iter_frames(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            outer = json.loads(line)
            raw = outer.get("raw")
            if raw is None:
                continue
            try:
                inner = json.loads(raw)
            except json.JSONDecodeError:
                continue
            params = inner.get("params")
            if not params:
                continue
            value = params.get("result", {}).get("value", {})
            logs = value.get("logs")
            if logs:
                yield outer.get("t_wall"), value


def main(path: str) -> None:
    disc_counts: dict[str, int] = {}
    undecodable = 0
    decoded_trades: list[TradeEvent] = []
    decoded_creates: list[CreateEvent] = []
    mismatches = 0
    frames_seen = 0

    for _t_wall, value in iter_frames(path):
        frames_seen += 1
        logs = value["logs"]
        blobs = find_program_data_lines(logs)
        ix_names = instruction_names(logs)
        for b64 in blobs:
            try:
                payload = base64.b64decode(b64)
            except Exception:
                undecodable += 1
                continue
            if len(payload) < 8:
                undecodable += 1
                continue
            disc = payload[:8]
            name = KNOWN_DISCRIMINATORS.get(disc)
            disc_counts[name or disc.hex()] = disc_counts.get(name or disc.hex(), 0) + 1
            if name == "TradeEvent":
                try:
                    ev = decode_trade_event(payload)
                except Exception:
                    undecodable += 1
                    continue
                decoded_trades.append(ev)
                expect_buy = "Buy" in ix_names
                expect_sell = "Sell" in ix_names
                if expect_buy and not ev.is_buy:
                    mismatches += 1
                if expect_sell and ev.is_buy:
                    mismatches += 1
            elif name == "CreateEvent":
                try:
                    ev = decode_create_event(payload)
                except Exception:
                    undecodable += 1
                    continue
                decoded_creates.append(ev)

    print(f"frames with logs: {frames_seen}")
    print(f"discriminators seen: {disc_counts}")
    print(f"undecodable blobs: {undecodable}")
    print(f"decoded TradeEvents: {len(decoded_trades)}  (is_buy/log mismatches: {mismatches})")
    print(f"decoded CreateEvents: {len(decoded_creates)}")
    print()

    if decoded_trades:
        ev = decoded_trades[0]
        print("--- example decoded TradeEvent ---")
        for k, v in ev.__dict__.items():
            print(f"  {k}: {v}")
        print(f"  price (SOL/token, from virtual reserves) = "
              f"{ev.virtual_sol_reserves / ev.virtual_token_reserves:.12e}")

    if decoded_creates:
        ev = decoded_creates[0]
        print()
        print("--- example decoded CreateEvent ---")
        for k, v in ev.__dict__.items():
            print(f"  {k}: {v}")

    # Per-mint virtual-reserve monotonicity check (sanity: reserves should
    # move consistently with buy/sell direction across trades for the same
    # mint, when multiple trades for one mint are present in the sample).
    by_mint: dict[str, list[TradeEvent]] = {}
    for ev in decoded_trades:
        by_mint.setdefault(ev.mint, []).append(ev)
    multi = {m: evs for m, evs in by_mint.items() if len(evs) > 1}
    print()
    print(f"mints with >1 decoded trade in this sample: {len(multi)} (of {len(by_mint)} total mints)")
    for mint, evs in list(multi.items())[:3]:
        evs_sorted = sorted(evs, key=lambda e: e.timestamp)
        print(f"  mint {mint}: {len(evs_sorted)} trades")
        for e in evs_sorted:
            direction = "BUY" if e.is_buy else "SELL"
            print(f"    t={e.timestamp} {direction} vSol={e.virtual_sol_reserves} vTok={e.virtual_token_reserves} "
                  f"sol_amount={e.sol_amount} token_amount={e.token_amount}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures/providers/helius/logs_subscribe_pumpfun.jsonl")
