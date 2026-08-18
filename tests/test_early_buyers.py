import json
from pathlib import Path

from memebot.early_buyers import EarlyBuyerReader
from memebot.ingest.pumpfun_decode import decode_trade_event_from_program_data
from memebot.safety.rpc import RpcError, SignatureInfo


FIXTURE = Path("tests/fixtures/providers/helius/logs_subscribe_pumpfun.jsonl")


def _first_program_data(*, is_buy: bool, mint: str | None = None) -> str:
    for raw_line in FIXTURE.read_text().splitlines():
        outer = json.loads(raw_line)
        rpc = json.loads(outer["raw"])
        logs = rpc.get("params", {}).get("result", {}).get("value", {}).get("logs", [])
        for line in logs:
            ev = decode_trade_event_from_program_data(line)
            if ev is not None and ev.is_buy is is_buy and (mint is None or ev.mint == mint):
                return line
    raise AssertionError("fixture line not found")


BUY_LINE = _first_program_data(is_buy=True)
BUY_EVENT = decode_trade_event_from_program_data(BUY_LINE)
assert BUY_EVENT is not None
SELL_LINE = _first_program_data(is_buy=False, mint=BUY_EVENT.mint)


class FakeRpc:
    def __init__(self, sigs, logs_by_sig):
        self.sigs = sigs
        self.logs_by_sig = logs_by_sig
        self.signature_calls = []
        self.tx_calls = []

    async def signatures_for_address(self, address, *, limit):
        self.signature_calls.append((address, limit))
        return self.sigs[:limit]

    async def transaction_logs(self, signature):
        self.tx_calls.append(signature)
        value = self.logs_by_sig[signature]
        if isinstance(value, Exception):
            raise value
        return value


def sig(name, *, slot=1, block_time=100):
    return SignatureInfo(signature=name, slot=slot, block_time=block_time)


async def test_early_buyer_reader_returns_first_unique_buyers_for_mint():
    mint = BUY_EVENT.mint
    rpc = FakeRpc(
        [sig("S1"), sig("S2"), sig("S3")],
        {
            "S1": [SELL_LINE],                 # sells do not count
            "S2": [BUY_LINE],
            "S3": [BUY_LINE],                  # duplicate wallet does not repeat
        },
    )
    reader = EarlyBuyerReader(rpc, signature_limit=3, buyer_limit=5)

    snap = await reader.read(mint=mint, bonding_curve_key="CURVE")

    assert snap.buyers == (BUY_EVENT.user,)
    assert snap.signatures_scanned == 3
    assert snap.transactions_scanned == 3
    assert snap.unavailable_reason == ""
    assert rpc.signature_calls == [("CURVE", 3)]


async def test_early_buyer_reader_stops_after_buyer_limit():
    mint = BUY_EVENT.mint
    rpc = FakeRpc([sig("S1"), sig("S2")], {"S1": [BUY_LINE], "S2": [BUY_LINE]})
    reader = EarlyBuyerReader(rpc, signature_limit=2, buyer_limit=1)

    snap = await reader.read(mint=mint, bonding_curve_key="CURVE")

    assert snap.buyers == (BUY_EVENT.user,)
    assert rpc.tx_calls == ["S2"]


async def test_early_buyer_reader_processes_oldest_signature_first_within_bounded_window():
    """getSignaturesForAddress returns newest first; early-buyer reads must invert it."""
    mint = BUY_EVENT.mint
    rpc = FakeRpc(
        [sig("NEW", slot=30, block_time=300), sig("OLD", slot=10, block_time=100)],
        {"NEW": [BUY_LINE], "OLD": [BUY_LINE]},
    )
    reader = EarlyBuyerReader(rpc, signature_limit=2, buyer_limit=1)

    snap = await reader.read(mint=mint, bonding_curve_key="CURVE")

    assert snap.buyers == (BUY_EVENT.user,)
    assert rpc.tx_calls == ["OLD"]


async def test_early_buyer_reader_unavailable_when_no_matching_buys():
    rpc = FakeRpc([sig("S1")], {"S1": [SELL_LINE]})
    reader = EarlyBuyerReader(rpc, signature_limit=1, buyer_limit=5)

    snap = await reader.read(mint="OTHER", bonding_curve_key="CURVE")

    assert snap.buyers == ()
    assert snap.unavailable_reason == "no_matching_buy_events"


async def test_early_buyer_reader_unavailable_on_rpc_error():
    rpc = FakeRpc([sig("S1")], {"S1": RpcError("boom")})
    reader = EarlyBuyerReader(rpc, signature_limit=1, buyer_limit=5)

    snap = await reader.read(mint=BUY_EVENT.mint, bonding_curve_key="CURVE")

    assert snap.buyers == ()
    assert snap.unavailable_reason == "rpc_error"


async def test_early_buyer_rpc_reason_mapping():
    provider_detail = "private provider failure detail"

    class SignatureFailureRpc(FakeRpc):
        async def signatures_for_address(self, address, *, limit):
            self.signature_calls.append((address, limit))
            raise RpcError(provider_detail)

    signature_rpc = SignatureFailureRpc([], {})
    reader = EarlyBuyerReader(signature_rpc, signature_limit=2, buyer_limit=5)

    signature_snap = await reader.read(mint=BUY_EVENT.mint, bonding_curve_key="CURVE")

    assert signature_snap.buyers == ()
    assert signature_snap.signatures_scanned == 0
    assert signature_snap.transactions_scanned == 0
    assert signature_snap.unavailable_reason == "rpc_error"
    assert provider_detail not in repr(signature_snap)
    assert signature_rpc.signature_calls == [("CURVE", 2)]
    assert signature_rpc.tx_calls == []

    transaction_rpc = FakeRpc(
        [sig("NEW", slot=20), sig("OLD", slot=10)],
        {"NEW": RpcError(provider_detail), "OLD": [BUY_LINE]},
    )
    reader = EarlyBuyerReader(transaction_rpc, signature_limit=2, buyer_limit=5)

    transaction_snap = await reader.read(mint=BUY_EVENT.mint, bonding_curve_key="CURVE")

    assert transaction_snap.buyers == ()
    assert transaction_snap.signatures_scanned == 2
    assert transaction_snap.transactions_scanned == 1
    assert transaction_snap.unavailable_reason == "rpc_error"
    assert provider_detail not in repr(transaction_snap)
    assert transaction_rpc.signature_calls == [("CURVE", 2)]
    assert transaction_rpc.tx_calls == ["OLD", "NEW"]


async def test_early_buyer_empty_chain_reason_mapping():
    empty_rpc = FakeRpc([], {})
    reader = EarlyBuyerReader(empty_rpc, signature_limit=2, buyer_limit=5)

    empty_snap = await reader.read(mint=BUY_EVENT.mint, bonding_curve_key="CURVE")

    assert empty_snap.buyers == ()
    assert empty_snap.signatures_scanned == 0
    assert empty_snap.transactions_scanned == 0
    assert empty_snap.unavailable_reason == "no_signatures"
    assert empty_rpc.signature_calls == [("CURVE", 2)]
    assert empty_rpc.tx_calls == []

    other_buy_line = None
    for raw_line in FIXTURE.read_text().splitlines():
        outer = json.loads(raw_line)
        rpc_payload = json.loads(outer["raw"])
        logs = rpc_payload.get("params", {}).get("result", {}).get("value", {}).get("logs", [])
        for line in logs:
            event = decode_trade_event_from_program_data(line)
            if event is not None and event.is_buy and event.mint != BUY_EVENT.mint:
                other_buy_line = line
                break
        if other_buy_line is not None:
            break
    assert other_buy_line is not None

    no_match_rpc = FakeRpc(
        [sig("NEW", slot=20), sig("OLD", slot=10)],
        {"NEW": [other_buy_line], "OLD": [SELL_LINE]},
    )
    reader = EarlyBuyerReader(no_match_rpc, signature_limit=2, buyer_limit=5)

    no_match_snap = await reader.read(mint=BUY_EVENT.mint, bonding_curve_key="CURVE")

    assert no_match_snap.buyers == ()
    assert no_match_snap.signatures_scanned == 2
    assert no_match_snap.transactions_scanned == 2
    assert no_match_snap.unavailable_reason == "no_matching_buy_events"
    assert no_match_rpc.signature_calls == [("CURVE", 2)]
    assert no_match_rpc.tx_calls == ["OLD", "NEW"]
