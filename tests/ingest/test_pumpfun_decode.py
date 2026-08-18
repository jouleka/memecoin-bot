import json
from pathlib import Path

from memebot.ingest.pumpfun_decode import (
    decode_trade_event_from_program_data,
    trade_events_from_logs,
)


def _first_trade_line() -> str:
    path = Path("tests/fixtures/providers/helius/logs_subscribe_pumpfun.jsonl")
    for raw_line in path.read_text().splitlines():
        outer = json.loads(raw_line)
        rpc = json.loads(outer["raw"])
        logs = rpc.get("params", {}).get("result", {}).get("value", {}).get("logs", [])
        for line in logs:
            if line.startswith("Program data: "):
                return line
    raise AssertionError("fixture has no Program data line")


def test_decode_trade_event_from_real_program_data_line():
    ev = decode_trade_event_from_program_data(_first_trade_line())
    assert ev is not None
    assert ev.mint == "CJiJAKFMD7HruUZwDhK5vBPV379yGhnR7YuLNbFxpump"
    assert ev.sol_amount == 1_068_544_735
    assert ev.token_amount == 25_511_755_060_526
    assert ev.is_buy is False
    assert ev.user == "6Q9413Rt2YTH2rKCjVueFwnxM4Ri472EmrgRPDzMyxxf"
    assert ev.virtual_sol_reserves == 36_188_261_779
    assert ev.virtual_token_reserves == 889_514_954_991_884
    assert ev.real_sol_reserves == 6_188_261_779
    assert ev.real_token_reserves == 609_614_954_991_884
    assert ev.creator == "96rrAeq23jwPC9NwVEtCVxgbWbtsc2XPf5zU1pF1frc9"


def test_non_trade_or_malformed_program_data_returns_none():
    assert decode_trade_event_from_program_data("Program log: Instruction: Buy") is None
    assert decode_trade_event_from_program_data("Program data: not-base64!!!") is None


def test_trade_events_from_logs_filters_non_trade_lines():
    events = trade_events_from_logs([
        "Program log: Instruction: Sell",
        _first_trade_line(),
        "Program data: not-base64!!!",
    ])
    assert [ev.user for ev in events] == ["6Q9413Rt2YTH2rKCjVueFwnxM4Ri472EmrgRPDzMyxxf"]
