import json
from pathlib import Path

from memebot.events import TokenCreated, TokenGraduated
from memebot.ingest.pumpportal import parse_frame

FIXTURE = Path("tests/fixtures/providers/pumpportal/session1.jsonl")


def frames():
    for line in FIXTURE.read_text().splitlines():
        yield json.loads(line)["raw"]


def test_parses_pump_create():
    raw = next(r for r in frames()
               if '"txType":"create"' in r and '"pool":"pump"' in r)
    e = parse_frame(raw, t_wall=1.0, t_mono=2.0)
    assert isinstance(e, TokenCreated)
    assert e.mint and e.creator
    assert e.raw["bondingCurveKey"]          # poller needs this
    assert e.t_wall == 1.0 and e.t_mono == 2.0


def test_parses_migrate():
    raw = next(r for r in frames() if '"txType":"migrate"' in r)
    e = parse_frame(raw, t_wall=1.0, t_mono=2.0)
    assert isinstance(e, TokenGraduated)
    assert e.mint and e.dex == "pump-amm" and e.pool == ""


def test_replay_full_fixture_counts():
    created, graduated, skipped = 0, 0, 0
    for raw in frames():
        e = parse_frame(raw, t_wall=0.0, t_mono=0.0)
        if isinstance(e, TokenCreated):
            created += 1
        elif isinstance(e, TokenGraduated):
            graduated += 1
        else:
            skipped += 1
    # Observed in recon: 197 pump creates, 1 bonk create (skipped), 7 migrates, 3 acks.
    assert created == 197
    assert graduated == 7
    assert skipped == 4


def test_metadata_less_create_gets_sentinels():
    raw = next(r for r in frames()
               if '"txType":"create"' in r and '"pool":"pump"' in r
               and '"name"' not in r)
    e = parse_frame(raw, t_wall=0.0, t_mono=0.0)
    assert isinstance(e, TokenCreated)
    assert e.name == "" and e.symbol == ""


def test_garbage_and_acks_return_none():
    assert parse_frame("not json", t_wall=0.0, t_mono=0.0) is None
    assert parse_frame('{"message": "subscribed"}', t_wall=0.0, t_mono=0.0) is None
