import json

import httpx

from memebot.early_buyers import EarlyBuyerSnapshot
from memebot.safety.gate import LiveProbes
from memebot.safety.governor import Governor
from memebot.store import (
    latest_early_buyer_read,
    open_db,
    set_terminal_state_with_reputation,
    upsert_token,
)


def gov():
    return Governor(per_minute=600, sleep=lambda s: __import__("asyncio").sleep(0))


def rpc_ok(mint_auth=None, freeze_auth=None, holders=None, owners=None):
    def handler(request):
        body = json.loads(request.content)
        m = body["method"]
        if m == "getAccountInfo":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": {"data": {
                "parsed": {"info": {"mintAuthority": mint_auth, "freezeAuthority": freeze_auth,
                                    "supply": "1000", "decimals": 6}}}}}})
        if m == "getMultipleAccounts":
            addrs = body["params"][0]
            om = owners or {}
            value = [({"data": {"parsed": {"info": {"owner": om[a]}}}} if a in om else None)
                     for a in addrs]
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": value}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": holders or []}})
    return httpx.MockTransport(handler)


def probes(conn, rpc_transport, *, early_reader=None, early_cfg=None, clock=None):
    safety_cfg = {"top10_holder_max_pct": 30.0, "dev_wallet_max_pct": 10.0,
                  "honeypot_max_impact_pct": 30.0, "rugcheck_base": "https://rc",
                  "goplus_base": "https://gp", "jupiter_base": "https://jup"}
    if early_cfg is not None:
        safety_cfg["early_buyers"] = early_cfg
    kwargs = {} if clock is None else {"clock": clock}
    return LiveProbes(
        rpc_url="https://rpc.test",
        rpc_client=httpx.AsyncClient(transport=rpc_transport),
        ext_client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"score_normalised": 0, "risks": []}))),
        jup_client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"outAmount": "1000000000", "priceImpactPct": "0"}))),
        conn=conn,
        cfg=safety_cfg,
        governors={"rugcheck": gov(), "goplus": gov(), "jupiter": gov()},
        early_buyer_reader=early_reader,
        **kwargs)


def token(mint="M1", state="CLIMBING", creator="DEV", bck="CURVE"):
    return {"mint": mint, "state": state, "bonding_curve_key": bck,
            "meta_json": json.dumps({"creator": creator})}


def test_live_probe_fixtures_use_terminal_reputation_writer():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    legacy_imports = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "memebot.store"
        and any(alias.name == "mark_rugged" for alias in node.names)
    ]
    legacy_names = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "mark_rugged"
    ]
    legacy_attributes = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "mark_rugged"
    ]
    terminal_calls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "set_terminal_state_with_reputation"
    ]

    assert legacy_imports == []
    assert legacy_names == []
    assert legacy_attributes == []
    assert terminal_calls


async def test_onchain_maps_all_checks_clean(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # one well-distributed real wallet (10% of supply), owned by a non-curve, non-dev wallet
    p = probes(conn, rpc_ok(holders=[{"address": "W_ATA", "amount": "100", "decimals": 6}],
                            owners={"W_ATA": "WALLET"}))
    results = await p.onchain(token())
    names = {r.name for r in results}
    assert {"mint_authority", "freeze_authority", "holder_concentration", "dev_wallet",
            "creator_rug_history"} <= names
    assert all(r.available for r in results)
    assert all(r.passed for r in results)   # clean token, all pass


async def test_onchain_funding_graph_hardfails_repeat_rugger(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="PRIOR", created_at=1.0)
    conn.execute("UPDATE tokens SET meta_json = ? WHERE mint = ?", ('{"creator": "DEV"}', "PRIOR"))
    conn.commit()
    set_terminal_state_with_reputation(
        conn,
        mint="PRIOR",
        outcome="RUGGED",
        raw_processed_at=2.0,
        creator="DEV",
        creator_conflicted=False,
    )
    p = probes(conn, rpc_ok(holders=[]))
    results = await p.onchain(token(creator="DEV"))
    fg = next(r for r in results if r.name == "creator_rug_history")
    assert not fg.passed and fg.hard and fg.detail["prior_rugs"] == 1


async def test_onchain_unknown_creator_dev_wallet_is_soft(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # non-empty holders: this test's subject is the unknown-creator dev_wallet soft-fail,
    # not holder-data availability (that's test_onchain_empty_holders_is_fail_closed) - an
    # empty list would now short-circuit onchain() before dev_wallet is ever appended.
    p = probes(conn, rpc_ok(holders=[{"address": "W_ATA", "amount": "100", "decimals": 6}],
                            owners={"W_ATA": "WALLET"}))
    results = await p.onchain(token(creator=""))    # metadata-less create
    dev = next(r for r in results if r.name == "dev_wallet")
    assert not dev.passed and not dev.hard and dev.available   # SOFT: raises risk, doesn't block


async def test_onchain_excludes_curve_owned_token_account(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # Reality getTokenLargestAccounts returns TOKEN ACCOUNTS. The bonding curve's own token
    # account ("CURVE_ATA") holds 80% of supply and is OWNED BY the curve PDA ("CURVE" == bck).
    # Pre-fix this tripped >30% concentration on ~every climbing token; now it's excluded.
    holders = [{"address": "CURVE_ATA", "amount": "800", "decimals": 6},
               {"address": "W_ATA", "amount": "50", "decimals": 6}]
    owners = {"CURVE_ATA": "CURVE", "W_ATA": "WALLET"}
    p = probes(conn, rpc_ok(holders=holders, owners=owners))
    results = await p.onchain(token(bck="CURVE"))
    hc = next(r for r in results if r.name == "holder_concentration")
    assert hc.passed and hc.available              # curve account excluded -> 5% real -> passes
    assert "CURVE_ATA" in hc.detail["excluded"]    # excluded by OWNER, not by address


async def test_onchain_flags_genuine_whale_concentration(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # No curve account among the top holders; a single real wallet holds 55% -> real rug risk
    # that must still hard-fail (the fix must not blunt the check into always-pass).
    holders = [{"address": "WHALE_ATA", "amount": "550", "decimals": 6},
               {"address": "W_ATA", "amount": "50", "decimals": 6}]
    owners = {"WHALE_ATA": "WHALE", "W_ATA": "WALLET"}
    p = probes(conn, rpc_ok(holders=holders, owners=owners))
    results = await p.onchain(token(bck="CURVE"))
    hc = next(r for r in results if r.name == "holder_concentration")
    assert not hc.passed and hc.hard and hc.available   # 60% > 30% -> correct hard-fail


async def test_onchain_dev_wallet_soft_flag_by_owner(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # DEV owns a token account holding 15% (> dev_wallet_max_pct 10%): flagged but SOFT.
    # Concentration stays clean (15% + 5% = 20% < 30%) -- dev holdings count there, but pass.
    holders = [{"address": "DEV_ATA", "amount": "150", "decimals": 6},
               {"address": "W_ATA", "amount": "50", "decimals": 6}]
    owners = {"DEV_ATA": "DEV", "W_ATA": "WALLET"}
    p = probes(conn, rpc_ok(holders=holders, owners=owners))
    results = await p.onchain(token(creator="DEV", bck="CURVE"))
    dev = next(r for r in results if r.name == "dev_wallet")
    hc = next(r for r in results if r.name == "holder_concentration")
    assert not dev.passed and not dev.hard and abs(dev.detail["dev_share_pct"] - 15.0) < 0.01
    assert hc.passed


class FakeEarlyBuyerReader:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.calls = []

    async def read(self, *, mint, bonding_curve_key):
        self.calls.append((mint, bonding_curve_key))
        return self.snapshot


def early_cfg(max_pct=25.0):
    return {"enabled": True, "signature_limit": 25, "buyer_limit": 20, "max_supply_pct": max_pct}


async def test_live_probes_embeds_early_buyer_without_direct_persistence(tmp_path, monkeypatch):
    from dataclasses import fields
    from typing import get_type_hints

    from memebot import early_buyers
    from memebot.safety import checks

    conn = open_db(tmp_path / "t.db")
    holders = [{"address": "EARLY_ATA", "amount": "100", "decimals": 6},
               {"address": "OTHER_ATA", "amount": "900", "decimals": 6}]
    owners = {"EARLY_ATA": "EARLY", "OTHER_ATA": "OTHER"}
    trace = []

    class TracedEarlyBuyerReader:
        def __init__(self, *snapshots):
            self._snapshots = iter(snapshots)
            self.calls = []

        async def read(self, *, mint, bonding_curve_key):
            trace.append("reader")
            self.calls.append((mint, bonding_curve_key))
            return next(self._snapshots)

    reader = TracedEarlyBuyerReader(
        EarlyBuyerSnapshot(
            mint="M1", buyers=("EARLY",), signatures_scanned=2, transactions_scanned=2,
        ),
        EarlyBuyerSnapshot(
            mint="M1", buyers=(), signatures_scanned=0, transactions_scanned=0,
            unavailable_reason="rpc_error",
        ),
    )

    clock_values = iter((100.0, 123.5, 200.0, 234.5))

    def clock():
        value = next(clock_values)
        trace.append(f"clock:{value}")
        return value

    original_check = checks.early_buyer_concentration_check

    def traced_check(*args, **kwargs):
        trace.append("early_buyer_concentration_check")
        return original_check(*args, **kwargs)

    monkeypatch.setattr(checks, "early_buyer_concentration_check", traced_check)
    rows_before = conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0]
    p = probes(conn, rpc_ok(holders=holders, owners=owners), early_reader=reader,
               early_cfg=early_cfg(max_pct=25.0), clock=clock)

    try:
        results = await p.onchain(token(mint="M1", bck="CURVE"))

        early_buyer_results = [
            result for result in results
            if result.name == "early_buyer_concentration"
        ]
        assert len(early_buyer_results) == 1
        eb = early_buyer_results[0]
        rows_after = conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0]
        violations = []
        if rows_after != rows_before:
            violations.append("LiveProbes persisted early_buyer_reads directly")
        if "early_buyer_evidence_v1" not in eb.detail:
            violations.append("early_buyer_evidence_v1 missing")
        assert not violations, violations

        assert eb.passed and eb.available and eb.detail["early_buyer_share_pct"] == 10.0
        assert eb.reason == ""
        assert eb.detail["early_buyers"] == ["EARLY"]
        assert eb.detail["signatures_scanned"] == 2
        assert eb.detail["transactions_scanned"] == 2
        assert eb.detail["early_buyer_evidence_v1"] == {
            "checked_at": 123.5,
            "buyers": ("EARLY",),
            "unavailable_reason": "",
            "inputs_hash": "f0000c7bfdf16e841f47f4e7fb77b1191a29a8cb25492d533a63a7c0d09c0d08",
        }
        assert reader.calls == [("M1", "CURVE")]
        assert trace == [
            "clock:100.0", "reader", "clock:123.5", "early_buyer_concentration_check",
        ]
        assert latest_early_buyer_read(conn, "M1") is None

        draft_type = early_buyers.EarlyBuyerEvidenceDraft
        assert draft_type.__dataclass_params__.frozen
        assert draft_type.__slots__ == (
            "checked_at", "buyers", "unavailable_reason", "inputs_hash",
        )
        assert tuple(field.name for field in fields(draft_type)) == draft_type.__slots__
        assert get_type_hints(draft_type) == {
            "checked_at": float | None,
            "buyers": tuple[str, ...],
            "unavailable_reason": str,
            "inputs_hash": str,
        }

        unavailable_results = await p.onchain(token(mint="M1", bck="CURVE"))

        unavailable_early_buyer_results = [
            result for result in unavailable_results
            if result.name == "early_buyer_concentration"
        ]
        assert len(unavailable_early_buyer_results) == 1
        unavailable = unavailable_early_buyer_results[0]
        assert not unavailable.passed and unavailable.hard and not unavailable.available
        assert unavailable.reason == "check_unavailable"
        assert unavailable.detail == {
            "reason": "rpc_error",
            "signatures_scanned": 0,
            "transactions_scanned": 0,
            "early_buyer_evidence_v1": {
                "checked_at": 234.5,
                "buyers": (),
                "unavailable_reason": "rpc_error",
                "inputs_hash": "0a731e0b59b18580c6796644d248d4f11d5c4a17d0239208e39f00a0e6164ede",
            },
        }
        assert reader.calls == [("M1", "CURVE"), ("M1", "CURVE")]
        assert trace == [
            "clock:100.0", "reader", "clock:123.5", "early_buyer_concentration_check",
            "clock:200.0", "reader", "clock:234.5",
        ]
        assert latest_early_buyer_read(conn, "M1") is None
    finally:
        await p._rpc._client.aclose()
        await p._ext_client.aclose()
        await p._jup_client.aclose()


async def test_onchain_early_buyer_concentration_hardfails_over_threshold(tmp_path):
    conn = open_db(tmp_path / "t.db")
    holders = [{"address": "EARLY_ATA", "amount": "300", "decimals": 6},
               {"address": "OTHER_ATA", "amount": "700", "decimals": 6}]
    owners = {"EARLY_ATA": "EARLY", "OTHER_ATA": "OTHER"}
    reader = FakeEarlyBuyerReader(EarlyBuyerSnapshot(
        mint="M1", buyers=("EARLY",), signatures_scanned=1, transactions_scanned=1))
    p = probes(conn, rpc_ok(holders=holders, owners=owners), early_reader=reader,
               early_cfg=early_cfg(max_pct=25.0))

    results = await p.onchain(token(mint="M1", bck="CURVE"))

    eb = next(r for r in results if r.name == "early_buyer_concentration")
    assert not eb.passed and eb.hard and eb.reason == "early_buyer_concentration"


async def test_onchain_early_buyer_unavailable_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    holders = [{"address": "W_ATA", "amount": "100", "decimals": 6}]
    owners = {"W_ATA": "WALLET"}
    reader = FakeEarlyBuyerReader(EarlyBuyerSnapshot(
        mint="M1", buyers=(), signatures_scanned=0, transactions_scanned=0,
        unavailable_reason="rpc_error"))
    p = probes(conn, rpc_ok(holders=holders, owners=owners), early_reader=reader,
               early_cfg=early_cfg(max_pct=25.0))

    results = await p.onchain(token(mint="M1", bck="CURVE"))

    eb = next(r for r in results if r.name == "early_buyer_concentration")
    assert not eb.passed and eb.hard and not eb.available
    assert eb.reason == "check_unavailable"


async def test_onchain_early_buyer_missing_curve_key_fails_closed_without_reader_call(tmp_path):
    conn = open_db(tmp_path / "t.db")
    holders = [{"address": "W_ATA", "amount": "100", "decimals": 6}]
    owners = {"W_ATA": "WALLET"}
    reader = FakeEarlyBuyerReader(EarlyBuyerSnapshot(
        mint="M1", buyers=("WALLET",), signatures_scanned=1, transactions_scanned=1))
    p = probes(conn, rpc_ok(holders=holders, owners=owners), early_reader=reader,
               early_cfg=early_cfg(max_pct=25.0))

    results = await p.onchain(token(mint="M1", bck=""))

    eb = next(r for r in results if r.name == "early_buyer_concentration")
    assert not eb.passed and eb.hard and not eb.available
    assert eb.detail["reason"] == "missing_bonding_curve_key"
    assert reader.calls == []


async def test_onchain_early_buyer_partial_owner_resolution_fails_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # EARLY_ATA could be an early-buyer wallet holding 30% (> max 25%), but owner
    # resolution returned no parseable owner for it. The P2 hard gate must not count
    # unresolved holders as 0% early-buyer share and pass.
    holders = [{"address": "EARLY_ATA", "amount": "300", "decimals": 6},
               {"address": "OTHER_ATA", "amount": "700", "decimals": 6}]
    owners = {"OTHER_ATA": "OTHER"}
    reader = FakeEarlyBuyerReader(EarlyBuyerSnapshot(
        mint="M1", buyers=("EARLY",), signatures_scanned=1, transactions_scanned=1))
    p = probes(conn, rpc_ok(holders=holders, owners=owners), early_reader=reader,
               early_cfg=early_cfg(max_pct=25.0))

    results = await p.onchain(token(mint="M1", bck="CURVE"))

    eb = next(r for r in results if r.name == "early_buyer_concentration")
    assert not eb.passed and eb.hard and not eb.available
    assert eb.detail["reason"] == "owner_resolution_incomplete"
    assert eb.detail["unresolved"] == ["EARLY_ATA"]
    assert reader.calls == []


async def test_early_buyer_required_input_reason_mapping(tmp_path):
    conn = open_db(tmp_path / "required-input-reasons.db")
    reader = FakeEarlyBuyerReader(EarlyBuyerSnapshot(
        mint="M1", buyers=("WALLET",), signatures_scanned=1, transactions_scanned=1,
    ))
    opened_probes = []

    def traced_rpc(holders, owners, trace):
        def handler(request):
            body = json.loads(request.content)
            method = body["method"]
            if method == "getMultipleAccounts":
                addresses = body["params"][0]
                trace.append((method, tuple(addresses)))
                value = [
                    ({"data": {"parsed": {"info": {"owner": owners[address]}}}}
                     if address in owners else None)
                    for address in addresses
                ]
                return httpx.Response(
                    200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": value}},
                )
            trace.append((method, None))
            if method == "getAccountInfo":
                value = {"data": {"parsed": {"info": {
                    "mintAuthority": None,
                    "freezeAuthority": None,
                    "supply": "1000",
                    "decimals": 6,
                }}}}
            else:
                value = holders
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": value}},
            )

        return httpx.MockTransport(handler)

    cases = (
        {
            "bonding_curve_key": "",
            "holders": [{"address": "W_ATA", "amount": "100", "decimals": 6}],
            "owners": {"W_ATA": "WALLET"},
            "detail": {"reason": "missing_bonding_curve_key"},
        },
        {
            "bonding_curve_key": "CURVE",
            "holders": [
                {"address": "Z_ATA", "amount": "50", "decimals": 6},
                {"address": "M_ATA", "amount": "50", "decimals": 6},
                {"address": "A_ATA", "amount": "50", "decimals": 6},
            ],
            "owners": {"M_ATA": "WALLET"},
            "detail": {
                "reason": "owner_resolution_incomplete",
                "unresolved": ["A_ATA", "Z_ATA"],
            },
        },
    )

    try:
        for case in cases:
            trace = []
            p = probes(
                conn,
                traced_rpc(case["holders"], case["owners"], trace),
                early_reader=reader,
                early_cfg=early_cfg(max_pct=25.0),
            )
            opened_probes.append(p)

            results = await p.onchain(token(
                mint="M1", bck=case["bonding_curve_key"],
            ))

            early_results = [
                result for result in results
                if result.name == "early_buyer_concentration"
            ]
            assert len(early_results) == 1
            early = early_results[0]
            assert not early.passed and early.hard and not early.available
            assert early.reason == "check_unavailable"
            assert early.detail == case["detail"]
            assert "early_buyer_evidence_v1" not in early.detail

            holder_results = [
                result for result in results
                if result.name == "holder_concentration"
            ]
            assert len(holder_results) == 1
            assert holder_results[0].passed and holder_results[0].available
            assert "holder_evidence_v1" in holder_results[0].detail
            assert trace == [
                ("getAccountInfo", None),
                ("getTokenLargestAccounts", None),
                ("getMultipleAccounts", tuple(
                    holder["address"] for holder in case["holders"]
                )),
            ]
        assert reader.calls == []
    finally:
        for p in opened_probes:
            await p._rpc._client.aclose()
            await p._ext_client.aclose()
            await p._jup_client.aclose()


async def test_early_buyer_reader_unavailable_reason_mapping(tmp_path):
    conn = open_db(tmp_path / "reader-unavailable.db")
    holders = [{"address": "W_ATA", "amount": "100", "decimals": 6}]
    rpc_calls = []
    base_rpc = rpc_ok(holders=holders, owners={"W_ATA": "WALLET"})

    def traced_rpc(request):
        body = json.loads(request.content)
        detail = tuple(body["params"][0]) if body["method"] == "getMultipleAccounts" else None
        rpc_calls.append((body["method"], detail))
        return base_rpc.handle_request(request)

    clock_calls = []

    def clock():
        clock_calls.append("clock")
        return 123.5

    p = probes(
        conn,
        httpx.MockTransport(traced_rpc),
        early_cfg=early_cfg(max_pct=25.0),
        clock=clock,
    )
    assert p._early_buyer_reader is not None
    p._early_buyer_reader = None

    try:
        results = await p.onchain(token(mint="M1", bck="CURVE"))

        early_results = [
            result for result in results
            if result.name == "early_buyer_concentration"
        ]
        assert len(early_results) == 1
        early = early_results[0]
        assert not early.passed and early.hard and not early.available
        assert early.reason == "check_unavailable"
        assert early.detail == {"reason": "reader_unavailable"}
        assert "early_buyer_evidence_v1" not in early.detail

        holder_results = [
            result for result in results
            if result.name == "holder_concentration"
        ]
        assert len(holder_results) == 1
        assert holder_results[0].passed and holder_results[0].available
        assert "holder_evidence_v1" in holder_results[0].detail
        assert rpc_calls == [
            ("getAccountInfo", None),
            ("getTokenLargestAccounts", None),
            ("getMultipleAccounts", ("W_ATA",)),
        ]
        assert clock_calls == ["clock"]
    finally:
        await p._rpc._client.aclose()
        await p._ext_client.aclose()
        await p._jup_client.aclose()


async def test_onchain_owner_resolution_unavailable_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    # getAccountInfo + getTokenLargestAccounts succeed, but getMultipleAccounts errors:
    # we cannot tell curve reserves from real wallets -> concentration is UNAVAILABLE ->
    # fail-closed (never a silent pass just because owner resolution flaked).
    def handler(request):
        body = json.loads(request.content)
        m = body["method"]
        if m == "getAccountInfo":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": {"data": {
                "parsed": {"info": {"mintAuthority": None, "freezeAuthority": None,
                                    "supply": "1000", "decimals": 6}}}}}})
        if m == "getTokenLargestAccounts":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": [
                {"address": "W_ATA", "amount": "100", "decimals": 6}]}})
        return httpx.Response(500, text="getMultipleAccounts down")
    p = probes(conn, httpx.MockTransport(handler))
    results = await p.onchain(token())
    hc = next(r for r in results if r.name == "holder_concentration")
    assert not hc.available and not hc.passed


async def test_onchain_rpc_error_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    err = httpx.MockTransport(lambda r: httpx.Response(500, text="down"))
    p = probes(conn, err)
    results = await p.onchain(token())
    assert any(not r.available for r in results)    # fail-closed on RpcError


async def test_onchain_empty_holders_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    p = probes(conn, rpc_ok(holders=[]))   # clean authorities, but ZERO holders returned
    results = await p.onchain(token())
    hc = next(r for r in results if r.name == "holder_concentration")
    assert not hc.available and not hc.passed   # fail-closed, not a silent pass


async def test_external_and_honeypot_run(tmp_path):
    conn = open_db(tmp_path / "t.db")
    p = probes(conn, rpc_ok(holders=[]))
    ext = await p.external(token())
    assert {r.name for r in ext} == {"rugcheck", "goplus"}
    hp = await p.honeypot(token(state="GRADUATED"))
    assert hp[0].name == "honeypot"


async def test_live_probes_embeds_holder_evidence_without_extra_rpc(tmp_path, monkeypatch):
    from memebot.safety import checks
    from memebot.safety.rpc import Holder, MintInfo, RpcError

    conn = open_db(tmp_path / "holder-evidence.db")
    rpc_client, ext_client, jup_client = object(), object(), object()
    cfg = {
        "top10_holder_max_pct": 30.0,
        "dev_wallet_max_pct": 10.0,
        "honeypot_max_impact_pct": 30.0,
        "rugcheck_base": "https://rc",
        "goplus_base": "https://gp",
        "jupiter_base": "https://jup",
    }
    full_holders = [Holder("curve_ata", 800), Holder("wallet_ata", 50)]
    full_owners = {"curve_ata": "CURVE", "wallet_ata": "WALLET"}
    active_trace = None
    original_holder_check = checks.holder_concentration_check

    def traced_holder_check(*args, **kwargs):
        active_trace.append(("holder_concentration_check", None))
        return original_holder_check(*args, **kwargs)

    monkeypatch.setattr(checks, "holder_concentration_check", traced_holder_check)

    class FakeRpc:
        def __init__(self, *, trace, supply, holders, owners, failure=""):
            self.trace = trace
            self.supply = supply
            self.holders = holders
            self.owners = owners
            self.failure = failure

        async def mint_info(self, mint):
            self.trace.append(("mint_info", mint))
            if self.failure == "mint_info":
                raise RpcError("mint unavailable")
            return MintInfo(None, None, self.supply, 6)

        async def largest_accounts(self, mint):
            self.trace.append(("largest_accounts", mint))
            if self.failure == "holders":
                raise RpcError("holders unavailable")
            return self.holders

        async def account_owners(self, addresses):
            self.trace.append(("account_owners", tuple(addresses)))
            if self.failure == "owners":
                raise RpcError("owners unavailable")
            return self.owners

    cases = [
        {
            "label": "available",
            "supply": 1_000,
            "holders": full_holders,
            "owners": full_owners,
            "curve": "CURVE",
            "observed_at": 10.0,
            "reason": "",
            "inputs_hash": "080185df46d0d3c6ecd9a139f318e180ba3594fca10f264ac0820e3ab98bc3bb",
            "metrics": (2, 1, 5.0),
            "legacy": (True, "", True, 5.0),
            "legacy_detail": {
                "top10_share_pct": 5.0,
                "excluded": ["CURVE", "curve_ata"],
            },
            "rpc": ("mint_info", "largest_accounts", "account_owners"),
        },
        {
            "label": "holders_unavailable",
            "supply": 1_000,
            "holders": full_holders,
            "owners": full_owners,
            "curve": "CURVE",
            "failure": "holders",
            "observed_at": 20.0,
            "reason": "holder_accounts_unavailable",
            "inputs_hash": "d0a7eee09f39f6fb8b6b8c308d30bac89394cfd48dcde0889105bb0b86d02b40",
            "metrics": (None, None, None),
            "legacy": (False, "check_unavailable", False, None),
            "legacy_detail": {"error": "RpcError('holders unavailable')"},
            "rpc": ("mint_info", "largest_accounts"),
        },
        {
            "label": "holders_empty",
            "supply": 1_000,
            "holders": [],
            "owners": None,
            "curve": "CURVE",
            "observed_at": 30.0,
            "reason": "holder_accounts_empty",
            "inputs_hash": "6c2d6c2012e043f35a8ada93c3715c2f68e9081c9b9c760fbe96f9bd0928d4f5",
            "metrics": (None, None, None),
            "legacy": (False, "check_unavailable", False, None),
            "legacy_detail": {"reason": "no_holder_data"},
            "rpc": ("mint_info", "largest_accounts"),
        },
        {
            "label": "owners_unavailable",
            "supply": 1_000,
            "holders": full_holders,
            "owners": full_owners,
            "curve": "CURVE",
            "failure": "owners",
            "observed_at": 40.0,
            "reason": "holder_owner_resolution_unavailable",
            "inputs_hash": "0f02a0f62097823c022e77d7fe1aa5172a75a36bea3438e43732aa6271b2a741",
            "metrics": (None, None, None),
            "legacy": (False, "check_unavailable", False, None),
            "legacy_detail": {"error": "RpcError('owners unavailable')"},
            "rpc": ("mint_info", "largest_accounts", "account_owners"),
        },
        {
            "label": "owners_incomplete",
            "supply": 1_000,
            "holders": full_holders,
            "owners": {"curve_ata": "CURVE"},
            "curve": "CURVE",
            "observed_at": 50.0,
            "reason": "holder_owner_resolution_incomplete",
            "inputs_hash": "86aebd47f6b8a0914421020f07ae381b9d0d8dbcd3c06534c34fce0025620669",
            "metrics": (None, None, None),
            "legacy": (True, "", True, 5.0),
            "legacy_detail": {
                "top10_share_pct": 5.0,
                "excluded": ["CURVE", "curve_ata"],
            },
            "rpc": ("mint_info", "largest_accounts", "account_owners"),
        },
        {
            "label": "curve_missing",
            "supply": 1_000,
            "holders": full_holders,
            "owners": full_owners,
            "curve": "",
            "observed_at": 60.0,
            "reason": "holder_curve_owner_unavailable",
            "inputs_hash": "9c90c563d4e8bd7296964a598d2a642f5defc3de963503124e7488777df325f3",
            "metrics": (None, None, None),
            "legacy": (False, "holder_concentration", True, 85.0),
            "legacy_detail": {"top10_share_pct": 85.0, "excluded": []},
            "rpc": ("mint_info", "largest_accounts", "account_owners"),
        },
        {
            "label": "noncurve_empty",
            "supply": 1_000,
            "holders": [Holder("curve_ata", 800)],
            "owners": {"curve_ata": "CURVE"},
            "curve": "CURVE",
            "observed_at": 70.0,
            "reason": "holder_non_curve_owners_empty",
            "inputs_hash": "de7d5660ec2e953bbab3759a41b0f05859770de24fdfce2e96fabf7c46ef2917",
            "metrics": (None, None, None),
            "legacy": (True, "", True, 0.0),
            "legacy_detail": {
                "top10_share_pct": 0.0,
                "excluded": ["CURVE", "curve_ata"],
            },
            "rpc": ("mint_info", "largest_accounts", "account_owners"),
        },
        {
            "label": "supply_unavailable",
            "supply": 0,
            "holders": full_holders,
            "owners": full_owners,
            "curve": "CURVE",
            "observed_at": 80.0,
            "reason": "holder_mint_supply_unavailable",
            "inputs_hash": "269fcd7d9b9f40d42e83623f13cd2a116cf26244374123279940a6042432ef2f",
            "metrics": (None, None, None),
            "legacy": (False, "holder_concentration", True, 100.0),
            "legacy_detail": {
                "top10_share_pct": 100.0,
                "excluded": ["CURVE", "curve_ata"],
            },
            "rpc": ("mint_info", "largest_accounts", "account_owners"),
        },
    ]

    for case in cases:
        trace = []
        active_trace = trace

        def clock():
            trace.append(("clock", case["observed_at"]))
            return case["observed_at"]

        probe = LiveProbes(
            rpc_url="https://rpc.test",
            rpc_client=rpc_client,
            ext_client=ext_client,
            jup_client=jup_client,
            conn=conn,
            cfg=cfg,
            governors={"rugcheck": gov(), "goplus": gov(), "jupiter": gov()},
            clock=clock,
        )
        probe._rpc = FakeRpc(
            trace=trace,
            supply=case["supply"],
            holders=case["holders"],
            owners=case["owners"],
            failure=case.get("failure", ""),
        )

        results = await probe.onchain(token(creator="", bck=case["curve"]))
        holder_results = [
            result for result in results
            if result.name == "holder_concentration"
        ]
        assert len(holder_results) == 1, case["label"]
        holder_result = holder_results[0]
        passed, reason, available, share = case["legacy"]
        assert (
            holder_result.passed,
            holder_result.reason,
            holder_result.available,
        ) == (passed, reason, available)
        if share is not None:
            assert holder_result.detail["top10_share_pct"] == share
        assert {
            key: holder_result.detail[key]
            for key in case["legacy_detail"]
        } == case["legacy_detail"]
        evidence = holder_result.detail["holder_evidence_v1"]
        sampled, distinct, top10 = case["metrics"]
        assert evidence == {
            "sampled_token_accounts": sampled,
            "distinct_non_curve_owners": distinct,
            "top10_non_curve_owner_share_pct": top10,
            "holder_observed_at": case["observed_at"],
            "unavailable_reason": case["reason"],
            "inputs_hash": case["inputs_hash"],
        }
        expected_order = [*case["rpc"], "clock"]
        if case.get("failure") not in {"holders", "owners"} and case["holders"]:
            expected_order.append("holder_concentration_check")
        assert [item[0] for item in trace] == expected_order
        assert trace.count(("clock", case["observed_at"])) == 1

    trace = []

    def forbidden_clock():
        trace.append(("clock", 90.0))
        return 90.0

    failed_probe = LiveProbes(
        rpc_url="https://rpc.test",
        rpc_client=rpc_client,
        ext_client=ext_client,
        jup_client=jup_client,
        conn=conn,
        cfg=cfg,
        governors={"rugcheck": gov(), "goplus": gov(), "jupiter": gov()},
        clock=forbidden_clock,
    )
    failed_probe._rpc = FakeRpc(
        trace=trace,
        supply=1_000,
        holders=full_holders,
        owners=full_owners,
        failure="mint_info",
    )
    mint_failure = await failed_probe.onchain(token(creator=""))
    assert [result.name for result in mint_failure] == ["mint_info"]
    assert "holder_evidence_v1" not in mint_failure[0].detail
    assert trace == [("mint_info", "M1")]
