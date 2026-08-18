import asyncio
import hashlib
import json
from dataclasses import asdict

import pytest

from memebot.early_buyers import EarlyBuyerEvidenceDraft, EarlyBuyerSnapshot
from memebot.safety.checks import CheckResult, HolderEvidenceDraft
from memebot.safety.gate import LiveProbes, SafetyGate, _extract_early_buyer_evidence
from memebot.safety.rpc import Holder, MintInfo
from memebot.store import latest_safety_report, open_db, upsert_token


def PASS(name):
    return CheckResult(name, True, hard=True)


def FAIL(name, reason):
    return CheckResult(name, False, hard=True, reason=reason)


def UNAVAIL(name):
    return CheckResult(name, False, hard=True, reason="check_unavailable", available=False)


class FakeProbes:
    """Injected check providers; records call order to prove cascade short-circuit."""
    def __init__(self, onchain, external=None, honeypot=None):
        self._onchain, self._external, self._honeypot = onchain, external, honeypot
        self.calls = []

    async def onchain(self, token):
        self.calls.append("onchain")
        return self._onchain

    async def external(self, token):
        self.calls.append("external")
        return self._external

    async def honeypot(self, token):
        self.calls.append("honeypot")
        return self._honeypot


def token(mint="M1", state="CLIMBING"):
    return {"mint": mint, "state": state, "bonding_curve_key": "B", "meta_json": "{}"}


async def test_gate_evaluate_unpersisted_is_async_probe_once_and_write_free(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    holder = HolderEvidenceDraft(
        sampled_token_accounts=3,
        distinct_non_curve_owners=2,
        top10_non_curve_owner_share_pct=40.0,
        holder_observed_at=10.0,
        unavailable_reason="",
        inputs_hash="a" * 64,
    )
    early_buyer = EarlyBuyerEvidenceDraft(
        checked_at=11.0,
        buyers=("BUYER_A", "BUYER_B"),
        unavailable_reason="",
        inputs_hash="b" * 64,
    )
    probe_results = {
        "onchain": [
            CheckResult(
                "holder_concentration", True, hard=True,
                detail={"holder_evidence_v1": asdict(holder)},
            ),
            CheckResult(
                "early_buyer_concentration", True, hard=True,
                detail={"early_buyer_evidence_v1": asdict(early_buyer)},
            ),
        ],
        "external": [PASS("rugcheck")],
        "honeypot": [PASS("honeypot")],
    }

    class ProbeOnce:
        def __init__(self):
            self.calls = []

        async def onchain(self, raw_token):
            self.calls.append(("onchain", raw_token))
            return probe_results["onchain"]

        async def external(self, raw_token):
            self.calls.append(("external", raw_token))
            return probe_results["external"]

        async def honeypot(self, raw_token):
            self.calls.append(("honeypot", raw_token))
            return probe_results["honeypot"]

    probes = ProbeOnce()
    clock_calls = []

    def completion_clock():
        clock_calls.append(tuple(name for name, _token in probes.calls))
        return 12.0

    raw_token = token(state="GRADUATED")
    statements = []
    conn.set_trace_callback(statements.append)
    draft = await SafetyGate(
        conn, probes=probes, clock=completion_clock,
    ).evaluate_unpersisted(raw_token)
    conn.set_trace_callback(None)

    expected_results = tuple(
        probe_results["onchain"]
        + probe_results["external"]
        + probe_results["honeypot"]
    )
    assert [name for name, _token in probes.calls] == [
        "onchain", "external", "honeypot",
    ]
    assert all(seen_token is raw_token for _name, seen_token in probes.calls)
    assert clock_calls == [("onchain", "external", "honeypot")]
    assert statements == []
    assert draft.mint == "M1"
    assert draft.raw_completed_at == 12.0
    assert draft.segment == "GRADUATED"
    assert draft.hard_fails == ()
    assert draft.risk_score == 0.0
    assert json.loads(draft.results_json) == json.loads(json.dumps(
        [asdict(result) for result in expected_results],
    ))
    assert draft.safety_inputs_hash == hashlib.sha256(
        json.dumps(
            [(result.name, result.reason, result.detail) for result in expected_results],
            sort_keys=True,
            default=str,
        ).encode()
    ).hexdigest()
    assert draft.holder == holder
    assert draft.early_buyer == early_buyer


async def test_gate_evaluate_unpersisted_rejects_malformed_token_before_probe(tmp_path):
    conn = open_db(tmp_path / "t.db")
    malformed_tokens = (
        object(),
        {},
        {"mint": None, "state": "CLIMBING"},
        {"mint": True, "state": "CLIMBING"},
        {"mint": " ", "state": "CLIMBING"},
        {"mint": "M" * 129, "state": "CLIMBING"},
        {"mint": "M1", "state": None},
        {"mint": "M1", "state": True},
        {"mint": "M1", "state": "climbing"},
        {"mint": "M1", "state": "TRENDING"},
    )

    for malformed_token in malformed_tokens:
        probes = FakeProbes(onchain=[])
        gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)

        with pytest.raises(ValueError, match="invalid safety token|must be a mapping"):
            await gate.evaluate_unpersisted(malformed_token)

        assert probes.calls == []


async def test_gate_evaluate_unpersisted_propagates_cancellation(tmp_path):
    conn = open_db(tmp_path / "t.db")

    class CancelledProbe:
        def __init__(self):
            self.calls = []

        async def onchain(self, raw_token):
            self.calls.append(("onchain", raw_token))
            raise asyncio.CancelledError

        async def external(self, raw_token):
            self.calls.append(("external", raw_token))
            return [PASS("rugcheck")]

        async def honeypot(self, raw_token):
            self.calls.append(("honeypot", raw_token))
            return [PASS("honeypot")]

    probes = CancelledProbe()
    clock_calls = []

    def completion_clock():
        clock_calls.append("clock")
        return 100.0

    raw_token = token(state="GRADUATED")
    statements = []
    conn.set_trace_callback(statements.append)
    with pytest.raises(asyncio.CancelledError):
        await SafetyGate(
            conn, probes=probes, clock=completion_clock,
        ).evaluate_unpersisted(raw_token)
    conn.set_trace_callback(None)

    assert [name for name, _ in probes.calls] == ["onchain"]
    assert probes.calls[0][1] is raw_token
    assert clock_calls == []
    assert statements == []


async def test_gate_persist_delegates_atomic_p3_evidence_helper(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    holder = HolderEvidenceDraft(
        sampled_token_accounts=3,
        distinct_non_curve_owners=2,
        top10_non_curve_owner_share_pct=40.0,
        holder_observed_at=10.0,
        unavailable_reason="",
        inputs_hash="a" * 64,
    )
    early_buyer = EarlyBuyerEvidenceDraft(
        checked_at=11.0,
        buyers=("BUYER_A", "BUYER_B"),
        unavailable_reason="",
        inputs_hash="b" * 64,
    )
    gate = SafetyGate(
        conn,
        probes=FakeProbes(
            onchain=[
                CheckResult(
                    "holder_concentration", True, hard=True,
                    detail={"holder_evidence_v1": asdict(holder)},
                ),
                CheckResult(
                    "early_buyer_concentration", True, hard=True,
                    detail={"early_buyer_evidence_v1": asdict(early_buyer)},
                ),
            ],
            external=[PASS("rugcheck")],
        ),
        clock=lambda: 12.0,
    )
    draft = await gate.evaluate_unpersisted(token())

    report = gate.persist(draft)

    assert report.report_id is not None
    assert report.mint == draft.mint
    assert report.checked_at > draft.raw_completed_at
    assert report.segment == draft.segment
    assert report.hard_fails == draft.hard_fails
    assert report.risk_score == draft.risk_score
    assert report.inputs_hash == draft.safety_inputs_hash
    assert tuple(asdict(result) for result in report.results) == tuple(
        json.loads(draft.results_json)
    )
    parent = conn.execute(
        "SELECT * FROM safety_reports WHERE id=?", (report.report_id,),
    ).fetchone()
    holders = conn.execute(
        "SELECT * FROM holder_evidence WHERE safety_report_id=?",
        (report.report_id,),
    ).fetchall()
    early_buyers = conn.execute(
        "SELECT * FROM early_buyer_reads WHERE safety_report_id=?",
        (report.report_id,),
    ).fetchall()
    assert parent is not None
    assert parent["checked_at"] == report.checked_at
    assert len(holders) == 1
    assert holders[0]["holder_observed_at"] == holder.holder_observed_at
    assert len(early_buyers) == 1
    assert early_buyers[0]["checked_at"] == early_buyer.checked_at
    assert conn.in_transaction is False


async def test_holder_source_time_precedes_delayed_report_time(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    wall = 10.0

    class Rpc:
        async def mint_info(self, mint):
            assert mint == "M1"
            return MintInfo(None, None, supply=1_000, decimals=6)

        async def largest_accounts(self, mint):
            assert mint == "M1"
            return [Holder("ATA", 100)]

        async def account_owners(self, addresses):
            assert addresses == ["ATA"]
            return {"ATA": "OWNER"}

    probes = LiveProbes(
        rpc_url="https://rpc.test",
        rpc_client=object(),
        ext_client=object(),
        jup_client=object(),
        conn=conn,
        cfg={
            "top10_holder_max_pct": 30.0,
            "dev_wallet_max_pct": 10.0,
            "early_buyers": {"enabled": False},
        },
        governors={},
        clock=lambda: wall,
    )
    probes._rpc = Rpc()

    async def delayed_external(raw_token):
        nonlocal wall
        assert raw_token is raw
        wall = 20.0
        return [PASS("rugcheck")]

    probes.external = delayed_external
    raw = token()
    gate = SafetyGate(conn, probes=probes, clock=lambda: wall)

    report = gate.persist(await gate.evaluate_unpersisted(raw))

    holder = conn.execute(
        "SELECT * FROM holder_evidence WHERE safety_report_id=?",
        (report.report_id,),
    ).fetchone()
    assert holder is not None
    assert holder["holder_observed_at"] == 10.0
    assert report.checked_at > 20.0
    assert holder["holder_observed_at"] < report.checked_at


async def test_early_buyer_source_time_precedes_delayed_report_time(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    wall = 10.0

    class Rpc:
        async def mint_info(self, mint):
            assert mint == "M1"
            return MintInfo(None, None, supply=1_000, decimals=6)

        async def largest_accounts(self, mint):
            assert mint == "M1"
            return [Holder("ATA", 100)]

        async def account_owners(self, addresses):
            assert addresses == ["ATA"]
            return {"ATA": "OWNER"}

    class EarlyReader:
        async def read(self, *, mint, bonding_curve_key):
            assert mint == "M1"
            assert bonding_curve_key == "B"
            return EarlyBuyerSnapshot(
                mint="M1",
                buyers=("OWNER",),
                signatures_scanned=1,
                transactions_scanned=1,
            )

    probes = LiveProbes(
        rpc_url="https://rpc.test",
        rpc_client=object(),
        ext_client=object(),
        jup_client=object(),
        conn=conn,
        cfg={
            "top10_holder_max_pct": 30.0,
            "dev_wallet_max_pct": 10.0,
            "early_buyers": {"enabled": True, "max_supply_pct": 25.0},
        },
        governors={},
        early_buyer_reader=EarlyReader(),
        clock=lambda: wall,
    )
    probes._rpc = Rpc()

    async def delayed_external(raw_token):
        nonlocal wall
        assert raw_token is raw
        wall = 20.0
        return [PASS("rugcheck")]

    probes.external = delayed_external
    raw = token()
    gate = SafetyGate(conn, probes=probes, clock=lambda: wall)

    report = gate.persist(await gate.evaluate_unpersisted(raw))

    early_buyer = conn.execute(
        "SELECT * FROM early_buyer_reads WHERE safety_report_id=?",
        (report.report_id,),
    ).fetchone()
    assert early_buyer is not None
    assert early_buyer["checked_at"] == 10.0
    assert report.checked_at > 20.0
    assert early_buyer["checked_at"] < report.checked_at


async def test_regressed_report_clock_allocates_parent_after_valid_child_sources(
    tmp_path,
):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)

    def evidence_results(holder_at, early_buyer_at):
        holder = HolderEvidenceDraft(
            sampled_token_accounts=3,
            distinct_non_curve_owners=2,
            top10_non_curve_owner_share_pct=40.0,
            holder_observed_at=holder_at,
            unavailable_reason="",
            inputs_hash="a" * 64,
        )
        early_buyer = EarlyBuyerEvidenceDraft(
            checked_at=early_buyer_at,
            buyers=("BUYER_A", "BUYER_B"),
            unavailable_reason="",
            inputs_hash="b" * 64,
        )
        return [
            CheckResult(
                "holder_concentration", True, hard=True,
                detail={"holder_evidence_v1": asdict(holder)},
            ),
            CheckResult(
                "early_buyer_concentration", True, hard=True,
                detail={"early_buyer_evidence_v1": asdict(early_buyer)},
            ),
        ]

    prior_gate = SafetyGate(
        conn,
        probes=FakeProbes(
            onchain=evidence_results(10.0, 11.0),
            external=[PASS("rugcheck")],
        ),
        clock=lambda: 20.0,
    )
    prior_report = prior_gate.persist(
        await prior_gate.evaluate_unpersisted(token())
    )

    async def persist_regressed(raw_completed_at, holder_source_at, early_source_at):
        gate = SafetyGate(
            conn,
            probes=FakeProbes(
                onchain=evidence_results(holder_source_at, early_source_at),
                external=[PASS("rugcheck")],
            ),
            clock=lambda: raw_completed_at,
        )
        draft = await gate.evaluate_unpersisted(token())
        report = gate.persist(draft)
        holder = conn.execute(
            "SELECT * FROM holder_evidence WHERE safety_report_id=?",
            (report.report_id,),
        ).fetchone()
        early_buyer = conn.execute(
            "SELECT * FROM early_buyer_reads WHERE safety_report_id=?",
            (report.report_id,),
        ).fetchone()
        assert draft.raw_completed_at == raw_completed_at
        assert holder is not None
        assert early_buyer is not None
        assert holder["holder_observed_at"] == holder_source_at
        assert early_buyer["checked_at"] == early_source_at
        return report

    holder_source_at = prior_report.checked_at + 10.0
    early_source_at = prior_report.checked_at + 5.0
    holder_dominant = await persist_regressed(
        5.0, holder_source_at, early_source_at,
    )
    assert holder_source_at > early_source_at
    assert holder_source_at > prior_report.checked_at
    assert holder_dominant.checked_at > 5.0
    assert holder_dominant.checked_at > holder_source_at
    assert holder_dominant.checked_at > early_source_at
    assert holder_dominant.checked_at > prior_report.checked_at

    holder_source_at = holder_dominant.checked_at + 5.0
    early_source_at = holder_dominant.checked_at + 10.0
    early_dominant = await persist_regressed(
        4.0, holder_source_at, early_source_at,
    )
    assert early_source_at > holder_source_at
    assert early_source_at > holder_dominant.checked_at
    assert early_dominant.checked_at > 4.0
    assert early_dominant.checked_at > early_source_at
    assert early_dominant.checked_at > holder_source_at
    assert early_dominant.checked_at > holder_dominant.checked_at

    holder_source_at = holder_dominant.checked_at
    early_source_at = holder_dominant.checked_at
    watermark_dominant = await persist_regressed(
        3.0, holder_source_at, early_source_at,
    )
    assert early_dominant.checked_at > holder_source_at
    assert early_dominant.checked_at > early_source_at
    assert watermark_dominant.checked_at > 3.0
    assert watermark_dominant.checked_at > early_dominant.checked_at
    assert watermark_dominant.checked_at > holder_source_at
    assert watermark_dominant.checked_at > early_source_at


def test_safety_gate_rejects_removed_evaluate_wrapper(tmp_path):
    conn = open_db(tmp_path / "t.db")
    gate = SafetyGate(conn, probes=FakeProbes(onchain=[]), clock=lambda: 100.0)

    assert "evaluate" not in SafetyGate.__dict__
    with pytest.raises(AttributeError):
        getattr(gate, "evaluate")


def test_safety_gate_direct_callers_use_evaluate_once():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    legacy_calls = [
        node.lineno
        for function in tree.body
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "evaluate"
    ]

    assert legacy_calls == []


def test_all_safety_gate_callers_use_final_interface():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    paths = sorted((root / "src" / "memebot").rglob("*.py"))
    paths += sorted((root / "tests").rglob("*.py"))
    paths += sorted((root / "scripts").rglob("*.py"))
    legacy_calls = []
    for path in paths:
        relative_path = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text())
        for top_level in tree.body:
            for call in (
                node for node in ast.walk(top_level)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "evaluate"
            ):
                legacy_calls.append((relative_path, call.lineno))

    assert legacy_calls == []


async def test_clean_climbing_passes(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[PASS("mint_authority"), PASS("holder_concentration")],
                        external=[PASS("rugcheck"), PASS("goplus")])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token()))
    assert rep.passed and rep.hard_fails == ()
    assert probes.calls == ["onchain", "external"]   # no honeypot for CLIMBING
    assert latest_safety_report(conn, "M1") is not None


async def test_onchain_hardfail_short_circuits_before_external(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[FAIL("mint_authority", "mint_authority_active")],
                        external=[PASS("rugcheck")])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token()))
    assert not rep.passed and "mint_authority_active" in rep.hard_fails
    assert probes.calls == ["onchain"]   # SHORT-CIRCUIT: external never called (budget saved)


async def test_graduated_runs_honeypot(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[PASS("mint_authority")],
                        external=[PASS("rugcheck")], honeypot=[PASS("honeypot")])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token(state="GRADUATED")))
    assert rep.passed and probes.calls == ["onchain", "external", "honeypot"]


async def test_unavailable_required_check_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[UNAVAIL("holder_concentration")])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token()))
    assert not rep.passed and any("check_unavailable" in h for h in rep.hard_fails)


async def test_external_hardfail_skips_honeypot(tmp_path):
    """A GRADUATED token whose onchain checks all pass but external hard-fails must
    never reach the honeypot probe — the same short-circuit budget-saving guard that
    protects external after an onchain hard-fail must also protect honeypot after an
    external hard-fail."""
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[PASS("mint_authority")],
                        external=[FAIL("rugcheck", "rugcheck_critical:danger")],
                        honeypot=[PASS("honeypot")])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token(state="GRADUATED")))
    assert not rep.passed
    assert probes.calls == ["onchain", "external"]   # honeypot never called


async def test_inputs_hash_deterministic(tmp_path):
    """inputs_hash must be a pure function of the check results: two evaluations of the
    same token with fresh FakeProbes instances returning equal CheckResults must hash
    identically. This is what lets downstream code correlate reports by inputs_hash
    without depending on wall-clock or object identity."""
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)

    def make_probes():
        return FakeProbes(onchain=[PASS("mint_authority"), PASS("holder_concentration")],
                          external=[PASS("rugcheck"), PASS("goplus")])

    gate1 = SafetyGate(conn, probes=make_probes(), clock=lambda: 100.0)
    rep1 = gate1.persist(await gate1.evaluate_unpersisted(token()))
    gate2 = SafetyGate(conn, probes=make_probes(), clock=lambda: 200.0)  # different clock
    rep2 = gate2.persist(await gate2.evaluate_unpersisted(token()))

    assert rep1.inputs_hash == rep2.inputs_hash
    assert rep1.checked_at != rep2.checked_at   # sanity: clocks actually differed


async def test_empty_onchain_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[], external=[PASS("rugcheck")])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token()))
    assert not rep.passed and any("no_checks_ran" in h for h in rep.hard_fails)
    assert probes.calls == ["onchain"]                 # short-circuits: external not called


async def test_empty_external_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[PASS("mint_authority")], external=[])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token()))
    assert not rep.passed and any("no_checks_ran" in h for h in rep.hard_fails)


async def test_graduated_empty_honeypot_is_fail_closed(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    probes = FakeProbes(onchain=[PASS("mint_authority")], external=[PASS("rugcheck")], honeypot=[])
    gate = SafetyGate(conn, probes=probes, clock=lambda: 100.0)
    rep = gate.persist(await gate.evaluate_unpersisted(token(state="GRADUATED")))
    assert not rep.passed and any("no_checks_ran" in h for h in rep.hard_fails)


async def test_probe_exception_is_fail_closed_not_crash(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)

    class RaisingProbes:
        calls = []

        async def onchain(self, t):
            raise RuntimeError("mapping bug")

        async def external(self, t): ...

        async def honeypot(self, t): ...

    gate = SafetyGate(conn, probes=RaisingProbes(), clock=lambda: 100.0)
    rep = gate.persist(
        await gate.evaluate_unpersisted(token())
    )  # must NOT raise
    assert not rep.passed and any("gate_error" in h for h in rep.hard_fails)
    assert latest_safety_report(conn, "M1") is not None  # still persisted (rug-forensics)


def test_early_buyer_synthetic_reason_mapping():
    def synthetic(reason):
        payload = {"checked_at": None, "buyers": (), "unavailable_reason": reason}
        inputs_hash = hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode()).hexdigest()
        return EarlyBuyerEvidenceDraft(
            checked_at=None,
            buyers=(),
            unavailable_reason=reason,
            inputs_hash=inputs_hash,
        )

    missing = synthetic("early_buyer_check_not_run")
    malformed = synthetic("early_buyer_evidence_malformed")
    valid_available = EarlyBuyerEvidenceDraft(
        checked_at=123.5,
        buyers=("BUYER_A", "BUYER_B"),
        unavailable_reason="",
        inputs_hash="a" * 64,
    )
    valid_unavailable = EarlyBuyerEvidenceDraft(
        checked_at=124.5,
        buyers=(),
        unavailable_reason="rpc_error",
        inputs_hash="b" * 64,
    )

    assert _extract_early_buyer_evidence([PASS("mint_authority")]) == missing
    assert _extract_early_buyer_evidence([]) == missing

    no_source_results = (
        (
            {"reason": "missing_bonding_curve_key"},
            synthetic("missing_bonding_curve_key"),
        ),
        (
            {
                "reason": "owner_resolution_incomplete",
                "unresolved": ["A_ATA", "Z_ATA"],
            },
            synthetic("owner_resolution_incomplete"),
        ),
        (
            {"reason": "reader_unavailable"},
            synthetic("reader_unavailable"),
        ),
    )
    for detail, expected in no_source_results:
        result = CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable", detail=detail, available=False,
        )
        assert _extract_early_buyer_evidence([result]) == expected

    available_result = CheckResult(
        "early_buyer_concentration", True, hard=True,
        detail={"early_buyer_evidence_v1": asdict(valid_available)},
    )
    unavailable_result = CheckResult(
        "early_buyer_concentration", False, hard=True,
        reason="check_unavailable", available=False,
        detail={"early_buyer_evidence_v1": asdict(valid_unavailable)},
    )
    assert _extract_early_buyer_evidence([
        PASS("mint_authority"), available_result, PASS("rugcheck"),
    ]) == valid_available
    assert _extract_early_buyer_evidence([unavailable_result]) == valid_unavailable
    for checked_at, inputs_hash in (
        (0.0, "c" * 64),
        (4_102_444_800.0, "d" * 64),
    ):
        endpoint_draft = EarlyBuyerEvidenceDraft(
            checked_at=checked_at,
            buyers=("BUYER_A",),
            unavailable_reason="",
            inputs_hash=inputs_hash,
        )
        endpoint_result = CheckResult(
            "early_buyer_concentration", True, hard=True,
            detail={"early_buyer_evidence_v1": asdict(endpoint_draft)},
        )
        assert _extract_early_buyer_evidence([endpoint_result]) == endpoint_draft
    for index, reason in enumerate(("no_signatures", "no_matching_buy_events"), start=1):
        draft = EarlyBuyerEvidenceDraft(
            checked_at=124.5 + index,
            buyers=(),
            unavailable_reason=reason,
            inputs_hash=str(index) * 64,
        )
        result = CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable", available=False,
            detail={"early_buyer_evidence_v1": asdict(draft)},
        )
        assert _extract_early_buyer_evidence([result]) == draft

    for index, reason in enumerate((
        "unknown_reason",
        "missing_bonding_curve_key",
        "owner_resolution_incomplete",
        "reader_unavailable",
    ), start=1):
        invalid_embedded_reason = EarlyBuyerEvidenceDraft(
            checked_at=127.5 + index,
            buyers=(),
            unavailable_reason=reason,
            inputs_hash=f"{index:x}" * 64,
        )
        assert _extract_early_buyer_evidence([
            CheckResult(
                "early_buyer_concentration", False, hard=True,
                reason="check_unavailable", available=False,
                detail={
                    "early_buyer_evidence_v1": asdict(invalid_embedded_reason),
                },
            ),
        ]) == malformed

    assert _extract_early_buyer_evidence((
        PASS("mint_authority"), available_result,
    )) == valid_available
    assert _extract_early_buyer_evidence([object()]) == malformed
    assert _extract_early_buyer_evidence([PASS("mint_authority"), object()]) == malformed
    assert _extract_early_buyer_evidence("not-results") == malformed
    assert _extract_early_buyer_evidence(None) == malformed

    malformed_payloads = (
        {},
        {**asdict(valid_available), "extra": "field"},
        {**asdict(valid_available), "checked_at": True},
        {**asdict(valid_available), "checked_at": float("nan")},
        {**asdict(valid_available), "checked_at": float("inf")},
        {**asdict(valid_available), "checked_at": -0.1},
        {**asdict(valid_available), "checked_at": 4_102_444_800.1},
        {**asdict(valid_available), "checked_at": "123.5"},
        {**asdict(valid_available), "buyers": ["BUYER_A"]},
        {**asdict(valid_available), "buyers": "BUYER_A"},
        {**asdict(valid_available), "buyers": (123,)},
        {**asdict(valid_available), "buyers": ("BUYER_A", "BUYER_A")},
        {**asdict(valid_available), "buyers": ("",)},
        {**asdict(valid_available), "buyers": (" ",)},
        {**asdict(valid_available), "unavailable_reason": "unknown_reason"},
        {**asdict(valid_available), "unavailable_reason": ["rpc_error"]},
        {
            **asdict(valid_unavailable),
            "unavailable_reason": "missing_bonding_curve_key",
        },
        {**asdict(valid_unavailable), "unavailable_reason": "reader_unavailable"},
        {**asdict(valid_available), "buyers": (), "unavailable_reason": ""},
        {**asdict(valid_available), "unavailable_reason": "rpc_error"},
        {**asdict(valid_available), "inputs_hash": "A" * 64},
        {**asdict(valid_available), "inputs_hash": "a" * 63},
        {**asdict(valid_available), "inputs_hash": 123},
    )
    for payload in malformed_payloads:
        result = CheckResult(
            "early_buyer_concentration", True, hard=True,
            detail={"early_buyer_evidence_v1": payload},
        )
        assert _extract_early_buyer_evidence([result]) == malformed

    assert _extract_early_buyer_evidence([
        available_result, unavailable_result,
    ]) == malformed
    assert _extract_early_buyer_evidence([
        CheckResult("early_buyer_concentration", False, hard=True),
    ]) == malformed
    assert _extract_early_buyer_evidence([
        CheckResult("early_buyer_concentration", False, hard=True, detail=None),
    ]) == malformed

    no_source_near_misses = (
        CheckResult(
            "early_buyer_concentration", True, hard=True,
            reason="check_unavailable", detail={"reason": "reader_unavailable"},
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=False,
            reason="check_unavailable", detail={"reason": "reader_unavailable"},
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="reader_unavailable", detail={"reason": "reader_unavailable"},
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable", detail={"reason": "reader_unavailable"},
            available=True,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable",
            detail={"reason": "missing_bonding_curve_key", "extra": True},
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable",
            detail={"reason": "owner_resolution_incomplete", "unresolved": []},
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable",
            detail={
                "reason": "owner_resolution_incomplete",
                "unresolved": ["Z_ATA", "A_ATA"],
            },
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable",
            detail={
                "reason": "owner_resolution_incomplete",
                "unresolved": ["A_ATA", "A_ATA"],
            },
            available=False,
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable",
            detail={
                "reason": "owner_resolution_incomplete",
                "unresolved": ["A_ATA", ""],
            },
            available=False,
        ),
    )
    for result in no_source_near_misses:
        assert _extract_early_buyer_evidence([result]) == malformed

    embedded_status_near_misses = (
        CheckResult(
            "early_buyer_concentration", True, hard=False,
            detail={"early_buyer_evidence_v1": asdict(valid_available)},
        ),
        CheckResult(
            "early_buyer_concentration", True, hard=True, reason="unexpected",
            detail={"early_buyer_evidence_v1": asdict(valid_available)},
        ),
        CheckResult(
            "early_buyer_concentration", True, hard=True, available=False,
            detail={"early_buyer_evidence_v1": asdict(valid_available)},
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            detail={"early_buyer_evidence_v1": asdict(valid_available)},
        ),
        CheckResult(
            "early_buyer_concentration", True, hard=True,
            reason="check_unavailable", available=False,
            detail={"early_buyer_evidence_v1": asdict(valid_unavailable)},
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=False,
            reason="check_unavailable", available=False,
            detail={"early_buyer_evidence_v1": asdict(valid_unavailable)},
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="rpc_error", available=False,
            detail={"early_buyer_evidence_v1": asdict(valid_unavailable)},
        ),
        CheckResult(
            "early_buyer_concentration", False, hard=True,
            reason="check_unavailable", available=True,
            detail={"early_buyer_evidence_v1": asdict(valid_unavailable)},
        ),
    )
    for result in embedded_status_near_misses:
        assert _extract_early_buyer_evidence([result]) == malformed

    concentration_fail = CheckResult(
        "early_buyer_concentration", False, hard=True,
        reason="early_buyer_concentration", available=True,
        detail={"early_buyer_evidence_v1": asdict(valid_available)},
    )
    assert _extract_early_buyer_evidence([concentration_fail]) == valid_available
