import sqlite3

import pytest

from memebot.store import mark_clean_shutdown, open_db, record_boot

LEDGER_TABLES = ["decisions", "paper_trades", "outcomes", "regime_log"]
EVIDENCE_TABLES = ["wallet_pnl_events", "early_buyer_reads"]


def _open_v4_fixture(path):
    import memebot.store as store

    conn = sqlite3.connect(path)
    conn.create_function(
        "p3_fee_sum", 1, store.p3_fee_sum_json, deterministic=True,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA recursive_triggers=ON")
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version < 1:
        conn.executescript(store.SCHEMA_V1)
        conn.executescript(store._append_only_triggers())
        conn.execute("PRAGMA user_version=1")
        conn.commit()
    if version < 2:
        try:
            conn.execute(store.SCHEMA_V2)
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise
        conn.execute("PRAGMA user_version=2")
        conn.commit()
    if version < 3:
        try:
            conn.execute(store.SCHEMA_V3)
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise
        conn.execute("PRAGMA user_version=3")
        conn.commit()
    if version < 4:
        conn.executescript(store.SCHEMA_V4)
        conn.executescript(store._append_only_triggers(tuple(EVIDENCE_TABLES)))
        conn.execute("PRAGMA user_version=4")
        conn.commit()
    return conn


def test_v5_additive_column_manifest_and_healer(tmp_path):
    from typing import Any, cast

    import memebot.store as store

    expected = (
        (
            "tokens",
            "p3_identity_ingested_at",
            "ALTER TABLE tokens ADD COLUMN p3_identity_ingested_at REAL;",
        ),
        (
            "tokens",
            "curve_progress_observed_at",
            "ALTER TABLE tokens ADD COLUMN curve_progress_observed_at REAL;",
        ),
        (
            "tokens",
            "curve_progress_source_wall",
            "ALTER TABLE tokens ADD COLUMN curve_progress_source_wall REAL;",
        ),
        (
            "tokens",
            "curve_progress_source_boot_id",
            "ALTER TABLE tokens ADD COLUMN curve_progress_source_boot_id INTEGER;",
        ),
        (
            "tokens",
            "curve_progress_source_seq",
            "ALTER TABLE tokens ADD COLUMN curve_progress_source_seq INTEGER;",
        ),
        (
            "paper_trades",
            "canonical_recheck_id",
            "ALTER TABLE paper_trades ADD COLUMN canonical_recheck_id INTEGER\n"
            "  REFERENCES canonical_rechecks(id);",
        ),
        (
            "paper_trades",
            "canonical_proof_hash",
            "ALTER TABLE paper_trades ADD COLUMN canonical_proof_hash TEXT;",
        ),
        (
            "paper_trades",
            "p3_entry_execution_id",
            "ALTER TABLE paper_trades ADD COLUMN p3_entry_execution_id INTEGER\n"
            "  REFERENCES paper_entry_executions(id);",
        ),
        (
            "outcomes",
            "p3_exit_trade_id",
            "ALTER TABLE outcomes ADD COLUMN p3_exit_trade_id INTEGER\n"
            "  REFERENCES paper_trades(id);",
        ),
        (
            "early_buyer_reads",
            "safety_report_id",
            "ALTER TABLE early_buyer_reads ADD COLUMN safety_report_id INTEGER\n"
            "  REFERENCES safety_reports(id);",
        ),
    )
    assert store.V5_ADDITIVE_COLUMNS == expected

    for table, column, sql in expected:
        conn = sqlite3.connect(":memory:")
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
        conn.execute(sql)
        assert column in {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        conn.close()

    conn = _open_v4_fixture(tmp_path / "v4.db")
    conn.execute(expected[0][2])
    store._apply_v5_additive_columns(conn)
    for table, column, _ in expected:
        assert column in {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    store._apply_v5_additive_columns(conn)
    conn.close()

    class FailingConnection:
        def __init__(self, message):
            self.message = message
            self.executed = []

        def execute(self, sql):
            self.executed.append(sql)
            raise sqlite3.OperationalError(self.message)

    for message in ("database is locked", "duplicate column name: wrong_column"):
        failing = FailingConnection(message)
        with pytest.raises(sqlite3.OperationalError, match=message):
            store._apply_v5_additive_columns(cast(Any, failing))
        assert failing.executed == [expected[0][2]]


def test_v5_table_ddl_manifest_executes_in_dependency_order(tmp_path):
    from hashlib import sha256

    import memebot.store as store

    expected = (
        ("holder_evidence", "86e523316c59c4885bf5d2373d22ce6d3fcf92e1fd7b371adea00eb57961d6d8"),
        ("creator_reputation_events", "67cf26e4d875f00154b9dafb01cdb4cf9bd1186ca504066ecd334cfe19cdd311"),
        ("creator_reputation_current", "22d5e11202f35b02e138cb5d191a183f07499525d0364eeb22b0ce798abb129a"),
        ("p3_causal_clock", "54828cea6a4dd51de44a561f47f73aa07e2eff908d268761df072db391028bb3"),
        ("wallet_pnl_summary", "21361a76656794e39a7e9d18d2057c71d41a10c5543667964fa7d5628c0c1658"),
        ("canonical_observations", "474d6752ec2b982ade860b596280f76d6162a27e41160e7c6640fd8817dd7c39"),
        ("canonical_generations", "00d11a7fe4eb6e4e43836745110a4b769b88be4c699d4da387be0fd5780b0a85"),
        ("canonical_rechecks", "64dc9bc2f0c13314dbbc3baf062c8fe286d612df0f74edb42c5cc69d2a6044ea"),
        ("paper_entry_executions", "791c10b8b7f553eebbf4b07127d2c64b05d05469da124c78a3cac4602750fd1e"),
        ("p3_position_current", "aefd7ae8b19297430f2a2fa7334928d96cfdd6f4ec97dd03f5c893be13974c6a"),
        ("canonical_pending_current", "0077befc1dcc21d41063a8f7208337c9f7facdebb2293d70b47be05de9ea0361"),
    )
    assert tuple(
        (name, sha256(sql.encode()).hexdigest()) for name, sql in store.V5_TABLE_DDL
    ) == expected

    conn = _open_v4_fixture(tmp_path / "v4.db")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
    store._apply_v5_additive_columns(conn)
    new_table_names = {name for name, _ in expected}
    existing_tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    created = set()
    for name, sql in store.V5_TABLE_DDL:
        assert name not in existing_tables
        conn.execute(sql)
        created.add(name)
        referenced_new_tables = {
            row["table"]
            for row in conn.execute(f"PRAGMA foreign_key_list({name})")
            if row["table"] in new_table_names
        }
        assert referenced_new_tables <= created

    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"):
        conn.execute(
            "INSERT INTO canonical_pending_current"
            "(observation_id,decision_id,horizons_json,full_mask) VALUES (999,999,'[1]',1)"
        )
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()


def test_v5_index_ddl_manifest_executes_after_tables_and_columns(tmp_path):
    import memebot.store as store

    expected = (
        (
            "outcomes_p3_exit_trade_unique_idx",
            "CREATE UNIQUE INDEX IF NOT EXISTS outcomes_p3_exit_trade_unique_idx\n"
            "  ON outcomes(p3_exit_trade_id) WHERE p3_exit_trade_id IS NOT NULL;",
        ),
        (
            "safety_reports_mint_latest_idx",
            "CREATE INDEX IF NOT EXISTS safety_reports_mint_latest_idx\n"
            "  ON safety_reports(mint, id DESC);",
        ),
        (
            "wallet_pnl_events_at_idx",
            "CREATE INDEX IF NOT EXISTS wallet_pnl_events_at_idx\n"
            "  ON wallet_pnl_events(at, id);",
        ),
        (
            "early_buyer_report_unique",
            "CREATE UNIQUE INDEX IF NOT EXISTS early_buyer_report_unique\n"
            "  ON early_buyer_reads(safety_report_id)\n"
            "  WHERE safety_report_id IS NOT NULL;",
        ),
        (
            "creator_reputation_current_bounded_idx",
            "CREATE INDEX IF NOT EXISTS creator_reputation_current_bounded_idx\n"
            "  ON creator_reputation_current(creator, observed_at, event_id, mint);",
        ),
        (
            "canonical_observations_decision_idx",
            "CREATE INDEX IF NOT EXISTS canonical_observations_decision_idx\n"
            "  ON canonical_observations(decision_id,id);",
        ),
        (
            "canonical_rechecks_decision_idx",
            "CREATE INDEX IF NOT EXISTS canonical_rechecks_decision_idx\n"
            "  ON canonical_rechecks(decision_id,attempt);",
        ),
        (
            "canonical_rechecks_decision_status_idx",
            "CREATE INDEX IF NOT EXISTS canonical_rechecks_decision_status_idx\n"
            "  ON canonical_rechecks(decision_id,status,id);",
        ),
        (
            "p3_position_current_open_idx",
            "CREATE INDEX IF NOT EXISTS p3_position_current_open_idx\n"
            "  ON p3_position_current(decision_id) WHERE sold_qty<bought_qty;",
        ),
        (
            "canonical_pending_incomplete_idx",
            "CREATE INDEX IF NOT EXISTS canonical_pending_incomplete_idx\n"
            "  ON canonical_pending_current(observation_id)\n"
            "  WHERE completed_mask<>full_mask;",
        ),
        (
            "canonical_outcome_horizon_unique",
            "CREATE UNIQUE INDEX IF NOT EXISTS canonical_outcome_horizon_unique\n"
            "ON outcomes(ref_id, json_extract(detail_json,'$.horizon_s'))\n"
            "WHERE ref_kind='canonical_observation';",
        ),
    )
    assert store.V5_INDEX_DDL == expected

    conn = _open_v4_fixture(tmp_path / "v4.db")
    try:
        expected_names = {name for name, _ in expected}
        before = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_schema WHERE type='index'")
        }
        assert expected_names.isdisjoint(before)

        store._apply_v5_additive_columns(conn)
        for _, sql in store.V5_TABLE_DDL:
            conn.execute(sql)

        table_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        }
        assert {name for name, _ in store.V5_TABLE_DDL} <= table_names
        for table, column, _ in store.V5_ADDITIVE_COLUMNS:
            assert column in {
                row["name"] for row in conn.execute(f"PRAGMA table_info({table})")
            }

        for _, sql in store.V5_INDEX_DDL:
            conn.execute(sql)

        created = {
            row["name"]: row["tbl_name"]
            for row in conn.execute(
                "SELECT name,tbl_name FROM sqlite_schema WHERE type='index'"
                f" AND name IN ({','.join('?' for _ in expected_names)})",
                tuple(expected_names),
            )
        }
        assert created == {
            "outcomes_p3_exit_trade_unique_idx": "outcomes",
            "safety_reports_mint_latest_idx": "safety_reports",
            "wallet_pnl_events_at_idx": "wallet_pnl_events",
            "early_buyer_report_unique": "early_buyer_reads",
            "creator_reputation_current_bounded_idx": "creator_reputation_current",
            "canonical_observations_decision_idx": "canonical_observations",
            "canonical_rechecks_decision_idx": "canonical_rechecks",
            "canonical_rechecks_decision_status_idx": "canonical_rechecks",
            "p3_position_current_open_idx": "p3_position_current",
            "canonical_pending_incomplete_idx": "canonical_pending_current",
            "canonical_outcome_horizon_unique": "outcomes",
        }
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_v5_legacy_safety_validator_unit_contract(tmp_path):
    import json

    import memebot.store as store

    def insert_report(conn, *, row_id, **overrides):
        report = {
            "mint": "MINT",
            "checked_at": 1.0,
            "hard_fails_json": '["authority active"]',
            "risk_score": 50.0,
            "inputs_hash": "a" * 64,
        }
        report.update(overrides)
        conn.execute(
            "INSERT INTO safety_reports"
            "(id,mint,checked_at,hard_fails_json,risk_score,inputs_hash)"
            " VALUES (?,?,?,?,?,?)",
            (row_id, *(report[column] for column in report)),
        )
        conn.commit()

    empty = _open_v4_fixture(tmp_path / "empty.db")
    store._validate_v5_legacy_safety_reports(empty)
    assert empty.execute("PRAGMA user_version").fetchone()[0] == 4
    empty.close()

    valid = _open_v4_fixture(tmp_path / "valid.db")
    max_json = json.dumps(["x" * 8188], separators=(",", ":"))
    assert len(max_json) == 8192
    insert_report(
        valid,
        row_id=7,
        mint="m" * 128,
        checked_at=0,
        hard_fails_json=max_json,
        risk_score=0,
        inputs_hash="0" * 64,
    )
    insert_report(
        valid,
        row_id=8,
        mint=" M ",
        checked_at=4102444800.0,
        hard_fails_json='["ok","later"]',
        risk_score=100.0,
        inputs_hash="abcdef0123456789" * 4,
    )
    before = tuple(valid.iterdump())
    before_changes = valid.total_changes
    store._validate_v5_legacy_safety_reports(valid)
    assert tuple(valid.iterdump()) == before
    assert valid.total_changes == before_changes
    assert valid.execute("PRAGMA user_version").fetchone()[0] == 4
    assert valid.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='trigger'"
        " AND name='p3_safety_report_shape_guard'"
    ).fetchone() is None
    valid.close()

    invalid_reports = (
        {"mint": b"MINT"},
        {"mint": " "},
        {"mint": "m" * 129},
        {"checked_at": b"1"},
        {"checked_at": -1},
        {"checked_at": 4102444800.1},
        {"checked_at": float("inf")},
        {"hard_fails_json": json.dumps(["x" * 8189], separators=(",", ":"))},
        {"hard_fails_json": "["},
        {"hard_fails_json": "{}"},
        {"hard_fails_json": "[1]"},
        {"hard_fails_json": '["ok"," "]'},
        {"risk_score": b"50"},
        {"risk_score": -0.1},
        {"risk_score": 100.1},
        {"risk_score": float("inf")},
        {"inputs_hash": b"a" * 64},
        {"inputs_hash": "a" * 63},
        {"inputs_hash": "A" * 64},
        {"inputs_hash": "g" * 64},
    )
    for index, overrides in enumerate(invalid_reports):
        path = tmp_path / f"invalid-{index}.db"
        conn = _open_v4_fixture(path)
        insert_report(conn, row_id=7, **overrides)
        conn.close()

        # The validator is migration-only: ordinary v4 reopen neither calls it nor activates v5.
        conn = _open_v4_fixture(path)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        before = tuple(conn.iterdump())
        with pytest.raises(ValueError, match=r"legacy safety_reports row id=7"):
            store._validate_v5_legacy_safety_reports(conn)
        assert tuple(conn.iterdump()) == before
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='trigger'"
            " AND name='p3_safety_report_shape_guard'"
        ).fetchone() is None
        conn.close()


def test_v5_legacy_wallet_validator_unit_contract(tmp_path):
    import json
    import math

    import memebot.store as store

    def insert_event(conn, *, row_id, **overrides):
        event = {
            "at": 1.0,
            "wallet": "WALLET",
            "mint": "MINT",
            "realized_pnl_sol": 1.0,
            "source": "test",
            "detail_json": '{"ok":true}',
        }
        event.update(overrides)
        conn.execute(
            "INSERT INTO wallet_pnl_events"
            "(id,at,wallet,mint,realized_pnl_sol,source,detail_json)"
            " VALUES (?,?,?,?,?,?,?)",
            (row_id, *(event[column] for column in event)),
        )
        conn.commit()

    def assert_v4_inert(conn):
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='trigger'"
            " AND name='p3_wallet_pnl_shape_guard'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table'"
            " AND name='wallet_pnl_summary'"
        ).fetchone() is None

    empty = _open_v4_fixture(tmp_path / "empty-wallet.db")
    before = tuple(empty.iterdump())
    before_changes = empty.total_changes
    store._validate_v5_legacy_wallet_pnl_events(empty)
    assert tuple(empty.iterdump()) == before
    assert empty.total_changes == before_changes
    assert_v4_inert(empty)
    empty.close()

    valid = _open_v4_fixture(tmp_path / "valid-wallet.db")
    max_detail_json = json.dumps(
        {"k": "x" * 65528}, separators=(",", ":")
    )
    assert len(max_detail_json) == 65536
    insert_event(
        valid,
        row_id=7,
        at=0,
        wallet="w" * 128,
        mint=" M ",
        realized_pnl_sol=-1000000000000,
        source="s" * 64,
        detail_json=max_detail_json,
    )
    insert_event(
        valid,
        row_id=8,
        at=4102444800.0,
        wallet=" W ",
        mint="m" * 128,
        realized_pnl_sol=1000000000000.0,
        source=" source ",
        detail_json="{}",
    )
    before = tuple(valid.iterdump())
    before_changes = valid.total_changes
    store._validate_v5_legacy_wallet_pnl_events(valid)
    assert tuple(valid.iterdump()) == before
    assert valid.total_changes == before_changes
    assert_v4_inert(valid)
    valid.close()

    too_large_detail_json = json.dumps(
        {"k": "x" * 65529}, separators=(",", ":")
    )
    assert len(too_large_detail_json) == 65537
    invalid_events = (
        ("at_blob", {"at": b"1"}, "at"),
        ("at_below", {"at": math.nextafter(0.0, -math.inf)}, None),
        (
            "at_above",
            {"at": math.nextafter(4102444800.0, math.inf)},
            None,
        ),
        ("wallet_blob", {"wallet": b"WALLET"}, "wallet"),
        ("wallet_empty", {"wallet": " "}, None),
        ("wallet_long", {"wallet": "w" * 129}, None),
        ("mint_blob", {"mint": b"MINT"}, "mint"),
        ("mint_empty", {"mint": " "}, None),
        ("mint_long", {"mint": "m" * 129}, None),
        (
            "pnl_blob",
            {"realized_pnl_sol": b"1"},
            "realized_pnl_sol",
        ),
        (
            "pnl_below",
            {"realized_pnl_sol": math.nextafter(-1000000000000.0, -math.inf)},
            None,
        ),
        (
            "pnl_above",
            {"realized_pnl_sol": math.nextafter(1000000000000.0, math.inf)},
            None,
        ),
        ("source_blob", {"source": b"test"}, "source"),
        ("source_empty", {"source": " "}, None),
        ("source_long", {"source": "s" * 65}, None),
        ("detail_too_large", {"detail_json": too_large_detail_json}, None),
        ("detail_malformed", {"detail_json": "["}, None),
        ("detail_non_object", {"detail_json": "[]"}, None),
    )
    for label, overrides, blob_column in invalid_events:
        path = tmp_path / f"invalid-wallet-{label}.db"
        conn = _open_v4_fixture(path)
        insert_event(conn, row_id=7, **overrides)
        if blob_column is not None:
            assert conn.execute(
                f"SELECT typeof({blob_column}) FROM wallet_pnl_events WHERE id=7"
            ).fetchone()[0] == "blob"
        conn.close()

        # The validator is migration-only: ordinary v4 reopen neither calls it nor activates v5.
        conn = _open_v4_fixture(path)
        assert_v4_inert(conn)
        malformed = conn.execute(
            "SELECT wallet,mint FROM wallet_pnl_events WHERE id=7"
        ).fetchone()
        before = tuple(conn.iterdump())
        before_changes = conn.total_changes
        with pytest.raises(ValueError) as exc_info:
            store._validate_v5_legacy_wallet_pnl_events(conn)
        assert str(exc_info.value) == (
            "invalid legacy wallet_pnl_events row id=7"
            f" wallet={malformed[0]!r} mint={malformed[1]!r}"
        )
        assert tuple(conn.iterdump()) == before
        assert conn.total_changes == before_changes
        assert_v4_inert(conn)
        conn.close()

    later = _open_v4_fixture(tmp_path / "later-invalid-wallet.db")
    insert_event(later, row_id=7)
    insert_event(later, row_id=8, wallet=b"bad-wallet")
    insert_event(later, row_id=9, mint=" ")
    later.close()

    # A valid first row must not hide later corruption; report the first malformed ID.
    later = _open_v4_fixture(tmp_path / "later-invalid-wallet.db")
    before = tuple(later.iterdump())
    before_changes = later.total_changes
    with pytest.raises(
        ValueError,
        match=r"^invalid legacy wallet_pnl_events row id=8 ",
    ):
        store._validate_v5_legacy_wallet_pnl_events(later)
    assert tuple(later.iterdump()) == before
    assert later.total_changes == before_changes
    assert_v4_inert(later)
    later.close()


def test_v5_legacy_early_buyer_validator_unit_contract(tmp_path):
    import json
    import math

    import memebot.store as store

    unavailable_reasons = (
        "rpc_error",
        "no_signatures",
        "no_matching_buy_events",
        "missing_bonding_curve_key",
        "owner_resolution_incomplete",
        "reader_unavailable",
        "early_buyer_check_not_run",
        "early_buyer_evidence_malformed",
    )

    def insert_read(conn, *, row_id, **overrides):
        read = {
            "mint": "MINT",
            "checked_at": 1.0,
            "buyers_json": '["BUYER"]',
            "unavailable_reason": "",
            "inputs_hash": "a" * 64,
        }
        read.update(overrides)
        conn.execute(
            "INSERT INTO early_buyer_reads"
            "(id,mint,checked_at,buyers_json,unavailable_reason,inputs_hash)"
            " VALUES (?,?,?,?,?,?)",
            (row_id, *(read[column] for column in read)),
        )
        conn.commit()

    def assert_v4_inert(conn):
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='trigger'"
            " AND name='p3_early_buyer_shape_guard'"
        ).fetchone() is None
        assert "safety_report_id" not in {
            row[1] for row in conn.execute("PRAGMA table_info(early_buyer_reads)")
        }
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table'"
            " AND name='holder_evidence'"
        ).fetchone() is None

    empty = _open_v4_fixture(tmp_path / "empty-early-buyer.db")
    before = tuple(empty.iterdump())
    before_changes = empty.total_changes
    store._validate_v5_legacy_early_buyer_reads(empty)
    assert tuple(empty.iterdump()) == before
    assert empty.total_changes == before_changes
    assert_v4_inert(empty)
    empty.close()

    valid = _open_v4_fixture(tmp_path / "valid-early-buyer.db")
    max_buyers_json = json.dumps(["x" * 8188], separators=(",", ":"))
    assert len(max_buyers_json) == 8192
    insert_read(
        valid,
        row_id=7,
        mint=" M ",
        checked_at=0,
        buyers_json=max_buyers_json,
        inputs_hash="0" * 64,
    )
    insert_read(
        valid,
        row_id=8,
        mint="m" * 128,
        checked_at=4102444800.0,
        buyers_json='["first"," later "]',
        inputs_hash="abcdef0123456789" * 4,
    )
    for offset, reason in enumerate(unavailable_reasons, start=9):
        insert_read(
            valid,
            row_id=offset,
            buyers_json="[]",
            unavailable_reason=reason,
        )
    before = tuple(valid.iterdump())
    before_changes = valid.total_changes
    store._validate_v5_legacy_early_buyer_reads(valid)
    assert tuple(valid.iterdump()) == before
    assert valid.total_changes == before_changes
    assert_v4_inert(valid)
    valid.close()

    too_large_buyers_json = json.dumps(["x" * 8189], separators=(",", ":"))
    assert len(too_large_buyers_json) == 8193
    invalid_reads = (
        ("mint_blob", {"mint": b"MINT"}, "mint"),
        ("mint_empty", {"mint": " "}, None),
        ("mint_long", {"mint": "m" * 129}, None),
        ("checked_at_blob", {"checked_at": b"1"}, "checked_at"),
        (
            "checked_at_below",
            {"checked_at": math.nextafter(0.0, -math.inf)},
            None,
        ),
        (
            "checked_at_above",
            {"checked_at": math.nextafter(4102444800.0, math.inf)},
            None,
        ),
        ("buyers_too_large", {"buyers_json": too_large_buyers_json}, None),
        ("buyers_blob_malformed", {"buyers_json": b"\x80"}, "buyers_json"),
        ("buyers_malformed", {"buyers_json": "["}, None),
        ("buyers_non_array", {"buyers_json": "{}"}, None),
        ("buyers_non_text", {"buyers_json": "[1]"}, None),
        ("buyers_later_non_text", {"buyers_json": '["ok",1]'}, None),
        ("buyers_blank", {"buyers_json": '["ok"," "]'}, None),
        ("available_empty", {"buyers_json": "[]"}, None),
        (
            "unavailable_nonempty",
            {"unavailable_reason": "rpc_error"},
            None,
        ),
        (
            "reason_outside_domain",
            {"buyers_json": "[]", "unavailable_reason": "unknown"},
            None,
        ),
        (
            "reason_blob",
            {"buyers_json": "[]", "unavailable_reason": b"rpc_error"},
            "unavailable_reason",
        ),
        ("hash_blob", {"inputs_hash": b"a" * 64}, "inputs_hash"),
        ("hash_short", {"inputs_hash": "a" * 63}, None),
        ("hash_uppercase", {"inputs_hash": "A" * 64}, None),
        ("hash_outside_domain", {"inputs_hash": "g" * 64}, None),
    )
    for label, overrides, blob_column in invalid_reads:
        path = tmp_path / f"invalid-early-buyer-{label}.db"
        conn = _open_v4_fixture(path)
        insert_read(conn, row_id=7, **overrides)
        if blob_column is not None:
            assert conn.execute(
                f"SELECT typeof({blob_column}) FROM early_buyer_reads WHERE id=7"
            ).fetchone()[0] == "blob"
        conn.close()

        # The validator is migration-only: ordinary v4 reopen neither calls it nor activates v5.
        conn = _open_v4_fixture(path)
        assert_v4_inert(conn)
        malformed_mint = conn.execute(
            "SELECT mint FROM early_buyer_reads WHERE id=7"
        ).fetchone()[0]
        before = tuple(conn.iterdump())
        before_changes = conn.total_changes
        with pytest.raises(ValueError) as exc_info:
            store._validate_v5_legacy_early_buyer_reads(conn)
        assert str(exc_info.value) == (
            f"invalid legacy early_buyer_reads row id=7 mint={malformed_mint!r}"
        )
        assert tuple(conn.iterdump()) == before
        assert conn.total_changes == before_changes
        assert_v4_inert(conn)
        conn.close()

    later = _open_v4_fixture(tmp_path / "later-invalid-early-buyer.db")
    insert_read(later, row_id=7)
    insert_read(later, row_id=8, buyers_json="[]", unavailable_reason="rpc_error")
    insert_read(later, row_id=9, buyers_json='["ok",1]')
    insert_read(later, row_id=10, mint=" ")
    later.close()

    # A valid prefix must not hide later corruption; report the first malformed stable ID and mint.
    later = _open_v4_fixture(tmp_path / "later-invalid-early-buyer.db")
    before = tuple(later.iterdump())
    before_changes = later.total_changes
    with pytest.raises(ValueError) as exc_info:
        store._validate_v5_legacy_early_buyer_reads(later)
    assert str(exc_info.value) == "invalid legacy early_buyer_reads row id=9 mint='MINT'"
    assert tuple(later.iterdump()) == before
    assert later.total_changes == before_changes
    assert_v4_inert(later)
    later.close()


def test_v5_legacy_creator_validator_unit_contract(tmp_path):
    import math

    import memebot.store as store

    creator_events_sql = dict(store.V5_TABLE_DDL)["creator_reputation_events"]
    weak_creator_events_sql = """CREATE TABLE creator_reputation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT,
    creator TEXT,
    outcome TEXT,
    observed_at REAL
);"""

    def create_v4(path, *, weak=False):
        conn = _open_v4_fixture(path)
        conn.execute(weak_creator_events_sql if weak else creator_events_sql)
        conn.commit()
        return conn

    def insert_token(conn, mint):
        conn.execute(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES (?,?,?)",
            (mint, 0.0, 0.0),
        )

    def insert_event(conn, *, row_id, **overrides):
        event = {
            "mint": "MINT",
            "creator": "CREATOR",
            "outcome": "GRADUATED",
            "observed_at": 1.0,
        }
        event.update(overrides)
        conn.execute(
            "INSERT INTO creator_reputation_events"
            "(id,mint,creator,outcome,observed_at) VALUES (?,?,?,?,?)",
            (row_id, *(event[column] for column in event)),
        )
        conn.commit()

    def assert_v4_inert(conn):
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='trigger'"
            " AND name LIKE 'creator_reputation_%'"
        ).fetchall() == []
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table'"
            " AND name='creator_reputation_current'"
        ).fetchone() is None

    empty = create_v4(tmp_path / "empty-creator.db")
    before = tuple(empty.iterdump())
    before_changes = empty.total_changes
    store._validate_v5_legacy_creator_reputation_events(empty)
    assert tuple(empty.iterdump()) == before
    assert empty.total_changes == before_changes
    assert_v4_inert(empty)
    empty.close()

    valid = create_v4(tmp_path / "valid-creator.db")
    long_mint = "m" * 129
    blob_mint = b"MINT_BLOB"
    for mint in ("MINT_A", "MINT_B", " M ", "", " ", long_mint, blob_mint):
        insert_token(valid, mint)
    unicode_creator = "💩" * 128
    insert_event(
        valid,
        row_id=7,
        mint="MINT_A",
        creator=unicode_creator,
        observed_at=0,
    )
    insert_event(
        valid,
        row_id=8,
        mint="MINT_A",
        creator=unicode_creator,
        outcome="RUGGED",
        observed_at=4102444800.0,
    )
    insert_event(
        valid,
        row_id=9,
        mint="MINT_B",
        creator="\tC\u00a0",
        observed_at=1,
    )
    insert_event(
        valid,
        row_id=10,
        mint=" M ",
        creator="C",
        outcome="RUGGED",
        observed_at=0.0,
    )
    insert_event(valid, row_id=11, mint="", creator="EMPTY_MINT", observed_at=2.0)
    insert_event(valid, row_id=12, mint=" ", creator="SPACE_MINT", observed_at=3.0)
    insert_event(valid, row_id=13, mint=long_mint, creator="LONG_MINT", observed_at=4.0)
    insert_event(valid, row_id=14, mint=blob_mint, creator="BLOB_MINT", observed_at=5.0)
    assert valid.execute(
        "SELECT typeof(mint) FROM creator_reputation_events WHERE id=14"
    ).fetchone()[0] == "blob"
    assert tuple(
        valid.execute(
            "SELECT length(creator),typeof(observed_at)"
            " FROM creator_reputation_events WHERE id=7"
        ).fetchone()
    ) == (128, "real")
    before = tuple(valid.iterdump())
    before_changes = valid.total_changes
    store._validate_v5_legacy_creator_reputation_events(valid)
    assert tuple(valid.iterdump()) == before
    assert valid.total_changes == before_changes
    assert_v4_inert(valid)
    valid.close()

    invalid_rows = (
        ("mint_missing_token", {"mint": "MISSING"}, None, False),
        ("creator_blob", {"creator": b"CREATOR"}, "creator", True),
        ("creator_empty", {"creator": ""}, None, True),
        ("creator_unicode_long", {"creator": "💩" * 129}, None, True),
        ("creator_leading_ascii_space", {"creator": " CREATOR"}, None, True),
        ("creator_trailing_ascii_space", {"creator": "CREATOR "}, None, True),
        ("creator_nul", {"creator": "CRE\x00ATOR"}, None, True),
        ("outcome_blob", {"outcome": b"GRADUATED"}, "outcome", True),
        ("outcome_wrong_case", {"outcome": "graduated"}, None, True),
        ("outcome_outside_domain", {"outcome": "UNKNOWN"}, None, True),
        ("observed_at_blob", {"observed_at": b"1"}, "observed_at", True),
        (
            "observed_at_below",
            {"observed_at": math.nextafter(0.0, -math.inf)},
            None,
            True,
        ),
        (
            "observed_at_above",
            {"observed_at": math.nextafter(4102444800.0, math.inf)},
            None,
            True,
        ),
        ("observed_at_infinite", {"observed_at": math.inf}, None, True),
    )
    for label, overrides, blob_column, referenced in invalid_rows:
        path = tmp_path / f"invalid-creator-{label}.db"
        conn = create_v4(path, weak=True)
        if referenced:
            insert_token(conn, overrides.get("mint", "MINT"))
        insert_event(conn, row_id=7, **overrides)
        if blob_column is not None:
            assert conn.execute(
                f"SELECT typeof({blob_column})"
                " FROM creator_reputation_events WHERE id=7"
            ).fetchone()[0] == "blob"
        conn.close()

        # Ordinary v4 reopen neither validates this healing state nor activates v5.
        conn = _open_v4_fixture(path)
        assert_v4_inert(conn)
        malformed = conn.execute(
            "SELECT mint,outcome FROM creator_reputation_events WHERE id=7"
        ).fetchone()
        before = tuple(conn.iterdump())
        before_changes = conn.total_changes
        with pytest.raises(ValueError) as exc_info:
            store._validate_v5_legacy_creator_reputation_events(conn)
        assert str(exc_info.value) == (
            "invalid legacy creator_reputation_events row id=7"
            f" mint={malformed[0]!r} outcome={malformed[1]!r}"
        )
        assert tuple(conn.iterdump()) == before
        assert conn.total_changes == before_changes
        assert_v4_inert(conn)
        conn.close()

    invalid_histories = (
        (
            "duplicate_identity",
            ({"row_id": 7}, {"row_id": 8, "observed_at": 2.0}),
        ),
        (
            "creator_mismatch",
            (
                {"row_id": 7},
                {"row_id": 8, "creator": "OTHER", "outcome": "RUGGED", "observed_at": 2.0},
            ),
        ),
        (
            "graduation_after_rug",
            (
                {"row_id": 7, "outcome": "RUGGED"},
                {"row_id": 8, "outcome": "GRADUATED", "observed_at": 2.0},
            ),
        ),
        (
            "equal_rugged_time",
            (
                {"row_id": 7, "observed_at": 2.0},
                {"row_id": 8, "outcome": "RUGGED", "observed_at": 2.0},
            ),
        ),
        (
            "retrograde_rugged_time",
            (
                {"row_id": 7, "observed_at": 2.0},
                {"row_id": 8, "outcome": "RUGGED", "observed_at": 1.0},
            ),
        ),
    )
    for label, events in invalid_histories:
        path = tmp_path / f"invalid-creator-history-{label}.db"
        conn = create_v4(path, weak=True)
        insert_token(conn, "MINT")
        for event in events:
            insert_event(conn, **event)
        before = tuple(conn.iterdump())
        before_changes = conn.total_changes
        with pytest.raises(
            ValueError,
            match=r"^invalid legacy creator_reputation_events row id=8 ",
        ):
            store._validate_v5_legacy_creator_reputation_events(conn)
        assert tuple(conn.iterdump()) == before
        assert conn.total_changes == before_changes
        assert_v4_inert(conn)
        conn.close()

    later = create_v4(tmp_path / "later-invalid-creator.db", weak=True)
    insert_token(later, "MINT")
    insert_event(later, row_id=7)
    insert_event(
        later,
        row_id=8,
        creator="OTHER",
        outcome="RUGGED",
        observed_at=2.0,
    )
    insert_event(later, row_id=9, mint=" ")
    before = tuple(later.iterdump())
    before_changes = later.total_changes
    with pytest.raises(ValueError) as exc_info:
        store._validate_v5_legacy_creator_reputation_events(later)
    assert str(exc_info.value) == (
        "invalid legacy creator_reputation_events row id=8 mint='MINT' outcome='RUGGED'"
    )
    assert tuple(later.iterdump()) == before
    assert later.total_changes == before_changes
    assert_v4_inert(later)
    later.close()


def test_v5_legacy_p3_validator_unit_contract(tmp_path):
    import hashlib
    import json
    import math

    import memebot.store as store

    weak_tables = {
        "canonical_observations": """CREATE TABLE canonical_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER,
    mint TEXT,
    observed_at REAL,
    is_subject INTEGER,
    is_canonical INTEGER,
    eligible INTEGER,
    start_price_sol REAL,
    price_observed_at REAL,
    price_source TEXT,
    unavailable_reason TEXT
);""",
        "canonical_rechecks": """CREATE TABLE canonical_rechecks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER,
    attempt INTEGER,
    rechecked_at REAL,
    causal_target_report_id INTEGER,
    latest_target_report_id INTEGER,
    status TEXT,
    reason TEXT,
    canonical_mint TEXT,
    prior_inputs_hash TEXT,
    recheck_inputs_hash TEXT,
    payload_json TEXT
);""",
        "paper_entry_executions": """CREATE TABLE paper_entry_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER,
    at REAL,
    status TEXT,
    reason TEXT,
    planned_size_sol REAL,
    canonical_recheck_id INTEGER,
    paper_trade_id INTEGER
);""",
    }
    columns = {
        "safety_reports": (
            "id", "mint", "checked_at", "hard_fails_json", "risk_score", "inputs_hash",
        ),
        "decisions": (
            "id", "at", "mint", "segment", "action", "score", "feature_vector_json",
            "safety_report_id", "config_hash",
        ),
        "canonical_observations": (
            "id", "decision_id", "mint", "observed_at", "is_subject", "is_canonical",
            "eligible", "start_price_sol", "price_observed_at", "price_source",
            "unavailable_reason",
        ),
        "canonical_rechecks": (
            "id", "decision_id", "attempt", "rechecked_at", "causal_target_report_id",
            "latest_target_report_id", "status", "reason", "canonical_mint",
            "prior_inputs_hash", "recheck_inputs_hash", "payload_json",
        ),
        "paper_trades": (
            "id", "decision_id", "at", "mint", "segment", "side", "qty",
            "quote_price", "fill_price", "fees_json", "realism_grade",
            "canonical_recheck_id", "canonical_proof_hash", "p3_entry_execution_id",
        ),
        "paper_entry_executions": (
            "id", "decision_id", "at", "status", "reason", "planned_size_sol",
            "canonical_recheck_id", "paper_trade_id",
        ),
        "outcomes": (
            "id", "at", "ref_kind", "ref_id", "pnl_sol", "detail_json",
            "p3_exit_trade_id",
        ),
    }

    def insert_row(conn, table, row):
        names = columns[table]
        conn.execute(
            f"INSERT INTO {table}({','.join(names)}) VALUES ({','.join('?' * len(names))})",
            tuple(row[name] for name in names),
        )

    def canonical_json(value):
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )

    def exact_recheck_payload(recheck, *, safety_trigger=False):
        rechecked_at = recheck["rechecked_at"]
        fill_event_at = None
        if not safety_trigger:
            fill_event_at = (
                math.nextafter(float(rechecked_at), -math.inf)
                if type(rechecked_at) in (int, float)
                and math.isfinite(rechecked_at)
                and rechecked_at > 0.0
                else 0.0
            )
        target_snapshot = None if safety_trigger else {
            "t_wall": fill_event_at,
            "t_mono": 9.0,
            "virtual_sol_reserves": 70_000_000_000,
            "virtual_token_reserves": 70_000_000_000_000,
            "real_sol_reserves": 42_500_000_000,
            "real_token_reserves": 400_000_000_000_000,
            "liquidity_sol": 42.5,
            "spot_price_sol": 0.000001,
            "progress_pct": 50.0,
        }
        verdict_status = "CANONICAL" if recheck["status"] == "PASS" else "SUPPRESSED"
        return canonical_json({
            "decision_id": recheck["decision_id"],
            "attempt": recheck["attempt"],
            "trigger": "safety_hard_fail" if safety_trigger else "curve_progress",
            "trigger_report_id": 10 if safety_trigger else None,
            "rechecked_at": recheck["rechecked_at"],
            "fill_event_at": fill_event_at,
            "causal_target_report_id": recheck["causal_target_report_id"],
            "latest_target_report_id": recheck["latest_target_report_id"],
            "prior_inputs_hash": recheck["prior_inputs_hash"],
            "target_snapshot": target_snapshot,
            "verdict": {
                "status": verdict_status,
                "reason": recheck["reason"],
                "canonical_mint": recheck["canonical_mint"],
                "inputs_hash": "d" * 64,
            },
        })

    def seal_recheck(row, *, safety_trigger=False):
        sealed = dict(row)
        sealed["payload_json"] = exact_recheck_payload(
            sealed, safety_trigger=safety_trigger,
        )
        sealed["recheck_inputs_hash"] = hashlib.sha256(
            sealed["payload_json"].encode()
        ).hexdigest()
        return sealed

    def healed_v4(
        path, *, terminal="FILLED", with_sell=False, with_outcome=True,
        overrides=None, extras=(),
    ):
        overrides = overrides or {}
        conn = _open_v4_fixture(path)
        conn.commit()
        conn.execute("PRAGMA foreign_keys=OFF")
        for name, ddl in store.V5_TABLE_DDL:
            conn.execute(weak_tables.get(name, ddl))
        store._apply_v5_additive_columns(conn)

        hashes = {"prior": "a" * 64, "safety": "c" * 64}
        feature = {
            "canonical": {
                "status": "CANONICAL",
                "inputs_hash": hashes["prior"],
                "planned_size_sol": 10.0,
            }
        }
        rows = {
            "safety_reports": {
                "id": 10, "mint": "MINT", "checked_at": 1.0,
                "hard_fails_json": "[]", "risk_score": 0.0,
                "inputs_hash": hashes["safety"],
            },
            "decisions": {
                "id": 20, "at": 2.0, "mint": "MINT", "segment": "CLIMBING",
                "action": "BUY", "score": 90.0,
                "feature_vector_json": json.dumps(feature, separators=(",", ":")),
                "safety_report_id": 10, "config_hash": "cfg",
            },
            "canonical_observations": {
                "id": 30, "decision_id": 20, "mint": "MINT", "observed_at": 2.0,
                "is_subject": 1, "is_canonical": 1, "eligible": 1,
                "start_price_sol": 1.0, "price_observed_at": 2.0,
                "price_source": "curve_snapshot", "unavailable_reason": "",
            },
            "canonical_rechecks": {
                "id": 40, "decision_id": 20, "attempt": 1, "rechecked_at": 3.0,
                "causal_target_report_id": 10, "latest_target_report_id": 10,
                "status": "PASS", "reason": "canonical", "canonical_mint": "MINT",
                "prior_inputs_hash": hashes["prior"],
                "recheck_inputs_hash": None, "payload_json": None,
            },
            "paper_trades": {
                "id": 50, "decision_id": 20, "at": 4.0, "mint": "MINT",
                "segment": "CLIMBING", "side": "buy", "qty": 2.0,
                "quote_price": 5.0, "fill_price": 5.0, "fees_json": "{}",
                "realism_grade": "B", "canonical_recheck_id": 40,
                "canonical_proof_hash": None,
                "p3_entry_execution_id": None,
            },
            "paper_entry_executions": {
                "id": 60, "decision_id": 20, "at": 4.0, "status": terminal,
                "reason": "filled", "planned_size_sol": 10.0,
                "canonical_recheck_id": 40, "paper_trade_id": 50,
            },
            "outcomes": {
                "id": 80, "at": 5.0, "ref_kind": "trade", "ref_id": 70,
                "pnl_sol": 2.0,
                "detail_json": json.dumps(
                    {"grade": "B", "hold_s": 1.0, "reason": "time_stop"},
                    separators=(",", ":"), sort_keys=True,
                ),
                "p3_exit_trade_id": 70,
            },
        }
        sell = {
            "id": 70, "decision_id": 20, "at": 5.0, "mint": "MINT",
            "segment": "CLIMBING", "side": "sell", "qty": 2.0,
            "quote_price": 6.0, "fill_price": 6.0, "fees_json": "{}",
            "realism_grade": "B", "canonical_recheck_id": None,
            "canonical_proof_hash": None, "p3_entry_execution_id": 60,
        }

        if terminal == "CANCELLED":
            rows["canonical_rechecks"].update(
                status="CANCEL", reason="safety_flip", canonical_mint=None,
            )
            rows["paper_entry_executions"].update(
                at=4.0, reason="safety_flip", canonical_recheck_id=40,
                paper_trade_id=None,
            )
        elif terminal == "ABANDONED_BEFORE":
            rows.pop("canonical_rechecks")
            rows["paper_entry_executions"].update(
                status="ABANDONED", at=3.0, reason="restart_before_fill",
                canonical_recheck_id=None, paper_trade_id=None,
            )
        elif terminal == "ABANDONED_AFTER":
            rows["paper_entry_executions"].update(
                status="ABANDONED", reason="restart_after_pass", paper_trade_id=None,
            )
        elif terminal != "FILLED":
            raise AssertionError(terminal)

        for table, changes in overrides.items():
            target = sell if table == "sell" else rows[table]
            target.update(changes)
        observation_changes = overrides.get("canonical_observations", {})
        if "observed_at" not in observation_changes:
            rows["canonical_observations"]["observed_at"] = rows["decisions"]["at"]
        if (
            "price_observed_at" not in observation_changes
            and rows["canonical_observations"]["unavailable_reason"] == ""
        ):
            rows["canonical_observations"]["price_observed_at"] = rows["decisions"]["at"]

        recheck = rows.get("canonical_rechecks")
        if recheck is not None and recheck["payload_json"] is None:
            recheck["payload_json"] = exact_recheck_payload(recheck)
        if recheck is not None:
            recheck_overrides = overrides.get("canonical_rechecks", {})
            if "recheck_inputs_hash" not in recheck_overrides:
                recheck["recheck_inputs_hash"] = hashlib.sha256(
                    recheck["payload_json"].encode()
                ).hexdigest()
            trade_overrides = overrides.get("paper_trades", {})
            if "canonical_proof_hash" not in trade_overrides:
                rows["paper_trades"]["canonical_proof_hash"] = recheck[
                    "recheck_inputs_hash"
                ]

        conn.execute(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES (?,?,?)",
            (rows["decisions"]["mint"], 0.0, 0.0),
        )
        insert_row(conn, "safety_reports", rows["safety_reports"])
        insert_row(conn, "decisions", rows["decisions"])
        insert_row(conn, "canonical_observations", rows["canonical_observations"])
        if recheck is not None:
            insert_row(conn, "canonical_rechecks", recheck)
        if terminal == "FILLED":
            insert_row(conn, "paper_trades", rows["paper_trades"])
        insert_row(conn, "paper_entry_executions", rows["paper_entry_executions"])
        if with_sell:
            insert_row(conn, "paper_trades", sell)
            if with_outcome:
                insert_row(conn, "outcomes", rows["outcomes"])
        for table, row in extras:
            insert_row(conn, table, row)
        conn.commit()
        return conn

    def assert_inert(conn):
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='trigger' AND name LIKE 'p3_%'"
        ).fetchall() == []
        assert conn.execute("SELECT count(*) FROM p3_position_current").fetchone()[0] == 0

    empty = _open_v4_fixture(tmp_path / "empty-p3.db")
    empty.commit()
    empty.execute("PRAGMA foreign_keys=OFF")
    for name, ddl in store.V5_TABLE_DDL:
        empty.execute(weak_tables.get(name, ddl))
    store._apply_v5_additive_columns(empty)
    empty.commit()
    before = tuple(empty.iterdump())
    before_changes = empty.total_changes
    store._validate_v5_legacy_p3_trade_execution_graph(empty)
    assert tuple(empty.iterdump()) == before
    assert empty.total_changes == before_changes
    assert_inert(empty)
    empty.close()

    valid_modes = (
        ("filled-open", "FILLED", False),
        ("filled-closed", "FILLED", True),
        ("cancelled", "CANCELLED", False),
        ("abandoned-before", "ABANDONED_BEFORE", False),
        ("abandoned-after", "ABANDONED_AFTER", False),
    )
    for label, terminal, with_sell in valid_modes:
        conn = healed_v4(tmp_path / f"valid-{label}.db", terminal=terminal, with_sell=with_sell)
        before = tuple(conn.iterdump())
        before_changes = conn.total_changes
        store._validate_v5_legacy_p3_trade_execution_graph(conn)
        assert tuple(conn.iterdump()) == before
        assert conn.total_changes == before_changes
        assert_inert(conn)
        conn.close()

    observation_invalid = (
        ("missing-decision", {"decision_id": 999}),
        ("missing-token", {"mint": "OTHER"}),
        ("blank-mint", {"mint": "\x00"}),
        ("retrograde-observed-at", {"observed_at": -1.0}),
        ("future-observed-at", {"observed_at": 2.5}),
        ("subject-storage", {"is_subject": b"1"}),
        ("canonical-domain", {"is_canonical": 2}),
        ("eligible-storage", {"eligible": b"1"}),
        ("available-start-null", {"start_price_sol": None}),
        ("available-start-zero", {"start_price_sol": 0.0}),
        ("available-price-time", {"price_observed_at": 2.5}),
        ("available-price-source", {"price_source": "other"}),
        ("unavailable-reason", {"unavailable_reason": "other"}),
        ("unavailable-start", {"unavailable_reason": "start_price_missing"}),
        (
            "unavailable-price-time",
            {
                "unavailable_reason": "start_price_missing",
                "start_price_sol": None,
                "price_source": "",
            },
        ),
        (
            "unavailable-price-source",
            {
                "unavailable_reason": "start_price_missing",
                "start_price_sol": None,
                "price_observed_at": None,
            },
        ),
    )
    for label, observation_override in observation_invalid:
        conn = healed_v4(
            tmp_path / f"invalid-p3-observation-{label}.db",
            overrides={"canonical_observations": observation_override},
        )
        with pytest.raises(
            ValueError, match=r"^invalid legacy P3 graph canonical_observations id=30 ",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    baseline_recheck = {
        "decision_id": 20, "attempt": 1, "rechecked_at": 3.0,
        "causal_target_report_id": 10, "latest_target_report_id": 10,
        "status": "PASS", "reason": "canonical", "canonical_mint": "MINT",
        "prior_inputs_hash": "a" * 64,
    }
    baseline_payload = exact_recheck_payload(baseline_recheck)
    missing_payload = json.loads(baseline_payload)
    missing_payload.pop("trigger")
    extra_payload = json.loads(baseline_payload)
    extra_payload["extra"] = 1
    huge_numeric_payload = json.loads(baseline_payload)
    huge_numeric_payload["target_snapshot"]["liquidity_sol"] = 10**1000
    noncanonical_payload = json.dumps(json.loads(baseline_payload), indent=2)
    sqlite_nul_hash = "a" * 64 + "\x00retained"
    sqlite_nul_feature = canonical_json({
        "canonical": {
            "status": "CANONICAL", "inputs_hash": sqlite_nul_hash,
            "planned_size_sol": 10.0,
        }
    })
    strict_payload_cases = (
        (
            "blob-proof",
            {
                "canonical_rechecks": {
                    "recheck_inputs_hash": b"b" * 64,
                    "payload_json": baseline_payload.encode(),
                },
                "paper_trades": {"canonical_proof_hash": b"b" * 64},
            },
        ),
        (
            "nul-hash",
            {
                "decisions": {"feature_vector_json": sqlite_nul_feature},
                "canonical_rechecks": {"prior_inputs_hash": sqlite_nul_hash},
            },
        ),
        (
            "nonhex-hash",
            {"canonical_rechecks": {"recheck_inputs_hash": "z" * 64}},
        ),
        (
            "noncanonical-json",
            {"canonical_rechecks": {"payload_json": noncanonical_payload}},
        ),
        (
            "missing-key",
            {"canonical_rechecks": {"payload_json": canonical_json(missing_payload)}},
        ),
        (
            "extra-key",
            {"canonical_rechecks": {"payload_json": canonical_json(extra_payload)}},
        ),
        (
            "digest-mismatch",
            {"canonical_rechecks": {"recheck_inputs_hash": "f" * 64}},
        ),
        (
            "huge-numeric",
            {
                "canonical_rechecks": {
                    "payload_json": canonical_json(huge_numeric_payload),
                }
            },
        ),
    )
    for label, overrides in strict_payload_cases:
        conn = healed_v4(tmp_path / f"invalid-p3-{label}.db", overrides=overrides)
        with pytest.raises(
            ValueError, match=r"^invalid legacy P3 graph canonical_rechecks id=40 ",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    for verdict_status in ("SUPPRESSED", "UNRESOLVED"):
        conn = healed_v4(tmp_path / f"valid-cancel-{verdict_status}.db", terminal="CANCELLED")
        payload = json.loads(conn.execute(
            "SELECT payload_json FROM canonical_rechecks WHERE id=40"
        ).fetchone()[0])
        payload["verdict"]["status"] = verdict_status
        payload_json = canonical_json(payload)
        payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()
        conn.execute(
            "UPDATE canonical_rechecks SET payload_json=?,recheck_inputs_hash=? WHERE id=40",
            (payload_json, payload_hash),
        )
        conn.commit()
        store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    safety_cancel = healed_v4(
        tmp_path / "valid-cancel-safety-hard-fail.db", terminal="CANCELLED",
    )
    safety_cancel_row = dict(safety_cancel.execute(
        "SELECT * FROM canonical_rechecks WHERE id=40"
    ).fetchone())
    safety_payload = exact_recheck_payload(
        safety_cancel_row, safety_trigger=True,
    )
    safety_hash = hashlib.sha256(safety_payload.encode()).hexdigest()
    safety_cancel.execute(
        "UPDATE safety_reports SET hard_fails_json=? WHERE id=10", ('["rug"]',),
    )
    safety_cancel.execute(
        "UPDATE canonical_rechecks SET payload_json=?,recheck_inputs_hash=? WHERE id=40",
        (safety_payload, safety_hash),
    )
    safety_cancel.commit()
    store._validate_v5_legacy_p3_trade_execution_graph(safety_cancel)
    safety_cancel.close()

    safety_link_cases = (
        ("missing", 999, None),
        (
            "cross-mint",
            9,
            {
                "id": 9, "mint": "OTHER", "checked_at": 1.5,
                "hard_fails_json": '["rug"]', "risk_score": 100.0,
                "inputs_hash": "e" * 64,
            },
        ),
        (
            "not-prior",
            9,
            {
                "id": 9, "mint": "MINT", "checked_at": 3.0,
                "hard_fails_json": '["rug"]', "risk_score": 100.0,
                "inputs_hash": "e" * 64,
            },
        ),
        ("not-hard-fail", 10, None),
    )
    for label, trigger_report_id, trigger_report in safety_link_cases:
        conn = healed_v4(
            tmp_path / f"invalid-safety-trigger-{label}.db", terminal="CANCELLED",
        )
        if trigger_report is not None:
            insert_row(conn, "safety_reports", trigger_report)
        recheck = dict(conn.execute(
            "SELECT * FROM canonical_rechecks WHERE id=40"
        ).fetchone())
        payload = json.loads(exact_recheck_payload(recheck, safety_trigger=True))
        payload["trigger_report_id"] = trigger_report_id
        payload_json = canonical_json(payload)
        payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()
        conn.execute(
            "UPDATE canonical_rechecks SET payload_json=?,recheck_inputs_hash=? WHERE id=40",
            (payload_json, payload_hash),
        )
        conn.commit()
        with pytest.raises(
            ValueError, match=r"^invalid legacy P3 graph canonical_rechecks id=40 ",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    newer_cancel_report = {
        "id": 11, "mint": "MINT", "checked_at": 2.5, "hard_fails_json": "[]",
        "risk_score": 0.0, "inputs_hash": "d" * 64,
    }
    cancel_after_newer_report = healed_v4(
        tmp_path / "valid-cancel-newer-absolute-latest-report.db",
        terminal="CANCELLED",
        overrides={"canonical_rechecks": {"latest_target_report_id": 11}},
        extras=(("safety_reports", newer_cancel_report),),
    )
    store._validate_v5_legacy_p3_trade_execution_graph(cancel_after_newer_report)
    cancel_after_newer_report.close()

    post_entry_report = {
        "id": 11, "mint": "MINT", "checked_at": 4.5, "hard_fails_json": "[]",
        "risk_score": 0.0, "inputs_hash": "d" * 64,
    }
    valid_later_evidence = healed_v4(
        tmp_path / "valid-report-after-entry-recheck.db",
        extras=(("safety_reports", post_entry_report),),
    )
    store._validate_v5_legacy_p3_trade_execution_graph(valid_later_evidence)
    valid_later_evidence.close()

    final_reasons = (
        "time_stop", "trailing_stop", "graduated", "dead", "graduated_no_price",
        "safety_flip", "stale", "restart_safety_hard_fail",
    )
    for reason in final_reasons:
        detail = json.dumps(
            {"grade": "B", "hold_s": 1.0, "reason": reason},
            separators=(",", ":"), sort_keys=True,
        )
        conn = healed_v4(
            tmp_path / f"valid-final-{reason}.db", with_sell=True,
            overrides={"outcomes": {"detail_json": detail}},
        )
        store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    fee_graph = healed_v4(
        tmp_path / "valid-final-fee-accounting.db", with_sell=True,
        overrides={
            "paper_trades": {"fees_json": '{"buy":1}'},
            "sell": {"fees_json": '{"sell":0.5}'},
            "outcomes": {"pnl_sol": 0.5},
        },
    )
    store._validate_v5_legacy_p3_trade_execution_graph(fee_graph)
    fee_graph.close()

    grouped_pnl_graph = healed_v4(
        tmp_path / "valid-final-grouped-pnl-cancellation.db", with_sell=True,
        overrides={
            "decisions": {"feature_vector_json": json.dumps({
                "canonical": {
                    "status": "CANONICAL", "inputs_hash": "a" * 64,
                    "planned_size_sol": 1e16,
                }
            }, separators=(",", ":"))},
            "paper_trades": {
                "qty": 1.0, "quote_price": 1e16, "fill_price": 1e16,
                "fees_json": '{"buy":1}',
            },
            "paper_entry_executions": {"planned_size_sol": 1e16},
            "sell": {
                "qty": 1.0, "quote_price": 1e16, "fill_price": 1e16,
                "fees_json": '{"sell":1}',
            },
            "outcomes": {"pnl_sol": 0.0},
        },
    )
    store._validate_v5_legacy_p3_trade_execution_graph(grouped_pnl_graph)
    grouped_pnl_graph.close()

    zero_close = healed_v4(
        tmp_path / "valid-zero-price-safety-close.db", with_sell=True,
        overrides={
            "sell": {"quote_price": 0.0, "fill_price": 0.0},
            "outcomes": {"pnl_sol": -10.0},
        },
    )
    store._validate_v5_legacy_p3_trade_execution_graph(zero_close)
    zero_close.close()

    second_sell = {
        "id": 71, "decision_id": 20, "at": 6.0, "mint": "MINT",
        "segment": "CLIMBING", "side": "sell", "qty": 1.5,
        "quote_price": 6.0, "fill_price": 6.0, "fees_json": "{}",
        "realism_grade": "B", "canonical_recheck_id": None,
        "canonical_proof_hash": None, "p3_entry_execution_id": 60,
    }
    multi_sell_close = healed_v4(
        tmp_path / "valid-multi-sell-full-close.db",
        with_sell=True,
        overrides={
            "sell": {"qty": 0.5},
            "outcomes": {
                "at": 6.0,
                "ref_id": 71,
                "p3_exit_trade_id": 71,
                "detail_json": '{"grade":"B","hold_s":2.0,"reason":"time_stop"}',
            },
        },
        extras=(("paper_trades", second_sell),),
    )
    store._validate_v5_legacy_p3_trade_execution_graph(multi_sell_close)
    multi_sell_close.close()

    multi_sell_invalid = (
        ("equal-time", {"at": 5.0}),
        ("cumulative-oversell", {"qty": 1.6}),
        ("remaining-partial", {"qty": 1.4}),
        ("wrong-entry", {"p3_entry_execution_id": 999}),
    )
    for label, changes in multi_sell_invalid:
        malformed_second_sell = dict(second_sell)
        malformed_second_sell.update(changes)
        conn = healed_v4(
            tmp_path / f"invalid-multi-sell-{label}.db",
            with_sell=True,
            overrides={
                "sell": {"qty": 0.5},
                "outcomes": {
                    "at": 6.0,
                    "ref_id": 71,
                    "p3_exit_trade_id": 71,
                    "detail_json": '{"grade":"B","hold_s":2.0,"reason":"time_stop"}',
                },
            },
            extras=(("paper_trades", malformed_second_sell),),
        )
        with pytest.raises(ValueError, match=r" P3 graph paper_trades id=71 "):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    # Exact accepted shape/price/notional boundaries, including zero-price P3 safety close.
    accepted_notional = math.nextafter(1.0 + 1e-12, -math.inf)
    boundary_cases = (
        (
            "lower",
            {
                "decisions": {"at": math.nextafter(0.0, math.inf), "mint": "M", "feature_vector_json": json.dumps({
                    "canonical": {"status": "CANONICAL", "inputs_hash": "a" * 64,
                                  "planned_size_sol": accepted_notional}
                }, separators=(",", ":"))},
                "safety_reports": {"mint": "M", "checked_at": 0.0},
                "canonical_observations": {"mint": "M"},
                "canonical_rechecks": {
                    "rechecked_at": math.nextafter(
                        math.nextafter(0.0, math.inf), math.inf
                    ),
                    "canonical_mint": "M",
                },
                "paper_trades": {"at": 1.0, "mint": "M", "segment": "S", "qty": 1.0,
                                 "quote_price": 1.0, "fill_price": 1.0,
                                 "realism_grade": "G"},
                "paper_entry_executions": {"at": 1.0, "planned_size_sol": accepted_notional},
            },
        ),
        (
            "upper",
            {
                "decisions": {"at": 4102444799.0, "mint": "M" * 128},
                "safety_reports": {"mint": "M" * 128, "checked_at": 4102444798.0},
                "canonical_observations": {"mint": "M" * 128},
                "canonical_rechecks": {"rechecked_at": 4102444799.5,
                                         "canonical_mint": "M" * 128},
                "paper_trades": {"at": 4102444800.0, "mint": "M" * 128,
                                 "segment": "S" * 64, "qty": 1e100,
                                 "quote_price": 1.0, "fill_price": 1.0,
                                 "realism_grade": "G" * 32},
                "paper_entry_executions": {"at": 4102444800.0,
                                             "planned_size_sol": 1e100},
            },
        ),
    )
    for label, overrides in boundary_cases:
        if label == "upper":
            feature = {
                "canonical": {"status": "CANONICAL", "inputs_hash": "a" * 64,
                              "planned_size_sol": 1e100}
            }
            overrides["decisions"]["feature_vector_json"] = json.dumps(
                feature, separators=(",", ":")
            )
        conn = healed_v4(tmp_path / f"valid-boundary-{label}.db", overrides=overrides)
        store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    # A legacy non-P3 trade/outcome remains outside this validator even when its bytes are
    # not valid strict-P3 bytes. Canonical-observation outcomes belong to row 0t's pending rebuild.
    legacy = healed_v4(tmp_path / "valid-legacy-non-p3.db")
    insert_row(legacy, "paper_trades", {
        "id": 5, "decision_id": None, "at": 1.0, "mint": "LEGACY", "segment": "legacy",
        "side": "BUY", "qty": 1.0, "quote_price": 1.0, "fill_price": 1.0,
        "fees_json": "{ \"legacy\": true }", "realism_grade": "",
        "canonical_recheck_id": None, "canonical_proof_hash": None,
        "p3_entry_execution_id": None,
    })
    insert_row(legacy, "outcomes", {
        "id": 6, "at": b"legacy", "ref_kind": "canonical_observation", "ref_id": 30,
        "pnl_sol": b"legacy", "detail_json": b"legacy", "p3_exit_trade_id": None,
    })
    legacy.commit()
    before = tuple(legacy.iterdump())
    store._validate_v5_legacy_p3_trade_execution_graph(legacy)
    assert tuple(legacy.iterdump()) == before
    assert_inert(legacy)
    legacy.close()

    base_recheck_payload = json.dumps(
        {
            "attempt": 1, "causal_target_report_id": 10, "decision_id": 20,
            "latest_target_report_id": 10, "prior_inputs_hash": "a" * 64,
            "rechecked_at": 3.0,
            "verdict": {"canonical_mint": "MINT", "reason": "canonical",
                        "status": "CANONICAL"},
        },
        separators=(",", ":"), sort_keys=True,
    )
    invalid_cases = (
        ("recheck-attempt-blob", {"canonical_rechecks": {
            "attempt": b"1", "payload_json": base_recheck_payload,
        }}, "canonical_rechecks", "attempt"),
        ("recheck-at-below", {"canonical_rechecks": {"rechecked_at": math.nextafter(0.0, -math.inf)}}, "canonical_rechecks", None),
        ("recheck-at-above", {"canonical_rechecks": {"rechecked_at": math.nextafter(4102444800.0, math.inf)}}, "canonical_rechecks", None),
        ("recheck-status", {"canonical_rechecks": {"status": "pass"}}, "canonical_rechecks", None),
        ("recheck-reason", {"canonical_rechecks": {"reason": " "}}, "canonical_rechecks", None),
        ("recheck-reason-leading-nul", {"canonical_rechecks": {"reason": "\x00reason"}}, "canonical_rechecks", None),
        ("recheck-prior-hash-leading-nul", {"canonical_rechecks": {
            "prior_inputs_hash": "\x00" + "a" * 63,
            "payload_json": base_recheck_payload,
        }}, "canonical_rechecks", None),
        ("recheck-proof-hash-leading-nul", {
            "canonical_rechecks": {"recheck_inputs_hash": "\x00" + "b" * 63},
            "paper_trades": {"canonical_proof_hash": "\x00" + "b" * 63},
        }, "canonical_rechecks", None),
        ("recheck-payload-malformed", {"canonical_rechecks": {"payload_json": "{"}}, "canonical_rechecks", None),
        ("recheck-payload-array", {"canonical_rechecks": {"payload_json": "[]"}}, "canonical_rechecks", None),
        ("recheck-decision-link", {"canonical_rechecks": {"decision_id": 999}}, "canonical_rechecks", None),
        ("recheck-action", {"decisions": {"action": "SKIP"}}, "canonical_rechecks", None),
        ("recheck-decision-status", {"decisions": {"feature_vector_json": "{}"}}, "canonical_rechecks", None),
        ("recheck-causal-target", {"canonical_rechecks": {"causal_target_report_id": 999}}, "canonical_rechecks", None),
        ("recheck-latest-target", {"canonical_rechecks": {"latest_target_report_id": None}}, "canonical_rechecks", None),
        ("recheck-time-equal-decision", {"canonical_rechecks": {"rechecked_at": 2.0}}, "canonical_rechecks", None),
        ("recheck-pass-mint", {"canonical_rechecks": {"canonical_mint": "OTHER"}}, "canonical_rechecks", None),
        ("recheck-prior-proof", {"canonical_rechecks": {"prior_inputs_hash": "d" * 64}}, "canonical_rechecks", None),
        ("recheck-causal-report-after-decision", {
            "safety_reports": {"checked_at": 2.5},
        }, "canonical_rechecks", None),
        ("recheck-latest-report-time", {"safety_reports": {"checked_at": 3.0}}, "canonical_rechecks", None),
        ("trade-side", {"paper_trades": {"side": "BUY"}}, "paper_trades", None),
        ("trade-proof-pair-recheck", {"paper_trades": {"canonical_proof_hash": None}}, "paper_trades", None),
        ("trade-proof-pair-hash", {"paper_trades": {"canonical_recheck_id": None}}, "paper_trades", None),
        ("trade-mint-blob", {"paper_trades": {"mint": b"MINT"}}, "paper_trades", "mint"),
        ("trade-mint-empty", {"paper_trades": {"mint": " "}}, "paper_trades", None),
        ("trade-mint-long", {"paper_trades": {"mint": "m" * 129}}, "paper_trades", None),
        ("trade-segment-blob", {"paper_trades": {"segment": b"S"}}, "paper_trades", "segment"),
        ("trade-segment-empty", {"paper_trades": {"segment": " "}}, "paper_trades", None),
        ("trade-segment-leading-nul", {"paper_trades": {"segment": "\x00segment"}}, "paper_trades", None),
        ("trade-segment-long", {"paper_trades": {"segment": "s" * 65}}, "paper_trades", None),
        ("trade-at-blob", {"paper_trades": {"at": b"4"}}, "paper_trades", "at"),
        ("trade-at-below", {"paper_trades": {"at": math.nextafter(0.0, -math.inf)}}, "paper_trades", None),
        ("trade-at-above", {"paper_trades": {"at": math.nextafter(4102444800.0, math.inf)}}, "paper_trades", None),
        ("trade-qty-blob", {"paper_trades": {"qty": b"2"}}, "paper_trades", "qty"),
        ("trade-qty-zero", {"paper_trades": {"qty": 0.0}}, "paper_trades", None),
        ("trade-qty-above", {"paper_trades": {"qty": math.nextafter(1e100, math.inf)}}, "paper_trades", None),
        ("trade-quote-blob", {"paper_trades": {"quote_price": b"5"}}, "paper_trades", "quote_price"),
        ("trade-quote-below", {"paper_trades": {"quote_price": math.nextafter(0.0, -math.inf)}}, "paper_trades", None),
        ("trade-quote-above", {"paper_trades": {"quote_price": math.nextafter(1e100, math.inf)}}, "paper_trades", None),
        ("trade-fill-blob", {"paper_trades": {"fill_price": b"5"}}, "paper_trades", "fill_price"),
        ("trade-fill-below", {"paper_trades": {"fill_price": math.nextafter(0.0, -math.inf)}}, "paper_trades", None),
        ("trade-fill-above", {"paper_trades": {"fill_price": math.nextafter(1e100, math.inf)}}, "paper_trades", None),
        ("trade-buy-zero-quote", {"paper_trades": {"quote_price": 0.0}}, "paper_trades", None),
        ("trade-buy-zero-fill", {"paper_trades": {"fill_price": 0.0}}, "paper_trades", None),
        ("trade-better-than-quote", {"paper_trades": {"quote_price": math.nextafter(5.0, math.inf)}}, "paper_trades", None),
        ("trade-fees-blob", {"paper_trades": {"fees_json": b"{}"}}, "paper_trades", "fees_json"),
        ("trade-fees-malformed", {"paper_trades": {"fees_json": "{"}}, "paper_trades", None),
        ("trade-fees-huge", {"paper_trades": {"fees_json": '{"fee":' + "9" * 1000 + '}'}}, "paper_trades", None),
        ("trade-fees-later-key", {"paper_trades": {"fees_json": '{"a":0,"z":-1}'}}, "paper_trades", None),
        ("trade-grade-blob", {"paper_trades": {"realism_grade": b"B"}}, "paper_trades", "realism_grade"),
        ("trade-grade-empty", {"paper_trades": {"realism_grade": ""}}, "paper_trades", None),
        ("trade-grade-leading-nul", {"paper_trades": {"realism_grade": "\x00B"}}, "paper_trades", None),
        ("trade-grade-long", {"paper_trades": {"realism_grade": "g" * 33}}, "paper_trades", None),
        ("buy-stale-recheck", {"paper_trades": {"canonical_recheck_id": 999}}, "paper_trades", None),
        ("buy-proof-hash", {"paper_trades": {"canonical_proof_hash": "d" * 64}}, "paper_trades", None),
        ("buy-cross-mint", {"paper_trades": {"mint": "OTHER"}}, "paper_trades", None),
        ("buy-time-equal-recheck", {"paper_trades": {"at": 3.0}, "paper_entry_executions": {"at": 3.0}}, "paper_trades", None),
        ("execution-at-blob", {"paper_entry_executions": {"at": b"4"}}, "paper_entry_executions", "at"),
        ("execution-at-below", {"paper_entry_executions": {"at": math.nextafter(0.0, -math.inf)}}, "paper_entry_executions", None),
        ("execution-at-above", {"paper_entry_executions": {"at": math.nextafter(4102444800.0, math.inf)}}, "paper_entry_executions", None),
        ("execution-status", {"paper_entry_executions": {"status": "filled"}}, "paper_entry_executions", None),
        ("execution-reason-blank", {"paper_entry_executions": {"reason": " "}}, "paper_entry_executions", None),
        ("execution-planned-blob", {"paper_entry_executions": {"planned_size_sol": b"10"}}, "paper_entry_executions", "planned_size_sol"),
        ("execution-planned-zero", {"paper_entry_executions": {"planned_size_sol": 0.0}}, "paper_entry_executions", None),
        ("execution-planned-above", {"paper_entry_executions": {"planned_size_sol": math.nextafter(1e100, math.inf)}}, "paper_entry_executions", None),
        ("execution-filled-reason", {"paper_entry_executions": {"reason": "other"}}, "paper_entry_executions", None),
        ("execution-filled-no-recheck", {"paper_entry_executions": {"canonical_recheck_id": None}}, "paper_entry_executions", None),
        ("execution-filled-no-trade", {"paper_entry_executions": {"paper_trade_id": None}}, "paper_entry_executions", None),
        ("execution-decision", {"paper_entry_executions": {"decision_id": 999}}, "paper_entry_executions", None),
        ("execution-time-equal-decision", {"paper_entry_executions": {"at": 2.0}}, "paper_entry_executions", None),
        ("execution-planned-payload", {"paper_entry_executions": {"planned_size_sol": 9.0}}, "paper_entry_executions", None),
        ("execution-notional-immediate-reject", {
            "decisions": {"feature_vector_json": json.dumps({
                "canonical": {"status": "CANONICAL", "inputs_hash": "a" * 64,
                              "planned_size_sol": 1.0 + 1e-12}
            }, separators=(",", ":"))},
            "paper_trades": {"qty": 1.0, "quote_price": 1.0, "fill_price": 1.0},
            "paper_entry_executions": {"planned_size_sol": 1.0 + 1e-12},
        }, "paper_entry_executions", None),
    )

    for label, overrides, malformed_table, blob_column in invalid_cases:
        path = tmp_path / f"invalid-p3-{label}.db"
        conn = healed_v4(path, overrides=overrides)
        if blob_column is not None:
            table = "paper_trades" if malformed_table == "paper_trades" else malformed_table
            assert conn.execute(
                f"SELECT typeof({blob_column}) FROM {table} ORDER BY id LIMIT 1"
            ).fetchone()[0] == "blob"
        conn.close()

        # Ordinary reopen remains v4 and does not invoke this migration-only validator.
        conn = _open_v4_fixture(path)
        assert_inert(conn)
        before = tuple(conn.iterdump())
        before_changes = conn.total_changes
        with pytest.raises(
            ValueError,
            match=rf"^invalid legacy P3 graph {malformed_table} id=",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        assert tuple(conn.iterdump()) == before
        assert conn.total_changes == before_changes
        assert_inert(conn)
        conn.close()

    branch_invalid = (
        ("cancelled-reason", "CANCELLED", {"paper_entry_executions": {"reason": "other"}}),
        ("cancelled-no-recheck", "CANCELLED", {"paper_entry_executions": {"canonical_recheck_id": None}}),
        ("cancelled-trade", "CANCELLED", {"paper_entry_executions": {"paper_trade_id": 50}}),
        ("cancelled-time-equal", "CANCELLED", {"paper_entry_executions": {"at": 3.0}}),
        ("abandoned-before-wrong-reason", "ABANDONED_BEFORE", {"paper_entry_executions": {"reason": "restart_after_pass"}}),
        ("abandoned-before-has-proof", "ABANDONED_BEFORE", {"paper_entry_executions": {"canonical_recheck_id": 40}}),
        ("abandoned-after-wrong-reason", "ABANDONED_AFTER", {"paper_entry_executions": {"reason": "restart_before_fill"}}),
        ("abandoned-after-no-proof", "ABANDONED_AFTER", {"paper_entry_executions": {"canonical_recheck_id": None}}),
        ("abandoned-after-trade", "ABANDONED_AFTER", {"paper_entry_executions": {"paper_trade_id": 50}}),
    )
    for label, terminal, overrides in branch_invalid:
        conn = healed_v4(tmp_path / f"invalid-p3-{label}.db", terminal=terminal, overrides=overrides)
        with pytest.raises(ValueError, match=r" P3 graph paper_entry_executions id=60 "):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    newer_report = {
        "id": 11, "mint": "MINT", "checked_at": 2.5, "hard_fails_json": "[]",
        "risk_score": 0.0, "inputs_hash": "d" * 64,
    }
    stale_absolute_latest = healed_v4(
        tmp_path / "invalid-p3-absolute-latest-report.db",
        extras=(("safety_reports", newer_report),),
    )
    with pytest.raises(ValueError, match=r" P3 graph canonical_rechecks id=40 "):
        store._validate_v5_legacy_p3_trade_execution_graph(stale_absolute_latest)
    stale_absolute_latest.close()

    stale_attempt = seal_recheck({
        "id": 41, "decision_id": 20, "attempt": 2, "rechecked_at": 3.5,
        "causal_target_report_id": 10, "latest_target_report_id": 10,
        "status": "PASS", "reason": "canonical", "canonical_mint": "MINT",
        "prior_inputs_hash": "a" * 64,
    })
    stale_trade = healed_v4(
        tmp_path / "invalid-p3-stale-recheck-attempt.db",
        extras=(("canonical_rechecks", stale_attempt),),
    )
    with pytest.raises(ValueError, match=r" P3 graph paper_trades id=50 "):
        store._validate_v5_legacy_p3_trade_execution_graph(stale_trade)
    stale_trade.close()

    for label, changes in (
        ("gap", {"attempt": 3}),
        ("retrograde-time", {"rechecked_at": 2.5}),
    ):
        malformed_sequence = dict(stale_attempt)
        malformed_sequence.update(changes)
        malformed_sequence = seal_recheck(malformed_sequence)
        conn = healed_v4(
            tmp_path / f"invalid-p3-recheck-sequence-{label}.db",
            extras=(("canonical_rechecks", malformed_sequence),),
        )
        with pytest.raises(
            ValueError, match=r"^invalid legacy P3 graph canonical_rechecks id=41 ",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    terminal_cancel_followups = (
        (
            "pass-filled",
            "FILLED",
            {
                "paper_trades": {
                    "canonical_recheck_id": 41,
                    "canonical_proof_hash": stale_attempt["recheck_inputs_hash"],
                },
                "paper_entry_executions": {"canonical_recheck_id": 41},
            },
            seal_recheck({
                "id": 41, "decision_id": 20, "attempt": 2, "rechecked_at": 3.5,
                "causal_target_report_id": 10, "latest_target_report_id": 10,
                "status": "PASS", "reason": "canonical", "canonical_mint": "MINT",
                "prior_inputs_hash": "a" * 64,
            }),
        ),
        (
            "later-cancel",
            "CANCELLED",
            {
                "paper_entry_executions": {
                    "at": 4.0,
                    "canonical_recheck_id": 41,
                    "paper_trade_id": None,
                    "reason": "safety_flip",
                    "status": "CANCELLED",
                },
            },
            seal_recheck({
                "id": 41, "decision_id": 20, "attempt": 2, "rechecked_at": 3.5,
                "causal_target_report_id": 10, "latest_target_report_id": 10,
                "status": "CANCEL", "reason": "safety_flip", "canonical_mint": None,
                "prior_inputs_hash": "a" * 64,
            }),
        ),
    )
    for label, terminal, overrides, followup in terminal_cancel_followups:
        conn = healed_v4(
            tmp_path / f"invalid-p3-cancel-{label}.db",
            terminal=terminal,
            overrides={
                "canonical_rechecks": {
                    "status": "CANCEL", "reason": "safety_flip", "canonical_mint": None,
                },
                **overrides,
            },
            extras=(("canonical_rechecks", followup),),
        )
        with pytest.raises(
            ValueError, match=r"^invalid legacy P3 graph canonical_rechecks id=41 ",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    inverted_attempt_cancel = seal_recheck({
        "id": 41, "decision_id": 20, "attempt": 1, "rechecked_at": 3.5,
        "causal_target_report_id": 10, "latest_target_report_id": 10,
        "status": "CANCEL", "reason": "safety_flip", "canonical_mint": None,
        "prior_inputs_hash": "a" * 64,
    })
    inverted_attempts = healed_v4(
        tmp_path / "invalid-p3-inverted-recheck-attempts.db",
        overrides={"canonical_rechecks": {"attempt": 2}},
        extras=(("canonical_rechecks", inverted_attempt_cancel),),
    )
    with pytest.raises(
        ValueError, match=r"^invalid legacy P3 graph canonical_rechecks id=40 ",
    ):
        store._validate_v5_legacy_p3_trade_execution_graph(inverted_attempts)
    inverted_attempts.close()

    # Strict SELLs are graph-tagged even with null proof columns. Pre-v5 partial SELLs
    # are unreconstructable contamination because no trustworthy ladder identity exists.
    sell_invalid = (
        ("sell-no-entry", {"sell": {"p3_entry_execution_id": None}}, "paper_trades"),
        ("sell-proof", {"sell": {"canonical_recheck_id": 40,
                                  "canonical_proof_hash": "b" * 64}}, "paper_trades"),
        ("sell-cross-mint", {"sell": {"mint": "OTHER"}}, "paper_trades"),
        ("sell-better-than-quote", {"sell": {"quote_price": 5.0}}, "paper_trades"),
        ("sell-id-before-buy", {
            "sell": {"id": 49},
            "outcomes": {"ref_id": 49, "p3_exit_trade_id": 49},
        }, "paper_trades"),
        ("sell-time-equal-entry", {"sell": {"at": 4.0}, "outcomes": {"at": 4.0,
                                    "detail_json": '{"grade":"B","hold_s":0.0,"reason":"time_stop"}'}}, "paper_trades"),
        ("sell-over", {"sell": {"qty": math.nextafter(2.0, math.inf)}}, "paper_trades"),
        ("sell-partial", {"sell": {"qty": math.nextafter(2.0, -math.inf)}}, "paper_trades"),
        ("outcome-at-blob", {"outcomes": {"at": b"5"}}, "outcomes"),
        ("outcome-pnl-blob", {"outcomes": {"pnl_sol": b"2"}}, "outcomes"),
        ("outcome-detail-blob", {"outcomes": {"detail_json": b"{}"}}, "outcomes"),
        ("outcome-detail-oversize", {"outcomes": {"detail_json": "{" + " " * 8192 + "}"}}, "outcomes"),
        ("outcome-detail-malformed", {"outcomes": {"detail_json": "{"}}, "outcomes"),
        ("outcome-ref-kind", {"outcomes": {"ref_kind": "legacy"}}, "outcomes"),
        ("outcome-ref-id", {"outcomes": {"ref_id": 50}}, "outcomes"),
        ("outcome-exit-id", {"outcomes": {"p3_exit_trade_id": 50}}, "outcomes"),
        ("outcome-time", {"outcomes": {"at": math.nextafter(5.0, math.inf)}}, "outcomes"),
        ("outcome-pnl", {"outcomes": {"pnl_sol": math.nextafter(2.0, math.inf)}}, "outcomes"),
        ("outcome-extra-key", {"outcomes": {"detail_json": '{"extra":0,"grade":"B","hold_s":1.0,"reason":"time_stop"}'}}, "outcomes"),
        ("outcome-reason", {"outcomes": {"detail_json": '{"grade":"B","hold_s":1.0,"reason":"ladder_0"}'}}, "outcomes"),
        ("outcome-hold", {"outcomes": {"detail_json": '{"grade":"B","hold_s":2.0,"reason":"time_stop"}'}}, "outcomes"),
        ("outcome-grade", {"outcomes": {"detail_json": '{"grade":"A","hold_s":1.0,"reason":"time_stop"}'}}, "outcomes"),
    )
    for label, overrides, malformed_table in sell_invalid:
        conn = healed_v4(
            tmp_path / f"invalid-p3-{label}.db", with_sell=True, overrides=overrides,
        )
        with pytest.raises(
            ValueError, match=rf"^invalid legacy P3 graph {malformed_table} id=",
        ):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    closed_without_outcome = healed_v4(
        tmp_path / "invalid-p3-closed-without-outcome.db",
        with_sell=True, with_outcome=False,
    )
    with pytest.raises(ValueError, match=r" P3 graph paper_trades id=70 "):
        store._validate_v5_legacy_p3_trade_execution_graph(closed_without_outcome)
    closed_without_outcome.close()

    # Exact natural identities and graph completeness are replayed even when a weak
    # healing-state table bypassed the future UNIQUE declarations.
    duplicate_recheck = seal_recheck({
        "id": 41, "decision_id": 20, "attempt": 1, "rechecked_at": 3.5,
        "causal_target_report_id": 10, "latest_target_report_id": 10,
        "status": "PASS", "reason": "canonical", "canonical_mint": "MINT",
        "prior_inputs_hash": "a" * 64,
    })
    duplicate_execution = {
        "id": 61, "decision_id": 20, "at": 4.0, "status": "FILLED",
        "reason": "filled", "planned_size_sol": 10.0,
        "canonical_recheck_id": 40, "paper_trade_id": 50,
    }
    duplicate_outcome = {
        "id": 81, "at": 5.0, "ref_kind": "trade", "ref_id": 70, "pnl_sol": 2.0,
        "detail_json": '{"grade":"B","hold_s":1.0,"reason":"time_stop"}',
        "p3_exit_trade_id": 70,
    }
    natural_cases = (
        ("duplicate-recheck", False, (("canonical_rechecks", duplicate_recheck),), "canonical_rechecks"),
        ("duplicate-execution", False, (("paper_entry_executions", duplicate_execution),), "paper_entry_executions"),
        ("duplicate-outcome", True, (("outcomes", duplicate_outcome),), "outcomes"),
    )
    for label, with_sell, extras, malformed_table in natural_cases:
        conn = healed_v4(
            tmp_path / f"invalid-p3-{label}.db", with_sell=with_sell, extras=extras,
        )
        with pytest.raises(ValueError, match=rf" P3 graph {malformed_table} id="):
            store._validate_v5_legacy_p3_trade_execution_graph(conn)
        conn.close()

    missing = healed_v4(tmp_path / "invalid-p3-missing-execution.db")
    missing.execute("DELETE FROM paper_entry_executions")
    missing.commit()
    with pytest.raises(ValueError, match=r" P3 graph decisions id=20 "):
        store._validate_v5_legacy_p3_trade_execution_graph(missing)
    missing.close()

    # Earliest malformed stable identity wins even when malformed rows occur later in a list.
    later = healed_v4(
        tmp_path / "invalid-p3-deterministic.db",
        overrides={"paper_trades": {"canonical_proof_hash": "bad"}},
    )
    insert_row(later, "paper_trades", {
        "id": 51, "decision_id": 20, "at": 4.5, "mint": "MINT", "segment": "CLIMBING",
        "side": "buy", "qty": 2.0, "quote_price": 5.0, "fill_price": 5.0,
        "fees_json": "{}", "realism_grade": "B", "canonical_recheck_id": 40,
        "canonical_proof_hash": "wrong", "p3_entry_execution_id": None,
    })
    later.commit()
    before = tuple(later.iterdump())
    before_changes = later.total_changes
    with pytest.raises(ValueError) as exc_info:
        store._validate_v5_legacy_p3_trade_execution_graph(later)
    assert str(exc_info.value) == (
        "invalid legacy P3 graph paper_trades id=50 decision_id=20 mint='MINT'"
    )
    assert tuple(later.iterdump()) == before
    assert later.total_changes == before_changes
    assert_inert(later)
    later.close()


def test_v5_causal_clock_initializer_unit_contract():
    import memebot.store as store

    sources = (
        ("decisions", ("at",)),
        ("paper_trades", ("at",)),
        ("outcomes", ("at",)),
        ("safety_reports", ("checked_at",)),
        ("wallet_pnl_events", ("at",)),
        ("early_buyer_reads", ("checked_at",)),
        (
            "tokens",
            (
                "created_at",
                "last_seen",
                "p3_identity_ingested_at",
                "curve_progress_observed_at",
            ),
        ),
        ("creator_reputation_events", ("observed_at",)),
        ("canonical_observations", ("observed_at",)),
        ("canonical_generations", ("created_at",)),
        ("canonical_rechecks", ("rechecked_at",)),
        ("paper_entry_executions", ("at",)),
    )

    conn = sqlite3.connect(":memory:")
    conn.execute(dict(store.V5_TABLE_DDL)["p3_causal_clock"])
    next_wall = 10.0
    for table, columns in sources:
        definitions = ",".join(f"{column} REAL" for column in columns)
        conn.execute(f"CREATE TABLE {table} ({definitions})")
        values = []
        for column in columns:
            values.append(
                None
                if column in (
                    "p3_identity_ingested_at", "curve_progress_observed_at",
                )
                else next_wall
            )
            next_wall += 1.0
        placeholders = ",".join("?" for _ in columns)
        conn.execute(
            f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
            values,
        )
    conn.commit()
    expected_seed = next_wall - 1.0

    with pytest.raises(RuntimeError, match="active transaction"):
        store.initialize_p3_causal_clock(conn, raw_now=1.0)
    assert conn.execute("SELECT * FROM p3_causal_clock").fetchall() == []

    conn.execute("BEGIN IMMEDIATE")
    assert store.initialize_p3_causal_clock(conn, raw_now=1.0) == expected_seed
    assert conn.execute("SELECT * FROM p3_causal_clock").fetchall() == [
        (1, expected_seed)
    ]
    assert conn.in_transaction
    conn.rollback()
    assert conn.execute("SELECT * FROM p3_causal_clock").fetchall() == []

    for index, (table, columns) in enumerate(sources):
        for column in columns:
            source_max = 100.0 + index + columns.index(column) / 10.0
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f'UPDATE "{table}" SET "{column}"=?', (source_max,))
            assert store.initialize_p3_causal_clock(conn, raw_now=1.0) == source_max
            conn.rollback()
            assert conn.execute("SELECT * FROM p3_causal_clock").fetchall() == []

    conn.execute("INSERT INTO p3_causal_clock VALUES (1, ?)", (expected_seed + 10.0,))
    conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    before_changes = conn.total_changes
    assert store.initialize_p3_causal_clock(conn, raw_now=2.0) == expected_seed + 10.0
    assert conn.execute("SELECT last_wall FROM p3_causal_clock").fetchone() == (
        expected_seed + 10.0,
    )
    assert conn.total_changes == before_changes
    conn.execute(
        "UPDATE paper_entry_executions SET at=?", (expected_seed + 20.0,)
    )
    assert store.initialize_p3_causal_clock(conn, raw_now=2.0) == expected_seed + 20.0
    assert conn.execute("SELECT last_wall FROM p3_causal_clock").fetchone() == (
        expected_seed + 20.0,
    )
    assert conn.in_transaction
    conn.rollback()
    assert conn.execute("SELECT last_wall FROM p3_causal_clock").fetchone() == (
        expected_seed + 10.0,
    )
    conn.close()

    partial = sqlite3.connect(":memory:")
    partial.execute(dict(store.V5_TABLE_DDL)["p3_causal_clock"])
    partial.execute("CREATE TABLE tokens (created_at REAL, last_seen REAL)")
    partial.execute("INSERT INTO tokens VALUES (7.0, 6.5)")
    partial.commit()
    partial.execute("BEGIN IMMEDIATE")
    assert store.initialize_p3_causal_clock(partial, raw_now=8.0) == 8.0
    partial.rollback()
    partial.close()

    for raw_now in (
        True, -1.0, float("nan"), float("inf"), 4102444800.1, 10**1000,
    ):
        invalid_raw = sqlite3.connect(":memory:")
        invalid_raw.execute(dict(store.V5_TABLE_DDL)["p3_causal_clock"])
        invalid_raw.commit()
        invalid_raw.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError, match="raw_now"):
            store.initialize_p3_causal_clock(invalid_raw, raw_now=raw_now)
        assert invalid_raw.in_transaction
        assert invalid_raw.execute("SELECT * FROM p3_causal_clock").fetchall() == []
        invalid_raw.rollback()
        invalid_raw.close()

    for malformed_value in ("bad", None):
        malformed_source = sqlite3.connect(":memory:")
        malformed_source.execute(dict(store.V5_TABLE_DDL)["p3_causal_clock"])
        malformed_source.execute("CREATE TABLE decisions (at)")
        malformed_source.execute("INSERT INTO decisions VALUES (?)", (malformed_value,))
        malformed_source.commit()
        malformed_source.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError, match=r"decisions\.at"):
            store.initialize_p3_causal_clock(malformed_source, raw_now=1.0)
        assert malformed_source.in_transaction
        assert malformed_source.execute(
            "SELECT * FROM p3_causal_clock"
        ).fetchall() == []
        malformed_source.rollback()
        malformed_source.close()

    for rows in (((1, "bad"),), ((2, 3.0),), ((1, 2.0), (2, 3.0))):
        malformed_clock = sqlite3.connect(":memory:")
        malformed_clock.execute("CREATE TABLE p3_causal_clock (singleton,last_wall)")
        malformed_clock.executemany("INSERT INTO p3_causal_clock VALUES (?,?)", rows)
        malformed_clock.commit()
        malformed_clock.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError, match="p3_causal_clock"):
            store.initialize_p3_causal_clock(malformed_clock, raw_now=1.0)
        assert malformed_clock.in_transaction
        malformed_clock.rollback()
        malformed_clock.close()


def test_p3_causal_clock_fences_allocates_and_survives_reopen(tmp_path):
    import math

    import memebot.store as store

    path = tmp_path / "causal-clock.db"
    v4 = _open_v4_fixture(path)
    v4.execute(
        "INSERT INTO tokens(mint,created_at,last_seen) VALUES ('MINT',10.0,250.0)"
    )
    v4.commit()
    v4.close()

    conn = store.open_db(path, migration_clock=lambda: 5.0)
    assert tuple(conn.execute(
        "SELECT singleton,last_wall FROM p3_causal_clock"
    ).fetchone()) == (1, 250.0)

    with pytest.raises(RuntimeError, match="active transaction"):
        store.fence_p3_causal_wall(conn, observed_wall=250.0)
    with pytest.raises(RuntimeError, match="active transaction"):
        store.allocate_p3_causal_wall(conn, raw_wall=250.0)

    conn.execute("BEGIN IMMEDIATE")
    for helper, keyword in (
        (store.fence_p3_causal_wall, "observed_wall"),
        (store.allocate_p3_causal_wall, "raw_wall"),
    ):
        for invalid in (
            True,
            -1.0,
            float("nan"),
            float("inf"),
            4102444800.1,
            10**1000,
        ):
            with pytest.raises(ValueError, match="invalid p3 causal wall"):
                helper(conn, **{keyword: invalid})
            assert conn.execute(
                "SELECT last_wall FROM p3_causal_clock"
            ).fetchone()[0] == 250.0
            assert conn.in_transaction

    assert store.fence_p3_causal_wall(conn, observed_wall=200.0) == 250.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock"
    ).fetchone()[0] == 250.0
    assert store.fence_p3_causal_wall(conn, observed_wall=300.0) == 300.0
    assert conn.in_transaction
    assert store.allocate_p3_causal_wall(conn, raw_wall=350.0) == 350.0
    regressed = store.allocate_p3_causal_wall(conn, raw_wall=349.0)
    assert regressed == math.nextafter(350.0, math.inf)
    assert conn.in_transaction
    conn.rollback()
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock"
    ).fetchone()[0] == 250.0

    conn.execute("BEGIN IMMEDIATE")
    assert store.fence_p3_causal_wall(conn, observed_wall=400.0) == 400.0
    persisted = store.allocate_p3_causal_wall(conn, raw_wall=399.0)
    assert persisted == math.nextafter(400.0, math.inf)
    assert conn.in_transaction
    conn.commit()
    conn.close()

    reopened = store.open_db(path, migration_clock=lambda: 1.0)
    assert reopened.execute(
        "SELECT last_wall FROM p3_causal_clock"
    ).fetchone()[0] == persisted
    reopened.execute("BEGIN IMMEDIATE")
    continued = store.allocate_p3_causal_wall(reopened, raw_wall=1.0)
    assert continued == math.nextafter(persisted, math.inf)
    assert continued > persisted
    assert reopened.in_transaction
    reopened.rollback()

    reopened.execute("BEGIN IMMEDIATE")
    reopened.execute(
        "UPDATE p3_causal_clock SET last_wall=4102444800.0 WHERE singleton=1"
    )
    with pytest.raises(ValueError, match="invalid p3 causal allocation"):
        store.allocate_p3_causal_wall(reopened, raw_wall=4102444800.0)
    assert reopened.execute(
        "SELECT last_wall FROM p3_causal_clock"
    ).fetchone()[0] == 4102444800.0
    assert reopened.in_transaction
    reopened.rollback()

    reopened.execute("BEGIN IMMEDIATE")
    reopened.execute("DELETE FROM p3_causal_clock")
    with pytest.raises(ValueError, match="p3_causal_clock"):
        store.fence_p3_causal_wall(reopened, observed_wall=1.0)
    assert reopened.in_transaction
    reopened.rollback()
    reopened.close()

    malformed = sqlite3.connect(":memory:")
    malformed.execute("CREATE TABLE p3_causal_clock (singleton,last_wall)")
    malformed.execute("INSERT INTO p3_causal_clock VALUES (2,3.0)")
    malformed.commit()
    malformed.execute("BEGIN IMMEDIATE")
    with pytest.raises(ValueError, match="p3_causal_clock"):
        store.allocate_p3_causal_wall(malformed, raw_wall=4.0)
    assert malformed.in_transaction
    malformed.rollback()
    malformed.close()


def test_v5_wallet_summary_rebuilder_unit_contract():
    import memebot.store as store

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE wallet_pnl_events ("
        "id INTEGER PRIMARY KEY, at REAL, wallet TEXT, realized_pnl_sol REAL)"
    )
    conn.execute(dict(store.V5_TABLE_DDL)["wallet_pnl_summary"])
    events = (
        (4, 30.0, "A", 0.1),
        (5, 40.0, "B", -2.0),
        (9, 20.0, "A", 0.2),
        (12, 10.0, "B", 3.5),
        (20, 50.0, "A", -0.3),
    )
    conn.executemany("INSERT INTO wallet_pnl_events VALUES (?,?,?,?)", events)
    conn.executemany(
        "INSERT INTO wallet_pnl_summary VALUES (?,?,?,?,?)",
        (("A", 99, 123.0, 0.0, 4), ("stale", 1, 1.0, 1.0, 5)),
    )
    conn.commit()
    before = tuple(conn.iterdump())

    with pytest.raises(RuntimeError, match="active transaction"):
        store._rebuild_v5_wallet_pnl_summary(conn)
    assert tuple(conn.iterdump()) == before

    conn.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_wallet_pnl_summary(conn)
    expected = [
        ("A", 3, 5.551115123125783e-17, 50.0, 20),
        ("B", 2, 1.5, 40.0, 12),
    ]
    assert conn.execute(
        "SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id "
        "FROM wallet_pnl_summary ORDER BY wallet"
    ).fetchall() == expected
    assert store._verify_v5_wallet_pnl_summary(conn) is None
    assert conn.in_transaction
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    assert conn.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='trigger' "
        "AND name='p3_wallet_pnl_summary_insert'"
    ).fetchone() is None

    corruptions = (
        "UPDATE wallet_pnl_summary SET event_count=4 WHERE wallet='A'",
        "UPDATE wallet_pnl_summary SET realized_pnl_sol=1.0 WHERE wallet='A'",
        "UPDATE wallet_pnl_summary SET last_at=49.0 WHERE wallet='A'",
        "UPDATE wallet_pnl_summary SET last_event_id=9 WHERE wallet='A'",
        "DELETE FROM wallet_pnl_summary WHERE wallet='A'",
    )
    for sql in corruptions:
        conn.execute("SAVEPOINT corrupt_summary")
        conn.execute(sql)
        with pytest.raises(ValueError, match="wallet_pnl_summary"):
            store._verify_v5_wallet_pnl_summary(conn)
        conn.execute("ROLLBACK TO corrupt_summary")
        conn.execute("RELEASE corrupt_summary")
    conn.rollback()
    assert tuple(conn.iterdump()) == before
    conn.close()

    # A half-applied migration may have a weak, malformed operational table. Rebuild
    # deletes it before independently verifying even when the evidence history is empty.
    empty = sqlite3.connect(":memory:")
    empty.execute(
        "CREATE TABLE wallet_pnl_events ("
        "id INTEGER PRIMARY KEY, at REAL, wallet TEXT, realized_pnl_sol REAL)"
    )
    empty.execute(
        "CREATE TABLE wallet_pnl_summary ("
        "wallet, event_count, realized_pnl_sol, last_at, last_event_id)"
    )
    malformed = (b"wallet", "many", "bad", None, b"event")
    empty.execute("INSERT INTO wallet_pnl_summary VALUES (?,?,?,?,?)", malformed)
    empty.commit()
    empty.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_wallet_pnl_summary(empty)
    assert empty.execute("SELECT * FROM wallet_pnl_summary").fetchall() == []
    assert store._verify_v5_wallet_pnl_summary(empty) is None
    assert empty.in_transaction
    empty.rollback()
    assert empty.execute("SELECT * FROM wallet_pnl_summary").fetchall() == [malformed]
    empty.close()


def test_v5_creator_summary_rebuilder_unit_contract():
    import memebot.store as store

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE creator_reputation_events ("
        "id INTEGER PRIMARY KEY, mint TEXT, creator TEXT, outcome TEXT, observed_at REAL)"
    )
    conn.execute(dict(store.V5_TABLE_DDL)["creator_reputation_current"])
    events = (
        (4, "MINT_A", "CREATOR_A", "GRADUATED", 30.0),
        (9, "MINT_A", "CREATOR_A", "RUGGED", 50.0),
        (5, "MINT_B", "CREATOR_B", "GRADUATED", 40.0),
        (12, "MINT_C", "CREATOR_C", "RUGGED", 10.0),
        (15, "MINT_D", "CREATOR_D", "GRADUATED", 20.0),
        (17, "MINT_D", "CREATOR_D", "RUGGED", 20.0),
        (30, "MINT_E", "CREATOR_E", "GRADUATED", 100.0),
        (40, "MINT_E", "CREATOR_E", "RUGGED", 90.0),
    )
    conn.executemany(
        "INSERT INTO creator_reputation_events VALUES (?,?,?,?,?)", events
    )
    conn.executemany(
        "INSERT INTO creator_reputation_current VALUES (?,?,?,?,?)",
        (
            ("MINT_A", "STALE", "GRADUATED", 1.0, 4),
            ("STALE", "CREATOR", "RUGGED", 2.0, 5),
        ),
    )
    conn.commit()
    before = tuple(conn.iterdump())

    with pytest.raises(RuntimeError, match="active transaction"):
        store._rebuild_v5_creator_reputation_current(conn)
    with pytest.raises(RuntimeError, match="active transaction"):
        store._verify_v5_creator_reputation_current(conn)
    assert tuple(conn.iterdump()) == before

    conn.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_creator_reputation_current(conn)
    expected = [
        ("MINT_A", "CREATOR_A", "RUGGED", 50.0, 9),
        ("MINT_B", "CREATOR_B", "GRADUATED", 40.0, 5),
        ("MINT_C", "CREATOR_C", "RUGGED", 10.0, 12),
        ("MINT_D", "CREATOR_D", "RUGGED", 20.0, 17),
        ("MINT_E", "CREATOR_E", "GRADUATED", 100.0, 30),
    ]
    assert conn.execute(
        "SELECT mint,creator,outcome,observed_at,event_id "
        "FROM creator_reputation_current ORDER BY mint"
    ).fetchall() == expected
    assert store._verify_v5_creator_reputation_current(conn) is None
    assert conn.in_transaction
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    assert conn.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='trigger' "
        "AND name='creator_reputation_current_after_insert'"
    ).fetchone() is None

    corruptions = (
        "UPDATE creator_reputation_current SET creator='OTHER' WHERE mint='MINT_A'",
        "UPDATE creator_reputation_current SET outcome='GRADUATED' WHERE mint='MINT_A'",
        "UPDATE creator_reputation_current SET observed_at=49.0 WHERE mint='MINT_A'",
        "UPDATE creator_reputation_current SET event_id=4 WHERE mint='MINT_A'",
        "DELETE FROM creator_reputation_current WHERE mint='MINT_A'",
        "INSERT INTO creator_reputation_current VALUES "
        "('EXTRA','CREATOR','RUGGED',1.0,100)",
    )
    for sql in corruptions:
        conn.execute("SAVEPOINT corrupt_creator_summary")
        conn.execute(sql)
        with pytest.raises(ValueError, match="creator_reputation_current"):
            store._verify_v5_creator_reputation_current(conn)
        conn.execute("ROLLBACK TO corrupt_creator_summary")
        conn.execute("RELEASE corrupt_creator_summary")
    conn.rollback()
    assert tuple(conn.iterdump()) == before
    conn.close()

    # A half-applied migration may have a weak, malformed operational table. Rebuild
    # deletes it before independently verifying even when the evidence history is empty.
    empty = sqlite3.connect(":memory:")
    empty.execute(
        "CREATE TABLE creator_reputation_events ("
        "id INTEGER PRIMARY KEY, mint TEXT, creator TEXT, outcome TEXT, observed_at REAL)"
    )
    empty.execute(
        "CREATE TABLE creator_reputation_current ("
        "mint, creator, outcome, observed_at, event_id)"
    )
    malformed = (b"mint", None, "UNKNOWN", "yesterday", b"event")
    empty.execute(
        "INSERT INTO creator_reputation_current VALUES (?,?,?,?,?)", malformed
    )
    empty.commit()
    empty.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_creator_reputation_current(empty)
    assert empty.execute("SELECT * FROM creator_reputation_current").fetchall() == []
    assert store._verify_v5_creator_reputation_current(empty) is None
    assert empty.in_transaction
    empty.rollback()
    assert empty.execute("SELECT * FROM creator_reputation_current").fetchall() == [
        malformed
    ]
    empty.close()

    # A valid healing table need not yet have the eventual creator index. Bound VM
    # work so a correlated per-row scan cannot re-enter this migration path.
    bounded = sqlite3.connect(":memory:")
    bounded.execute(
        "CREATE TABLE creator_reputation_events ("
        "id INTEGER PRIMARY KEY, mint TEXT, creator TEXT, outcome TEXT, observed_at REAL)"
    )
    bounded.execute(dict(store.V5_TABLE_DDL)["creator_reputation_current"])
    bounded.executemany(
        "INSERT INTO creator_reputation_events VALUES (?,?,?,?,?)",
        (
            (index, f"MINT_{index}", f"CREATOR_{index}", "GRADUATED", float(index))
            for index in range(1, 1001)
        ),
    )
    bounded.commit()
    progress_calls = 0

    def count_progress():
        nonlocal progress_calls
        progress_calls += 1
        return 0

    bounded.set_progress_handler(count_progress, 100)
    bounded.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_creator_reputation_current(bounded)
    bounded.set_progress_handler(None, 0)
    assert progress_calls < 5_000
    assert bounded.execute(
        "SELECT count(*) FROM creator_reputation_current"
    ).fetchone() == (1_000,)
    bounded.rollback()
    bounded.close()


def test_v5_p3_position_summary_rebuilder_unit_contract():
    import memebot.store as store

    def connection():
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE paper_entry_executions ("
            "id INTEGER PRIMARY KEY, decision_id INTEGER, at REAL, status TEXT, "
            "paper_trade_id INTEGER)"
        )
        conn.execute(
            "CREATE TABLE paper_trades ("
            "id INTEGER PRIMARY KEY, decision_id INTEGER, at REAL, mint TEXT, "
            "side TEXT, qty REAL, fill_price REAL, fees_json TEXT, "
            "p3_entry_execution_id INTEGER)"
        )
        conn.execute(
            "CREATE TABLE outcomes ("
            "id INTEGER PRIMARY KEY, ref_kind TEXT, ref_id INTEGER, "
            "pnl_sol REAL, p3_exit_trade_id INTEGER)"
        )
        conn.execute(dict(store.V5_TABLE_DDL)["p3_position_current"])
        return conn

    def seed(conn):
        conn.executemany(
            "INSERT INTO paper_entry_executions VALUES (?,?,?,?,?)",
            ((60, 20, 4.0, "FILLED", 50), (61, 21, 10.0, "FILLED", 51)),
        )
        conn.executemany(
            "INSERT INTO paper_trades VALUES (?,?,?,?,?,?,?,?,?)",
            (
                (50, 20, 4.0, "MINT_OPEN", "buy", 2.0, 5.0,
                 '{"network":0.25}', None),
                (51, 21, 10.0, "MINT_CLOSED", "buy", 4.0, 2.0,
                 '{"network":0.5}', None),
                (70, 21, 11.0, "MINT_CLOSED", "sell", 1.5, 3.0,
                 '{"network":0.25}', 61),
                (72, 21, 12.0, "MINT_CLOSED", "sell", 2.5, 4.0,
                 '{"network":0.25}', 61),
            ),
        )
        conn.execute(
            "INSERT INTO outcomes VALUES (80,'trade',72,5.5,72)"
        )

    conn = connection()
    seed(conn)
    conn.execute(
        "INSERT INTO p3_position_current VALUES "
        "(999,'STALE',999,1.0,0.0,1.0,0.0,7,1.0)"
    )
    conn.commit()
    before = tuple(conn.iterdump())

    with pytest.raises(RuntimeError, match="active transaction"):
        store._rebuild_v5_p3_position_current(conn)
    with pytest.raises(RuntimeError, match="active transaction"):
        store._verify_v5_p3_position_current(conn)
    assert tuple(conn.iterdump()) == before

    conn.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_p3_position_current(conn)
    assert conn.execute(
        "SELECT decision_id,mint,entry_execution_id,bought_qty,sold_qty,"
        "buy_notional_sol,sell_proceeds_sol,ladder_mask,last_trade_at "
        "FROM p3_position_current ORDER BY decision_id"
    ).fetchall() == [
        (20, "MINT_OPEN", 60, 2.0, 0.0, 10.25, 0.0, 0, 4.0),
        (21, "MINT_CLOSED", 61, 4.0, 4.0, 8.5, 14.0, 0, 12.0),
    ]
    assert conn.execute(
        "SELECT decision_id FROM p3_position_current "
        "WHERE sold_qty<bought_qty ORDER BY decision_id"
    ).fetchall() == [(20,)]
    assert store._verify_v5_p3_position_current(conn) is None
    assert conn.in_transaction
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    assert conn.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='trigger' "
        "AND name='p3_position_after_filled_entry'"
    ).fetchone() is None

    corruptions = (
        "UPDATE p3_position_current SET mint='OTHER' WHERE decision_id=20",
        "UPDATE p3_position_current SET entry_execution_id=999 WHERE decision_id=20",
        "UPDATE p3_position_current SET bought_qty=1.0 WHERE decision_id=20",
        "UPDATE p3_position_current SET buy_notional_sol=10.0 WHERE decision_id=20",
        "UPDATE p3_position_current SET ladder_mask=1 WHERE decision_id=20",
        "UPDATE p3_position_current SET last_trade_at=3.0 WHERE decision_id=20",
        "UPDATE p3_position_current SET sold_qty=3.5 WHERE decision_id=21",
        "UPDATE p3_position_current SET sell_proceeds_sol=13.0 WHERE decision_id=21",
        "UPDATE p3_position_current SET last_trade_at=11.0 WHERE decision_id=21",
        "DELETE FROM p3_position_current WHERE decision_id=20",
        "INSERT INTO p3_position_current VALUES "
        "(999,'EXTRA',999,1.0,0.0,1.0,0.0,0,1.0)",
    )
    for sql in corruptions:
        conn.execute("SAVEPOINT corrupt_p3_position")
        conn.execute(sql)
        with pytest.raises(ValueError, match="p3_position_current"):
            store._verify_v5_p3_position_current(conn)
        conn.execute("ROLLBACK TO corrupt_p3_position")
        conn.execute("RELEASE corrupt_p3_position")

    evidence_disagreements = (
        ("UPDATE paper_entry_executions SET at=4.5 WHERE id=60",),
        (
            "UPDATE paper_trades SET mint='' WHERE id=50",
            "UPDATE p3_position_current SET mint='' WHERE decision_id=20",
        ),
        ("DELETE FROM outcomes WHERE id=80",),
        ("INSERT INTO outcomes VALUES (81,'trade',999,0.0,999)",),
        ("INSERT INTO outcomes VALUES (81,'trade',50,0.0,50)",),
        ("INSERT INTO outcomes VALUES (81,'trade',72,5.5,NULL)",),
        (
            "INSERT INTO paper_trades VALUES "
            "(90,20,6.0,'MINT_OPEN','sell',1.0,1.0,'{}',NULL)",
            "INSERT INTO outcomes VALUES (81,'trade',90,0.0,NULL)",
        ),
        (
            "INSERT INTO paper_trades VALUES "
            "(69,21,10.5,'MINT_CLOSED','sell',0.0,1.0,'{}',61)",
        ),
        (
            "INSERT INTO paper_trades VALUES "
            "(69,21,10.5,'MINT_CLOSED','sell',0.0,-1.0,'{}',61)",
        ),
        (
            "INSERT INTO paper_trades VALUES "
            "(69,21,10.5,'MINT_CLOSED','sell',0.0,1e101,'{}',61)",
        ),
        (
            "INSERT INTO paper_trades VALUES "
            "(69,21,10.5,'MINT_CLOSED','sell',-1.0,1.0,'{}',61)",
        ),
        (
            "INSERT INTO paper_trades VALUES "
            "(69,21,10.5,'MINT_CLOSED','sell',1e101,1.0,'{}',61)",
        ),
    )
    for statements in evidence_disagreements:
        conn.execute("SAVEPOINT corrupt_p3_evidence")
        for sql in statements:
            conn.execute(sql)
        with pytest.raises(ValueError, match="p3_position_current"):
            store._verify_v5_p3_position_current(conn)
        conn.execute("ROLLBACK TO corrupt_p3_evidence")
        conn.execute("RELEASE corrupt_p3_evidence")

    sqlite_invalid_mint = "a" * 128 + "\t"
    conn.execute("SAVEPOINT corrupt_p3_sqlite_mint")
    conn.execute("UPDATE paper_trades SET mint=? WHERE id=50", (sqlite_invalid_mint,))
    conn.execute(
        "UPDATE p3_position_current SET mint=? WHERE decision_id=20",
        (sqlite_invalid_mint,),
    )
    with pytest.raises(ValueError, match="p3_position_current"):
        store._verify_v5_p3_position_current(conn)
    conn.execute("ROLLBACK TO corrupt_p3_sqlite_mint")
    conn.execute("RELEASE corrupt_p3_sqlite_mint")
    conn.rollback()
    assert tuple(conn.iterdump()) == before
    conn.close()

    invalid_graphs = (
        (
            "entry execution BUY time",
            "UPDATE paper_entry_executions SET at=4.5 WHERE id=60",
        ),
        (
            "entry execution BUY time",
            "UPDATE paper_entry_executions SET at=-1.0 WHERE id=60",
        ),
        (
            "entry execution BUY time",
            (
                "UPDATE paper_entry_executions SET at=-1.0 WHERE id=60",
                "UPDATE paper_trades SET at=-1.0 WHERE id=50",
            ),
        ),
        (
            "missing entry BUY link",
            "UPDATE paper_entry_executions SET paper_trade_id=999 WHERE id=60",
        ),
        (
            "over-sold",
            "UPDATE paper_trades SET qty=4.5 WHERE id=72",
        ),
        (
            "retrograde trade time",
            "UPDATE paper_trades SET at=10.0 WHERE id=70",
        ),
        (
            "non-finite position arithmetic",
            "UPDATE paper_trades SET qty=1e100,fill_price=1e100 WHERE id=50",
        ),
        (
            "partial pre-v5 P3 SELL",
            "UPDATE paper_trades SET qty=2.0 WHERE id=72",
        ),
        (
            "closed position outcome mismatch",
            "DELETE FROM outcomes WHERE id=80",
        ),
        (
            "closed position outcome mismatch",
            "UPDATE outcomes SET pnl_sol=5.4 WHERE id=80",
        ),
    )
    for message, mutation in invalid_graphs:
        malformed = connection()
        seed(malformed)
        statements = (mutation,) if isinstance(mutation, str) else mutation
        for sql in statements:
            malformed.execute(sql)
        malformed.commit()
        malformed.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError, match=message):
            store._rebuild_v5_p3_position_current(malformed)
        assert malformed.in_transaction
        malformed.rollback()
        malformed.close()

    empty = connection()
    empty.execute(
        "INSERT INTO p3_position_current VALUES "
        "(999,'STALE',999,1.0,0.0,1.0,0.0,7,1.0)"
    )
    empty.commit()
    empty.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_p3_position_current(empty)
    assert empty.execute("SELECT * FROM p3_position_current").fetchall() == []
    assert store._verify_v5_p3_position_current(empty) is None
    assert empty.in_transaction
    empty.rollback()
    empty.close()

    sqlite_trim_valid = connection()
    seed(sqlite_trim_valid)
    sqlite_trim_valid.execute("UPDATE paper_trades SET mint='\t' WHERE id=50")
    sqlite_trim_valid.commit()
    sqlite_trim_valid.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_p3_position_current(sqlite_trim_valid)
    assert sqlite_trim_valid.execute(
        "SELECT mint FROM p3_position_current WHERE decision_id=20"
    ).fetchone() == ("\t",)
    assert store._verify_v5_p3_position_current(sqlite_trim_valid) is None
    sqlite_trim_valid.rollback()
    sqlite_trim_valid.close()

    sqlite_trim_invalid = connection()
    seed(sqlite_trim_invalid)
    sqlite_trim_invalid.execute(
        "UPDATE paper_trades SET mint=? WHERE id=50", (sqlite_invalid_mint,)
    )
    sqlite_trim_invalid.commit()
    sqlite_trim_invalid.execute("BEGIN IMMEDIATE")
    with pytest.raises(ValueError, match="missing entry BUY link"):
        store._rebuild_v5_p3_position_current(sqlite_trim_invalid)
    assert sqlite_trim_invalid.in_transaction
    sqlite_trim_invalid.rollback()
    sqlite_trim_invalid.close()


def test_v5_canonical_pending_summary_rebuilder_unit_contract():
    import json

    import memebot.store as store

    def connection():
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE decisions (id INTEGER PRIMARY KEY, feature_vector_json TEXT)"
        )
        conn.execute(
            "CREATE TABLE canonical_observations ("
            "id INTEGER PRIMARY KEY, decision_id INTEGER, mint TEXT, observed_at REAL, "
            "is_subject INTEGER, is_canonical INTEGER, eligible INTEGER, "
            "start_price_sol REAL, price_observed_at REAL, price_source TEXT, "
            "unavailable_reason TEXT)"
        )
        conn.execute(
            "CREATE TABLE outcomes (id INTEGER PRIMARY KEY, at REAL, ref_kind TEXT, "
            "ref_id INTEGER, pnl_sol REAL, detail_json TEXT, p3_exit_trade_id INTEGER)"
        )
        conn.execute(dict(store.V5_TABLE_DDL)["canonical_pending_current"])
        return conn

    def feature(horizons):
        return json.dumps(
            {
                "canonical": {
                    "ranking_inputs": {
                        "counterfactual_horizons_s": horizons,
                    }
                }
            },
            separators=(",", ":"),
        )

    def detail(
        horizon,
        *,
        price0=2.0,
        price0_observed_at=99.0,
        forward_return_pct=100.0,
        price_now=4.0,
        price_now_observed_at=101.0,
        terminal=None,
        unavailable_reason="",
    ):
        return json.dumps(
            {
                "horizon_s": horizon,
                "forward_return_pct": forward_return_pct,
                "price0": price0,
                "price0_observed_at": price0_observed_at,
                "price_now": price_now,
                "price_now_observed_at": price_now_observed_at,
                "terminal": terminal,
                "unavailable_reason": unavailable_reason,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def seed(conn):
        conn.executemany(
            "INSERT INTO decisions VALUES (?,?)",
            (
                (10, feature([1, 2.0, 4])),
                (11, feature([0.5, 1.5])),
                (12, feature(list(range(1, 33)))),
            ),
        )
        conn.executemany(
            "INSERT INTO canonical_observations VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                (20, 10, "MINT_A", 100.0, 1, 1, 1, 2.0, 99.0,
                 "curve_snapshot", ""),
                (21, 10, "MINT_B", 100.0, 0, 0, 1, 3.0, 98.0,
                 "curve_snapshot", ""),
                (22, 11, "MINT_C", 200.0, 0, 1, 1, 5.0, 199.0,
                 "curve_snapshot", ""),
                (23, 11, "MINT_D", 200.0, 0, 0, 1, None, None, "",
                 "start_price_missing"),
                (24, 11, "MINT_E", 200.0, 0, 0, 0, 6.0, 199.0,
                 "curve_snapshot", ""),
                (25, 12, "MINT_FULL", 300.0, 0, 0, 1, 7.0, 299.0,
                 "curve_snapshot", ""),
            ),
        )
        conn.executemany(
            "INSERT INTO outcomes(id,at,ref_kind,ref_id,pnl_sol,detail_json) "
            "VALUES (?,?,?,?,?,?)",
            (
                (30, 104.0, "canonical_observation", 20, 0.0,
                 detail(
                     4,
                     forward_return_pct=None,
                     price_now=None,
                     price_now_observed_at=None,
                     terminal=None,
                     unavailable_reason="journal_replay_gap",
                 )),
                (29, 101.0, "canonical_observation", 20, 0.0, detail(1)),
                (31, 200.5, "canonical_observation", 22, 0.0,
                 detail(
                     0.5,
                     price0=5.0,
                     price0_observed_at=199.0,
                     forward_return_pct=None,
                     price_now=None,
                     price_now_observed_at=None,
                     terminal="GRADUATED",
                     unavailable_reason="graduated_no_price",
                 )),
                (32, 1.0, "candidate", 999, -1.0, "{}"),
                (34, 201.5, "canonical_observation", 22, 0.0,
                 detail(
                     1.5,
                     price0=5.0,
                     price0_observed_at=199.0,
                     forward_return_pct=None,
                     price_now=None,
                     price_now_observed_at=None,
                     terminal=None,
                     unavailable_reason="journal_replay_gap",
                 )),
            ),
        )
        conn.executemany(
            "INSERT INTO outcomes(id,at,ref_kind,ref_id,pnl_sol,detail_json) "
            "VALUES (?,?,?,?,?,?)",
            (
                (
                    99 + horizon,
                    300.0 + horizon,
                    "canonical_observation",
                    25,
                    0.0,
                    detail(
                        horizon,
                        price0=7.0,
                        price0_observed_at=299.0,
                        forward_return_pct=None,
                        price_now=None,
                        price_now_observed_at=None,
                        terminal=None,
                        unavailable_reason="journal_replay_gap",
                    ),
                )
                for horizon in range(1, 33)
            ),
        )

    conn = connection()
    seed(conn)
    conn.execute(
        "INSERT INTO canonical_pending_current VALUES (999,999,'[1]',1,1)"
    )
    conn.commit()
    before = tuple(conn.iterdump())

    with pytest.raises(RuntimeError, match="active transaction"):
        store._rebuild_v5_canonical_pending_current(conn)
    with pytest.raises(RuntimeError, match="active transaction"):
        store._verify_v5_canonical_pending_current(conn)
    assert tuple(conn.iterdump()) == before

    conn.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_canonical_pending_current(conn)
    assert conn.execute(
        "SELECT observation_id,decision_id,horizons_json,full_mask,completed_mask "
        "FROM canonical_pending_current ORDER BY observation_id"
    ).fetchall() == [
        (20, 10, "[1,2.0,4]", 7, 5),
        (21, 10, "[1,2.0,4]", 7, 0),
        (22, 11, "[0.5,1.5]", 3, 3),
        (25, 12, "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,"
         "21,22,23,24,25,26,27,28,29,30,31,32]", 4294967295, 4294967295),
    ]
    assert store._verify_v5_canonical_pending_current(conn) is None
    assert conn.in_transaction
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    assert conn.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='trigger' "
        "AND name='p3_canonical_pending_after_observation'"
    ).fetchone() is None

    accepted_exit_links = []
    conn.execute("SAVEPOINT canonical_outcome_exit_link")
    conn.execute(
        "INSERT INTO outcomes VALUES "
        "(35,102.0,'canonical_observation',21,0.0,?,777)",
        (detail(
            2.0,
            price0=3.0,
            price0_observed_at=98.0,
            price_now=6.0,
            price_now_observed_at=102.0,
        ),),
    )
    conn.execute(
        "UPDATE canonical_pending_current SET completed_mask=2 "
        "WHERE observation_id=21"
    )
    try:
        store._verify_v5_canonical_pending_current(conn)
    except ValueError as exc:
        assert str(exc) == "canonical_pending_current source mismatch"
    else:
        accepted_exit_links.append("verifier")
    conn.execute("ROLLBACK TO canonical_outcome_exit_link")
    conn.execute("RELEASE canonical_outcome_exit_link")

    corruptions = (
        "UPDATE canonical_pending_current SET decision_id=11 WHERE observation_id=20",
        "UPDATE canonical_pending_current SET horizons_json='[1,2,4]' "
        "WHERE observation_id=20",
        "UPDATE canonical_pending_current SET completed_mask=1 WHERE observation_id=20",
        "DELETE FROM canonical_pending_current WHERE observation_id=20",
        "INSERT INTO canonical_pending_current VALUES (999,999,'[1]',1,0)",
    )
    for sql in corruptions:
        conn.execute("SAVEPOINT corrupt_pending_summary")
        conn.execute(sql)
        with pytest.raises(ValueError, match="canonical_pending_current"):
            store._verify_v5_canonical_pending_current(conn)
        conn.execute("ROLLBACK TO corrupt_pending_summary")
        conn.execute("RELEASE corrupt_pending_summary")

    # Mutating source evidence and the operational row together must still be caught
    # by the independent verifier rather than merely comparing a replayed mask.
    conn.execute("SAVEPOINT corrupt_pending_evidence")
    conn.execute(
        "UPDATE outcomes SET detail_json=? WHERE id=29",
        (detail(2, price_now_observed_at=101.0),),
    )
    conn.execute(
        "UPDATE canonical_pending_current SET completed_mask=6 WHERE observation_id=20"
    )
    with pytest.raises(ValueError, match="canonical_pending_current"):
        store._verify_v5_canonical_pending_current(conn)
    conn.execute("ROLLBACK TO corrupt_pending_evidence")
    conn.execute("RELEASE corrupt_pending_evidence")
    conn.rollback()
    assert tuple(conn.iterdump()) == before
    conn.close()

    exit_linked = connection()
    seed(exit_linked)
    exit_linked.execute(
        "INSERT INTO outcomes VALUES "
        "(35,102.0,'canonical_observation',21,0.0,?,777)",
        (detail(
            2.0,
            price0=3.0,
            price0_observed_at=98.0,
            price_now=6.0,
            price_now_observed_at=102.0,
        ),),
    )
    exit_linked.commit()
    exit_linked.execute("BEGIN IMMEDIATE")
    try:
        store._rebuild_v5_canonical_pending_current(exit_linked)
    except ValueError as exc:
        assert str(exc) == "invalid canonical outcome id=35"
    else:
        accepted_exit_links.append("rebuild")
    exit_linked.rollback()
    exit_linked.close()
    assert accepted_exit_links == []

    invalid_graphs = (
        ("observation decision link", "UPDATE canonical_observations SET decision_id=999 WHERE id=20"),
        ("canonical observation", "UPDATE canonical_observations SET price_source='other' WHERE id=20"),
        ("canonical observation", "UPDATE canonical_observations SET unavailable_reason='start_price_missing' WHERE id=20"),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json='{}' WHERE id=10"),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([1, 2.0, 4]).encode()),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([])),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([1, True])),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([1, 1.0])),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([9223372036854775808, 9223372036854775809])),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([2, 1])),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature(list(range(1, 34)))),
        ("horizon tuple", "UPDATE decisions SET feature_vector_json=? WHERE id=10", feature([1, float("inf")])),
        ("canonical outcome", "UPDATE outcomes SET ref_id=999 WHERE id=29"),
        ("canonical outcome", "UPDATE outcomes SET ref_id=23 WHERE id=29"),
        ("canonical outcome", "UPDATE outcomes SET ref_id=24 WHERE id=29"),
        ("canonical outcome", "UPDATE outcomes SET pnl_sol=1.0 WHERE id=29"),
        ("canonical outcome", "UPDATE outcomes SET at=100.5 WHERE id=29"),
        ("canonical outcome", "UPDATE outcomes SET detail_json='{}' WHERE id=29"),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1)[:-1] + ',"unexpected":1}'),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(3)),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1, price0=3.0)),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1, price_now_observed_at=102.0)),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1, forward_return_pct=99.0)),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1, terminal="UNKNOWN")),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1, price_now=-1.0, forward_return_pct=-150.0)),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1).replace('"forward_return_pct":100.0', '"forward_return_pct":Infinity')),
        ("canonical outcome", "UPDATE outcomes SET detail_json=? WHERE id=29", detail(1, forward_return_pct=None, price_now=None, price_now_observed_at=None, terminal="GRADUATED", unavailable_reason="journal_replay_gap")),
    )
    for case in invalid_graphs:
        malformed = connection()
        seed(malformed)
        sql, parameters = case[1], case[2:]
        malformed.execute(sql, parameters)
        malformed.commit()
        malformed.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError, match=case[0]):
            store._rebuild_v5_canonical_pending_current(malformed)
        assert malformed.in_transaction
        malformed.rollback()
        malformed.close()

    duplicate = connection()
    seed(duplicate)
    duplicate.execute(
        "INSERT INTO outcomes(id,at,ref_kind,ref_id,pnl_sol,detail_json) VALUES "
        "(33,102.0,'canonical_observation',20,0.0,?)",
        (detail(1.0, price_now_observed_at=101.0),),
    )
    duplicate.commit()
    duplicate.execute("BEGIN IMMEDIATE")
    with pytest.raises(ValueError, match="duplicate canonical outcome id=33"):
        store._rebuild_v5_canonical_pending_current(duplicate)
    assert duplicate.in_transaction
    duplicate.rollback()
    duplicate.close()

    empty = connection()
    empty.execute(
        "INSERT INTO canonical_pending_current VALUES (999,999,'[1]',1,1)"
    )
    empty.commit()
    empty.execute("BEGIN IMMEDIATE")
    store._rebuild_v5_canonical_pending_current(empty)
    assert empty.execute("SELECT * FROM canonical_pending_current").fetchall() == []
    assert store._verify_v5_canonical_pending_current(empty) is None
    assert empty.in_transaction
    empty.rollback()
    assert empty.execute("SELECT * FROM canonical_pending_current").fetchall() == [
        (999, 999, "[1]", 1, 1)
    ]
    empty.close()


def test_v5_canonical_pending_rebuild_creates_exact_observation_rows(tmp_path):
    import json

    import memebot.store as store

    path = tmp_path / "v4-pending.db"
    conn = _open_v4_fixture(path)
    store._apply_v5_additive_columns(conn)
    for _, ddl in store.V5_TABLE_DDL:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO tokens(mint,created_at,state,curve_progress,last_seen,meta_json,bonding_curve_key) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            ("ELIGIBLE", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-eligible"),
            ("INELIGIBLE", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-ineligible"),
            ("UNAVAILABLE", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-unavailable"),
            ("MISSING", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-missing"),
        ),
    )
    feature = json.dumps(
        {"canonical": {"ranking_inputs": {"counterfactual_horizons_s": [60, 3600]}}},
        separators=(",", ":"),
    )
    conn.executemany(
        "INSERT INTO decisions(id,at,mint,segment,action,score,feature_vector_json,safety_report_id,config_hash) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            (10, 10.0, "ELIGIBLE", "CLIMBING", "BUY", 90.0, feature, None, "cfg"),
            (11, 11.0, "INELIGIBLE", "CLIMBING", "SKIP", 0.0, feature, None, "cfg"),
            (12, 12.0, "UNAVAILABLE", "CLIMBING", "SKIP", 0.0, feature, None, "cfg"),
            (13, 13.0, "MISSING", "CLIMBING", "SKIP", 0.0, feature, None, "cfg"),
        ),
    )
    conn.executemany(
        "INSERT INTO canonical_observations "
        "(id,decision_id,mint,observed_at,is_subject,is_canonical,eligible,start_price_sol,"
        "price_observed_at,price_source,unavailable_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            (20, 10, "ELIGIBLE", 10.0, 1, 1, 1, 2.0, 9.0, "curve_snapshot", ""),
            (21, 11, "INELIGIBLE", 11.0, 0, 0, 0, 2.0, 10.0, "curve_snapshot", ""),
            (22, 12, "UNAVAILABLE", 12.0, 0, 0, 1, None, None, "", "start_price_stale"),
            (23, 13, "MISSING", 13.0, 0, 0, 1, None, None, "", "start_price_missing"),
        ),
    )
    conn.execute("PRAGMA user_version=4")
    conn.commit()
    conn.close()

    upgraded = store.open_db(path)
    try:
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 6
        assert [tuple(row) for row in upgraded.execute(
            "SELECT observation_id,decision_id,horizons_json,full_mask,completed_mask "
            "FROM canonical_pending_current ORDER BY observation_id"
        ).fetchall()] == [(20, 10, "[60,3600]", 3, 0)]
        assert upgraded.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert upgraded.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        upgraded.close()


def test_v5_canonical_pending_rebuild_replays_completed_horizons(tmp_path):
    import json

    import memebot.store as store

    path = tmp_path / "v4-pending-completed.db"
    conn = _open_v4_fixture(path)
    store._apply_v5_additive_columns(conn)
    for _, ddl in store.V5_TABLE_DDL:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO tokens(mint,created_at,state,curve_progress,last_seen,meta_json,bonding_curve_key) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            ("FULL", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-full"),
            ("PARTIAL", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-partial"),
            ("EXCLUDED", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-excluded"),
            ("UNAVAILABLE", 1.0, "CLIMBING", 0.1, 1.0, "{}", "curve-unavailable"),
        ),
    )
    feature = json.dumps(
        {"canonical": {"ranking_inputs": {"counterfactual_horizons_s": [60, 3600]}}},
        separators=(",", ":"),
    )
    conn.executemany(
        "INSERT INTO decisions(id,at,mint,segment,action,score,feature_vector_json,safety_report_id,config_hash) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            (10, 10.0, "FULL", "CLIMBING", "BUY", 90.0, feature, None, "cfg"),
            (11, 11.0, "PARTIAL", "CLIMBING", "BUY", 90.0, feature, None, "cfg"),
            (12, 12.0, "EXCLUDED", "CLIMBING", "SKIP", 0.0, feature, None, "cfg"),
            (13, 13.0, "UNAVAILABLE", "CLIMBING", "SKIP", 0.0, feature, None, "cfg"),
        ),
    )
    conn.executemany(
        "INSERT INTO canonical_observations "
        "(id,decision_id,mint,observed_at,is_subject,is_canonical,eligible,start_price_sol,"
        "price_observed_at,price_source,unavailable_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            (20, 10, "FULL", 10.0, 1, 1, 1, 2.0, 9.0, "curve_snapshot", ""),
            (21, 11, "PARTIAL", 11.0, 1, 1, 1, 2.0, 10.0, "curve_snapshot", ""),
            (22, 12, "EXCLUDED", 12.0, 0, 0, 0, 2.0, 11.0, "curve_snapshot", ""),
            (23, 13, "UNAVAILABLE", 13.0, 1, 1, 1, None, None, "", "start_price_stale"),
        ),
    )

    def outcome(outcome_id, at, observation_id, horizon, price_now, observed_at):
        detail = {
            "forward_return_pct": 100.0 * (price_now - 2.0) / 2.0,
            "horizon_s": horizon,
            "price0": 2.0,
            "price0_observed_at": observed_at - 1.0,
            "price_now": price_now,
            "price_now_observed_at": observed_at + horizon,
            "terminal": None,
            "unavailable_reason": "",
        }
        return (
            outcome_id, at, "canonical_observation", observation_id, 0.0,
            json.dumps(detail, sort_keys=True, separators=(",", ":")), None,
        )

    conn.executemany(
        "INSERT INTO outcomes(id,at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            outcome(1, 71.0, 20, 60, 2.2, 10.0),
            outcome(2, 3611.0, 20, 3600, 2.4, 10.0),
            outcome(3, 72.0, 21, 60, 2.1, 11.0),
        ),
    )
    conn.execute("PRAGMA user_version=4")
    conn.commit()
    conn.close()

    upgraded = store.open_db(path)
    try:
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 6
        assert [tuple(row) for row in upgraded.execute(
            "SELECT observation_id,decision_id,horizons_json,full_mask,completed_mask "
            "FROM canonical_pending_current ORDER BY observation_id"
        ).fetchall()] == [
            (20, 10, "[60,3600]", 3, 3),
            (21, 11, "[60,3600]", 3, 1),
        ]
        assert upgraded.execute(
            "SELECT count(*) FROM canonical_pending_current WHERE observation_id IN (22,23)"
        ).fetchone()[0] == 0
        assert upgraded.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert upgraded.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        upgraded.close()


def test_v5_evidence_tables_reject_update_delete(tmp_path):
    import json

    import memebot.store as store

    expected_tables = (
        "decisions",
        "paper_trades",
        "outcomes",
        "regime_log",
        "wallet_pnl_events",
        "early_buyer_reads",
        "safety_reports",
        "holder_evidence",
        "creator_reputation_events",
        "canonical_observations",
        "canonical_generations",
        "canonical_rechecks",
        "paper_entry_executions",
    )
    assert store.SCHEMA_V5_IMMUTABLE_TABLES == expected_tables
    assert tuple(store.SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE) == expected_tables[:6]
    assert store.SCHEMA_V5_NEW_IMMUTABLE_TABLES == expected_tables[6:]

    conn = open_db(tmp_path / "v5-immutable.db")
    try:
        conn.execute(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES ('MINT',0.0,0.0)"
        )
        safety_report_id = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES ('MINT',1.0,'[]',0.0,?)",
            ("b" * 64,),
        ).lastrowid
        conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,unavailable_reason,"
            "inputs_hash) VALUES (?,?,?,?,?,?,?)",
            (safety_report_id, 2, 2, 25.0, 0.5, "", "c" * 64),
        )
        conn.execute(
            "INSERT INTO creator_reputation_events("
            "mint,creator,outcome,observed_at) "
            "VALUES ('MINT','CREATOR','GRADUATED',1.0)"
        )

        canonical_inputs_hash = "a" * 64
        feature_vector = json.dumps(
            {
                "canonical": {
                    "inputs_hash": canonical_inputs_hash,
                    "planned_size_sol": 1.0,
                    "ranking_inputs": {"counterfactual_horizons_s": [60.0]},
                    "status": "CANONICAL",
                }
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        decision_id = conn.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,safety_report_id,"
            "config_hash) VALUES (?,?,?,?,?,?,?,?)",
            (2.0, "MINT", "CLIMBING", "BUY", 90.0, feature_vector,
             safety_report_id, "cfg"),
        ).lastrowid
        conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (decision_id, "MINT", 2.0, 1, 1, 1, 1.0, 2.0,
             "curve_snapshot", ""),
        )
        conn.execute(
            "INSERT INTO canonical_generations("
            "generation_hash,first_decision_id,created_at) VALUES (?,?,?)",
            ("e" * 64, decision_id, 2.0),
        )

        recheck_payload = json.dumps(
            {
                "attempt": 1,
                "causal_target_report_id": safety_report_id,
                "decision_id": decision_id,
                "latest_target_report_id": safety_report_id,
                "prior_inputs_hash": canonical_inputs_hash,
                "rechecked_at": 3.0,
                "verdict": {
                    "canonical_mint": None,
                    "reason": "copycat_cluster",
                    "status": "SUPPRESSED",
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        recheck_id = conn.execute(
            "INSERT INTO canonical_rechecks("
            "decision_id,attempt,rechecked_at,causal_target_report_id,"
            "latest_target_report_id,status,reason,canonical_mint,"
            "prior_inputs_hash,recheck_inputs_hash,payload_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (decision_id, 1, 3.0, safety_report_id, safety_report_id,
             "CANCEL", "copycat_cluster", None, canonical_inputs_hash,
             "f" * 64, recheck_payload),
        ).lastrowid
        conn.execute(
            "INSERT INTO paper_entry_executions("
            "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id,"
            "paper_trade_id) VALUES (?,?,?,?,?,?,?)",
            (decision_id, 4.0, "CANCELLED", "copycat_cluster", 1.0,
             recheck_id, None),
        )

        paper_trade_id = conn.execute(
            "INSERT INTO paper_trades("
            "at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade) VALUES (1.0,'LEGACY','CLIMBING','buy',1.0,1.0,"
            "1.0,'{}','B')"
        ).lastrowid
        conn.execute(
            "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
            "VALUES (1.0,'trade',?,0.0,'{}')",
            (paper_trade_id,),
        )
        conn.execute(
            "INSERT INTO regime_log(at,state,inputs_json) "
            "VALUES (1.0,'risk_on','{}')"
        )
        conn.execute(
            "INSERT INTO wallet_pnl_events("
            "at,wallet,mint,realized_pnl_sol,source,detail_json) "
            "VALUES (1.0,'WALLET','MINT',1.0,'test','{}')"
        )
        conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash) "
            "VALUES ('MINT',1.0,'[\"WALLET\"]','',?)",
            ("d" * 64,),
        )
        conn.commit()

        for table in expected_tables:
            before = tuple(
                tuple(row)
                for row in conn.execute(f"SELECT rowid,* FROM {table} ORDER BY rowid")
            )
            assert len(before) == 1
            rowid = before[0][0]
            error = (
                "append-only"
                if table in store.SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE
                else "immutable evidence"
            )
            with pytest.raises(sqlite3.IntegrityError, match=error):
                conn.execute(
                    f"UPDATE {table} SET rowid=rowid WHERE rowid=?", (rowid,)
                )
            assert conn.execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0] == 1
            assert tuple(
                tuple(row)
                for row in conn.execute(f"SELECT rowid,* FROM {table} ORDER BY rowid")
            ) == before

            with pytest.raises(sqlite3.IntegrityError, match=error):
                conn.execute(f"DELETE FROM {table} WHERE rowid=?", (rowid,))
            assert conn.execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0] == 1
            assert tuple(
                tuple(row)
                for row in conn.execute(f"SELECT rowid,* FROM {table} ORDER BY rowid")
            ) == before
    finally:
        conn.close()


def test_v5_insert_or_replace_cannot_bypass_immutable_insert_guards(tmp_path):
    import json

    import memebot.store as store

    expected_existing_where = {
        "decisions": "old.id IS NEW.id",
        "paper_trades": "old.id IS NEW.id",
        "outcomes": (
            "old.id IS NEW.id"
            " OR (NEW.p3_exit_trade_id IS NOT NULL"
            " AND old.p3_exit_trade_id IS NEW.p3_exit_trade_id)"
            " OR (NEW.ref_kind='canonical_observation'"
            " AND old.ref_kind='canonical_observation'"
            " AND old.ref_id IS NEW.ref_id"
            " AND json_extract(old.detail_json,'$.horizon_s')"
            " IS json_extract(NEW.detail_json,'$.horizon_s'))"
        ),
        "regime_log": "old.id IS NEW.id",
        "wallet_pnl_events": "old.id IS NEW.id",
        "early_buyer_reads": (
            "old.id IS NEW.id"
            " OR (NEW.safety_report_id IS NOT NULL"
            " AND old.safety_report_id IS NEW.safety_report_id)"
        ),
    }
    expected_new_keys = {
        "safety_reports": (("id",),),
        "holder_evidence": (("id",), ("safety_report_id",)),
        "creator_reputation_events": (("id",), ("mint", "outcome")),
        "canonical_observations": (("id",), ("decision_id", "mint")),
        "canonical_generations": (("generation_hash",), ("first_decision_id",)),
        "canonical_rechecks": (("id",), ("decision_id", "attempt")),
        "paper_entry_executions": (
            ("id",),
            ("decision_id",),
            ("paper_trade_id",),
        ),
    }
    expected_tables = (*expected_existing_where, *expected_new_keys)

    assert store.SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE == expected_existing_where
    assert store.SCHEMA_V5_NEW_IMMUTABLE_KEYS == expected_new_keys
    assert store.SCHEMA_V5_IMMUTABLE_TABLES == expected_tables

    def canonical_json(value):
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )

    def feature(inputs_hash):
        return canonical_json({
            "canonical": {
                "inputs_hash": inputs_hash,
                "planned_size_sol": 1.0,
                "ranking_inputs": {"counterfactual_horizons_s": [60.0]},
                "status": "CANONICAL",
            }
        })

    def recheck_payload(
        *, decision_id, attempt, rechecked_at, report_id, prior_hash,
        status, reason, canonical_mint,
    ):
        return canonical_json({
            "attempt": attempt,
            "causal_target_report_id": report_id,
            "decision_id": decision_id,
            "latest_target_report_id": report_id,
            "prior_inputs_hash": prior_hash,
            "rechecked_at": rechecked_at,
            "verdict": {
                "canonical_mint": canonical_mint,
                "reason": reason,
                "status": status,
            },
        })

    path = tmp_path / "v5-no-replace.db"
    seeded = open_db(path)
    try:
        seeded.executemany(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES (?,?,?)",
            (
                ("MINT", 0.0, 0.0),
                ("FILLED", 0.0, 0.0),
                ("OTHER", 0.0, 0.0),
                ("ALT", 0.0, 0.0),
            ),
        )
        seeded.executemany(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES (?,?,?,?,?)",
            (
                ("MINT", 1.0, "[]", 0.0, "a" * 64),
                ("FILLED", 10.0, "[]", 0.0, "b" * 64),
                ("OTHER", 19.0, "[]", 0.0, "c" * 64),
            ),
        )
        seeded.executemany(
            "INSERT INTO holder_evidence("
            "id,safety_report_id,sampled_token_accounts,"
            "distinct_non_curve_owners,top10_non_curve_owner_share_pct,"
            "holder_observed_at,unavailable_reason,inputs_hash) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                (20, 1, 2, 2, 25.0, 0.5, "", "d" * 64),
                (21, 2, 2, 2, 25.0, 9.5, "", "e" * 64),
            ),
        )
        seeded.execute(
            "INSERT INTO creator_reputation_events("
            "id,mint,creator,outcome,observed_at) "
            "VALUES (80,'MINT','CREATOR','GRADUATED',1.0)"
        )

        prior_cancel = "f" * 64
        prior_filled = "1" * 64
        prior_other = "2" * 64
        seeded.executemany(
            "INSERT INTO decisions("
            "id,at,mint,segment,action,score,feature_vector_json,"
            "safety_report_id,config_hash) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                (30, 2.0, "MINT", "CLIMBING", "BUY", 90.0,
                 feature(prior_cancel), 1, "cfg"),
                (31, 11.0, "FILLED", "CLIMBING", "BUY", 90.0,
                 feature(prior_filled), 2, "cfg"),
                (32, 20.0, "OTHER", "CLIMBING", "BUY", 90.0,
                 feature(prior_other), 3, "cfg"),
                (33, 30.0, "ALT", "CLIMBING", "SKIP", 0.0, "{}", None,
                 "cfg"),
            ),
        )
        seeded.executemany(
            "INSERT INTO canonical_observations("
            "id,decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                (40, 30, "MINT", 2.0, 1, 1, 1, 1.0, 2.0,
                 "curve_snapshot", ""),
                (41, 31, "FILLED", 11.0, 1, 1, 1, 1.0, 11.0,
                 "curve_snapshot", ""),
                (42, 32, "OTHER", 20.0, 1, 0, 0, None, None, "",
                 "start_price_missing"),
            ),
        )
        seeded.executemany(
            "INSERT INTO canonical_generations("
            "generation_hash,first_decision_id,created_at) VALUES (?,?,?)",
            (("3" * 64, 30, 2.0), ("4" * 64, 31, 11.0)),
        )

        cancel_payload = recheck_payload(
            decision_id=30, attempt=1, rechecked_at=3.0, report_id=1,
            prior_hash=prior_cancel, status="SUPPRESSED",
            reason="copycat_cluster", canonical_mint=None,
        )
        pass_payload = recheck_payload(
            decision_id=31, attempt=1, rechecked_at=12.0, report_id=2,
            prior_hash=prior_filled, status="CANONICAL",
            reason="canonical_selected", canonical_mint="FILLED",
        )
        seeded.executemany(
            "INSERT INTO canonical_rechecks("
            "id,decision_id,attempt,rechecked_at,causal_target_report_id,"
            "latest_target_report_id,status,reason,canonical_mint,"
            "prior_inputs_hash,recheck_inputs_hash,payload_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                (50, 30, 1, 3.0, 1, 1, "CANCEL", "copycat_cluster", None,
                 prior_cancel, "5" * 64, cancel_payload),
                (51, 31, 1, 12.0, 2, 2, "PASS", "canonical_selected",
                 "FILLED", prior_filled, "6" * 64, pass_payload),
            ),
        )
        seeded.execute(
            "INSERT INTO paper_entry_executions("
            "id,decision_id,at,status,reason,planned_size_sol,"
            "canonical_recheck_id,paper_trade_id) "
            "VALUES (60,30,4.0,'CANCELLED','copycat_cluster',1.0,50,NULL)"
        )

        seeded.execute(
            "INSERT INTO paper_trades("
            "id,at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade) VALUES "
            "(110,1.0,'LEGACY','CLIMBING','buy',1.0,1.0,1.0,'{}','B')"
        )
        seeded.execute(
            "INSERT INTO paper_trades("
            "id,decision_id,at,mint,segment,side,qty,quote_price,fill_price,"
            "fees_json,realism_grade,canonical_recheck_id,canonical_proof_hash) "
            "VALUES (111,31,13.0,'FILLED','CLIMBING','buy',1.0,1.0,1.0,"
            "'{}','B',51,?)",
            ("6" * 64,),
        )
        seeded.execute(
            "INSERT INTO paper_entry_executions("
            "id,decision_id,at,status,reason,planned_size_sol,"
            "canonical_recheck_id,paper_trade_id) "
            "VALUES (61,31,13.0,'FILLED','filled',1.0,51,111)"
        )
        seeded.execute(
            "INSERT INTO paper_trades("
            "id,decision_id,at,mint,segment,side,qty,quote_price,fill_price,"
            "fees_json,realism_grade,p3_entry_execution_id) "
            "VALUES (112,31,14.0,'FILLED','CLIMBING','sell',1.0,2.0,2.0,"
            "'{}','B',61)"
        )

        horizon_detail = canonical_json({
            "forward_return_pct": 100.0,
            "horizon_s": 60.0,
            "price0": 1.0,
            "price0_observed_at": 2.0,
            "price_now": 2.0,
            "price_now_observed_at": 62.0,
            "terminal": None,
            "unavailable_reason": "",
        })
        exit_detail = canonical_json({
            "grade": "B", "hold_s": 1.0, "reason": "time_stop",
        })
        seeded.executemany(
            "INSERT INTO outcomes("
            "id,at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                (120, 1.0, "trade", 110, 0.0, "{}", None),
                (121, 62.0, "canonical_observation", 40, 0.0,
                 horizon_detail, None),
                (122, 14.0, "trade", 112, 1.0, exit_detail, 112),
            ),
        )
        seeded.execute(
            "INSERT INTO regime_log(id,at,state,inputs_json) "
            "VALUES (90,1.0,'risk_on','{}')"
        )
        seeded.execute(
            "INSERT INTO wallet_pnl_events("
            "id,at,wallet,mint,realized_pnl_sol,source,detail_json) "
            "VALUES (100,1.0,'WALLET','MINT',1.0,'test','{}')"
        )
        seeded.execute(
            "INSERT INTO early_buyer_reads("
            "id,mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id) VALUES (70,'MINT',1.0,'[\"WALLET\"]','',?,1)",
            ("7" * 64,),
        )
        seeded.commit()
    finally:
        seeded.close()

    horizon_replacement_detail = canonical_json({
        "forward_return_pct": 200.0,
        "horizon_s": 60.0,
        "price0": 1.0,
        "price0_observed_at": 2.0,
        "price_now": 3.0,
        "price_now_observed_at": 62.0,
        "terminal": None,
        "unavailable_reason": "",
    })
    recheck_id_payload = recheck_payload(
        decision_id=32, attempt=1, rechecked_at=21.0, report_id=3,
        prior_hash=prior_other, status="SUPPRESSED", reason="copycat_cluster",
        canonical_mint=None,
    )
    recheck_attempt_payload = recheck_payload(
        decision_id=30, attempt=1, rechecked_at=3.5, report_id=1,
        prior_hash=prior_cancel, status="SUPPRESSED", reason="copycat_cluster",
        canonical_mint=None,
    )
    replacement_cases = (
        (
            "decisions.id",
            "decisions",
            {
                "id": 30,
                "at": 99.0,
                "mint": "ALT",
                "segment": "CLIMBING",
                "action": "SKIP",
                "score": 0.0,
                "feature_vector_json": "{}",
                "safety_report_id": None,
                "config_hash": "replacement",
            },
        ),
        (
            "paper_trades.id",
            "paper_trades",
            {
                "id": 110,
                "at": 1.5,
                "mint": "LEGACY-REPLACEMENT",
                "segment": "CLIMBING",
                "side": "buy",
                "qty": 2.0,
                "quote_price": 1.0,
                "fill_price": 1.0,
                "fees_json": "{}",
                "realism_grade": "B",
            },
        ),
        (
            "outcomes.id",
            "outcomes",
            {
                "id": 120,
                "at": 2.0,
                "ref_kind": "trade",
                "ref_id": 110,
                "pnl_sol": 9.0,
                "detail_json": '{"replacement":true}',
                "p3_exit_trade_id": None,
            },
        ),
        (
            "outcomes.p3_exit_trade_id",
            "outcomes",
            {
                "id": 123,
                "at": 14.0,
                "ref_kind": "trade",
                "ref_id": 112,
                "pnl_sol": 1.0,
                "detail_json": exit_detail,
                "p3_exit_trade_id": 112,
            },
        ),
        (
            "outcomes.canonical_horizon",
            "outcomes",
            {
                "id": 124,
                "at": 62.0,
                "ref_kind": "canonical_observation",
                "ref_id": 40,
                "pnl_sol": 0.0,
                "detail_json": horizon_replacement_detail,
                "p3_exit_trade_id": None,
            },
        ),
        (
            "regime_log.id",
            "regime_log",
            {"id": 90, "at": 2.0, "state": "risk_off", "inputs_json": "{}"},
        ),
        (
            "wallet_pnl_events.id",
            "wallet_pnl_events",
            {
                "id": 100,
                "at": 2.0,
                "wallet": "OTHER_WALLET",
                "mint": "OTHER",
                "realized_pnl_sol": 2.0,
                "source": "replacement",
                "detail_json": "{}",
            },
        ),
        (
            "early_buyer_reads.id",
            "early_buyer_reads",
            {
                "id": 70,
                "mint": "OTHER",
                "checked_at": 19.0,
                "buyers_json": '["OTHER_WALLET"]',
                "unavailable_reason": "",
                "inputs_hash": "8" * 64,
                "safety_report_id": 3,
            },
        ),
        (
            "early_buyer_reads.safety_report_id",
            "early_buyer_reads",
            {
                "id": 71,
                "mint": "MINT",
                "checked_at": 1.0,
                "buyers_json": '["OTHER_WALLET"]',
                "unavailable_reason": "",
                "inputs_hash": "9" * 64,
                "safety_report_id": 1,
            },
        ),
        (
            "safety_reports.id",
            "safety_reports",
            {
                "id": 1,
                "mint": "MINT",
                "checked_at": 1.0,
                "hard_fails_json": "[]",
                "risk_score": 1.0,
                "inputs_hash": "a" * 64,
            },
        ),
        (
            "holder_evidence.id",
            "holder_evidence",
            {
                "id": 20,
                "safety_report_id": 3,
                "sampled_token_accounts": 2,
                "distinct_non_curve_owners": 2,
                "top10_non_curve_owner_share_pct": 20.0,
                "holder_observed_at": 18.0,
                "unavailable_reason": "",
                "inputs_hash": "a" * 64,
            },
        ),
        (
            "holder_evidence.safety_report_id",
            "holder_evidence",
            {
                "id": 22,
                "safety_report_id": 1,
                "sampled_token_accounts": 2,
                "distinct_non_curve_owners": 2,
                "top10_non_curve_owner_share_pct": 20.0,
                "holder_observed_at": 0.5,
                "unavailable_reason": "",
                "inputs_hash": "b" * 64,
            },
        ),
        (
            "creator_reputation_events.id",
            "creator_reputation_events",
            {
                "id": 80,
                "mint": "FILLED",
                "creator": "OTHER_CREATOR",
                "outcome": "GRADUATED",
                "observed_at": 10.5,
            },
        ),
        (
            "creator_reputation_events.mint_outcome",
            "creator_reputation_events",
            {
                "id": 81,
                "mint": "MINT",
                "creator": "CREATOR",
                "outcome": "GRADUATED",
                "observed_at": 1.5,
            },
        ),
        (
            "canonical_observations.id",
            "canonical_observations",
            {
                "id": 40,
                "decision_id": 33,
                "mint": "ALT",
                "observed_at": 30.0,
                "is_subject": 1,
                "is_canonical": 0,
                "eligible": 0,
                "start_price_sol": None,
                "price_observed_at": None,
                "price_source": "",
                "unavailable_reason": "start_price_missing",
            },
        ),
        (
            "canonical_observations.decision_mint",
            "canonical_observations",
            {
                "id": 43,
                "decision_id": 30,
                "mint": "MINT",
                "observed_at": 2.0,
                "is_subject": 1,
                "is_canonical": 1,
                "eligible": 1,
                "start_price_sol": 1.0,
                "price_observed_at": 2.0,
                "price_source": "curve_snapshot",
                "unavailable_reason": "",
            },
        ),
        (
            "canonical_generations.generation_hash",
            "canonical_generations",
            {
                "generation_hash": "3" * 64,
                "first_decision_id": 32,
                "created_at": 20.0,
            },
        ),
        (
            "canonical_generations.first_decision_id",
            "canonical_generations",
            {
                "generation_hash": "a" * 64,
                "first_decision_id": 30,
                "created_at": 2.5,
            },
        ),
        (
            "canonical_rechecks.id",
            "canonical_rechecks",
            {
                "id": 50,
                "decision_id": 32,
                "attempt": 1,
                "rechecked_at": 21.0,
                "causal_target_report_id": 3,
                "latest_target_report_id": 3,
                "status": "CANCEL",
                "reason": "copycat_cluster",
                "canonical_mint": None,
                "prior_inputs_hash": prior_other,
                "recheck_inputs_hash": "b" * 64,
                "payload_json": recheck_id_payload,
            },
        ),
        (
            "canonical_rechecks.decision_attempt",
            "canonical_rechecks",
            {
                "id": 52,
                "decision_id": 30,
                "attempt": 1,
                "rechecked_at": 3.5,
                "causal_target_report_id": 1,
                "latest_target_report_id": 1,
                "status": "CANCEL",
                "reason": "copycat_cluster",
                "canonical_mint": None,
                "prior_inputs_hash": prior_cancel,
                "recheck_inputs_hash": "c" * 64,
                "payload_json": recheck_attempt_payload,
            },
        ),
        (
            "paper_entry_executions.id",
            "paper_entry_executions",
            {
                "id": 60,
                "decision_id": 32,
                "at": 22.0,
                "status": "ABANDONED",
                "reason": "restart_before_fill",
                "planned_size_sol": 1.0,
                "canonical_recheck_id": None,
                "paper_trade_id": None,
            },
        ),
        (
            "paper_entry_executions.decision_id",
            "paper_entry_executions",
            {
                "id": 62,
                "decision_id": 30,
                "at": 4.0,
                "status": "CANCELLED",
                "reason": "copycat_cluster",
                "planned_size_sol": 1.0,
                "canonical_recheck_id": 50,
                "paper_trade_id": None,
            },
        ),
        (
            "paper_entry_executions.paper_trade_id",
            "paper_entry_executions",
            {
                "id": 63,
                "decision_id": 32,
                "at": 13.0,
                "status": "FILLED",
                "reason": "filled",
                "planned_size_sol": 1.0,
                "canonical_recheck_id": 51,
                "paper_trade_id": 111,
            },
        ),
    )
    assert tuple(label for label, _, _ in replacement_cases) == (
        "decisions.id",
        "paper_trades.id",
        "outcomes.id",
        "outcomes.p3_exit_trade_id",
        "outcomes.canonical_horizon",
        "regime_log.id",
        "wallet_pnl_events.id",
        "early_buyer_reads.id",
        "early_buyer_reads.safety_report_id",
        "safety_reports.id",
        "holder_evidence.id",
        "holder_evidence.safety_report_id",
        "creator_reputation_events.id",
        "creator_reputation_events.mint_outcome",
        "canonical_observations.id",
        "canonical_observations.decision_mint",
        "canonical_generations.generation_hash",
        "canonical_generations.first_decision_id",
        "canonical_rechecks.id",
        "canonical_rechecks.decision_attempt",
        "paper_entry_executions.id",
        "paper_entry_executions.decision_id",
        "paper_entry_executions.paper_trade_id",
    )

    independent = sqlite3.connect(path)
    independent.create_function(
        "p3_fee_sum", 1, store.p3_fee_sum_json, deterministic=True,
    )
    try:
        independent.execute("PRAGMA recursive_triggers=OFF")
        assert independent.execute("PRAGMA recursive_triggers").fetchone()[0] == 0

        def snapshots():
            return {
                table: tuple(independent.execute(
                    f"SELECT rowid,* FROM {table} ORDER BY rowid"
                ))
                for table in expected_tables
            }

        baseline = snapshots()
        baseline_counts = {
            table: len(rows) for table, rows in baseline.items()
        }
        assert all(count > 0 for count in baseline_counts.values())

        for label, table, values in replacement_cases:
            columns = tuple(values)
            placeholders = ",".join("?" for _ in columns)
            sql = (
                f"INSERT OR REPLACE INTO {table}({','.join(columns)}) "
                f"VALUES ({placeholders})"
            )
            before = snapshots()
            with pytest.raises(
                sqlite3.IntegrityError, match=r"^immutable evidence$",
            ):
                independent.execute(sql, tuple(values[column] for column in columns))
            after = snapshots()
            assert after == before == baseline, label
            assert {
                name: len(rows) for name, rows in after.items()
            } == baseline_counts, label
            independent.rollback()
    finally:
        independent.close()


def test_outcome_replace_guards_cover_both_partial_unique_identities(tmp_path):
    import json

    import memebot.store as store

    def canonical_json(value):
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )

    path = tmp_path / "v5-outcome-no-replace.db"
    seeded = open_db(path)
    try:
        seeded.execute(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES ('MINT',0.0,0.0)"
        )
        safety_report_id = seeded.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES ('MINT',1.0,'[]',0.0,?)",
            ("a" * 64,),
        ).lastrowid
        canonical_inputs_hash = "b" * 64
        feature_vector_json = canonical_json({
            "canonical": {
                "inputs_hash": canonical_inputs_hash,
                "planned_size_sol": 1.0,
                "ranking_inputs": {"counterfactual_horizons_s": [60.0]},
                "status": "CANONICAL",
            }
        })
        decision_id = seeded.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,safety_report_id,"
            "config_hash) VALUES (2.0,'MINT','CLIMBING','BUY',90.0,?,?,?)",
            (feature_vector_json, safety_report_id, "cfg"),
        ).lastrowid
        observation_id = seeded.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason) "
            "VALUES (?,'MINT',2.0,1,1,1,1.0,2.0,'curve_snapshot','')",
            (decision_id,),
        ).lastrowid
        recheck_payload = canonical_json({
            "attempt": 1,
            "causal_target_report_id": safety_report_id,
            "decision_id": decision_id,
            "latest_target_report_id": safety_report_id,
            "prior_inputs_hash": canonical_inputs_hash,
            "rechecked_at": 3.0,
            "verdict": {
                "canonical_mint": "MINT",
                "reason": "canonical_selected",
                "status": "CANONICAL",
            },
        })
        recheck_id = seeded.execute(
            "INSERT INTO canonical_rechecks("
            "decision_id,attempt,rechecked_at,causal_target_report_id,"
            "latest_target_report_id,status,reason,canonical_mint,"
            "prior_inputs_hash,recheck_inputs_hash,payload_json) "
            "VALUES (?,1,3.0,?,?,'PASS','canonical_selected','MINT',?,?,?)",
            (
                decision_id,
                safety_report_id,
                safety_report_id,
                canonical_inputs_hash,
                "c" * 64,
                recheck_payload,
            ),
        ).lastrowid
        buy_trade_id = seeded.execute(
            "INSERT INTO paper_trades("
            "decision_id,at,mint,segment,side,qty,quote_price,fill_price,"
            "fees_json,realism_grade,canonical_recheck_id,canonical_proof_hash) "
            "VALUES (?,4.0,'MINT','CLIMBING','buy',1.0,1.0,1.0,'{}','B',?,?)",
            (decision_id, recheck_id, "c" * 64),
        ).lastrowid
        execution_id = seeded.execute(
            "INSERT INTO paper_entry_executions("
            "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id,"
            "paper_trade_id) VALUES (?,4.0,'FILLED','filled',1.0,?,?)",
            (decision_id, recheck_id, buy_trade_id),
        ).lastrowid
        exit_trade_id = seeded.execute(
            "INSERT INTO paper_trades("
            "decision_id,at,mint,segment,side,qty,quote_price,fill_price,"
            "fees_json,realism_grade,p3_entry_execution_id) "
            "VALUES (?,5.0,'MINT','CLIMBING','sell',1.0,2.0,2.0,'{}','B',?)",
            (decision_id, execution_id),
        ).lastrowid
        exit_detail = canonical_json({
            "grade": "B", "hold_s": 1.0, "reason": "time_stop",
        })
        exit_outcome_id = seeded.execute(
            "INSERT INTO outcomes("
            "at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id) "
            "VALUES (5.0,'trade',?,1.0,?,?)",
            (exit_trade_id, exit_detail, exit_trade_id),
        ).lastrowid
        horizon_detail = canonical_json({
            "forward_return_pct": 100.0,
            "horizon_s": 60.0,
            "price0": 1.0,
            "price0_observed_at": 2.0,
            "price_now": 2.0,
            "price_now_observed_at": 62.0,
            "terminal": None,
            "unavailable_reason": "",
        })
        horizon_outcome_id = seeded.execute(
            "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
            "VALUES (62.0,'canonical_observation',?,0.0,?)",
            (observation_id, horizon_detail),
        ).lastrowid
        seeded.commit()
    finally:
        seeded.close()

    independent = sqlite3.connect(path)
    independent.create_function(
        "p3_fee_sum", 1, store.p3_fee_sum_json, deterministic=True,
    )
    try:
        independent.execute("PRAGMA recursive_triggers=OFF")
        assert independent.execute("PRAGMA recursive_triggers").fetchone()[0] == 0

        related_tables = (
            "decisions",
            "canonical_observations",
            "canonical_pending_current",
            "canonical_rechecks",
            "paper_trades",
            "paper_entry_executions",
            "p3_position_current",
            "outcomes",
        )

        def snapshot():
            return {
                table: tuple(independent.execute(
                    f"SELECT rowid,* FROM {table} ORDER BY rowid"
                ))
                for table in related_tables
            }

        baseline = snapshot()
        baseline_counts = {
            table: len(rows) for table, rows in baseline.items()
        }
        assert tuple(
            row[0] for row in independent.execute(
                "SELECT id FROM outcomes ORDER BY id"
            )
        ) == (exit_outcome_id, horizon_outcome_id)

        replacement_cases = (
            (
                "p3_exit_trade_id",
                (
                    100,
                    5.0,
                    "trade",
                    exit_trade_id,
                    1.0,
                    exit_detail,
                    exit_trade_id,
                ),
            ),
            (
                "canonical_observation_horizon",
                (
                    101,
                    62.0,
                    "canonical_observation",
                    observation_id,
                    0.0,
                    horizon_detail,
                    None,
                ),
            ),
        )
        assert tuple(label for label, _ in replacement_cases) == (
            "p3_exit_trade_id",
            "canonical_observation_horizon",
        )

        for label, values in replacement_cases:
            try:
                independent.execute(
                    "INSERT OR REPLACE INTO outcomes("
                    "id,at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id) "
                    "VALUES (?,?,?,?,?,?,?)",
                    values,
                )
            except sqlite3.IntegrityError as exc:
                assert str(exc) == "immutable evidence", label
            else:
                pytest.fail(f"{label}: REPLACE unexpectedly succeeded")
            after = snapshot()
            assert after == baseline, label
            assert {
                table: len(rows) for table, rows in after.items()
            } == baseline_counts, label
            independent.rollback()
    finally:
        independent.close()


def test_v5_explicit_trigger_ddl_manifest_has_exact_inventory(tmp_path):
    from hashlib import sha256

    import memebot.store as store

    expected = (
        ("early_buyer_report_guard", "early_buyer_reads", "afa6db61cb98b345d7cc46b1783997db2554957cc00acc67d12ff0580c992d43"),
        ("creator_reputation_creator_stable", "creator_reputation_events", "724c06ae69cfbf1a5253a68382d9186cada32e7bd686f7134dea1777a465533e"),
        ("creator_reputation_no_graduation_after_rug", "creator_reputation_events", "277d5cb2192dca52365515c658d6a8a3ec99d0d82f35072e689d7a49325585a6"),
        ("creator_reputation_rug_not_retrograde", "creator_reputation_events", "f7557a02ab08623593ff220d02ea1e488fdb8b1460ae06ee585e4293a6d2778b"),
        ("creator_reputation_current_after_insert", "creator_reputation_events", "bac3f9912d6221942357f7b3f3de87cfd8ce9174f50a6f1740bf9be28e4146ee"),
        ("p3_safety_report_shape_guard", "safety_reports", "0d399c18408f2528408377c78b2514ee26ef90997420fa750d5cd6d2a33f4959"),
        ("p3_wallet_pnl_shape_guard", "wallet_pnl_events", "cec61d1134f2e938a03498a43e1e8d14d5d48a9e12b91ec6443d112510df3197"),
        ("p3_wallet_pnl_summary_insert", "wallet_pnl_events", "eecc4656ee31ab2fc5cf817f6ad950039b7dd0f48d90d182a363241d7e44b380"),
        ("p3_early_buyer_shape_guard", "early_buyer_reads", "00b5799ad3a6c4ec9f710bb353565b5813a29da04ca0bb82bdd5491521264748"),
        ("p3_paper_trade_side_domain", "paper_trades", "d321156bde80926b71384e281bfab011a051b48831e4fcf6c27c5cff553faa2b"),
        ("p3_trade_shape_guard", "paper_trades", "bfb76be71f5564455ad647108d6370f6d923e6a0719f88c66947dcc03d39afb3"),
        ("p3_recheck_requires_valid_decision_link", "canonical_rechecks", "b22267a3965299c1717eb2392f4f804d8155bfe0d7e054273a98fef9804166e6"),
        ("p3_buy_requires_canonical_proof", "paper_trades", "560457799ed87a2d82068eb57463737387a6eda703fa5990cb84002323ba2c27"),
        ("p3_execution_requires_valid_terminal_link", "paper_entry_executions", "dc02b617f8dc3b50772d26ba6c2ed1cbc0211792186210319fdf06af91e76192"),
        ("p3_position_after_filled_entry", "paper_entry_executions", "565b2507d042f53fe0c90fda3a5c03575c7d64d47ba214fe4cf0ee91854c41d0"),
        ("p3_sell_requires_filled_entry", "paper_trades", "4e886aa084d5dee44d409857d216ad904303a4f81da449c1f431e586fcd98f22"),
        ("p3_position_after_sell", "paper_trades", "fac1a3c4d977c8dd3f9e8b45264310c403ae41e89e6f31f15e7f6b8d1f9b4215"),
        ("p3_canonical_pending_after_observation", "canonical_observations", "4ee5e11f198309bdecc9767d267a78879ca3f731799891d9dcb376f21eef7f1d"),
        ("p3_canonical_pending_after_outcome", "outcomes", "3046fbfc5b3b2acc9ab40d6543ff77f92a156b8bde61c725cb91b1188bfc18fa"),
        ("p3_outcome_shape_guard", "outcomes", "98a8892838ff77b893e4d2d2a51f35523b8ce7f6ec9458b1f94d514b088d6d3f"),
        ("p3_outcome_requires_exit_or_observation_chronology", "outcomes", "323ff7743aa49a10bd2a1e5f216db7eff580670a6229fcc26a84e1134fa62cdd"),
    )
    assert len(store.V5_EXPLICIT_TRIGGER_DDL) == 21
    assert tuple(
        (name, sql.splitlines()[1].split()[-1], sha256(sql.encode()).hexdigest())
        for name, sql in store.V5_EXPLICIT_TRIGGER_DDL
    ) == expected

    conn = _open_v4_fixture(tmp_path / "v4.db")
    try:
        store._apply_v5_additive_columns(conn)
        for _, sql in store.V5_TABLE_DDL:
            conn.execute(sql)
        for _, sql in store.V5_INDEX_DDL:
            conn.execute(sql)
        for _, sql in store.V5_EXPLICIT_TRIGGER_DDL:
            conn.execute(sql)

        expected_inventory = tuple((name, table) for name, table, _ in expected)
        names = tuple(name for name, _, _ in expected)
        inventory = tuple(
            (row["name"], row["tbl_name"])
            for row in conn.execute(
                "SELECT name,tbl_name FROM sqlite_schema WHERE type='trigger'"
                f" AND name IN ({','.join('?' for _ in names)}) ORDER BY rowid",
                names,
            )
        )
        assert inventory == expected_inventory
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_creator_reputation_direct_guards_reject_mismatch_retrograde_and_postrug_graduation(
    tmp_path,
):
    conn = open_db(tmp_path / "creator-reputation-direct-guards.db")
    try:
        conn.executemany(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES (?,?,?)",
            (
                ("MISMATCH", 0.0, 0.0),
                ("RETROGRADE", 0.0, 0.0),
                ("POSTRUG", 0.0, 0.0),
            ),
        )
        conn.executemany(
            "INSERT INTO creator_reputation_events(mint,creator,outcome,observed_at) "
            "VALUES (?,?,?,?)",
            (
                ("MISMATCH", "CREATOR_A", "GRADUATED", 1.0),
                ("RETROGRADE", "CREATOR_B", "GRADUATED", 2.0),
                ("POSTRUG", "CREATOR_C", "RUGGED", 3.0),
            ),
        )
        conn.commit()

        def snapshot():
            events = tuple(
                tuple(row)
                for row in conn.execute(
                    "SELECT id,mint,creator,outcome,observed_at "
                    "FROM creator_reputation_events ORDER BY id"
                )
            )
            current = tuple(
                tuple(row)
                for row in conn.execute(
                    "SELECT mint,creator,outcome,observed_at,event_id "
                    "FROM creator_reputation_current ORDER BY mint"
                )
            )
            return events, current

        baseline = snapshot()
        violations = (
            (
                ("MISMATCH", "OTHER_CREATOR", "RUGGED", 4.0),
                "creator reputation creator mismatch",
            ),
            (
                ("RETROGRADE", "CREATOR_B", "RUGGED", 2.0),
                "RUGGED reputation time does not follow prior evidence",
            ),
            (
                ("POSTRUG", "CREATOR_C", "GRADUATED", 4.0),
                "RUGGED reputation is terminal",
            ),
        )

        for values, expected_error in violations:
            with pytest.raises(sqlite3.IntegrityError) as exc_info:
                conn.execute(
                    "INSERT INTO creator_reputation_events("
                    "mint,creator,outcome,observed_at) VALUES (?,?,?,?)",
                    values,
                )
            assert str(exc_info.value) == expected_error
            conn.rollback()
            assert snapshot() == baseline

        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_creator_reputation_current_lookup_has_bounded_work_and_overflow(tmp_path):
    import memebot.store as store

    conn = open_db(tmp_path / "creator-reputation-current-lookup.db")
    try:
        mints = tuple(f"MINT_{index}" for index in range(7))
        conn.executemany(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES (?,?,?)",
            ((mint, 0.0, 0.0) for mint in mints),
        )
        conn.executemany(
            "INSERT INTO creator_reputation_events("
            "mint,creator,outcome,observed_at) VALUES (?,?,?,?)",
            (
                ("MINT_0", "CREATOR", "GRADUATED", 1.0),
                ("MINT_0", "CREATOR", "RUGGED", 2.0),
                ("MINT_1", "CREATOR", "GRADUATED", 3.0),
                ("MINT_2", "CREATOR", "RUGGED", 4.0),
                ("MINT_3", "CREATOR", "GRADUATED", 5.0),
                ("MINT_5", "OTHER_CREATOR", "GRADUATED", 1_000.0),
            ),
        )
        conn.commit()

        selected_ids = tuple(
            row[0]
            for row in conn.execute(
                "SELECT event_id FROM creator_reputation_current "
                "WHERE creator='CREATOR' AND mint<>'MINT_3' "
                "ORDER BY observed_at,event_id"
            )
        )
        trace: list[str] = []
        conn.set_trace_callback(trace.append)
        result = store.validated_creator_reputation_current(
            conn,
            creator="CREATOR",
            candidate_mint="MINT_3",
            as_of=10.0,
            max_creator_history_mints=3,
        )
        conn.set_trace_callback(None)

        assert result == store.CreatorReputationResult(
            prior_successes=1,
            prior_rugs=2,
            selected_event_ids=selected_ids,
            as_of=10.0,
            unavailable_reason="",
        )
        selects = tuple(sql for sql in trace if sql.lstrip().upper().startswith("SELECT"))
        assert len(selects) == 2
        assert all("creator_reputation_current" in sql for sql in selects)
        assert all("creator_reputation_events" not in sql for sql in selects)
        assert "LIMIT 1" in selects[0]
        assert "LIMIT 4" in selects[1]

        plans = (
            conn.execute(
                "EXPLAIN QUERY PLAN SELECT observed_at,event_id "
                "FROM creator_reputation_current WHERE creator=? "
                "ORDER BY observed_at DESC,event_id DESC LIMIT 1",
                ("CREATOR",),
            ).fetchall(),
            conn.execute(
                "EXPLAIN QUERY PLAN SELECT event_id,mint,creator,outcome,observed_at "
                "FROM creator_reputation_current "
                "WHERE creator=? AND mint<>? ORDER BY observed_at,event_id LIMIT ?",
                ("CREATOR", "MINT_3", 4),
            ).fetchall(),
        )
        assert all(
            any("creator_reputation_current_bounded_idx" in row[3] for row in plan)
            for plan in plans
        )
        assert all(
            not any("SCAN creator_reputation_events" in row[3] for row in plan)
            for plan in plans
        )

        overflow = store.validated_creator_reputation_current(
            conn,
            creator="CREATOR",
            candidate_mint="MINT_3",
            as_of=10.0,
            max_creator_history_mints=2,
        )
        assert overflow == store.CreatorReputationResult(
            prior_successes=0,
            prior_rugs=0,
            selected_event_ids=(),
            as_of=10.0,
            unavailable_reason="creator_history_overflow",
        )

        conn.execute(
            "INSERT INTO creator_reputation_events("
            "mint,creator,outcome,observed_at) VALUES (?,?,?,?)",
            ("MINT_4", "CREATOR", "GRADUATED", 10.0),
        )
        conn.commit()
        trace.clear()
        conn.set_trace_callback(trace.append)
        unavailable = store.validated_creator_reputation_current(
            conn,
            creator="CREATOR",
            candidate_mint="MINT_3",
            as_of=10.0,
            max_creator_history_mints=3,
        )
        conn.set_trace_callback(None)
        assert unavailable == store.CreatorReputationResult(
            prior_successes=0,
            prior_rugs=0,
            selected_event_ids=(),
            as_of=10.0,
            unavailable_reason="creator_reputation_unavailable",
        )
        selects = tuple(sql for sql in trace if sql.lstrip().upper().startswith("SELECT"))
        assert len(selects) == 1
        assert "creator_reputation_current" in selects[0]
        assert "creator_reputation_events" not in selects[0]
    finally:
        conn.set_trace_callback(None)
        conn.close()


def test_v5_immutable_trigger_builder_covers_existing_and_new_tables(tmp_path):
    import memebot.store as store

    expected_new_keys = {
        "safety_reports": (("id",),),
        "holder_evidence": (("id",), ("safety_report_id",)),
        "creator_reputation_events": (("id",), ("mint", "outcome")),
        "canonical_observations": (("id",), ("decision_id", "mint")),
        "canonical_generations": (("generation_hash",), ("first_decision_id",)),
        "canonical_rechecks": (("id",), ("decision_id", "attempt")),
        "paper_entry_executions": (("id",), ("decision_id",), ("paper_trade_id",)),
    }
    expected_existing_where = {
        "decisions": "old.id IS NEW.id",
        "paper_trades": "old.id IS NEW.id",
        "outcomes": (
            "old.id IS NEW.id"
            " OR (NEW.p3_exit_trade_id IS NOT NULL"
            " AND old.p3_exit_trade_id IS NEW.p3_exit_trade_id)"
            " OR (NEW.ref_kind='canonical_observation'"
            " AND old.ref_kind='canonical_observation'"
            " AND old.ref_id IS NEW.ref_id"
            " AND json_extract(old.detail_json,'$.horizon_s')"
            " IS json_extract(NEW.detail_json,'$.horizon_s'))"
        ),
        "regime_log": "old.id IS NEW.id",
        "wallet_pnl_events": "old.id IS NEW.id",
        "early_buyer_reads": (
            "old.id IS NEW.id"
            " OR (NEW.safety_report_id IS NOT NULL"
            " AND old.safety_report_id IS NEW.safety_report_id)"
        ),
    }

    def immutable_insert(table, duplicate):
        return (
            f"CREATE TRIGGER IF NOT EXISTS {table}_no_replace BEFORE INSERT ON {table} "
            f"WHEN EXISTS(SELECT 1 FROM {table} AS old WHERE {duplicate}) "
            "BEGIN SELECT RAISE(ABORT,'immutable evidence'); END"
        )

    expected_sql = [
        immutable_insert(table, duplicate)
        for table, duplicate in expected_existing_where.items()
    ]
    for table, keys in expected_new_keys.items():
        duplicate = " OR ".join(
            "(({}) AND ({}))".format(
                " AND ".join(f"NEW.{column} IS NOT NULL" for column in key),
                " AND ".join(f"old.{column} IS NEW.{column}" for column in key),
            )
            for key in keys
        )
        expected_sql.extend(
            (
                immutable_insert(table, duplicate),
                f"CREATE TRIGGER IF NOT EXISTS {table}_append_only_update BEFORE UPDATE ON {table} "
                "BEGIN SELECT RAISE(ABORT,'immutable evidence'); END",
                f"CREATE TRIGGER IF NOT EXISTS {table}_append_only_delete BEFORE DELETE ON {table} "
                "BEGIN SELECT RAISE(ABORT,'immutable evidence'); END",
            )
        )
    expected_sql = tuple(expected_sql)
    expected_names = tuple(sql.split()[5] for sql in expected_sql)
    expected_tables = (
        *expected_existing_where,
        *(table for table in expected_new_keys for _ in range(3)),
    )

    assert store.SCHEMA_V5_NEW_IMMUTABLE_KEYS == expected_new_keys
    assert store.SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE == expected_existing_where
    assert store.SCHEMA_V5_NEW_IMMUTABLE_TABLES == tuple(expected_new_keys)
    assert store.SCHEMA_V5_IMMUTABLE_TABLES == (
        *expected_existing_where,
        *expected_new_keys,
    )
    assert len(expected_sql) == 27
    assert store._v5_immutable_triggers() == expected_sql
    assert expected_names == (
        "decisions_no_replace",
        "paper_trades_no_replace",
        "outcomes_no_replace",
        "regime_log_no_replace",
        "wallet_pnl_events_no_replace",
        "early_buyer_reads_no_replace",
        "safety_reports_no_replace",
        "safety_reports_append_only_update",
        "safety_reports_append_only_delete",
        "holder_evidence_no_replace",
        "holder_evidence_append_only_update",
        "holder_evidence_append_only_delete",
        "creator_reputation_events_no_replace",
        "creator_reputation_events_append_only_update",
        "creator_reputation_events_append_only_delete",
        "canonical_observations_no_replace",
        "canonical_observations_append_only_update",
        "canonical_observations_append_only_delete",
        "canonical_generations_no_replace",
        "canonical_generations_append_only_update",
        "canonical_generations_append_only_delete",
        "canonical_rechecks_no_replace",
        "canonical_rechecks_append_only_update",
        "canonical_rechecks_append_only_delete",
        "paper_entry_executions_no_replace",
        "paper_entry_executions_append_only_update",
        "paper_entry_executions_append_only_delete",
    )

    conn = _open_v4_fixture(tmp_path / "v4.db")
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        store._apply_v5_additive_columns(conn)
        for _, sql in store.V5_TABLE_DDL:
            conn.execute(sql)
        for _, sql in store.V5_INDEX_DDL:
            conn.execute(sql)
        for _, sql in store.V5_EXPLICIT_TRIGGER_DDL:
            conn.execute(sql)
        before = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_schema WHERE type='trigger'")
        }
        for sql in store._v5_immutable_triggers():
            conn.execute(sql)

        inventory = tuple(
            (row["name"], row["tbl_name"], row["sql"])
            for row in conn.execute(
                "SELECT name,tbl_name,sql FROM sqlite_schema WHERE type='trigger'"
                f" AND name IN ({','.join('?' for _ in expected_names)}) ORDER BY rowid",
                expected_names,
            )
        )
        stored_sql = tuple(
            sql.replace("CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER ", 1)
            for sql in expected_sql
        )
        assert inventory == tuple(
            zip(expected_names, expected_tables, stored_sql, strict=True)
        )
        after = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_schema WHERE type='trigger'")
        }
        assert after - before == set(expected_names)

        conn.execute(
            "INSERT INTO decisions(id,at,mint,segment,action,score,feature_vector_json,config_hash)"
            " VALUES (1,1,'M','climbing','SKIP',0,'{}','h')"
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable evidence"):
            conn.execute(
                "INSERT OR REPLACE INTO decisions"
                "(id,at,mint,segment,action,score,feature_vector_json,config_hash)"
                " VALUES (1,2,'M2','climbing','SKIP',0,'{}','h2')"
            )

        conn.execute(
            "INSERT INTO canonical_generations(generation_hash,first_decision_id,created_at)"
            " VALUES (?,1,1)",
            ("a" * 64,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable evidence"):
            conn.execute(
                "INSERT OR REPLACE INTO canonical_generations"
                "(generation_hash,first_decision_id,created_at) VALUES (?,1,2)",
                ("a" * 64,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable evidence"):
            conn.execute("UPDATE canonical_generations SET created_at=2")
        with pytest.raises(sqlite3.IntegrityError, match="immutable evidence"):
            conn.execute("DELETE FROM canonical_generations")

        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_v5_schema_manifest_attestor_unit_contract(tmp_path):
    import memebot.store as store

    assert store._sqlite_stored_table_name(
        'CREATE /* stored */ TABLE main."Quoted Table"(x INTEGER)'
    ) == "quoted table"
    assert store._sqlite_stored_table_name(
        "CREATE VIRTUAL TABLE [Quoted FTS] USING fts5(x)"
    ) == "quoted fts"
    assert store._sqlite_fts_external_content(
        "CREATE VIRTUAL TABLE stealth_fts3 USING fts3("
        "id,content='canonical_observations')"
    ) is None
    assert store._sqlite_fts_external_content(
        "CREATE VIRTUAL TABLE stealth_fts4 USING fts4("
        "id,content='canonical_observations')"
    ) == ("canonical_observations", None, ("id",))
    assert store._sqlite_fts_external_content(
        "CREATE VIRTUAL TABLE quoted_content_fts5 USING fts5("
        "mint,content='paper_trades',content_rowid='id')"
    ) == ("paper_trades", "id", ("mint",))
    manifest = store._build_v5_schema_manifest()
    assert manifest == store.V5_SCHEMA_MANIFEST
    assert len(manifest) == 11 + 11 + 48
    assert tuple(row[0] for row in manifest) == (
        *("table" for _ in range(11)),
        *("index" for _ in range(11)),
        *("trigger" for _ in range(48)),
    )
    assert len({row[1] for row in manifest}) == len(manifest)
    assert tuple(row[1] for row in manifest[:11]) == tuple(
        name for name, _ in store.V5_TABLE_DDL
    )
    assert tuple(row[1] for row in manifest[11:22]) == tuple(
        name for name, _ in store.V5_INDEX_DDL
    )
    assert tuple(row[1] for row in manifest[22:43]) == tuple(
        name for name, _ in store.V5_EXPLICIT_TRIGGER_DDL
    )
    assert tuple(row[1] for row in manifest[43:]) == tuple(
        sql.split()[5] for sql in store._v5_immutable_triggers()
    )
    assert all("IF NOT EXISTS" not in row[3] for row in manifest)
    assert all(not row[3].rstrip().endswith(";") for row in manifest)
    autoindexes = store._build_v5_autoindex_manifest()
    assert autoindexes == store._V5_SCHEMA_AUTOINDEX_MANIFEST
    assert autoindexes == (
        ("index", "sqlite_autoindex_holder_evidence_1", "holder_evidence", None),
        (
            "index", "sqlite_autoindex_creator_reputation_events_1",
            "creator_reputation_events", None,
        ),
        (
            "index", "sqlite_autoindex_creator_reputation_current_1",
            "creator_reputation_current", None,
        ),
        (
            "index", "sqlite_autoindex_creator_reputation_current_2",
            "creator_reputation_current", None,
        ),
        (
            "index", "sqlite_autoindex_wallet_pnl_summary_1",
            "wallet_pnl_summary", None,
        ),
        (
            "index", "sqlite_autoindex_wallet_pnl_summary_2",
            "wallet_pnl_summary", None,
        ),
        (
            "index", "sqlite_autoindex_canonical_observations_1",
            "canonical_observations", None,
        ),
        (
            "index", "sqlite_autoindex_canonical_generations_1",
            "canonical_generations", None,
        ),
        (
            "index", "sqlite_autoindex_canonical_generations_2",
            "canonical_generations", None,
        ),
        (
            "index", "sqlite_autoindex_canonical_rechecks_1",
            "canonical_rechecks", None,
        ),
        (
            "index", "sqlite_autoindex_paper_entry_executions_1",
            "paper_entry_executions", None,
        ),
        (
            "index", "sqlite_autoindex_paper_entry_executions_2",
            "paper_entry_executions", None,
        ),
        (
            "index", "sqlite_autoindex_p3_position_current_1",
            "p3_position_current", None,
        ),
    )

    seed_path = tmp_path / "manifest-v4-seed.db"
    seed_conn = _open_v4_fixture(seed_path)
    assert seed_conn.execute("PRAGMA user_version").fetchone()[0] == 4
    assert tuple(seed_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()) == (
        0, 0, 0,
    )
    seed_conn.close()
    assert not (tmp_path / f"{seed_path.name}-wal").exists()
    assert not (tmp_path / f"{seed_path.name}-shm").exists()
    seed_bytes = seed_path.read_bytes()

    counter = 0

    def make_v4(*, file_backed=False):
        nonlocal counter
        if not file_backed:
            return _open_v4_fixture(":memory:"), None
        counter += 1
        path = tmp_path / f"manifest-{counter}.db"
        path.write_bytes(seed_bytes)
        return _open_v4_fixture(path), path

    def install(conn):
        conn.execute("BEGIN")
        for _, sql in store.V5_TABLE_DDL:
            conn.execute(sql)
        store._apply_v5_additive_columns(conn)
        for _, sql in store.V5_INDEX_DDL:
            conn.execute(sql)
        for _, sql in store.V5_EXPLICIT_TRIGGER_DDL:
            conn.execute(sql)
        for sql in store._v5_immutable_triggers():
            conn.execute(sql)
        conn.commit()

    conn, path = make_v4(file_backed=True)
    assert path is not None
    conn.execute("BEGIN")
    conn.execute("CREATE TABLE user_notes (id INTEGER PRIMARY KEY, note TEXT)")
    conn.execute('CREATE TABLE "legacy quoted table"(note TEXT)')
    conn.execute("CREATE INDEX user_notes_note_idx ON user_notes(note)")
    conn.execute(
        "CREATE VIRTUAL TABLE legacy_notes_fts USING fts5("
        "note,content='user_notes',content_rowid='id')"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE legacy_term_fts USING fts5("
        "canonical_proof_hash)"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE stealth_fts3 USING fts3("
        "id,content='canonical_observations')"
    )
    assert tuple(
        row[1] for row in conn.execute("PRAGMA table_info(stealth_fts3)")
    ) == ("id", "content")
    conn.execute(
        "CREATE TABLE legacy_fts4_source("
        "id INTEGER PRIMARY KEY,note TEXT)"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE legacy_external_fts4 USING fts4("
        "note,content='legacy_fts4_source')"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE legacy_contentless_fts4 USING fts4("
        "note,content='')"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE legacy_standalone_fts4 USING fts4("
        "canonical_proof_hash)"
    )
    conn.execute(
        "CREATE TABLE legacy_same_rowid_source("
        "canonical_recheck_id INTEGER PRIMARY KEY,note TEXT)"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE legacy_same_rowid_fts USING fts5("
        "note,content='legacy_same_rowid_source',"
        "content_rowid='canonical_recheck_id')"
    )
    conn.execute("CREATE TABLE legacy_lookup(k TEXT UNIQUE)")
    conn.execute("CREATE INDEX legacy_lookup_k_idx ON legacy_lookup(k)")
    conn.execute(
        "CREATE VIEW legacy_lookup_indexed_view AS "
        "SELECT k FROM legacy_lookup INDEXED BY legacy_lookup_k_idx"
    )
    conn.execute(
        "CREATE VIEW legacy_lookup_autoindexed_view AS "
        "SELECT k FROM legacy_lookup "
        "INDEXED BY sqlite_autoindex_legacy_lookup_1"
    )
    conn.execute(
        "CREATE VIEW legacy_lookup_not_indexed_view AS "
        "SELECT k FROM legacy_lookup NOT INDEXED"
    )
    conn.execute("CREATE TABLE legacy_parent(k TEXT PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE legacy_child("
        "k TEXT REFERENCES legacy_parent(k))"
    )
    conn.execute(
        "CREATE TRIGGER user_notes_guard BEFORE DELETE ON user_notes "
        "BEGIN SELECT RAISE(ABORT,'notes immutable'); END"
    )
    conn.execute(
        "CREATE INDEX arbitrary_legacy_mint_idx "
        "ON paper_trades(mint /* canonical_proof_hash */)"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_legacy_mint_guard "
        "BEFORE INSERT ON paper_trades "
        "WHEN NEW.mint='canonical_proof_hash' /* canonical_proof_hash */ "
        "BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TABLE other_table("
        "canonical_proof_hash TEXT, canonical_proof_hash_suffix TEXT)"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_similar_identifier_guard "
        "BEFORE INSERT ON paper_trades "
        "WHEN (SELECT canonical_proof_hash_suffix FROM other_table LIMIT 1) "
        "IS NULL BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_cross_table_column_guard "
        "BEFORE INSERT ON paper_trades "
        "WHEN (SELECT canonical_proof_hash FROM other_table LIMIT 1) IS NOT NULL "
        "BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TRIGGER canonical_proof_hash BEFORE INSERT ON paper_trades "
        "WHEN NEW.mint<>'' BEGIN SELECT 1; END"
    )
    conn.create_function("caller_only", 1, lambda _value: 0)
    conn.execute(
        "CREATE TRIGGER arbitrary_caller_udf_guard "
        "BEFORE INSERT ON paper_trades "
        "WHEN caller_only(NEW.mint)=1 BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_legacy_replace_guard "
        "AFTER INSERT ON tokens WHEN NEW.mint='never' BEGIN "
        "REPLACE INTO tokens(mint,created_at,last_seen) "
        "VALUES('shadow',1,1); END"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_global_legacy_read_guard "
        "AFTER INSERT ON decisions WHEN NEW.mint='never' BEGIN "
        "SELECT mint FROM paper_trades LIMIT 1; END"
    )
    conn.execute(
        "CREATE VIEW arbitrary_global_legacy_view AS "
        "SELECT mint FROM paper_trades"
    )
    for storage in ("VIRTUAL", "STORED"):
        suffix = storage.lower()
        conn.execute(
            f"CREATE TABLE legacy_generated_{suffix}("
            "a INTEGER,b INTEGER GENERATED ALWAYS AS (a+1) "
            f"{storage})"
        )
        conn.execute(
            f"CREATE VIEW legacy_generated_{suffix}_view AS "
            f"SELECT b FROM legacy_generated_{suffix}"
        )
        conn.execute(
            f"CREATE TRIGGER legacy_generated_{suffix}_guard "
            f"BEFORE INSERT ON legacy_generated_{suffix} "
            "WHEN NEW.b IS NOT NULL BEGIN SELECT 1; END"
        )
        conn.execute(
            f"CREATE INDEX legacy_generated_{suffix}_b_idx "
            f"ON legacy_generated_{suffix}(b)"
        )
    conn.execute(
        "CREATE TRIGGER arbitrary_global_legacy_insert_guard "
        "AFTER INSERT ON decisions WHEN NEW.mint='never' BEGIN "
        "INSERT INTO paper_trades("
        "at,mint,segment,side,qty,quote_price,fill_price,fees_json,realism_grade) "
        "VALUES(1,'shadow','climbing','buy',1,1,1,'{}','B'); END"
    )
    conn.execute("CREATE TABLE legacy_sink(x)")
    conn.execute("INSERT INTO legacy_sink VALUES ('legacy')")
    conn.execute("CREATE VIEW legacy_sink_view AS SELECT x FROM legacy_sink")
    conn.execute(
        "CREATE VIEW arbitrary_legacy_cte_view AS "
        "WITH hidden AS (SELECT x FROM legacy_sink) SELECT * FROM hidden"
    )
    conn.execute(
        "CREATE VIEW arbitrary_legacy_nested_view AS "
        "SELECT x FROM arbitrary_legacy_cte_view"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_update AFTER UPDATE OF x ON legacy_sink "
        "BEGIN SELECT x FROM legacy_sink; END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_lookup_upsert "
        "AFTER INSERT ON legacy_sink BEGIN "
        "INSERT INTO legacy_lookup(k) VALUES(NEW.x) "
        "ON CONFLICT(k) DO UPDATE SET k=excluded.k; END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_rowid_update "
        "AFTER UPDATE OF rowid ON legacy_sink "
        "BEGIN SELECT x FROM legacy_sink; END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_underscore_rowid_update "
        "AFTER UPDATE OF _rowid_ ON legacy_sink BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_oid_update "
        "AFTER UPDATE OF oid ON legacy_sink BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_quoted_update "
        "AFTER UPDATE OF 'x' ON legacy_sink "
        "BEGIN SELECT x FROM legacy_sink; END"
    )
    conn.execute("CREATE TABLE legacy_shadow_sink(rowid,x)")
    conn.execute(
        "CREATE TRIGGER legacy_shadow_rowid_update "
        "AFTER UPDATE OF rowid ON legacy_shadow_sink BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_view_insert "
        "INSTEAD OF INSERT ON legacy_sink_view BEGIN "
        "INSERT INTO legacy_sink(x) VALUES(NEW.x); END"
    )
    conn.execute(
        "CREATE TRIGGER legacy_sink_view_quoted_insert "
        "INSTEAD OF INSERT ON legacy_sink_view BEGIN "
        "INSERT INTO legacy_sink('x') VALUES(NEW.x); END"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_legacy_nested_cte_trigger "
        "AFTER INSERT ON legacy_sink BEGIN "
        "SELECT * FROM (WITH hidden AS (SELECT x FROM legacy_sink) "
        "SELECT * FROM hidden); END"
    )
    conn.execute(
        "CREATE TRIGGER arbitrary_legacy_nested_view_trigger "
        "AFTER INSERT ON legacy_sink BEGIN "
        "SELECT x FROM arbitrary_legacy_nested_view; END"
    )

    class CallerWindow:
        def step(self, _value):
            pass

        def value(self):
            return 0

        def inverse(self, _value):
            pass

        def finalize(self):
            return 0

    conn.create_window_function("caller_window", 1, CallerWindow)
    conn.execute(
        "CREATE VIEW arbitrary_legacy_window_view AS "
        "SELECT caller_window(x) OVER () AS value FROM legacy_sink"
    )

    class MixedWindow:
        def __init__(self):
            self.count = 0

        def step(self, _left, _right):
            self.count += 1

        def value(self):
            return self.count

        def inverse(self, _left, _right):
            self.count -= 1

        def finalize(self):
            return self.count

    conn.create_function("caller_mixed", 1, lambda value: value)
    conn.create_window_function("caller_mixed", 2, MixedWindow)
    conn.execute(
        "CREATE VIEW arbitrary_legacy_mixed_function_view AS "
        "SELECT x,caller_mixed(x,x) OVER () AS window_value FROM legacy_sink "
        "WHERE caller_mixed(x)='legacy'"
    )
    assert [
        tuple(row) for row in conn.execute(
            "SELECT x,window_value "
            "FROM arbitrary_legacy_mixed_function_view"
        )
    ] == [("legacy", 1)]
    conn.create_collation(
        "caller_cmp", lambda left, right: (left > right) - (left < right),
    )
    conn.execute(
        "CREATE TABLE legacy_custom_unique("
        "k TEXT COLLATE caller_cmp UNIQUE)"
    )
    conn.execute(
        "CREATE TRIGGER legacy_custom_unique_upsert "
        "AFTER INSERT ON legacy_sink BEGIN "
        "INSERT INTO legacy_custom_unique(k) VALUES(NEW.x) "
        "ON CONFLICT(k) DO UPDATE SET k=excluded.k; END"
    )
    conn.execute(
        "CREATE INDEX arbitrary_legacy_collation_idx "
        "ON paper_trades(mint COLLATE caller_cmp)"
    )
    conn.execute(
        "CREATE INDEX user_notes_custom_idx "
        "ON user_notes(note COLLATE caller_cmp)"
    )
    conn.create_function(
        "abs", 2, lambda left, right: left if left == right else None,
        deterministic=True,
    )
    conn.create_function(
        "caller_generated", 1, lambda value: value, deterministic=True,
    )
    conn.execute(
        "CREATE TABLE legacy_generated_udf("
        "a TEXT,b TEXT GENERATED ALWAYS AS (caller_generated(a)) STORED)"
    )
    conn.execute(
        "CREATE VIEW legacy_generated_udf_view AS "
        "SELECT b FROM legacy_generated_udf"
    )
    conn.execute(
        "CREATE INDEX legacy_generated_udf_idx ON legacy_generated_udf(b)"
    )
    conn.execute(
        "CREATE TRIGGER legacy_generated_udf_trigger "
        "BEFORE INSERT ON legacy_generated_udf "
        "WHEN NEW.b IS NOT NULL BEGIN SELECT 1; END"
    )
    conn.execute(
        "CREATE VIEW arbitrary_legacy_builtin_overload_view AS "
        "SELECT abs(x,x) AS value FROM legacy_sink"
    )
    assert [
        tuple(row) for row in conn.execute(
            "SELECT value FROM arbitrary_legacy_builtin_overload_view"
        )
    ] == [("legacy",)]
    conn.commit()
    install(conn)
    conn.execute(
        "ALTER TABLE paper_trades ADD COLUMN legacy_shadow TEXT "
        "GENERATED ALWAYS AS (mint) VIRTUAL"
    )
    conn.execute(
        "ALTER TABLE paper_trades ADD COLUMN legacy_parent_key TEXT "
        "REFERENCES legacy_parent(k) CHECK(legacy_parent_key IS NULL)"
    )
    conn.execute(
        "CREATE VIEW paper_trades_legacy_shadow_view AS "
        "SELECT legacy_shadow FROM paper_trades"
    )
    conn.execute(
        "CREATE INDEX paper_trades_legacy_shadow_idx "
        "ON paper_trades(legacy_shadow)"
    )
    conn.execute(
        "CREATE TRIGGER paper_trades_legacy_shadow_trigger "
        "AFTER INSERT ON tokens BEGIN "
        "SELECT legacy_shadow FROM paper_trades LIMIT 1; END"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE paper_trades_legacy_mint_fts USING fts5("
        '/* legacy field */ "mint" UNINDEXED,'
        "content='paper_trades',content_rowid='id')"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE paper_trades_legacy_shadow_fts USING fts5("
        "legacy_shadow,content='paper_trades',content_rowid='id')"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE paper_trades_legacy_mint_fts4 USING fts4("
        "mint,content='paper_trades')"
    )
    conn.commit()
    before_serialized = conn.serialize()
    before_file = path.read_bytes()
    before_version = conn.execute("PRAGMA user_version").fetchone()[0]
    before_changes = conn.total_changes
    before_transaction = conn.in_transaction
    trace = []
    conn.set_trace_callback(trace.append)
    assert store._attest_v5_schema_manifest(conn) is None
    conn.set_trace_callback(None)
    assert conn.serialize() == before_serialized
    assert path.read_bytes() == before_file
    assert conn.execute("PRAGMA user_version").fetchone()[0] == before_version == 4
    assert conn.total_changes == before_changes
    assert conn.in_transaction is before_transaction is False
    assert trace and all(
        statement.lstrip().upper().startswith(("SELECT", "PRAGMA"))
        for statement in trace
    )
    conn.execute("BEGIN")
    active_serialized = conn.serialize()
    assert store._attest_v5_schema_manifest(conn) is None
    assert conn.in_transaction
    assert conn.serialize() == active_serialized
    conn.rollback()
    conn.close()

    class NoSerializeConnection(sqlite3.Connection):
        def serialize(self, *args, **kwargs):
            raise AssertionError("schema attestation must not serialize row payload")

    resource_path = tmp_path / "manifest-resource.db"
    resource_seed = _open_v4_fixture(resource_path)
    resource_seed.close()
    resource_conn = sqlite3.connect(
        resource_path, factory=NoSerializeConnection,
    )
    resource_conn.execute("CREATE TABLE legacy_payload(data BLOB)")
    resource_conn.execute(
        "INSERT INTO legacy_payload VALUES(zeroblob(8 * 1024 * 1024))"
    )
    resource_conn.commit()
    install(resource_conn)
    assert store._attest_v5_schema_manifest(resource_conn) is None
    resource_conn.close()

    def assert_rejected(mutate, match):
        candidate, _ = make_v4()
        install(candidate)
        mutate(candidate)
        candidate.commit()
        with pytest.raises(ValueError, match=match):
            store._attest_v5_schema_manifest(candidate)
        candidate.close()

    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIEW arbitrary_v5_index_hint_view AS "
            "SELECT mint FROM safety_reports "
            'InDeXeD BY "safety_reports_mint_latest_idx"'
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_v5_index_hint_trigger "
            "AFTER INSERT ON tokens BEGIN "
            "SELECT at FROM wallet_pnl_events "
            "INDEXED BY [wallet_pnl_events_at_idx] LIMIT 1; END"
        ),
        "extra v5-owned schema object",
    )

    for storage in ("VIRTUAL", "STORED"):
        for object_type in ("view", "index", "trigger"):
            def add_transitive_generated_dependency(
                candidate, object_type=object_type, storage=storage,
            ):
                candidate.execute(
                    "ALTER TABLE paper_trades ADD COLUMN proof_shadow TEXT "
                    "GENERATED ALWAYS AS (canonical_proof_hash) "
                    f"{storage}"
                )
                if object_type == "view":
                    candidate.execute(
                        "CREATE VIEW arbitrary_transitive_generated_view AS "
                        "SELECT proof_shadow FROM paper_trades"
                    )
                elif object_type == "index":
                    candidate.execute(
                        "CREATE INDEX arbitrary_transitive_generated_idx "
                        "ON paper_trades(proof_shadow)"
                    )
                else:
                    candidate.execute(
                        "CREATE TRIGGER arbitrary_transitive_generated_trigger "
                        "AFTER INSERT ON tokens BEGIN "
                        "SELECT proof_shadow FROM paper_trades LIMIT 1; END"
                    )

            assert_rejected(
                add_transitive_generated_dependency,
                "extra v5-owned schema object",
            )

    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_additive_fts USING fts5("
            "mint,content='paper_trades',"
            "content_rowid='canonical_recheck_id')"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIRTUAL TABLE stealth_fts4 USING fts4("
            "id,content='canonical_observations')"
        ),
        "extra v5-owned schema object",
    )
    for qualified_fts_module in ("fts4", "fts5"):
        assert_rejected(
            lambda candidate, module=qualified_fts_module: candidate.execute(
                f"CREATE VIRTUAL TABLE malformed_qualified_{module} "
                f"USING {module}(canonical_proof_hash,"
                "content='main.paper_trades')"
            ),
            "schema object",
        )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_additive_fts4 USING fts4("
            "canonical_proof_hash,content='paper_trades')"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_additive_language_fts4 "
            "USING fts4(mint,content='paper_trades',"
            "languageid='canonical_recheck_id')"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_new_table_fts USING fts5("
            "group_key,content='canonical_observations',content_rowid='id')"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_additive_field_fts USING fts5("
            '/* exact field */ "canonical_proof_hash" UNINDEXED,'
            "content='paper_trades',content_rowid='id')"
        ),
        "extra v5-owned schema object",
    )

    def add_transitive_generated_fts(candidate):
        candidate.execute(
            "ALTER TABLE paper_trades ADD COLUMN proof_shadow TEXT "
            "GENERATED ALWAYS AS (canonical_proof_hash) STORED"
        )
        candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_transitive_field_fts USING fts5("
            "proof_shadow,content='paper_trades',content_rowid='id')"
        )

    assert_rejected(
        add_transitive_generated_fts, "extra v5-owned schema object",
    )

    def add_transitive_generated_fts4(candidate):
        candidate.execute(
            "ALTER TABLE paper_trades ADD COLUMN proof_shadow TEXT "
            "GENERATED ALWAYS AS (canonical_proof_hash) STORED"
        )
        candidate.execute(
            "CREATE VIRTUAL TABLE arbitrary_transitive_field_fts4 "
            "USING fts4(proof_shadow,content='paper_trades')"
        )

    assert_rejected(
        add_transitive_generated_fts4, "extra v5-owned schema object",
    )

    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TABLE unrelated_name_new_table_fk("
            "observation_id INTEGER REFERENCES canonical_observations(id))"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TABLE unrelated_name_additive_fk("
            "proof_hash TEXT REFERENCES paper_trades(canonical_proof_hash))"
        ),
        "extra v5-owned schema object",
    )

    def wrong_table(conn):
        conn.execute("DROP TABLE holder_evidence")
        conn.execute("CREATE TABLE holder_evidence (wrong TEXT)")

    def wrong_index(conn):
        conn.execute("DROP INDEX outcomes_p3_exit_trade_unique_idx")
        conn.execute(
            "CREATE UNIQUE INDEX outcomes_p3_exit_trade_unique_idx "
            "ON outcomes(p3_exit_trade_id)"
        )

    def wrong_partial_index(conn):
        conn.execute("DROP INDEX p3_position_current_open_idx")
        conn.execute(
            "CREATE INDEX p3_position_current_open_idx "
            "ON p3_position_current(decision_id) WHERE sold_qty<=bought_qty"
        )

    def wrong_trigger(conn):
        conn.execute("DROP TRIGGER p3_position_after_sell")
        conn.execute(
            "CREATE TRIGGER p3_position_after_sell AFTER INSERT ON outcomes "
            "BEGIN SELECT 1; END"
        )

    assert_rejected(wrong_table, "wrong schema object")
    assert_rejected(wrong_index, "wrong schema object")
    assert_rejected(wrong_partial_index, "wrong schema object")
    assert_rejected(wrong_trigger, "wrong schema object")
    assert_rejected(
        lambda candidate: candidate.execute(
            "DROP INDEX canonical_pending_incomplete_idx"
        ),
        "missing schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE INDEX extra_v5_owned_idx "
            "ON canonical_pending_current(decision_id)"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE INDEX p3_unexpected_legacy_idx ON outcomes(ref_id)"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE INDEX arbitrary_additive_column_idx "
            "ON paper_trades(canonical_proof_hash)"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_additive_column_guard "
            "BEFORE INSERT ON paper_trades "
            "WHEN NEW.canonical_proof_hash IS NOT NULL "
            "BEGIN SELECT 1; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            'CREATE INDEX arbitrary_quoted_additive_idx '
            'ON paper_trades("canonical_proof_hash")'
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_quoted_additive_guard "
            "BEFORE INSERT ON paper_trades "
            "WHEN NEW.[canonical_proof_hash] IS NOT NULL "
            "BEGIN SELECT 1; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_cross_additive_guard "
            "BEFORE INSERT ON paper_trades "
            "WHEN (SELECT p3_exit_trade_id FROM outcomes LIMIT 1) IS NOT NULL "
            "BEGIN SELECT 1; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_additive_update_guard "
            "AFTER INSERT ON paper_trades WHEN NEW.mint='never' BEGIN "
            "UPDATE paper_trades SET canonical_proof_hash=NULL WHERE id=NEW.id; "
            "END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_additive_insert_guard "
            "AFTER INSERT ON paper_trades WHEN NEW.mint='never' BEGIN "
            "INSERT INTO paper_trades("
            "at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade,canonical_proof_hash) "
            "VALUES(1,'x','climbing','buy',1,1,1,'{}','B','proof'); END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_alias_additive_insert_guard "
            "AFTER INSERT ON paper_trades WHEN NEW.mint='never' BEGIN "
            "INSERT INTO paper_trades AS p("
            "at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade,canonical_proof_hash) "
            "VALUES(1,'x','climbing','buy',1,1,1,'{}','B','proof'); END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_additive_replace_guard "
            "AFTER INSERT ON tokens BEGIN "
            "REPLACE INTO tokens(mint,p3_identity_ingested_at) "
            "VALUES('shadow',1); END"
        ),
        "extra v5-owned schema object",
    )

    def add_all_columns_update_trigger(candidate):
        columns = ",".join(
            row[1] for row in candidate.execute("PRAGMA table_info(paper_trades)")
        )
        candidate.execute(
            "CREATE TRIGGER arbitrary_all_columns_update_guard "
            f"BEFORE UPDATE OF {columns} ON paper_trades "
            "BEGIN SELECT 1; END"
        )

    assert_rejected(
        add_all_columns_update_trigger, "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_new_table_read_guard "
            "AFTER INSERT ON tokens BEGIN "
            "SELECT id FROM canonical_observations LIMIT 1; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_new_table_delete_guard "
            "AFTER INSERT ON tokens BEGIN DELETE FROM p3_position_current; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_global_additive_read_guard "
            "AFTER INSERT ON decisions BEGIN "
            "SELECT canonical_proof_hash FROM paper_trades LIMIT 1; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIEW arbitrary_new_table_view AS "
            "SELECT id FROM canonical_observations"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIEW arbitrary_new_table_indexed_view AS "
            "SELECT decision_id FROM canonical_observations "
            "INDEXED BY canonical_observations_decision_idx"
        ),
        "extra v5-owned schema object",
    )

    def add_v5_unique_upsert(candidate):
        candidate.execute(
            "CREATE TABLE legacy_v5_upsert_source(x TEXT)"
        )
        candidate.execute(
            "CREATE TRIGGER arbitrary_v5_unique_upsert "
            "AFTER INSERT ON legacy_v5_upsert_source BEGIN "
            "INSERT INTO canonical_generations("
            "generation_hash,first_decision_id,created_at) "
            "VALUES(NEW.x,1,1) ON CONFLICT(generation_hash) "
            "DO UPDATE SET created_at=excluded.created_at; END"
        )

    assert_rejected(
        add_v5_unique_upsert, "extra v5-owned schema object",
    )

    def add_generated_v5_view(candidate):
        candidate.execute(
            "CREATE TABLE legacy_generated_v5_view_source("
            "a INTEGER,b INTEGER GENERATED ALWAYS AS (a+1) VIRTUAL)"
        )
        candidate.execute(
            "CREATE VIEW legacy_generated_v5_view AS "
            "SELECT b FROM legacy_generated_v5_view_source "
            "UNION ALL SELECT id FROM canonical_observations"
        )

    assert_rejected(
        add_generated_v5_view, "extra v5-owned schema object",
    )

    def add_generated_v5_trigger(candidate):
        candidate.execute(
            "CREATE TABLE legacy_generated_v5_trigger_source("
            "a INTEGER,b INTEGER GENERATED ALWAYS AS (a+1) STORED)"
        )
        candidate.execute(
            "CREATE TRIGGER legacy_generated_v5_trigger "
            "BEFORE INSERT ON legacy_generated_v5_trigger_source "
            "WHEN NEW.b IS NOT NULL BEGIN "
            "SELECT canonical_proof_hash FROM paper_trades LIMIT 1; END"
        )

    assert_rejected(
        add_generated_v5_trigger, "extra v5-owned schema object",
    )

    for event, body in (
        ("INSERT", "SELECT id FROM canonical_observations LIMIT 1;"),
        (
            "UPDATE OF x",
            "SELECT canonical_proof_hash FROM paper_trades LIMIT 1;",
        ),
        ("DELETE", "DELETE FROM p3_position_current;"),
    ):
        def add_v5_touching_view_trigger(
            candidate, event=event, body=body,
        ):
            operation = event.split()[0].lower()
            candidate.execute(f"CREATE TABLE legacy_{operation}_sink(x)")
            candidate.execute(
                f"CREATE VIEW legacy_{operation}_sink_view AS "
                f"SELECT x FROM legacy_{operation}_sink"
            )
            candidate.execute(
                f"CREATE TRIGGER legacy_{operation}_sink_view_trigger "
                f"INSTEAD OF {event} ON legacy_{operation}_sink_view "
                f"BEGIN {body} END"
            )

        assert_rejected(
            add_v5_touching_view_trigger, "extra v5-owned schema object",
        )

    def add_v5_window_view(candidate):
        candidate.create_window_function("caller_window", 1, CallerWindow)
        candidate.execute(
            "CREATE VIEW arbitrary_v5_window_view AS "
            "SELECT caller_window(id) OVER () AS value "
            "FROM canonical_observations"
        )

    assert_rejected(add_v5_window_view, "extra v5-owned schema object")

    def add_v5_mixed_function_view(candidate):
        candidate.create_function("caller_mixed", 1, lambda value: value)
        candidate.create_window_function("caller_mixed", 2, MixedWindow)
        candidate.execute(
            "CREATE VIEW arbitrary_v5_mixed_function_view AS "
            "SELECT id,caller_mixed(id,id) OVER () AS window_value "
            "FROM canonical_observations WHERE caller_mixed(id) IS NOT NULL"
        )

    assert_rejected(
        add_v5_mixed_function_view, "extra v5-owned schema object",
    )

    def add_v5_collated_index(candidate):
        candidate.create_collation(
            "caller_cmp", lambda left, right: (left > right) - (left < right),
        )
        candidate.execute(
            "CREATE INDEX arbitrary_additive_collation_idx "
            "ON paper_trades(canonical_proof_hash COLLATE caller_cmp)"
        )

    assert_rejected(add_v5_collated_index, "extra v5-owned schema object")

    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_single_quoted_additive_update "
            "AFTER UPDATE OF 'canonical_proof_hash' ON paper_trades "
            "BEGIN SELECT 1; END"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_single_quoted_additive_insert "
            "AFTER INSERT ON tokens BEGIN "
            "INSERT INTO paper_trades("
            "at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade,'canonical_proof_hash') "
            "VALUES(1,'x','climbing','buy',1,1,1,'{}','B','proof'); END"
        ),
        "extra v5-owned schema object",
    )

    def add_v5_builtin_overload_view(candidate):
        candidate.create_function(
            "abs", 2, lambda left, right: left if left == right else None,
            deterministic=True,
        )
        candidate.execute(
            "CREATE VIEW arbitrary_v5_builtin_overload_view AS "
            "SELECT abs(id,id) AS value FROM canonical_observations"
        )

    assert_rejected(
        add_v5_builtin_overload_view, "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIEW malformed_builtin_arity_view AS "
            "SELECT abs(mint,mint) FROM tokens"
        ),
        "schema object",
    )

    def add_unknown_update_trigger(candidate, name, body):
        candidate.execute(f"CREATE TABLE {name}_sink(x)")
        candidate.execute(
            f"CREATE TRIGGER {name} AFTER UPDATE OF never_column "
            f"ON {name}_sink BEGIN {body} END"
        )

    assert_rejected(
        lambda candidate: add_unknown_update_trigger(
            candidate,
            "hidden_v5_guard",
            "SELECT id FROM canonical_observations LIMIT 1;",
        ),
        "schema object",
    )
    assert_rejected(
        lambda candidate: add_unknown_update_trigger(
            candidate, "unrelated_unknown_update_guard", "SELECT 1;",
        ),
        "schema object",
    )

    def add_without_rowid_update_trigger(candidate):
        candidate.execute(
            "CREATE TABLE legacy_without_rowid_sink("
            "x TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        candidate.execute(
            "CREATE TRIGGER legacy_without_rowid_update "
            "AFTER UPDATE OF rowid ON legacy_without_rowid_sink "
            "BEGIN SELECT 1; END"
        )

    assert_rejected(add_without_rowid_update_trigger, "schema object")

    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE VIEW arbitrary_v5_cte_view AS "
            "WITH hidden AS (SELECT id FROM canonical_observations) "
            "SELECT * FROM hidden"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "CREATE TRIGGER arbitrary_v5_nested_cte_trigger "
            "AFTER INSERT ON tokens BEGIN "
            "SELECT * FROM (WITH hidden AS ("
            "SELECT id FROM canonical_observations) SELECT * FROM hidden); END"
        ),
        "extra v5-owned schema object",
    )

    def add_v5_nested_view_trigger(candidate):
        candidate.execute(
            "CREATE VIEW arbitrary_v5_nested_source_view AS "
            "SELECT id FROM canonical_observations"
        )
        candidate.execute(
            "CREATE TRIGGER arbitrary_v5_nested_view_trigger "
            "AFTER INSERT ON tokens BEGIN "
            "SELECT id FROM arbitrary_v5_nested_source_view; END"
        )

    assert_rejected(
        add_v5_nested_view_trigger, "extra v5-owned schema object",
    )

    def rewrite_schema_catalog(candidate, sql, parameters=()):
        candidate.execute("PRAGMA writable_schema=ON")
        candidate.execute(sql, parameters)
        candidate.execute("PRAGMA writable_schema=OFF")

    def forge_table_attachment(candidate):
        candidate.execute(
            "CREATE TABLE stealth_fk("
            "x INTEGER REFERENCES canonical_observations(id))"
        )
        rewrite_schema_catalog(
            candidate,
            "UPDATE sqlite_schema SET tbl_name='innocent' "
            "WHERE name='stealth_fk'",
        )

    assert_rejected(forge_table_attachment, "schema object")

    def forge_table_ddl_name(candidate):
        candidate.execute("CREATE TABLE stealth_ddl_name(x INTEGER)")
        table_sql = candidate.execute(
            "SELECT sql FROM sqlite_schema WHERE name='stealth_ddl_name'"
        ).fetchone()[0]
        rewrite_schema_catalog(
            candidate,
            "UPDATE sqlite_schema SET sql=? "
            "WHERE name='stealth_ddl_name'",
            (table_sql.replace("stealth_ddl_name", "innocent_ddl_name", 1),),
        )

    assert_rejected(forge_table_ddl_name, "schema object")

    for active_mutation in (forge_table_attachment, forge_table_ddl_name):
        active, _ = make_v4()
        install(active)
        active_mutation(active)
        assert active.in_transaction
        with pytest.raises(ValueError, match="schema object"):
            store._attest_v5_schema_manifest(active)
        assert active.in_transaction
        active.rollback()
        active.close()

    for malformed_type in ("VIEW ", None, "bogus", "table"):
        def rewrite_view_type(candidate, malformed_type=malformed_type):
            candidate.execute(
                "CREATE VIEW malformed_catalog_view AS "
                "SELECT id FROM canonical_observations"
            )
            rewrite_schema_catalog(
                candidate,
                "UPDATE sqlite_schema SET type=? "
                "WHERE name='malformed_catalog_view'",
                (malformed_type,),
            )

        assert_rejected(rewrite_view_type, "schema object")

    def null_view_sql(candidate):
        candidate.execute(
            "CREATE VIEW null_sql_catalog_view AS "
            "SELECT id FROM canonical_observations"
        )
        rewrite_schema_catalog(
            candidate,
            "UPDATE sqlite_schema SET sql=NULL "
            "WHERE name='null_sql_catalog_view'",
        )

    assert_rejected(null_view_sql, "schema object")

    for actual_type, wrong_type, create_sql, name in (
        (
            "table", "view", "CREATE TABLE mismatched_catalog_table(x)",
            "mismatched_catalog_table",
        ),
        (
            "index", "trigger",
            "CREATE INDEX mismatched_catalog_index ON tokens(mint)",
            "mismatched_catalog_index",
        ),
        (
            "trigger", "index",
            "CREATE TRIGGER mismatched_catalog_trigger "
            "AFTER INSERT ON tokens BEGIN SELECT 1; END",
            "mismatched_catalog_trigger",
        ),
    ):
        def rewrite_object_type(
            candidate, actual_type=actual_type, wrong_type=wrong_type,
            create_sql=create_sql, name=name,
        ):
            candidate.execute(create_sql)
            rewrite_schema_catalog(
                candidate,
                "UPDATE sqlite_schema SET type=? "
                "WHERE type=? AND name=?",
                (wrong_type, actual_type, name),
            )

        assert_rejected(rewrite_object_type, "schema object")

    for name, index_sql in (
        (
            "forged_new_table_idx",
            "CREATE INDEX forged_new_table_idx "
            "ON canonical_observations(id)",
        ),
        (
            "forged_additive_column_idx",
            "CREATE INDEX forged_additive_column_idx "
            "ON paper_trades(canonical_proof_hash)",
        ),
    ):
        def forge_index_attachment(
            candidate, name=name, index_sql=index_sql,
        ):
            candidate.execute(index_sql)
            rewrite_schema_catalog(
                candidate,
                "UPDATE sqlite_schema SET tbl_name='user_notes' "
                "WHERE name=?",
                (name,),
            )

        assert_rejected(
            forge_index_attachment, "wrong schema object",
        )

    for schema_type, sql in (
        ("index", None),
        (
            "index",
            "CREATE INDEX invalid_replay_attached_idx "
            "ON paper_trades(mint) @@@",
        ),
        (
            "INDEX ",
            "CREATE INDEX invalid_type_attached_idx ON paper_trades(mint)",
        ),
    ):
        def malformed_legacy_attachment(
            candidate, schema_type=schema_type, sql=sql,
        ):
            name = (
                "null_sql_attached_idx" if sql is None
                else sql.split()[2]
            )
            rewrite_schema_catalog(
                candidate,
                "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
                "VALUES (?,?, 'paper_trades',0,?)",
                (schema_type, name, sql),
            )

        assert_rejected(malformed_legacy_attachment, "schema object")

    assert_rejected(
        lambda candidate: candidate.execute(
            "ALTER TABLE paper_trades ADD COLUMN proof_shadow TEXT "
            "GENERATED ALWAYS AS (canonical_proof_hash) VIRTUAL"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "ALTER TABLE paper_trades ADD COLUMN p3_unexpected TEXT"
        ),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute(
            "ALTER TABLE paper_trades ADD COLUMN legacy_proof_guard TEXT "
            "CHECK(canonical_proof_hash IS NULL)"
        ),
        "extra v5-owned schema object",
    )

    def add_legacy_column_v5_foreign_key(candidate):
        table_sql = candidate.execute(
            "SELECT sql FROM sqlite_schema WHERE name='paper_trades'"
        ).fetchone()[0]
        prefix, closing, suffix = table_sql.rpartition(")")
        assert closing and not suffix.strip()
        rewrite_schema_catalog(
            candidate,
            "UPDATE sqlite_schema SET sql=? WHERE name='paper_trades'",
            (
                prefix
                + ",FOREIGN KEY(mint) REFERENCES canonical_observations(id))",
            ),
        )
        schema_version = candidate.execute(
            "PRAGMA schema_version"
        ).fetchone()[0]
        candidate.execute(f"PRAGMA schema_version={schema_version + 1}")

    assert_rejected(
        add_legacy_column_v5_foreign_key, "extra v5-owned schema object",
    )

    whitespace, _ = make_v4()
    install(whitespace)
    table_sql = whitespace.execute(
        "SELECT sql FROM sqlite_schema WHERE name='paper_trades'"
    ).fetchone()[0]
    rewrite_schema_catalog(
        whitespace,
        "UPDATE sqlite_schema SET sql=? WHERE name='paper_trades'",
        (table_sql.replace("CREATE TABLE paper_trades (", "CREATE TABLE paper_trades  (", 1),),
    )
    schema_version = whitespace.execute("PRAGMA schema_version").fetchone()[0]
    whitespace.execute(f"PRAGMA schema_version={schema_version + 1}")
    assert store._attest_v5_schema_manifest(whitespace) is None
    whitespace.close()

    def fake_v5_autoindex(candidate):
        rewrite_schema_catalog(
            candidate,
            "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
            "VALUES ('index','sqlite_autoindex_holder_evidence_999',"
            "'holder_evidence',0,NULL)",
        )

    assert_rejected(fake_v5_autoindex, "extra v5-owned schema object")

    def fake_uppercase_v5_attachment(candidate):
        rewrite_schema_catalog(
            candidate,
            "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
            "VALUES ('index','unexpected_uppercase_attachment',"
            "'HOLDER_EVIDENCE',0,NULL)",
        )

    assert_rejected(
        fake_uppercase_v5_attachment, "extra v5-owned schema object",
    )

    for schema_type in ("INDEX", "TRIGGER"):
        def fake_uppercase_type(candidate, schema_type=schema_type):
            rewrite_schema_catalog(
                candidate,
                "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
                "VALUES (?,?,'HOLDER_EVIDENCE',0,NULL)",
                (schema_type, f"unexpected_uppercase_{schema_type.lower()}"),
            )

        assert_rejected(
            fake_uppercase_type, "extra v5-owned schema object",
        )
    for ordinal, schema_type in enumerate((None, "INDEX ", "view")):
        def fake_malformed_type(
            candidate, ordinal=ordinal, schema_type=schema_type,
        ):
            rewrite_schema_catalog(
                candidate,
                "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
                "VALUES (?,?,'holder_evidence',0,NULL)",
                (schema_type, f"unexpected_attached_type_{ordinal}"),
            )

        assert_rejected(fake_malformed_type, "extra v5-owned schema object")
    assert_rejected(
        lambda candidate: rewrite_schema_catalog(
            candidate,
            "DELETE FROM sqlite_schema "
            "WHERE name='sqlite_autoindex_holder_evidence_1'",
        ),
        "missing schema object",
    )
    assert_rejected(
        lambda candidate: rewrite_schema_catalog(
            candidate,
            "UPDATE sqlite_schema SET tbl_name='tokens' "
            "WHERE name='sqlite_autoindex_holder_evidence_1'",
        ),
        "wrong schema object",
    )

    def duplicate_case_variant(candidate):
        rewrite_schema_catalog(
            candidate,
            "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
            "VALUES ('index','CANONICAL_PENDING_INCOMPLETE_IDX',"
            "'canonical_pending_current',0,NULL)",
        )

    assert_rejected(duplicate_case_variant, "duplicate schema object")

    def single_case_variant(candidate):
        candidate.execute("DROP INDEX canonical_pending_incomplete_idx")
        candidate.execute(
            "CREATE INDEX CANONICAL_PENDING_INCOMPLETE_IDX "
            "ON canonical_pending_current(observation_id) "
            "WHERE completed_mask<>full_mask"
        )

    assert_rejected(single_case_variant, "wrong schema object")
    assert_rejected(
        lambda candidate: candidate.execute("CREATE TABLE P3_extension(x)"),
        "extra v5-owned schema object",
    )
    assert_rejected(
        lambda candidate: candidate.execute("CREATE TABLE CANONICAL_extension(x)"),
        "extra v5-owned schema object",
    )

    duplicate, _ = make_v4()
    install(duplicate)
    duplicate.execute("PRAGMA writable_schema=ON")
    duplicate.execute(
        "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
        "VALUES ('index','canonical_pending_incomplete_idx',"
        "'canonical_pending_current',0,NULL)"
    )
    duplicate.execute("PRAGMA writable_schema=OFF")
    duplicate.commit()
    with pytest.raises(ValueError, match="duplicate schema object"):
        store._attest_v5_schema_manifest(duplicate)
    duplicate.close()

    def rewrite_schema(conn, table, replacements):
        schema_sql = conn.execute(
            "SELECT sql FROM sqlite_schema WHERE name=?", (table,)
        ).fetchone()[0]
        for old, new in replacements:
            assert old in schema_sql
            schema_sql = schema_sql.replace(old, new, 1)
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_schema SET sql=? WHERE name=?", (schema_sql, table),
        )
        schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
        conn.execute(f"PRAGMA schema_version={schema_version + 1}")
        conn.execute("PRAGMA writable_schema=OFF")

    for replacement in (
        "canonical_proof_hash BLOB",
        "canonical_proof_hash TEXT DEFAULT ''",
        "canonical_proof_hash TEXT NOT NULL",
    ):
        malformed, _ = make_v4()
        install(malformed)
        rewrite_schema(
            malformed, "paper_trades",
            (("canonical_proof_hash TEXT", replacement),),
        )
        with pytest.raises(ValueError, match="additive column mismatch"):
            store._attest_v5_schema_manifest(malformed)
        malformed.close()

    for original, replacement in (
        (
            "canonical_proof_hash TEXT",
            "canonical_proof_hash TEXT CHECK(mint<>'')",
        ),
        (
            "canonical_proof_hash TEXT",
            "canonical_proof_hash TEXT COLLATE RTRIM",
        ),
        (
            "REFERENCES canonical_rechecks(id)",
            "REFERENCES canonical_rechecks(id) "
            "DEFERRABLE INITIALLY DEFERRED",
        ),
    ):
        malformed, _ = make_v4()
        install(malformed)
        rewrite_schema(
            malformed, "paper_trades", ((original, replacement),),
        )
        with pytest.raises(
            ValueError, match="additive column definition mismatch",
        ):
            store._attest_v5_schema_manifest(malformed)
        malformed.close()

    cosmetic_definition, _ = make_v4()
    install(cosmetic_definition)
    rewrite_schema(
        cosmetic_definition,
        "paper_trades",
        (
            (
                "canonical_proof_hash TEXT",
                "canonical_proof_hash /* harmless */ tExT",
            ),
            (
                "REFERENCES canonical_rechecks(id)",
                "rEfErEnCeS /* harmless */ canonical_rechecks ( id )",
            ),
        ),
    )
    assert store._attest_v5_schema_manifest(cosmetic_definition) is None
    cosmetic_definition.close()

    malformed, _ = make_v4()
    install(malformed)
    rewrite_schema(
        malformed, "tokens",
        (
            ("mint TEXT PRIMARY KEY", "mint TEXT"),
            (
                "p3_identity_ingested_at REAL",
                "p3_identity_ingested_at REAL PRIMARY KEY",
            ),
        ),
    )
    with pytest.raises(ValueError, match="additive column mismatch"):
        store._attest_v5_schema_manifest(malformed)
    malformed.close()

    for replacements in (
        (("REFERENCES canonical_rechecks(id)", ""),),
        (("REFERENCES canonical_rechecks(id)", "REFERENCES decisions(id)"),),
        (
            (
                "canonical_proof_hash TEXT",
                "canonical_proof_hash TEXT REFERENCES decisions(id)",
            ),
        ),
    ):
        malformed, _ = make_v4()
        install(malformed)
        rewrite_schema(malformed, "paper_trades", replacements)
        with pytest.raises(ValueError, match="additive foreign key mismatch"):
            store._attest_v5_schema_manifest(malformed)
        malformed.close()


def test_p3_fee_sum_json_unit_contract():
    import json
    import math
    from typing import Any, cast

    from memebot.store import p3_fee_sum_json

    assert p3_fee_sum_json("{}") == 0.0
    assert p3_fee_sum_json('{"base":1,"priority":0.25}') == 1.25
    assert p3_fee_sum_json('{"' + "k" * 64 + '":0}') == 0.0

    cancellation_values = {
        "a": 0.023256414847064033,
        "b": 1.92038142197666e-33,
        "c": 0.48020612315678757,
    }
    cancellation_json = json.dumps(
        cancellation_values, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    assert math.fsum(cancellation_values.values()) != sum(cancellation_values.values())
    assert p3_fee_sum_json(cancellation_json) == math.fsum(cancellation_values.values())

    oversized_canonical_json = json.dumps(
        {f"k{i:04d}": 0 for i in range(500)},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    assert len(oversized_canonical_json) > 4096

    invalid_values = (
        None,
        b"{}",
        oversized_canonical_json,
        "[]",
        '{"a":1,"a":2}',
        '{"b":1,"a":2}',
        '{"":1}',
        '{"' + "a" * 65 + '":1}',
        '{"a":true}',
        '{"a":"1"}',
        '{"a":NaN}',
        '{"a":Infinity}',
        '{"a":-1}',
        '{"a":1000000.1}',
        '{ "a":1}',
    )
    for value in invalid_values:
        with pytest.raises(ValueError):
            p3_fee_sum_json(cast(Any, value))


def test_p3_fee_sum_json_is_canonical_ordered_and_strict():
    import json
    import math

    from memebot.store import p3_fee_sum_json

    assert p3_fee_sum_json("{}") == 0.0
    assert p3_fee_sum_json('{"base":1,"priority":0.25}') == 1.25

    with pytest.raises(ValueError, match="fee keys must be unique and sorted"):
        p3_fee_sum_json('{"base":1,"base":2}')
    with pytest.raises(ValueError, match="fee keys must be unique and sorted"):
        p3_fee_sum_json('{"priority":0.25,"base":1}')
    with pytest.raises(ValueError, match="fees_json is not canonical"):
        p3_fee_sum_json('{"base":1, "priority":0.25}')

    overlong_key_json = json.dumps(
        {"k" * 65: 0}, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    with pytest.raises(ValueError, match="invalid fee key"):
        p3_fee_sum_json(overlong_key_json)
    with pytest.raises(ValueError, match="invalid fee value"):
        p3_fee_sum_json('{"base":true}')
    with pytest.raises(ValueError, match="invalid fee value"):
        p3_fee_sum_json('{"credit":1,"debit":-0.5}')

    cancellation_values = {
        "a": 0.023256414847064033,
        "b": 1.92038142197666e-33,
        "c": 0.48020612315678757,
    }
    cancellation_json = json.dumps(
        cancellation_values, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    exact_total = math.fsum(cancellation_values.values())
    assert sum(cancellation_values.values()) != exact_total
    assert p3_fee_sum_json(cancellation_json) == exact_total


def test_open_db_registers_p3_fee_sum_before_version_inspection(tmp_path, monkeypatch):
    import memebot.store as store

    events = []
    original_connect = sqlite3.connect

    class TracingConnection(sqlite3.Connection):
        def create_function(self, name, narg, func, *, deterministic=False):
            events.append(("create_function", name, narg, func, deterministic))
            return super().create_function(name, narg, func, deterministic=deterministic)

        def execute(self, sql, parameters=(), /):
            if sql.strip().upper() == "PRAGMA USER_VERSION":
                events.append(("user_version",))
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        return original_connect(*args, factory=TracingConnection, **kwargs)

    monkeypatch.setattr(store.sqlite3, "connect", connect)

    conn = store.open_db(tmp_path / "t.db")

    registration = (
        "create_function", "p3_fee_sum", 1, store.p3_fee_sum_json, True,
    )
    assert registration in events
    assert events.index(registration) < events.index(("user_version",))
    assert conn.execute("SELECT p3_fee_sum(?)", ('{"base":1,"priority":0.25}',)).fetchone()[0] == 1.25


def test_open_db_registers_deterministic_p3_fee_sum(tmp_path, monkeypatch):
    import memebot.store as store

    db_path = tmp_path / "t.db"
    store.open_db(db_path).close()
    registrations = []
    original_connect = sqlite3.connect

    class RecordingConnection(sqlite3.Connection):
        def create_function(self, name, narg, func, *, deterministic=False):
            registrations.append((name, narg, func, deterministic))
            return super().create_function(
                name, narg, func, deterministic=deterministic,
            )

    def connect(*args, **kwargs):
        return original_connect(*args, factory=RecordingConnection, **kwargs)

    monkeypatch.setattr(store.sqlite3, "connect", connect)

    conn = store.open_db(db_path)
    fees_json = (
        '{"a":0.023256414847064033,"b":1.92038142197666e-33,'
        '"c":0.48020612315678757}'
    )

    sql_total = conn.execute("SELECT p3_fee_sum(?)", (fees_json,)).fetchone()[0]
    assert sql_total == store.p3_fee_sum_json(fees_json)
    assert (
        "p3_fee_sum", 1, store.p3_fee_sum_json, True,
    ) in registrations


def test_v5_checks_reject_mixed_affinity_and_nonfinite_shapes(tmp_path):
    conn = open_db(tmp_path / "v5-shapes.db")
    conn.execute(
        "INSERT INTO tokens(mint,created_at,last_seen) VALUES ('MINT',0.0,0.0)"
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash) "
        "VALUES (1.0,'MINT','CLIMBING','SKIP',0.0,'{}','cfg')"
    ).lastrowid
    conn.commit()

    observation_count = conn.execute(
        "SELECT count(*) FROM canonical_observations"
    ).fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason) "
            "VALUES (?,?,?,1,1,1,NULL,NULL,'','start_price_missing')",
            (decision_id, "MINT", float("inf")),
        )
    assert conn.execute(
        "SELECT count(*) FROM canonical_observations"
    ).fetchone()[0] == observation_count

    safety_count = conn.execute(
        "SELECT count(*) FROM safety_reports"
    ).fetchone()[0]
    with pytest.raises(
        sqlite3.IntegrityError, match="invalid safety report shape",
    ):
        conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES ('MINT',2.0,'[]',?,?)",
            (float("inf"), "a" * 64),
        )
    assert conn.execute(
        "SELECT count(*) FROM safety_reports"
    ).fetchone()[0] == safety_count

    wallet_count = conn.execute(
        "SELECT count(*) FROM wallet_pnl_events"
    ).fetchone()[0]
    with pytest.raises(
        sqlite3.IntegrityError, match="invalid wallet PnL shape",
    ):
        conn.execute(
            "INSERT INTO wallet_pnl_events("
            "at,wallet,mint,realized_pnl_sol,source,detail_json) "
            "VALUES (3.0,'WALLET','MINT',1.0,?,'{}')",
            (sqlite3.Binary(b"fixture"),),
        )
    assert conn.execute(
        "SELECT count(*) FROM wallet_pnl_events"
    ).fetchone()[0] == wallet_count

    early_buyer_count = conn.execute(
        "SELECT count(*) FROM early_buyer_reads"
    ).fetchone()[0]
    with pytest.raises(
        sqlite3.IntegrityError, match="invalid early-buyer shape",
    ):
        conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id) VALUES ('MINT',4.0,'[]','unknown_reason',?,NULL)",
            ("b" * 64,),
        )
    assert conn.execute(
        "SELECT count(*) FROM early_buyer_reads"
    ).fetchone()[0] == early_buyer_count


def test_open_db_enables_recursive_triggers_before_version_inspection(tmp_path, monkeypatch):
    import memebot.store as store

    events = []
    original_connect = sqlite3.connect

    class TracingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            events.append(sql.strip().upper())
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        return original_connect(*args, factory=TracingConnection, **kwargs)

    monkeypatch.setattr(store.sqlite3, "connect", connect)

    conn = store.open_db(tmp_path / "t.db")

    assert conn.execute("PRAGMA recursive_triggers").fetchone()[0] == 1
    fk_index = events.index("PRAGMA FOREIGN_KEYS=ON")
    assert events[fk_index:fk_index + 5] == [
        "PRAGMA FOREIGN_KEYS=ON",
        "PRAGMA RECURSIVE_TRIGGERS=ON",
        "PRAGMA FOREIGN_KEYS",
        "PRAGMA RECURSIVE_TRIGGERS",
        "PRAGMA USER_VERSION",
    ]


def test_open_db_enables_recursive_triggers(tmp_path):
    conn = open_db(tmp_path / "t.db")

    assert conn.execute("PRAGMA recursive_triggers").fetchone()[0] == 1


def test_open_db_rejects_unavailable_recursive_triggers(tmp_path, monkeypatch):
    import memebot.store as store

    original_connect = sqlite3.connect

    class UnavailableRecursiveTriggersConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            if sql.strip().upper() == "PRAGMA RECURSIVE_TRIGGERS":
                return super().execute("SELECT 0")
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        return original_connect(
            *args, factory=UnavailableRecursiveTriggersConnection, **kwargs,
        )

    monkeypatch.setattr(store.sqlite3, "connect", connect)

    with pytest.raises(RuntimeError, match="recursive_triggers"):
        store.open_db(tmp_path / "t.db")


@pytest.mark.parametrize("pragma_row", [0, 2, None])
def test_open_db_fails_closed_when_foreign_keys_unavailable(
    tmp_path, monkeypatch, pragma_row,
):
    import memebot.store as store

    events = []
    connections = []
    original_connect = sqlite3.connect

    class UnavailableForeignKeysConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            normalized = sql.strip().upper()
            events.append(normalized)
            if normalized == "PRAGMA FOREIGN_KEYS":
                if pragma_row is None:
                    return super().execute("SELECT 1 WHERE 0")
                return super().execute("SELECT ?", (pragma_row,))
            return super().execute(sql, parameters)

        def executescript(self, sql_script, /):
            events.append("EXECUTESCRIPT")
            return super().executescript(sql_script)

        def close(self):
            events.append("CLOSE")
            return super().close()

    def connect(*args, **kwargs):
        conn = original_connect(
            *args, factory=UnavailableForeignKeysConnection, **kwargs,
        )
        connections.append(conn)
        return conn

    monkeypatch.setattr(store.sqlite3, "connect", connect)

    with pytest.raises(RuntimeError, match="foreign_keys"):
        store.open_db(tmp_path / "t.db")

    assert "PRAGMA USER_VERSION" not in events
    assert "EXECUTESCRIPT" not in events
    assert events[-1] == "CLOSE"
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connections[0].execute("SELECT 1")


@pytest.mark.parametrize("pragma_row", [0, 2, None])
def test_open_db_fails_closed_when_recursive_triggers_unavailable(
    tmp_path, monkeypatch, pragma_row,
):
    import memebot.store as store

    events = []
    connections = []
    original_connect = sqlite3.connect

    class UnavailableRecursiveTriggersConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            normalized = sql.strip().upper()
            events.append(normalized)
            if normalized == "PRAGMA RECURSIVE_TRIGGERS":
                if pragma_row is None:
                    return super().execute("SELECT 1 WHERE 0")
                return super().execute("SELECT ?", (pragma_row,))
            return super().execute(sql, parameters)

        def executescript(self, sql_script, /):
            events.append("EXECUTESCRIPT")
            return super().executescript(sql_script)

        def close(self):
            events.append("CLOSE")
            return super().close()

    def connect(*args, **kwargs):
        conn = original_connect(
            *args, factory=UnavailableRecursiveTriggersConnection, **kwargs,
        )
        connections.append(conn)
        return conn

    monkeypatch.setattr(store.sqlite3, "connect", connect)

    with pytest.raises(RuntimeError, match="recursive_triggers"):
        store.open_db(tmp_path / "t.db")

    assert "PRAGMA USER_VERSION" not in events
    assert "EXECUTESCRIPT" not in events
    assert events[-1] == "CLOSE"
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connections[0].execute("SELECT 1")


def test_open_sets_wal_and_version(tmp_path):
    conn = open_db(tmp_path / "t.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6


def test_reopen_is_idempotent(tmp_path):
    open_db(tmp_path / "t.db").close()
    conn = open_db(tmp_path / "t.db")  # second open must not re-apply DDL
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6


@pytest.mark.parametrize("table", LEDGER_TABLES)
def test_ledger_tables_reject_update_and_delete(tmp_path, table):
    conn = open_db(tmp_path / "t.db")
    if table == "decisions":
        conn.execute(
            "INSERT INTO decisions(at, mint, segment, action, score, feature_vector_json,"
            " config_hash) VALUES (1, 'M', 'trending', 'skip', 0.5, '{}', 'h')"
        )
    elif table == "paper_trades":
        conn.execute(
            "INSERT INTO paper_trades(at, mint, segment, side, qty, quote_price, fill_price,"
            " fees_json, realism_grade) VALUES (1, 'M', 'trending', 'buy', 1, 1, 1, '{}', 'B')"
        )
    elif table == "outcomes":
        conn.execute(
            "INSERT INTO outcomes(at, ref_kind, ref_id, pnl_sol, detail_json)"
            " VALUES (1, 'trade', 1, 0.0, '{}')"
        )
    else:
        conn.execute("INSERT INTO regime_log(at, state, inputs_json) VALUES (1, 'normal', '{}')")
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute(f"UPDATE {table} SET at = 2")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute(f"DELETE FROM {table}")


def test_open_db_heals_crash_between_ddl_and_version_stamp(tmp_path):
    # simulate SIGKILL after schema DDL but before user_version was stamped
    import memebot.store as store

    raw = sqlite3.connect(tmp_path / "t.db")
    raw.executescript(store.SCHEMA_V1)  # tables exist, no triggers, user_version still 0
    raw.close()

    conn = store.open_db(tmp_path / "t.db")  # must heal, not raise
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    conn.execute("INSERT INTO regime_log(at, state, inputs_json) VALUES (1, 'normal', '{}')")
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute("UPDATE regime_log SET at = 2")  # triggers were installed by the healing open


def test_boot_row_lifecycle(tmp_path):
    # open_db sets row_factory = sqlite3.Row globally (C10): fetched rows are Row
    # objects (name-indexable), which never compare equal to a plain tuple even
    # for a single column — assert by column name instead of tuple equality.
    conn = open_db(tmp_path / "t.db")
    boot_id = record_boot(conn, "confighash123")
    row = conn.execute(
        "SELECT config_hash, clean_shutdown FROM boots WHERE id = ?", (boot_id,)
    ).fetchone()
    assert row["config_hash"] == "confighash123" and row["clean_shutdown"] == 0
    mark_clean_shutdown(conn, boot_id)
    row = conn.execute("SELECT clean_shutdown FROM boots WHERE id = ?", (boot_id,)).fetchone()
    assert row["clean_shutdown"] == 1


def test_v2_migration_adds_bonding_curve_key(tmp_path):
    conn = open_db(tmp_path / "t.db")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tokens)").fetchall()]
    assert "bonding_curve_key" in cols


def test_v1_db_migrates_to_v2(tmp_path):
    import memebot.store as store
    raw = sqlite3.connect(tmp_path / "t.db")
    raw.executescript(store.SCHEMA_V1)
    raw.executescript(store._append_only_triggers())
    raw.execute("PRAGMA user_version=1")
    raw.commit()
    raw.close()
    conn = open_db(tmp_path / "t.db")  # must upgrade in place
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tokens)").fetchall()]
    assert "bonding_curve_key" in cols


def test_token_registry_roundtrip(tmp_path):
    from memebot.store import get_token, set_token_state, tracked_tokens, upsert_token
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")
    upsert_token(conn, mint="M1", created_at=1.0, bonding_curve_key="B1")  # idempotent
    row = get_token(conn, "M1")
    assert row["state"] == "FRESH" and row["bonding_curve_key"] == "B1"
    set_token_state(conn, "M1", "CLIMBING", progress_pct=12.5, last_seen=2.0)
    row = get_token(conn, "M1")
    assert row["state"] == "CLIMBING" and row["curve_progress"] == 12.5
    tracked = tracked_tokens(conn, states=("FRESH", "CLIMBING"), limit=10)
    assert [t["mint"] for t in tracked] == ["M1"]


def test_set_token_state_rejects_reserved_graduation(tmp_path):
    from memebot.store import (
        get_token,
        set_terminal_state_with_reputation,
        set_token_state,
        upsert_token,
    )

    conn = open_db(tmp_path / "t.db")
    for mint in ("TARGET", "ABANDONED", "TERMINAL"):
        upsert_token(conn, mint=mint, created_at=1.0)
    set_token_state(
        conn, "TARGET", "CLIMBING", progress_pct=12.5, last_seen=2.0
    )
    before_target = tuple(get_token(conn, "TARGET"))
    before_clock = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    before_reputation = conn.execute(
        "SELECT count(*) FROM creator_reputation_events"
    ).fetchone()[0]

    traced = []
    conn.set_trace_callback(traced.append)
    with pytest.raises(ValueError, match="GRADUATED"):
        set_token_state(
            conn, "TARGET", "GRADUATED", progress_pct=100.0, last_seen=3.0
        )
    conn.set_trace_callback(None)

    assert traced == []
    assert conn.in_transaction is False
    assert tuple(get_token(conn, "TARGET")) == before_target
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == before_clock
    assert conn.execute(
        "SELECT count(*) FROM creator_reputation_events"
    ).fetchone()[0] == before_reputation

    set_token_state(conn, "TARGET", "FRESH", progress_pct=25.0, last_seen=4.0)
    target = get_token(conn, "TARGET")
    assert (target["state"], target["curve_progress"], target["last_seen"]) == (
        "FRESH",
        25.0,
        4.0,
    )
    set_token_state(conn, "ABANDONED", "DEAD")
    abandoned = get_token(conn, "ABANDONED")
    assert (abandoned["state"], abandoned["rugged"]) == ("DEAD", 0)

    result = set_terminal_state_with_reputation(
        conn,
        mint="TERMINAL",
        outcome="GRADUATED",
        raw_processed_at=5.0,
        creator="CREATOR",
        creator_conflicted=False,
    )
    terminal = get_token(conn, "TERMINAL")
    assert result.state == "GRADUATED"
    assert result.reputation_event_id is not None
    assert (terminal["state"], terminal["rugged"]) == ("GRADUATED", 0)


def test_v3_adds_rugged_flag(tmp_path):
    conn = open_db(tmp_path / "t.db")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tokens)").fetchall()]
    assert "rugged" in cols


def test_v4_adds_wallet_pnl_and_early_buyer_tables(tmp_path):
    conn = open_db(tmp_path / "t.db")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "wallet_pnl_events" in tables
    assert "early_buyer_reads" in tables


def test_v5_complete_schema_contract(tmp_path, monkeypatch):
    import memebot.store as store

    path = tmp_path / "v5.db"
    calls = []
    immutable_triggers = store._v5_immutable_triggers()
    v5_trigger_names = {
        *(name for name, _ in store.V5_EXPLICIT_TRIGGER_DDL),
        *(sql.split()[5] for sql in immutable_triggers),
    }
    pre_trigger_units = (
        "initialize_p3_causal_clock",
        "_validate_v5_legacy_safety_reports",
        "_validate_v5_legacy_wallet_pnl_events",
        "_validate_v5_legacy_early_buyer_reads",
        "_validate_v5_legacy_creator_reputation_events",
        "_validate_v5_legacy_p3_trade_execution_graph",
        "_rebuild_v5_wallet_pnl_summary",
        "_rebuild_v5_creator_reputation_current",
        "_rebuild_v5_p3_position_current",
        "_rebuild_v5_canonical_pending_current",
    )

    def installed_v5_triggers(conn):
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type='trigger'"
            )
            if row[0] in v5_trigger_names
        }

    for name in (*pre_trigger_units, "_attest_v5_schema_manifest"):
        original = getattr(store, name)

        def observed(conn, *args, _name=name, _original=original, **kwargs):
            assert conn.in_transaction
            current_version = conn.execute(
                "PRAGMA user_version"
            ).fetchone()[0]
            if _name in pre_trigger_units:
                assert current_version == 4
                assert installed_v5_triggers(conn) == set()
            else:
                expected_version = (
                    4
                    if not any(
                        name == "_attest_v5_schema_manifest"
                        for name, _version in calls
                    )
                    else 5
                )
                assert current_version == expected_version
                assert installed_v5_triggers(conn) == v5_trigger_names
            calls.append((_name, current_version))
            return _original(conn, *args, **kwargs)

        monkeypatch.setattr(store, name, observed)

    conn = store.open_db(path)
    try:
        assert calls == [
            *((name, 4) for name in pre_trigger_units),
            ("_attest_v5_schema_manifest", 4),
            ("_attest_v5_schema_manifest", 5),
        ]
        assert len(store.V5_TABLE_DDL) + len(store.V5_ADDITIVE_COLUMNS) + len(
            store.V5_INDEX_DDL
        ) + len(store.V5_EXPLICIT_TRIGGER_DDL) + len(immutable_triggers) == 80
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
        assert not conn.in_transaction
        assert installed_v5_triggers(conn) == v5_trigger_names
        assert conn.execute(
            "SELECT singleton,last_wall FROM p3_causal_clock"
        ).fetchone()[0] == 1
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_v5_half_applied_migration_heals(tmp_path, monkeypatch):
    """Every committed DDL prefix heals; collisions and late failures roll back."""
    import memebot.store as store

    immutable = store._v5_immutable_triggers()
    steps = [
        *(sql for _name, sql in store.V5_TABLE_DDL),
        *(sql for _table, _column, sql in store.V5_ADDITIVE_COLUMNS),
        *(sql for _name, sql in store.V5_INDEX_DDL),
        *(sql for _name, sql in store.V5_EXPLICIT_TRIGGER_DDL),
        *immutable,
    ]
    assert len(steps) == 80

    def apply_prefix(path, count):
        conn = _open_v4_fixture(path)
        try:
            for sql in steps[:count]:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).casefold():
                        raise
            conn.commit()
        finally:
            conn.close()

    for count in range(81):
        path = tmp_path / f"prefix-{count}.db"
        apply_prefix(path, count)
        conn = open_db(path, migration_clock=lambda: 1000.0)
        try:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
            assert store._attest_v5_schema_manifest(conn) is None
        finally:
            conn.close()

    for kind, ddl in (
        ("table", "CREATE TABLE wallet_pnl_summary(wrong INTEGER)"),
        ("index", "CREATE INDEX wallet_pnl_events_at_idx ON wallet_pnl_events(id)"),
        ("trigger", "CREATE TRIGGER early_buyer_report_guard BEFORE INSERT ON early_buyer_reads BEGIN SELECT 1; END"),
    ):
        path = tmp_path / f"wrong-{kind}.db"
        conn = _open_v4_fixture(path)
        conn.execute(ddl)
        conn.commit()
        before = conn.serialize()
        conn.close()
        with pytest.raises((sqlite3.OperationalError, ValueError)):
            open_db(path, migration_clock=lambda: 1000.0)
        check = sqlite3.connect(path)
        try:
            assert check.serialize() == before
            assert check.execute("PRAGMA user_version").fetchone()[0] == 4
        finally:
            check.close()

    for hook in (
        "initialize_p3_causal_clock",
        "_rebuild_v5_wallet_pnl_summary",
        "_rebuild_v5_creator_reputation_current",
        "_rebuild_v5_p3_position_current",
        "_rebuild_v5_canonical_pending_current",
        "_attest_v5_schema_manifest",
        "_v5_immutable_triggers",
    ):
        path = tmp_path / f"failure-{hook}.db"
        conn = _open_v4_fixture(path)
        before = conn.serialize()
        conn.close()
        original = getattr(store, hook)

        def fail(*args, _original=original, **kwargs):
            _original(*args, **kwargs)
            raise RuntimeError(f"injected {hook}")

        monkeypatch.setattr(store, hook, fail)
        with pytest.raises(RuntimeError, match="injected"):
            open_db(path, migration_clock=lambda: 1000.0)
        monkeypatch.setattr(store, hook, original)
        check = sqlite3.connect(path)
        try:
            assert check.serialize() == before
            assert check.execute("PRAGMA user_version").fetchone()[0] == 4
        finally:
            check.close()

    class StampFailConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if str(sql).strip().casefold() == "pragma user_version=5":
                raise RuntimeError("injected version stamp")
            return super().execute(sql, parameters)

    path = tmp_path / "failure-version-stamp.db"
    conn = _open_v4_fixture(path)
    before = conn.serialize()
    conn.close()
    connect = sqlite3.connect
    monkeypatch.setattr(
        sqlite3,
        "connect",
        lambda *args, **kwargs: connect(
            *args, **kwargs, factory=StampFailConnection,
        ),
    )
    with pytest.raises(RuntimeError, match="version stamp"):
        open_db(path, migration_clock=lambda: 1000.0)
    monkeypatch.setattr(sqlite3, "connect", connect)
    check = sqlite3.connect(path)
    try:
        assert check.serialize() == before
        assert check.execute("PRAGMA user_version").fetchone()[0] == 4
    finally:
        check.close()


def test_v6_curve_progress_reserve_columns_migrate_heal_and_attest(
    tmp_path, monkeypatch,
):
    import memebot.store as store

    expected = (
        (
            "curve_progress_virtual_sol_reserves",
            "ALTER TABLE tokens ADD COLUMN curve_progress_virtual_sol_reserves INTEGER\n"
            "  CHECK (curve_progress_virtual_sol_reserves IS NULL OR\n"
            "         (typeof(curve_progress_virtual_sol_reserves)='integer' AND\n"
            "          curve_progress_virtual_sol_reserves BETWEEN 1 AND 9223372036854775807));",
        ),
        (
            "curve_progress_virtual_token_reserves",
            "ALTER TABLE tokens ADD COLUMN curve_progress_virtual_token_reserves INTEGER\n"
            "  CHECK (curve_progress_virtual_token_reserves IS NULL OR\n"
            "         (typeof(curve_progress_virtual_token_reserves)='integer' AND\n"
            "          curve_progress_virtual_token_reserves BETWEEN 1 AND 9223372036854775807));",
        ),
        (
            "curve_progress_real_sol_reserves",
            "ALTER TABLE tokens ADD COLUMN curve_progress_real_sol_reserves INTEGER\n"
            "  CHECK (curve_progress_real_sol_reserves IS NULL OR\n"
            "         (typeof(curve_progress_real_sol_reserves)='integer' AND\n"
            "          curve_progress_real_sol_reserves BETWEEN 0 AND 9223372036854775807));",
        ),
        (
            "curve_progress_real_token_reserves",
            "ALTER TABLE tokens ADD COLUMN curve_progress_real_token_reserves INTEGER\n"
            "  CHECK (\n"
            "    (curve_progress_virtual_sol_reserves IS NULL AND\n"
            "     curve_progress_virtual_token_reserves IS NULL AND\n"
            "     curve_progress_real_sol_reserves IS NULL AND\n"
            "     curve_progress_real_token_reserves IS NULL)\n"
            "    OR\n"
            "    (typeof(curve_progress_virtual_sol_reserves)='integer' AND\n"
            "     curve_progress_virtual_sol_reserves BETWEEN 1 AND 9223372036854775807 AND\n"
            "     typeof(curve_progress_virtual_token_reserves)='integer' AND\n"
            "     curve_progress_virtual_token_reserves BETWEEN 1 AND 9223372036854775807 AND\n"
            "     typeof(curve_progress_real_sol_reserves)='integer' AND\n"
            "     curve_progress_real_sol_reserves BETWEEN 0 AND 9223372036854775807 AND\n"
            "     typeof(curve_progress_real_token_reserves)='integer' AND\n"
            "     curve_progress_real_token_reserves BETWEEN 0 AND 9223372036854775807)\n"
            "  );",
        ),
    )

    future_path = tmp_path / "future-v7.db"
    future = sqlite3.connect(future_path)
    future.execute("CREATE TABLE sentinel(value TEXT NOT NULL)")
    future.execute("INSERT INTO sentinel(value) VALUES ('unchanged')")
    future.execute("PRAGMA user_version=7")
    future.commit()
    future_mode = future.execute("PRAGMA journal_mode").fetchone()[0]
    future_version = future.execute("PRAGMA user_version").fetchone()[0]
    future_bytes = future.serialize()
    future.close()
    assert future_mode == "delete"
    with pytest.raises(RuntimeError, match="unsupported database schema version 7"):
        store.open_db(future_path, migration_clock=lambda: 1000.0)
    future = sqlite3.connect(future_path)
    try:
        assert future.execute("PRAGMA journal_mode").fetchone()[0] == future_mode
        assert future.execute("PRAGMA user_version").fetchone()[0] == future_version
        assert future.execute("SELECT value FROM sentinel").fetchone()[0] == "unchanged"
        assert future.serialize() == future_bytes
    finally:
        future.close()

    fresh = store.open_db(tmp_path / "fresh.db", migration_clock=lambda: 1000.0)
    try:
        assert fresh.execute("PRAGMA user_version").fetchone()[0] == 6
        assert tuple(
            row[1] for row in fresh.execute("PRAGMA table_info(tokens)")
        )[-4:] == tuple(name for name, _sql in expected)
        assert store.SCHEMA_V6_ADDITIVE_COLUMNS == expected
        fresh.execute(
            "INSERT INTO tokens(mint,created_at,last_seen) VALUES ('M',1.0,1.0)"
        )
        assert tuple(fresh.execute(
            "SELECT curve_progress_virtual_sol_reserves,"
            "curve_progress_virtual_token_reserves,"
            "curve_progress_real_sol_reserves,"
            "curve_progress_real_token_reserves FROM tokens WHERE mint='M'"
        ).fetchone()) == (None, None, None, None)
        fresh.execute(
            "UPDATE tokens SET curve_progress_virtual_sol_reserves=1,"
            "curve_progress_virtual_token_reserves=2,"
            "curve_progress_real_sol_reserves=0,"
            "curve_progress_real_token_reserves=0 WHERE mint='M'"
        )
        with pytest.raises(sqlite3.IntegrityError):
            fresh.execute(
                "UPDATE tokens SET curve_progress_virtual_sol_reserves=NULL "
                "WHERE mint='M'"
            )
        fresh.rollback()
    finally:
        fresh.close()

    def make_v5(path):
        reference = store._v5_reference_connection()
        try:
            reference.row_factory = sqlite3.Row
            reference.execute("BEGIN IMMEDIATE")
            store.initialize_p3_causal_clock(reference, raw_now=1000.0)
            reference.execute("PRAGMA user_version=5")
            reference.commit()
            store._ensure_v5_performance_indexes(reference)
            destination = sqlite3.connect(path)
            try:
                reference.backup(destination)
                destination.execute("PRAGMA journal_mode=WAL")
            finally:
                destination.close()
        finally:
            reference.close()

    for count in range(5):
        path = tmp_path / f"prefix-{count}.db"
        make_v5(path)
        raw = sqlite3.connect(path)
        try:
            for _name, sql in expected[:count]:
                raw.execute(sql)
            raw.commit()
        finally:
            raw.close()
        healed = store.open_db(path, migration_clock=lambda: 1000.0)
        try:
            assert healed.execute("PRAGMA user_version").fetchone()[0] == 6
            assert store._attest_v6_curve_progress_reserve_schema(healed) is None
        finally:
            healed.close()

    path = tmp_path / "failure-v6-version-stamp.db"
    make_v5(path)
    check = sqlite3.connect(path)
    before = check.serialize()
    check.close()
    original_connect = sqlite3.connect

    class V6StampFailConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            if str(sql).strip().casefold() == "pragma user_version=6":
                raise RuntimeError("injected v6 version stamp failure")
            return super().execute(sql, parameters)

    def stamp_fail_connect(*args, **kwargs):
        return original_connect(
            *args, **kwargs, factory=V6StampFailConnection,
        )

    monkeypatch.setattr(store.sqlite3, "connect", stamp_fail_connect)
    with pytest.raises(RuntimeError, match="injected v6 version stamp failure"):
        store.open_db(path, migration_clock=lambda: 1000.0)
    monkeypatch.setattr(store.sqlite3, "connect", original_connect)
    check = original_connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 5
        assert {
            row[1] for row in check.execute("PRAGMA table_info(tokens)")
        }.isdisjoint(name for name, _sql in expected)
        assert check.serialize() == before
    finally:
        check.close()

    path = tmp_path / "legacy-progress-no-reserve-backfill.db"
    make_v5(path)
    legacy = sqlite3.connect(path)
    legacy.execute(
        "INSERT INTO tokens("
        "mint,created_at,state,curve_progress,last_seen,meta_json,"
        "bonding_curve_key,rugged"
        ") VALUES (?,?,?,?,?,?,?,?)",
        (
            "LEGACY", 10.0, "CLIMBING", 73.5, 12.0,
            "{}", "CURVE", 0,
        ),
    )
    legacy.commit()
    legacy.close()
    migrated = store.open_db(path, migration_clock=lambda: 1000.0)
    try:
        row = migrated.execute(
            "SELECT curve_progress,state,meta_json,p3_identity_ingested_at,"
            "curve_progress_observed_at,curve_progress_source_wall,"
            "curve_progress_source_boot_id,curve_progress_source_seq,"
            "curve_progress_virtual_sol_reserves,"
            "curve_progress_virtual_token_reserves,"
            "curve_progress_real_sol_reserves,"
            "curve_progress_real_token_reserves "
            "FROM tokens WHERE mint='LEGACY'"
        ).fetchone()
        assert tuple(row[:3]) == (73.5, "CLIMBING", "{}")
        assert tuple(row[3:8]) == (None, None, None, None, None)
        assert tuple(row[8:]) == (None, None, None, None)
    finally:
        migrated.close()

    path = tmp_path / "existing-v6-contiguous-prefix.db"
    make_v5(path)
    partial = sqlite3.connect(path)
    for _name, sql in expected[:3]:
        partial.execute(sql)
    partial.execute("PRAGMA user_version=6")
    partial.commit()
    before = partial.serialize()
    partial.close()
    with pytest.raises(ValueError, match="v6 curve reserve column inventory"):
        store.open_db(path, migration_clock=lambda: 1000.0)
    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 6
        actual_columns = tuple(
            row[1] for row in check.execute("PRAGMA table_info(tokens)")
        )
        assert actual_columns[-3:] == tuple(name for name, _sql in expected[:3])
        assert expected[3][0] not in actual_columns
        assert check.serialize() == before
    finally:
        check.close()

    for label, ddl in (
        ("noncontiguous", expected[1][1]),
        (
            "wrong",
            "ALTER TABLE tokens ADD COLUMN "
            "curve_progress_virtual_sol_reserves REAL",
        ),
        ("extra", "ALTER TABLE tokens ADD COLUMN unexpected INTEGER"),
    ):
        path = tmp_path / f"{label}.db"
        make_v5(path)
        raw = sqlite3.connect(path)
        raw.execute(ddl)
        raw.commit()
        before = raw.serialize()
        raw.close()
        with pytest.raises(ValueError):
            store.open_db(path, migration_clock=lambda: 1000.0)
        check = sqlite3.connect(path)
        try:
            assert check.execute("PRAGMA user_version").fetchone()[0] == 5
            assert check.serialize() == before
        finally:
            check.close()

    path = tmp_path / "pre-v5-origin-boundary-race.db"
    original_connect = sqlite3.connect

    class V5BoundaryMutationConnection(sqlite3.Connection):
        begin_count = 0

        def execute(self, sql, parameters=(), /):
            if str(sql).strip().casefold() == "begin immediate":
                self.begin_count += 1
                if self.begin_count == 2:
                    super().execute("DROP TRIGGER p3_safety_report_shape_guard")
                    super().commit()
            return super().execute(sql, parameters)

    def boundary_connect(*args, **kwargs):
        return original_connect(
            *args, **kwargs, factory=V5BoundaryMutationConnection,
        )

    monkeypatch.setattr(store.sqlite3, "connect", boundary_connect)
    with pytest.raises(ValueError, match="missing schema object"):
        store.open_db(path, migration_clock=lambda: 1000.0)
    monkeypatch.setattr(store.sqlite3, "connect", original_connect)
    check = original_connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 5
        columns = {
            row[1] for row in check.execute("PRAGMA table_info(tokens)")
        }
        assert columns.isdisjoint(name for name, _sql in expected)
        assert check.execute(
            "SELECT 1 FROM sqlite_schema "
            "WHERE type='trigger' AND name='p3_safety_report_shape_guard'"
        ).fetchone() is None
    finally:
        check.close()

    path = tmp_path / "existing-v6-malformed.db"
    make_v5(path)
    raw = sqlite3.connect(path)
    for _name, sql in expected:
        raw.execute(sql)
    raw.execute("ALTER TABLE tokens ADD COLUMN unexpected INTEGER")
    raw.execute("PRAGMA user_version=6")
    raw.commit()
    before = raw.serialize()
    raw.close()
    with pytest.raises(ValueError):
        store.open_db(path, migration_clock=lambda: 1000.0)
    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 6
        assert check.serialize() == before
    finally:
        check.close()

    path = tmp_path / "existing-v6-missing-performance-index.db"
    stamped = store.open_db(path, migration_clock=lambda: 1000.0)
    stamped.execute("DROP INDEX p3_position_current_open_idx")
    stamped.commit()
    before = stamped.serialize()
    stamped.close()
    with pytest.raises(
        RuntimeError,
        match="incompatible performance index p3_position_current_open_idx",
    ):
        store.open_db(path, migration_clock=lambda: 1000.0)
    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 6
        assert check.serialize() == before
    finally:
        check.close()

    path = tmp_path / "failure-attestation.db"
    make_v5(path)
    raw = sqlite3.connect(path)
    before = raw.serialize()
    raw.close()
    original_attest = store._attest_v6_curve_progress_reserve_schema

    def fail_after_attestation(conn, **kwargs):
        result = original_attest(conn, **kwargs)
        if not kwargs.get("allow_prefix", False):
            raise RuntimeError("injected v6 attestation failure")
        return result

    monkeypatch.setattr(
        store, "_attest_v6_curve_progress_reserve_schema", fail_after_attestation,
    )
    with pytest.raises(RuntimeError, match="injected v6 attestation"):
        store.open_db(path, migration_clock=lambda: 1000.0)
    monkeypatch.setattr(
        store, "_attest_v6_curve_progress_reserve_schema", original_attest,
    )
    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 5
        assert check.execute(
            "SELECT 1 FROM sqlite_schema "
            "WHERE type='index' AND name='p3_position_current_open_idx'"
        ).fetchone() is not None
        assert check.serialize() == before
    finally:
        check.close()


def test_v5_upgrade_from_v4_preserves_rows_without_backfill(tmp_path):
    path = tmp_path / "legacy-v4.db"
    conn = _open_v4_fixture(path)
    try:
        conn.executemany(
            "INSERT INTO tokens("
            "mint,created_at,state,curve_progress,last_seen,meta_json,"
            "bonding_curve_key,rugged) VALUES (?,?,?,?,?,?,?,?)",
            (
                (
                    "MINT-A", 10.0, "CLIMBING", 42.5, 12.0,
                    '{"creator":"CREATOR-A","name":"Alpha"}', "CURVE-A", 0,
                ),
                (
                    "MINT-B", 20.0, "DEAD", 9.5, 25.0,
                    '{"creator":"CREATOR-B","name":"Beta"}', "CURVE-B", 1,
                ),
            ),
        )
        conn.executemany(
            "INSERT INTO safety_reports("
            "id,mint,checked_at,hard_fails_json,risk_score,inputs_hash)"
            " VALUES (?,?,?,?,?,?)",
            (
                (201, "MINT-A", 30.0, "[]", 12.5, "a" * 64),
                (202, "MINT-B", 31.0, '["mint_authority_active"]', 88.0, "b" * 64),
            ),
        )
        conn.executemany(
            "INSERT INTO decisions("
            "id,at,mint,segment,action,score,feature_vector_json,"
            "safety_report_id,config_hash) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                (301, 40.0, "MINT-A", "climbing", "BUY", 91.0,
                 '{"velocity":1.5}', 201, "config-a"),
                (302, 41.0, "MINT-B", "climbing", "SKIP", 4.0,
                 '{"velocity":0.1}', 202, "config-b"),
            ),
        )
        conn.execute(
            "INSERT INTO paper_trades("
            "id,decision_id,at,mint,segment,side,qty,quote_price,fill_price,"
            "fees_json,realism_grade) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (401, 301, 50.0, "MINT-A", "climbing", "buy", 2.0, 0.4, 0.5,
             '{"curve_fee":0.01}', "B"),
        )
        conn.execute(
            "INSERT INTO outcomes(id,at,ref_kind,ref_id,pnl_sol,detail_json)"
            " VALUES (?,?,?,?,?,?)",
            (501, 60.0, "trade", 401, -0.25, '{"reason":"time_stop"}'),
        )
        conn.execute(
            "INSERT INTO regime_log(id,at,state,inputs_json) VALUES (?,?,?,?)",
            (601, 61.0, "risk_on", '{"sol_drawdown":0.01}'),
        )
        conn.execute(
            "INSERT INTO boots(id,at,config_hash,clean_shutdown) VALUES (?,?,?,?)",
            (701, 62.0, "config-a", 1),
        )
        conn.executemany(
            "INSERT INTO wallet_pnl_events("
            "id,at,wallet,mint,realized_pnl_sol,source,detail_json)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                (801, 70.0, "WALLET-A", "MINT-A", 1.25, "fixture", '{"n":1}'),
                (802, 71.0, "WALLET-A", "MINT-B", -0.25, "fixture", '{"n":2}'),
            ),
        )
        conn.execute(
            "INSERT INTO early_buyer_reads("
            "id,mint,checked_at,buyers_json,unavailable_reason,inputs_hash)"
            " VALUES (?,?,?,?,?,?)",
            (901, "MINT-A", 72.0, '["WALLET-A","WALLET-B"]', "", "c" * 64),
        )
        conn.commit()

        legacy_columns = {
            "tokens": (
                "mint", "created_at", "state", "curve_progress", "last_seen",
                "meta_json", "bonding_curve_key", "rugged",
            ),
            "safety_reports": (
                "id", "mint", "checked_at", "hard_fails_json", "risk_score",
                "inputs_hash",
            ),
            "decisions": (
                "id", "at", "mint", "segment", "action", "score",
                "feature_vector_json", "safety_report_id", "config_hash",
            ),
            "paper_trades": (
                "id", "decision_id", "at", "mint", "segment", "side", "qty",
                "quote_price", "fill_price", "fees_json", "realism_grade",
            ),
            "outcomes": ("id", "at", "ref_kind", "ref_id", "pnl_sol", "detail_json"),
            "regime_log": ("id", "at", "state", "inputs_json"),
            "boots": ("id", "at", "config_hash", "clean_shutdown"),
            "wallet_pnl_events": (
                "id", "at", "wallet", "mint", "realized_pnl_sol", "source",
                "detail_json",
            ),
            "early_buyer_reads": (
                "id", "mint", "checked_at", "buyers_json", "unavailable_reason",
                "inputs_hash",
            ),
        }
        before = {
            table: tuple(
                tuple(row) for row in conn.execute(
                    f"SELECT {','.join(columns)} FROM {table} ORDER BY rowid"
                )
            )
            for table, columns in legacy_columns.items()
        }
        sequence_before = tuple(
            tuple(row) for row in conn.execute(
                "SELECT name,seq FROM sqlite_sequence ORDER BY name"
            )
        )
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
    finally:
        conn.close()

    conn = open_db(path, migration_clock=lambda: 1000.0)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
        after = {
            table: tuple(
                tuple(row) for row in conn.execute(
                    f"SELECT {','.join(columns)} FROM {table} ORDER BY rowid"
                )
            )
            for table, columns in legacy_columns.items()
        }
        assert after == before
        assert tuple(
            tuple(row) for row in conn.execute(
                "SELECT name,seq FROM sqlite_sequence "
                "WHERE name IN ("
                f"{','.join('?' for _ in sequence_before)}"
                ") ORDER BY name",
                tuple(row[0] for row in sequence_before),
            )
        ) == sequence_before

        assert tuple(tuple(row) for row in conn.execute(
            "SELECT id,safety_report_id FROM decisions ORDER BY id"
        )) == ((301, 201), (302, 202))
        assert tuple(tuple(row) for row in conn.execute(
            "SELECT id,decision_id FROM paper_trades ORDER BY id"
        )) == ((401, 301),)
        assert tuple(tuple(row) for row in conn.execute(
            "SELECT id,ref_kind,ref_id FROM outcomes ORDER BY id"
        )) == ((501, "trade", 401),)

        for table, columns in (
            ("tokens", (
                "p3_identity_ingested_at", "curve_progress_observed_at",
                "curve_progress_source_wall", "curve_progress_source_boot_id",
                "curve_progress_source_seq",
            )),
            ("paper_trades", (
                "canonical_recheck_id", "canonical_proof_hash",
                "p3_entry_execution_id",
            )),
            ("outcomes", ("p3_exit_trade_id",)),
            ("early_buyer_reads", ("safety_report_id",)),
        ):
            assert all(
                value is None
                for row in conn.execute(f"SELECT {','.join(columns)} FROM {table}")
                for value in row
            )

        for table in (
            "holder_evidence",
            "creator_reputation_events",
            "creator_reputation_current",
            "canonical_observations",
            "canonical_generations",
            "canonical_rechecks",
            "paper_entry_executions",
            "p3_position_current",
            "canonical_pending_current",
        ):
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0

        assert tuple(tuple(row) for row in conn.execute(
            "SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id "
            "FROM wallet_pnl_summary"
        )) == (("WALLET-A", 2, 1.0, 71.0, 802),)
        assert tuple(tuple(row) for row in conn.execute(
            "SELECT singleton,last_wall FROM p3_causal_clock"
        )) == ((1, 1000.0),)
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_store_safety_and_early_buyer_fixture_hashes_are_v5_valid():
    import ast
    import re
    from pathlib import Path

    fixture_writers = {"record_early_buyer_read", "save_safety_report"}
    fixture_hashes = []
    tree = ast.parse(Path(__file__).read_text())
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        writer = call.func.id if isinstance(call.func, ast.Name) else None
        if writer not in fixture_writers:
            continue
        inputs_hash = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "inputs_hash"),
            None,
        )
        assert isinstance(inputs_hash, ast.Constant) and isinstance(inputs_hash.value, str), (
            f"{writer} fixture at line {call.lineno} must use a literal inputs_hash"
        )
        fixture_hashes.append((call.lineno, inputs_hash.value))

    assert fixture_hashes
    assert [
        (line, value)
        for line, value in fixture_hashes
        if re.fullmatch(r"[0-9a-f]{64}", value) is None
    ] == []


@pytest.mark.parametrize("table", EVIDENCE_TABLES)
def test_p2_evidence_tables_reject_update_and_delete(tmp_path, table):
    from memebot.store import record_early_buyer_read, record_wallet_pnl_event
    conn = open_db(tmp_path / "t.db")
    if table == "wallet_pnl_events":
        record_wallet_pnl_event(conn, at=1.0, wallet="W", mint="M", realized_pnl_sol=1.5,
                                source="test", detail={"x": 1})
        set_clause = "at = 2"
    else:
        record_early_buyer_read(conn, mint="M", checked_at=1.0, buyers=("W1", "W2"),
                                unavailable_reason="", inputs_hash=(
                                    "2a86383aeb88f6303044a916ea2e50673d44a6c24f8ad38a4b214212d658defd"
                                ))
        set_clause = "checked_at = 2"
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute(f"UPDATE {table} SET {set_clause}")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute(f"DELETE FROM {table}")


def test_smart_wallet_snapshot_is_predecision_and_thresholded(tmp_path):
    from memebot.store import record_wallet_pnl_event, smart_wallets_snapshot
    conn = open_db(tmp_path / "t.db")
    record_wallet_pnl_event(conn, at=10.0, wallet="SMART", mint="A", realized_pnl_sol=0.7,
                            source="test", detail={})
    record_wallet_pnl_event(conn, at=20.0, wallet="SMART", mint="B", realized_pnl_sol=0.6,
                            source="test", detail={})
    record_wallet_pnl_event(conn, at=30.0, wallet="LOOKAHEAD", mint="C", realized_pnl_sol=99.0,
                            source="future", detail={})
    record_wallet_pnl_event(conn, at=10.0, wallet="ONEHIT", mint="D", realized_pnl_sol=9.0,
                            source="test", detail={})

    snap = smart_wallets_snapshot(conn, before_at=25.0, min_events=2, min_realized_pnl_sol=1.0)

    assert set(snap) == {"SMART"}
    assert snap["SMART"]["events"] == 2
    assert snap["SMART"]["realized_pnl_sol"] == pytest.approx(1.3)


def test_early_buyer_read_roundtrip(tmp_path):
    import json
    from memebot.store import latest_early_buyer_read, record_early_buyer_read
    conn = open_db(tmp_path / "t.db")
    row_id = record_early_buyer_read(conn, mint="M", checked_at=1.0,
                                     buyers=("W1", "W2"), unavailable_reason="",
                                     inputs_hash=(
                                         "220a7fcf667bbe5d0a38035faf3b028fab66b8f2d4263dc3c6da20024db6dd0f"
                                     ))
    row = latest_early_buyer_read(conn, "M")
    assert row["id"] == row_id
    assert json.loads(row["buyers_json"]) == ["W1", "W2"]
    assert row["unavailable_reason"] == ""


def test_save_and_load_safety_report(tmp_path):
    from memebot.store import latest_safety_report, save_safety_report, upsert_token
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    save_safety_report(conn, mint="M1", raw_completed_at=5.0, segment="CLIMBING",
                       hard_fails=["mint_authority_active"], risk_score=20.0,
                       results_json='[]', inputs_hash=(
                           "f022b7bd45198354e31ac34544933a25dbfabe975dfc71e68d3628bf480384ee"
                       ))
    rep = latest_safety_report(conn, "M1")
    assert rep["hard_fails_json"] == '["mint_authority_active"]' and rep["risk_score"] == 20.0


def test_latest_safety_report_follows_append_order_not_reordered_clock(tmp_path):
    from memebot.store import latest_safety_report, save_safety_report, upsert_token

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=1.0)
    save_safety_report(
        conn, mint="M", raw_completed_at=10.0, segment="CLIMBING", hard_fails=[],
        risk_score=0.0, results_json="[]",
        inputs_hash="5555555555555555555555555555555555555555555555555555555555555555",
    )
    latest_id = save_safety_report(
        conn, mint="M", raw_completed_at=5.0, segment="CLIMBING", hard_fails=[],
        risk_score=100.0, results_json="[]",
        inputs_hash="6666666666666666666666666666666666666666666666666666666666666666",
    )

    report = latest_safety_report(conn, "M")
    assert report["id"] == latest_id
    assert report["risk_score"] == 100.0


def test_store_fixtures_use_terminal_reputation_writer():
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


def test_mark_rugged_helper_is_removed():
    import ast
    from pathlib import Path

    import memebot.store as store

    tree = ast.parse(Path(store.__file__).read_text())
    definitions = [
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "mark_rugged"
    ]
    module_exports = getattr(store, "__all__", ())

    assert "mark_rugged" not in vars(store)
    assert not hasattr(store, "mark_rugged")
    assert isinstance(module_exports, (list, tuple))
    assert all(isinstance(name, str) for name in module_exports)
    assert "mark_rugged" not in module_exports
    assert definitions == []


def test_mark_rugged_and_creator_rug_history(tmp_path):
    from memebot.store import (
        creator_rug_history,
        set_terminal_state_with_reputation,
        upsert_token,
    )
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="R1", created_at=1.0)
    conn.execute("UPDATE tokens SET meta_json = ? WHERE mint = ?",
                 ('{"creator": "DEV9"}', "R1"))
    conn.commit()
    set_terminal_state_with_reputation(
        conn,
        mint="R1",
        outcome="RUGGED",
        raw_processed_at=2.0,
        creator="DEV9",
        creator_conflicted=False,
    )
    row = conn.execute("SELECT rugged, state FROM tokens WHERE mint='R1'").fetchone()
    assert row[0] == 1 and row[1] == "DEAD"
    assert creator_rug_history(conn, "DEV9") == 1     # DEV9 has 1 prior rugged token
    assert creator_rug_history(conn, "CLEAN") == 0


def test_pending_safety_pass_selector_is_bounded_stable_and_fail_closed(tmp_path):
    import math

    from memebot.store import (pending_safety_passes_for_scoring, record_decision,
                               save_safety_report,
                               set_terminal_state_with_reputation,
                               set_token_state, upsert_token)

    conn = open_db(tmp_path / "t.db", migration_clock=lambda: 1.0)

    def report(mint, checked_at, hard_fails=()):
        return save_safety_report(
            conn, mint=mint, raw_completed_at=checked_at, segment="CLIMBING",
            hard_fails=list(hard_fails), risk_score=10.0, results_json="[]",
            inputs_hash="f6fb407a2f292f57d3b91f57a3fc0825dc859bdc7601b1ec004b5d632bae312a",
        )

    for mint, checked_at in (("OLD", 10.0), ("NEW", 20.0), ("THIRD", 5.0),
                             ("HARD", 30.0), ("RUG", 40.0), ("GRAD", 50.0),
                             ("DONE", 60.0)):
        upsert_token(conn, mint=mint, created_at=1.0)
        set_token_state(conn, mint, "CLIMBING")
        report(mint, checked_at)

    report("HARD", 31.0, ("rug",))  # latest report supersedes the older pass
    conn.execute("UPDATE tokens SET rugged=1 WHERE mint='RUG'")
    conn.commit()
    set_terminal_state_with_reputation(
        conn, mint="GRAD", outcome="GRADUATED", raw_processed_at=52.0,
        creator=None, creator_conflicted=False,
    )
    record_decision(conn, at=61.0, mint="DONE", segment="CLIMBING", action="SKIP",
                    score=1.0, feature_vector={}, config_hash="cfg")

    page = pending_safety_passes_for_scoring(
        conn, limit=2, scan_cap=20, before_id=None, now=100.0, stale_after_s=100.0,
    )

    new_t = math.nextafter(20.0, math.inf)
    third_t = math.nextafter(new_t, math.inf)
    assert [(row["mint"], row["checked_at"]) for row in page.rows] == [
        ("THIRD", third_t),
        ("NEW", new_t),
    ]
    assert page.next_before_id == 2
    assert page.raw_overflow is False
    assert page.exhausted is False

    final_page = pending_safety_passes_for_scoring(
        conn, limit=2, scan_cap=20, before_id=page.next_before_id,
        now=100.0, stale_after_s=100.0,
    )
    assert [row["mint"] for row in final_page.rows] == ["OLD"]
    assert final_page.exhausted is True


def test_pending_safety_pass_selector_plan_is_indexed_and_work_bounded(tmp_path):
    import re

    from memebot.store import (pending_safety_passes_for_scoring, save_safety_report,
                               set_token_state, upsert_token)

    db_path = tmp_path / "populated-v5.db"
    conn = open_db(db_path)
    conn.execute("DROP INDEX IF EXISTS safety_reports_pending_scoring_idx")
    conn.execute("DROP INDEX IF EXISTS decisions_climbing_mint_idx")
    conn.execute("PRAGMA user_version=5")
    conn.commit()
    conn.close()

    # Reopening an already-v5 database must idempotently install performance-only DDL.
    conn = open_db(db_path)

    # Sparse startup has no synthetic work and still exercises the installed indexes.
    empty = pending_safety_passes_for_scoring(
        conn, limit=2, scan_cap=2, before_id=None, now=100.0, stale_after_s=300.0,
    )
    assert empty.rows == ()
    assert empty.exhausted is True

    def add_candidate(mint, checked_at):
        upsert_token(conn, mint=mint, created_at=1.0)
        set_token_state(conn, mint, "CLIMBING")
        return save_safety_report(
            conn, mint=mint, raw_completed_at=checked_at, segment="CLIMBING",
            hard_fails=[], risk_score=10.0, results_json="[]",
            inputs_hash="f6fb407a2f292f57d3b91f57a3fc0825dc859bdc7601b1ec004b5d632bae312a",
        )

    ids = {mint: add_candidate(mint, checked_at) for mint, checked_at in (
        ("A", 40.0), ("B", 10.0), ("C", 30.0), ("D", 20.0),
    )}

    # Make both indexes look prohibitively expensive. Forced INDEXED BY clauses are
    # still required, and the raw id order must not gain a temporary sort.
    conn.execute("ANALYZE")
    conn.execute(
        "UPDATE sqlite_stat1 SET stat='1000000000 1000000000' "
        "WHERE idx IN ('safety_reports_pending_scoring_idx',"
        " 'decisions_climbing_mint_idx')"
    )
    conn.execute("ANALYZE sqlite_schema")

    traced = []
    conn.set_trace_callback(traced.append)
    page = pending_safety_passes_for_scoring(
        conn, limit=2, scan_cap=2, before_id=None, now=100.0, stale_after_s=300.0,
    )
    conn.set_trace_callback(None)

    selector_statements = [
        statement for statement in traced
        if statement.lstrip().upper().startswith(("SELECT ", "WITH "))
    ]
    assert len(selector_statements) == 1
    selector_sql = selector_statements[0]
    plan = [
        row["detail"]
        for row in conn.execute(f"EXPLAIN QUERY PLAN {selector_sql}")
    ]
    plan_text = "\n".join(plan)

    assert [row["mint"] for row in page.rows] == ["D", "C"]
    assert [row["id"] for row in page.rows] == [ids["D"], ids["C"]]
    assert page.raw_overflow is True
    assert page.exhausted is False
    assert page.next_before_id == ids["C"]
    assert re.search(r"ORDER BY\s+sr\.id\s+DESC\s+LIMIT\s+3\b", selector_sql, re.I)
    assert not re.search(r"LIMIT\s+(?!1\b|3\b)\d+", selector_sql, re.I)
    assert "INDEXED BY safety_reports_pending_scoring_idx" in selector_sql
    assert "INDEXED BY decisions_climbing_mint_idx" in selector_sql
    plan_violations = [
        detail for detail in plan
        if re.search(r"\bSCAN (?:t|d)\b", detail) or "USE TEMP B-TREE" in detail
    ]
    required_plan_shapes = (
        r"\bSEARCH sr USING (?:COVERING )?INDEX safety_reports_pending_scoring_idx\b|"
        r"\bSCAN sr USING (?:COVERING )?INDEX safety_reports_pending_scoring_idx\b",
        r"\bSEARCH latest USING (?:COVERING )?INDEX safety_reports_mint_latest_idx\b",
        r"\bSEARCH d USING (?:COVERING )?INDEX decisions_climbing_mint_idx\b",
        r"\bSEARCH t USING (?:COVERING )?INDEX sqlite_autoindex_tokens_1\b",
    )
    for required_shape in required_plan_shapes:
        if not any(re.search(required_shape, detail) for detail in plan):
            plan_violations.append(f"MISSING PLAN SHAPE {required_shape}")
    if not any(detail.startswith("MATERIALIZE ") for detail in plan):
        plan_violations.append("MISSING MATERIALIZED SAFETY PAGE")
    assert plan_violations == [], plan_text
    assert {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='index' AND name IN (?,?)",
            ("safety_reports_pending_scoring_idx", "decisions_climbing_mint_idx"),
        )
    } == {"safety_reports_pending_scoring_idx", "decisions_climbing_mint_idx"}
    conn.close()

    restarted = open_db(db_path)
    older = pending_safety_passes_for_scoring(
        restarted, limit=2, scan_cap=2, before_id=page.next_before_id,
        now=100.0, stale_after_s=300.0,
    )
    assert [row["mint"] for row in older.rows] == ["B", "A"]
    assert older.exhausted is True


def test_pending_safety_pass_selector_vm_work_is_raw_page_bounded(tmp_path):
    from memebot.store import pending_safety_passes_for_scoring

    def query_steps(row_count):
        conn = open_db(tmp_path / f"work-{row_count}.db")
        tokens = [
            (f"M{index:05d}", 1.0, 1.0, "GRADUATED", "")
            for index in range(row_count)
        ]
        conn.executemany(
            "INSERT INTO tokens(mint,created_at,last_seen,state,bonding_curve_key) "
            "VALUES (?,?,?,?,?)",
            tokens,
        )
        conn.executemany(
            "INSERT INTO safety_reports(mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES (?,100,'[]',10,?)",
            ((mint, "7" * 64) for mint, *_ in tokens),
        )
        conn.commit()
        def measure(before_id):
            steps = 0

            def count_step():
                nonlocal steps
                steps += 1
                return 0

            conn.set_progress_handler(count_step, 1)
            try:
                page = pending_safety_passes_for_scoring(
                    conn, limit=2, scan_cap=8, before_id=before_id,
                    now=100.0, stale_after_s=300.0,
                )
            finally:
                conn.set_progress_handler(None, 0)
            assert page.rows == ()
            return steps, page

        newest_steps, newest_page = measure(None)
        deep_steps, deep_page = measure(5)
        assert newest_page.raw_overflow is True
        assert newest_page.exhausted is False
        assert deep_page.raw_overflow is False
        assert deep_page.exhausted is True
        return newest_steps, deep_steps

    small_newest, small_deep = query_steps(100)
    large_newest, large_deep = query_steps(10_000)

    assert large_newest <= small_newest * 2
    assert large_deep <= small_deep * 2


@pytest.mark.parametrize(
    ("index_name", "replacement_sql"),
    (
        (
            "safety_reports_pending_scoring_idx",
            "CREATE INDEX safety_reports_pending_scoring_idx "
            "ON safety_reports(checked_at DESC) "
            "WHERE json_array_length(hard_fails_json)=0",
        ),
        (
            "safety_reports_pending_scoring_idx",
            "CREATE INDEX safety_reports_pending_scoring_idx "
            "ON safety_reports(id ASC) "
            "WHERE json_array_length(hard_fails_json)=0",
        ),
        (
            "safety_reports_pending_scoring_idx",
            "CREATE INDEX safety_reports_pending_scoring_idx "
            "ON safety_reports(id DESC) "
            "WHERE json_array_length(hard_fails_json)>0",
        ),
        (
            "safety_reports_pending_scoring_idx",
            "CREATE INDEX safety_reports_pending_scoring_idx "
            "ON decisions(id DESC) WHERE segment='CLIMBING'",
        ),
        (
            "decisions_climbing_mint_idx",
            "CREATE INDEX decisions_climbing_mint_idx "
            "ON decisions(segment) WHERE segment='CLIMBING'",
        ),
        (
            "decisions_climbing_mint_idx",
            "CREATE INDEX decisions_climbing_mint_idx "
            "ON decisions(mint DESC) WHERE segment='CLIMBING'",
        ),
        (
            "decisions_climbing_mint_idx",
            "CREATE INDEX decisions_climbing_mint_idx "
            "ON decisions(mint) WHERE segment='TRENDING'",
        ),
        (
            "decisions_climbing_mint_idx",
            "CREATE INDEX decisions_climbing_mint_idx "
            "ON decisions(mint) WHERE segment='climbing'",
        ),
    ),
)
def test_open_db_rejects_incompatible_recovery_performance_indexes(
    tmp_path, index_name, replacement_sql,
):
    db_path = tmp_path / f"incompatible-{index_name}.db"
    conn = open_db(db_path)
    conn.execute(f"DROP INDEX {index_name}")
    conn.execute(replacement_sql)
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match=f"incompatible performance index {index_name}"):
        open_db(db_path)


def test_watch_startup_rejects_unmatched_canonical_buy_but_exempts_legacy_and_terminal_graphs():
    import hashlib
    import json

    import memebot.store as store

    def make_connection():
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.create_function(
            "p3_fee_sum", 1, store.p3_fee_sum_json, deterministic=True,
        )
        conn.executescript(
            """CREATE TABLE decisions (
  id INTEGER PRIMARY KEY,
  at REAL,
  mint TEXT,
  action TEXT,
  feature_vector_json TEXT,
  safety_report_id INTEGER
);
CREATE INDEX decisions_p3_canonical_buy_idx ON decisions(id)
WHERE action='BUY' AND CASE WHEN json_valid(feature_vector_json)
THEN json_extract(feature_vector_json,'$.canonical.status') END='CANONICAL';
CREATE TABLE canonical_observations (
  id INTEGER PRIMARY KEY,
  decision_id INTEGER,
  mint TEXT,
  is_subject INTEGER,
  is_canonical INTEGER,
  eligible INTEGER,
  UNIQUE(decision_id,mint)
);
CREATE INDEX canonical_observations_decision_idx
  ON canonical_observations(decision_id,id);
CREATE TABLE safety_reports (
  id INTEGER PRIMARY KEY,
  mint TEXT,
  checked_at REAL,
  hard_fails_json TEXT
);
CREATE TABLE canonical_rechecks (
  id INTEGER PRIMARY KEY,
  decision_id INTEGER,
  attempt INTEGER,
  rechecked_at REAL,
  status TEXT,
  reason TEXT,
  canonical_mint TEXT,
  causal_target_report_id INTEGER,
  latest_target_report_id INTEGER,
  prior_inputs_hash TEXT,
  recheck_inputs_hash TEXT,
  payload_json TEXT
);
CREATE UNIQUE INDEX canonical_rechecks_decision_attempt
  ON canonical_rechecks(decision_id,attempt);
CREATE INDEX canonical_rechecks_decision_idx
  ON canonical_rechecks(decision_id,attempt);
CREATE INDEX canonical_rechecks_decision_status_idx
  ON canonical_rechecks(decision_id,status,id);
CREATE TABLE paper_trades (
  id INTEGER PRIMARY KEY,
  decision_id INTEGER,
  at REAL,
  mint TEXT,
  segment TEXT,
  side TEXT,
  qty REAL,
  quote_price REAL,
  fill_price REAL,
  fees_json TEXT,
  realism_grade TEXT,
  canonical_recheck_id INTEGER,
  canonical_proof_hash TEXT,
  p3_entry_execution_id INTEGER
);
CREATE TABLE paper_entry_executions (
  id INTEGER PRIMARY KEY,
  decision_id INTEGER UNIQUE,
  at REAL,
  status TEXT,
  reason TEXT,
  planned_size_sol REAL,
  canonical_recheck_id INTEGER,
  paper_trade_id INTEGER UNIQUE
);"""
        )
        conn.commit()
        return conn

    def feature(status="CANONICAL", planned_size_sol=10.0):
        return json.dumps(
            {"canonical": {
                "status": status,
                "planned_size_sol": planned_size_sol,
                "inputs_hash": "b" * 64,
            }},
            separators=(",", ":"),
        )

    def add_decision(conn, decision_id, *, action="BUY", status="CANONICAL"):
        report_id = 500 + decision_id
        conn.execute(
            "INSERT INTO safety_reports VALUES (?,?,?,?)",
            (report_id, f"M{decision_id}", 1.0, "[]"),
        )
        conn.execute(
            "INSERT INTO decisions VALUES (?,?,?,?,?,?)",
            (
                decision_id, 2.0, f"M{decision_id}", action, feature(status),
                report_id,
            ),
        )

    def add_observation(conn, decision_id):
        conn.execute(
            "INSERT INTO canonical_observations VALUES (?,?,?,?,?,?)",
            (1_000_000 + decision_id, decision_id, f"M{decision_id}", 1, 1, 1),
        )

    def add_recheck(conn, decision_id, *, status, reason, recheck_id=None):
        recheck_id = recheck_id or 100 + decision_id
        canonical_mint = f"M{decision_id}" if status == "PASS" else None
        fill_event_at = 2.5
        payload_json = json.dumps(
            {
                "attempt": 1,
                "causal_target_report_id": 500 + decision_id,
                "decision_id": decision_id,
                "fill_event_at": fill_event_at,
                "latest_target_report_id": 500 + decision_id,
                "prior_inputs_hash": "b" * 64,
                "rechecked_at": 3.0,
                "target_snapshot": {
                    "liquidity_sol": 42.5,
                    "progress_pct": 50.0,
                    "real_sol_reserves": 42_500_000_000,
                    "real_token_reserves": 400_000_000_000_000,
                    "spot_price_sol": 0.000001,
                    "t_mono": 9.0,
                    "t_wall": fill_event_at,
                    "virtual_sol_reserves": 70_000_000_000,
                    "virtual_token_reserves": 70_000_000_000_000,
                },
                "trigger": "curve_progress",
                "trigger_report_id": None,
                "verdict": {
                    "canonical_mint": canonical_mint,
                    "inputs_hash": "d" * 64,
                    "reason": reason,
                    "status": "CANONICAL" if status == "PASS" else "SUPPRESSED",
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        proof_hash = hashlib.sha256(payload_json.encode()).hexdigest()
        conn.execute(
            "INSERT INTO canonical_rechecks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                recheck_id, decision_id, 1, 3.0, status, reason,
                canonical_mint,
                500 + decision_id, 500 + decision_id, "b" * 64,
                proof_hash, payload_json,
            ),
        )
        return recheck_id, proof_hash

    def add_filled(conn, decision_id):
        add_decision(conn, decision_id)
        add_observation(conn, decision_id)
        recheck_id, proof_hash = add_recheck(
            conn, decision_id, status="PASS", reason="canonical_selected",
        )
        trade_id = 200 + decision_id
        conn.execute(
            "INSERT INTO paper_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                trade_id, decision_id, 4.0, f"M{decision_id}", "CLIMBING",
                "buy", 2.0, 5.0, 5.0, "{}", "B", recheck_id, proof_hash,
                None,
            ),
        )
        conn.execute(
            "INSERT INTO paper_entry_executions VALUES (?,?,?,?,?,?,?,?)",
            (
                300 + decision_id, decision_id, 4.0, "FILLED", "filled", 10.0,
                recheck_id, trade_id,
            ),
        )

    def add_cancelled(conn, decision_id):
        add_decision(conn, decision_id)
        add_observation(conn, decision_id)
        recheck_id, _ = add_recheck(
            conn, decision_id, status="CANCEL", reason="safety_flip",
        )
        conn.execute(
            "INSERT INTO paper_entry_executions VALUES (?,?,?,?,?,?,?,?)",
            (
                300 + decision_id, decision_id, 4.0, "CANCELLED", "safety_flip",
                10.0, recheck_id, None,
            ),
        )

    def add_abandoned(conn, decision_id, *, after_pass):
        add_decision(conn, decision_id)
        add_observation(conn, decision_id)
        recheck_id = None
        reason = "restart_before_fill"
        at = 3.0
        if after_pass:
            recheck_id, _ = add_recheck(
                conn, decision_id, status="PASS", reason="canonical_selected",
            )
            reason = "restart_after_pass"
            at = 4.0
        conn.execute(
            "INSERT INTO paper_entry_executions VALUES (?,?,?,?,?,?,?,?)",
            (
                300 + decision_id, decision_id, at, "ABANDONED", reason, 10.0,
                recheck_id, None,
            ),
        )

    valid = make_connection()
    add_decision(valid, 1, status="LEGACY")
    add_decision(valid, 2, action="SKIP")
    add_filled(valid, 10)
    add_cancelled(valid, 20)
    add_abandoned(valid, 30, after_pass=False)
    add_abandoned(valid, 40, after_pass=True)
    valid.commit()
    before_changes = valid.total_changes
    assert valid.in_transaction is False

    store.assert_p3_buy_terminal_coverage(valid)

    assert valid.total_changes == before_changes
    assert valid.in_transaction is False

    cancel_before_filled = make_connection()
    add_filled(cancel_before_filled, 10)
    pass_payload = json.loads(cancel_before_filled.execute(
        "SELECT payload_json FROM canonical_rechecks WHERE decision_id=10"
    ).fetchone()[0])
    pass_payload["attempt"] = 2
    pass_payload_json = json.dumps(
        pass_payload, sort_keys=True, separators=(",", ":"),
    )
    pass_proof_hash = hashlib.sha256(pass_payload_json.encode()).hexdigest()
    cancel_before_filled.execute(
        "UPDATE canonical_rechecks SET attempt=2,recheck_inputs_hash=?,payload_json=? "
        "WHERE decision_id=10",
        (pass_proof_hash, pass_payload_json),
    )
    cancel_before_filled.execute(
        "UPDATE paper_trades SET canonical_proof_hash=? WHERE decision_id=10",
        (pass_proof_hash,),
    )
    cancel_payload = dict(pass_payload)
    cancel_payload["attempt"] = 1
    cancel_payload["rechecked_at"] = 2.75
    cancel_payload["verdict"] = dict(pass_payload["verdict"])
    cancel_payload["verdict"].update({
        "canonical_mint": None,
        "reason": "canonical_cancelled",
        "status": "SUPPRESSED",
    })
    cancel_payload_json = json.dumps(
        cancel_payload, sort_keys=True, separators=(",", ":"),
    )
    cancel_before_filled.execute(
        "INSERT INTO canonical_rechecks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            109, 10, 1, 2.75, "CANCEL", "canonical_cancelled", None,
            510, 510, "b" * 64,
            hashlib.sha256(cancel_payload_json.encode()).hexdigest(),
            cancel_payload_json,
        ),
    )
    cancel_before_filled.commit()
    before_changes = cancel_before_filled.total_changes
    assert cancel_before_filled.in_transaction is False
    try:
        store.assert_p3_buy_terminal_coverage(cancel_before_filled)
    except RuntimeError as exc:
        assert str(exc) == (
            "WATCH startup terminal coverage invalid for decision_id=10"
        ), "cancel-before-filled"
    else:
        pytest.fail("cancel-before-filled terminal graph was accepted")
    assert cancel_before_filled.total_changes == before_changes, "cancel-before-filled"
    assert cancel_before_filled.in_transaction is False, "cancel-before-filled"

    missing = make_connection()
    add_decision(missing, 5)
    add_observation(missing, 5)
    add_decision(missing, 6)
    add_observation(missing, 6)
    missing.commit()
    with pytest.raises(
        RuntimeError,
        match=r"^WATCH startup terminal coverage invalid for decision_id=5$",
    ):
        store.assert_p3_buy_terminal_coverage(missing)

    wrong_pass_reason = make_connection()
    add_filled(wrong_pass_reason, 10)
    wrong_pass_reason.execute(
        "UPDATE canonical_rechecks SET reason='other'"
    )
    wrong_pass_reason.commit()
    try:
        store.assert_p3_buy_terminal_coverage(wrong_pass_reason)
    except RuntimeError as exc:
        assert str(exc) == (
            "WATCH startup terminal coverage invalid for decision_id=10"
        )
    else:
        pytest.fail("wrong-pass-reason terminal graph was accepted")

    cancelled_unresolved = make_connection()
    add_cancelled(cancelled_unresolved, 20)
    unresolved_payload = json.loads(cancelled_unresolved.execute(
        "SELECT payload_json FROM canonical_rechecks"
    ).fetchone()[0])
    unresolved_payload["verdict"]["status"] = "UNRESOLVED"
    unresolved_payload_json = json.dumps(
        unresolved_payload, sort_keys=True, separators=(",", ":"),
    )
    cancelled_unresolved.execute(
        "UPDATE canonical_rechecks SET payload_json=?,recheck_inputs_hash=?",
        (
            unresolved_payload_json,
            hashlib.sha256(unresolved_payload_json.encode()).hexdigest(),
        ),
    )
    cancelled_unresolved.commit()
    store.assert_p3_buy_terminal_coverage(cancelled_unresolved)

    bound_reduced_payload_json = json.dumps(
        {
            "attempt": 1,
            "causal_target_report_id": 510,
            "decision_id": 10,
            "latest_target_report_id": 510,
            "prior_inputs_hash": "b" * 64,
            "rechecked_at": 3.0,
            "verdict": {
                "canonical_mint": "M10",
                "inputs_hash": "d" * 64,
                "reason": "canonical_selected",
                "status": "CANONICAL",
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    bound_reduced_payload_hash = hashlib.sha256(
        bound_reduced_payload_json.encode()
    ).hexdigest()
    bound_reduced_payload = make_connection()
    add_filled(bound_reduced_payload, 10)
    bound_reduced_payload.execute(
        "UPDATE canonical_rechecks SET payload_json=?,recheck_inputs_hash=?",
        (bound_reduced_payload_json, bound_reduced_payload_hash),
    )
    bound_reduced_payload.execute(
        "UPDATE paper_trades SET canonical_proof_hash=?",
        (bound_reduced_payload_hash,),
    )
    bound_reduced_payload.commit()
    try:
        store.assert_p3_buy_terminal_coverage(bound_reduced_payload)
    except RuntimeError as exc:
        assert str(exc) == (
            "WATCH startup terminal coverage invalid for decision_id=10"
        )
    else:
        pytest.fail("bound-reduced-recheck-payload terminal graph was accepted")

    huge_snapshot_numeric = make_connection()
    add_filled(huge_snapshot_numeric, 10)
    huge_payload = json.loads(huge_snapshot_numeric.execute(
        "SELECT payload_json FROM canonical_rechecks"
    ).fetchone()[0])
    huge_payload["target_snapshot"]["liquidity_sol"] = 10**1000
    huge_payload_json = json.dumps(
        huge_payload, sort_keys=True, separators=(",", ":"),
    )
    huge_payload_hash = hashlib.sha256(huge_payload_json.encode()).hexdigest()
    huge_snapshot_numeric.execute(
        "UPDATE canonical_rechecks SET payload_json=?,recheck_inputs_hash=?",
        (huge_payload_json, huge_payload_hash),
    )
    huge_snapshot_numeric.execute(
        "UPDATE paper_trades SET canonical_proof_hash=?",
        (huge_payload_hash,),
    )
    huge_snapshot_numeric.commit()
    try:
        store.assert_p3_buy_terminal_coverage(huge_snapshot_numeric)
    except RuntimeError as exc:
        assert str(exc) == (
            "WATCH startup terminal coverage invalid for decision_id=10"
        )
    else:
        pytest.fail("huge-snapshot-numeric terminal graph was accepted")

    reduced_payload_hash = hashlib.sha256(b"{}").hexdigest()
    mutations = (
        ("bad-status", "UPDATE paper_entry_executions SET status='BROKEN'"),
        ("cross-recheck", "UPDATE canonical_rechecks SET decision_id=999"),
        ("missing-recheck", "DELETE FROM canonical_rechecks"),
        ("missing-trade", "DELETE FROM paper_trades"),
        ("wrong-side", "UPDATE paper_trades SET side='sell'"),
        ("wrong-mint", "UPDATE paper_trades SET mint='OTHER'"),
        ("wrong-decision", "UPDATE paper_trades SET decision_id=999"),
        ("wrong-trade-link", "UPDATE paper_entry_executions SET paper_trade_id=999"),
        ("better-than-quote", "UPDATE paper_trades SET fill_price=4.0"),
        ("bad-trade-shape", "UPDATE paper_trades SET fees_json='[]'"),
        ("planned-mismatch", "UPDATE paper_entry_executions SET planned_size_sol=9.0"),
        ("nonqualifying-observation", "UPDATE canonical_observations SET is_subject=0"),
        ("noncanonical-observation", "UPDATE canonical_observations SET is_canonical=0"),
        ("ineligible-observation", "UPDATE canonical_observations SET eligible=0"),
        ("cross-causal-report", "UPDATE canonical_rechecks SET causal_target_report_id=999"),
        ("wrong-latest-report", "UPDATE canonical_rechecks SET latest_target_report_id=999"),
        ("wrong-prior-proof", "UPDATE canonical_rechecks SET prior_inputs_hash='c' || substr(prior_inputs_hash,2)"),
        ("recheck-payload-digest", "UPDATE canonical_rechecks SET payload_json='{\"changed\":1}'"),
        ("wrong-pass-reason", "UPDATE canonical_rechecks SET reason='other'"),
        ("nul-pass-reason", "UPDATE canonical_rechecks SET reason=reason || char(0)"),
        (
            "reduced-recheck-payload",
            "UPDATE canonical_rechecks SET payload_json='{}',"
            f"recheck_inputs_hash='{reduced_payload_hash}';"
            "UPDATE paper_trades SET "
            f"canonical_proof_hash='{reduced_payload_hash}'",
        ),
        ("nul-segment", "UPDATE paper_trades SET segment=char(0) || 'CLIMBING'"),
        ("nul-grade", "UPDATE paper_trades SET realism_grade=char(0) || 'B'"),
        (
            "nul-segment-suffix",
            "UPDATE paper_trades SET segment='CLIMBING' || char(0) || printf('%064d',0)",
        ),
        (
            "nul-grade-suffix",
            "UPDATE paper_trades SET realism_grade='B' || char(0) || printf('%032d',0)",
        ),
    )
    for label, mutation in mutations:
        malformed = make_connection()
        add_filled(malformed, 10)
        malformed.executescript(mutation)
        malformed.commit()
        before_changes = malformed.total_changes
        with pytest.raises(
            RuntimeError,
            match=r"^WATCH startup terminal coverage invalid for decision_id=10$",
        ):
            store.assert_p3_buy_terminal_coverage(malformed)
        assert malformed.total_changes == before_changes, label
        assert malformed.in_transaction is False, label

    def observation_audit_steps(observation_count):
        measured = make_connection()
        add_filled(measured, 10)
        measured.executemany(
            "INSERT INTO canonical_observations VALUES (?,?,?,?,?,?)",
            (
                (row_id, 10, f"COHORT{row_id}", 0, 0, 1)
                for row_id in range(1, observation_count + 1)
            ),
        )
        measured.commit()
        steps = 0

        def count_step():
            nonlocal steps
            steps += 1
            return 0

        measured.set_progress_handler(count_step, 1)
        try:
            store.assert_p3_buy_terminal_coverage(measured)
        finally:
            measured.set_progress_handler(None, 0)
            measured.close()
        return steps

    small_observation_steps = observation_audit_steps(100)
    large_observation_steps = observation_audit_steps(10_000)
    assert large_observation_steps <= small_observation_steps * 2

    terminal_mutations = (
        (
            "cancelled-pass", add_cancelled,
            "UPDATE canonical_rechecks SET status='PASS',canonical_mint='M20'",
        ),
        (
            "cancelled-reason", add_cancelled,
            "UPDATE paper_entry_executions SET reason='other'",
        ),
        (
            "cancelled-trade", add_cancelled,
            "UPDATE paper_entry_executions SET paper_trade_id=999",
        ),
        (
            "abandoned-before-recheck", lambda conn, decision_id: add_abandoned(
                conn, decision_id, after_pass=False,
            ),
            "UPDATE paper_entry_executions SET canonical_recheck_id=999",
        ),
        (
            "abandoned-before-reason", lambda conn, decision_id: add_abandoned(
                conn, decision_id, after_pass=False,
            ),
            "UPDATE paper_entry_executions SET reason='other'",
        ),
        (
            "abandoned-after-no-recheck", lambda conn, decision_id: add_abandoned(
                conn, decision_id, after_pass=True,
            ),
            "UPDATE paper_entry_executions SET canonical_recheck_id=NULL",
        ),
        (
            "abandoned-after-cancel", lambda conn, decision_id: add_abandoned(
                conn, decision_id, after_pass=True,
            ),
            "UPDATE canonical_rechecks SET status='CANCEL',canonical_mint=NULL",
        ),
        (
            "abandoned-trade", lambda conn, decision_id: add_abandoned(
                conn, decision_id, after_pass=True,
            ),
            "UPDATE paper_entry_executions SET paper_trade_id=999",
        ),
    )
    for label, seed, mutation in terminal_mutations:
        malformed = make_connection()
        seed(malformed, 20)
        malformed.execute(mutation)
        malformed.commit()
        before_changes = malformed.total_changes
        with pytest.raises(
            RuntimeError,
            match=r"^WATCH startup terminal coverage invalid for decision_id=20$",
        ):
            store.assert_p3_buy_terminal_coverage(malformed)
        assert malformed.total_changes == before_changes, label
        assert malformed.in_transaction is False, label

    abandoned_with_invalid_pass = make_connection()
    add_abandoned(abandoned_with_invalid_pass, 20, after_pass=True)
    abandoned_with_invalid_pass.execute(
        "INSERT INTO safety_reports VALUES (999,'M20',2.5,'[]')"
    )
    abandoned_with_invalid_pass.execute(
        "UPDATE canonical_rechecks SET latest_target_report_id=999"
    )
    abandoned_with_invalid_pass.commit()
    with pytest.raises(
        RuntimeError,
        match=r"^WATCH startup terminal coverage invalid for decision_id=20$",
    ):
        store.assert_p3_buy_terminal_coverage(abandoned_with_invalid_pass)


def test_watch_terminal_audit_uses_canonical_buy_partial_index(tmp_path):
    import re

    import memebot.store as store

    index_name = "decisions_p3_canonical_buy_idx"
    expected_sql = (
        "CREATE INDEX IF NOT EXISTS decisions_p3_canonical_buy_idx\n"
        "  ON decisions(id)\n"
        "  WHERE action='BUY' AND CASE WHEN json_valid(feature_vector_json)\n"
        "  THEN json_extract(feature_vector_json,'$.canonical.status') END='CANONICAL';"
    )
    assert dict(store.V5_PERFORMANCE_INDEX_DDL)[index_name] == expected_sql
    assert store.V5_PERFORMANCE_INDEX_CONTRACT[index_name] == (
        "decisions", (("id", 0),), True,
    )

    db_path = tmp_path / "watch-index.db"
    conn = open_db(db_path)
    stored_sql = conn.execute(
        "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?",
        (index_name,),
    ).fetchone()[0]
    assert store._sqlite_ddl_tokens(stored_sql, preserve_strings=True) == (
        store._sqlite_ddl_tokens(
            expected_sql.replace(" IF NOT EXISTS", "", 1).rstrip(";"),
            preserve_strings=True,
        )
    )

    traced = []
    conn.set_trace_callback(traced.append)
    store.assert_p3_buy_terminal_coverage(conn)
    conn.set_trace_callback(None)
    selectors = [
        statement for statement in traced
        if "INDEXED BY decisions_p3_canonical_buy_idx" in statement
    ]
    assert len(selectors) == 1
    selector_sql = selectors[0]
    plan = [
        row["detail"]
        for row in conn.execute(f"EXPLAIN QUERY PLAN {selector_sql}")
    ]
    plan_text = "\n".join(plan)
    assert "INDEXED BY decisions_p3_canonical_buy_idx" in selector_sql
    assert re.search(
        r"d\.action='BUY'\s+AND CASE WHEN json_valid\(d\.feature_vector_json\)\s+"
        r"THEN json_extract\(d\.feature_vector_json,'\$\.canonical\.status'\) "
        r"END='CANONICAL'",
        selector_sql,
    )
    assert re.search(r"d\.id>0\s+ORDER BY d\.id\s+LIMIT 128\b", selector_sql)
    assert any(
        re.search(
            r"\b(?:SCAN|SEARCH) d USING INDEX decisions_p3_canonical_buy_idx\b",
            detail,
        )
        for detail in plan
    ), plan_text
    assert not any(
        "USE TEMP B-TREE" in detail
        or re.search(r"\bSCAN d\b(?! USING INDEX decisions_p3_canonical_buy_idx)", detail)
        for detail in plan
    ), plan_text
    conn.close()

    def audit_steps(row_count):
        measured = open_db(tmp_path / f"watch-work-{row_count}.db")
        measured.executemany(
            "INSERT INTO decisions("
            "id,at,mint,segment,action,score,feature_vector_json,config_hash"
            ") VALUES (?,?,?,?,?,?,?,?)",
            (
                (row_id, 1.0, f"M{row_id}", "CLIMBING", "BUY", 1.0, "{}", "cfg")
                for row_id in range(1, row_count + 1)
            ),
        )
        measured.commit()
        steps = 0

        def count_step():
            nonlocal steps
            steps += 1
            return 0

        measured.set_progress_handler(count_step, 1)
        try:
            store.assert_p3_buy_terminal_coverage(measured)
        finally:
            measured.set_progress_handler(None, 0)
            measured.close()
        return steps

    small_steps = audit_steps(100)
    large_steps = audit_steps(10_000)
    assert large_steps <= small_steps * 2

    replacements = (
        "CREATE INDEX decisions_p3_canonical_buy_idx ON decisions(at) "
        "WHERE action='BUY' AND CASE WHEN json_valid(feature_vector_json) "
        "THEN json_extract(feature_vector_json,'$.canonical.status') END='CANONICAL'",
        "CREATE INDEX decisions_p3_canonical_buy_idx ON decisions(id DESC) "
        "WHERE action='BUY' AND CASE WHEN json_valid(feature_vector_json) "
        "THEN json_extract(feature_vector_json,'$.canonical.status') END='CANONICAL'",
        "CREATE INDEX decisions_p3_canonical_buy_idx ON decisions(id) "
        "WHERE action='BUY' AND CASE WHEN json_valid(feature_vector_json) "
        "THEN json_extract(feature_vector_json,'$.canonical.status') END='SUPPRESSED'",
        "CREATE INDEX decisions_p3_canonical_buy_idx ON decisions(id) "
        "WHERE action='buy' AND CASE WHEN json_valid(feature_vector_json) "
        "THEN json_extract(feature_vector_json,'$.canonical.status') END='CANONICAL'",
        "CREATE TABLE decisions_p3_canonical_buy_idx(impostor INTEGER)",
    )
    for ordinal, replacement in enumerate(replacements):
        incompatible_path = tmp_path / f"watch-incompatible-{ordinal}.db"
        incompatible = open_db(incompatible_path)
        incompatible.execute(f"DROP INDEX {index_name}")
        incompatible.execute(replacement)
        incompatible.commit()
        incompatible.close()

        with pytest.raises(
            RuntimeError,
            match=rf"^incompatible performance index {index_name}$",
        ):
            open_db(incompatible_path)


def test_watch_startup_rejects_open_p3_position_but_accepts_closed_and_legacy_positions():
    import memebot.store as store

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE p3_position_current (
  decision_id INTEGER PRIMARY KEY,
  bought_qty REAL NOT NULL,
  sold_qty REAL NOT NULL
);
CREATE INDEX p3_position_current_open_idx
  ON p3_position_current(decision_id) WHERE sold_qty<bought_qty;
CREATE TABLE decisions (id INTEGER PRIMARY KEY, action TEXT NOT NULL);
CREATE TABLE paper_trades (
  id INTEGER PRIMARY KEY,
  decision_id INTEGER NOT NULL,
  side TEXT NOT NULL
);
INSERT INTO decisions VALUES (1,'BUY');
INSERT INTO paper_trades VALUES (1,1,'buy');"""
    )
    conn.commit()
    before_changes = conn.total_changes
    before_in_transaction = conn.in_transaction

    assert store.assert_no_open_p3_positions(conn) is None

    assert conn.total_changes == before_changes
    assert conn.in_transaction is before_in_transaction

    conn.execute("INSERT INTO p3_position_current VALUES (30,2.0,2.0)")
    conn.commit()
    before_changes = conn.total_changes
    before_in_transaction = conn.in_transaction

    assert store.assert_no_open_p3_positions(conn) is None

    assert conn.total_changes == before_changes
    assert conn.in_transaction is before_in_transaction

    conn.executemany(
        "INSERT INTO p3_position_current VALUES (?,?,?)",
        ((20, 2.0, 1.0), (10, 3.0, 0.0)),
    )
    conn.commit()
    before_changes = conn.total_changes
    before_in_transaction = conn.in_transaction

    with pytest.raises(RuntimeError) as exc_info:
        store.assert_no_open_p3_positions(conn)

    assert str(exc_info.value) == (
        "WATCH-only startup blocked by open P3 position decision_id=10"
    )
    assert conn.total_changes == before_changes
    assert conn.in_transaction is before_in_transaction
    conn.close()


def test_watch_open_position_audit_uses_attested_partial_index():
    import memebot.store as store

    def make_connection(index_sql=None):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """CREATE TABLE p3_position_current (
  decision_id INTEGER PRIMARY KEY,
  bought_qty REAL NOT NULL,
  sold_qty REAL NOT NULL
);
CREATE TABLE decisions (id INTEGER PRIMARY KEY, action TEXT NOT NULL);
CREATE TABLE paper_trades (
  id INTEGER PRIMARY KEY,
  decision_id INTEGER NOT NULL,
  side TEXT NOT NULL
);"""
        )
        if index_sql is not None:
            conn.execute(index_sql)
        return conn

    exact_index_sql = (
        "CREATE INDEX p3_position_current_open_idx "
        "ON p3_position_current(decision_id) WHERE sold_qty<bought_qty"
    )
    conn = make_connection(exact_index_sql)
    traced = []
    conn.set_trace_callback(traced.append)
    assert store.assert_no_open_p3_positions(conn) is None
    conn.set_trace_callback(None)
    selectors = [
        statement for statement in traced
        if "INDEXED BY p3_position_current_open_idx" in statement
    ]
    assert len(selectors) == 1
    plan_text = "\n".join(
        row["detail"]
        for row in conn.execute(f"EXPLAIN QUERY PLAN {selectors[0]}")
    )
    assert (
        "SCAN p3_position_current USING INDEX p3_position_current_open_idx"
        in plan_text
    )
    assert "USE TEMP B-TREE" not in plan_text
    conn.close()

    def audit_steps(row_count):
        measured = make_connection(exact_index_sql)
        measured.executemany(
            "INSERT INTO p3_position_current VALUES (?,?,?)",
            ((row_id, 1.0, 1.0) for row_id in range(1, row_count + 1)),
        )
        measured.executemany(
            "INSERT INTO decisions VALUES (?,?)",
            ((row_id, "BUY") for row_id in range(1, row_count + 1)),
        )
        measured.executemany(
            "INSERT INTO paper_trades VALUES (?,?,?)",
            ((row_id, row_id, "buy") for row_id in range(1, row_count + 1)),
        )
        measured.commit()
        steps = 0

        def count_step():
            nonlocal steps
            steps += 1
            return 0

        measured.set_progress_handler(count_step, 1)
        try:
            store.assert_no_open_p3_positions(measured)
        finally:
            measured.set_progress_handler(None, 0)
            measured.close()
        return steps

    small_steps = audit_steps(100)
    large_steps = audit_steps(10_000)
    assert large_steps <= small_steps * 2

    missing_index = make_connection()
    with pytest.raises(sqlite3.OperationalError):
        store.assert_no_open_p3_positions(missing_index)
    missing_index.close()

    incompatible_index = make_connection(
        "CREATE INDEX p3_position_current_open_idx "
        "ON p3_position_current(decision_id) WHERE sold_qty<=bought_qty"
    )
    with pytest.raises(sqlite3.OperationalError):
        store.assert_no_open_p3_positions(incompatible_index)
    incompatible_index.close()


def test_open_db_heals_missing_watch_open_position_index_on_existing_v5(tmp_path):
    import memebot.store as store

    index_name = "p3_position_current_open_idx"
    expected_sql = (
        "CREATE INDEX IF NOT EXISTS p3_position_current_open_idx\n"
        "  ON p3_position_current(decision_id) WHERE sold_qty<bought_qty;"
    )
    db_path = tmp_path / "watch-open-index-missing.db"
    conn = open_db(db_path)
    conn.execute(f"DROP INDEX {index_name}")
    conn.execute("PRAGMA user_version=5")
    conn.commit()
    conn.close()

    conn = open_db(db_path)
    traced = []
    conn.set_trace_callback(traced.append)
    store.assert_no_open_p3_positions(conn)
    conn.set_trace_callback(None)

    assert dict(store.V5_INDEX_DDL)[index_name] == expected_sql
    assert store.V5_PERFORMANCE_INDEX_CONTRACT[index_name] == (
        "p3_position_current", (("decision_id", 0),), True,
    )
    stored_sql = conn.execute(
        "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?",
        (index_name,),
    ).fetchone()[0]
    assert store._sqlite_ddl_tokens(stored_sql, preserve_strings=True) == (
        store._sqlite_ddl_tokens(
            expected_sql.replace(" IF NOT EXISTS", "", 1).rstrip(";"),
            preserve_strings=True,
        )
    )
    assert tuple(
        (row["name"], row["desc"])
        for row in conn.execute(f"PRAGMA index_xinfo({index_name})")
        if row["key"] == 1
    ) == (("decision_id", 0),)
    index_list_row = next(
        row for row in conn.execute("PRAGMA index_list(p3_position_current)")
        if row["name"] == index_name
    )
    assert index_list_row["partial"] == 1

    selectors = [
        statement for statement in traced
        if f"INDEXED BY {index_name}" in statement
    ]
    assert len(selectors) == 1
    plan_text = "\n".join(
        row["detail"]
        for row in conn.execute(f"EXPLAIN QUERY PLAN {selectors[0]}")
    )
    assert f"USING INDEX {index_name}" in plan_text
    assert "USE TEMP B-TREE" not in plan_text
    conn.close()


def test_open_db_rejects_case_changed_watch_open_position_owning_table_on_existing_v5(
    tmp_path,
):
    db_path = tmp_path / "watch-open-index-uppercase-owner.db"
    conn = open_db(db_path)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
    conn.execute(
        "ALTER TABLE p3_position_current RENAME TO p3_position_current_case_temp"
    )
    conn.execute(
        "ALTER TABLE p3_position_current_case_temp RENAME TO P3_POSITION_CURRENT"
    )
    conn.commit()

    schema_before = conn.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema"
        " WHERE name IN ('P3_POSITION_CURRENT','p3_position_current_open_idx')"
        " ORDER BY type,name"
    ).fetchall()
    assert [(row["type"], row["name"], row["tbl_name"]) for row in schema_before] == [
        ("index", "p3_position_current_open_idx", "P3_POSITION_CURRENT"),
        ("table", "P3_POSITION_CURRENT", "P3_POSITION_CURRENT"),
    ]
    raw_w3_sql = (
        "SELECT decision_id FROM p3_position_current "
        "INDEXED BY p3_position_current_open_idx "
        "WHERE sold_qty<bought_qty ORDER BY decision_id LIMIT 1"
    )
    assert conn.execute(raw_w3_sql).fetchall() == []
    conn.close()

    with pytest.raises(RuntimeError) as exc_info:
        open_db(db_path)
    assert str(exc_info.value) == (
        "incompatible performance index p3_position_current_open_idx"
    )

    inspected = sqlite3.connect(db_path)
    inspected.row_factory = sqlite3.Row
    assert inspected.in_transaction is False
    assert inspected.execute("PRAGMA user_version").fetchone()[0] == 6
    assert inspected.execute(raw_w3_sql).fetchall() == []
    assert inspected.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema"
        " WHERE name IN ('P3_POSITION_CURRENT','p3_position_current_open_idx')"
        " ORDER BY type,name"
    ).fetchall() == schema_before
    inspected.execute("BEGIN IMMEDIATE")
    inspected.rollback()
    inspected.close()


@pytest.mark.parametrize(
    ("case", "replacement_sql", "witness_temp_sort"),
    (
        (
            "wrong-column",
            "CREATE INDEX p3_position_current_open_idx "
            "ON p3_position_current(sold_qty) WHERE sold_qty<bought_qty",
            True,
        ),
        (
            "descending-column",
            "CREATE INDEX p3_position_current_open_idx "
            "ON p3_position_current(decision_id DESC) WHERE sold_qty<bought_qty",
            False,
        ),
        (
            "wrong-predicate",
            "CREATE INDEX p3_position_current_open_idx "
            "ON p3_position_current(decision_id) WHERE sold_qty<=bought_qty",
            False,
        ),
        (
            "wrong-table",
            "CREATE INDEX p3_position_current_open_idx "
            "ON decisions(id) WHERE action='BUY'",
            False,
        ),
        (
            "table-impostor",
            "CREATE TABLE p3_position_current_open_idx(impostor INTEGER)",
            False,
        ),
        (
            "coexisting-trigger-impostor",
            "CREATE INDEX p3_position_current_open_idx "
            "ON p3_position_current(decision_id) WHERE sold_qty<bought_qty",
            False,
        ),
    ),
    ids=(
        "wrong-column",
        "descending-column",
        "wrong-predicate",
        "wrong-table",
        "table-impostor",
        "coexisting-trigger-impostor",
    ),
)
def test_open_db_rejects_incompatible_watch_open_position_index_on_existing_v5(
    tmp_path, case, replacement_sql, witness_temp_sort,
):
    index_name = "p3_position_current_open_idx"
    db_path = tmp_path / f"watch-open-index-{case}.db"
    conn = open_db(db_path)
    conn.execute(f"DROP INDEX {index_name}")
    conn.execute(replacement_sql)
    if case == "coexisting-trigger-impostor":
        conn.execute(
            "CREATE TRIGGER P3_POSITION_CURRENT_OPEN_IDX "
            "AFTER INSERT ON tokens BEGIN SELECT 1; END"
        )
    conn.commit()

    if witness_temp_sort:
        raw_w3_sql = (
            "SELECT decision_id FROM p3_position_current "
            "INDEXED BY p3_position_current_open_idx "
            "WHERE sold_qty<bought_qty ORDER BY decision_id LIMIT 1"
        )
        assert conn.execute(raw_w3_sql).fetchall() == []
        plan_text = "\n".join(
            row["detail"]
            for row in conn.execute(f"EXPLAIN QUERY PLAN {raw_w3_sql}")
        )
        assert f"USING INDEX {index_name}" in plan_text
        assert "USE TEMP B-TREE" in plan_text

    conn.close()
    with pytest.raises(RuntimeError) as exc_info:
        open_db(db_path)
    assert str(exc_info.value) == f"incompatible performance index {index_name}"


def test_safety_report_p3_children_are_atomic_and_linked(tmp_path):
    import json
    from dataclasses import dataclass, replace

    from memebot.early_buyers import EarlyBuyerEvidenceDraft
    from memebot.safety.checks import CheckResult, HolderEvidenceDraft
    from memebot.safety.gate import SafetyReport
    from memebot.store import save_safety_report_with_p3_evidence

    @dataclass(frozen=True, slots=True)
    class Draft:
        mint: str
        raw_completed_at: float
        segment: str
        hard_fails: tuple[str, ...]
        risk_score: float
        results_json: str
        safety_inputs_hash: str
        holder: HolderEvidenceDraft
        early_buyer: EarlyBuyerEvidenceDraft

    results = (
        CheckResult("check_a", False, hard=True, reason="reason_a"),
        CheckResult("check_b", False, hard=True, reason="reason_b"),
    )
    results_json = json.dumps(
        [
            {
                "name": result.name,
                "passed": result.passed,
                "hard": result.hard,
                "reason": result.reason,
                "detail": result.detail,
                "available": result.available,
            }
            for result in results
        ],
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    def make_draft(*, synthetic=False):
        return Draft(
            mint="MINT",
            raw_completed_at=10.0,
            segment="CLIMBING",
            hard_fails=("reason_a", "reason_b"),
            risk_score=0.0,
            results_json=results_json,
            safety_inputs_hash="a" * 64,
            holder=HolderEvidenceDraft(
                sampled_token_accounts=None if synthetic else 3,
                distinct_non_curve_owners=None if synthetic else 2,
                top10_non_curve_owner_share_pct=None if synthetic else 60.0,
                holder_observed_at=None if synthetic else 8.0,
                unavailable_reason="holder_check_not_run" if synthetic else "",
                inputs_hash="b" * 64,
            ),
            early_buyer=EarlyBuyerEvidenceDraft(
                checked_at=None if synthetic else 9.0,
                buyers=() if synthetic else ("BUYER_A", "BUYER_B"),
                unavailable_reason=(
                    "early_buyer_check_not_run" if synthetic else ""
                ),
                inputs_hash="c" * 64,
            ),
        )

    for label, synthetic in (("real", False), ("synthetic", True)):
        conn = open_db(tmp_path / f"success-{label}.db", migration_clock=lambda: 1.0)
        statements = []
        conn.set_trace_callback(statements.append)
        report, holder_id, early_id = save_safety_report_with_p3_evidence(
            conn, draft=make_draft(synthetic=synthetic),
        )
        assert [
            statement.strip().upper()
            for statement in statements
            if statement.strip().upper().startswith("BEGIN")
        ] == ["BEGIN IMMEDIATE"]

        parent = conn.execute("SELECT * FROM safety_reports").fetchone()
        holder = conn.execute("SELECT * FROM holder_evidence").fetchone()
        early = conn.execute("SELECT * FROM early_buyer_reads").fetchone()
        assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM holder_evidence").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0] == 1
        assert report == SafetyReport(
            mint="MINT",
            checked_at=parent["checked_at"],
            segment="CLIMBING",
            hard_fails=("reason_a", "reason_b"),
            risk_score=0.0,
            results=results,
            inputs_hash="a" * 64,
            report_id=parent["id"],
        )
        with pytest.raises((AttributeError, TypeError)):
            report.report_id = 999
        assert (holder_id, holder["safety_report_id"]) == (holder["id"], parent["id"])
        assert (early_id, early["safety_report_id"]) == (early["id"], parent["id"])
        assert parent["hard_fails_json"] == '["reason_a","reason_b"]'
        assert parent["inputs_hash"] == "a" * 64
        assert holder["inputs_hash"] == "b" * 64
        assert early["inputs_hash"] == "c" * 64
        assert early["buyers_json"] == (
            "[]" if synthetic else '["BUYER_A","BUYER_B"]'
        )
        assert holder["holder_observed_at"] == (
            parent["checked_at"] if synthetic else 8.0
        )
        assert early["checked_at"] == (
            parent["checked_at"] if synthetic else 9.0
        )
        conn.close()

    for child_table in ("holder_evidence", "early_buyer_reads"):
        conn = open_db(
            tmp_path / f"rollback-{child_table}.db", migration_clock=lambda: 1.0,
        )
        clock_before = conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0]
        conn.execute(
            f"CREATE TEMP TRIGGER inject_{child_table}_failure "
            f"BEFORE INSERT ON {child_table} BEGIN "
            f"SELECT RAISE(ABORT,'injected {child_table} failure'); END"
        )
        conn.commit()

        with pytest.raises(
            sqlite3.IntegrityError, match=f"injected {child_table} failure",
        ):
            save_safety_report_with_p3_evidence(conn, draft=make_draft())

        assert not conn.in_transaction
        assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM holder_evidence").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0] == 0
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == clock_before
        conn.close()

    conn = open_db(
        tmp_path / "rollback-report-reconstruction.db", migration_clock=lambda: 1.0,
    )
    clock_before = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    with pytest.raises(ValueError, match="invalid safety results JSON"):
        save_safety_report_with_p3_evidence(
            conn, draft=replace(make_draft(), results_json="{}"),
        )
    assert not conn.in_transaction
    assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM holder_evidence").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0] == 0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before
    conn.close()

    guard_conn = open_db(tmp_path / "report-link-guard.db", migration_clock=lambda: 1.0)
    report_id = guard_conn.execute(
        "INSERT INTO safety_reports(mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
        "VALUES ('MINT',10.0,'[]',0.0,?)",
        ("d" * 64,),
    ).lastrowid
    guard_conn.commit()
    for mint, checked_at in (("OTHER", 9.0), ("MINT", 11.0)):
        with pytest.raises(
            sqlite3.IntegrityError, match="invalid early-buyer report link",
        ):
            guard_conn.execute(
                "INSERT INTO early_buyer_reads("
                "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,safety_report_id) "
                "VALUES (?,?,'[]','rpc_error',?,?)",
                (mint, checked_at, "e" * 64, report_id),
            )
        guard_conn.rollback()
    assert guard_conn.execute(
        "SELECT count(*) FROM early_buyer_reads"
    ).fetchone()[0] == 0
    guard_conn.close()


def test_safety_report_allocates_strict_processing_time_with_children_atomically(
    tmp_path,
):
    import math
    from dataclasses import dataclass, replace

    from memebot.early_buyers import EarlyBuyerEvidenceDraft
    from memebot.safety.checks import HolderEvidenceDraft
    from memebot.store import (
        fence_p3_causal_wall,
        save_safety_report_with_p3_evidence,
    )

    @dataclass(frozen=True, slots=True)
    class Draft:
        mint: str
        raw_completed_at: float
        segment: str
        hard_fails: tuple[str, ...]
        risk_score: float
        results_json: str
        safety_inputs_hash: str
        holder: HolderEvidenceDraft
        early_buyer: EarlyBuyerEvidenceDraft

    def make_draft(*, raw_completed_at=10.0, holder_at=8.0, early_at=9.0):
        return Draft(
            mint="MINT",
            raw_completed_at=raw_completed_at,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=0.0,
            results_json="[]",
            safety_inputs_hash="a" * 64,
            holder=HolderEvidenceDraft(
                sampled_token_accounts=3,
                distinct_non_curve_owners=2,
                top10_non_curve_owner_share_pct=60.0,
                holder_observed_at=holder_at,
                unavailable_reason="",
                inputs_hash="b" * 64,
            ),
            early_buyer=EarlyBuyerEvidenceDraft(
                checked_at=early_at,
                buyers=("BUYER",),
                unavailable_reason="",
                inputs_hash="c" * 64,
            ),
        )

    cases = (
        ("forward-raw", make_draft(), 10.0),
        (
            "later-holder-source",
            make_draft(raw_completed_at=10.0, holder_at=15.0, early_at=12.0),
            15.0,
        ),
        (
            "later-early-source",
            make_draft(raw_completed_at=10.0, holder_at=12.0, early_at=16.0),
            16.0,
        ),
    )
    for label, draft, envelope in cases:
        conn = open_db(tmp_path / f"{label}.db", migration_clock=lambda: 1.0)
        statements = []
        conn.set_trace_callback(statements.append)

        report, holder_id, early_id = save_safety_report_with_p3_evidence(
            conn, draft=draft,
        )

        expected_t = math.nextafter(envelope, math.inf)
        parent = conn.execute("SELECT * FROM safety_reports").fetchone()
        holder = conn.execute(
            "SELECT * FROM holder_evidence WHERE id=?", (holder_id,),
        ).fetchone()
        early = conn.execute(
            "SELECT * FROM early_buyer_reads WHERE id=?", (early_id,),
        ).fetchone()
        assert report.checked_at == parent["checked_at"] == expected_t
        assert holder["holder_observed_at"] == draft.holder.holder_observed_at
        assert early["checked_at"] == draft.early_buyer.checked_at
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == expected_t
        assert [
            statement.strip().upper()
            for statement in statements
            if statement.strip().upper().startswith("BEGIN")
        ] == ["BEGIN IMMEDIATE"]
        assert [
            statement.strip().upper()
            for statement in statements
            if statement.strip().upper() == "COMMIT"
        ] == ["COMMIT"]
        conn.close()

    conn = open_db(tmp_path / "regressed-prior.db", migration_clock=lambda: 1.0)
    conn.execute("BEGIN IMMEDIATE")
    fence_p3_causal_wall(conn, observed_wall=20.0)
    prior_id = conn.execute(
        "INSERT INTO safety_reports(mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
        "VALUES ('MINT',20.0,'[]',0.0,?)",
        ("d" * 64,),
    ).lastrowid
    conn.commit()

    report, _, _ = save_safety_report_with_p3_evidence(
        conn,
        draft=make_draft(raw_completed_at=5.0, holder_at=4.0, early_at=6.0),
    )
    expected_t = math.nextafter(20.0, math.inf)
    assert report.report_id > prior_id
    assert report.checked_at == expected_t
    assert [
        row["checked_at"]
        for row in conn.execute("SELECT checked_at FROM safety_reports ORDER BY id")
    ] == [20.0, expected_t]
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == expected_t
    conn.close()

    invalid_draft = make_draft()
    invalid_sources = (
        replace(
            invalid_draft,
            holder=replace(invalid_draft.holder, holder_observed_at=float("nan")),
        ),
        replace(
            invalid_draft,
            early_buyer=replace(invalid_draft.early_buyer, checked_at=True),
        ),
    )
    for index, draft in enumerate(invalid_sources):
        conn = open_db(
            tmp_path / f"invalid-source-{index}.db", migration_clock=lambda: 1.0,
        )
        with pytest.raises(ValueError, match="invalid p3 causal wall"):
            save_safety_report_with_p3_evidence(conn, draft=draft)
        assert not conn.in_transaction
        assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM holder_evidence").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0] == 0
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == 1.0
        conn.close()

    conn = open_db(tmp_path / "allocated-rollback.db", migration_clock=lambda: 1.0)
    conn.execute(
        "CREATE TEMP TRIGGER inject_early_failure "
        "BEFORE INSERT ON early_buyer_reads BEGIN "
        "SELECT RAISE(ABORT,'injected post-allocation failure'); END"
    )
    conn.commit()
    with pytest.raises(
        sqlite3.IntegrityError, match="injected post-allocation failure",
    ):
        save_safety_report_with_p3_evidence(
            conn,
            draft=make_draft(
                raw_completed_at=30.0, holder_at=28.0, early_at=29.0,
            ),
        )
    assert not conn.in_transaction
    assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM holder_evidence").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0] == 0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 1.0
    conn.close()


def test_legacy_childless_report_allocates_strict_processing_time(tmp_path):
    import math

    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "legacy-childless.db", migration_clock=lambda: 1.0)
    statements = []
    conn.set_trace_callback(statements.append)

    def save(raw_completed_at):
        return save_safety_report(
            conn,
            mint="MINT",
            raw_completed_at=raw_completed_at,
            segment="CLIMBING",
            hard_fails=[],
            risk_score=0.0,
            results_json="[]",
            inputs_hash="d1309bd963671335b85400f13c76a5fd9a27af044358bf2838fd7a887315cd79",
        )

    first_id = save(10.0)
    second_id = save(5.0)

    first_t = math.nextafter(10.0, math.inf)
    second_t = math.nextafter(first_t, math.inf)
    assert second_id > first_id
    assert [
        (row["id"], row["checked_at"])
        for row in conn.execute(
            "SELECT id,checked_at FROM safety_reports ORDER BY id"
        )
    ] == [(first_id, first_t), (second_id, second_t)]
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == second_t
    assert [
        statement.strip().upper()
        for statement in statements
        if statement.strip().upper().startswith("BEGIN")
    ] == ["BEGIN IMMEDIATE", "BEGIN IMMEDIATE"]
    assert [
        statement.strip().upper()
        for statement in statements
        if statement.strip().upper() == "COMMIT"
    ] == ["COMMIT", "COMMIT"]

    third_id = save(20.0)
    third_t = math.nextafter(20.0, math.inf)
    assert third_id > second_id
    assert conn.execute(
        "SELECT checked_at FROM safety_reports WHERE id=?", (third_id,),
    ).fetchone()[0] == third_t
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == third_t

    report_count = conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0]
    clock_before = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]

    conn.execute(
        "CREATE TEMP TRIGGER inject_legacy_report_failure "
        "BEFORE INSERT ON safety_reports BEGIN "
        "SELECT RAISE(ABORT,'injected legacy report failure'); END"
    )
    conn.commit()
    with pytest.raises(
        sqlite3.IntegrityError, match="injected legacy report failure",
    ):
        save(30.0)
    assert not conn.in_transaction
    assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == report_count
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before
    assert conn.execute("SELECT count(*) FROM holder_evidence").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM early_buyer_reads").fetchone()[0] == 0
    conn.close()


def test_save_safety_report_rejects_removed_checked_at_seam(tmp_path):
    import inspect

    from memebot.store import save_safety_report

    conn = open_db(tmp_path / "removed-checked-at.db", migration_clock=lambda: 1.0)
    writer = save_safety_report
    with pytest.raises(TypeError, match="unexpected keyword argument 'checked_at'"):
        writer(
            conn,
            mint="MINT",
            checked_at=10.0,
            raw_completed_at=10.0,
            segment="CLIMBING",
            hard_fails=[],
            risk_score=0.0,
            results_json="[]",
            inputs_hash="d1309bd963671335b85400f13c76a5fd9a27af044358bf2838fd7a887315cd79",
        )

    parameters = inspect.signature(save_safety_report).parameters
    assert "checked_at" not in parameters
    assert parameters["raw_completed_at"].default is inspect.Parameter.empty
    assert conn.execute("SELECT count(*) FROM safety_reports").fetchone()[0] == 0
    conn.close()


def test_store_safety_fixture_uses_raw_completed_at():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(__file__).read_text())
    legacy_calls = []
    missing_raw_calls = []
    raw_calls = []
    for function in (
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ):
        for call in (
            node for node in ast.walk(function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "save_safety_report"
        ):
            keywords = {keyword.arg for keyword in call.keywords}
            if "checked_at" in keywords:
                legacy_calls.append(call.lineno)
            if "raw_completed_at" in keywords:
                raw_calls.append(call.lineno)
            else:
                missing_raw_calls.append(call.lineno)

    assert legacy_calls == []
    assert missing_raw_calls == []
    assert raw_calls


def test_all_legacy_safety_report_callers_use_raw_completed_at():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src" / "memebot").rglob("*.py"))
    paths += sorted((root / "tests").rglob("*.py"))
    paths += sorted((root / "scripts").rglob("*.py"))
    callers = []
    invalid_callers = []
    for path in paths:
        relative_path = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text())
        for call in (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and (
                isinstance(node.func, ast.Name)
                and node.func.id == "save_safety_report"
                or isinstance(node.func, ast.Attribute)
                and node.func.attr == "save_safety_report"
            )
        ):
            location = (relative_path, call.lineno)
            callers.append(location)
            keywords = {keyword.arg for keyword in call.keywords}
            if (
                "raw_completed_at" not in keywords
                or "checked_at" in keywords
                or None in keywords
            ):
                invalid_callers.append(location)

    assert callers
    assert invalid_callers == []


def test_wallet_writer_advances_causal_fence(tmp_path):
    from memebot.store import record_wallet_pnl_event

    conn = open_db(tmp_path / "wallet-fence.db", migration_clock=lambda: 1.0)
    statements = []
    conn.set_trace_callback(statements.append)

    first_id = record_wallet_pnl_event(
        conn,
        at=10.0,
        wallet="WALLET",
        mint="MINT-A",
        realized_pnl_sol=1.5,
        source="test",
        detail={"sequence": 1},
    )
    second_id = record_wallet_pnl_event(
        conn,
        at=5.0,
        wallet="WALLET",
        mint="MINT-B",
        realized_pnl_sol=-0.25,
        source="test",
        detail={"sequence": 2},
    )

    assert second_id > first_id
    assert [
        tuple(row)
        for row in conn.execute(
            "SELECT id,at,wallet,mint,realized_pnl_sol "
            "FROM wallet_pnl_events ORDER BY id"
        )
    ] == [
        (first_id, 10.0, "WALLET", "MINT-A", 1.5),
        (second_id, 5.0, "WALLET", "MINT-B", -0.25),
    ]
    assert tuple(conn.execute(
        "SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id "
        "FROM wallet_pnl_summary WHERE wallet='WALLET'"
    ).fetchone()) == ("WALLET", 2, 1.25, 10.0, second_id)
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 10.0
    assert [
        statement.strip().upper()
        for statement in statements
        if statement.strip().upper().startswith("BEGIN")
    ] == ["BEGIN IMMEDIATE", "BEGIN IMMEDIATE"]
    assert [
        statement.strip().upper()
        for statement in statements
        if statement.strip().upper() == "COMMIT"
    ] == ["COMMIT", "COMMIT"]

    event_count = conn.execute(
        "SELECT count(*) FROM wallet_pnl_events"
    ).fetchone()[0]
    summary_before = tuple(conn.execute(
        "SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id "
        "FROM wallet_pnl_summary WHERE wallet='WALLET'"
    ).fetchone())
    for invalid_at in (True, float("nan")):
        with pytest.raises(ValueError, match="invalid p3 causal wall"):
            record_wallet_pnl_event(
                conn,
                at=invalid_at,
                wallet="WALLET",
                mint="INVALID",
                realized_pnl_sol=1.0,
                source="test",
                detail={},
            )
        assert not conn.in_transaction
        assert conn.execute(
            "SELECT count(*) FROM wallet_pnl_events"
        ).fetchone()[0] == event_count
        assert tuple(conn.execute(
            "SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id "
            "FROM wallet_pnl_summary WHERE wallet='WALLET'"
        ).fetchone()) == summary_before
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == 10.0

    conn.execute(
        "CREATE TEMP TRIGGER inject_wallet_failure "
        "BEFORE INSERT ON wallet_pnl_events BEGIN "
        "SELECT RAISE(ABORT,'injected wallet failure'); END"
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="injected wallet failure"):
        record_wallet_pnl_event(
            conn,
            at=30.0,
            wallet="WALLET",
            mint="ROLLBACK",
            realized_pnl_sol=2.0,
            source="test",
            detail={},
        )
    assert not conn.in_transaction
    assert conn.execute(
        "SELECT count(*) FROM wallet_pnl_events"
    ).fetchone()[0] == event_count
    assert tuple(conn.execute(
        "SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id "
        "FROM wallet_pnl_summary WHERE wallet='WALLET'"
    ).fetchone()) == summary_before
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 10.0
    conn.close()


def test_legacy_early_buyer_writer_advances_causal_fence(tmp_path):
    from memebot.store import record_early_buyer_read

    conn = open_db(tmp_path / "early-buyer-fence.db", migration_clock=lambda: 1.0)
    statements = []
    conn.set_trace_callback(statements.append)

    first_id = record_early_buyer_read(
        conn,
        mint="MINT-A",
        checked_at=10.0,
        buyers=("BUYER-A", "BUYER-B"),
        unavailable_reason="",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )
    second_id = record_early_buyer_read(
        conn,
        mint="MINT-B",
        checked_at=5.0,
        buyers=[],
        unavailable_reason="no_signatures",
        inputs_hash="2222222222222222222222222222222222222222222222222222222222222222",
    )

    assert second_id > first_id
    assert [
        tuple(row)
        for row in conn.execute(
            "SELECT id,mint,checked_at,buyers_json,unavailable_reason,"
            "inputs_hash,safety_report_id FROM early_buyer_reads ORDER BY id"
        )
    ] == [
        (first_id, "MINT-A", 10.0, '["BUYER-A", "BUYER-B"]', "", "1" * 64, None),
        (second_id, "MINT-B", 5.0, "[]", "no_signatures", "2" * 64, None),
    ]
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 10.0
    assert [
        statement.strip().upper()
        for statement in statements
        if statement.strip().upper().startswith("BEGIN")
    ] == ["BEGIN IMMEDIATE", "BEGIN IMMEDIATE"]
    assert [
        statement.strip().upper()
        for statement in statements
        if statement.strip().upper() == "COMMIT"
    ] == ["COMMIT", "COMMIT"]

    row_count = conn.execute(
        "SELECT count(*) FROM early_buyer_reads"
    ).fetchone()[0]
    for invalid_checked_at in (True, float("nan")):
        with pytest.raises(ValueError, match="invalid p3 causal wall"):
            record_early_buyer_read(
                conn,
                mint="INVALID",
                checked_at=invalid_checked_at,
                buyers=(),
                unavailable_reason="rpc_error",
                inputs_hash="3333333333333333333333333333333333333333333333333333333333333333",
            )
        assert not conn.in_transaction
        assert conn.execute(
            "SELECT count(*) FROM early_buyer_reads"
        ).fetchone()[0] == row_count
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == 10.0

    conn.execute("BEGIN IMMEDIATE")
    with pytest.raises(
        RuntimeError, match="early-buyer persistence owns its transaction"
    ):
        record_early_buyer_read(
            conn,
            mint="OUTER",
            checked_at=20.0,
            buyers=(),
            unavailable_reason="rpc_error",
            inputs_hash="4444444444444444444444444444444444444444444444444444444444444444",
        )
    assert conn.in_transaction
    conn.rollback()
    assert conn.execute(
        "SELECT count(*) FROM early_buyer_reads"
    ).fetchone()[0] == row_count
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 10.0

    conn.execute(
        "CREATE TEMP TRIGGER inject_early_buyer_failure "
        "BEFORE INSERT ON early_buyer_reads BEGIN "
        "SELECT RAISE(ABORT,'injected early-buyer failure'); END"
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="injected early-buyer failure"):
        record_early_buyer_read(
            conn,
            mint="ROLLBACK",
            checked_at=30.0,
            buyers=(),
            unavailable_reason="rpc_error",
            inputs_hash="5555555555555555555555555555555555555555555555555555555555555555",
        )
    assert not conn.in_transaction
    assert conn.execute(
        "SELECT count(*) FROM early_buyer_reads"
    ).fetchone()[0] == row_count
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == 10.0
    conn.close()


def test_validated_latest_report_is_indexed_single_row_and_never_falls_back(
    tmp_path,
):
    from memebot.safety.checks import HolderEvidenceDraft
    from memebot.store import (
        EvidenceIntegrityError,
        ValidatedSafetyHolder,
        validated_latest_report_as_of,
        validated_report_by_id,
    )

    conn = open_db(tmp_path / "validated-latest.db", migration_clock=lambda: 1.0)

    def insert_parent(*, checked_at, risk_score=5.0, hard_fails_json="[]"):
        report_id = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
            ") VALUES ('MINT',?,?,?,?)",
            (checked_at, hard_fails_json, risk_score, "a" * 64),
        ).lastrowid
        conn.commit()
        return report_id

    def insert_available_holder(report_id, *, holder_observed_at):
        holder_id = conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,"
            "unavailable_reason,inputs_hash"
            ") VALUES (?,?,?,?,?,'',?)",
            (report_id, 20, 12, 25.0, holder_observed_at, "b" * 64),
        ).lastrowid
        conn.commit()
        return holder_id

    older_id = insert_parent(checked_at=10.0)
    older_holder_id = insert_available_holder(
        older_id, holder_observed_at=9.0,
    )
    newest_childless_id = insert_parent(checked_at=20.0)

    statements = []
    conn.set_trace_callback(statements.append)
    childless = validated_latest_report_as_of(conn, mint="MINT", as_of=30.0)
    conn.set_trace_callback(None)

    assert childless == ValidatedSafetyHolder(
        safety_report_id=newest_childless_id,
        mint="MINT",
        checked_at=20.0,
        risk_score=5.0,
        hard_fails=(),
        safety_inputs_hash="a" * 64,
        holder_evidence_id=None,
        holder=None,
        holder_unavailable_reason="holder_evidence_missing",
    )
    parent_selects = [
        statement
        for statement in statements
        if "FROM safety_reports" in statement
    ]
    assert len(parent_selects) == 1
    assert "INDEXED BY safety_reports_mint_latest_idx" in parent_selects[0]
    assert "ORDER BY id DESC LIMIT 1" in parent_selects[0]
    assert f"id={older_id}" not in parent_selects[0]
    assert any(
        "FROM holder_evidence" in statement
        and f"safety_report_id={newest_childless_id}" in statement
        for statement in statements
    )
    assert all(
        f"safety_report_id={older_id}" not in statement
        and f"id={older_holder_id}" not in statement
        for statement in statements
    )

    available_id = insert_parent(checked_at=21.0, hard_fails_json='["warning"]')
    available_holder_id = insert_available_holder(
        available_id, holder_observed_at=20.5,
    )
    available = validated_latest_report_as_of(conn, mint="MINT", as_of=30.0)
    assert available == ValidatedSafetyHolder(
        safety_report_id=available_id,
        mint="MINT",
        checked_at=21.0,
        risk_score=5.0,
        hard_fails=("warning",),
        safety_inputs_hash="a" * 64,
        holder_evidence_id=available_holder_id,
        holder=HolderEvidenceDraft(
            sampled_token_accounts=20,
            distinct_non_curve_owners=12,
            top10_non_curve_owner_share_pct=25.0,
            holder_observed_at=20.5,
            unavailable_reason="",
            inputs_hash="b" * 64,
        ),
        holder_unavailable_reason="",
    )
    assert validated_report_by_id(
        conn, report_id=available_id, expected_mint="MINT",
    ) == available
    with pytest.raises(EvidenceIntegrityError, match="safety report mint mismatch"):
        validated_report_by_id(
            conn, report_id=available_id, expected_mint="OTHER",
        )

    future_id = insert_parent(checked_at=40.0)
    insert_available_holder(future_id, holder_observed_at=39.0)
    assert validated_latest_report_as_of(
        conn, mint="MINT", as_of=30.0,
    ) is None

    malformed_holder_id = insert_parent(checked_at=25.0)
    insert_available_holder(malformed_holder_id, holder_observed_at=26.0)
    malformed_holder = validated_latest_report_as_of(
        conn, mint="MINT", as_of=30.0,
    )
    assert malformed_holder is not None
    assert malformed_holder.safety_report_id == malformed_holder_id
    assert malformed_holder.holder_evidence_id is None
    assert malformed_holder.holder is None
    assert (
        malformed_holder.holder_unavailable_reason
        == "holder_evidence_malformed"
    )

    conn.execute("DROP TRIGGER p3_safety_report_shape_guard")
    conn.execute(
        "INSERT INTO safety_reports("
        "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
        ") VALUES ('MINT',25.0,'{}',5.0,?)",
        ("c" * 64,),
    )
    conn.commit()
    assert validated_latest_report_as_of(
        conn, mint="MINT", as_of=30.0,
    ) is None
    conn.close()


def test_latest_report_equal_to_asof_is_unavailable(tmp_path):
    from memebot.store import validated_latest_report_as_of

    conn = open_db(tmp_path / "validated-latest-equality.db", migration_clock=lambda: 1.0)
    report_id = conn.execute(
        "INSERT INTO safety_reports("
        "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
        ") VALUES ('MINT',10.0,'[]',5.0,?)",
        ("a" * 64,),
    ).lastrowid
    conn.execute(
        "INSERT INTO holder_evidence("
        "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
        "top10_non_curve_owner_share_pct,holder_observed_at,"
        "unavailable_reason,inputs_hash"
        ") VALUES (?,20,12,25.0,9.0,'',?)",
        (report_id, "b" * 64),
    )
    conn.commit()

    assert validated_latest_report_as_of(
        conn, mint="MINT", as_of=10.0,
    ) is None
    conn.close()


def test_safety_report_explicit_id_is_rejected(tmp_path):
    from memebot.store import validated_latest_report_as_of

    conn = open_db(tmp_path / "safety-report-id-guard.db", migration_clock=lambda: 1.0)
    legitimate_id = conn.execute(
        "INSERT INTO safety_reports("
        "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
        ") VALUES ('MINT',10.0,'[]',5.0,?)",
        ("a" * 64,),
    ).lastrowid
    conn.execute(
        "INSERT INTO holder_evidence("
        "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
        "top10_non_curve_owner_share_pct,holder_observed_at,"
        "unavailable_reason,inputs_hash"
        ") VALUES (?,20,12,25.0,9.0,'',?)",
        (legitimate_id, "b" * 64),
    )
    conn.commit()

    forged_id = legitimate_id + 1_000
    forged_was_rejected = False
    try:
        conn.execute(
            "INSERT INTO safety_reports("
            "id,mint,checked_at,hard_fails_json,risk_score,inputs_hash"
            ") VALUES (?,'MINT',11.0,'[]',1.0,?)",
            (forged_id, "c" * 64),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        assert str(exc) == "invalid safety report shape"
        forged_was_rejected = True
        conn.rollback()

    latest = validated_latest_report_as_of(conn, mint="MINT", as_of=20.0)
    assert latest is not None
    assert latest.safety_report_id == legitimate_id
    assert forged_was_rejected
    assert conn.execute(
        "SELECT count(*) FROM safety_reports WHERE id=?",
        (forged_id,),
    ).fetchone()[0] == 0
    conn.close()


def test_p3_early_buyer_is_report_linked_strict_asof_and_fail_closed(tmp_path):
    import json

    from memebot.early_buyers import EarlyBuyerEvidenceDraft
    from memebot.store import validated_early_buyer_for_report

    conn = open_db(tmp_path / "report-linked-early-buyers.db", migration_clock=lambda: 1.0)

    def insert_report(*, mint="MINT", checked_at=10.0):
        report_id = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
            ") VALUES (?,?,'[]',5.0,?)",
            (mint, checked_at, "a" * 64),
        ).lastrowid
        conn.commit()
        return report_id

    def insert_early(
        *,
        report_id,
        mint="MINT",
        checked_at=9.0,
        buyers=("W1", "W2"),
        unavailable_reason="",
        inputs_hash="b" * 64,
        buyers_json=None,
    ):
        if buyers_json is None:
            buyers_json = json.dumps(
                list(buyers),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        row_id = conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id"
            ") VALUES (?,?,?,?,?,?)",
            (
                mint,
                checked_at,
                buyers_json,
                unavailable_reason,
                inputs_hash,
                report_id,
            ),
        ).lastrowid
        conn.commit()
        return row_id

    available_report_id = insert_report()
    available_early_id = insert_early(report_id=available_report_id)
    statements = []
    conn.set_trace_callback(statements.append)
    assert validated_early_buyer_for_report(
        conn,
        report_id=available_report_id,
        expected_mint="MINT",
        as_of=10.0,
    ) == (
        available_early_id,
        EarlyBuyerEvidenceDraft(
            checked_at=9.0,
            buyers=("W1", "W2"),
            unavailable_reason="",
            inputs_hash="b" * 64,
        ),
    )
    conn.set_trace_callback(None)
    early_selects = [
        statement
        for statement in statements
        if "FROM early_buyer_reads" in statement
    ]
    assert len(early_selects) == 1
    assert "WHERE safety_report_id=" in early_selects[0]
    assert "ORDER BY checked_at" not in early_selects[0]

    assert validated_early_buyer_for_report(
        conn,
        report_id=available_report_id,
        expected_mint="MINT",
        as_of=9.0,
    ) is None
    assert validated_early_buyer_for_report(
        conn,
        report_id=available_report_id,
        expected_mint="OTHER",
        as_of=10.0,
    ) is None

    unavailable_report_id = insert_report(checked_at=20.0)
    unavailable_early_id = insert_early(
        report_id=unavailable_report_id,
        checked_at=19.0,
        buyers=(),
        unavailable_reason="no_signatures",
        inputs_hash="c" * 64,
    )
    assert validated_early_buyer_for_report(
        conn,
        report_id=unavailable_report_id,
        expected_mint="MINT",
        as_of=20.0,
    ) == (
        unavailable_early_id,
        EarlyBuyerEvidenceDraft(
            checked_at=19.0,
            buyers=(),
            unavailable_reason="no_signatures",
            inputs_hash="c" * 64,
        ),
    )

    childless_report_id = insert_report(checked_at=30.0)
    conn.execute(
        "INSERT INTO early_buyer_reads("
        "mint,checked_at,buyers_json,unavailable_reason,inputs_hash"
        ") VALUES ('MINT',29.0,'[\"UNLINKED\"]','',?)",
        ("d" * 64,),
    )
    conn.commit()
    assert validated_early_buyer_for_report(
        conn,
        report_id=childless_report_id,
        expected_mint="MINT",
        as_of=30.0,
    ) is None
    assert validated_early_buyer_for_report(
        conn,
        report_id=999_999,
        expected_mint="MINT",
        as_of=30.0,
    ) is None

    over_limit_report_id = insert_report(checked_at=35.0)
    insert_early(
        report_id=over_limit_report_id,
        checked_at=34.0,
        buyers=tuple(f"W{i:04d}" for i in range(1_001)),
        inputs_hash="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    )
    assert validated_early_buyer_for_report(
        conn,
        report_id=over_limit_report_id,
        expected_mint="MINT",
        as_of=35.0,
    ) is None

    conn.execute("DROP TRIGGER p3_early_buyer_shape_guard")
    conn.execute("DROP TRIGGER early_buyer_report_guard")
    malformed_cases = (
        {
            "report_checked_at": 40.0,
            "child_mint": "MINT",
            "child_checked_at": 41.0,
            "buyers_json": '["W1"]',
            "reason": "",
            "inputs_hash": "f" * 64,
        },
        {
            "report_checked_at": 45.0,
            "child_mint": "MINT",
            "child_checked_at": 44.0,
            "buyers_json": '["W1", "W2"]',
            "reason": "",
            "inputs_hash": "1" * 64,
        },
        {
            "report_checked_at": 50.0,
            "child_mint": "MINT",
            "child_checked_at": 49.0,
            "buyers_json": '["W1","W1"]',
            "reason": "",
            "inputs_hash": "2" * 64,
        },
        {
            "report_checked_at": 55.0,
            "child_mint": "OTHER",
            "child_checked_at": 54.0,
            "buyers_json": '["W1"]',
            "reason": "",
            "inputs_hash": "3" * 64,
        },
        {
            "report_checked_at": 60.0,
            "child_mint": "MINT",
            "child_checked_at": 59.0,
            "buyers_json": "[]",
            "reason": "provider_exception_text",
            "inputs_hash": "4" * 64,
        },
        {
            "report_checked_at": 65.0,
            "child_mint": "MINT",
            "child_checked_at": 64.0,
            "buyers_json": '["W1"]',
            "reason": "",
            "inputs_hash": "short",
        },
    )
    for case in malformed_cases:
        report_id = insert_report(checked_at=case["report_checked_at"])
        conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id"
            ") VALUES (?,?,?,?,?,?)",
            (
                case["child_mint"],
                case["child_checked_at"],
                case["buyers_json"],
                case["reason"],
                case["inputs_hash"],
                report_id,
            ),
        )
        conn.commit()
        assert validated_early_buyer_for_report(
            conn,
            report_id=report_id,
            expected_mint="MINT",
            as_of=case["report_checked_at"] + 2.0,
        ) is None

    conn.execute("DROP TRIGGER p3_safety_report_shape_guard")
    malformed_parent_id = conn.execute(
        "INSERT INTO safety_reports("
        "mint,checked_at,hard_fails_json,risk_score,inputs_hash"
        ") VALUES ('MINT',70.0,'{}',5.0,?)",
        ("5" * 64,),
    ).lastrowid
    conn.execute(
        "INSERT INTO early_buyer_reads("
        "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
        "safety_report_id"
        ") VALUES ('MINT',69.0,'[\"W1\"]','',?,?)",
        ("6" * 64, malformed_parent_id),
    )
    conn.commit()
    assert validated_early_buyer_for_report(
        conn,
        report_id=malformed_parent_id,
        expected_mint="MINT",
        as_of=71.0,
    ) is None
    conn.close()


def test_p3_smart_wallet_lookup_is_bounded_summary_strict_asof(tmp_path):
    from collections.abc import Sequence

    from memebot.store import (
        record_wallet_pnl_event,
        validated_smart_wallets_for_buyers,
    )

    conn = open_db(tmp_path / "p3-smart-wallet-summary.db", migration_clock=lambda: 1.0)

    def record(*, at, wallet, mint, pnl):
        return record_wallet_pnl_event(
            conn,
            at=at,
            wallet=wallet,
            mint=mint,
            realized_pnl_sol=pnl,
            source="test",
            detail={},
        )

    record(at=10.0, wallet="SMART", mint="A", pnl=0.75)
    smart_last_id = record(at=20.0, wallet="SMART", mint="B", pnl=0.5)
    record(at=15.0, wallet="BELOW_COUNT", mint="C", pnl=100.0)
    record(at=12.0, wallet="BELOW_PNL", mint="D", pnl=0.4)
    record(at=18.0, wallet="BELOW_PNL", mint="E", pnl=0.5)
    record(at=30.0, wallet="FUTURE", mint="F", pnl=10.0)
    other_first_id = record(at=11.0, wallet="OTHER", mint="G", pnl=1.0)
    record(at=19.0, wallet="OTHER", mint="H", pnl=1.0)
    single_id = record(at=14.0, wallet="SINGLE", mint="I", pnl=-5.0)
    single_at = conn.execute(
        "SELECT at FROM wallet_pnl_events WHERE id=?", (single_id,),
    ).fetchone()[0]

    statements = []
    conn.set_trace_callback(statements.append)
    selected = validated_smart_wallets_for_buyers(
        conn,
        buyers=("SMART", "MISSING", "BELOW_COUNT", "BELOW_PNL"),
        as_of=25.0,
        max_buyers=4,
        min_events=2,
        min_realized_pnl_sol=1.0,
    )
    conn.set_trace_callback(None)

    assert selected == {
        "SMART": {"events": 2, "realized_pnl_sol": 1.25},
    }
    all_selects = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith("SELECT")
    ]
    assert len(all_selects) == 4
    assert all(
        "FROM wallet_pnl_summary" in statement
        and "LEFT JOIN wallet_pnl_events AS e ON e.id=s.last_event_id" in statement
        and "WHERE s.wallet=" in statement
        for statement in all_selects
    )

    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=("SMART",),
        as_of=25.0,
        max_buyers=1,
        min_events=2,
        min_realized_pnl_sol=1.25,
    ) == {"SMART": {"events": 2, "realized_pnl_sol": 1.25}}
    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=(),
        as_of=25.0,
        max_buyers=1,
        min_events=1,
        min_realized_pnl_sol=0.0,
    ) == {}

    for as_of in (30.0, 29.0):
        assert validated_smart_wallets_for_buyers(
            conn,
            buyers=("SMART", "FUTURE"),
            as_of=as_of,
            max_buyers=2,
            min_events=1,
            min_realized_pnl_sol=0.0,
        ) is None

    conn.execute(
        "UPDATE wallet_pnl_summary SET event_count=1.5 WHERE wallet='SMART'"
    )
    conn.commit()
    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=("SMART",),
        as_of=25.0,
        max_buyers=1,
        min_events=1,
        min_realized_pnl_sol=0.0,
    ) is None
    conn.execute(
        "UPDATE wallet_pnl_summary SET event_count=2 WHERE wallet='SMART'"
    )
    conn.commit()

    conn.execute(
        "UPDATE wallet_pnl_summary SET last_event_id=? WHERE wallet='SMART'",
        (other_first_id,),
    )
    conn.commit()
    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=("SMART",),
        as_of=25.0,
        max_buyers=1,
        min_events=1,
        min_realized_pnl_sol=0.0,
    ) is None
    conn.execute(
        "UPDATE wallet_pnl_summary SET last_event_id=? WHERE wallet='SMART'",
        (smart_last_id,),
    )
    conn.commit()

    conn.execute(
        "UPDATE wallet_pnl_summary "
        "SET realized_pnl_sol=1000000000000.0 "
        "WHERE wallet='SINGLE'"
    )
    conn.commit()
    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=("SINGLE",),
        as_of=25.0,
        max_buyers=1,
        min_events=1,
        min_realized_pnl_sol=0.0,
    ) is None
    conn.execute(
        "UPDATE wallet_pnl_summary "
        "SET realized_pnl_sol=-5.0,last_at=20.0 "
        "WHERE wallet='SINGLE'"
    )
    conn.commit()
    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=("SINGLE",),
        as_of=25.0,
        max_buyers=1,
        min_events=1,
        min_realized_pnl_sol=-10.0,
    ) is None
    conn.execute(
        "UPDATE wallet_pnl_summary "
        "SET event_count=999,last_at=? "
        "WHERE wallet='SINGLE'",
        (single_at,),
    )
    conn.commit()
    assert validated_smart_wallets_for_buyers(
        conn,
        buyers=("SINGLE",),
        as_of=25.0,
        max_buyers=1,
        min_events=1,
        min_realized_pnl_sol=-10.0,
    ) is None

    class OversizedSequence(Sequence):
        def __len__(self):
            return 2

        def __getitem__(self, index):
            raise AssertionError("oversized buyers must not be materialized")

        def __iter__(self):
            raise AssertionError("oversized buyers must not be materialized")

    class LyingSequence(Sequence):
        def __len__(self):
            return 1

        def __getitem__(self, index):
            return "SMART"

    invalid_calls = (
        {"buyers": ("SMART", "SMART")},
        {"buyers": ("SMART", "")},
        {"buyers": ("SMART", "MISSING"), "max_buyers": 1},
        {"buyers": OversizedSequence(), "max_buyers": 1},
        {"buyers": LyingSequence(), "max_buyers": 1},
        {"buyers": "SMART"},
        {"as_of": True},
        {"max_buyers": True},
        {"max_buyers": 0},
        {"max_buyers": 1_001},
        {"min_events": True},
        {"min_events": 0},
        {"min_realized_pnl_sol": True},
        {"min_realized_pnl_sol": float("nan")},
        {"min_realized_pnl_sol": 1_000_000_000_000.1},
        {"min_realized_pnl_sol": 10**10_000},
    )
    defaults = {
        "buyers": ("SMART",),
        "as_of": 25.0,
        "max_buyers": 1,
        "min_events": 1,
        "min_realized_pnl_sol": 0.0,
    }
    for overrides in invalid_calls:
        with pytest.raises(ValueError):
            validated_smart_wallets_for_buyers(
                conn,
                **(defaults | overrides),
            )

    conn.close()


def test_decision_generation_observations_are_atomic_and_cardinal(tmp_path):
    import hashlib
    import json
    import sqlite3
    from queue import Queue
    from threading import Thread

    from memebot.canonical import (
        CanonicalObservationDraft,
        canonical_generation_hash,
    )
    from memebot.store import (
        EvidenceIntegrityError,
        allocate_p3_causal_wall,
        p3_fee_sum_json,
        p3_immediate_transaction,
        record_decision_with_canonical_observations,
        save_safety_report,
    )

    conn = open_db(tmp_path / "canonical-decisions.db", migration_clock=lambda: 1.0)
    for mint in ("WIN", "LOSE"):
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json)"
            " VALUES (?,1.0,'CLIMBING',1.0,'{}')",
            (mint,),
        )
    conn.commit()

    report_ids = {}
    holder_ids = {}
    for index, mint in enumerate(("WIN", "LOSE"), start=2):
        report_ids[mint] = save_safety_report(
            conn,
            mint=mint,
            raw_completed_at=float(index),
            segment="CLIMBING",
            hard_fails=(),
            risk_score=5.0,
            results_json="[]",
            inputs_hash="2222222222222222222222222222222222222222222222222222222222222222",
        )
        holder_ids[mint] = conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,"
            "unavailable_reason,inputs_hash"
            ") VALUES (?,?,?,10.0,1.5,'',?)",
            (report_ids[mint], 20, 15, str(index + 2) * 64),
        ).lastrowid
        conn.commit()

    config_hash = "a" * 64
    generation_hash = canonical_generation_hash(
        cluster_key="pepe:pepe",
        eligible=(
            {
                "mint": "WIN",
                "safety_report_id": report_ids["WIN"],
                "holder_evidence_id": holder_ids["WIN"],
            },
            {
                "mint": "LOSE",
                "safety_report_id": report_ids["LOSE"],
                "holder_evidence_id": holder_ids["LOSE"],
            },
        ),
        canonical_mint="WIN",
        resolver_version="canonical-v1",
        weights_version="canonical-weighted-v1",
        config_hash=config_hash,
    )

    def social(*, value):
        return {
            "value": value,
            "present": value is not None,
            "reuse": value is not None,
            "cluster_conflict": False,
            "metadata_conflict": False,
        }

    def candidate(
        *,
        mint,
        identity_at,
        rank,
        rank_points,
        liquidity_sol,
        curve_progress_pct,
        distinct_owners,
        top10_share_pct,
        creator,
        creator_successes,
        creator_event_ids,
    ):
        real_sol_reserves = int(liquidity_sol * 1_000_000_000)
        return {
            "mint": mint,
            "p3_identity_ingested_at": identity_at,
            "state": "CLIMBING",
            "rugged": 0,
            "normalized_name": "pepe",
            "normalized_symbol": "pepe",
            "creator": creator,
            "identity_observed_at": {
                "name": identity_at,
                "symbol": identity_at,
            },
            "identity_conflicts": [],
            "eligible": True,
            "ineligible_reason": "",
            "safety_report_id": report_ids[mint],
            "safety_checked_at": 2.0,
            "safety_inputs_hash": "2" * 64,
            "safety_hard_fails": [],
            "safety_risk_score": 5.0,
            "holder_evidence_id": holder_ids[mint],
            "holder_inputs_hash": (
                "4" * 64 if mint == "WIN" else "5" * 64
            ),
            "holder_observed_at": 1.5,
            "liquidity_source": "curve_snapshot",
            "liquidity_observed_at": 3.0,
            "raw": {
                "liquidity_sol": liquidity_sol,
                "curve_progress_pct": curve_progress_pct,
                "curve_snapshot": {
                    "t_wall": 3.0,
                    "t_mono": 8.0,
                    "virtual_sol_reserves": 70_000_000_000,
                    "virtual_token_reserves": 70_000_000_000_000,
                    "real_sol_reserves": real_sol_reserves,
                    "real_token_reserves": 400_000_000_000_000,
                    "spot_price_sol": 0.000001,
                },
                "sampled_token_accounts": 20,
                "distinct_non_curve_owners": distinct_owners,
                "top10_non_curve_owner_share_pct": top10_share_pct,
                "creator_prior_successes": creator_successes,
                "creator_prior_rugs": 0,
                "creator_reputation_event_ids": creator_event_ids,
                "social": {
                    "uri": social(value="ipfs://x"),
                    "website": social(value=None),
                    "twitter": social(value=None),
                    "telegram": social(value=None),
                },
            },
            "components_ppm": {
                "first_mover": 1_000_000 if mint == "WIN" else 0,
                "liquidity": (
                    500_000 if mint == "WIN" else 400_000
                ),
                "holder": 541_667 if mint == "WIN" else 500_000,
                "creator": 666_667 if mint == "WIN" else 500_000,
                "social": 250_000,
            },
            "rank_points": rank_points,
            "rank": rank,
        }

    def vector(*, subject, resolved_at):
        candidates = [
            candidate(
                mint="LOSE",
                identity_at=0.6,
                rank=2,
                rank_points=2_875_000_000,
                liquidity_sol=34.0,
                curve_progress_pct=40.0,
                distinct_owners=10,
                top10_share_pct=15.0,
                creator="creatorB",
                creator_successes=0,
                creator_event_ids=[],
            ),
            candidate(
                mint="WIN",
                identity_at=0.5,
                rank=1,
                rank_points=6_958_334_500,
                liquidity_sol=42.5,
                curve_progress_pct=50.0,
                distinct_owners=15,
                top10_share_pct=20.0,
                creator="creatorA",
                creator_successes=1,
                creator_event_ids=[31],
            ),
        ]
        ranking_inputs = {
            "subject_mint": subject,
            "target_report_id": report_ids[subject],
            "latest_target_report_id": report_ids[subject],
            "resolved_at": resolved_at,
            "cluster_key": "pepe:pepe",
            "resolver_version": "canonical-v1",
            "weights_version": "canonical-weighted-v1",
            "config_hash": config_hash,
            "counterfactual_horizons_s": [60.0],
            "limits": {
                "max_cluster_candidates": 50,
                "liquidity_max_age_s": 30.0,
                "holder_max_age_s": 900.0,
                "comparison_price_max_age_s": 300.0,
            },
            "component_parameters": {
                "graduation_sol": 85.0,
                "holder_owner_target": 20,
                "top10_holder_max_pct": 30.0,
                "token_decimals": 6,
                "creator_reputation_as_of": resolved_at,
            },
            "weights_bps": {
                "first_mover": 3500,
                "liquidity": 2500,
                "holder": 2000,
                "creator": 1500,
                "social": 500,
            },
            "social_weights_bps": {
                "uri": 2500,
                "website": 2500,
                "twitter": 2500,
                "telegram": 2500,
            },
            "candidates": candidates,
        }
        inputs_json = json.dumps(
            ranking_inputs,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        rank = 1 if subject == "WIN" else 2
        return {
            "velocity": 1.0,
            "canonical": {
                "resolver_version": "canonical-v1",
                "weights_version": "canonical-weighted-v1",
                "status": "CANONICAL" if rank == 1 else "SUPPRESSED",
                "reason": "canonical_selected" if rank == 1 else "copycat_cluster",
                "resolved_at": resolved_at,
                "cluster_key": "pepe:pepe",
                "cluster_size": 2,
                "eligible_cluster_size": 2,
                "canonical_mint": "WIN",
                "rank": rank,
                "rank_points": (
                    6_958_334_500 if rank == 1 else 2_875_000_000
                ),
                "generation_hash": generation_hash,
                "inputs_hash": hashlib.sha256(inputs_json.encode()).hexdigest(),
                "config_hash": config_hash,
                "ranking_order": ["WIN", "LOSE"],
                "ranking_inputs": ranking_inputs,
            },
        }

    def rehash_vector(value):
        ranking_json = json.dumps(
            value["canonical"]["ranking_inputs"],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        value["canonical"]["inputs_hash"] = hashlib.sha256(
            ranking_json.encode()
        ).hexdigest()

    observations = {
        "WIN": CanonicalObservationDraft(
            mint="WIN",
            is_subject=True,
            is_canonical=True,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=3.0,
            unavailable_reason="",
        ),
        "LOSE": CanonicalObservationDraft(
            mint="LOSE",
            is_subject=False,
            is_canonical=False,
            eligible=True,
            start_price_sol=0.000002,
            price_observed_at=3.0,
            unavailable_reason="",
        ),
    }

    def record(
        *,
        at,
        subject,
        rows,
        score,
        supplied_vector=None,
        selected_generation_hash=generation_hash,
        selected_action=None,
    ):
        return record_decision_with_canonical_observations(
            conn,
            at=at,
            mint=subject,
            segment="CLIMBING",
            action=(
                "BUY" if subject == "WIN" else "SKIP"
            ) if selected_action is None else selected_action,
            score=score,
            feature_vector=(
                vector(subject=subject, resolved_at=at)
                if supplied_vector is None
                else supplied_vector
            ),
            safety_report_id=report_ids[subject],
            config_hash=config_hash,
            generation_hash=selected_generation_hash,
            observations=rows,
            score_status="VALID",
            score_weights_version="climbing-v1",
            score_unavailable_reason="",
            planned_position_size_sol=0.1,
        )

    with p3_immediate_transaction(conn):
        first_at = allocate_p3_causal_wall(conn, raw_wall=10.0)
        first_id, first_observation_ids, first_primary = record(
            at=first_at,
            subject="WIN",
            rows=(observations["LOSE"], observations["WIN"]),
            score=90.0,
        )
    assert first_primary is True
    assert tuple(
        row["mint"]
        for row in conn.execute(
            "SELECT mint FROM canonical_observations WHERE id IN (?,?)"
            " ORDER BY id",
            first_observation_ids,
        )
    ) == ("LOSE", "WIN")
    assert tuple(
        row["observed_at"]
        for row in conn.execute(
            "SELECT observed_at FROM canonical_observations"
            " WHERE decision_id=? ORDER BY id",
            (first_id,),
        )
    ) == (first_at, first_at)
    first_feature = json.loads(
        conn.execute(
            "SELECT feature_vector_json FROM decisions WHERE id=?", (first_id,)
        ).fetchone()[0]
    )
    assert first_feature["score_status"] == "VALID"
    assert first_feature["score_weights_version"] == "climbing-v1"
    assert first_feature["score_unavailable_reason"] == ""
    assert first_feature["canonical"]["planned_size_sol"] == 0.1

    repeated = {
        "WIN": CanonicalObservationDraft(
            mint="WIN",
            is_subject=False,
            is_canonical=True,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=3.0,
            unavailable_reason="",
        ),
        "LOSE": CanonicalObservationDraft(
            mint="LOSE",
            is_subject=True,
            is_canonical=False,
            eligible=True,
            start_price_sol=0.000002,
            price_observed_at=3.0,
            unavailable_reason="",
        ),
    }
    with p3_immediate_transaction(conn):
        second_at = allocate_p3_causal_wall(conn, raw_wall=11.0)
        _second_id, second_observation_ids, second_primary = record(
            at=second_at,
            subject="LOSE",
            rows=(repeated["WIN"], repeated["LOSE"]),
            score=80.0,
        )
    assert second_primary is False
    assert tuple(
        row["mint"]
        for row in conn.execute(
            "SELECT mint FROM canonical_observations WHERE id IN (?,?)"
            " ORDER BY id",
            second_observation_ids,
        )
    ) == ("WIN", "LOSE")
    assert tuple(
        conn.execute(
            "SELECT first_decision_id,created_at FROM canonical_generations"
            " WHERE generation_hash=?",
            (generation_hash,),
        ).fetchone()
    ) == (first_id, first_at)

    malformed_generation_hash = canonical_generation_hash(
        cluster_key="pepe:pepe",
        eligible=(
            {
                "mint": "WIN",
                "safety_report_id": report_ids["WIN"],
                "holder_evidence_id": holder_ids["WIN"],
            },
        ),
        canonical_mint="WIN",
        resolver_version="canonical-v1",
        weights_version="canonical-weighted-v1",
        config_hash=config_hash,
    )
    with p3_immediate_transaction(conn):
        malformed_peer_at = allocate_p3_causal_wall(conn, raw_wall=12.0)
        malformed_peer_vector = vector(
            subject="WIN", resolved_at=malformed_peer_at
        )
        malformed_canonical = malformed_peer_vector["canonical"]
        malformed_peer = malformed_canonical["ranking_inputs"]["candidates"][0]
        malformed_peer.update(
            {
                "normalized_name": "",
                "normalized_symbol": "",
                "creator": "",
                "identity_observed_at": {},
                "identity_conflicts": [],
                "eligible": False,
                "ineligible_reason": "canonical_internal_error",
                "safety_report_id": None,
                "safety_checked_at": None,
                "safety_inputs_hash": None,
                "safety_hard_fails": None,
                "safety_risk_score": None,
                "holder_evidence_id": None,
                "holder_inputs_hash": None,
                "holder_observed_at": None,
                "liquidity_source": None,
                "liquidity_observed_at": None,
                "raw": {
                    key: None for key in malformed_peer["raw"]
                },
                "components_ppm": {},
                "rank_points": None,
                "rank": None,
            }
        )
        malformed_canonical.update(
            {
                "eligible_cluster_size": 1,
                "generation_hash": malformed_generation_hash,
                "ranking_order": ["WIN"],
            }
        )
        malformed_canonical["ranking_inputs"]["candidates"][1]["raw"][
            "social"
        ]["uri"]["reuse"] = False
        rehash_vector(malformed_peer_vector)
        malformed_rows = (
            CanonicalObservationDraft(
                mint="LOSE",
                is_subject=False,
                is_canonical=False,
                eligible=False,
                start_price_sol=0.000002,
                price_observed_at=3.0,
                unavailable_reason="",
            ),
            observations["WIN"],
        )
        _, _, malformed_primary = record(
            at=malformed_peer_at,
            subject="WIN",
            rows=malformed_rows,
            score=90.0,
            supplied_vector=malformed_peer_vector,
            selected_generation_hash=malformed_generation_hash,
        )
    assert malformed_primary is True

    with p3_immediate_transaction(conn):
        invalid_creator_at = allocate_p3_causal_wall(conn, raw_wall=13.0)
        invalid_creator_vector = vector(
            subject="LOSE", resolved_at=invalid_creator_at
        )
        invalid_creator = invalid_creator_vector["canonical"][
            "ranking_inputs"
        ]["candidates"][0]
        invalid_creator["creator"] = "x" * 129
        invalid_creator["components_ppm"]["creator"] = 0
        invalid_creator["rank_points"] = 2_125_000_000
        invalid_creator_vector["canonical"]["rank_points"] = 2_125_000_000
        rehash_vector(invalid_creator_vector)
        _, _, invalid_creator_primary = record(
            at=invalid_creator_at,
            subject="LOSE",
            rows=(repeated["WIN"], repeated["LOSE"]),
            score=80.0,
            supplied_vector=invalid_creator_vector,
        )
    assert invalid_creator_primary is False

    with p3_immediate_transaction(conn):
        subject_only_at = allocate_p3_causal_wall(conn, raw_wall=14.0)
        subject_only_vector = vector(
            subject="WIN", resolved_at=subject_only_at
        )
        subject_only_canonical = subject_only_vector["canonical"]
        subject_only_canonical.update(
            {
                "status": "UNRESOLVED",
                "reason": "canonical_target_not_live",
                "cluster_key": "",
                "cluster_size": 0,
                "eligible_cluster_size": 0,
                "canonical_mint": None,
                "rank": None,
                "rank_points": None,
                "generation_hash": None,
                "ranking_order": [],
            }
        )
        subject_only_inputs = subject_only_canonical["ranking_inputs"]
        subject_only_inputs.update(
            {
                "latest_target_report_id": None,
                "cluster_key": "",
                "candidates": [],
            }
        )
        rehash_vector(subject_only_vector)
        subject_only_observation = CanonicalObservationDraft(
            mint="WIN",
            is_subject=True,
            is_canonical=False,
            eligible=False,
            start_price_sol=0.000001,
            price_observed_at=3.0,
            unavailable_reason="",
        )
        _, subject_only_ids, subject_only_primary = record(
            at=subject_only_at,
            subject="WIN",
            rows=(subject_only_observation,),
            score=90.0,
            supplied_vector=subject_only_vector,
            selected_generation_hash=None,
            selected_action="SKIP",
        )
    assert len(subject_only_ids) == 1
    assert subject_only_primary is False

    with p3_immediate_transaction(conn):
        negative_mono_at = allocate_p3_causal_wall(conn, raw_wall=15.0)
        negative_mono_vector = vector(
            subject="WIN", resolved_at=negative_mono_at
        )
        negative_mono_vector["canonical"]["ranking_inputs"]["candidates"][0][
            "raw"
        ]["curve_snapshot"]["t_mono"] = -1.0
        rehash_vector(negative_mono_vector)
        _, _, negative_mono_primary = record(
            at=negative_mono_at,
            subject="WIN",
            rows=(observations["LOSE"], observations["WIN"]),
            score=90.0,
            supplied_vector=negative_mono_vector,
        )
    assert negative_mono_primary is False

    committed_counts = tuple(
        conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in (
            "decisions",
            "canonical_generations",
            "canonical_observations",
            "canonical_pending_current",
        )
    )
    committed_wall = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]

    def assert_rolled_back():
        assert tuple(
            conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "decisions",
                "canonical_generations",
                "canonical_observations",
                "canonical_pending_current",
            )
        ) == committed_counts
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == committed_wall

    with pytest.raises(RuntimeError, match="one-shot"):
        with p3_immediate_transaction(conn):
            duplicate_at = allocate_p3_causal_wall(conn, raw_wall=16.0)
            record(
                at=duplicate_at,
                subject="WIN",
                rows=(observations["LOSE"], observations["WIN"]),
                score=90.0,
            )
            try:
                record(
                    at=duplicate_at,
                    subject="WIN",
                    rows=(observations["LOSE"], observations["WIN"]),
                    score=90.0,
                )
            except RuntimeError:
                pass
    assert_rolled_back()

    with pytest.raises(RuntimeError, match="one-shot"):
        with p3_immediate_transaction(conn):
            allocate_p3_causal_wall(conn, raw_wall=16.0)
            try:
                allocate_p3_causal_wall(conn, raw_wall=17.0)
            except RuntimeError:
                pass
    assert_rolled_back()

    with pytest.raises(RuntimeError, match="exactly one"):
        with p3_immediate_transaction(conn):
            allocate_p3_causal_wall(conn, raw_wall=16.0)
    assert_rolled_back()

    sql_failure_version = "canonical-sql-failure"
    sql_failure_generation_hash = canonical_generation_hash(
        cluster_key="pepe:pepe",
        eligible=(
            {
                "mint": "WIN",
                "safety_report_id": report_ids["WIN"],
                "holder_evidence_id": holder_ids["WIN"],
            },
            {
                "mint": "LOSE",
                "safety_report_id": report_ids["LOSE"],
                "holder_evidence_id": holder_ids["LOSE"],
            },
        ),
        canonical_mint="WIN",
        resolver_version=sql_failure_version,
        weights_version="canonical-weighted-v1",
        config_hash=config_hash,
    )
    conn.execute(
        "CREATE TEMP TRIGGER fail_canonical_observation_insert "
        "BEFORE INSERT ON canonical_observations BEGIN "
        "SELECT RAISE(ABORT,'forced observation failure'); END"
    )
    with pytest.raises(RuntimeError, match="poisoned"):
        with p3_immediate_transaction(conn):
            sql_failure_at = allocate_p3_causal_wall(conn, raw_wall=16.0)
            sql_failure_vector = vector(
                subject="WIN", resolved_at=sql_failure_at
            )
            sql_failure_vector["canonical"]["resolver_version"] = (
                sql_failure_version
            )
            sql_failure_vector["canonical"]["generation_hash"] = (
                sql_failure_generation_hash
            )
            sql_failure_vector["canonical"]["ranking_inputs"][
                "resolver_version"
            ] = sql_failure_version
            rehash_vector(sql_failure_vector)
            try:
                record(
                    at=sql_failure_at,
                    subject="WIN",
                    rows=(observations["LOSE"], observations["WIN"]),
                    score=90.0,
                    supplied_vector=sql_failure_vector,
                    selected_generation_hash=sql_failure_generation_hash,
                )
            except sqlite3.IntegrityError:
                pass
    conn.execute("DROP TRIGGER fail_canonical_observation_insert")
    assert_rolled_back()

    def attempt_payload_mutation(mutate):
        with pytest.raises(ValueError):
            with p3_immediate_transaction(conn):
                invalid_at = allocate_p3_causal_wall(conn, raw_wall=12.0)
                invalid_vector = vector(subject="WIN", resolved_at=invalid_at)
                mutate(invalid_vector)
                ranking_inputs = invalid_vector["canonical"]["ranking_inputs"]
                ranking_json = json.dumps(
                    ranking_inputs,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                invalid_vector["canonical"]["inputs_hash"] = hashlib.sha256(
                    ranking_json.encode()
                ).hexdigest()
                record(
                    at=invalid_at,
                    subject="WIN",
                    rows=(observations["LOSE"], observations["WIN"]),
                    score=90.0,
                    supplied_vector=invalid_vector,
                )
        assert_rolled_back()

    def omit_candidate_key(value):
        value["canonical"]["ranking_inputs"]["candidates"][0].pop("state")

    def add_raw_key(value):
        value["canonical"]["ranking_inputs"]["candidates"][0]["raw"][
            "unexpected"
        ] = None

    def change_raw_component(value):
        value["canonical"]["ranking_inputs"]["candidates"][0]["raw"][
            "liquidity_sol"
        ] = 35.0

    def change_persisted_component(value):
        value["canonical"]["ranking_inputs"]["candidates"][0][
            "components_ppm"
        ]["holder"] += 1

    def change_component_bps(value):
        weights = value["canonical"]["ranking_inputs"]["weights_bps"]
        weights["first_mover"] -= 1
        weights["liquidity"] += 1

    def invalidate_limit(value):
        value["canonical"]["ranking_inputs"]["limits"][
            "max_cluster_candidates"
        ] = 0

    def exceed_declared_bound(value):
        value["canonical"]["ranking_inputs"]["limits"][
            "max_cluster_candidates"
        ] = 1

    for mutation in (
        omit_candidate_key,
        add_raw_key,
        change_raw_component,
        change_persisted_component,
        change_component_bps,
        invalidate_limit,
        exceed_declared_bound,
    ):
        attempt_payload_mutation(mutation)

    unresolved_vector = vector(subject="LOSE", resolved_at=12.0)
    unresolved_vector["canonical"].update(
        {
            "status": "UNRESOLVED",
            "reason": "canonical_holder_evidence_unavailable",
            "rank": None,
            "rank_points": None,
        }
    )
    with pytest.raises(ValueError):
        with p3_immediate_transaction(conn):
            invalid_at = allocate_p3_causal_wall(conn, raw_wall=12.0)
            unresolved_vector["canonical"]["resolved_at"] = invalid_at
            unresolved_vector["canonical"]["ranking_inputs"][
                "resolved_at"
            ] = invalid_at
            unresolved_vector["canonical"]["ranking_inputs"][
                "component_parameters"
            ]["creator_reputation_as_of"] = invalid_at
            ranking_json = json.dumps(
                unresolved_vector["canonical"]["ranking_inputs"],
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            unresolved_vector["canonical"]["inputs_hash"] = hashlib.sha256(
                ranking_json.encode()
            ).hexdigest()
            record(
                at=invalid_at,
                subject="LOSE",
                rows=(repeated["WIN"], repeated["LOSE"]),
                score=80.0,
                supplied_vector=unresolved_vector,
            )
    assert_rolled_back()

    from collections.abc import Sequence

    class OversizedObservations(Sequence):
        iterated = False

        def __len__(self):
            return 51

        def __getitem__(self, index):
            raise AssertionError("oversized observations were materialized")

        def __iter__(self):
            self.iterated = True
            raise AssertionError("oversized observations were materialized")

    oversized = OversizedObservations()
    with pytest.raises(ValueError, match="observation cardinality"):
        with p3_immediate_transaction(conn):
            invalid_at = allocate_p3_causal_wall(conn, raw_wall=12.0)
            record(
                at=invalid_at,
                subject="WIN",
                rows=oversized,
                score=90.0,
            )
    assert oversized.iterated is False
    assert_rolled_back()

    with pytest.raises(ValueError, match="observation cardinality"):
        with p3_immediate_transaction(conn):
            invalid_at = allocate_p3_causal_wall(conn, raw_wall=12.0)
            record(
                at=invalid_at,
                subject="WIN",
                rows=(observations["WIN"],),
                score=90.0,
            )
    assert_rolled_back()

    cross_row = (
        observations["LOSE"],
        CanonicalObservationDraft(
            mint="WIN",
            is_subject=True,
            is_canonical=False,
            eligible=True,
            start_price_sol=0.000001,
            price_observed_at=3.0,
            unavailable_reason="",
        ),
    )
    with pytest.raises(ValueError, match="canonical observation"):
        with p3_immediate_transaction(conn):
            invalid_at = allocate_p3_causal_wall(conn, raw_wall=12.0)
            record(
                at=invalid_at,
                subject="WIN",
                rows=cross_row,
                score=90.0,
            )
    assert_rolled_back()

    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json)"
        " VALUES ('EXTRA',1.0,'CLIMBING',1.0,'{}')"
    )
    conn.commit()
    with pytest.raises(EvidenceIntegrityError):
        with p3_immediate_transaction(conn):
            reuse_at = allocate_p3_causal_wall(conn, raw_wall=16.0)
            conn.execute(
                "INSERT INTO canonical_observations("
                "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
                "start_price_sol,price_observed_at,price_source,"
                "unavailable_reason"
                ") VALUES (?,?,?,1,1,0,?,?,?,'')",
                (
                    first_id,
                    "EXTRA",
                    first_at,
                    0.000003,
                    3.0,
                    "curve_snapshot",
                ),
            )
            record(
                at=reuse_at,
                subject="WIN",
                rows=(observations["LOSE"], observations["WIN"]),
                score=90.0,
            )
    assert conn.execute(
        "SELECT count(*) FROM canonical_observations WHERE decision_id=?",
        (first_id,),
    ).fetchone()[0] == 2
    assert_rolled_back()

    reuse_version = "canonical-reuse-audit"
    reuse_generation_hash = canonical_generation_hash(
        cluster_key="pepe:pepe",
        eligible=(
            {
                "mint": "WIN",
                "safety_report_id": report_ids["WIN"],
                "holder_evidence_id": holder_ids["WIN"],
            },
            {
                "mint": "LOSE",
                "safety_report_id": report_ids["LOSE"],
                "holder_evidence_id": holder_ids["LOSE"],
            },
        ),
        canonical_mint="WIN",
        resolver_version=reuse_version,
        weights_version="canonical-weighted-v1",
        config_hash=config_hash,
    )
    with pytest.raises(EvidenceIntegrityError):
        with p3_immediate_transaction(conn):
            reuse_at = allocate_p3_causal_wall(conn, raw_wall=16.0)
            reuse_vector = vector(subject="WIN", resolved_at=reuse_at)
            reuse_vector["canonical"]["resolver_version"] = reuse_version
            reuse_vector["canonical"]["generation_hash"] = reuse_generation_hash
            reuse_vector["canonical"]["ranking_inputs"]["resolver_version"] = (
                reuse_version
            )
            rehash_vector(reuse_vector)
            malformed_first = json.loads(json.dumps(reuse_vector))
            malformed_first.update(
                {
                    "score_status": "VALID",
                    "score_weights_version": "climbing-v1",
                    "score_unavailable_reason": "",
                }
            )
            malformed_first["canonical"]["planned_size_sol"] = 0.1
            malformed_first["canonical"]["unexpected"] = True
            malformed_first_json = json.dumps(
                malformed_first,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            malformed_first_id = conn.execute(
                "INSERT INTO decisions("
                "at,mint,segment,action,score,feature_vector_json,"
                "safety_report_id,config_hash"
                ") VALUES (?,?,?,?,?,?,?,?)",
                (
                    reuse_at,
                    "WIN",
                    "CLIMBING",
                    "BUY",
                    90.0,
                    malformed_first_json,
                    report_ids["WIN"],
                    config_hash,
                ),
            ).lastrowid
            conn.execute(
                "INSERT INTO canonical_generations("
                "generation_hash,first_decision_id,created_at"
                ") VALUES (?,?,?)",
                (reuse_generation_hash, malformed_first_id, reuse_at),
            )
            record(
                at=reuse_at,
                subject="WIN",
                rows=(observations["LOSE"], observations["WIN"]),
                score=90.0,
                supplied_vector=reuse_vector,
                selected_generation_hash=reuse_generation_hash,
            )
    assert conn.execute(
        "SELECT count(*) FROM canonical_generations WHERE generation_hash=?",
        (reuse_generation_hash,),
    ).fetchone()[0] == 0
    assert_rolled_back()

    with pytest.raises(RuntimeError, match="p3_immediate_transaction"):
        record(
            at=13.0,
            subject="WIN",
            rows=(observations["LOSE"], observations["WIN"]),
            score=90.0,
        )
    with pytest.raises(RuntimeError, match="allocated causal T"):
        with p3_immediate_transaction(conn):
            record(
                at=13.0,
                subject="WIN",
                rows=(observations["LOSE"], observations["WIN"]),
                score=90.0,
            )
    with pytest.raises(RuntimeError, match="allocated causal T"):
        with p3_immediate_transaction(conn):
            allocated_at = allocate_p3_causal_wall(conn, raw_wall=13.0)
            record(
                at=allocated_at + 1.0,
                subject="WIN",
                rows=(observations["LOSE"], observations["WIN"]),
                score=90.0,
            )
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == committed_wall

    threaded_path = tmp_path / "canonical-decisions-threaded.db"
    threaded_conn = sqlite3.connect(threaded_path, check_same_thread=False)
    threaded_conn.create_function(
        "p3_fee_sum", 1, p3_fee_sum_json, deterministic=True
    )
    threaded_conn.row_factory = sqlite3.Row
    threaded_conn.execute("PRAGMA foreign_keys=ON")
    threaded_conn.execute("PRAGMA recursive_triggers=ON")
    conn.backup(threaded_conn)
    threaded_counts = tuple(
        threaded_conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in (
            "decisions",
            "canonical_generations",
            "canonical_observations",
            "canonical_pending_current",
        )
    )
    threaded_wall = threaded_conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]

    def threaded_record(at):
        return record_decision_with_canonical_observations(
            threaded_conn,
            at=at,
            mint="WIN",
            segment="CLIMBING",
            action="BUY",
            score=90.0,
            feature_vector=vector(subject="WIN", resolved_at=at),
            safety_report_id=report_ids["WIN"],
            config_hash=config_hash,
            generation_hash=generation_hash,
            observations=(observations["LOSE"], observations["WIN"]),
            score_status="VALID",
            score_weights_version="climbing-v1",
            score_unavailable_reason="",
            planned_position_size_sol=0.1,
        )

    foreign_outcome = Queue()

    def foreign_writer(at):
        try:
            threaded_record(at)
        except BaseException as exc:
            foreign_outcome.put(exc)
        else:
            foreign_outcome.put(None)

    with pytest.raises(RuntimeError, match="poisoned"):
        with p3_immediate_transaction(threaded_conn):
            threaded_at = allocate_p3_causal_wall(
                threaded_conn, raw_wall=20.0
            )
            worker = Thread(target=foreign_writer, args=(threaded_at,))
            worker.start()
            worker.join(timeout=5.0)
            assert worker.is_alive() is False
    foreign_error = foreign_outcome.get_nowait()
    assert isinstance(foreign_error, RuntimeError)
    assert "owner thread" in str(foreign_error)
    assert tuple(
        threaded_conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in (
            "decisions",
            "canonical_generations",
            "canonical_observations",
            "canonical_pending_current",
        )
    ) == threaded_counts
    assert threaded_conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == threaded_wall

    with p3_immediate_transaction(threaded_conn):
        clean_threaded_at = allocate_p3_causal_wall(
            threaded_conn, raw_wall=20.0
        )
        _, clean_threaded_observations, clean_threaded_primary = (
            threaded_record(clean_threaded_at)
        )
    assert len(clean_threaded_observations) == 2
    assert clean_threaded_primary is False
    assert threaded_conn.execute(
        "SELECT count(*) FROM decisions"
    ).fetchone()[0] == threaded_counts[0] + 1
    assert threaded_conn.execute(
        "SELECT count(*) FROM canonical_observations"
    ).fetchone()[0] == threaded_counts[2] + 2
    threaded_conn.close()

    conn.close()


def test_generic_decision_writer_rejects_reserved_canonical_payload(tmp_path):
    import ast
    import json
    from pathlib import Path

    from memebot.store import record_decision

    conn = open_db(tmp_path / "generic-decision-reservation.db")
    canonical_values = (
        None,
        False,
        "malformed",
        [],
        {},
        {"status": None},
        {"status": "malformed"},
        {"status": "CANONICAL"},
        {"status": "SUPPRESSED"},
        {"status": "UNRESOLVED"},
    )
    for value in canonical_values:
        with pytest.raises(
            ValueError,
            match=(
                "p3_immediate_transaction.*"
                "record_decision_with_canonical_observations"
            ),
        ):
            record_decision(
                conn,
                at=1.0,
                mint="RESERVED",
                segment="CLIMBING",
                action="SKIP",
                score=0.0,
                feature_vector={"canonical": value},
                config_hash="cfg",
            )
        assert conn.execute("SELECT count(*) FROM decisions").fetchone()[0] == 0

    noncanonical_vector = {
        "velocity_sol_per_s": 0.03,
        "diagnostics": {"canonical": {"status": "CANONICAL"}},
    }
    expected_json = json.dumps(noncanonical_vector)
    decision_id = record_decision(
        conn,
        at=2.0,
        mint="LEGACY",
        segment="CLIMBING",
        action="BUY",
        score=72.0,
        feature_vector=noncanonical_vector,
        config_hash="cfg",
        safety_report_id=None,
    )
    row = conn.execute(
        "SELECT at,mint,segment,action,score,feature_vector_json,"
        "safety_report_id,config_hash FROM decisions WHERE id=?",
        (decision_id,),
    ).fetchone()
    assert tuple(row) == (
        2.0,
        "LEGACY",
        "CLIMBING",
        "BUY",
        72.0,
        expected_json,
        None,
        "cfg",
    )
    assert conn.in_transaction is False

    root = Path(__file__).resolve().parents[1]
    production_paths = sorted((root / "src").rglob("*.py"))
    production_paths += sorted((root / "scripts").rglob("*.py"))
    repository_paths = production_paths + sorted((root / "tests").rglob("*.py"))
    scope_types = (
        ast.Module,
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
        ast.Lambda,
    )
    ambiguous = object()
    private_name = "_record_claimed_decision_with_canonical_observations"

    def parsed(path):
        tree = ast.parse(path.read_text())
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }

        def scope(node):
            current = node
            while current is not None and not isinstance(current, scope_types):
                current = parents.get(current)
            return current

        def location(node):
            current = parents.get(node)
            while current is not None and not isinstance(
                current, (ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                current = parents.get(current)
            return (
                path.relative_to(root).as_posix(),
                current.name if current is not None else "<module>",
                node.lineno,
                node.col_offset,
            )

        return tree, parents, scope, location

    def dotted_name(node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = dotted_name(node.value)
            return None if base is None else f"{base}.{node.attr}"
        return None

    def assignment_targets(node):
        if isinstance(node, ast.Assign):
            return node.targets, node.value
        if isinstance(node, ast.AnnAssign):
            return (node.target,), node.value
        if isinstance(node, ast.NamedExpr):
            return (node.target,), node.value
        return (), None

    def scope_chain(node_scope, parents):
        chain = []
        current = node_scope
        while current is not None:
            if isinstance(current, scope_types):
                chain.append(current)
            current = parents.get(current)
        return tuple(reversed(chain))

    def assignment_expressions(tree, parents, scope):
        by_scope = {}
        for node in ast.walk(tree):
            targets, value = assignment_targets(node)
            if value is None:
                continue
            node_scope = scope(node)
            values = by_scope.setdefault(node_scope, {})
            for target in targets:
                if not isinstance(target, ast.Name):
                    continue
                prior = values.get(target.id)
                values[target.id] = value if prior is None else ambiguous
        return by_scope

    def static_string(node, environment, seen=()):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = static_string(node.left, environment, seen)
            right = static_string(node.right, environment, seen)
            return None if left is None or right is None else left + right
        if isinstance(node, ast.JoinedStr):
            parts = []
            for part in node.values:
                if isinstance(part, ast.Constant) and isinstance(part.value, str):
                    parts.append(part.value)
                elif (
                    isinstance(part, ast.FormattedValue)
                    and part.conversion == -1
                    and part.format_spec is None
                ):
                    value = static_string(part.value, environment, seen)
                    if value is None:
                        return None
                    parts.append(value)
                else:
                    return None
            return "".join(parts)
        if isinstance(node, ast.Name):
            if node.id in seen:
                return None
            value = environment.get(node.id)
            if value is None or value is ambiguous:
                return None
            return static_string(value, environment, (*seen, node.id))
        return None

    decision_insert_occurrences = []
    for path in production_paths:
        tree, parents, scope, location = parsed(path)
        expressions = assignment_expressions(tree, parents, scope)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Constant, ast.BinOp, ast.JoinedStr)):
                continue
            parent = parents.get(node)
            if isinstance(parent, (ast.BinOp, ast.JoinedStr, ast.FormattedValue)):
                continue
            environment = {}
            for node_scope in scope_chain(scope(node), parents):
                environment.update(expressions.get(node_scope, {}))
            value = static_string(node, environment)
            if value is None:
                continue
            normalized = " ".join(value.casefold().split())
            if "insert into decisions" in normalized:
                decision_insert_occurrences.append(location(node))

    assert [item[:2] for item in decision_insert_occurrences] == [
        (
            "src/memebot/store.py",
            "_record_claimed_decision_with_canonical_observations",
        ),
        ("src/memebot/store.py", "record_decision"),
    ]
    assert len(decision_insert_occurrences) == 2

    private_definitions = []
    private_references = []
    private_reflections = []
    for path in production_paths:
        tree, parents, scope, location = parsed(path)
        expressions = assignment_expressions(tree, parents, scope)

        def static_environment(node):
            environment = {}
            for node_scope in scope_chain(scope(node), parents):
                environment.update(expressions.get(node_scope, {}))
            return environment

        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == private_name
            ):
                private_definitions.append(
                    (path.relative_to(root).as_posix(), node.name)
                )
            if (
                isinstance(node, ast.Name) and node.id == private_name
                or isinstance(node, ast.Attribute) and node.attr == private_name
            ):
                parent = parents.get(node)
                private_references.append(
                    (*location(node), isinstance(parent, ast.Call)
                     and parent.func is node)
                )
            if isinstance(node, ast.Call):
                environment = static_environment(node)
                reflected_values = (
                    *node.args,
                    *(keyword.value for keyword in node.keywords),
                )
                if any(
                    static_string(value, environment) == private_name
                    for value in reflected_values
                ):
                    private_reflections.append(location(node))
            elif isinstance(node, ast.Subscript):
                if (
                    static_string(node.slice, static_environment(node))
                    == private_name
                ):
                    private_reflections.append(location(node))

    assert private_definitions == [
        ("src/memebot/store.py", private_name)
    ]
    assert len(private_definitions) == 1
    assert [(*item[:2], item[4]) for item in private_references] == [
        (
            "src/memebot/store.py",
            "record_decision_with_canonical_observations",
            True,
        )
    ]
    assert len(private_references) == 1
    assert private_reflections == []

    store_module = "memebot.store"
    private_writer = f"{store_module}.{private_name}"
    strict_writer = (
        f"{store_module}.record_decision_with_canonical_observations"
    )
    generic_writer = f"{store_module}.record_decision"
    protected = {private_writer, strict_writer, generic_writer}
    protected_names = {target.rsplit(".", 1)[1] for target in protected}
    protected_calls = {target: [] for target in protected}
    external_private_imports = []
    unresolved_aliases = []

    for path in repository_paths:
        tree, parents, scope, location = parsed(path)
        bindings = {}

        def bind(node_scope, name, target, node):
            scoped = bindings.setdefault(node_scope, {})
            prior = scoped.get(name)
            if prior is not None and prior != target:
                unresolved_aliases.append((*location(node), name))
                return False
            if prior == target:
                return False
            scoped[name] = target
            return True

        module_scope = tree
        relative_path = path.relative_to(root)
        if relative_path.as_posix() == "src/memebot/store.py":
            for target in protected:
                bind(module_scope, target.rsplit(".", 1)[1], target, tree)

        def import_from_module(node):
            if node.level == 0:
                return node.module
            module_parts = list(relative_path.with_suffix("").parts)
            if module_parts and module_parts[0] == "src":
                module_parts.pop(0)
            package_parts = module_parts[:-1]
            retained = len(package_parts) - node.level + 1
            if retained < 0:
                return None
            imported_parts = package_parts[:retained]
            if node.module:
                imported_parts.extend(node.module.split("."))
            return ".".join(imported_parts)

        for node in ast.walk(tree):
            node_scope = scope(node)
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == store_module:
                        bind(
                            node_scope,
                            alias.asname or "memebot",
                            store_module if alias.asname else "memebot",
                            node,
                        )
            elif isinstance(node, ast.ImportFrom):
                imported_module = import_from_module(node)
                if imported_module == store_module:
                    for alias in node.names:
                        if alias.name == "*":
                            unresolved_aliases.append(
                                (*location(node), "star import")
                            )
                        elif alias.name in protected_names:
                            if (
                                alias.name == private_name
                                and relative_path.as_posix()
                                != "src/memebot/store.py"
                            ):
                                external_private_imports.append(
                                    (*location(node), alias.asname)
                                )
                            bind(
                                node_scope,
                                alias.asname or alias.name,
                                f"{store_module}.{alias.name}",
                                node,
                            )
                elif imported_module == "memebot":
                    for alias in node.names:
                        if alias.name == "store":
                            bind(
                                node_scope,
                                alias.asname or alias.name,
                                store_module,
                                node,
                            )

        def resolve_symbol(node, node_scope):
            name = dotted_name(node)
            if name is None:
                return None
            visible = {}
            for parent_scope in scope_chain(node_scope, parents):
                visible.update(bindings.get(parent_scope, {}))
            parts = name.split(".")
            for length in range(len(parts), 0, -1):
                prefix = ".".join(parts[:length])
                target = visible.get(prefix)
                if target is None:
                    continue
                suffix = ".".join(parts[length:])
                return target if not suffix else f"{target}.{suffix}"
            return None

        expressions = assignment_expressions(tree, parents, scope)
        protected_by_name = {
            target.rsplit(".", 1)[1]: target for target in protected
        }

        def expression_taint(value, node_scope):
            tainted = {
                target
                for candidate in ast.walk(value)
                if isinstance(candidate, (ast.Name, ast.Attribute))
                if not (
                    isinstance(parents.get(candidate), ast.Call)
                    and parents[candidate].func is candidate
                )
                if (target := resolve_symbol(candidate, node_scope)) in protected
            }
            if path in production_paths:
                environment = {}
                for parent_scope in scope_chain(node_scope, parents):
                    environment.update(expressions.get(parent_scope, {}))
                for candidate in ast.walk(value):
                    reflected = static_string(candidate, environment)
                    target = protected_by_name.get(reflected, reflected)
                    if target in protected:
                        tainted.add(target)
            return tainted

        assignments = [
            node for node in ast.walk(tree)
            if assignment_targets(node)[1] is not None
        ]
        for _ in range(len(assignments) + 1):
            changed = False
            for node in assignments:
                targets, value = assignment_targets(node)
                target_symbol = resolve_symbol(value, scope(node))
                if target_symbol == store_module:
                    assigned_symbol = target_symbol
                else:
                    tainted = expression_taint(value, scope(node))
                    if not tainted:
                        continue
                    if len(tainted) != 1:
                        unresolved_aliases.append(
                            (*location(node), "ambiguous protected assignment")
                        )
                        continue
                    assigned_symbol = next(iter(tainted))
                if assigned_symbol not in protected and assigned_symbol != store_module:
                    continue
                for target in targets:
                    target_name = dotted_name(target)
                    if target_name is None:
                        unresolved_aliases.append(
                            (*location(node), "complex assignment")
                        )
                        continue
                    changed = bind(
                        scope(node), target_name, assigned_symbol, node
                    ) or changed
            if not changed:
                break

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            target = resolve_symbol(node.func, scope(node))
            if target not in protected:
                tainted = expression_taint(node.func, scope(node))
                if len(tainted) == 1:
                    target = next(iter(tainted))
                elif tainted:
                    unresolved_aliases.append(
                        (*location(node), "ambiguous protected call")
                    )
                    continue
            if target in protected:
                protected_calls[target].append(location(node))
                continue
            referenced = {
                value.id for value in ast.walk(node.func)
                if isinstance(value, ast.Name)
            }
            referenced.update(
                value.attr for value in ast.walk(node.func)
                if isinstance(value, ast.Attribute)
            )
            referenced.update(
                value.value for value in ast.walk(node.func)
                if isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            )
            hidden = referenced & protected_names
            if (
                not hidden
                and isinstance(node.func, ast.Call)
                and dotted_name(node.func.func) == "getattr"
                and len(node.func.args) >= 2
            ):
                attribute = static_string(node.func.args[1], {})
                if attribute in protected_names:
                    hidden = {attribute}
            if hidden:
                unresolved_aliases.append(
                    (*location(node), ",".join(sorted(hidden)))
                )

    assert external_private_imports == []
    assert unresolved_aliases == []
    assert [item[:2] for item in protected_calls[private_writer]] == [
        (
            "src/memebot/store.py",
            "record_decision_with_canonical_observations",
        )
    ]
    assert len(protected_calls[private_writer]) == 1
    assert protected_calls[strict_writer]
    assert protected_calls[generic_writer]


def test_canonical_outcome_strict_writer_rejects_malformed_shapes(
    tmp_path, monkeypatch,
):
    import json

    import memebot.store as store
    from memebot.store import (
        EvidenceIntegrityError,
        record_canonical_observation_outcome,
    )

    conn = open_db(tmp_path / "canonical-outcome.db", migration_clock=lambda: 1.0)
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
    )
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [
                        10.0, 20.0, 30.0, 40.0, 50.0, 60.0,
                    ],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
        (feature_json, "a" * 64),
    ).lastrowid
    observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (decision_id,),
    ).lastrowid
    conn.commit()

    real_json_dumps = json.dumps
    canonical_allow_nan = []

    def capture_json_dumps(value, *args, **kwargs):
        if type(value) is dict and set(value) == {
            "horizon_s",
            "forward_return_pct",
            "price0",
            "price0_observed_at",
            "price_now",
            "price_now_observed_at",
            "terminal",
            "unavailable_reason",
        }:
            canonical_allow_nan.append(kwargs.get("allow_nan"))
        return real_json_dumps(value, *args, **kwargs)

    monkeypatch.setattr(store.json, "dumps", capture_json_dumps)

    def write(**overrides):
        values = {
            "raw_wall": 120.0,
            "observation_id": observation_id,
            "horizon_s": 20.0,
            "forward_return_pct": 100.0,
            "price0": 2.0,
            "price0_observed_at": 99.0,
            "price_now": 4.0,
            "price_now_observed_at": 120.0,
            "terminal": None,
            "unavailable_reason": "",
        }
        values.update(overrides)
        return record_canonical_observation_outcome(conn, **values)

    first_id = write(
        raw_wall=110.0,
        horizon_s=10.0,
        price_now_observed_at=110.0,
    )
    first = conn.execute(
        "SELECT at,ref_kind,ref_id,pnl_sol,detail_json FROM outcomes WHERE id=?",
        (first_id,),
    ).fetchone()
    assert tuple(first[:4]) == (
        110.0,
        "canonical_observation",
        observation_id,
        0.0,
    )
    expected_detail = {
        "horizon_s": 10.0,
        "forward_return_pct": 100.0,
        "price0": 2.0,
        "price0_observed_at": 99.0,
        "price_now": 4.0,
        "price_now_observed_at": 110.0,
        "terminal": None,
        "unavailable_reason": "",
    }
    assert first["detail_json"] == json.dumps(
        expected_detail,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    invalid = (
        {"raw_wall": True},
        {"raw_wall": float("nan")},
        {"observation_id": True},
        {"observation_id": 0},
        {"horizon_s": "20.0"},
        {"horizon_s": True},
        {"horizon_s": float("nan")},
        {"horizon_s": 0.0},
        {"horizon_s": 15.0},
        {"forward_return_pct": "100.0"},
        {"forward_return_pct": True},
        {"forward_return_pct": float("nan")},
        {"forward_return_pct": 99.0},
        {"price0": "2.0"},
        {"price0": True},
        {"price0": float("nan")},
        {"price0": 0.0},
        {"price0": 3.0},
        {"price0_observed_at": "99.0"},
        {"price0_observed_at": True},
        {"price0_observed_at": float("inf")},
        {"price0_observed_at": 98.0},
        {"price_now": "4.0"},
        {"price_now": True},
        {"price_now": float("nan")},
        {"price_now": -1.0},
        {"price_now": 0.0, "forward_return_pct": -100.0},
        {"price_now_observed_at": "120.0"},
        {"price_now_observed_at": True},
        {"price_now_observed_at": float("nan")},
        {"price_now_observed_at": 99.0},
        {"price_now_observed_at": 121.0},
        {"terminal": "UNKNOWN"},
        {"terminal": "DEAD"},
        {
            "terminal": "DEAD",
            "price_now": 0.0,
            "forward_return_pct": -99.0,
        },
        {"terminal": "STALE", "forward_return_pct": -100.0},
        {
            "terminal": "STALE",
            "price_now": 0.0,
            "forward_return_pct": -99.0,
        },
        {"unavailable_reason": "unknown"},
        {"unavailable_reason": "journal_replay_gap"},
        {
            "unavailable_reason": "journal_replay_gap",
            "forward_return_pct": None,
            "price_now": None,
            "price_now_observed_at": None,
            "terminal": "GRADUATED",
        },
        {
            "unavailable_reason": "graduated_no_price",
            "forward_return_pct": None,
            "price_now": None,
            "price_now_observed_at": None,
            "terminal": None,
        },
    )
    for malformed in invalid:
        before_clock = conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0]
        with pytest.raises(ValueError):
            write(**malformed)
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 1
        assert conn.execute(
            "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
        ).fetchone()[0] == before_clock

    with pytest.raises(EvidenceIntegrityError):
        write(observation_id=observation_id + 10_000)

    dead_id = write(
        terminal="DEAD",
        price_now=0.0,
        forward_return_pct=-100.0,
    )
    assert dead_id > first_id
    stale_id = write(
        raw_wall=130.0,
        horizon_s=30.0,
        terminal="STALE",
        price_now=0.0,
        price_now_observed_at=130.0,
        forward_return_pct=-100.0,
    )
    assert stale_id > dead_id
    gap_id = write(
        raw_wall=140.0,
        horizon_s=40.0,
        forward_return_pct=None,
        price_now=None,
        price_now_observed_at=None,
        terminal=None,
        unavailable_reason="journal_replay_gap",
    )
    graduated_id = write(
        raw_wall=150.0,
        horizon_s=50.0,
        forward_return_pct=None,
        price_now=None,
        price_now_observed_at=None,
        terminal="GRADUATED",
        unavailable_reason="graduated_no_price",
    )
    assert graduated_id > gap_id
    available_graduated_id = write(
        raw_wall=160.0,
        horizon_s=60.0,
        terminal="GRADUATED",
        price_now_observed_at=160.0,
    )
    assert available_graduated_id > graduated_id
    available_graduated = json.loads(conn.execute(
        "SELECT detail_json FROM outcomes WHERE id=?",
        (available_graduated_id,),
    ).fetchone()[0])
    assert available_graduated["terminal"] == "GRADUATED"
    assert available_graduated["price_now"] == 4.0
    assert available_graduated["forward_return_pct"] == 100.0
    assert tuple(conn.execute(
        "SELECT completed_mask,full_mask FROM canonical_pending_current "
        "WHERE observation_id=?",
        (observation_id,),
    ).fetchone()) == (63, 63)

    with pytest.raises(EvidenceIntegrityError):
        write(
            raw_wall=151.0,
            horizon_s=10.0,
            price_now_observed_at=110.0,
        )
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 6
    assert canonical_allow_nan
    assert set(canonical_allow_nan) == {False}
    conn.close()


def test_p3_outcome_direct_shape_guard(tmp_path):
    import json

    conn = open_db(tmp_path / "p3-outcome-direct-shape.db", migration_clock=lambda: 1.0)
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
    )
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [10.0, 20.0, 30.0],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
        (feature_json, "a" * 64),
    ).lastrowid
    observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (decision_id,),
    ).lastrowid
    conn.commit()

    insert_sql = (
        "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
        "VALUES (?,'canonical_observation',?,?,?)"
    )

    def detail(horizon_s, *, price_now_observed_at):
        return json.dumps(
            {
                "horizon_s": horizon_s,
                "forward_return_pct": 100.0,
                "price0": 2.0,
                "price0_observed_at": 99.0,
                "price_now": 4.0,
                "price_now_observed_at": price_now_observed_at,
                "terminal": None,
                "unavailable_reason": "",
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    conn.execute(
        insert_sql,
        (110.0, observation_id, 0.0, detail(10.0, price_now_observed_at=110.0)),
    )

    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 outcome shape"):
        conn.execute(
            insert_sql,
            (
                4_102_444_801.0,
                observation_id,
                0.0,
                detail(20.0, price_now_observed_at=120.0),
            ),
        )
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 outcome shape"):
        conn.execute(
            insert_sql,
            (130.0, observation_id, 1.0, detail(30.0, price_now_observed_at=130.0)),
        )

    assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 1
    conn.close()


def test_canonical_pending_summary_initializes_exact_horizon_mask(tmp_path):
    import json

    conn = open_db(tmp_path / "canonical-pending-init.db", migration_clock=lambda: 1.0)
    horizons = [10.0, 20.0, 40.0, 80.0]
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": horizons,
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    observation_ids = {}
    for mint, eligible, unavailable_reason in (
        ("ELIGIBLE", 1, ""),
        ("INELIGIBLE", 0, ""),
        ("UNAVAILABLE", 1, "start_price_missing"),
    ):
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
            "VALUES (?,1.0,'CLIMBING',1.0,'{}')",
            (mint,),
        )
        decision_id = conn.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,config_hash"
            ") VALUES (100.0,?,'CLIMBING','SKIP',0.0,?,?)",
            (mint, feature_json, "a" * 64),
        ).lastrowid
        available = unavailable_reason == ""
        observation_ids[mint] = conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason"
            ") VALUES (?,?,100.0,1,0,?,?,?,?,?)",
            (
                decision_id,
                mint,
                eligible,
                2.0 if available else None,
                99.0 if available else None,
                "curve_snapshot" if available else "",
                unavailable_reason,
            ),
        ).lastrowid
    conn.commit()

    pending = conn.execute(
        "SELECT observation_id,decision_id,horizons_json,full_mask,completed_mask "
        "FROM canonical_pending_current ORDER BY observation_id"
    ).fetchall()
    eligible_observation_id = observation_ids["ELIGIBLE"]
    eligible_decision_id = conn.execute(
        "SELECT decision_id FROM canonical_observations WHERE id=?",
        (eligible_observation_id,),
    ).fetchone()[0]
    assert [tuple(row) for row in pending] == [
        (
            eligible_observation_id,
            eligible_decision_id,
            json.dumps(horizons, separators=(",", ":")),
            (1 << len(horizons)) - 1,
            0,
        )
    ]
    assert set(observation_ids) == {"ELIGIBLE", "INELIGIBLE", "UNAVAILABLE"}
    conn.close()


def test_canonical_pending_summary_marks_exact_completed_horizon(tmp_path):
    import json

    conn = open_db(tmp_path / "canonical-pending-completion.db", migration_clock=lambda: 1.0)
    horizons = [10.0, 20.0, 40.0, 80.0]
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": horizons,
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    observation_ids = {}
    for mint in ("SUBJECT", "OTHER"):
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
            "VALUES (?,1.0,'CLIMBING',1.0,'{}')",
            (mint,),
        )
        decision_id = conn.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,config_hash"
            ") VALUES (100.0,?,'CLIMBING','SKIP',0.0,?,?)",
            (mint, feature_json, "a" * 64),
        ).lastrowid
        observation_ids[mint] = conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason"
            ") VALUES (?,?,100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
            (decision_id, mint),
        ).lastrowid
    conn.commit()

    full_mask = (1 << len(horizons)) - 1
    assert [tuple(row) for row in conn.execute(
        "SELECT observation_id,full_mask,completed_mask "
        "FROM canonical_pending_current ORDER BY observation_id"
    )] == [
        (observation_ids["SUBJECT"], full_mask, 0),
        (observation_ids["OTHER"], full_mask, 0),
    ]

    def outcome_detail(horizon_s):
        return json.dumps(
            {
                "horizon_s": horizon_s,
                "forward_return_pct": 100.0,
                "price0": 2.0,
                "price0_observed_at": 99.0,
                "price_now": 4.0,
                "price_now_observed_at": 100.0 + horizon_s,
                "terminal": None,
                "unavailable_reason": "",
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    conn.execute(
        "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
        "VALUES (120.0,'candidate',?,0.0,'{}')",
        (observation_ids["SUBJECT"],),
    )
    assert [tuple(row) for row in conn.execute(
        "SELECT observation_id,full_mask,completed_mask "
        "FROM canonical_pending_current ORDER BY observation_id"
    )] == [
        (observation_ids["SUBJECT"], full_mask, 0),
        (observation_ids["OTHER"], full_mask, 0),
    ]

    selected_horizon = 40.0
    selected_bit = 1 << horizons.index(selected_horizon)
    conn.execute(
        "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
        "VALUES (?,'canonical_observation',?,0.0,?)",
        (
            100.0 + selected_horizon,
            observation_ids["SUBJECT"],
            outcome_detail(selected_horizon),
        ),
    )
    subject_mask = conn.execute(
        "SELECT full_mask,completed_mask FROM canonical_pending_current "
        "WHERE observation_id=?",
        (observation_ids["SUBJECT"],),
    ).fetchone()
    assert tuple(subject_mask) == (full_mask, selected_bit)
    assert subject_mask["completed_mask"] & selected_bit == selected_bit
    assert subject_mask["completed_mask"] & ~selected_bit == 0
    assert tuple(conn.execute(
        "SELECT full_mask,completed_mask FROM canonical_pending_current "
        "WHERE observation_id=?",
        (observation_ids["OTHER"],),
    ).fetchone()) == (full_mask, 0)

    with pytest.raises(sqlite3.Error):
        conn.execute(
            "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
            "VALUES (?,'canonical_observation',?,0.0,?)",
            (
                100.0 + selected_horizon,
                observation_ids["SUBJECT"],
                outcome_detail(selected_horizon),
            ),
        )
    assert [tuple(row) for row in conn.execute(
        "SELECT observation_id,full_mask,completed_mask "
        "FROM canonical_pending_current ORDER BY observation_id"
    )] == [
        (observation_ids["SUBJECT"], full_mask, selected_bit),
        (observation_ids["OTHER"], full_mask, 0),
    ]
    conn.close()


def test_canonical_observation_horizon_is_unique(tmp_path):
    import json

    conn = open_db(tmp_path / "canonical-horizon-unique.db", migration_clock=lambda: 1.0)
    horizons = [10.0, 20.0]
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": horizons,
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    observation_ids = []
    for mint in ("SUBJECT", "PEER"):
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
            "VALUES (?,1.0,'CLIMBING',1.0,'{}')",
            (mint,),
        )
        decision_id = conn.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,config_hash"
            ") VALUES (100.0,?,'CLIMBING','SKIP',0.0,?,?)",
            (mint, feature_json, "a" * 64),
        ).lastrowid
        observation_ids.append(conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason"
            ") VALUES (?,?,100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
            (decision_id, mint),
        ).lastrowid)
    conn.commit()

    # Isolate the semantic unique index from the independent REPLACE guard.
    conn.execute("DROP TRIGGER outcomes_no_replace")
    insert_sql = (
        "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
        "VALUES (?,'canonical_observation',?,0.0,?)"
    )

    def detail(horizon_s):
        return json.dumps(
            {
                "horizon_s": horizon_s,
                "forward_return_pct": 100.0,
                "price0": 2.0,
                "price0_observed_at": 99.0,
                "price_now": 4.0,
                "price_now_observed_at": 100.0 + horizon_s,
                "terminal": None,
                "unavailable_reason": "",
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    subject_id, peer_id = observation_ids
    conn.execute(insert_sql, (110.0, subject_id, detail(10)))
    with pytest.raises(
        sqlite3.IntegrityError,
        match="canonical_outcome_horizon_unique",
    ):
        conn.execute(insert_sql, (110.0, subject_id, detail(10.0)))

    conn.execute(insert_sql, (120.0, subject_id, detail(20.0)))
    conn.execute(insert_sql, (110.0, peer_id, detail(10.0)))
    assert [tuple(row) for row in conn.execute(
        "SELECT ref_id,json_extract(detail_json,'$.horizon_s') "
        "FROM outcomes ORDER BY ref_id,id"
    )] == [
        (subject_id, 10),
        (subject_id, 20.0),
        (peer_id, 10.0),
    ]
    conn.close()


def test_canonical_outcome_processing_time_allocates_after_observation(tmp_path):
    import json
    import math

    from memebot.store import record_canonical_observation_outcome

    conn = open_db(
        tmp_path / "canonical-outcome-causal-time.db",
        migration_clock=lambda: 120.0,
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
    )
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [10.0],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
        (feature_json, "a" * 64),
    ).lastrowid
    observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (decision_id,),
    ).lastrowid
    conn.commit()

    outcome_id = record_canonical_observation_outcome(
        conn,
        raw_wall=90.0,
        observation_id=observation_id,
        horizon_s=10.0,
        forward_return_pct=100.0,
        price0=2.0,
        price0_observed_at=99.0,
        price_now=4.0,
        price_now_observed_at=110.0,
        terminal=None,
        unavailable_reason="",
    )

    expected_at = math.nextafter(120.0, math.inf)
    outcome_at = conn.execute(
        "SELECT at FROM outcomes WHERE id=?", (outcome_id,),
    ).fetchone()[0]
    assert outcome_at == expected_at
    assert outcome_at > 100.0
    assert outcome_at >= 110.0
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == expected_at
    conn.close()


def test_canonical_outcome_direct_guard_requires_exact_detail_and_proof(tmp_path):
    import json

    insert_sql = (
        "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
        "VALUES (?,'canonical_observation',?,0.0,?)"
    )

    def open_case(
        name,
        *,
        horizons=(10.0, 20.0),
        pending_horizons=None,
        start_price=2.0,
    ):
        conn = open_db(tmp_path / f"canonical-outcome-{name}.db", migration_clock=lambda: 1.0)
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
            "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
        )
        feature_json = json.dumps(
            {
                "canonical": {
                    "ranking_inputs": {
                        "counterfactual_horizons_s": list(horizons),
                    },
                },
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        decision_id = conn.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,config_hash"
            ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
            (feature_json, "a" * 64),
        ).lastrowid
        observation_id = conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason"
            ") VALUES (?,'MINT',100.0,1,1,1,?,99.0,'curve_snapshot','')",
            (decision_id, start_price),
        ).lastrowid
        if pending_horizons is not None:
            conn.execute(
                "UPDATE canonical_pending_current "
                "SET horizons_json=?,full_mask=? WHERE observation_id=?",
                (
                    json.dumps(pending_horizons, separators=(",", ":")),
                    (1 << len(pending_horizons)) - 1,
                    observation_id,
                ),
            )
        conn.commit()
        return conn, observation_id

    def available_detail(*, horizon_s=10.0, price0=2.0, price_now=4.0, **overrides):
        detail = {
            "horizon_s": horizon_s,
            "forward_return_pct": 100.0 * (price_now - price0) / price0,
            "price0": price0,
            "price0_observed_at": 99.0,
            "price_now": price_now,
            "price_now_observed_at": 100.0 + horizon_s,
            "terminal": None,
            "unavailable_reason": "",
        }
        detail.update(overrides)
        return detail

    def unavailable_detail(**overrides):
        detail = available_detail()
        detail.update(
            forward_return_pct=None,
            price_now=None,
            price_now_observed_at=None,
            terminal=None,
            unavailable_reason="journal_replay_gap",
        )
        detail.update(overrides)
        return detail

    valid_conn, valid_observation_id = open_case("valid")
    valid_detail = available_detail()
    valid_conn.execute(
        insert_sql,
        (
            110.0,
            valid_observation_id,
            json.dumps(
                valid_detail,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
        ),
    )
    assert valid_conn.execute(
        "SELECT count(*) FROM outcomes WHERE ref_kind='canonical_observation'"
    ).fetchone()[0] == 1
    valid_conn.close()

    price_for_return_domain = 1e100
    start_for_return_domain = 1e-100
    price_for_price_domain = 1.1e100
    start_for_price_domain = 1e100
    cases = (
        ("horizon-at", {}, 109.0, available_detail(price_now_observed_at=109.0)),
        (
            "horizon-declared",
            {"horizons": (10.0, 20.0), "pending_horizons": (10.0, 15.0, 20.0)},
            115.0,
            available_detail(horizon_s=15.0),
        ),
        ("keys-extra", {}, 110.0, {**available_detail(), "extra": 1}),
        (
            "keys-missing",
            {},
            110.0,
            {key: value for key, value in available_detail().items() if key != "terminal"},
        ),
        (
            "price0",
            {},
            110.0,
            available_detail(price0=3.0),
        ),
        (
            "price0-at",
            {},
            110.0,
            available_detail(price0_observed_at=98.0),
        ),
        (
            "terminal-domain",
            {},
            110.0,
            available_detail(terminal="UNKNOWN"),
        ),
        (
            "unavailable-return",
            {},
            110.0,
            unavailable_detail(forward_return_pct=0.0),
        ),
        (
            "unavailable-price",
            {},
            110.0,
            unavailable_detail(price_now=0.0),
        ),
        (
            "unavailable-source",
            {},
            110.0,
            unavailable_detail(price_now_observed_at=110.0),
        ),
        (
            "unavailable-mapping",
            {},
            110.0,
            unavailable_detail(terminal="GRADUATED"),
        ),
        (
            "available-return",
            {"start_price": start_for_return_domain},
            110.0,
            available_detail(
                price0=start_for_return_domain,
                price_now=price_for_return_domain,
            ),
        ),
        (
            "available-price",
            {"start_price": start_for_price_domain},
            110.0,
            available_detail(
                price0=start_for_price_domain,
                price_now=price_for_price_domain,
            ),
        ),
        (
            "source-lower",
            {},
            110.0,
            available_detail(price_now_observed_at=99.0),
        ),
        (
            "source-upper",
            {},
            110.0,
            available_detail(price_now_observed_at=111.0),
        ),
        (
            "dead-price",
            {},
            110.0,
            available_detail(terminal="DEAD", price_now=1.0, forward_return_pct=-100.0),
        ),
        (
            "dead-return",
            {},
            110.0,
            available_detail(terminal="DEAD", price_now=0.0, forward_return_pct=-99.0),
        ),
        (
            "stale-price",
            {},
            110.0,
            available_detail(terminal="STALE", price_now=1.0, forward_return_pct=-100.0),
        ),
        (
            "stale-return",
            {},
            110.0,
            available_detail(terminal="STALE", price_now=0.0, forward_return_pct=-99.0),
        ),
        (
            "positive",
            {},
            110.0,
            available_detail(price_now=0.0, forward_return_pct=-100.0),
        ),
        (
            "return",
            {},
            110.0,
            available_detail(forward_return_pct=99.0),
        ),
    )

    for name, setup, at, detail in cases:
        conn, observation_id = open_case(name, **setup)
        with pytest.raises(
            sqlite3.IntegrityError,
            match="P3 outcome requires causal exit/observation proof",
        ):
            conn.execute(
                insert_sql,
                (
                    at,
                    observation_id,
                    json.dumps(
                        detail,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                ),
            )
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0, name
        conn.close()


def test_generic_outcome_writer_rejects_reserved_canonical_kind(tmp_path):
    import json

    from memebot.store import record_outcome

    conn = open_db(
        tmp_path / "generic-outcome-reservation.db",
        migration_clock=lambda: 1.0,
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
    )
    canonical_vector = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {"counterfactual_horizons_s": [10.0]},
                "status": "CANONICAL",
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','BUY',90.0,?,?)",
        (canonical_vector, "a" * 64),
    ).lastrowid
    legacy_trade_id = conn.execute(
        "INSERT INTO paper_trades("
        "at,mint,segment,side,qty,quote_price,fill_price,fees_json,realism_grade"
        ") VALUES (1.0,'LEGACY','CLIMBING','buy',1.0,1.0,1.0,'{}','B')"
    ).lastrowid
    canonical_trade_id = conn.execute(
        "INSERT INTO paper_trades("
        "decision_id,at,mint,segment,side,qty,quote_price,fill_price,"
        "fees_json,realism_grade"
        ") VALUES (?,101.0,'MINT','CLIMBING','buy',1.0,1.0,1.0,'{}','B')",
        (decision_id,),
    ).lastrowid
    observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (decision_id,),
    ).lastrowid
    conn.commit()
    assert legacy_trade_id == decision_id
    assert canonical_trade_id != decision_id

    canonical_detail = {
        "horizon_s": 10.0,
        "forward_return_pct": 100.0,
        "price0": 2.0,
        "price0_observed_at": 99.0,
        "price_now": 4.0,
        "price_now_observed_at": 110.0,
        "terminal": None,
        "unavailable_reason": "",
    }
    reserved = (
        ("canonical_observation", observation_id, 110.0, 0.0, canonical_detail),
        ("trade", canonical_trade_id, 102.0, 0.0, {}),
    )
    for ref_kind, ref_id, at, pnl_sol, detail in reserved:
        with pytest.raises(ValueError, match="strict P3 outcome helpers"):
            record_outcome(
                conn,
                at=at,
                ref_kind=ref_kind,
                ref_id=ref_id,
                pnl_sol=pnl_sol,
                detail=detail,
            )
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
        assert conn.in_transaction is False

    candidate_outcome_id = record_outcome(
        conn,
        at=2.0,
        ref_kind="candidate",
        ref_id=7,
        pnl_sol=0.0,
        detail={"horizon_s": 60.0},
    )
    trade_outcome_id = record_outcome(
        conn,
        at=2.0,
        ref_kind="trade",
        ref_id=legacy_trade_id,
        pnl_sol=1.0,
        detail={"reason": "legacy"},
    )
    assert [tuple(row) for row in conn.execute(
        "SELECT id,ref_kind,ref_id,pnl_sol,detail_json FROM outcomes ORDER BY id"
    )] == [
        (candidate_outcome_id, "candidate", 7, 0.0, '{"horizon_s": 60.0}'),
        (trade_outcome_id, "trade", legacy_trade_id, 1.0, '{"reason": "legacy"}'),
    ]
    conn.close()


def test_list_pending_canonical_observations_uses_persisted_horizons(tmp_path):
    import json

    from memebot.store import (
        EvidenceIntegrityError,
        canonical_outcome_exists,
        list_pending_canonical_observations,
        record_canonical_observation_outcome,
        record_outcome,
    )

    conn = open_db(
        tmp_path / "pending-canonical-observations.db",
        migration_clock=lambda: 1.0,
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
    )
    horizons = (10.0, 20.0)
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": list(horizons),
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
        (feature_json, "a" * 64),
    ).lastrowid
    observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (decision_id,),
    ).lastrowid
    conn.commit()

    record_canonical_observation_outcome(
        conn,
        raw_wall=110.0,
        observation_id=observation_id,
        horizon_s=10.0,
        forward_return_pct=100.0,
        price0=2.0,
        price0_observed_at=99.0,
        price_now=4.0,
        price_now_observed_at=110.0,
        terminal=None,
        unavailable_reason="",
    )

    second_decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
        (feature_json, "b" * 64),
    ).lastrowid
    second_observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (second_decision_id,),
    ).lastrowid
    conn.commit()
    assert second_observation_id != observation_id
    assert not canonical_outcome_exists(
        conn, observation_id=second_observation_id, horizon_s=10.0,
    )
    for horizon_s, raw_wall in ((10.0, 110.0), (20.0, 120.0)):
        record_canonical_observation_outcome(
            conn,
            raw_wall=raw_wall,
            observation_id=second_observation_id,
            horizon_s=horizon_s,
            forward_return_pct=100.0,
            price0=2.0,
            price0_observed_at=99.0,
            price_now=4.0,
            price_now_observed_at=100.0 + horizon_s,
            terminal=None,
            unavailable_reason="",
        )

    rows = list_pending_canonical_observations(
        conn,
        horizons=horizons,
        limit_plus_one=2,
    )
    assert len(rows) == 1
    assert rows[0]["id"] == observation_id
    assert rows[0]["decision_id"] == decision_id
    assert rows[0]["horizons_json"] == "[10.0,20.0]"
    assert rows[0]["full_mask"] == 3
    assert rows[0]["completed_mask"] == 1
    assert canonical_outcome_exists(
        conn, observation_id=observation_id, horizon_s=10.0,
    )
    record_outcome(
        conn,
        at=111.0,
        ref_kind="candidate",
        ref_id=observation_id,
        pnl_sol=0.0,
        detail={"horizon_s": 20.0},
    )
    assert not canonical_outcome_exists(
        conn, observation_id=observation_id, horizon_s=20.0,
    )

    record_canonical_observation_outcome(
        conn,
        raw_wall=120.0,
        observation_id=observation_id,
        horizon_s=20.0,
        forward_return_pct=100.0,
        price0=2.0,
        price0_observed_at=99.0,
        price_now=4.0,
        price_now_observed_at=120.0,
        terminal=None,
        unavailable_reason="",
    )
    assert canonical_outcome_exists(
        conn, observation_id=observation_id, horizon_s=20.0,
    )
    assert list_pending_canonical_observations(
        conn,
        horizons=horizons,
        limit_plus_one=2,
    ) == []

    conn.execute(
        "UPDATE canonical_pending_current "
        "SET horizons_json='[10,20]',completed_mask=0 "
        "WHERE observation_id=?",
        (observation_id,),
    )
    conn.commit()
    with pytest.raises(
        EvidenceIntegrityError,
        match="malformed canonical pending horizons",
    ):
        list_pending_canonical_observations(
            conn,
            horizons=horizons,
            limit_plus_one=2,
        )

    conn.execute(
        "UPDATE canonical_pending_current "
        "SET horizons_json='[10.0,30.0]',completed_mask=0 "
        "WHERE observation_id=?",
        (observation_id,),
    )
    conn.commit()
    with pytest.raises(
        EvidenceIntegrityError,
        match="malformed canonical pending horizons",
    ):
        list_pending_canonical_observations(
            conn,
            horizons=(10.0, 30.0),
            limit_plus_one=2,
        )

    conn.execute(
        "UPDATE canonical_pending_current "
        "SET horizons_json='[10.0,20.0]',completed_mask=3 "
        "WHERE observation_id=?",
        (observation_id,),
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MALFORMED',1.0,'CLIMBING',1.0,'{}')"
    )
    malformed_feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [True],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    malformed_decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MALFORMED','CLIMBING','SKIP',0.0,?,?)",
        (malformed_feature_json, "b" * 64),
    ).lastrowid
    malformed_observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MALFORMED',100.0,1,1,1,2.0,99.0,"
        "'curve_snapshot','')",
        (malformed_decision_id,),
    ).lastrowid
    conn.commit()
    malformed_pending = conn.execute(
        "SELECT horizons_json FROM canonical_pending_current "
        "WHERE observation_id=?",
        (malformed_observation_id,),
    ).fetchone()
    assert malformed_pending["horizons_json"] == "[true]"
    with pytest.raises(
        EvidenceIntegrityError,
        match="malformed canonical pending horizons",
    ):
        list_pending_canonical_observations(
            conn,
            horizons=(1.0,),
            limit_plus_one=2,
        )
    conn.close()


def test_pending_canonical_selector_is_indexed_and_limit_plus_one_bounded(
    tmp_path,
):
    import json

    from memebot.store import list_pending_canonical_observations

    conn = open_db(
        tmp_path / "pending-canonical-selector.db",
        migration_clock=lambda: 1.0,
    )
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [10.0],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    incomplete_ids = []
    for ordinal in range(8):
        mint = f"MINT-{ordinal}"
        conn.execute(
            "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
            "VALUES (?,1.0,'CLIMBING',1.0,'{}')",
            (mint,),
        )
        decision_id = conn.execute(
            "INSERT INTO decisions("
            "at,mint,segment,action,score,feature_vector_json,config_hash"
            ") VALUES (100.0,?,'CLIMBING','SKIP',0.0,?,?)",
            (mint, feature_json, f"{ordinal:064x}"),
        ).lastrowid
        observation_id = conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason"
            ") VALUES (?,?,100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
            (decision_id, mint),
        ).lastrowid
        if ordinal < 4:
            conn.execute(
                "UPDATE canonical_pending_current SET completed_mask=full_mask "
                "WHERE observation_id=?",
                (observation_id,),
            )
        else:
            incomplete_ids.append(observation_id)
    conn.commit()

    class RecordingConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped
            self.executions = []

        def execute(self, sql, parameters=()):
            self.executions.append((sql, parameters))
            return self._wrapped.execute(sql, parameters)

    recording_conn = RecordingConnection(conn)
    traced = []
    conn.set_trace_callback(traced.append)
    rows = list_pending_canonical_observations(
        recording_conn,
        horizons=(10.0,),
        limit_plus_one=3,
    )
    rows_limit_two = list_pending_canonical_observations(
        recording_conn,
        horizons=(10.0,),
        limit_plus_one=2,
    )
    conn.set_trace_callback(None)

    assert [row["id"] for row in rows] == incomplete_ids[:3]
    assert [row["id"] for row in rows_limit_two] == incomplete_ids[:2]
    raw_selector_sql = """SELECT o.*,cp.horizons_json,cp.full_mask,cp.completed_mask
FROM canonical_pending_current AS cp INDEXED BY canonical_pending_incomplete_idx
JOIN canonical_observations o NOT INDEXED ON o.id=cp.observation_id
JOIN decisions d ON d.id=cp.decision_id AND d.id=o.decision_id
WHERE cp.completed_mask<>cp.full_mask
ORDER BY cp.observation_id
LIMIT :limit_plus_one"""
    raw_selector_calls = [
        execution
        for execution in recording_conn.executions
        if "FROM canonical_pending_current AS cp" in execution[0]
    ]
    assert raw_selector_calls == [
        (raw_selector_sql, {"limit_plus_one": 3}),
        (raw_selector_sql, {"limit_plus_one": 2}),
    ]

    selectors = [
        statement
        for statement in traced
        if "FROM canonical_pending_current AS cp" in statement
    ]
    assert len(selectors) == 2
    selector_sqls = [" ".join(statement.split()) for statement in selectors]
    for selector_sql in selector_sqls:
        assert (
            "FROM canonical_pending_current AS cp "
            "INDEXED BY canonical_pending_incomplete_idx"
        ) in selector_sql
        assert (
            "JOIN canonical_observations o NOT INDEXED "
            "ON o.id=cp.observation_id"
        ) in selector_sql
        assert (
            "JOIN decisions d ON d.id=cp.decision_id AND d.id=o.decision_id"
        ) in selector_sql
        assert "WHERE cp.completed_mask<>cp.full_mask" in selector_sql
    assert "ORDER BY cp.observation_id LIMIT 3" in selector_sqls[0]
    assert "ORDER BY cp.observation_id LIMIT 2" in selector_sqls[1]

    expected_rows_read = len(rows) + len(rows_limit_two)
    traced_reads = [" ".join(statement.split()) for statement in traced]
    decision_reads = [
        statement
        for statement in traced_reads
        if statement.startswith(
            "SELECT feature_vector_json FROM decisions WHERE id="
        )
    ]
    affinity_reads = [
        statement
        for statement in traced_reads
        if statement.startswith("SELECT typeof(")
    ]
    json_shape_reads = [
        statement
        for statement in traced_reads
        if statement.startswith("SELECT json_valid(")
    ]
    json_element_reads = [
        statement
        for statement in traced_reads
        if statement.startswith("SELECT key,value,type FROM json_each(")
    ]
    assert len(traced_reads) == 2 + 4 * expected_rows_read, traced_reads
    assert len(decision_reads) == expected_rows_read, traced_reads
    assert len(affinity_reads) == expected_rows_read, traced_reads
    assert len(json_shape_reads) == expected_rows_read, traced_reads
    assert len(json_element_reads) == expected_rows_read, traced_reads
    allowed_reads = {
        *selector_sqls,
        *decision_reads,
        *affinity_reads,
        *json_shape_reads,
        *json_element_reads,
    }
    assert all(statement in allowed_reads for statement in traced_reads), traced_reads

    plan = [
        row["detail"]
        for row in conn.execute(f"EXPLAIN QUERY PLAN {selectors[0]}")
    ]
    cp_scans = [
        detail
        for detail in plan
        if "SCAN cp USING INDEX canonical_pending_incomplete_idx" in detail
    ]
    observation_lookups = [
        detail
        for detail in plan
        if "SEARCH o USING INTEGER PRIMARY KEY" in detail
    ]
    decision_lookups = [
        detail
        for detail in plan
        if "SEARCH d USING INTEGER PRIMARY KEY" in detail
    ]
    assert len(plan) == 3, plan
    assert len(cp_scans) == 1, plan
    assert len(observation_lookups) == 1, plan
    assert len(decision_lookups) == 1, plan
    assert all(
        "SCAN cp USING INDEX canonical_pending_incomplete_idx" in detail
        or "SEARCH o USING INTEGER PRIMARY KEY" in detail
        or "SEARCH d USING INTEGER PRIMARY KEY" in detail
        for detail in plan
    ), plan
    assert all("USE TEMP B-TREE" not in detail for detail in plan), plan

    executions_before_invalid = len(recording_conn.executions)
    for invalid in (True, 1.0, 0, -1):
        with pytest.raises(
            ValueError,
            match=r"^limit_plus_one must be a positive integer$",
        ):
            list_pending_canonical_observations(
                recording_conn,
                horizons=(10.0,),
                limit_plus_one=invalid,
            )
    assert len(recording_conn.executions) == executions_before_invalid
    conn.close()


def test_pending_canonical_selector_rejects_horizon_config_drift(tmp_path):
    import json

    from memebot.store import (
        EvidenceIntegrityError,
        list_pending_canonical_observations,
    )

    conn = open_db(
        tmp_path / "pending-canonical-horizon-drift.db",
        migration_clock=lambda: 1.0,
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('MINT',1.0,'CLIMBING',1.0,'{}')"
    )
    feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [10.0, 20.0],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'MINT','CLIMBING','SKIP',0.0,?,?)",
        (feature_json, "a" * 64),
    ).lastrowid
    observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'MINT',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (decision_id,),
    ).lastrowid
    conn.commit()

    class RecordingConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped
            self.executions = []

        def execute(self, sql, parameters=()):
            self.executions.append((sql, parameters))
            return self._wrapped.execute(sql, parameters)

    recording_conn = RecordingConnection(conn)
    for ordered_horizons in (
        (10, 20),
        [10.0, 20.0],
        range(10, 30, 10),
    ):
        rows = list_pending_canonical_observations(
            recording_conn,
            horizons=ordered_horizons,
            limit_plus_one=2,
        )
        assert [row["id"] for row in rows] == [observation_id]

    for drifted in (
        (10.0,),
        (10.0, 30.0),
        (10.0, 20.0, 30.0),
    ):
        with pytest.raises(
            EvidenceIntegrityError,
            match=r"^canonical pending horizon config drift$",
        ):
            list_pending_canonical_observations(
                recording_conn,
                horizons=drifted,
                limit_plus_one=2,
            )

    from collections.abc import Sequence

    class OversizedBombSequence(Sequence):
        def __init__(self):
            self.element_reads = 0

        def __len__(self):
            return 100_000

        def __getitem__(self, index):
            self.element_reads += 1
            raise AssertionError(f"unexpected element read at {index}")

    class LengthLiarSequence(Sequence):
        def __init__(self):
            self.element_reads = 0

        def __len__(self):
            return 1

        def __getitem__(self, index):
            self.element_reads += 1
            return float(index + 1)

    oversized_bomb = OversizedBombSequence()
    length_liar = LengthLiarSequence()
    malformed_horizons = (
        (),
        (True,),
        ("10.0",),
        {10.0},
        {10.0: "value"},
        None,
        10.0,
        (float("nan"),),
        (float("inf"),),
        (10**1000,),
        (0.0,),
        (10.0, 10.0),
        (20.0, 10.0),
        tuple(float(index) for index in range(1, 34)),
        oversized_bomb,
        length_liar,
    )
    executions_before_malformed = len(recording_conn.executions)
    traced = []
    conn.set_trace_callback(traced.append)
    for malformed in malformed_horizons:
        with pytest.raises(
            ValueError,
            match=r"^invalid supplied canonical horizon tuple$",
        ):
            list_pending_canonical_observations(
                recording_conn,
                horizons=malformed,
                limit_plus_one=2,
            )
    conn.set_trace_callback(None)
    assert len(recording_conn.executions) == executions_before_malformed
    assert traced == []

    assert oversized_bomb.element_reads == 0
    assert length_liar.element_reads == 2
    conn.execute(
        "UPDATE canonical_pending_current SET completed_mask=full_mask "
        "WHERE observation_id=?",
        (observation_id,),
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('HUGE',1.0,'CLIMBING',1.0,'{}')"
    )
    huge_horizon = 10**100
    huge_feature_json = json.dumps(
        {
            "canonical": {
                "ranking_inputs": {
                    "counterfactual_horizons_s": [huge_horizon],
                },
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    huge_decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,config_hash"
        ") VALUES (100.0,'HUGE','CLIMBING','SKIP',0.0,?,?)",
        (huge_feature_json, "b" * 64),
    ).lastrowid
    huge_observation_id = conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'HUGE',100.0,1,1,1,2.0,99.0,'curve_snapshot','')",
        (huge_decision_id,),
    ).lastrowid
    conn.commit()

    huge_rows = list_pending_canonical_observations(
        recording_conn,
        horizons=(huge_horizon,),
        limit_plus_one=2,
    )
    assert [row["id"] for row in huge_rows] == [huge_observation_id]
    with pytest.raises(
        EvidenceIntegrityError,
        match=r"^canonical pending horizon config drift$",
    ):
        list_pending_canonical_observations(
            recording_conn,
            horizons=(huge_horizon + 1,),
            limit_plus_one=2,
        )
    conn.close()


def _seed_strict_p3_buy(tmp_path, name):
    import hashlib
    import json

    from memebot.store import save_safety_report

    conn = open_db(tmp_path / name, migration_clock=lambda: 0.0)
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('M',0.0,'CLIMBING',0.0,'{}')"
    )
    conn.commit()
    report_id = save_safety_report(
        conn,
        mint="M",
        raw_completed_at=1.0,
        segment="CLIMBING",
        hard_fails=(),
        risk_score=1.0,
        results_json="[]",
        inputs_hash="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    feature_json = json.dumps(
        {
            "canonical": {
                "status": "CANONICAL",
                "planned_size_sol": 10.0,
                "inputs_hash": "b" * 64,
                "ranking_inputs": {"counterfactual_horizons_s": [60.0]},
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    decision_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,safety_report_id,config_hash"
        ") VALUES (2.0,'M','CLIMBING','BUY',90.0,?,?,?)",
        (feature_json, report_id, "c" * 64),
    ).lastrowid
    conn.execute(
        "INSERT INTO canonical_observations("
        "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
        "start_price_sol,price_observed_at,price_source,unavailable_reason"
        ") VALUES (?,'M',2.0,1,1,1,1.0,2.0,'curve_snapshot','')",
        (decision_id,),
    )
    conn.commit()

    def recheck_payload(
        *, at, reason="canonical_selected", verdict_inputs_hash="d" * 64,
    ):
        payload = {
            "decision_id": decision_id,
            "attempt": 1,
            "trigger": "curve_progress",
            "trigger_report_id": None,
            "rechecked_at": at,
            "fill_event_at": 2.5,
            "causal_target_report_id": report_id,
            "latest_target_report_id": report_id,
            "prior_inputs_hash": "b" * 64,
            "target_snapshot": {
                "t_wall": 2.5,
                "t_mono": 9.0,
                "virtual_sol_reserves": 70_000_000_000,
                "virtual_token_reserves": 70_000_000_000_000,
                "real_sol_reserves": 42_500_000_000,
                "real_token_reserves": 400_000_000_000_000,
                "liquidity_sol": 42.5,
                "spot_price_sol": 0.000001,
                "progress_pct": 50.0,
            },
            "verdict": {
                "status": "CANONICAL",
                "reason": reason,
                "canonical_mint": "M",
                "inputs_hash": verdict_inputs_hash,
            },
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        return payload, hashlib.sha256(encoded.encode()).hexdigest()

    return conn, decision_id, report_id, recheck_payload


def _insert_strict_p3_recheck(conn, decision_id, report_id, recheck_payload):
    from memebot.store import (allocate_p3_causal_wall, p3_immediate_transaction,
                               record_canonical_recheck)

    with p3_immediate_transaction(conn):
        rechecked_at = allocate_p3_causal_wall(conn, raw_wall=3.0)
        payload, proof_hash = recheck_payload(at=rechecked_at)
        recheck_id = record_canonical_recheck(
            conn,
            decision_id=decision_id,
            attempt=1,
            rechecked_at=rechecked_at,
            causal_target_report_id=report_id,
            latest_target_report_id=report_id,
            status="PASS",
            reason="canonical_selected",
            canonical_mint="M",
            prior_inputs_hash="b" * 64,
            recheck_inputs_hash=proof_hash,
            payload=payload,
        )
    return recheck_id, proof_hash


def test_canonical_recheck_insert_and_exact_retry_are_idempotent(tmp_path):
    from memebot.store import (allocate_p3_causal_wall, p3_immediate_transaction,
                               record_canonical_recheck)

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "recheck-idempotent.db",
    )
    with p3_immediate_transaction(conn):
        rechecked_at = allocate_p3_causal_wall(conn, raw_wall=3.0)
        payload, proof_hash = payload_for(at=rechecked_at)
        kwargs = {
            "decision_id": decision_id,
            "attempt": 1,
            "rechecked_at": rechecked_at,
            "causal_target_report_id": report_id,
            "latest_target_report_id": report_id,
            "status": "PASS",
            "reason": "canonical_selected",
            "canonical_mint": "M",
            "prior_inputs_hash": "b" * 64,
            "recheck_inputs_hash": proof_hash,
            "payload": payload,
        }
        first = record_canonical_recheck(conn, **kwargs)
        repeated = record_canonical_recheck(conn, **kwargs)

    assert repeated == first
    assert conn.execute("SELECT count(*) FROM canonical_rechecks").fetchone()[0] == 1
    conn.close()


def test_canonical_recheck_conflicting_retry_is_rejected(tmp_path):
    from memebot.store import (EvidenceIntegrityError, allocate_p3_causal_wall,
                               p3_immediate_transaction, record_canonical_recheck)

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "recheck-conflict.db",
    )
    recheck_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    with pytest.raises(EvidenceIntegrityError, match="conflicting canonical recheck"):
        with p3_immediate_transaction(conn):
            rechecked_at = allocate_p3_causal_wall(conn, raw_wall=4.0)
            payload, proof_hash = payload_for(
                at=rechecked_at, verdict_inputs_hash="e" * 64,
            )
            record_canonical_recheck(
                conn,
                decision_id=decision_id,
                attempt=1,
                rechecked_at=rechecked_at,
                causal_target_report_id=report_id,
                latest_target_report_id=report_id,
                status="PASS",
                reason="canonical_selected",
                canonical_mint="M",
                prior_inputs_hash="b" * 64,
                recheck_inputs_hash=proof_hash,
                payload=payload,
            )

    assert tuple(conn.execute(
        "SELECT id,reason FROM canonical_rechecks"
    ).fetchone()) == (recheck_id, "canonical_selected")
    conn.close()


def test_p3_buy_planned_size_uses_fill_notional():
    from memebot.store import validate_p3_fill_notional

    assert validate_p3_fill_notional(
        qty=2.0, fill_price=5.0, planned_size_sol=10.0,
    ) is None
    assert validate_p3_fill_notional(
        qty=3.0, fill_price=0.1, planned_size_sol=0.30000000000000004,
    ) is None
    for values in (
        {"qty": 2.0, "fill_price": 4.0, "planned_size_sol": 10.0},
        {"qty": True, "fill_price": 5.0, "planned_size_sol": 10.0},
        {"qty": 2.0, "fill_price": float("nan"), "planned_size_sol": 10.0},
        {"qty": 2.0, "fill_price": 5.0, "planned_size_sol": 0.0},
    ):
        with pytest.raises(ValueError):
            validate_p3_fill_notional(**values)


def test_canonical_paper_buy_trade_and_execution_are_atomic(tmp_path):
    import sqlite3

    from memebot.store import record_canonical_paper_buy

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "canonical-buy.db",
    )
    recheck_id, proof_hash = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    trade_id, execution_id = record_canonical_paper_buy(
        conn,
        decision_id=decision_id,
        recheck_id=recheck_id,
        raw_wall=4.0,
        mint="M",
        segment="CLIMBING",
        qty=2.0,
        quote_price=4.5,
        fill_price=5.0,
        fees={"base_sol": 0.01},
        realism_grade="B",
        planned_size_sol=10.0,
    )
    trade = conn.execute("SELECT * FROM paper_trades WHERE id=?", (trade_id,)).fetchone()
    execution = conn.execute(
        "SELECT * FROM paper_entry_executions WHERE id=?", (execution_id,),
    ).fetchone()
    position = conn.execute(
        "SELECT * FROM p3_position_current WHERE decision_id=?", (decision_id,),
    ).fetchone()
    assert trade["canonical_recheck_id"] == recheck_id
    assert trade["canonical_proof_hash"] == proof_hash
    assert trade["at"] == execution["at"] == position["last_trade_at"]
    assert execution["status"] == "FILLED"
    assert execution["paper_trade_id"] == trade_id
    assert position["entry_execution_id"] == execution_id
    assert position["buy_notional_sol"] == 10.01
    conn.close()

    failing, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "canonical-buy-rollback.db",
    )
    recheck_id, _ = _insert_strict_p3_recheck(
        failing, decision_id, report_id, payload_for,
    )
    clock_before = failing.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    failing.execute(
        "CREATE TRIGGER test_fail_filled BEFORE INSERT ON paper_entry_executions "
        "BEGIN SELECT RAISE(ABORT,'injected execution failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected execution failure"):
        record_canonical_paper_buy(
            failing,
            decision_id=decision_id,
            recheck_id=recheck_id,
            raw_wall=4.0,
            mint="M",
            segment="CLIMBING",
            qty=2.0,
            quote_price=4.5,
            fill_price=5.0,
            fees={},
            realism_grade="B",
            planned_size_sol=10.0,
        )
    assert failing.execute("SELECT count(*) FROM paper_trades").fetchone()[0] == 0
    assert failing.execute(
        "SELECT count(*) FROM paper_entry_executions"
    ).fetchone()[0] == 0
    assert failing.execute("SELECT count(*) FROM p3_position_current").fetchone()[0] == 0
    assert failing.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before
    failing.close()


def _insert_cancel_recheck(conn, decision_id, report_id):
    import hashlib
    import json

    from memebot.store import (allocate_p3_causal_wall, p3_immediate_transaction,
                               record_canonical_recheck)

    with p3_immediate_transaction(conn):
        rechecked_at = allocate_p3_causal_wall(conn, raw_wall=3.0)
        payload = {
            "decision_id": decision_id,
            "attempt": 1,
            "trigger": "safety_hard_fail",
            "trigger_report_id": report_id,
            "rechecked_at": rechecked_at,
            "fill_event_at": None,
            "causal_target_report_id": report_id,
            "latest_target_report_id": report_id,
            "prior_inputs_hash": "b" * 64,
            "target_snapshot": None,
            "verdict": {
                "status": "UNRESOLVED",
                "reason": "safety_flip",
                "canonical_mint": None,
                "inputs_hash": "d" * 64,
            },
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        return record_canonical_recheck(
            conn,
            decision_id=decision_id,
            attempt=1,
            rechecked_at=rechecked_at,
            causal_target_report_id=report_id,
            latest_target_report_id=report_id,
            status="CANCEL",
            reason="safety_flip",
            canonical_mint=None,
            prior_inputs_hash="b" * 64,
            recheck_inputs_hash=hashlib.sha256(encoded.encode()).hexdigest(),
            payload=payload,
        )


def _fill_strict_p3_buy(conn, decision_id, report_id, payload_for):
    from memebot.store import record_canonical_paper_buy

    recheck_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    trade_id, execution_id = record_canonical_paper_buy(
        conn,
        decision_id=decision_id,
        recheck_id=recheck_id,
        raw_wall=4.0,
        mint="M",
        segment="CLIMBING",
        qty=2.0,
        quote_price=4.5,
        fill_price=5.0,
        fees={"base_sol": 0.01},
        realism_grade="B",
        planned_size_sol=10.0,
    )
    return trade_id, execution_id


def test_terminal_entry_execution_insert_and_exact_retry_are_idempotent(tmp_path):
    from memebot.store import (EvidenceIntegrityError,
                               record_terminal_entry_execution)

    conn, decision_id, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "terminal-cancelled.db",
    )
    recheck_id = _insert_cancel_recheck(conn, decision_id, report_id)
    first = record_terminal_entry_execution(
        conn,
        decision_id=decision_id,
        raw_wall=4.0,
        status="CANCELLED",
        reason="safety_flip",
        recheck_id=recheck_id,
    )
    clock_after_first = conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    assert record_terminal_entry_execution(
        conn,
        decision_id=decision_id,
        raw_wall=999.0,
        status="CANCELLED",
        reason="safety_flip",
        recheck_id=recheck_id,
    ) == first
    assert conn.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_after_first
    with pytest.raises(EvidenceIntegrityError, match="conflicting terminal entry execution"):
        record_terminal_entry_execution(
            conn,
            decision_id=decision_id,
            raw_wall=5.0,
            status="ABANDONED",
            reason="restart_before_fill",
            recheck_id=None,
        )
    conn.close()

    abandoned, decision_id, _, _ = _seed_strict_p3_buy(
        tmp_path, "terminal-abandoned.db",
    )
    abandoned_id = record_terminal_entry_execution(
        abandoned,
        decision_id=decision_id,
        raw_wall=3.0,
        status="ABANDONED",
        reason="restart_before_fill",
        recheck_id=None,
    )
    assert record_terminal_entry_execution(
        abandoned,
        decision_id=decision_id,
        raw_wall=999.0,
        status="ABANDONED",
        reason="restart_before_fill",
        recheck_id=None,
    ) == abandoned_id
    assert abandoned.execute(
        "SELECT status,reason,paper_trade_id FROM paper_entry_executions"
    ).fetchone()[0:] == ("ABANDONED", "restart_before_fill", None)
    abandoned.close()


def test_p3_sell_allocates_after_filled_entry_and_links_it(tmp_path):
    from memebot.store import record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "partial-p3-sell.db",
    )
    _, execution_id = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    entry_at = conn.execute(
        "SELECT at FROM paper_entry_executions WHERE id=?", (execution_id,),
    ).fetchone()[0]
    sell_id, outcome_id = record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=1.0,
        mint="M",
        segment="CLIMBING",
        qty=0.5,
        quote_price=4.0,
        fill_price=3.5,
        fees={"base_sol": 0.01},
        realism_grade="B",
        exit_reason="ladder_0",
        ladder_index=0,
    )
    sell = conn.execute("SELECT * FROM paper_trades WHERE id=?", (sell_id,)).fetchone()
    position = conn.execute(
        "SELECT * FROM p3_position_current WHERE decision_id=?", (decision_id,),
    ).fetchone()
    assert outcome_id is None
    assert sell["side"] == "sell"
    assert sell["p3_entry_execution_id"] == execution_id
    assert sell["canonical_recheck_id"] is None
    assert sell["canonical_proof_hash"] is None
    assert sell["at"] > entry_at
    assert position["sold_qty"] == 0.5
    assert position["ladder_mask"] == 1
    conn.close()


def test_final_p3_sell_and_trade_outcome_are_atomic_and_same_time(tmp_path):
    import json
    import sqlite3

    from memebot.store import record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "final-p3-sell.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    sell_id, outcome_id = record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=2.0,
        quote_price=6.0,
        fill_price=5.5,
        fees={"base_sol": 0.02},
        realism_grade="B",
        exit_reason="time_stop",
        ladder_index=None,
    )
    sell = conn.execute("SELECT * FROM paper_trades WHERE id=?", (sell_id,)).fetchone()
    outcome = conn.execute("SELECT * FROM outcomes WHERE id=?", (outcome_id,)).fetchone()
    position = conn.execute(
        "SELECT * FROM p3_position_current WHERE decision_id=?", (decision_id,),
    ).fetchone()
    detail = json.loads(outcome["detail_json"])
    assert sell["at"] == outcome["at"] == position["last_trade_at"]
    assert outcome["ref_kind"] == "trade"
    assert outcome["ref_id"] == sell_id
    assert outcome["p3_exit_trade_id"] == sell_id
    assert outcome["pnl_sol"] == position["sell_proceeds_sol"] - position["buy_notional_sol"]
    assert detail["reason"] == "time_stop"
    assert position["sold_qty"] == position["bought_qty"]
    conn.close()

    failing, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "final-p3-sell-rollback.db",
    )
    _fill_strict_p3_buy(failing, decision_id, report_id, payload_for)
    before = tuple(failing.execute(
        "SELECT sold_qty,sell_proceeds_sol,last_trade_at FROM p3_position_current"
    ).fetchone())
    clock_before = failing.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0]
    failing.execute(
        "CREATE TRIGGER test_fail_final_outcome BEFORE INSERT ON outcomes "
        "BEGIN SELECT RAISE(ABORT,'injected outcome failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected outcome failure"):
        record_canonical_paper_sell(
            failing,
            decision_id=decision_id,
            raw_wall=5.0,
            mint="M",
            segment="CLIMBING",
            qty=2.0,
            quote_price=6.0,
            fill_price=5.5,
            fees={},
            realism_grade="B",
            exit_reason="time_stop",
            ladder_index=None,
        )
    assert failing.execute(
        "SELECT count(*) FROM paper_trades WHERE side='sell'"
    ).fetchone()[0] == 0
    assert failing.execute(
        "SELECT count(*) FROM outcomes"
    ).fetchone()[0] == 0
    assert tuple(failing.execute(
        "SELECT sold_qty,sell_proceeds_sol,last_trade_at FROM p3_position_current"
    ).fetchone()) == before
    assert failing.execute(
        "SELECT last_wall FROM p3_causal_clock WHERE singleton=1"
    ).fetchone()[0] == clock_before
    failing.close()


def test_open_p3_position_selector_uses_filled_summary_and_persisted_ladder(tmp_path):
    from memebot.store import (EvidenceIntegrityError, list_open_p3_filled_positions,
                               record_canonical_paper_sell)

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "bounded-open-p3.db",
    )
    _, execution_id = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=0.5,
        quote_price=5.0,
        fill_price=4.5,
        fees={"base_sol": 0.01},
        realism_grade="B",
        exit_reason="ladder_0",
        ladder_index=0,
    )

    rows = list_open_p3_filled_positions(conn, max_open_positions=1)
    assert len(rows) == 1
    restored = rows[0]
    assert restored.entry_execution_id == execution_id
    assert restored.bought_qty == 2.0
    assert restored.sold_qty == 0.5
    assert restored.qty_remaining == 1.5
    assert restored.buy_notional_sol == 10.01
    assert restored.sell_proceeds_sol == 2.24
    assert restored.ladder_mask == 1
    with pytest.raises(ValueError, match="positive integer"):
        list_open_p3_filled_positions(conn, max_open_positions=0)
    conn.execute(
        "UPDATE p3_position_current SET buy_notional_sol=? WHERE decision_id=?",
        (10.010000000000002, decision_id),
    )
    conn.commit()
    with pytest.raises(EvidenceIntegrityError, match="invalid open P3 position"):
        list_open_p3_filled_positions(conn, max_open_positions=1)
    conn.close()


def test_open_p3_position_selector_is_indexed_bounded_and_fails_overflow(tmp_path):
    from memebot.store import EvidenceIntegrityError, list_open_p3_filled_positions

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "bounded-open-p3-overflow.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    captured = {}

    class DuplicateCursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows + self._rows

    class OverflowConnection:
        def execute(self, sql, parameters=()):
            captured["sql"] = sql
            captured["parameters"] = parameters
            return DuplicateCursor(conn.execute(sql, parameters).fetchall())

    with pytest.raises(EvidenceIntegrityError, match="open P3 position limit exceeded"):
        list_open_p3_filled_positions(OverflowConnection(), max_open_positions=1)

    assert captured["parameters"] == (2,)
    assert "LIMIT ?" in captured["sql"]
    assert "INDEXED BY p3_position_current_open_idx" in captured["sql"]
    plan = conn.execute(
        "EXPLAIN QUERY PLAN " + captured["sql"], captured["parameters"],
    ).fetchall()
    assert any(
        "p3_position_current_open_idx" in row["detail"] for row in plan
    )
    conn.close()


def _add_valid_p3_safety_children(conn, report_id, *, mint="M"):
    report = conn.execute(
        "SELECT checked_at FROM safety_reports WHERE id=?", (report_id,),
    ).fetchone()
    conn.execute(
        "INSERT INTO holder_evidence("
        "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
        "top10_non_curve_owner_share_pct,holder_observed_at,unavailable_reason,"
        "inputs_hash) VALUES (?,?,?,?,?,'',?)",
        (report_id, 2, 2, 50.0, report["checked_at"], "e" * 64),
    )
    conn.execute(
        "INSERT INTO early_buyer_reads("
        "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,safety_report_id"
        ") VALUES (?,?,'[\"BUYER\"]','',?,?)",
        (mint, report["checked_at"], "f" * 64, report_id),
    )
    conn.commit()


def test_restart_reconciles_newer_hardfail_before_generic_unmatched_buy(tmp_path):
    from memebot.store import reconcile_unmatched_p3_buys, save_safety_report

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "restart-hardfail-precedence.db",
    )
    _add_valid_p3_safety_children(conn, report_id)
    _insert_strict_p3_recheck(conn, decision_id, report_id, payload_for)
    hard_fail_id = save_safety_report(
        conn, mint="M", raw_completed_at=5.0, segment="CLIMBING",
        hard_fails=("rug",), risk_score=100.0, results_json="[]",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )
    _add_valid_p3_safety_children(conn, hard_fail_id)

    assert reconcile_unmatched_p3_buys(conn, raw_wall=10.0) == 1
    recheck = conn.execute(
        "SELECT * FROM canonical_rechecks ORDER BY attempt DESC LIMIT 1"
    ).fetchone()
    execution = conn.execute(
        "SELECT * FROM paper_entry_executions WHERE decision_id=?", (decision_id,),
    ).fetchone()
    assert (recheck["status"], recheck["reason"]) == (
        "CANCEL", "restart_safety_hard_fail",
    )
    assert recheck["latest_target_report_id"] == hard_fail_id
    assert (execution["status"], execution["reason"]) == (
        "CANCELLED", "restart_safety_hard_fail",
    )
    assert execution["canonical_recheck_id"] == recheck["id"]
    conn.close()


def test_restart_hardfail_reuses_exact_cancel_after_proof_commit_crash(
    tmp_path, monkeypatch,
):
    from memebot import store

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "restart-hardfail-proof-crash.db",
    )
    _add_valid_p3_safety_children(conn, report_id)
    _insert_strict_p3_recheck(conn, decision_id, report_id, payload_for)
    hard_fail_id = store.save_safety_report(
        conn, mint="M", raw_completed_at=5.0, segment="CLIMBING",
        hard_fails=("rug",), risk_score=100.0, results_json="[]",
        inputs_hash="1" * 64,
    )
    _add_valid_p3_safety_children(conn, hard_fail_id)
    real_terminal = store.record_terminal_entry_execution

    def crash_after_proof(*args, **kwargs):
        raise RuntimeError("injected crash after restart CANCEL proof")

    monkeypatch.setattr(store, "record_terminal_entry_execution", crash_after_proof)
    with pytest.raises(RuntimeError, match="injected crash"):
        store.reconcile_unmatched_p3_buys(conn, raw_wall=10.0)
    cancel = conn.execute(
        "SELECT * FROM canonical_rechecks WHERE status='CANCEL'"
    ).fetchone()
    assert cancel["reason"] == "restart_safety_hard_fail"
    assert conn.execute(
        "SELECT count(*) FROM paper_entry_executions"
    ).fetchone()[0] == 0

    statements = []
    conn.set_trace_callback(statements.append)
    monkeypatch.setattr(store, "record_terminal_entry_execution", real_terminal)
    assert store.reconcile_unmatched_p3_buys(conn, raw_wall=20.0) == 1
    conn.set_trace_callback(None)

    assert conn.execute(
        "SELECT count(*) FROM canonical_rechecks WHERE status='CANCEL'"
    ).fetchone()[0] == 1
    execution = conn.execute(
        "SELECT * FROM paper_entry_executions WHERE decision_id=?", (decision_id,),
    ).fetchone()
    assert execution["canonical_recheck_id"] == cancel["id"]
    newest_reads = [
        statement for statement in statements
        if "INDEXED BY safety_reports_mint_latest_idx" in statement
        and "ORDER BY id DESC LIMIT 1" in statement
    ]
    assert len(newest_reads) == 1
    conn.close()


def test_restart_unmatched_buy_malformed_latest_report_blocks(tmp_path):
    from memebot.store import (EvidenceIntegrityError, reconcile_unmatched_p3_buys,
                               save_safety_report)

    conn, _, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "restart-malformed-latest.db",
    )
    _add_valid_p3_safety_children(conn, report_id)
    save_safety_report(
        conn, mint="M", raw_completed_at=5.0, segment="CLIMBING",
        hard_fails=(), risk_score=0.0, results_json="[]",
        inputs_hash="1111111111111111111111111111111111111111111111111111111111111111",
    )

    with pytest.raises(EvidenceIntegrityError, match="latest safety evidence"):
        reconcile_unmatched_p3_buys(conn, raw_wall=10.0)
    assert conn.execute("SELECT count(*) FROM paper_entry_executions").fetchone()[0] == 0
    conn.close()


def test_restart_reconciles_remaining_unmatched_p3_buys(tmp_path):
    from memebot.store import reconcile_unmatched_p3_buys

    conn, decision_id, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "restart-before-fill.db",
    )
    _add_valid_p3_safety_children(conn, report_id)

    assert reconcile_unmatched_p3_buys(conn, raw_wall=10.0) == 1
    execution = conn.execute(
        "SELECT * FROM paper_entry_executions WHERE decision_id=?", (decision_id,),
    ).fetchone()
    assert (execution["status"], execution["reason"]) == (
        "ABANDONED", "restart_before_fill",
    )
    assert execution["canonical_recheck_id"] is None
    assert reconcile_unmatched_p3_buys(conn, raw_wall=20.0) == 0
    conn.close()


def _direct_p3_trade(conn, *, decision_id, **overrides):
    values = {
        "at": 5.0,
        "mint": "M",
        "segment": "CLIMBING",
        "side": "buy",
        "qty": 2.0,
        "quote_price": 4.5,
        "fill_price": 5.0,
        "fees_json": "{}",
        "realism_grade": "B",
        "canonical_recheck_id": None,
        "canonical_proof_hash": None,
        "p3_entry_execution_id": None,
    }
    values.update(overrides)
    return conn.execute(
        "INSERT INTO paper_trades("
        "decision_id,at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
        "realism_grade,canonical_recheck_id,canonical_proof_hash,"
        "p3_entry_execution_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (decision_id, *values.values()),
    ).lastrowid


def _direct_p3_execution(conn, *, decision_id, **overrides):
    values = {
        "at": 5.0,
        "status": "ABANDONED",
        "reason": "restart_before_fill",
        "planned_size_sol": 10.0,
        "canonical_recheck_id": None,
        "paper_trade_id": None,
    }
    values.update(overrides)
    return conn.execute(
        "INSERT INTO paper_entry_executions("
        "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id,"
        "paper_trade_id) VALUES (?,?,?,?,?,?,?)",
        (decision_id, *values.values()),
    ).lastrowid


def _direct_followup_recheck(
    conn, *, source_recheck_id, attempt=2, rechecked_at=6.0, **overrides,
):
    import hashlib
    import json

    source = conn.execute(
        "SELECT * FROM canonical_rechecks WHERE id=?", (source_recheck_id,),
    ).fetchone()
    values = {
        key: source[key]
        for key in (
            "decision_id",
            "causal_target_report_id",
            "latest_target_report_id",
            "status",
            "reason",
            "canonical_mint",
            "prior_inputs_hash",
        )
    }
    values.update(attempt=attempt, rechecked_at=rechecked_at)
    values.update(overrides)
    payload = json.loads(source["payload_json"])
    payload.update({
        "decision_id": values["decision_id"],
        "attempt": values["attempt"],
        "rechecked_at": values["rechecked_at"],
        "causal_target_report_id": values["causal_target_report_id"],
        "latest_target_report_id": values["latest_target_report_id"],
        "prior_inputs_hash": values["prior_inputs_hash"],
    })
    payload["verdict"].update({
        "status": "CANONICAL" if values["status"] == "PASS" else "UNRESOLVED",
        "reason": values["reason"],
        "canonical_mint": values["canonical_mint"],
    })
    payload_json = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    proof_hash = hashlib.sha256(payload_json.encode()).hexdigest()
    cursor = conn.execute(
        "INSERT INTO canonical_rechecks("
        "decision_id,attempt,rechecked_at,causal_target_report_id,"
        "latest_target_report_id,status,reason,canonical_mint,prior_inputs_hash,"
        "recheck_inputs_hash,payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            values["decision_id"],
            values["attempt"],
            values["rechecked_at"],
            values["causal_target_report_id"],
            values["latest_target_report_id"],
            values["status"],
            values["reason"],
            values["canonical_mint"],
            values["prior_inputs_hash"],
            proof_hash,
            payload_json,
        ),
    )
    return cursor.lastrowid, proof_hash


def _direct_close_p3_position(conn, *, decision_id, execution_id, at=6.0):
    sell_id = _direct_p3_trade(
        conn,
        decision_id=decision_id,
        at=at,
        side="sell",
        qty=2.0,
        quote_price=6.0,
        fill_price=5.5,
        fees_json='{"base_sol":0.02}',
        canonical_recheck_id=None,
        canonical_proof_hash=None,
        p3_entry_execution_id=execution_id,
    )
    return sell_id


def test_canonical_recheck_direct_guard_rejects_cross_mint_link(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "recheck-cross-mint.db",
    )
    recheck_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    with pytest.raises(
        sqlite3.IntegrityError,
        match="P3 recheck requires matching canonical decision proof",
    ):
        _direct_followup_recheck(
            conn,
            source_recheck_id=recheck_id,
            canonical_mint="OTHER",
        )
    assert conn.execute("SELECT count(*) FROM canonical_rechecks").fetchone()[0] == 1
    conn.close()


def test_canonical_recheck_requires_absolute_latest_target(tmp_path):
    from memebot.store import save_safety_report

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "recheck-absolute-latest.db",
    )
    recheck_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    newer_id = save_safety_report(
        conn,
        mint="M",
        raw_completed_at=4.0,
        segment="CLIMBING",
        hard_fails=(),
        risk_score=1.0,
        results_json="[]",
        inputs_hash="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    )
    assert newer_id > report_id
    with pytest.raises(
        sqlite3.IntegrityError,
        match="P3 recheck requires matching canonical decision proof",
    ):
        _direct_followup_recheck(
            conn,
            source_recheck_id=recheck_id,
            rechecked_at=6.0,
            latest_target_report_id=report_id,
        )
    assert conn.execute(
        "SELECT max(id) FROM canonical_rechecks",
    ).fetchone()[0] == recheck_id
    conn.close()


def test_paper_trade_side_domain_is_exact(tmp_path):
    conn, decision_id, _, _ = _seed_strict_p3_buy(tmp_path, "trade-side.db")
    for side in ("BUY", "Sell", " sell", "sell ", "", "hold"):
        with pytest.raises(
            sqlite3.IntegrityError,
            match="paper trade side must be exact lowercase buy/sell",
        ):
            _direct_p3_trade(conn, decision_id=decision_id, side=side)
    assert conn.execute("SELECT count(*) FROM paper_trades").fetchone()[0] == 0
    conn.close()


def test_p3_trade_shape_direct_guard(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "trade-shape.db",
    )
    recheck_id, proof_hash = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    malformed = (
        {"mint": " "},
        {"segment": " "},
        {"at": sqlite3.Binary(b"5")},
        {"qty": 0.0},
        {"quote_price": -1.0},
        {"fill_price": -1.0},
        {"fees_json": '{"fee":-1}'},
        {"realism_grade": ""},
        {"quote_price": 0.0},
        {"fill_price": 0.0},
    )
    with pytest.raises(sqlite3.IntegrityError):
        _direct_p3_trade(
            conn,
            decision_id=decision_id,
            canonical_recheck_id=recheck_id,
            canonical_proof_hash=None,
        )
    for override in malformed:
        trade_proof = {
            "canonical_recheck_id": recheck_id,
            "canonical_proof_hash": proof_hash,
        }
        trade_proof.update(override)
        with pytest.raises(sqlite3.Error):
            _direct_p3_trade(
                conn,
                decision_id=decision_id,
                **trade_proof,
            )
    assert conn.execute("SELECT count(*) FROM paper_trades").fetchone()[0] == 0
    conn.close()


def test_p3_buy_requires_same_mint_pass_proof(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "buy-same-mint.db",
    )
    recheck_id, proof_hash = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    conn.execute(
        "INSERT INTO tokens(mint,created_at,state,last_seen,meta_json) "
        "VALUES ('OTHER',0.0,'CLIMBING',0.0,'{}')",
    )
    with pytest.raises(
        sqlite3.IntegrityError, match="P3 BUY requires matching canonical PASS proof",
    ):
        _direct_p3_trade(
            conn,
            decision_id=decision_id,
            mint="OTHER",
            canonical_recheck_id=recheck_id,
            canonical_proof_hash=proof_hash,
        )
    conn.rollback()
    assert conn.execute("SELECT count(*) FROM paper_trades").fetchone()[0] == 0
    conn.close()


def test_p3_buy_reserved_proof_columns_require_canonical_observation(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "buy-reserved-proof.db",
    )
    recheck_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    decision = conn.execute(
        "SELECT feature_vector_json FROM decisions WHERE id=?", (decision_id,),
    ).fetchone()
    observationless_id = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,safety_report_id,config_hash"
        ") VALUES (2.5,'M','CLIMBING','BUY',90.0,?,?,?)",
        (decision["feature_vector_json"], report_id, "c" * 64),
    ).lastrowid
    conn.commit()
    observationless_recheck, observationless_hash = _direct_followup_recheck(
        conn,
        source_recheck_id=recheck_id,
        decision_id=observationless_id,
        attempt=1,
    )
    conn.commit()
    with pytest.raises(
        sqlite3.IntegrityError, match="P3 BUY requires matching canonical PASS proof",
    ):
        _direct_p3_trade(
            conn,
            decision_id=observationless_id,
            canonical_recheck_id=observationless_recheck,
            canonical_proof_hash=observationless_hash,
        )
    assert conn.execute("SELECT count(*) FROM paper_trades").fetchone()[0] == 0
    conn.close()


def test_p3_trade_and_terminal_links_require_latest_recheck(tmp_path):
    cases = {}
    for label in ("buy", "filled", "cancelled", "abandoned"):
        cases[label] = _seed_strict_p3_buy(tmp_path, f"latest-{label}.db")

    conn, decision_id, report_id, payload_for = cases["buy"]
    old_id, old_hash = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    _direct_followup_recheck(conn, source_recheck_id=old_id)
    conn.commit()
    with pytest.raises(
        sqlite3.IntegrityError, match="P3 BUY requires matching canonical PASS proof",
    ):
        _direct_p3_trade(
            conn,
            decision_id=decision_id,
            at=7.0,
            canonical_recheck_id=old_id,
            canonical_proof_hash=old_hash,
        )
    conn.close()

    conn, decision_id, report_id, payload_for = cases["filled"]
    old_id, old_hash = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    trade_id = _direct_p3_trade(
        conn,
        decision_id=decision_id,
        canonical_recheck_id=old_id,
        canonical_proof_hash=old_hash,
    )
    _direct_followup_recheck(conn, source_recheck_id=old_id)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            at=7.0,
            status="FILLED",
            reason="filled",
            canonical_recheck_id=old_id,
            paper_trade_id=trade_id,
        )
    conn.close()

    conn, decision_id, report_id, _ = cases["cancelled"]
    old_id = _insert_cancel_recheck(conn, decision_id, report_id)
    _direct_followup_recheck(
        conn,
        source_recheck_id=old_id,
        status="CANCEL",
        reason="safety_flip",
        canonical_mint=None,
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            at=7.0,
            status="CANCELLED",
            reason="safety_flip",
            canonical_recheck_id=old_id,
        )
    conn.close()

    conn, decision_id, report_id, payload_for = cases["abandoned"]
    old_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    _direct_followup_recheck(conn, source_recheck_id=old_id)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            at=7.0,
            status="ABANDONED",
            reason="restart_after_pass",
            canonical_recheck_id=old_id,
        )
    conn.close()


def test_p3_filled_execution_direct_guard_requires_fill_notional(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "filled-notional-guard.db",
    )
    recheck_id, proof_hash = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    trade_id = _direct_p3_trade(
        conn,
        decision_id=decision_id,
        quote_price=4.0,
        fill_price=4.5,
        canonical_recheck_id=recheck_id,
        canonical_proof_hash=proof_hash,
    )
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            status="FILLED",
            reason="filled",
            canonical_recheck_id=recheck_id,
            paper_trade_id=trade_id,
        )
    assert conn.execute("SELECT count(*) FROM paper_entry_executions").fetchone()[0] == 0
    conn.close()


def test_p3_terminal_execution_requires_decision_planned_size(tmp_path):
    conn, decision_id, _, _ = _seed_strict_p3_buy(
        tmp_path, "terminal-planned-size.db",
    )
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            planned_size_sol=11.0,
        )
    assert conn.execute("SELECT count(*) FROM paper_entry_executions").fetchone()[0] == 0
    conn.close()


def test_terminal_entry_execution_conflicting_retry_is_rejected(tmp_path):
    from memebot.store import EvidenceIntegrityError, record_terminal_entry_execution

    conn, decision_id, _, _ = _seed_strict_p3_buy(
        tmp_path, "terminal-conflicting-retry.db",
    )
    execution_id = record_terminal_entry_execution(
        conn,
        decision_id=decision_id,
        raw_wall=3.0,
        status="ABANDONED",
        reason="restart_before_fill",
        recheck_id=None,
    )
    with pytest.raises(
        EvidenceIntegrityError, match="conflicting terminal entry execution",
    ):
        record_terminal_entry_execution(
            conn,
            decision_id=decision_id,
            raw_wall=4.0,
            status="CANCELLED",
            reason="safety_flip",
            recheck_id=None,
        )
    assert conn.execute(
        "SELECT id,status,reason FROM paper_entry_executions",
    ).fetchone()[0:] == (execution_id, "ABANDONED", "restart_before_fill")
    conn.close()


def test_cancelled_reason_must_equal_cancel_recheck_reason(tmp_path):
    conn, decision_id, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "cancel-reason.db",
    )
    recheck_id = _insert_cancel_recheck(conn, decision_id, report_id)
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            status="CANCELLED",
            reason="different_reason",
            canonical_recheck_id=recheck_id,
        )
    assert conn.execute("SELECT count(*) FROM paper_entry_executions").fetchone()[0] == 0
    conn.close()


def test_abandoned_cannot_link_cancel_recheck(tmp_path):
    conn, decision_id, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "abandoned-cancel-link.db",
    )
    recheck_id = _insert_cancel_recheck(conn, decision_id, report_id)
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(
            conn,
            decision_id=decision_id,
            status="ABANDONED",
            reason="restart_after_pass",
            canonical_recheck_id=recheck_id,
        )
    conn.close()


def test_abandoned_null_recheck_rejected_when_cancel_exists(tmp_path):
    conn, decision_id, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "abandoned-null-cancel.db",
    )
    _insert_cancel_recheck(conn, decision_id, report_id)
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(conn, decision_id=decision_id)
    conn.close()


def test_abandoned_null_recheck_rejected_when_pass_exists(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "abandoned-null-pass.db",
    )
    _insert_strict_p3_recheck(conn, decision_id, report_id, payload_for)
    with pytest.raises(sqlite3.IntegrityError, match="invalid P3 terminal execution"):
        _direct_p3_execution(conn, decision_id=decision_id)
    conn.close()


def test_p3_position_summary_tracks_filled_and_sells(tmp_path):
    from memebot.store import record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "position-summary-qty.db",
    )
    _, execution_id = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    initial = conn.execute(
        "SELECT * FROM p3_position_current WHERE decision_id=?", (decision_id,),
    ).fetchone()
    assert (initial["entry_execution_id"], initial["bought_qty"], initial["sold_qty"]) == (
        execution_id,
        2.0,
        0.0,
    )
    record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=0.5,
        quote_price=5.0,
        fill_price=4.5,
        fees={},
        realism_grade="B",
        exit_reason="ladder_0",
        ladder_index=0,
    )
    partial = conn.execute(
        "SELECT sold_qty,ladder_mask FROM p3_position_current WHERE decision_id=?",
        (decision_id,),
    ).fetchone()
    assert tuple(partial) == (0.5, 1)
    record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=6.0,
        mint="M",
        segment="CLIMBING",
        qty=1.5,
        quote_price=5.0,
        fill_price=4.0,
        fees={},
        realism_grade="B",
        exit_reason="time_stop",
        ladder_index=None,
    )
    assert conn.execute(
        "SELECT bought_qty,sold_qty FROM p3_position_current WHERE decision_id=?",
        (decision_id,),
    ).fetchone()[0:] == (2.0, 2.0)
    conn.close()


def test_p3_position_summary_tracks_bounded_financials(tmp_path):
    from memebot.store import record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "position-summary-money.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    assert conn.execute(
        "SELECT buy_notional_sol,sell_proceeds_sol FROM p3_position_current",
    ).fetchone()[0:] == (10.01, 0.0)
    record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=0.5,
        quote_price=5.0,
        fill_price=4.5,
        fees={"base_sol": 0.01},
        realism_grade="B",
        exit_reason="ladder_0",
        ladder_index=0,
    )
    summary = conn.execute(
        "SELECT buy_notional_sol,sell_proceeds_sol FROM p3_position_current",
    ).fetchone()
    assert tuple(summary) == (10.01, 2.24)
    conn.close()


def test_buy_trade_and_filled_processing_time_allocates_after_recheck(tmp_path):
    from memebot.store import record_canonical_paper_buy

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "buy-causal-time.db",
    )
    recheck_id, _ = _insert_strict_p3_recheck(
        conn, decision_id, report_id, payload_for,
    )
    rechecked_at = conn.execute(
        "SELECT rechecked_at FROM canonical_rechecks WHERE id=?", (recheck_id,),
    ).fetchone()[0]
    trade_id, execution_id = record_canonical_paper_buy(
        conn,
        decision_id=decision_id,
        recheck_id=recheck_id,
        raw_wall=0.0,
        mint="M",
        segment="CLIMBING",
        qty=2.0,
        quote_price=4.5,
        fill_price=5.0,
        fees={},
        realism_grade="B",
        planned_size_sol=10.0,
    )
    trade_at = conn.execute(
        "SELECT at FROM paper_trades WHERE id=?", (trade_id,),
    ).fetchone()[0]
    execution_at = conn.execute(
        "SELECT at FROM paper_entry_executions WHERE id=?", (execution_id,),
    ).fetchone()[0]
    assert trade_at == execution_at > rechecked_at
    conn.close()


def test_terminal_execution_processing_time_allocates_after_proof(tmp_path):
    from memebot.store import record_terminal_entry_execution

    conn, decision_id, report_id, _ = _seed_strict_p3_buy(
        tmp_path, "terminal-causal-time.db",
    )
    recheck_id = _insert_cancel_recheck(conn, decision_id, report_id)
    rechecked_at = conn.execute(
        "SELECT rechecked_at FROM canonical_rechecks WHERE id=?", (recheck_id,),
    ).fetchone()[0]
    execution_id = record_terminal_entry_execution(
        conn,
        decision_id=decision_id,
        raw_wall=0.0,
        status="CANCELLED",
        reason="safety_flip",
        recheck_id=recheck_id,
    )
    assert conn.execute(
        "SELECT at FROM paper_entry_executions WHERE id=?", (execution_id,),
    ).fetchone()[0] > rechecked_at
    conn.close()


def test_restart_execution_processing_time_survives_regression(tmp_path):
    from memebot.store import record_terminal_entry_execution

    path = tmp_path / "restart-causal-time.db"
    conn, decision_id, _, _ = _seed_strict_p3_buy(tmp_path, path.name)
    decision_at = conn.execute(
        "SELECT at FROM decisions WHERE id=?", (decision_id,),
    ).fetchone()[0]
    conn.close()
    reopened = open_db(path, migration_clock=lambda: 0.0)
    execution_id = record_terminal_entry_execution(
        reopened,
        decision_id=decision_id,
        raw_wall=0.0,
        status="ABANDONED",
        reason="restart_before_fill",
        recheck_id=None,
    )
    assert reopened.execute(
        "SELECT at FROM paper_entry_executions WHERE id=?", (execution_id,),
    ).fetchone()[0] > decision_at
    reopened.close()


def test_p3_sell_reserved_columns_enter_strict_scope(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "sell-reserved-scope.db",
    )
    _, execution_id = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    recheck = conn.execute(
        "SELECT canonical_recheck_id,canonical_proof_hash FROM paper_trades "
        "WHERE side='buy'",
    ).fetchone()
    invalid = (
        {},
        {
            "p3_entry_execution_id": execution_id,
            "canonical_recheck_id": recheck["canonical_recheck_id"],
            "canonical_proof_hash": recheck["canonical_proof_hash"],
        },
        {"p3_entry_execution_id": execution_id + 999},
    )
    for proof in invalid:
        with pytest.raises(
            sqlite3.IntegrityError, match="P3 SELL requires matching open FILLED entry",
        ):
            _direct_p3_trade(
                conn,
                decision_id=decision_id,
                at=6.0,
                side="sell",
                qty=0.5,
                quote_price=5.0,
                fill_price=4.5,
                **proof,
            )
    assert conn.execute(
        "SELECT count(*) FROM paper_trades WHERE side='sell'",
    ).fetchone()[0] == 0
    conn.close()


def test_p3_partial_sell_updates_ladder_mask_atomically(tmp_path):
    from memebot.store import EvidenceIntegrityError, record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "partial-ladder-atomic.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    sell_id, outcome_id = record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=0.5,
        quote_price=5.0,
        fill_price=4.5,
        fees={},
        realism_grade="B",
        exit_reason="ladder_3",
        ladder_index=3,
    )
    assert outcome_id is None
    assert conn.execute(
        "SELECT ladder_mask,sold_qty FROM p3_position_current",
    ).fetchone()[0:] == (8, 0.5)
    with pytest.raises(EvidenceIntegrityError, match="repeated P3 ladder"):
        record_canonical_paper_sell(
            conn,
            decision_id=decision_id,
            raw_wall=6.0,
            mint="M",
            segment="CLIMBING",
            qty=0.5,
            quote_price=5.0,
            fill_price=4.5,
            fees={},
            realism_grade="B",
            exit_reason="ladder_3",
            ladder_index=3,
        )
    assert conn.execute(
        "SELECT id FROM paper_trades WHERE side='sell'",
    ).fetchall()[0][0] == sell_id
    conn.close()


def test_p3_final_sell_has_exactly_one_outcome(tmp_path):
    from memebot.store import record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "final-one-outcome.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    sell_id, outcome_id = record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=2.0,
        quote_price=6.0,
        fill_price=5.5,
        fees={},
        realism_grade="B",
        exit_reason="time_stop",
        ladder_index=None,
    )
    outcome = conn.execute("SELECT * FROM outcomes WHERE id=?", (outcome_id,)).fetchone()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outcomes("
            "at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id"
            ") VALUES (?,?,?,?,?,?)",
            (
                outcome["at"],
                outcome["ref_kind"],
                outcome["ref_id"],
                outcome["pnl_sol"],
                outcome["detail_json"],
                sell_id,
            ),
        )
    assert conn.execute(
        "SELECT count(*) FROM outcomes WHERE p3_exit_trade_id=?", (sell_id,),
    ).fetchone()[0] == 1
    conn.close()


def _valid_direct_p3_outcome(conn, *, sell_id):
    import json

    sell = conn.execute(
        "SELECT pt.at,pt.realism_grade,e.at AS entry_at,"
        "pc.sell_proceeds_sol-pc.buy_notional_sol AS pnl "
        "FROM paper_trades pt "
        "JOIN paper_entry_executions e ON e.id=pt.p3_entry_execution_id "
        "JOIN p3_position_current pc ON pc.decision_id=pt.decision_id "
        "WHERE pt.id=?",
        (sell_id,),
    ).fetchone()
    detail = {
        "grade": sell["realism_grade"],
        "hold_s": sell["at"] - sell["entry_at"],
        "reason": "time_stop",
    }
    return {
        "at": sell["at"],
        "ref_kind": "trade",
        "ref_id": sell_id,
        "pnl_sol": sell["pnl"],
        "detail_json": json.dumps(detail, sort_keys=True, separators=(",", ":")),
        "p3_exit_trade_id": sell_id,
    }


def _insert_direct_p3_outcome(conn, values):
    conn.execute(
        "INSERT INTO outcomes("
        "at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id"
        ") VALUES (?,?,?,?,?,?)",
        tuple(values[key] for key in (
            "at",
            "ref_kind",
            "ref_id",
            "pnl_sol",
            "detail_json",
            "p3_exit_trade_id",
        )),
    )


def test_p3_final_outcome_direct_guard_requires_exact_summary_and_detail(tmp_path):
    import json

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "final-outcome-guard.db",
    )
    _, execution_id = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    sell_id = _direct_close_p3_position(
        conn, decision_id=decision_id, execution_id=execution_id,
    )
    valid = _valid_direct_p3_outcome(conn, sell_id=sell_id)
    bad_details = (
        {"grade": "B", "hold_s": 2.0},
        {"grade": "B", "hold_s": 2.0, "reason": "ladder_0"},
        {"grade": "A", "hold_s": 2.0, "reason": "time_stop"},
        {"grade": "B", "hold_s": 999.0, "reason": "time_stop"},
        {"grade": "B", "hold_s": 2.0, "reason": "time_stop", "extra": 1},
    )
    variants = [{**valid, "pnl_sol": valid["pnl_sol"] + 1.0}]
    variants.extend({
        **valid,
        "detail_json": json.dumps(
            detail, sort_keys=True, separators=(",", ":"),
        ),
    } for detail in bad_details)
    for malformed in variants:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="P3 outcome requires causal exit/observation proof",
        ):
            _insert_direct_p3_outcome(conn, malformed)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
    conn.close()


def test_p3_exit_outcome_wrong_ref_kind_or_id_is_rejected(tmp_path):
    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "final-outcome-ref.db",
    )
    _, execution_id = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    sell_id = _direct_close_p3_position(
        conn, decision_id=decision_id, execution_id=execution_id,
    )
    valid = _valid_direct_p3_outcome(conn, sell_id=sell_id)
    for malformed in (
        {**valid, "ref_kind": "decision"},
        {**valid, "ref_id": sell_id + 999},
        {**valid, "p3_exit_trade_id": sell_id + 999},
    ):
        with pytest.raises(sqlite3.IntegrityError):
            _insert_direct_p3_outcome(conn, malformed)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
    conn.close()


def test_final_p3_outcome_uses_bounded_summary_pnl(tmp_path):
    from memebot.store import record_canonical_paper_sell

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "final-summary-pnl.db",
    )
    _fill_strict_p3_buy(conn, decision_id, report_id, payload_for)
    statements = []
    conn.set_trace_callback(statements.append)
    _, outcome_id = record_canonical_paper_sell(
        conn,
        decision_id=decision_id,
        raw_wall=5.0,
        mint="M",
        segment="CLIMBING",
        qty=2.0,
        quote_price=6.0,
        fill_price=5.5,
        fees={"base_sol": 0.02},
        realism_grade="B",
        exit_reason="time_stop",
        ladder_index=None,
    )
    conn.set_trace_callback(None)
    summary = conn.execute(
        "SELECT buy_notional_sol,sell_proceeds_sol FROM p3_position_current",
    ).fetchone()
    pnl = conn.execute(
        "SELECT pnl_sol FROM outcomes WHERE id=?", (outcome_id,),
    ).fetchone()[0]
    assert pnl == summary["sell_proceeds_sol"] - summary["buy_notional_sol"]
    assert not any(
        "sum(" in statement.lower() and "paper_trades" in statement.lower()
        for statement in statements
    )
    conn.close()


def test_generic_trade_and_outcome_writers_reject_p3_refs(tmp_path):
    from memebot.store import record_outcome, record_paper_trade

    conn, decision_id, report_id, payload_for = _seed_strict_p3_buy(
        tmp_path, "generic-p3-writers.db",
    )
    for side in ("buy", "sell"):
        with pytest.raises(ValueError, match="strict P3 trade helpers"):
            record_paper_trade(
                conn,
                decision_id=decision_id,
                at=3.0,
                mint="M",
                segment="CLIMBING",
                side=side,
                qty=1.0,
                quote_price=1.0,
                fill_price=1.0,
                fees={},
                realism_grade="B",
            )
    trade_id, _ = _fill_strict_p3_buy(
        conn, decision_id, report_id, payload_for,
    )
    with pytest.raises(ValueError, match="strict P3 outcome helpers"):
        record_outcome(
            conn,
            at=10.0,
            ref_kind="trade",
            ref_id=trade_id,
            pnl_sol=0.0,
            detail={},
        )
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
    conn.close()
