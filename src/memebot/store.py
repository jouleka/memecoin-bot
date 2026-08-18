"""SQLite store: WAL, versioned migrations, append-only ledger tables (spec §5.7).

Ledger tables (decisions/paper_trades/outcomes/regime_log) get BEFORE UPDATE/DELETE
triggers that RAISE(ABORT) — append-only is enforced by the database, not by
convention. Later milestones extend the schema with new user_version migrations.
`boots` and `tokens` are operational state, NOT ledger tables — updates allowed.
"""
from __future__ import annotations

import hashlib
import json
import json as _json_ledger
import math
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from threading import get_ident
from typing import TYPE_CHECKING, Any, Iterator, Literal

from memebot.canonical import (
    CanonicalObservationDraft,
    canonical_generation_hash,
    creator_component,
    first_mover_component,
    holder_component,
    integer_rank_points,
    liquidity_component,
    normalize_identity,
    normalize_telegram,
    normalize_twitter,
    normalize_uri,
    normalize_website,
    quantize_component,
    rank_eligible_candidates,
    social_component,
)
from memebot.early_buyers import EarlyBuyerEvidenceDraft
from memebot.safety.checks import HolderEvidenceDraft

if TYPE_CHECKING:
    from memebot.safety.gate import SafetyReport

# NEVER edit SCHEMA_V1 in place once shipped: IF NOT EXISTS silently no-ops on
# changed column defs (verified) — any schema change is a new SCHEMA_V2 string
# and user_version bump in open_db.
SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS tokens (
    mint TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    state TEXT NOT NULL DEFAULT 'FRESH',
    curve_progress REAL NOT NULL DEFAULT 0,
    last_seen REAL NOT NULL,
    meta_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS safety_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL,
    checked_at REAL NOT NULL,
    hard_fails_json TEXT NOT NULL,
    risk_score REAL NOT NULL,
    inputs_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    mint TEXT NOT NULL,
    segment TEXT NOT NULL,
    action TEXT NOT NULL,
    score REAL NOT NULL,
    feature_vector_json TEXT NOT NULL,
    safety_report_id INTEGER REFERENCES safety_reports(id),
    config_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER REFERENCES decisions(id),
    at REAL NOT NULL,
    mint TEXT NOT NULL,
    segment TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    quote_price REAL NOT NULL,
    fill_price REAL NOT NULL,
    fees_json TEXT NOT NULL,
    realism_grade TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    ref_kind TEXT NOT NULL,
    ref_id INTEGER NOT NULL,
    pnl_sol REAL NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS regime_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    state TEXT NOT NULL,
    inputs_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS boots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    config_hash TEXT NOT NULL,
    clean_shutdown INTEGER NOT NULL DEFAULT 0
);
"""

# Same guardrail as SCHEMA_V1 above: never edit this string in place once shipped.
# Schema changes are new version strings + a user_version bump, applied through
# open_db's migration chain, never in-place edits of a shipped ALTER/CREATE string.
SCHEMA_V2 = "ALTER TABLE tokens ADD COLUMN bonding_curve_key TEXT NOT NULL DEFAULT ''"

# Same guardrail as SCHEMA_V1/V2 above: never edit this string in place once shipped.
# M3 (MB-5) delta 7: a token DEAD via a safety hard-fail must never be resurrected by
# authoritative graduation (M2 C10 behavior) — `rugged` is the sticky marker the
# lifecycle resurrection guard checks. Abandoned-DEAD (rugged=0) still resurrects.
SCHEMA_V3 = "ALTER TABLE tokens ADD COLUMN rugged INTEGER NOT NULL DEFAULT 0"

# Same guardrail as previous schema constants: P2 adds append-only evidence tables for
# smart-money snapshots and early-buyer gate reads; do not edit in place after shipping.
SCHEMA_V4 = """
CREATE TABLE IF NOT EXISTS wallet_pnl_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    realized_pnl_sol REAL NOT NULL,
    source TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS early_buyer_reads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL,
    checked_at REAL NOT NULL,
    buyers_json TEXT NOT NULL,
    unavailable_reason TEXT NOT NULL DEFAULT '',
    inputs_hash TEXT NOT NULL
);
"""

# Inert v5 metadata: open_db does not apply these until the complete v5 migration is activated.
V5_ADDITIVE_COLUMNS = (
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

# P3 curve-progress reserve persistence is an additive operational migration.
# Never edit shipped v1-v5 schema definitions in place.
SCHEMA_V6_ADDITIVE_COLUMNS = (
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

V5_TABLE_DDL = (
    (
        "holder_evidence",
        """CREATE TABLE IF NOT EXISTS holder_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    safety_report_id INTEGER NOT NULL UNIQUE REFERENCES safety_reports(id),
    sampled_token_accounts INTEGER,
    distinct_non_curve_owners INTEGER,
    top10_non_curve_owner_share_pct REAL,
    holder_observed_at REAL NOT NULL CHECK (
      typeof(holder_observed_at) IN ('integer','real')
      AND holder_observed_at BETWEEN 0.0 AND 4102444800.0),
    unavailable_reason TEXT NOT NULL DEFAULT '',
    inputs_hash TEXT NOT NULL CHECK (length(inputs_hash)=64),
    CHECK (
      (unavailable_reason=''
       AND typeof(sampled_token_accounts)='integer' AND sampled_token_accounts>0
       AND typeof(distinct_non_curve_owners)='integer'
       AND distinct_non_curve_owners BETWEEN 1 AND sampled_token_accounts
       AND typeof(top10_non_curve_owner_share_pct) IN ('integer','real')
       AND top10_non_curve_owner_share_pct BETWEEN 0.0 AND 100.0)
      OR
      (unavailable_reason<>''
       AND sampled_token_accounts IS NULL
       AND distinct_non_curve_owners IS NULL
       AND top10_non_curve_owner_share_pct IS NULL)
    )
);""",
    ),
    (
        "creator_reputation_events",
        """CREATE TABLE IF NOT EXISTS creator_reputation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL REFERENCES tokens(mint),
    creator TEXT NOT NULL CHECK (
      typeof(creator)='text'
      AND length(creator) BETWEEN 1 AND 128
      AND creator=trim(creator,' ')
      AND instr(creator,char(0))=0),
    outcome TEXT NOT NULL CHECK (outcome IN ('GRADUATED','RUGGED')),
    observed_at REAL NOT NULL CHECK (
      typeof(observed_at) IN ('integer','real')
      AND observed_at BETWEEN 0.0 AND 4102444800.0),
    UNIQUE(mint,outcome)
);""",
    ),
    (
        "creator_reputation_current",
        """CREATE TABLE IF NOT EXISTS creator_reputation_current (
    mint TEXT PRIMARY KEY REFERENCES tokens(mint),
    creator TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('GRADUATED','RUGGED')),
    observed_at REAL NOT NULL,
    event_id INTEGER NOT NULL UNIQUE REFERENCES creator_reputation_events(id)
);""",
    ),
    (
        "p3_causal_clock",
        """CREATE TABLE IF NOT EXISTS p3_causal_clock (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    last_wall REAL NOT NULL CHECK (
      typeof(last_wall) IN ('integer','real')
      AND last_wall BETWEEN 0.0 AND 4102444800.0)
);""",
    ),
    (
        "wallet_pnl_summary",
        """CREATE TABLE IF NOT EXISTS wallet_pnl_summary (
    wallet TEXT PRIMARY KEY,
    event_count INTEGER NOT NULL CHECK (event_count>0),
    realized_pnl_sol REAL NOT NULL CHECK (
      typeof(realized_pnl_sol) IN ('integer','real')
      AND realized_pnl_sol BETWEEN -1000000000000.0 AND 1000000000000.0),
    last_at REAL NOT NULL CHECK (
      typeof(last_at) IN ('integer','real') AND last_at BETWEEN 0.0 AND 4102444800.0),
    last_event_id INTEGER NOT NULL UNIQUE REFERENCES wallet_pnl_events(id)
);""",
    ),
    (
        "canonical_observations",
        """CREATE TABLE IF NOT EXISTS canonical_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER NOT NULL REFERENCES decisions(id),
    mint TEXT NOT NULL REFERENCES tokens(mint)
      CHECK (length(trim(mint)) BETWEEN 1 AND 128),
    observed_at REAL NOT NULL CHECK (
      typeof(observed_at) IN ('integer','real')
      AND observed_at BETWEEN 0.0 AND 4102444800.0),
    is_subject INTEGER NOT NULL CHECK (typeof(is_subject)='integer' AND is_subject IN (0,1)),
    is_canonical INTEGER NOT NULL CHECK (typeof(is_canonical)='integer' AND is_canonical IN (0,1)),
    eligible INTEGER NOT NULL CHECK (typeof(eligible)='integer' AND eligible IN (0,1)),
    start_price_sol REAL,
    price_observed_at REAL,
    price_source TEXT NOT NULL DEFAULT '',
    unavailable_reason TEXT NOT NULL DEFAULT '',
    UNIQUE(decision_id,mint),
    CHECK (
      (unavailable_reason=''
       AND typeof(start_price_sol) IN ('integer','real')
       AND start_price_sol>0.0 AND start_price_sol<=1e100
       AND typeof(price_observed_at) IN ('integer','real')
       AND price_observed_at BETWEEN 0.0 AND observed_at
       AND price_source='curve_snapshot')
      OR
      (unavailable_reason IN ('start_price_missing','start_price_stale','start_price_malformed')
       AND start_price_sol IS NULL AND price_observed_at IS NULL AND price_source='')
    )
);""",
    ),
    (
        "canonical_generations",
        """CREATE TABLE IF NOT EXISTS canonical_generations (
    generation_hash TEXT PRIMARY KEY CHECK (length(generation_hash)=64),
    first_decision_id INTEGER NOT NULL UNIQUE REFERENCES decisions(id),
    created_at REAL NOT NULL CHECK (
      typeof(created_at) IN ('integer','real')
      AND created_at BETWEEN 0.0 AND 4102444800.0)
);""",
    ),
    (
        "canonical_rechecks",
        """CREATE TABLE IF NOT EXISTS canonical_rechecks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER NOT NULL REFERENCES decisions(id),
    attempt INTEGER NOT NULL CHECK (typeof(attempt)='integer' AND attempt>=1),
    rechecked_at REAL NOT NULL CHECK (
      typeof(rechecked_at) IN ('integer','real')
      AND rechecked_at BETWEEN 0.0 AND 4102444800.0),
    causal_target_report_id INTEGER NOT NULL REFERENCES safety_reports(id),
    latest_target_report_id INTEGER REFERENCES safety_reports(id),
    status TEXT NOT NULL CHECK (status IN ('PASS','CANCEL')),
    reason TEXT NOT NULL CHECK (length(trim(reason))>0),
    canonical_mint TEXT,
    prior_inputs_hash TEXT NOT NULL CHECK (length(prior_inputs_hash)=64),
    recheck_inputs_hash TEXT NOT NULL CHECK (length(recheck_inputs_hash)=64),
    payload_json TEXT NOT NULL CHECK (
      length(payload_json)>1 AND json_valid(payload_json)
      AND json_type(payload_json)='object'),
    UNIQUE(decision_id,attempt),
    CHECK (decision_id=json_extract(payload_json,'$.decision_id')),
    CHECK (attempt=json_extract(payload_json,'$.attempt')),
    CHECK (rechecked_at=json_extract(payload_json,'$.rechecked_at')),
    CHECK (causal_target_report_id=json_extract(payload_json,'$.causal_target_report_id')),
    CHECK (latest_target_report_id IS json_extract(payload_json,'$.latest_target_report_id')),
    CHECK (prior_inputs_hash=json_extract(payload_json,'$.prior_inputs_hash')),
    CHECK (reason=json_extract(payload_json,'$.verdict.reason')),
    CHECK (canonical_mint IS json_extract(payload_json,'$.verdict.canonical_mint')),
    CHECK (
      (status='PASS'
       AND latest_target_report_id=causal_target_report_id
       AND typeof(canonical_mint)='text' AND length(trim(canonical_mint)) BETWEEN 1 AND 128
       AND json_extract(payload_json,'$.verdict.status')='CANONICAL')
      OR
      (status='CANCEL'
       AND json_extract(payload_json,'$.verdict.status') IN ('SUPPRESSED','UNRESOLVED'))
    )
);""",
    ),
    (
        "paper_entry_executions",
        """CREATE TABLE IF NOT EXISTS paper_entry_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER NOT NULL UNIQUE REFERENCES decisions(id),
    at REAL NOT NULL CHECK (
      typeof(at) IN ('integer','real') AND at BETWEEN 0.0 AND 4102444800.0),
    status TEXT NOT NULL CHECK (status IN ('FILLED','CANCELLED','ABANDONED')),
    reason TEXT NOT NULL CHECK (length(trim(reason))>0),
    planned_size_sol REAL NOT NULL CHECK (
      typeof(planned_size_sol) IN ('integer','real')
      AND planned_size_sol>0.0 AND planned_size_sol<=1e100),
    canonical_recheck_id INTEGER REFERENCES canonical_rechecks(id),
    paper_trade_id INTEGER UNIQUE REFERENCES paper_trades(id),
    CHECK (
      (status='FILLED' AND reason='filled'
       AND canonical_recheck_id IS NOT NULL AND paper_trade_id IS NOT NULL)
      OR
      (status='CANCELLED' AND canonical_recheck_id IS NOT NULL
       AND paper_trade_id IS NULL)
      OR
      (status='ABANDONED' AND paper_trade_id IS NULL)
    )
);""",
    ),
    (
        "p3_position_current",
        """CREATE TABLE IF NOT EXISTS p3_position_current (
    decision_id INTEGER PRIMARY KEY REFERENCES decisions(id),
    mint TEXT NOT NULL REFERENCES tokens(mint),
    entry_execution_id INTEGER NOT NULL UNIQUE REFERENCES paper_entry_executions(id),
    bought_qty REAL NOT NULL CHECK (
      typeof(bought_qty) IN ('integer','real') AND bought_qty>0.0 AND bought_qty<=1e100),
    sold_qty REAL NOT NULL DEFAULT 0 CHECK (
      typeof(sold_qty) IN ('integer','real') AND sold_qty>=0.0 AND sold_qty<=bought_qty),
    buy_notional_sol REAL NOT NULL CHECK (
      typeof(buy_notional_sol) IN ('integer','real') AND buy_notional_sol>=0.0 AND buy_notional_sol<=1e100),
    sell_proceeds_sol REAL NOT NULL DEFAULT 0 CHECK (
      typeof(sell_proceeds_sol) IN ('integer','real') AND sell_proceeds_sol BETWEEN -1e100 AND 1e100),
    ladder_mask INTEGER NOT NULL DEFAULT 0 CHECK (
      typeof(ladder_mask)='integer' AND ladder_mask BETWEEN 0 AND 4611686018427387903),
    last_trade_at REAL NOT NULL CHECK (
      typeof(last_trade_at) IN ('integer','real') AND last_trade_at BETWEEN 0.0 AND 4102444800.0)
);""",
    ),
    (
        "canonical_pending_current",
        """CREATE TABLE IF NOT EXISTS canonical_pending_current (
    observation_id INTEGER PRIMARY KEY REFERENCES canonical_observations(id),
    decision_id INTEGER NOT NULL REFERENCES decisions(id),
    horizons_json TEXT NOT NULL CHECK (
      json_valid(horizons_json)=1 AND json_type(horizons_json)='array'
      AND json_array_length(horizons_json) BETWEEN 1 AND 32),
    full_mask INTEGER NOT NULL CHECK (
      typeof(full_mask)='integer' AND full_mask BETWEEN 1 AND 4294967295
      AND full_mask=(1 << json_array_length(horizons_json))-1),
    completed_mask INTEGER NOT NULL DEFAULT 0 CHECK (
      typeof(completed_mask)='integer' AND completed_mask BETWEEN 0 AND full_mask)
);""",
    ),
)

V5_PERFORMANCE_INDEX_DDL = (
    (
        "safety_reports_pending_scoring_idx",
        "CREATE INDEX IF NOT EXISTS safety_reports_pending_scoring_idx\n"
        "  ON safety_reports(id DESC)\n"
        "  WHERE json_array_length(hard_fails_json)=0;",
    ),
    (
        "decisions_climbing_mint_idx",
        "CREATE INDEX IF NOT EXISTS decisions_climbing_mint_idx\n"
        "  ON decisions(mint) WHERE segment='CLIMBING';",
    ),
    (
        "decisions_p3_canonical_buy_idx",
        "CREATE INDEX IF NOT EXISTS decisions_p3_canonical_buy_idx\n"
        "  ON decisions(id)\n"
        "  WHERE action='BUY' AND CASE WHEN json_valid(feature_vector_json)\n"
        "  THEN json_extract(feature_vector_json,'$.canonical.status') END='CANONICAL';",
    ),
)

V5_PERFORMANCE_INDEX_CONTRACT = {
    "safety_reports_pending_scoring_idx": (
        "safety_reports", (("id", 1),), True,
    ),
    "decisions_climbing_mint_idx": (
        "decisions", (("mint", 0),), True,
    ),
    "decisions_p3_canonical_buy_idx": (
        "decisions", (("id", 0),), True,
    ),
    "p3_position_current_open_idx": (
        "p3_position_current", (("decision_id", 0),), True,
    ),
}

V5_INDEX_DDL = (
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

V5_EXPLICIT_TRIGGER_DDL = (
    (
        "early_buyer_report_guard",
        """CREATE TRIGGER IF NOT EXISTS early_buyer_report_guard
BEFORE INSERT ON early_buyer_reads
WHEN NEW.safety_report_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM safety_reports sr
  WHERE sr.id=NEW.safety_report_id AND sr.mint=NEW.mint
    AND NEW.checked_at<=sr.checked_at
)
BEGIN
  SELECT RAISE(ABORT,'invalid early-buyer report link');
END;""",
    ),
    (
        "creator_reputation_creator_stable",
        """CREATE TRIGGER IF NOT EXISTS creator_reputation_creator_stable
BEFORE INSERT ON creator_reputation_events
WHEN EXISTS (
  SELECT 1 FROM creator_reputation_events old
  WHERE old.mint=NEW.mint AND old.creator<>NEW.creator
)
BEGIN
  SELECT RAISE(ABORT,'creator reputation creator mismatch');
END;""",
    ),
    (
        "creator_reputation_no_graduation_after_rug",
        """CREATE TRIGGER IF NOT EXISTS creator_reputation_no_graduation_after_rug
BEFORE INSERT ON creator_reputation_events
WHEN NEW.outcome='GRADUATED' AND EXISTS (
  SELECT 1 FROM creator_reputation_events old
  WHERE old.mint=NEW.mint AND old.outcome='RUGGED'
)
BEGIN
  SELECT RAISE(ABORT,'RUGGED reputation is terminal');
END;""",
    ),
    (
        "creator_reputation_rug_not_retrograde",
        """CREATE TRIGGER IF NOT EXISTS creator_reputation_rug_not_retrograde
BEFORE INSERT ON creator_reputation_events
WHEN NEW.outcome='RUGGED' AND EXISTS (
  SELECT 1 FROM creator_reputation_events old
  WHERE old.mint=NEW.mint AND old.observed_at>=NEW.observed_at
)
BEGIN
  SELECT RAISE(ABORT,'RUGGED reputation time does not follow prior evidence');
END;""",
    ),
    (
        "creator_reputation_current_after_insert",
        """CREATE TRIGGER IF NOT EXISTS creator_reputation_current_after_insert
AFTER INSERT ON creator_reputation_events
BEGIN
  INSERT INTO creator_reputation_current(mint,creator,outcome,observed_at,event_id)
  VALUES(NEW.mint,NEW.creator,NEW.outcome,NEW.observed_at,NEW.id)
  ON CONFLICT(mint) DO UPDATE SET
    creator=excluded.creator,
    outcome=excluded.outcome,
    observed_at=excluded.observed_at,
    event_id=excluded.event_id;
END;""",
    ),
    (
        "p3_safety_report_shape_guard",
        """CREATE TRIGGER IF NOT EXISTS p3_safety_report_shape_guard
BEFORE INSERT ON safety_reports
WHEN NEW.id<>-1
 OR typeof(NEW.mint)<>'text' OR length(trim(NEW.mint)) NOT BETWEEN 1 AND 128
 OR typeof(NEW.checked_at) NOT IN ('integer','real')
 OR NEW.checked_at NOT BETWEEN 0.0 AND 4102444800.0
 OR length(NEW.hard_fails_json)>8192
 OR NOT json_valid(NEW.hard_fails_json) OR json_type(NEW.hard_fails_json)<>'array'
 OR EXISTS (SELECT 1 FROM json_each(NEW.hard_fails_json)
            WHERE type<>'text' OR length(trim(value))=0)
 OR typeof(NEW.risk_score) NOT IN ('integer','real')
 OR NEW.risk_score NOT BETWEEN 0.0 AND 100.0
 OR typeof(NEW.inputs_hash)<>'text' OR length(NEW.inputs_hash)<>64
 OR NEW.inputs_hash<>lower(NEW.inputs_hash) OR NEW.inputs_hash GLOB '*[^0-9a-f]*'
BEGIN
  SELECT RAISE(ABORT,'invalid safety report shape');
END;""",
    ),
    (
        "p3_wallet_pnl_shape_guard",
        """CREATE TRIGGER IF NOT EXISTS p3_wallet_pnl_shape_guard
BEFORE INSERT ON wallet_pnl_events
WHEN typeof(NEW.at) NOT IN ('integer','real') OR NEW.at NOT BETWEEN 0.0 AND 4102444800.0
 OR typeof(NEW.wallet)<>'text' OR length(trim(NEW.wallet)) NOT BETWEEN 1 AND 128
 OR typeof(NEW.mint)<>'text' OR length(trim(NEW.mint)) NOT BETWEEN 1 AND 128
 OR typeof(NEW.realized_pnl_sol) NOT IN ('integer','real')
 OR NEW.realized_pnl_sol NOT BETWEEN -1000000000000.0 AND 1000000000000.0
 OR typeof(NEW.source)<>'text' OR length(trim(NEW.source)) NOT BETWEEN 1 AND 64
 OR length(NEW.detail_json)>65536
 OR NOT json_valid(NEW.detail_json) OR json_type(NEW.detail_json)<>'object'
BEGIN
  SELECT RAISE(ABORT,'invalid wallet PnL shape');
END;""",
    ),
    (
        "p3_wallet_pnl_summary_insert",
        """CREATE TRIGGER IF NOT EXISTS p3_wallet_pnl_summary_insert
AFTER INSERT ON wallet_pnl_events
BEGIN
  INSERT INTO wallet_pnl_summary(wallet,event_count,realized_pnl_sol,last_at,last_event_id)
  VALUES(NEW.wallet,1,NEW.realized_pnl_sol,NEW.at,NEW.id)
  ON CONFLICT(wallet) DO UPDATE SET
    event_count=event_count+1,
    realized_pnl_sol=realized_pnl_sol+NEW.realized_pnl_sol,
    last_at=max(last_at,NEW.at),
    last_event_id=NEW.id;
END;""",
    ),
    (
        "p3_early_buyer_shape_guard",
        """CREATE TRIGGER IF NOT EXISTS p3_early_buyer_shape_guard
BEFORE INSERT ON early_buyer_reads
WHEN typeof(NEW.mint)<>'text' OR length(trim(NEW.mint)) NOT BETWEEN 1 AND 128
 OR typeof(NEW.checked_at) NOT IN ('integer','real')
 OR NEW.checked_at NOT BETWEEN 0.0 AND 4102444800.0
 OR length(NEW.buyers_json)>8192
 OR NOT json_valid(NEW.buyers_json) OR json_type(NEW.buyers_json)<>'array'
 OR EXISTS (SELECT 1 FROM json_each(NEW.buyers_json)
            WHERE type<>'text' OR length(trim(value))=0)
 OR NEW.unavailable_reason NOT IN (
   '', 'rpc_error', 'no_signatures', 'no_matching_buy_events',
   'missing_bonding_curve_key', 'owner_resolution_incomplete',
   'reader_unavailable', 'early_buyer_check_not_run',
   'early_buyer_evidence_malformed')
 OR (NEW.unavailable_reason='' AND json_array_length(NEW.buyers_json)=0)
 OR (NEW.unavailable_reason<>'' AND json_array_length(NEW.buyers_json)<>0)
 OR typeof(NEW.inputs_hash)<>'text' OR length(NEW.inputs_hash)<>64
 OR NEW.inputs_hash<>lower(NEW.inputs_hash) OR NEW.inputs_hash GLOB '*[^0-9a-f]*'
BEGIN
  SELECT RAISE(ABORT,'invalid early-buyer shape');
END;""",
    ),
    (
        "p3_paper_trade_side_domain",
        """CREATE TRIGGER IF NOT EXISTS p3_paper_trade_side_domain
BEFORE INSERT ON paper_trades
WHEN NEW.side NOT IN ('buy','sell')
BEGIN
  SELECT RAISE(ABORT,'paper trade side must be exact lowercase buy/sell');
END;""",
    ),
    (
        "p3_trade_shape_guard",
        """CREATE TRIGGER IF NOT EXISTS p3_trade_shape_guard
BEFORE INSERT ON paper_trades
WHEN (NEW.canonical_recheck_id IS NOT NULL OR NEW.canonical_proof_hash IS NOT NULL
      OR NEW.p3_entry_execution_id IS NOT NULL)
 AND (
   (NEW.canonical_recheck_id IS NULL)<>(NEW.canonical_proof_hash IS NULL)
   OR NOT (typeof(NEW.mint)='text' AND length(trim(NEW.mint)) BETWEEN 1 AND 128)
   OR NOT (typeof(NEW.segment)='text' AND length(trim(NEW.segment)) BETWEEN 1 AND 64)
   OR NOT (typeof(NEW.at) IN ('integer','real') AND NEW.at BETWEEN 0.0 AND 4102444800.0)
   OR NOT (typeof(NEW.qty) IN ('integer','real') AND NEW.qty>0.0 AND NEW.qty<=1e100)
   OR NOT (typeof(NEW.quote_price) IN ('integer','real')
           AND NEW.quote_price BETWEEN 0.0 AND 1e100)
   OR NOT (typeof(NEW.fill_price) IN ('integer','real')
           AND NEW.fill_price BETWEEN 0.0 AND 1e100)
   OR p3_fee_sum(NEW.fees_json) NOT BETWEEN 0.0 AND 1e100
   OR NOT (typeof(NEW.realism_grade)='text'
           AND length(NEW.realism_grade) BETWEEN 1 AND 32)
   OR (NEW.canonical_recheck_id IS NOT NULL AND (NEW.quote_price<=0.0 OR NEW.fill_price<=0.0))
 )
BEGIN
  SELECT RAISE(ABORT,'invalid P3 trade shape');
END;""",
    ),
    (
        "p3_recheck_requires_valid_decision_link",
        """CREATE TRIGGER IF NOT EXISTS p3_recheck_requires_valid_decision_link
BEFORE INSERT ON canonical_rechecks
WHEN NOT EXISTS (
  SELECT 1
  FROM decisions d
  JOIN safety_reports sr ON sr.id=NEW.causal_target_report_id
  WHERE d.id=NEW.decision_id
    AND d.action='BUY'
    AND json_extract(d.feature_vector_json,'$.canonical.status')='CANONICAL'
    AND d.safety_report_id=NEW.causal_target_report_id
    AND sr.mint=d.mint
    AND NEW.rechecked_at>d.at
    AND (NEW.status<>'PASS' OR NEW.canonical_mint=d.mint)
    AND NEW.prior_inputs_hash=json_extract(d.feature_vector_json,'$.canonical.inputs_hash')
    AND NEW.latest_target_report_id IS (
      SELECT latest.id FROM safety_reports latest
      WHERE latest.mint=d.mint
      ORDER BY latest.id DESC LIMIT 1
    )
    AND EXISTS (
      SELECT 1 FROM safety_reports latest
      WHERE latest.id=NEW.latest_target_report_id
        AND latest.checked_at<NEW.rechecked_at
    )
)
BEGIN
  SELECT RAISE(ABORT,'P3 recheck requires matching canonical decision proof');
END;""",
    ),
    (
        "p3_buy_requires_canonical_proof",
        """CREATE TRIGGER IF NOT EXISTS p3_buy_requires_canonical_proof
BEFORE INSERT ON paper_trades
WHEN NEW.side='buy'
 AND (NEW.canonical_recheck_id IS NOT NULL
      OR NEW.canonical_proof_hash IS NOT NULL
      OR NEW.p3_entry_execution_id IS NOT NULL
      OR EXISTS (
        SELECT 1 FROM canonical_observations o
        WHERE o.decision_id=NEW.decision_id
      ))
 AND NOT EXISTS (
   SELECT 1
   FROM canonical_rechecks cr
   JOIN decisions d ON d.id=cr.decision_id
   WHERE cr.id=NEW.canonical_recheck_id
   AND cr.status='PASS'
   AND cr.decision_id=NEW.decision_id
   AND cr.id=(
     SELECT latest.id FROM canonical_rechecks latest
     WHERE latest.decision_id=NEW.decision_id
     ORDER BY latest.attempt DESC,latest.id DESC LIMIT 1
   )
   AND NEW.p3_entry_execution_id IS NULL
   AND EXISTS (
     SELECT 1 FROM canonical_observations o
     WHERE o.decision_id=d.id AND o.mint=d.mint
       AND o.is_subject=1 AND o.is_canonical=1 AND o.eligible=1
   )
   AND NEW.mint=d.mint
     AND cr.canonical_mint=NEW.mint
     AND cr.causal_target_report_id=d.safety_report_id
     AND cr.latest_target_report_id=cr.causal_target_report_id
     AND cr.prior_inputs_hash=json_extract(
           d.feature_vector_json,'$.canonical.inputs_hash')
     AND cr.recheck_inputs_hash=NEW.canonical_proof_hash
     AND NEW.at>cr.rechecked_at
 )
BEGIN
  SELECT RAISE(ABORT,'P3 BUY requires matching canonical PASS proof');
END;""",
    ),
    (
        "p3_execution_requires_valid_terminal_link",
        """CREATE TRIGGER IF NOT EXISTS p3_execution_requires_valid_terminal_link
BEFORE INSERT ON paper_entry_executions
WHEN COALESCE(
  EXISTS (
    SELECT 1
    FROM decisions d
    JOIN canonical_observations o ON o.decision_id=d.id
    WHERE d.id=NEW.decision_id
      AND d.action='BUY'
      AND json_extract(d.feature_vector_json,'$.canonical.status')='CANONICAL'
      AND NEW.planned_size_sol IS json_extract(
        d.feature_vector_json,'$.canonical.planned_size_sol'
      )
      AND NEW.at>d.at
  )
  AND (
    (NEW.status='FILLED' AND EXISTS (
      SELECT 1
      FROM paper_trades pt
      JOIN canonical_rechecks cr ON cr.id=NEW.canonical_recheck_id
      WHERE pt.id=NEW.paper_trade_id
        AND pt.decision_id=NEW.decision_id
        AND pt.mint=(SELECT d2.mint FROM decisions d2 WHERE d2.id=NEW.decision_id)
        AND pt.canonical_recheck_id=cr.id
        AND pt.canonical_proof_hash=cr.recheck_inputs_hash
        AND cr.decision_id=NEW.decision_id AND cr.status='PASS'
        AND cr.id=(
          SELECT latest.id FROM canonical_rechecks latest
          WHERE latest.decision_id=NEW.decision_id
          ORDER BY latest.attempt DESC,latest.id DESC LIMIT 1
        )
        AND cr.canonical_mint=(SELECT d2.mint FROM decisions d2 WHERE d2.id=NEW.decision_id)
        AND NEW.at=pt.at AND NEW.at>cr.rechecked_at
        AND abs(NEW.planned_size_sol-pt.qty*pt.fill_price)
            <=max(1e-12,1e-12*max(abs(NEW.planned_size_sol),abs(pt.qty*pt.fill_price)))
    ))
    OR
    (NEW.status='CANCELLED' AND EXISTS (
      SELECT 1 FROM canonical_rechecks cr
      WHERE cr.id=NEW.canonical_recheck_id
        AND cr.decision_id=NEW.decision_id AND cr.status='CANCEL'
        AND cr.id=(
          SELECT latest.id FROM canonical_rechecks latest
          WHERE latest.decision_id=NEW.decision_id
          ORDER BY latest.attempt DESC,latest.id DESC LIMIT 1
        )
        AND NEW.reason=cr.reason
        AND NEW.at>cr.rechecked_at
    ))
    OR
    (NEW.status='ABANDONED'
      AND NOT EXISTS (
        SELECT 1 FROM canonical_rechecks cancel
        WHERE cancel.decision_id=NEW.decision_id AND cancel.status='CANCEL'
      )
      AND (
        (NEW.canonical_recheck_id IS NULL
         AND NEW.reason='restart_before_fill'
         AND NOT EXISTS (
           SELECT 1 FROM canonical_rechecks prior
           WHERE prior.decision_id=NEW.decision_id
         ))
        OR EXISTS (
          SELECT 1 FROM canonical_rechecks pass
          WHERE pass.id=NEW.canonical_recheck_id
            AND pass.decision_id=NEW.decision_id AND pass.status='PASS'
            AND pass.id=(
              SELECT latest.id FROM canonical_rechecks latest
              WHERE latest.decision_id=NEW.decision_id
              ORDER BY latest.attempt DESC,latest.id DESC LIMIT 1
            )
            AND NEW.reason='restart_after_pass'
            AND NEW.at>pass.rechecked_at
        )
      )
    )
  ), 0
)=0
BEGIN
  SELECT RAISE(ABORT,'invalid P3 terminal execution link');
END;""",
    ),
    (
        "p3_position_after_filled_entry",
        """CREATE TRIGGER IF NOT EXISTS p3_position_after_filled_entry
AFTER INSERT ON paper_entry_executions
WHEN NEW.status='FILLED'
BEGIN
  INSERT INTO p3_position_current(
    decision_id,mint,entry_execution_id,bought_qty,sold_qty,
    buy_notional_sol,sell_proceeds_sol,ladder_mask,last_trade_at)
  SELECT NEW.decision_id,d.mint,NEW.id,pt.qty,0.0,
         pt.qty*pt.fill_price+p3_fee_sum(pt.fees_json),
         0.0,0,NEW.at
  FROM decisions d JOIN paper_trades pt ON pt.id=NEW.paper_trade_id
  WHERE d.id=NEW.decision_id;
END;""",
    ),
    (
        "p3_sell_requires_filled_entry",
        """CREATE TRIGGER IF NOT EXISTS p3_sell_requires_filled_entry
BEFORE INSERT ON paper_trades
WHEN NEW.side='sell'
 AND (NEW.p3_entry_execution_id IS NOT NULL
      OR NEW.canonical_recheck_id IS NOT NULL
      OR NEW.canonical_proof_hash IS NOT NULL
      OR EXISTS (SELECT 1 FROM canonical_observations o
                 WHERE o.decision_id=NEW.decision_id))
 AND NOT EXISTS (
   SELECT 1 FROM paper_entry_executions e
   JOIN decisions d ON d.id=e.decision_id
   JOIN p3_position_current pc ON pc.entry_execution_id=e.id
   WHERE e.id=NEW.p3_entry_execution_id AND e.decision_id=NEW.decision_id
     AND NEW.canonical_recheck_id IS NULL AND NEW.canonical_proof_hash IS NULL
     AND e.status='FILLED' AND d.mint=NEW.mint AND pc.mint=NEW.mint
     AND EXISTS (
       SELECT 1 FROM canonical_observations o
       WHERE o.decision_id=d.id AND o.mint=d.mint
         AND o.is_subject=1 AND o.is_canonical=1 AND o.eligible=1
     )
     AND NEW.at>e.at AND NEW.at>pc.last_trade_at
     AND typeof(NEW.qty) IN ('integer','real') AND NEW.qty>0.0
     AND NEW.qty<=pc.bought_qty-pc.sold_qty
 )
BEGIN
  SELECT RAISE(ABORT,'P3 SELL requires matching open FILLED entry');
END;""",
    ),
    (
        "p3_position_after_sell",
        """CREATE TRIGGER IF NOT EXISTS p3_position_after_sell
AFTER INSERT ON paper_trades
WHEN NEW.side='sell'
 AND NEW.p3_entry_execution_id IS NOT NULL
BEGIN
  UPDATE p3_position_current
  SET sold_qty=sold_qty+NEW.qty,
      sell_proceeds_sol=sell_proceeds_sol
        +NEW.qty*NEW.fill_price-p3_fee_sum(NEW.fees_json),
      last_trade_at=NEW.at
  WHERE decision_id=NEW.decision_id;
END;""",
    ),
    (
        "p3_canonical_pending_after_observation",
        """CREATE TRIGGER IF NOT EXISTS p3_canonical_pending_after_observation
AFTER INSERT ON canonical_observations
WHEN NEW.eligible=1 AND NEW.unavailable_reason=''
BEGIN
  INSERT INTO canonical_pending_current(
    observation_id,decision_id,horizons_json,full_mask,completed_mask)
  SELECT NEW.id,NEW.decision_id,
         json_extract(d.feature_vector_json,
                      '$.canonical.ranking_inputs.counterfactual_horizons_s'),
         (1 << json_array_length(json_extract(
           d.feature_vector_json,
           '$.canonical.ranking_inputs.counterfactual_horizons_s')))-1,
         0
  FROM decisions d
  WHERE d.id=NEW.decision_id;
END;""",
    ),
    (
        "p3_canonical_pending_after_outcome",
        """CREATE TRIGGER IF NOT EXISTS p3_canonical_pending_after_outcome
AFTER INSERT ON outcomes
WHEN NEW.ref_kind='canonical_observation'
BEGIN
  UPDATE canonical_pending_current
  SET completed_mask=completed_mask | (1 << (
    SELECT CAST(h.key AS INTEGER)
    FROM json_each(canonical_pending_current.horizons_json) h
    WHERE h.type IN ('integer','real')
      AND h.value=json_extract(NEW.detail_json,'$.horizon_s')
  ))
  WHERE observation_id=NEW.ref_id;
END;""",
    ),
    (
        "p3_outcome_shape_guard",
        """CREATE TRIGGER IF NOT EXISTS p3_outcome_shape_guard
BEFORE INSERT ON outcomes
WHEN (
  NEW.p3_exit_trade_id IS NOT NULL
  OR (NEW.ref_kind='trade' AND EXISTS (
    SELECT 1 FROM paper_trades pt
    JOIN canonical_observations o ON o.decision_id=pt.decision_id
    WHERE pt.id=NEW.ref_id))
  OR NEW.ref_kind='canonical_observation'
) AND (
  NOT (typeof(NEW.at) IN ('integer','real') AND NEW.at BETWEEN 0.0 AND 4102444800.0)
  OR NOT (typeof(NEW.pnl_sol) IN ('integer','real') AND NEW.pnl_sol BETWEEN -1e100 AND 1e100)
  OR (NEW.ref_kind='canonical_observation' AND NEW.pnl_sol<>0.0)
  OR (NEW.ref_kind='canonical_observation' AND NOT (
       json_type(NEW.detail_json,'$.horizon_s') IN ('integer','real')
       AND json_extract(NEW.detail_json,'$.horizon_s')>0.0))
  OR NOT (typeof(NEW.detail_json)='text' AND length(NEW.detail_json)<=8192
          AND json_valid(NEW.detail_json)=1 AND json_type(NEW.detail_json)='object')
)
BEGIN
  SELECT RAISE(ABORT,'invalid P3 outcome shape');
END;""",
    ),
    (
        "p3_outcome_requires_exit_or_observation_chronology",
        """CREATE TRIGGER IF NOT EXISTS p3_outcome_requires_exit_or_observation_chronology
BEFORE INSERT ON outcomes
WHEN (
  (NEW.p3_exit_trade_id IS NOT NULL OR (
    NEW.ref_kind='trade'
    AND EXISTS (
      SELECT 1 FROM paper_trades pt
      JOIN canonical_observations o ON o.decision_id=pt.decision_id
      WHERE pt.id=NEW.ref_id
    )
  ))
  AND NOT EXISTS (
    SELECT 1 FROM paper_trades pt
    JOIN paper_entry_executions e ON e.id=pt.p3_entry_execution_id
    JOIN p3_position_current pc ON pc.decision_id=pt.decision_id
    WHERE NEW.ref_kind='trade'
      AND pt.id=NEW.p3_exit_trade_id AND pt.id=NEW.ref_id
      AND pt.side='sell' AND NEW.at=pt.at
      AND pc.sold_qty=pc.bought_qty AND pc.last_trade_at=NEW.at
      AND NEW.pnl_sol=pc.sell_proceeds_sol-pc.buy_notional_sol
      AND (SELECT count(*) FROM json_each(NEW.detail_json))=3
      AND json_type(NEW.detail_json,'$.reason')='text'
      AND json_extract(NEW.detail_json,'$.reason') IN (
        'time_stop','trailing_stop','graduated','dead','graduated_no_price',
        'safety_flip','stale','restart_safety_hard_fail')
      AND json_type(NEW.detail_json,'$.hold_s') IN ('integer','real')
      AND json_extract(NEW.detail_json,'$.hold_s')=NEW.at-e.at
      AND json_type(NEW.detail_json,'$.grade')='text'
      AND json_extract(NEW.detail_json,'$.grade')=pt.realism_grade
  )
) OR (
  NEW.ref_kind='canonical_observation'
  AND NOT EXISTS (
    SELECT 1 FROM canonical_observations o
    JOIN decisions d ON d.id=o.decision_id
    JOIN canonical_pending_current cp
      ON cp.observation_id=o.id AND cp.decision_id=d.id
    WHERE o.id=NEW.ref_id AND NEW.at>o.observed_at
      AND json_type(d.feature_vector_json,'$.canonical.ranking_inputs.counterfactual_horizons_s')='array'
      AND EXISTS (
        SELECT 1 FROM json_each(
          d.feature_vector_json,'$.canonical.ranking_inputs.counterfactual_horizons_s') h
        WHERE h.type IN ('integer','real')
          AND h.value=json_extract(NEW.detail_json,'$.horizon_s')
      )
      AND NEW.at>=o.observed_at+json_extract(NEW.detail_json,'$.horizon_s')
      AND (SELECT count(*) FROM json_each(NEW.detail_json))=8
      AND json_type(NEW.detail_json,'$.horizon_s') IN ('integer','real')
      AND json_extract(NEW.detail_json,'$.horizon_s')>0.0
      AND json_type(NEW.detail_json,'$.price0') IN ('integer','real')
      AND json_extract(NEW.detail_json,'$.price0')=o.start_price_sol
      AND json_type(NEW.detail_json,'$.price0_observed_at') IN ('integer','real')
      AND json_extract(NEW.detail_json,'$.price0_observed_at')=o.price_observed_at
      AND json_type(NEW.detail_json,'$.terminal') IN ('null','text')
      AND (json_extract(NEW.detail_json,'$.terminal') IS NULL
           OR json_extract(NEW.detail_json,'$.terminal') IN ('DEAD','STALE','GRADUATED'))
      AND json_type(NEW.detail_json,'$.unavailable_reason')='text'
      AND (
        (json_extract(NEW.detail_json,'$.unavailable_reason') IN (
           'journal_replay_gap','graduated_no_price')
         AND json_type(NEW.detail_json,'$.forward_return_pct')='null'
         AND json_type(NEW.detail_json,'$.price_now')='null'
         AND json_type(NEW.detail_json,'$.price_now_observed_at')='null'
         AND ((json_extract(NEW.detail_json,'$.unavailable_reason')='journal_replay_gap'
               AND json_type(NEW.detail_json,'$.terminal')='null')
              OR (json_extract(NEW.detail_json,'$.unavailable_reason')='graduated_no_price'
                  AND json_extract(NEW.detail_json,'$.terminal')='GRADUATED')))
        OR
        (json_extract(NEW.detail_json,'$.unavailable_reason')=''
         AND json_type(NEW.detail_json,'$.forward_return_pct') IN ('integer','real')
         AND json_extract(NEW.detail_json,'$.forward_return_pct') BETWEEN -1e100 AND 1e100
         AND json_type(NEW.detail_json,'$.price_now') IN ('integer','real')
         AND json_extract(NEW.detail_json,'$.price_now') BETWEEN 0.0 AND 1e100
         AND json_type(NEW.detail_json,'$.price_now_observed_at') IN ('integer','real')
         AND json_extract(NEW.detail_json,'$.price_now_observed_at')>=o.observed_at
         AND json_extract(NEW.detail_json,'$.price_now_observed_at')<=
             o.observed_at+json_extract(NEW.detail_json,'$.horizon_s')
         AND (
           (json_extract(NEW.detail_json,'$.terminal') IN ('DEAD','STALE')
            AND json_extract(NEW.detail_json,'$.price_now')=0.0
            AND json_extract(NEW.detail_json,'$.forward_return_pct')=-100.0)
           OR
           ((json_type(NEW.detail_json,'$.terminal')='null'
             OR json_extract(NEW.detail_json,'$.terminal')='GRADUATED')
            AND json_extract(NEW.detail_json,'$.price_now')>0.0
            AND abs(json_extract(NEW.detail_json,'$.forward_return_pct')-
                    100.0*(json_extract(NEW.detail_json,'$.price_now')-
                           json_extract(NEW.detail_json,'$.price0'))/
                          json_extract(NEW.detail_json,'$.price0'))
                <=max(1e-12,1e-12*max(
                    abs(json_extract(NEW.detail_json,'$.forward_return_pct')),
                    abs(100.0*(json_extract(NEW.detail_json,'$.price_now')-
                               json_extract(NEW.detail_json,'$.price0'))/
                              json_extract(NEW.detail_json,'$.price0'))))
           )
         )
        )
      )
  )
)
BEGIN
  SELECT RAISE(ABORT,'P3 outcome requires causal exit/observation proof');
END;""",
    ),
)

SCHEMA_V5_NEW_IMMUTABLE_KEYS = {
  "safety_reports": (("id",),),
  "holder_evidence": (("id",), ("safety_report_id",)),
  "creator_reputation_events": (("id",), ("mint","outcome")),
  "canonical_observations": (("id",), ("decision_id","mint")),
  "canonical_generations": (("generation_hash",), ("first_decision_id",)),
  "canonical_rechecks": (("id",), ("decision_id","attempt")),
  "paper_entry_executions": (("id",), ("decision_id",), ("paper_trade_id",)),
}
SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE = {
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
SCHEMA_V5_NEW_IMMUTABLE_TABLES = tuple(SCHEMA_V5_NEW_IMMUTABLE_KEYS)
SCHEMA_V5_IMMUTABLE_TABLES = (
    *SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE,
    *SCHEMA_V5_NEW_IMMUTABLE_TABLES,
)


def _immutable_insert_trigger(table: str, duplicate: str) -> str:
    return (
      f"CREATE TRIGGER IF NOT EXISTS {table}_no_replace BEFORE INSERT ON {table} "  # nosec B608
      f"WHEN EXISTS(SELECT 1 FROM {table} AS old WHERE {duplicate}) "
      "BEGIN SELECT RAISE(ABORT,'immutable evidence'); END"
    )


def _v5_immutable_triggers() -> tuple[str,...]:
    sql = [
      _immutable_insert_trigger(table, duplicate)
      for table, duplicate in SCHEMA_V5_EXISTING_IMMUTABLE_INSERT_WHERE.items()
    ]
    for table, keys in SCHEMA_V5_NEW_IMMUTABLE_KEYS.items():
        alternatives = []
        for key in keys:
            present = " AND ".join(f"NEW.{column} IS NOT NULL" for column in key)
            equal = " AND ".join(f"old.{column} IS NEW.{column}" for column in key)
            alternatives.append(f"(({present}) AND ({equal}))")
        duplicate = " OR ".join(alternatives)
        sql.extend((
            _immutable_insert_trigger(table, duplicate),
            f"CREATE TRIGGER IF NOT EXISTS {table}_append_only_update BEFORE UPDATE ON {table} "
            "BEGIN SELECT RAISE(ABORT,'immutable evidence'); END",
            f"CREATE TRIGGER IF NOT EXISTS {table}_append_only_delete BEFORE DELETE ON {table} "
            "BEGIN SELECT RAISE(ABORT,'immutable evidence'); END",
        ))
    return tuple(sql)


def _validate_v5_legacy_safety_reports(conn: sqlite3.Connection) -> None:
    """Reject legacy safety rows that the v5 INSERT shape guard would reject.

    Existing positive IDs are migration history, not new inserts. Holder evidence did
    not exist in v4, so its absence is intentionally not part of this validator.
    """
    malformed = conn.execute(
        """SELECT id,mint FROM safety_reports
WHERE typeof(mint)<>'text' OR length(trim(mint)) NOT BETWEEN 1 AND 128
 OR typeof(checked_at) NOT IN ('integer','real')
 OR checked_at NOT BETWEEN 0.0 AND 4102444800.0
 OR length(hard_fails_json)>8192
 OR NOT json_valid(hard_fails_json)
 OR CASE WHEN json_valid(hard_fails_json)
         THEN json_type(hard_fails_json)<>'array' ELSE 0 END
 OR CASE WHEN json_valid(hard_fails_json)
               AND json_type(hard_fails_json)='array'
         THEN EXISTS (SELECT 1 FROM json_each(hard_fails_json)
                      WHERE type<>'text' OR length(trim(value))=0)
         ELSE 0 END
 OR typeof(risk_score) NOT IN ('integer','real')
 OR risk_score NOT BETWEEN 0.0 AND 100.0
 OR typeof(inputs_hash)<>'text' OR length(inputs_hash)<>64
 OR inputs_hash<>lower(inputs_hash) OR inputs_hash GLOB '*[^0-9a-f]*'
ORDER BY id
LIMIT 1"""
    ).fetchone()
    if malformed is not None:
        raise ValueError(
            f"invalid legacy safety_reports row id={malformed[0]} mint={malformed[1]!r}"
        )


def _validate_v5_legacy_wallet_pnl_events(conn: sqlite3.Connection) -> None:
    """Reject legacy wallet-PnL rows that the v5 INSERT shape guard would reject."""
    malformed = conn.execute(
        """SELECT id,wallet,mint FROM wallet_pnl_events
WHERE typeof(at) NOT IN ('integer','real') OR at NOT BETWEEN 0.0 AND 4102444800.0
 OR typeof(wallet)<>'text' OR length(trim(wallet)) NOT BETWEEN 1 AND 128
 OR typeof(mint)<>'text' OR length(trim(mint)) NOT BETWEEN 1 AND 128
 OR typeof(realized_pnl_sol) NOT IN ('integer','real')
 OR realized_pnl_sol NOT BETWEEN -1000000000000.0 AND 1000000000000.0
 OR typeof(source)<>'text' OR length(trim(source)) NOT BETWEEN 1 AND 64
 OR length(detail_json)>65536
 OR NOT json_valid(detail_json)
 OR CASE WHEN json_valid(detail_json)
         THEN json_type(detail_json)<>'object' ELSE 0 END
ORDER BY id
LIMIT 1"""
    ).fetchone()
    if malformed is not None:
        raise ValueError(
            f"invalid legacy wallet_pnl_events row id={malformed[0]}"
            f" wallet={malformed[1]!r} mint={malformed[2]!r}"
        )


def _validate_v5_legacy_early_buyer_reads(conn: sqlite3.Connection) -> None:
    """Reject legacy early-buyer rows that the v5 INSERT shape guard would reject.

    The v4 rows predate both the safety-report link and holder evidence, so neither is
    part of this migration-only validation.
    """
    malformed = conn.execute(
        """SELECT id,mint FROM early_buyer_reads
WHERE typeof(mint)<>'text' OR length(trim(mint)) NOT BETWEEN 1 AND 128
 OR typeof(checked_at) NOT IN ('integer','real')
 OR checked_at NOT BETWEEN 0.0 AND 4102444800.0
 OR length(buyers_json)>8192
 OR NOT json_valid(buyers_json)
 OR CASE WHEN json_valid(buyers_json)
         THEN json_type(buyers_json)<>'array' ELSE 0 END
 OR CASE WHEN json_valid(buyers_json) AND json_type(buyers_json)='array'
         THEN EXISTS (SELECT 1 FROM json_each(buyers_json)
                      WHERE type<>'text' OR length(trim(value))=0)
         ELSE 0 END
 OR unavailable_reason NOT IN (
   '', 'rpc_error', 'no_signatures', 'no_matching_buy_events',
   'missing_bonding_curve_key', 'owner_resolution_incomplete',
   'reader_unavailable', 'early_buyer_check_not_run',
   'early_buyer_evidence_malformed')
 OR CASE WHEN json_valid(buyers_json) AND json_type(buyers_json)='array'
         THEN (unavailable_reason='' AND json_array_length(buyers_json)=0)
           OR (unavailable_reason<>'' AND json_array_length(buyers_json)<>0)
         ELSE 0 END
 OR typeof(inputs_hash)<>'text' OR length(inputs_hash)<>64
 OR inputs_hash<>lower(inputs_hash) OR inputs_hash GLOB '*[^0-9a-f]*'
ORDER BY id
LIMIT 1"""
    ).fetchone()
    if malformed is not None:
        raise ValueError(
            f"invalid legacy early_buyer_reads row id={malformed[0]} mint={malformed[1]!r}"
        )


def _validate_v5_legacy_creator_reputation_events(conn: sqlite3.Connection) -> None:
    """Reject creator history that the v5 table and INSERT guards would reject."""
    malformed = conn.execute(
        """SELECT event.id,event.mint,event.outcome
FROM creator_reputation_events AS event
WHERE NOT EXISTS (SELECT 1 FROM tokens WHERE tokens.mint=event.mint)
 OR typeof(event.creator)<>'text' OR length(event.creator) NOT BETWEEN 1 AND 128
 OR event.creator<>trim(event.creator,' ') OR instr(event.creator,char(0))<>0
 OR typeof(event.outcome)<>'text' OR event.outcome NOT IN ('GRADUATED','RUGGED')
 OR typeof(event.observed_at) NOT IN ('integer','real')
 OR event.observed_at NOT BETWEEN 0.0 AND 4102444800.0
 OR EXISTS (
   SELECT 1 FROM creator_reputation_events AS old
   WHERE old.id<event.id AND old.mint=event.mint AND old.outcome=event.outcome)
 OR EXISTS (
   SELECT 1 FROM creator_reputation_events AS old
   WHERE old.id<event.id AND old.mint=event.mint AND old.creator<>event.creator)
 OR (event.outcome='GRADUATED' AND EXISTS (
   SELECT 1 FROM creator_reputation_events AS old
   WHERE old.id<event.id AND old.mint=event.mint AND old.outcome='RUGGED'))
 OR (event.outcome='RUGGED' AND EXISTS (
   SELECT 1 FROM creator_reputation_events AS old
   WHERE old.id<event.id AND old.mint=event.mint
     AND old.observed_at>=event.observed_at))
ORDER BY event.id
LIMIT 1"""
    ).fetchone()
    if malformed is not None:
        raise ValueError(
            f"invalid legacy creator_reputation_events row id={malformed[0]}"
            f" mint={malformed[1]!r} outcome={malformed[2]!r}"
        )


def _validate_v5_legacy_p3_trade_execution_graph(conn: sqlite3.Connection) -> None:
    """Reject malformed healing-state strict-P3 trade/execution graphs.

    The migration calls this only after the v5 tables and proof columns exist and
    before summaries or triggers are installed.  It deliberately ignores legacy
    trades with no P3 link/observation and canonical-observation outcomes (row 0t
    validates those while rebuilding ``canonical_pending_current``).  This pass is
    SELECT-only and does not consult ``p3_position_current``; row 0s owns that rebuild.
    """

    def numeric(value: object, low: float, high: float, *, positive: bool = False) -> bool:
        return (
            type(value) in (int, float)
            and (value > low if positive else value >= low)
            and value <= high
            and math.isfinite(value)
        )

    def sqlite_text_metrics(value: object) -> tuple[str, int | None, int | None]:
        row = conn.execute(
            "SELECT typeof(?),length(?),length(trim(?))", (value, value, value)
        ).fetchone()
        return row[0], row[1], row[2]

    def bounded_text(
        value: object, low: int, high: int, *, trim: bool = False,
        require_text: bool = True,
    ) -> bool:
        storage, length, trimmed_length = sqlite_text_metrics(value)
        measured = trimmed_length if trim else length
        return (
            (not require_text or storage == "text")
            and measured is not None
            and low <= measured <= high
        )

    def same(left: object, right: object) -> bool:
        if left is None or right is None:
            return left is right
        if type(left) in (int, float) and type(right) in (int, float):
            return left == right
        return type(left) is type(right) and left == right

    def hash_text(value: object) -> bool:
        return (
            type(value) is str
            and len(value) == 64
            and value == value.lower()
            and all(character in "0123456789abcdef" for character in value)
        )

    def strict_json_object(value: object) -> dict[str, object] | None:
        if type(value) is not str:
            return None

        def object_no_duplicates(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError("duplicate JSON key")
                result[key] = item
            return result

        def reject_constant(token: str) -> object:
            raise ValueError(f"invalid JSON constant: {token}")

        try:
            parsed = json.loads(
                value,
                object_pairs_hook=object_no_duplicates,
                parse_constant=reject_constant,
            )
            if type(parsed) is not dict:
                return None
            canonical = json.dumps(
                parsed, sort_keys=True, separators=(",", ":"), allow_nan=False,
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return parsed if value == canonical else None

    def valid_recheck_payload(row: sqlite3.Row) -> bool:
        payload = strict_json_object(row["payload_json"])
        if payload is None or set(payload) != {
            "decision_id", "attempt", "trigger", "trigger_report_id",
            "rechecked_at", "fill_event_at", "causal_target_report_id",
            "latest_target_report_id", "prior_inputs_hash", "target_snapshot",
            "verdict",
        }:
            return False
        verdict = payload.get("verdict")
        if type(verdict) is not dict or set(verdict) != {
            "status", "reason", "canonical_mint", "inputs_hash",
        }:
            return False
        if not hash_text(verdict.get("inputs_hash")):
            return False

        trigger = payload.get("trigger")
        trigger_report_id = payload.get("trigger_report_id")
        fill_event_at = payload.get("fill_event_at")
        snapshot = payload.get("target_snapshot")
        if trigger == "curve_progress":
            if (
                trigger_report_id is not None
                or not numeric(fill_event_at, 0.0, 4102444800.0)
                or type(snapshot) is not dict
                or set(snapshot) != {
                    "t_wall", "t_mono", "virtual_sol_reserves",
                    "virtual_token_reserves", "real_sol_reserves",
                    "real_token_reserves", "liquidity_sol", "spot_price_sol",
                    "progress_pct",
                }
                or not same(snapshot.get("t_wall"), fill_event_at)
                or not numeric(snapshot.get("t_wall"), 0.0, 4102444800.0)
                or not numeric(snapshot.get("t_mono"), 0.0, 1e100)
                or type(snapshot.get("virtual_sol_reserves")) is not int
                or snapshot["virtual_sol_reserves"] <= 0
                or type(snapshot.get("virtual_token_reserves")) is not int
                or snapshot["virtual_token_reserves"] <= 0
                or type(snapshot.get("real_sol_reserves")) is not int
                or snapshot["real_sol_reserves"] < 0
                or type(snapshot.get("real_token_reserves")) is not int
                or snapshot["real_token_reserves"] < 0
                or not numeric(snapshot.get("liquidity_sol"), 0.0, 1e100)
                or not numeric(
                    snapshot.get("spot_price_sol"), 0.0, 1e100, positive=True,
                )
                or not numeric(snapshot.get("progress_pct"), 0.0, 100.0)
                or fill_event_at >= row["rechecked_at"]
            ):
                return False
        elif trigger == "safety_hard_fail":
            trigger_report = (
                report_by_id.get(trigger_report_id)
                if type(trigger_report_id) is int
                else None
            )
            decision = decision_by_id.get(row["decision_id"])
            try:
                trigger_hard_fails = (
                    json.loads(trigger_report["hard_fails_json"])
                    if trigger_report is not None
                    else None
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                trigger_hard_fails = None
            if (
                type(trigger_report_id) is not int
                or trigger_report_id <= 0
                or fill_event_at is not None
                or snapshot is not None
                or trigger_report is None
                or decision is None
                or trigger_report["mint"] != decision["mint"]
                or not numeric(
                    trigger_report["checked_at"], 0.0, 4102444800.0,
                )
                or trigger_report["checked_at"] >= row["rechecked_at"]
                or type(trigger_hard_fails) is not list
                or not trigger_hard_fails
            ):
                return False
        else:
            return False

        return (
            same(row["decision_id"], payload.get("decision_id"))
            and same(row["attempt"], payload.get("attempt"))
            and same(row["rechecked_at"], payload.get("rechecked_at"))
            and same(
                row["causal_target_report_id"],
                payload.get("causal_target_report_id"),
            )
            and same(
                row["latest_target_report_id"],
                payload.get("latest_target_report_id"),
            )
            and same(row["prior_inputs_hash"], payload.get("prior_inputs_hash"))
            and same(row["reason"], verdict.get("reason"))
            and same(row["canonical_mint"], verdict.get("canonical_mint"))
            and hash_text(row["prior_inputs_hash"])
            and hash_text(row["recheck_inputs_hash"])
            and hashlib.sha256(row["payload_json"].encode()).hexdigest()
            == row["recheck_inputs_hash"]
        )

    def fail(table: str, row: sqlite3.Row) -> None:
        message = f"invalid legacy P3 graph {table} id={row['id']}"
        if "decision_id" in row.keys():
            message += f" decision_id={row['decision_id']}"
        if "mint" in row.keys():
            message += f" mint={row['mint']!r}"
        raise ValueError(message)

    decisions = conn.execute(
        """SELECT d.*,
 CASE WHEN json_valid(d.feature_vector_json)
      THEN json_extract(d.feature_vector_json,'$.canonical.status') END AS p3_status,
 CASE WHEN json_valid(d.feature_vector_json)
      THEN json_extract(d.feature_vector_json,'$.canonical.inputs_hash') END AS p3_inputs_hash,
 CASE WHEN json_valid(d.feature_vector_json)
      THEN json_extract(d.feature_vector_json,'$.canonical.planned_size_sol') END AS p3_planned_size
FROM decisions AS d ORDER BY d.id"""
    ).fetchall()
    decision_by_id = {row["id"]: row for row in decisions}
    p3_decision_ids = {
        row["id"] for row in decisions
        if row["action"] == "BUY" and row["p3_status"] == "CANONICAL"
    }

    observations = conn.execute(
        """SELECT observation.*,
 EXISTS(SELECT 1 FROM decisions WHERE id=observation.decision_id) AS decision_exists,
 EXISTS(SELECT 1 FROM tokens WHERE mint=observation.mint) AS token_exists,
 (length(trim(observation.mint)) BETWEEN 1 AND 128
  AND typeof(observation.observed_at) IN ('integer','real')
  AND observation.observed_at BETWEEN 0.0 AND 4102444800.0
  AND typeof(observation.is_subject)='integer' AND observation.is_subject IN (0,1)
  AND typeof(observation.is_canonical)='integer' AND observation.is_canonical IN (0,1)
  AND typeof(observation.eligible)='integer' AND observation.eligible IN (0,1)
  AND (
    (observation.unavailable_reason=''
     AND typeof(observation.start_price_sol) IN ('integer','real')
     AND observation.start_price_sol>0.0 AND observation.start_price_sol<=1e100
     AND typeof(observation.price_observed_at) IN ('integer','real')
     AND observation.price_observed_at BETWEEN 0.0 AND observation.observed_at
     AND observation.price_source='curve_snapshot')
    OR
    (observation.unavailable_reason IN (
       'start_price_missing','start_price_stale','start_price_malformed')
     AND observation.start_price_sol IS NULL
     AND observation.price_observed_at IS NULL
     AND observation.price_source='')
  )) AS shape_valid,
 (SELECT d.at FROM decisions AS d WHERE d.id=observation.decision_id) AS decision_at
FROM canonical_observations AS observation ORDER BY observation.id"""
    ).fetchall()
    observation_decision_ids = {row["decision_id"] for row in observations}
    qualifying_observations: set[tuple[object, object]] = set()
    observation_keys: set[tuple[object, object]] = set()
    for row in observations:
        key = (row["decision_id"], row["mint"])
        if (
            key in observation_keys
            or row["decision_exists"] != 1
            or row["token_exists"] != 1
            or row["shape_valid"] != 1
            or row["observed_at"] != row["decision_at"]
        ):
            fail("canonical_observations", row)
        observation_keys.add(key)
        if row["is_subject"] == row["is_canonical"] == row["eligible"] == 1:
            qualifying_observations.add(key)

    reports = conn.execute("SELECT * FROM safety_reports ORDER BY id").fetchall()
    report_by_id = {row["id"]: row for row in reports}

    rechecks = conn.execute(
        """SELECT cr.*,
 CASE WHEN json_valid(cr.payload_json) THEN json_extract(cr.payload_json,'$.verdict.status') END AS payload_status
FROM canonical_rechecks AS cr ORDER BY cr.id"""
    ).fetchall()
    recheck_by_id = {row["id"]: row for row in rechecks}
    recheck_keys: set[tuple[object, object]] = set()
    rechecks_by_decision: dict[object, list[sqlite3.Row]] = {}
    previous_recheck_by_decision: dict[object, sqlite3.Row] = {}
    cancelled_decisions: set[object] = set()
    for row in rechecks:
        key = (row["decision_id"], row["attempt"])
        previous = previous_recheck_by_decision.get(row["decision_id"])
        invalid = (
            row["decision_id"] in cancelled_decisions
            or key in recheck_keys
            or type(row["attempt"]) is not int
            or row["attempt"] < 1
            or (previous is None and row["attempt"] != 1)
            or (
                previous is not None
                and (
                    row["attempt"] != previous["attempt"] + 1
                    or row["rechecked_at"] <= previous["rechecked_at"]
                )
            )
            or not numeric(row["rechecked_at"], 0.0, 4102444800.0)
            or row["status"] not in ("PASS", "CANCEL")
            or not bounded_text(
                row["reason"], 1, 2**63 - 1, trim=True, require_text=False
            )
            or not valid_recheck_payload(row)
        )
        if row["status"] == "PASS":
            invalid = invalid or (
                row["latest_target_report_id"] != row["causal_target_report_id"]
                or not bounded_text(row["canonical_mint"], 1, 128, trim=True)
                or row["payload_status"] != "CANONICAL"
            )
        else:
            invalid = invalid or row["payload_status"] not in ("SUPPRESSED", "UNRESOLVED")

        decision = decision_by_id.get(row["decision_id"])
        causal = report_by_id.get(row["causal_target_report_id"])
        latest_candidates = (
            [
                report for report in reports
                if decision is not None
                and report["mint"] == decision["mint"]
                and numeric(report["checked_at"], 0.0, 4102444800.0)
                and report["checked_at"] < row["rechecked_at"]
            ]
            if numeric(row["rechecked_at"], 0.0, 4102444800.0)
            else []
        )
        latest = max(latest_candidates, key=lambda report: report["id"], default=None)
        invalid = invalid or (
            decision is None
            or decision["action"] != "BUY"
            or decision["p3_status"] != "CANONICAL"
            or decision["safety_report_id"] != row["causal_target_report_id"]
            or causal is None
            or causal["mint"] != decision["mint"]
            or not numeric(causal["checked_at"], 0.0, 4102444800.0)
            or not numeric(decision["at"], 0.0, 4102444800.0)
            or causal["checked_at"] >= decision["at"]
            or row["rechecked_at"] <= decision["at"]
            or (row["status"] == "PASS" and row["canonical_mint"] != decision["mint"])
            or row["prior_inputs_hash"] != decision["p3_inputs_hash"]
            or latest is None
            or row["latest_target_report_id"] != latest["id"]
            or not numeric(latest["checked_at"], 0.0, 4102444800.0)
            or latest["checked_at"] >= row["rechecked_at"]
        )
        if invalid:
            fail("canonical_rechecks", row)
        recheck_keys.add(key)
        rechecks_by_decision.setdefault(row["decision_id"], []).append(row)
        previous_recheck_by_decision[row["decision_id"]] = row
        if row["status"] == "CANCEL":
            cancelled_decisions.add(row["decision_id"])

    latest_recheck_by_decision = dict(previous_recheck_by_decision)

    trades = conn.execute("SELECT * FROM paper_trades ORDER BY id").fetchall()
    trade_by_id = {row["id"]: row for row in trades}
    tagged_trade_ids: set[object] = set()
    buy_by_decision: dict[object, sqlite3.Row] = {}
    sells_by_decision: dict[object, list[sqlite3.Row]] = {}
    trade_fee_sum: dict[object, float] = {}
    for row in trades:
        tagged = (
            row["canonical_recheck_id"] is not None
            or row["canonical_proof_hash"] is not None
            or row["p3_entry_execution_id"] is not None
            or row["decision_id"] in observation_decision_ids
        )
        if not tagged:
            continue
        tagged_trade_ids.add(row["id"])
        try:
            fees = p3_fee_sum_json(row["fees_json"])
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
            fail("paper_trades", row)
        invalid = (
            (row["canonical_recheck_id"] is None) != (row["canonical_proof_hash"] is None)
            or row["side"] not in ("buy", "sell")
            or not bounded_text(row["mint"], 1, 128, trim=True)
            or not bounded_text(row["segment"], 1, 64, trim=True)
            or not numeric(row["at"], 0.0, 4102444800.0)
            or not numeric(row["qty"], 0.0, 1e100, positive=True)
            or not numeric(row["quote_price"], 0.0, 1e100)
            or not numeric(row["fill_price"], 0.0, 1e100)
            or (row["side"] == "sell" and row["fill_price"] > row["quote_price"])
            or not numeric(fees, 0.0, 1e100)
            or not bounded_text(row["realism_grade"], 1, 32)
            or (
                row["canonical_recheck_id"] is not None
                and (row["quote_price"] <= 0.0 or row["fill_price"] <= 0.0)
            )
        )
        if invalid:
            fail("paper_trades", row)
        decision = decision_by_id.get(row["decision_id"])
        if row["side"] == "buy":
            recheck = recheck_by_id.get(row["canonical_recheck_id"])
            invalid = (
                decision is None
                or recheck is None
                or recheck["status"] != "PASS"
                or latest_recheck_by_decision.get(row["decision_id"]) is not recheck
                or row["p3_entry_execution_id"] is not None
                or (row["decision_id"], decision["mint"]) not in qualifying_observations
                or row["mint"] != decision["mint"]
                or recheck["canonical_mint"] != row["mint"]
                or recheck["causal_target_report_id"] != decision["safety_report_id"]
                or recheck["latest_target_report_id"] != recheck["causal_target_report_id"]
                or recheck["prior_inputs_hash"] != decision["p3_inputs_hash"]
                or recheck["recheck_inputs_hash"] != row["canonical_proof_hash"]
                or row["at"] <= recheck["rechecked_at"]
                or row["fill_price"] < row["quote_price"]
                or row["decision_id"] in buy_by_decision
            )
            if invalid:
                fail("paper_trades", row)
            buy_by_decision[row["decision_id"]] = row
        else:
            if row["canonical_recheck_id"] is not None or row["canonical_proof_hash"] is not None:
                fail("paper_trades", row)
            sells_by_decision.setdefault(row["decision_id"], []).append(row)
        trade_fee_sum[row["id"]] = fees

    executions = conn.execute(
        "SELECT * FROM paper_entry_executions ORDER BY id"
    ).fetchall()
    execution_by_id = {row["id"]: row for row in executions}
    execution_decisions: set[object] = set()
    execution_trades: set[object] = set()
    for row in executions:
        invalid = (
            row["decision_id"] in execution_decisions
            or (row["paper_trade_id"] is not None and row["paper_trade_id"] in execution_trades)
            or not numeric(row["at"], 0.0, 4102444800.0)
            or row["status"] not in ("FILLED", "CANCELLED", "ABANDONED")
            or not bounded_text(
                row["reason"], 1, 2**63 - 1, trim=True, require_text=False
            )
            or not numeric(row["planned_size_sol"], 0.0, 1e100, positive=True)
        )
        decision = decision_by_id.get(row["decision_id"])
        invalid = invalid or (
            decision is None
            or decision["action"] != "BUY"
            or decision["p3_status"] != "CANONICAL"
            or row["decision_id"] not in observation_decision_ids
            or not same(row["planned_size_sol"], decision["p3_planned_size"])
            or not numeric(decision["at"], 0.0, 4102444800.0)
            or row["at"] <= decision["at"]
        )
        recheck = recheck_by_id.get(row["canonical_recheck_id"])
        trade = trade_by_id.get(row["paper_trade_id"])
        if row["status"] == "FILLED":
            invalid = invalid or (
                row["reason"] != "filled"
                or recheck is None
                or trade is None
                or trade["id"] not in tagged_trade_ids
                or trade["side"] != "buy"
                or trade["decision_id"] != row["decision_id"]
                or trade["mint"] != decision["mint"]
                or trade["canonical_recheck_id"] != recheck["id"]
                or trade["canonical_proof_hash"] != recheck["recheck_inputs_hash"]
                or recheck["decision_id"] != row["decision_id"]
                or recheck["status"] != "PASS"
                or latest_recheck_by_decision.get(row["decision_id"]) is not recheck
                or recheck["canonical_mint"] != decision["mint"]
                or row["at"] != trade["at"]
                or row["at"] <= recheck["rechecked_at"]
                or trade["quote_price"] <= 0.0
                or trade["fill_price"] <= 0.0
                or trade["fill_price"] < trade["quote_price"]
                or not math.isclose(
                    row["planned_size_sol"], trade["qty"] * trade["fill_price"],
                    rel_tol=1e-12, abs_tol=1e-12,
                )
            )
        elif row["status"] == "CANCELLED":
            invalid = invalid or (
                row["paper_trade_id"] is not None
                or recheck is None
                or recheck["decision_id"] != row["decision_id"]
                or recheck["status"] != "CANCEL"
                or latest_recheck_by_decision.get(row["decision_id"]) is not recheck
                or row["reason"] != recheck["reason"]
                or row["at"] <= recheck["rechecked_at"]
            )
        else:
            has_cancel = any(
                item["status"] == "CANCEL"
                for item in rechecks_by_decision.get(row["decision_id"], ())
            )
            before_fill = (
                row["canonical_recheck_id"] is None
                and row["reason"] == "restart_before_fill"
                and not rechecks_by_decision.get(row["decision_id"])
            )
            after_pass = (
                recheck is not None
                and recheck["decision_id"] == row["decision_id"]
                and recheck["status"] == "PASS"
                and latest_recheck_by_decision.get(row["decision_id"]) is recheck
                and row["reason"] == "restart_after_pass"
                and row["at"] > recheck["rechecked_at"]
            )
            invalid = invalid or row["paper_trade_id"] is not None or has_cancel or not (
                before_fill or after_pass
            )
        if invalid:
            fail("paper_entry_executions", row)
        execution_decisions.add(row["decision_id"])
        if row["paper_trade_id"] is not None:
            execution_trades.add(row["paper_trade_id"])

    for decision in decisions:
        if decision["id"] in p3_decision_ids and decision["id"] not in execution_decisions:
            fail("decisions", decision)

    full_sell_by_decision: dict[object, sqlite3.Row] = {}
    sell_proceeds_by_decision: dict[object, float] = {}
    for decision_id, sells in sells_by_decision.items():
        decision = decision_by_id.get(decision_id)
        buy = buy_by_decision.get(decision_id)
        entry = (
            execution_by_id.get(sells[0]["p3_entry_execution_id"])
            if sells else None
        )
        if decision is None or buy is None or entry is None:
            fail("paper_trades", sells[0])
        assert decision is not None and buy is not None and entry is not None
        sold_qty = 0.0
        sell_proceeds = 0.0
        previous_id = buy["id"]
        previous_at = entry["at"]
        for sell in sells:
            sell_entry = execution_by_id.get(sell["p3_entry_execution_id"])
            sold_qty += sell["qty"]
            sell_proceeds = (
                sell_proceeds + sell["qty"] * sell["fill_price"]
            ) - trade_fee_sum[sell["id"]]
            invalid = (
                sell_entry is not entry
                or entry["status"] != "FILLED"
                or entry["decision_id"] != decision_id
                or sell["mint"] != decision["mint"]
                or (decision_id, decision["mint"]) not in qualifying_observations
                or sell["id"] <= previous_id
                or sell["at"] <= previous_at
                or sold_qty > buy["qty"]
                or not math.isfinite(sell_proceeds)
            )
            if invalid:
                fail("paper_trades", sell)
            previous_id = sell["id"]
            previous_at = sell["at"]
        if sold_qty != buy["qty"]:
            fail("paper_trades", sells[-1])
        full_sell_by_decision[decision_id] = sells[-1]
        sell_proceeds_by_decision[decision_id] = sell_proceeds

    outcomes = conn.execute(
        """SELECT outcome.*,
 json_valid(outcome.detail_json) AS detail_valid,
 CASE WHEN json_valid(outcome.detail_json) THEN json_type(outcome.detail_json) END AS detail_type,
 CASE WHEN json_valid(outcome.detail_json) AND json_type(outcome.detail_json)='object'
      THEN (SELECT count(*) FROM json_each(outcome.detail_json)) END AS detail_count,
 CASE WHEN json_valid(outcome.detail_json) THEN json_type(outcome.detail_json,'$.reason') END AS reason_type,
 CASE WHEN json_valid(outcome.detail_json) THEN json_extract(outcome.detail_json,'$.reason') END AS reason,
 CASE WHEN json_valid(outcome.detail_json) THEN json_type(outcome.detail_json,'$.hold_s') END AS hold_type,
 CASE WHEN json_valid(outcome.detail_json) THEN json_extract(outcome.detail_json,'$.hold_s') END AS hold_s,
 CASE WHEN json_valid(outcome.detail_json) THEN json_type(outcome.detail_json,'$.grade') END AS grade_type,
 CASE WHEN json_valid(outcome.detail_json) THEN json_extract(outcome.detail_json,'$.grade') END AS grade
FROM outcomes AS outcome ORDER BY outcome.id"""
    ).fetchall()
    outcome_exit_ids: set[object] = set()
    outcome_decisions: set[object] = set()
    final_reasons = {
        "time_stop", "trailing_stop", "graduated", "dead", "graduated_no_price",
        "safety_flip", "stale", "restart_safety_hard_fail",
    }
    for row in outcomes:
        ref_trade = trade_by_id.get(row["ref_id"])
        tagged = row["p3_exit_trade_id"] is not None or (
            row["ref_kind"] == "trade"
            and ref_trade is not None
            and ref_trade["decision_id"] in observation_decision_ids
        )
        if not tagged:
            continue
        exit_trade = trade_by_id.get(row["p3_exit_trade_id"])
        entry = (
            execution_by_id.get(exit_trade["p3_entry_execution_id"])
            if exit_trade is not None else None
        )
        decision_id = exit_trade["decision_id"] if exit_trade is not None else None
        buy = buy_by_decision.get(decision_id)
        invalid = (
            row["p3_exit_trade_id"] in outcome_exit_ids
            or not numeric(row["at"], 0.0, 4102444800.0)
            or not numeric(row["pnl_sol"], -1e100, 1e100)
            or not bounded_text(row["detail_json"], 0, 8192)
            or row["detail_valid"] != 1
            or row["detail_type"] != "object"
            or row["ref_kind"] != "trade"
            or exit_trade is None
            or exit_trade["id"] != row["ref_id"]
            or exit_trade["side"] != "sell"
            or full_sell_by_decision.get(decision_id) is not exit_trade
            or entry is None
            or buy is None
            or row["at"] != exit_trade["at"]
            or row["detail_count"] != 3
            or row["reason_type"] != "text"
            or row["reason"] not in final_reasons
            or row["hold_type"] not in ("integer", "real")
            or row["hold_s"] != row["at"] - entry["at"]
            or row["grade_type"] != "text"
            or row["grade"] != exit_trade["realism_grade"]
        )
        if not invalid:
            expected_pnl = sell_proceeds_by_decision[decision_id] - (
                buy["qty"] * buy["fill_price"]
                + trade_fee_sum[buy["id"]]
            )
            invalid = not math.isfinite(expected_pnl) or row["pnl_sol"] != expected_pnl
        if invalid:
            fail("outcomes", row)
        outcome_exit_ids.add(row["p3_exit_trade_id"])
        outcome_decisions.add(decision_id)

    for decision_id, sell in full_sell_by_decision.items():
        if decision_id not in outcome_decisions:
            fail("paper_trades", sell)


def initialize_p3_causal_clock(
    conn: sqlite3.Connection, *, raw_now: float,
) -> float:
    """Seed the v5 causal watermark from every timestamp present during migration."""
    if not conn.in_transaction:
        raise RuntimeError("p3 causal clock initialization requires an active transaction")

    def valid_wall(value: object) -> bool:
        return (
            type(value) in (int, float)
            and 0.0 <= value <= 4102444800.0
            and math.isfinite(value)
        )

    if not valid_wall(raw_now):
        raise ValueError("invalid p3 causal clock raw_now")

    source_columns = (
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
    nullable_sources = {
        ("tokens", "p3_identity_ingested_at"),
        ("tokens", "curve_progress_observed_at"),
    }
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_schema WHERE type='table'")
    }
    seed = float(raw_now)
    for table, columns in source_columns:
        if table not in tables:
            continue
        present = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
        for column in columns:
            if column not in present:
                continue
            for (value,) in conn.execute(
                f'SELECT "{column}" FROM "{table}"'  # nosec B608
            ):
                if value is None and (table, column) in nullable_sources:
                    continue
                if not valid_wall(value):
                    raise ValueError(
                        f"invalid p3 causal clock source {table}.{column}"
                    )
                seed = max(seed, float(value))

    clock_rows = conn.execute(
        "SELECT singleton,last_wall FROM p3_causal_clock LIMIT 2"
    ).fetchall()
    if len(clock_rows) > 1:
        raise ValueError("invalid p3_causal_clock singleton rows")
    if clock_rows:
        singleton, last_wall = clock_rows[0]
        if type(singleton) is not int or singleton != 1 or not valid_wall(last_wall):
            raise ValueError("invalid p3_causal_clock row")
        seed = max(seed, float(last_wall))
        if seed > last_wall:
            conn.execute(
                "UPDATE p3_causal_clock SET last_wall=? WHERE singleton=1", (seed,)
            )
    else:
        conn.execute(
            "INSERT INTO p3_causal_clock(singleton,last_wall) VALUES (1,?)", (seed,)
        )
    return seed


def _validated_p3_causal_wall(value: object) -> float:
    if (
        type(value) not in (int, float)
        or not 0.0 <= value <= 4102444800.0
        or not math.isfinite(value)
    ):
        raise ValueError("invalid p3 causal wall")
    return float(value)


def _p3_causal_clock_last_wall(conn: sqlite3.Connection) -> float:
    rows = conn.execute(
        "SELECT singleton,last_wall FROM p3_causal_clock LIMIT 2"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError("invalid p3_causal_clock singleton rows")
    singleton, last_wall = rows[0]
    if type(singleton) is not int or singleton != 1:
        raise ValueError("invalid p3_causal_clock row")
    try:
        return _validated_p3_causal_wall(last_wall)
    except ValueError as exc:
        raise ValueError("invalid p3_causal_clock row") from exc


@dataclass(slots=True)
class _P3ImmediateState:
    owner_thread: int
    allocated_at: float | None = None
    writer_claimed: bool = False
    writer_completed: bool = False
    poisoned: bool = False


_P3_IMMEDIATE_STATES: dict[sqlite3.Connection, _P3ImmediateState] = {}


def allocate_p3_causal_wall(
    conn: sqlite3.Connection, *, raw_wall: float,
) -> float:
    """Allocate one durable processing time in the caller's transaction."""
    if not conn.in_transaction:
        raise RuntimeError("p3 causal clock allocation requires an active transaction")
    state = _P3_IMMEDIATE_STATES.get(conn)
    if state is not None:
        if state.owner_thread != get_ident():
            state.poisoned = True
            raise RuntimeError("p3 one-shot transaction owner thread mismatch")
        if (
            state.poisoned
            or state.allocated_at is not None
            or state.writer_claimed
        ):
            state.poisoned = True
            raise RuntimeError("p3 one-shot causal allocation already used")
    try:
        raw = _validated_p3_causal_wall(raw_wall)
        last_wall = _p3_causal_clock_last_wall(conn)
        allocated = raw if raw > last_wall else math.nextafter(last_wall, math.inf)
        if not math.isfinite(allocated) or not 0.0 <= allocated <= 4102444800.0:
            raise ValueError("invalid p3 causal allocation")
        conn.execute(
            "UPDATE p3_causal_clock SET last_wall=? WHERE singleton=1", (allocated,)
        )
    except BaseException:
        if state is not None:
            state.poisoned = True
        raise
    if state is not None:
        state.allocated_at = allocated
    return allocated


@contextmanager
def p3_immediate_transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """Own one serialized P3 evidence transaction and its rollback boundary."""
    if conn.in_transaction or conn in _P3_IMMEDIATE_STATES:
        raise RuntimeError("p3_immediate_transaction owns its transaction")
    conn.execute("BEGIN IMMEDIATE")
    state = _P3ImmediateState(owner_thread=get_ident())
    _P3_IMMEDIATE_STATES[conn] = state
    try:
        yield
        if state.poisoned:
            raise RuntimeError("p3 one-shot transaction poisoned")
        if (
            state.allocated_at is None
            or not state.writer_claimed
            or not state.writer_completed
        ):
            raise RuntimeError(
                "p3_immediate_transaction requires exactly one completed "
                "row26 writer"
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        _P3_IMMEDIATE_STATES.pop(conn, None)


def fence_p3_causal_wall(
    conn: sqlite3.Connection, *, observed_wall: float,
) -> float:
    """Advance the durable watermark to an observation without allocating after it."""
    if not conn.in_transaction:
        raise RuntimeError("p3 causal clock fence requires an active transaction")
    observed = _validated_p3_causal_wall(observed_wall)
    last_wall = _p3_causal_clock_last_wall(conn)
    fenced = max(last_wall, observed)
    if fenced > last_wall:
        conn.execute(
            "UPDATE p3_causal_clock SET last_wall=? WHERE singleton=1", (fenced,)
        )
    return fenced


def _verify_v5_wallet_pnl_summary(conn: sqlite3.Connection) -> None:
    """Independently compare the operational wallet summary with its evidence."""
    if not conn.in_transaction:
        raise RuntimeError("wallet PnL summary verification requires an active transaction")

    actual_rows = conn.execute(
        """SELECT wallet,event_count,realized_pnl_sol,last_at,last_event_id
FROM wallet_pnl_summary
ORDER BY wallet"""
    )

    def expected_rows():
        wallet = None
        event_count = 0
        realized_pnl_sol = 0.0
        last_at = 0.0
        last_event_id = 0
        for row in conn.execute(
            """SELECT wallet,id,at,realized_pnl_sol
FROM wallet_pnl_events
ORDER BY wallet,id"""
        ):
            if wallet is not None and row[0] != wallet:
                yield (
                    wallet, event_count, realized_pnl_sol, last_at, last_event_id,
                )
                event_count = 0
            if event_count == 0:
                wallet = row[0]
                realized_pnl_sol = row[3]
                last_at = row[2]
            else:
                realized_pnl_sol += row[3]
                last_at = max(last_at, row[2])
            event_count += 1
            last_event_id = row[1]
        if wallet is not None:
            yield wallet, event_count, realized_pnl_sol, last_at, last_event_id

    expected = iter(expected_rows())
    while True:
        expected_row = next(expected, None)
        actual = actual_rows.fetchone()
        if expected_row is None or actual is None:
            if expected_row is not None or actual is not None:
                raise ValueError("wallet_pnl_summary row set mismatch")
            return

        wallet, event_count, realized_pnl_sol, last_at, last_event_id = actual
        valid_shape = (
            type(wallet) is str
            and type(event_count) is int
            and event_count > 0
            and type(realized_pnl_sol) in (int, float)
            and math.isfinite(realized_pnl_sol)
            and -1000000000000.0 <= realized_pnl_sol <= 1000000000000.0
            and type(last_at) in (int, float)
            and math.isfinite(last_at)
            and 0.0 <= last_at <= 4102444800.0
            and type(last_event_id) is int
        )
        if not valid_shape or tuple(actual) != expected_row:
            raise ValueError(f"wallet_pnl_summary mismatch for wallet={wallet!r}")


def _rebuild_v5_wallet_pnl_summary(conn: sqlite3.Connection) -> None:
    """Replay validated wallet evidence using the live summary arithmetic."""
    if not conn.in_transaction:
        raise RuntimeError("wallet PnL summary rebuild requires an active transaction")

    conn.execute("DELETE FROM wallet_pnl_summary")
    events = conn.execute(
        """SELECT id,at,wallet,realized_pnl_sol
FROM wallet_pnl_events
ORDER BY id"""
    )
    for event_id, at, wallet, realized_pnl_sol in events:
        conn.execute(
            """INSERT INTO wallet_pnl_summary(
  wallet,event_count,realized_pnl_sol,last_at,last_event_id)
VALUES(?,1,?,?,?)
ON CONFLICT(wallet) DO UPDATE SET
  event_count=event_count+1,
  realized_pnl_sol=realized_pnl_sol+excluded.realized_pnl_sol,
  last_at=max(last_at,excluded.last_at),
  last_event_id=excluded.last_event_id""",
            (wallet, realized_pnl_sol, at, event_id),
        )
    _verify_v5_wallet_pnl_summary(conn)


def _verify_v5_creator_reputation_current(conn: sqlite3.Connection) -> None:
    """Independently compare creator-current state with the latest evidence per mint."""
    if not conn.in_transaction:
        raise RuntimeError(
            "creator reputation current verification requires an active transaction"
        )

    def expected_rows():
        current_mint = None
        current = None
        for event_id, mint, creator, outcome, observed_at in conn.execute(
            """SELECT id,mint,creator,outcome,observed_at
FROM creator_reputation_events
ORDER BY mint,id"""
        ):
            if current_mint is not None and mint != current_mint:
                yield current
                current = None
            current_mint = mint
            candidate = mint, creator, outcome, observed_at, event_id
            if current is None or (observed_at, event_id) > (current[3], current[4]):
                current = candidate
        if current is not None:
            yield current

    actual_rows = conn.execute(
        """SELECT mint,creator,outcome,observed_at,event_id
FROM creator_reputation_current
ORDER BY mint"""
    )
    expected = iter(expected_rows())
    while True:
        expected_row = next(expected, None)
        actual = actual_rows.fetchone()
        if expected_row is None or actual is None:
            if expected_row is not None or actual is not None:
                raise ValueError("creator_reputation_current row set mismatch")
            return

        mint, creator, outcome, observed_at, event_id = actual
        valid_shape = (
            type(creator) is str
            and 1 <= len(creator) <= 128
            and creator == creator.strip(" ")
            and "\x00" not in creator
            and type(outcome) is str
            and outcome in ("GRADUATED", "RUGGED")
            and type(observed_at) in (int, float)
            and math.isfinite(observed_at)
            and 0.0 <= observed_at <= 4102444800.0
            and type(event_id) is int
        )
        if (
            not valid_shape
            or tuple(actual) != expected_row
        ):
            raise ValueError(
                f"creator_reputation_current mismatch for mint={mint!r}"
            )


def _rebuild_v5_creator_reputation_current(conn: sqlite3.Connection) -> None:
    """Rebuild one operational creator-reputation row per mint from evidence."""
    if not conn.in_transaction:
        raise RuntimeError(
            "creator reputation current rebuild requires an active transaction"
        )

    conn.execute("DELETE FROM creator_reputation_current")
    previous_mint = None
    for event_id, mint, creator, outcome, observed_at in conn.execute(
        """SELECT id,mint,creator,outcome,observed_at
FROM creator_reputation_events
ORDER BY mint,observed_at DESC,id DESC"""
    ):
        if mint == previous_mint:
            continue
        conn.execute(
            """INSERT INTO creator_reputation_current(
  mint,creator,outcome,observed_at,event_id)
VALUES(?,?,?,?,?)""",
            (mint, creator, outcome, observed_at, event_id),
        )
        previous_mint = mint
    _verify_v5_creator_reputation_current(conn)


def _rebuild_v5_p3_position_current(conn: sqlite3.Connection) -> None:
    """Replay validated FILLED executions and their P3 trades in trade-ID order."""
    if not conn.in_transaction:
        raise RuntimeError("P3 position summary rebuild requires an active transaction")

    def finite(value: object, *, positive: bool = False) -> bool:
        return (
            type(value) in (int, float)
            and math.isfinite(value)
            and (value > 0.0 if positive else value >= 0.0)
            and value <= 1e100
        )

    def wall_time(value: object) -> bool:
        return (
            type(value) in (int, float)
            and math.isfinite(value)
            and 0.0 <= value <= 4102444800.0
        )

    def fees(value: object) -> float:
        try:
            result = p3_fee_sum_json(value)  # type: ignore[arg-type]
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError) as exc:
            raise ValueError("non-finite position arithmetic") from exc
        if not finite(result):
            raise ValueError("non-finite position arithmetic")
        return result

    conn.execute("DELETE FROM p3_position_current")
    filled = conn.execute(
        """SELECT id,decision_id,at,paper_trade_id
FROM paper_entry_executions
WHERE status='FILLED'
ORDER BY id"""
    )
    sells = conn.execute(
        """SELECT id,decision_id,at,mint,side,qty,fill_price,fees_json,
       p3_entry_execution_id
FROM paper_trades
WHERE p3_entry_execution_id IS NOT NULL
ORDER BY p3_entry_execution_id,id"""
    )
    next_sell = sells.fetchone()
    outcomes = conn.execute(
        """SELECT trade.p3_entry_execution_id,outcome.id,outcome.ref_kind,
       outcome.ref_id,outcome.pnl_sol,outcome.p3_exit_trade_id
FROM outcomes AS outcome
JOIN paper_trades AS trade ON trade.id=outcome.p3_exit_trade_id
ORDER BY trade.p3_entry_execution_id,outcome.id"""
    )
    next_outcome = outcomes.fetchone()
    for entry_id, decision_id, entry_at, paper_trade_id in filled:
        if (
            next_sell is not None
            and (
                type(next_sell[8]) is not int
                or next_sell[8] < entry_id
            )
        ):
            raise ValueError(f"missing linked P3 SELL id={next_sell[0]}")
        if (
            next_outcome is not None
            and (
                type(next_outcome[0]) is not int
                or next_outcome[0] < entry_id
            )
        ):
            raise ValueError(
                f"closed position outcome mismatch id={next_outcome[1]}"
            )
        buy = conn.execute(
            """SELECT id,decision_id,at,mint,side,qty,fill_price,fees_json,
       p3_entry_execution_id,typeof(mint),length(trim(mint))
FROM paper_trades WHERE id=?""",
            (paper_trade_id,),
        ).fetchone()
        if (
            buy is None
            or type(entry_id) is not int
            or type(decision_id) is not int
            or type(paper_trade_id) is not int
            or buy[0] != paper_trade_id
            or buy[1] != decision_id
            or buy[4] != "buy"
            or buy[8] is not None
            or buy[9] != "text"
            or type(buy[10]) is not int
            or not 1 <= buy[10] <= 128
            or not finite(buy[5], positive=True)
            or not finite(buy[6], positive=True)
        ):
            raise ValueError(f"missing entry BUY link for execution id={entry_id}")
        if (
            not wall_time(entry_at)
            or not wall_time(buy[2])
            or entry_at != buy[2]
        ):
            raise ValueError(
                f"entry execution BUY time mismatch for execution id={entry_id}"
            )

        buy_fee = fees(buy[7])
        buy_notional = buy[5] * buy[6] + buy_fee
        if not finite(buy_notional):
            raise ValueError("non-finite position arithmetic")

        sold_qty = 0.0
        sell_proceeds = 0.0
        previous_id = buy[0]
        previous_at = entry_at
        last_sell_id = None
        while next_sell is not None and next_sell[8] == entry_id:
            sell = next_sell
            if (
                sell[1] != decision_id
                or sell[3] != buy[3]
                or sell[4] != "sell"
                or sell[8] != entry_id
                or not wall_time(sell[2])
                or not finite(sell[5], positive=True)
                or not finite(sell[6])
            ):
                raise ValueError(f"missing linked P3 SELL for execution id={entry_id}")
            if type(sell[0]) is not int or sell[0] <= previous_id or sell[2] <= previous_at:
                raise ValueError(f"retrograde trade time for execution id={entry_id}")
            sell_fee = fees(sell[7])
            sold_qty += sell[5]
            sell_proceeds = sell_proceeds + sell[5] * sell[6] - sell_fee
            if not math.isfinite(sold_qty) or not math.isfinite(sell_proceeds):
                raise ValueError("non-finite position arithmetic")
            if sold_qty > buy[5]:
                raise ValueError(f"over-sold P3 position execution id={entry_id}")
            if sell_proceeds < -1e100 or sell_proceeds > 1e100:
                raise ValueError("non-finite position arithmetic")
            previous_id = sell[0]
            previous_at = sell[2]
            last_sell_id = sell[0]
            next_sell = sells.fetchone()

        outcome = None
        extra_outcome = None
        if next_outcome is not None and next_outcome[0] == entry_id:
            outcome = next_outcome[1:]
            next_outcome = outcomes.fetchone()
            if next_outcome is not None and next_outcome[0] == entry_id:
                extra_outcome = next_outcome[1:]
                while next_outcome is not None and next_outcome[0] == entry_id:
                    next_outcome = outcomes.fetchone()
        if last_sell_id is None:
            if outcome is not None:
                raise ValueError(
                    f"closed position outcome mismatch for execution id={entry_id}"
                )
        elif sold_qty < buy[5]:
            raise ValueError(f"partial pre-v5 P3 SELL for execution id={entry_id}")
        else:
            expected_pnl = sell_proceeds - buy_notional
            if (
                not math.isfinite(expected_pnl)
                or outcome is None
                or extra_outcome is not None
                or outcome[1] != "trade"
                or outcome[2] != last_sell_id
                or outcome[4] != last_sell_id
                or type(outcome[3]) not in (int, float)
                or not math.isfinite(outcome[3])
                or not -1e100 <= outcome[3] <= 1e100
                or outcome[3] != expected_pnl
            ):
                raise ValueError(
                    f"closed position outcome mismatch for execution id={entry_id}"
                )

        conn.execute(
            """INSERT INTO p3_position_current(
  decision_id,mint,entry_execution_id,bought_qty,sold_qty,
  buy_notional_sol,sell_proceeds_sol,ladder_mask,last_trade_at)
VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                decision_id, buy[3], entry_id, buy[5], sold_qty,
                buy_notional, sell_proceeds, 0, previous_at,
            ),
        )

    if next_sell is not None:
        raise ValueError(f"missing linked P3 SELL id={next_sell[0]}")
    if next_outcome is not None:
        raise ValueError(f"closed position outcome mismatch id={next_outcome[1]}")

    orphan_outcome = conn.execute(
        """SELECT outcome.id
FROM outcomes AS outcome
LEFT JOIN paper_trades AS trade ON trade.id=outcome.p3_exit_trade_id
LEFT JOIN paper_entry_executions AS execution
  ON execution.id=trade.p3_entry_execution_id
WHERE outcome.p3_exit_trade_id IS NOT NULL
  AND (trade.id IS NULL OR execution.id IS NULL OR execution.status<>'FILLED')
ORDER BY outcome.id LIMIT 1"""
    ).fetchone()
    if orphan_outcome is not None:
        raise ValueError(f"closed position outcome mismatch id={orphan_outcome[0]}")

    _verify_v5_p3_position_current(conn)


def _verify_v5_p3_position_current(conn: sqlite3.Connection) -> None:
    """Independently compare P3 position state with FILLED execution evidence."""
    if not conn.in_transaction:
        raise RuntimeError(
            "P3 position summary verification requires an active transaction"
        )

    malformed_outcome = conn.execute(
        """SELECT outcome.id
FROM outcomes AS outcome
LEFT JOIN paper_trades AS exit_trade
  ON exit_trade.id=outcome.p3_exit_trade_id
LEFT JOIN paper_entry_executions AS exit_execution
  ON exit_execution.id=exit_trade.p3_entry_execution_id
LEFT JOIN paper_trades AS ref_trade
  ON outcome.ref_kind='trade' AND ref_trade.id=outcome.ref_id
LEFT JOIN paper_entry_executions AS ref_entry_execution
  ON ref_entry_execution.paper_trade_id=ref_trade.id
 AND ref_entry_execution.status='FILLED'
LEFT JOIN paper_entry_executions AS ref_decision_execution
  ON ref_decision_execution.decision_id=ref_trade.decision_id
 AND ref_decision_execution.status='FILLED'
WHERE (
  outcome.p3_exit_trade_id IS NOT NULL
  AND (
    exit_trade.id IS NULL
    OR exit_trade.side<>'sell'
    OR exit_trade.p3_entry_execution_id IS NULL
    OR exit_execution.id IS NULL
    OR exit_execution.status<>'FILLED'
  )
) OR (
  outcome.ref_kind='trade'
  AND (
    ref_trade.p3_entry_execution_id IS NOT NULL
    OR ref_entry_execution.id IS NOT NULL
    OR ref_decision_execution.id IS NOT NULL
  )
  AND (
    outcome.p3_exit_trade_id IS NULL
    OR outcome.p3_exit_trade_id<>outcome.ref_id
  )
)
ORDER BY outcome.id
LIMIT 1"""
    ).fetchone()
    if malformed_outcome is not None:
        raise ValueError(
            f"p3_position_current outcome evidence mismatch id={malformed_outcome[0]}"
        )

    actual_rows = conn.execute(
        """SELECT decision_id,mint,entry_execution_id,bought_qty,sold_qty,
       buy_notional_sol,sell_proceeds_sol,ladder_mask,last_trade_at
FROM p3_position_current
ORDER BY entry_execution_id"""
    )
    filled = conn.execute(
        """SELECT id,decision_id,at,paper_trade_id
FROM paper_entry_executions
WHERE status='FILLED'
ORDER BY id"""
    )
    sells = conn.execute(
        """SELECT id,decision_id,at,mint,side,qty,fill_price,fees_json,
       p3_entry_execution_id
FROM paper_trades
WHERE p3_entry_execution_id IS NOT NULL
ORDER BY p3_entry_execution_id,id"""
    )
    next_sell = sells.fetchone()
    outcomes = conn.execute(
        """SELECT trade.p3_entry_execution_id,outcome.ref_kind,outcome.ref_id,
       outcome.pnl_sol,outcome.p3_exit_trade_id
FROM outcomes AS outcome
JOIN paper_trades AS trade ON trade.id=outcome.p3_exit_trade_id
ORDER BY trade.p3_entry_execution_id,outcome.id"""
    )
    next_outcome = outcomes.fetchone()
    for entry_id, decision_id, entry_at, paper_trade_id in filled:
        if (
            next_sell is not None
            and (type(next_sell[8]) is not int or next_sell[8] < entry_id)
        ) or (
            next_outcome is not None
            and (type(next_outcome[0]) is not int or next_outcome[0] < entry_id)
        ):
            raise ValueError("p3_position_current source mismatch")
        buy = conn.execute(
            """SELECT id,decision_id,at,mint,side,qty,fill_price,fees_json,
       p3_entry_execution_id,typeof(mint),length(trim(mint))
FROM paper_trades WHERE id=?""",
            (paper_trade_id,),
        ).fetchone()
        if (
            buy is None
            or type(entry_id) is not int
            or type(decision_id) is not int
            or type(paper_trade_id) is not int
            or type(buy[0]) is not int
            or buy[0] != paper_trade_id
            or buy[1] != decision_id
            or buy[4] != "buy"
            or buy[8] is not None
            or buy[9] != "text"
            or type(buy[10]) is not int
            or not 1 <= buy[10] <= 128
            or type(entry_at) not in (int, float)
            or not math.isfinite(entry_at)
            or not 0.0 <= entry_at <= 4102444800.0
            or type(buy[2]) not in (int, float)
            or not math.isfinite(buy[2])
            or not 0.0 <= buy[2] <= 4102444800.0
            or entry_at != buy[2]
            or type(buy[5]) not in (int, float)
            or not math.isfinite(buy[5])
            or not 0.0 < buy[5] <= 1e100
            or type(buy[6]) not in (int, float)
            or not math.isfinite(buy[6])
            or not 0.0 < buy[6] <= 1e100
        ):
            raise ValueError("p3_position_current source mismatch")
        try:
            buy_fee = p3_fee_sum_json(buy[7])
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError) as exc:
            raise ValueError("p3_position_current source mismatch") from exc
        bought_qty = buy[5]
        buy_notional = bought_qty * buy[6] + buy_fee
        sold_qty = 0.0
        sell_proceeds = 0.0
        previous_id = buy[0]
        previous_at = entry_at
        last_sell_id = None
        sell_count = 0
        while next_sell is not None and next_sell[8] == entry_id:
            sell = next_sell
            try:
                sell_fee = p3_fee_sum_json(sell[7])
            except (TypeError, ValueError, OverflowError, json.JSONDecodeError) as exc:
                raise ValueError("p3_position_current source mismatch") from exc
            if (
                sell[1] != decision_id
                or sell[3] != buy[3]
                or sell[4] != "sell"
                or type(sell[0]) is not int
                or sell[0] <= previous_id
                or type(sell[2]) not in (int, float)
                or not math.isfinite(sell[2])
                or not 0.0 <= sell[2] <= 4102444800.0
                or sell[2] <= previous_at
                or type(sell[5]) not in (int, float)
                or not math.isfinite(sell[5])
                or not 0.0 < sell[5] <= 1e100
                or type(sell[6]) not in (int, float)
                or not math.isfinite(sell[6])
                or not 0.0 <= sell[6] <= 1e100
            ):
                raise ValueError("p3_position_current source mismatch")
            sold_qty += sell[5]
            sell_proceeds = sell_proceeds + sell[5] * sell[6] - sell_fee
            previous_id = sell[0]
            previous_at = sell[2]
            last_sell_id = sell[0]
            sell_count += 1
            next_sell = sells.fetchone()
        if (
            not all(
                type(value) in (int, float) and math.isfinite(value)
                for value in (
                    buy_notional, sold_qty, sell_proceeds,
                )
            )
            or sold_qty < 0.0
            or sold_qty > 1e100
            or sold_qty > bought_qty
            or not 0.0 <= buy_notional <= 1e100
            or not -1e100 <= sell_proceeds <= 1e100
            or (sell_count and sold_qty < bought_qty)
        ):
            raise ValueError("p3_position_current source mismatch")

        outcome = None
        extra_outcome = None
        if next_outcome is not None and next_outcome[0] == entry_id:
            outcome = next_outcome[1:]
            next_outcome = outcomes.fetchone()
            if next_outcome is not None and next_outcome[0] == entry_id:
                extra_outcome = next_outcome
                while next_outcome is not None and next_outcome[0] == entry_id:
                    next_outcome = outcomes.fetchone()
        expected_pnl = sell_proceeds - buy_notional
        if (
            not math.isfinite(expected_pnl)
            or not -1e100 <= expected_pnl <= 1e100
            or (sell_count == 0 and outcome is not None)
        ) or (
            sell_count > 0
            and (
                outcome is None
                or extra_outcome is not None
                or outcome[0] != "trade"
                or outcome[1] != last_sell_id
                or outcome[3] != last_sell_id
                or outcome[2] != expected_pnl
            )
        ):
            raise ValueError("p3_position_current source mismatch")

        actual = actual_rows.fetchone()
        expected = (
            decision_id, buy[3], entry_id, bought_qty, sold_qty,
            buy_notional, sell_proceeds, 0, previous_at,
        )
        if actual is None or tuple(actual) != expected:
            raise ValueError(
                f"p3_position_current mismatch for decision_id={decision_id}"
            )

    if actual_rows.fetchone() is not None:
        raise ValueError("p3_position_current row set mismatch")
    if next_sell is not None or next_outcome is not None:
        raise ValueError("p3_position_current source mismatch")


def _v5_canonical_pending_horizons(
    conn: sqlite3.Connection, feature_vector_json: object,
) -> tuple[str, tuple[int | float, ...]]:
    """Extract and strictly validate one persisted decision-time horizon tuple."""
    if (
        type(feature_vector_json) is not str
        or conn.execute("SELECT typeof(?)", (feature_vector_json,)).fetchone()[0]
        != "text"
    ):
        raise ValueError("invalid canonical pending horizon tuple")
    path = "$.canonical.ranking_inputs.counterfactual_horizons_s"
    try:
        json_valid, json_kind, extracted = conn.execute(
            """SELECT json_valid(?),
CASE WHEN json_valid(?) THEN json_type(?,?) END,
CASE WHEN json_valid(?) THEN json_extract(?,?) END""",
            (
                feature_vector_json,
                feature_vector_json, feature_vector_json, path,
                feature_vector_json, feature_vector_json, path,
            ),
        ).fetchone()
    except sqlite3.Error as exc:
        raise ValueError("invalid canonical pending horizon tuple") from exc
    if json_valid != 1 or json_kind != "array" or type(extracted) is not str:
        raise ValueError("invalid canonical pending horizon tuple")

    try:
        parsed = conn.execute(
            "SELECT key,value,type FROM json_each(?) ORDER BY key", (extracted,)
        ).fetchall()
    except sqlite3.Error as exc:
        raise ValueError("invalid canonical pending horizon tuple") from exc
    if not 1 <= len(parsed) <= 32:
        raise ValueError("invalid canonical pending horizon tuple")
    previous: int | float | None = None
    horizons: list[int | float] = []
    for expected_key, (key, horizon, json_type) in enumerate(parsed):
        finite = (
            type(horizon) in (int, float)
            and math.isfinite(horizon)
        )
        if (
            key != expected_key
            or json_type not in ("integer", "real")
            or not finite
            or horizon <= 0.0
            or (previous is not None and horizon <= previous)
        ):
            raise ValueError("invalid canonical pending horizon tuple")
        previous = horizon
        horizons.append(horizon)
    return extracted, tuple(horizons)


def _v5_canonical_pending_sources(
    conn: sqlite3.Connection,
) -> tuple[
    dict[int, tuple[int, str, tuple[int | float, ...]]],
    dict[int, tuple[object, ...]],
]:
    """Validate observations and return the exact pending source population."""
    pending: dict[int, tuple[int, str, tuple[int | float, ...]]] = {}
    observations: dict[int, tuple[object, ...]] = {}
    seen_decision_mints: set[tuple[int, str]] = set()
    rows = conn.execute(
        """SELECT o.id,o.decision_id,o.mint,o.observed_at,o.is_subject,
       o.is_canonical,o.eligible,o.start_price_sol,o.price_observed_at,
       o.price_source,o.unavailable_reason,d.id,d.feature_vector_json,
       typeof(o.mint),length(trim(o.mint))
FROM canonical_observations AS o
LEFT JOIN decisions AS d ON d.id=o.decision_id
ORDER BY o.id"""
    )
    for row in rows:
        (
            observation_id, decision_id, mint, observed_at, is_subject,
            is_canonical, eligible, start_price, price_observed_at,
            price_source, unavailable_reason, linked_decision_id, feature_json,
            mint_type, trimmed_mint_length,
        ) = row
        valid_common = (
            type(observation_id) is int
            and type(decision_id) is int
            and linked_decision_id == decision_id
            and mint_type == "text"
            and type(trimmed_mint_length) is int
            and 1 <= trimmed_mint_length <= 128
            and type(observed_at) in (int, float)
            and math.isfinite(observed_at)
            and 0.0 <= observed_at <= 4102444800.0
            and type(is_subject) is int and is_subject in (0, 1)
            and type(is_canonical) is int and is_canonical in (0, 1)
            and type(eligible) is int and eligible in (0, 1)
            and type(unavailable_reason) is str
        )
        if not valid_common:
            raise ValueError(
                f"invalid canonical observation decision link id={observation_id}"
            )
        identity = (decision_id, mint)
        if identity in seen_decision_mints:
            raise ValueError(
                f"invalid canonical observation decision link id={observation_id}"
            )
        seen_decision_mints.add(identity)

        available = unavailable_reason == ""
        if available:
            valid_availability = (
                type(start_price) in (int, float)
                and math.isfinite(start_price)
                and 0.0 < start_price <= 1e100
                and type(price_observed_at) in (int, float)
                and math.isfinite(price_observed_at)
                and 0.0 <= price_observed_at <= observed_at
                and price_source == "curve_snapshot"
            )
        else:
            valid_availability = (
                unavailable_reason in (
                    "start_price_missing", "start_price_stale",
                    "start_price_malformed",
                )
                and start_price is None
                and price_observed_at is None
                and price_source == ""
            )
        if not valid_availability:
            raise ValueError(f"invalid canonical observation id={observation_id}")

        observations[observation_id] = tuple(row[:11])
        if eligible == 1 and available:
            horizons_json, horizons = _v5_canonical_pending_horizons(
                conn, feature_json,
            )
            pending[observation_id] = decision_id, horizons_json, horizons
    return pending, observations


def _v5_canonical_outcome_bit(
    conn: sqlite3.Connection,
    outcome: tuple[object, ...],
    *,
    pending: dict[int, tuple[int, str, tuple[int | float, ...]]],
    observations: dict[int, tuple[object, ...]],
) -> tuple[int, int]:
    """Validate one reserved outcome and return its observation and horizon bit."""
    outcome_id, at, ref_id, pnl_sol, detail_json, p3_exit_trade_id = outcome
    observation = observations.get(ref_id) if type(ref_id) is int else None
    pending_source = pending.get(ref_id) if type(ref_id) is int else None
    if observation is None or pending_source is None:
        raise ValueError(f"invalid canonical outcome id={outcome_id}")
    if (
        type(outcome_id) is not int
        or type(at) not in (int, float)
        or not math.isfinite(at)
        or not 0.0 <= at <= 4102444800.0
        or type(pnl_sol) not in (int, float)
        or not math.isfinite(pnl_sol)
        or pnl_sol != 0.0
        or type(detail_json) is not str
        or len(detail_json) > 8192
        or p3_exit_trade_id is not None
    ):
        raise ValueError(f"invalid canonical outcome id={outcome_id}")

    class Pairs(list[tuple[str, object]]):
        pass

    def reject_constant(token: str) -> object:
        raise ValueError(f"invalid JSON constant: {token}")

    try:
        pairs = json.loads(
            detail_json, object_pairs_hook=Pairs, parse_constant=reject_constant,
        )
        if not isinstance(pairs, Pairs):
            raise ValueError("outcome detail must be an object")
        keys = [key for key, _ in pairs]
        expected_keys = {
            "horizon_s", "forward_return_pct", "price0",
            "price0_observed_at", "price_now", "price_now_observed_at",
            "terminal", "unavailable_reason",
        }
        if len(keys) != len(set(keys)) or set(keys) != expected_keys:
            raise ValueError("invalid outcome detail keys")
        detail = dict(pairs)
        canonical = json.dumps(
            detail, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid canonical outcome id={outcome_id}") from exc
    if detail_json != canonical:
        raise ValueError(f"invalid canonical outcome id={outcome_id}")

    try:
        horizon_type, horizon = conn.execute(
            "SELECT json_type(?,'$.horizon_s'),json_extract(?,'$.horizon_s')",
            (detail_json, detail_json),
        ).fetchone()
    except sqlite3.Error as exc:
        raise ValueError(f"invalid canonical outcome id={outcome_id}") from exc
    horizons = pending_source[2]
    horizon_finite = (
        type(horizon) in (int, float)
        and math.isfinite(horizon)
    )
    if (
        horizon_type not in ("integer", "real")
        or not horizon_finite
        or horizon <= 0.0
        or horizon not in horizons
    ):
        raise ValueError(f"invalid canonical outcome id={outcome_id}")
    horizon_index = horizons.index(horizon)
    observed_at = observation[3]
    if at <= observed_at or at < observed_at + horizon:
        raise ValueError(f"invalid canonical outcome id={outcome_id}")

    def number(value: object, low: float, high: float) -> bool:
        if type(value) not in (int, float):
            return False
        try:
            return math.isfinite(value) and low <= value <= high
        except OverflowError:
            return False

    price0 = detail["price0"]
    price0_observed_at = detail["price0_observed_at"]
    terminal = detail["terminal"]
    reason = detail["unavailable_reason"]
    if (
        not number(price0, 0.0, 1e100)
        or price0 <= 0.0
        or price0 != observation[7]
        or not number(price0_observed_at, 0.0, 4102444800.0)
        or price0_observed_at != observation[8]
        or (terminal is not None and terminal not in ("DEAD", "STALE", "GRADUATED"))
        or type(reason) is not str
    ):
        raise ValueError(f"invalid canonical outcome id={outcome_id}")

    forward_return = detail["forward_return_pct"]
    price_now = detail["price_now"]
    price_now_observed_at = detail["price_now_observed_at"]
    if reason:
        valid_result = (
            reason in ("journal_replay_gap", "graduated_no_price")
            and forward_return is None
            and price_now is None
            and price_now_observed_at is None
            and (
                (reason == "journal_replay_gap" and terminal is None)
                or (reason == "graduated_no_price" and terminal == "GRADUATED")
            )
        )
    else:
        valid_result = (
            number(forward_return, -1e100, 1e100)
            and number(price_now, 0.0, 1e100)
            and number(price_now_observed_at, observed_at, observed_at + horizon)
        )
        if valid_result and terminal in ("DEAD", "STALE"):
            valid_result = price_now == 0.0 and forward_return == -100.0
        elif valid_result and terminal in (None, "GRADUATED"):
            expected_return = 100.0 * (price_now - price0) / price0
            valid_result = (
                price_now > 0.0
                and math.isfinite(expected_return)
                and math.isclose(
                    forward_return, expected_return,
                    rel_tol=1e-12, abs_tol=1e-12,
                )
            )
        else:
            valid_result = False
    if not valid_result:
        raise ValueError(f"invalid canonical outcome id={outcome_id}")
    return ref_id, horizon_index


def _rebuild_v5_canonical_pending_current(conn: sqlite3.Connection) -> None:
    """Rebuild pending observations and replay canonical outcomes in ID order."""
    if not conn.in_transaction:
        raise RuntimeError(
            "canonical pending summary rebuild requires an active transaction"
        )

    conn.execute("DELETE FROM canonical_pending_current")
    pending, observations = _v5_canonical_pending_sources(conn)
    completed: dict[int, int] = {}
    for observation_id, (decision_id, horizons_json, horizons) in pending.items():
        full_mask = (1 << len(horizons)) - 1
        conn.execute(
            """INSERT INTO canonical_pending_current(
  observation_id,decision_id,horizons_json,full_mask,completed_mask)
VALUES(?,?,?,?,0)""",
            (observation_id, decision_id, horizons_json, full_mask),
        )
        completed[observation_id] = 0

    for outcome in conn.execute(
        """SELECT id,at,ref_id,pnl_sol,detail_json,p3_exit_trade_id
FROM outcomes WHERE ref_kind='canonical_observation' ORDER BY id"""
    ):
        observation_id, bit = _v5_canonical_outcome_bit(
            conn, tuple(outcome), pending=pending, observations=observations,
        )
        bit_value = 1 << bit
        if completed[observation_id] & bit_value:
            raise ValueError(f"duplicate canonical outcome id={outcome[0]}")
        completed[observation_id] |= bit_value
        conn.execute(
            "UPDATE canonical_pending_current SET completed_mask=? "
            "WHERE observation_id=?",
            (completed[observation_id], observation_id),
        )
    _verify_v5_canonical_pending_current(conn)


def _verify_v5_canonical_pending_current(conn: sqlite3.Connection) -> None:
    """Set-recompute and compare every canonical-pending operational row."""
    if not conn.in_transaction:
        raise RuntimeError(
            "canonical pending summary verification requires an active transaction"
        )
    try:
        _, observations = _v5_canonical_pending_sources(conn)
        pending = {}
        for observation_id, decision_id, feature_json in conn.execute(
            """SELECT o.id,o.decision_id,d.feature_vector_json
FROM canonical_observations AS o
JOIN decisions AS d ON d.id=o.decision_id
WHERE o.eligible=1 AND o.unavailable_reason=''
ORDER BY o.id"""
        ):
            horizons_json, horizons = _v5_canonical_pending_horizons(
                conn, feature_json,
            )
            pending[observation_id] = decision_id, horizons_json, horizons
        completed_horizons: dict[int, set[int]] = {
            observation_id: set() for observation_id in pending
        }
        for outcome in conn.execute(
            """SELECT id,at,ref_id,pnl_sol,detail_json,p3_exit_trade_id
FROM outcomes WHERE ref_kind='canonical_observation'"""
        ):
            observation_id, bit = _v5_canonical_outcome_bit(
                conn, tuple(outcome), pending=pending, observations=observations,
            )
            if bit in completed_horizons[observation_id]:
                raise ValueError("duplicate canonical outcome evidence")
            completed_horizons[observation_id].add(bit)
    except ValueError as exc:
        raise ValueError("canonical_pending_current source mismatch") from exc

    actual_rows = conn.execute(
        """SELECT observation_id,decision_id,horizons_json,full_mask,completed_mask
FROM canonical_pending_current ORDER BY observation_id"""
    )
    for observation_id, (decision_id, horizons_json, horizons) in pending.items():
        actual = actual_rows.fetchone()
        completed_mask = sum(
            1 << bit for bit in completed_horizons[observation_id]
        )
        expected = (
            observation_id, decision_id, horizons_json,
            (1 << len(horizons)) - 1, completed_mask,
        )
        if (
            actual is None
            or type(actual[0]) is not int
            or type(actual[1]) is not int
            or type(actual[2]) is not str
            or type(actual[3]) is not int
            or type(actual[4]) is not int
            or tuple(actual) != expected
        ):
            raise ValueError(
                f"canonical_pending_current mismatch observation_id={observation_id}"
            )
    if actual_rows.fetchone() is not None:
        raise ValueError("canonical_pending_current row set mismatch")


def _apply_v5_additive_columns(conn: sqlite3.Connection) -> None:
    for _, column, sql in V5_ADDITIVE_COLUMNS:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as exc:
            if str(exc).casefold() != f"duplicate column name: {column}".casefold():
                raise


def _v5_reference_connection() -> sqlite3.Connection:
    """Build a disposable exact v4 plus inert-v5 reference schema."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.create_function(
            "p3_fee_sum", 1, p3_fee_sum_json, deterministic=True,
        )
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA recursive_triggers=ON")
        conn.executescript(SCHEMA_V1)
        conn.executescript(_append_only_triggers())
        conn.execute(SCHEMA_V2)
        conn.execute(SCHEMA_V3)
        conn.executescript(SCHEMA_V4)
        conn.executescript(_append_only_triggers(EVIDENCE_TABLES))
        conn.execute("PRAGMA user_version=4")
        for _, sql in V5_TABLE_DDL:
            conn.execute(sql)
        _apply_v5_additive_columns(conn)
        for _, sql in V5_INDEX_DDL:
            conn.execute(sql)
        for _, sql in V5_EXPLICIT_TRIGGER_DDL:
            conn.execute(sql)
        for sql in _v5_immutable_triggers():
            conn.execute(sql)
        return conn
    except BaseException:
        conn.close()
        raise


def _build_v5_schema_manifest() -> tuple[tuple[str, str, str, str], ...]:
    """Capture SQLite's stored form of every explicit v5 schema object."""
    ordered = (
        *(("table", name) for name, _ in V5_TABLE_DDL),
        *(("index", name) for name, _ in V5_INDEX_DDL),
        *(("trigger", name) for name, _ in V5_EXPLICIT_TRIGGER_DDL),
        *(("trigger", sql.split()[5]) for sql in _v5_immutable_triggers()),
    )
    if len(ordered) != 70 or len({name for _, name in ordered}) != len(ordered):
        raise RuntimeError("invalid v5 schema inventory")
    conn = _v5_reference_connection()
    try:
        manifest = []
        for expected_type, name in ordered:
            rows = conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name=?",
                (name,),
            ).fetchall()
            if len(rows) != 1 or rows[0][0] != expected_type or rows[0][3] is None:
                raise RuntimeError(f"invalid v5 reference schema object {name!r}")
            manifest.append(tuple(rows[0]))
        return tuple(manifest)
    finally:
        conn.close()


def _build_v5_autoindex_manifest() -> tuple[
    tuple[str, str, str, None], ...
]:
    """Capture exact implicit indexes owned by the eleven new v5 tables."""
    conn = _v5_reference_connection()
    try:
        new_tables = tuple(name for name, _ in V5_TABLE_DDL)
        marks = ",".join("?" for _ in new_tables)
        manifest = tuple(
            tuple(row) for row in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema "  # nosec B608
                f"WHERE type='index' AND tbl_name IN ({marks}) AND sql IS NULL "
                "ORDER BY rowid",
                new_tables,
            )
        )
        if (
            len(manifest) != 13
            or len({row[1] for row in manifest}) != len(manifest)
            or any(
                row[0] != "index"
                or not row[1].startswith("sqlite_autoindex_")
                or row[2] not in new_tables
                or row[3] is not None
                for row in manifest
            )
        ):
            raise RuntimeError("invalid v5 reference autoindex inventory")
        return manifest
    finally:
        conn.close()


def _build_v5_additive_column_contract() -> tuple[
    tuple[
        str,
        str,
        tuple[object, ...],
        tuple[tuple[object, ...], ...],
        tuple[tuple[str, str], ...],
    ], ...
]:
    """Capture exact metadata and definitions for ten additive columns."""
    conn = _v5_reference_connection()
    try:
        contract = []
        for table, column, _ in V5_ADDITIVE_COLUMNS:
            table_rows = [
                tuple(row) for row in conn.execute(f"PRAGMA table_info({table})")
                if row[1] == column
            ]
            if len(table_rows) != 1:
                raise RuntimeError(
                    f"invalid v5 reference additive column {table}.{column}"
                )
            foreign_keys = tuple(
                tuple(row) for row in conn.execute(f"PRAGMA foreign_key_list({table})")
                if row[3] == column
            )
            stored = conn.execute(
                "SELECT sql FROM sqlite_schema "
                "WHERE type='table' AND name=?",
                (table,),
            ).fetchall()
            if len(stored) != 1 or type(stored[0][0]) is not str:
                raise RuntimeError(
                    f"invalid v5 reference additive table {table!r}"
                )
            definition = _sqlite_column_definition_tokens(
                stored[0][0], column,
            )
            contract.append((
                table, column, table_rows[0], foreign_keys, definition,
            ))
        return tuple(contract)
    finally:
        conn.close()


def _sqlite_foreign_key_semantics(
    row: tuple[object, ...],
) -> tuple[int, str, str, str | None, str, str, str]:
    """Normalize one PRAGMA foreign_key_list row without its unstable id."""
    if (
        len(row) < 8
        or type(row[1]) is not int
        or type(row[2]) is not str
        or type(row[3]) is not str
        or row[4] is not None and type(row[4]) is not str
        or any(type(row[position]) is not str for position in (5, 6, 7))
    ):
        raise ValueError("malformed SQLite foreign key metadata")
    return (
        row[1], row[2].lower(), row[3].lower(),
        row[4].lower() if isinstance(row[4], str) else None,
        row[5].lower(), row[6].lower(), row[7].lower(),
    )


def _v5_affected_legacy_structure(
    conn: sqlite3.Connection,
) -> tuple[
    tuple[
        str,
        tuple[str, ...],
        frozenset[tuple[str, str]],
        tuple[tuple[int, str, str, str | None, str, str, str], ...],
    ], ...
]:
    """Replay affected table roots and capture bounded resolved structure."""
    tables = tuple(dict.fromkeys(
        table for table, _column, _sql in V5_ADDITIVE_COLUMNS
    ))
    function_specs = _v5_caller_function_specs(conn)
    clone = sqlite3.connect(":memory:")
    try:
        _install_v5_clone_functions(clone, function_specs)
        clone.create_function(
            "p3_fee_sum", 1, p3_fee_sum_json, deterministic=True,
        )
        structure = []
        for table in tables:
            stored = conn.execute(
                "SELECT sql FROM sqlite_schema "
                "WHERE type='table' AND name=?",
                (table,),
            ).fetchall()
            if len(stored) != 1 or type(stored[0][0]) is not str:
                raise ValueError(f"invalid schema table {table!r}")
            columns = tuple(
                str(row[1]).lower() for row in conn.execute(
                    f"PRAGMA table_xinfo({_sqlite_quote_identifier(table)})"
                )
                if len(row) >= 7 and isinstance(row[1], str)
            )
            if not columns or len(columns) != len(set(columns)):
                raise ValueError(f"invalid schema columns on {table!r}")
            events = []

            def authorizer(action, first, second, _database, _source):
                events.append((action, first, second))
                return sqlite3.SQLITE_OK

            clone.set_authorizer(authorizer)
            try:
                _execute_with_stubbed_functions(clone, stored[0][0])
            finally:
                clone.set_authorizer(None)
            dependencies = frozenset(
                (table.lower(), second.lower())
                for action, first, second in events
                if action == sqlite3.SQLITE_READ
                and isinstance(first, str)
                and first.lower() == table.lower()
                and isinstance(second, str)
            )
            foreign_keys = tuple(
                _sqlite_foreign_key_semantics(tuple(row))
                for row in conn.execute(
                    f"PRAGMA foreign_key_list("
                    f"{_sqlite_quote_identifier(table)})"
                )
            )
            structure.append((table, columns, dependencies, foreign_keys))
        return tuple(structure)
    except sqlite3.Error as exc:
        raise ValueError("malformed v5-affected table structure") from exc
    finally:
        clone.close()


def _build_v5_affected_legacy_structure_contract() -> tuple[
    tuple[
        str,
        tuple[str, ...],
        frozenset[tuple[str, str]],
        tuple[tuple[int, str, str, str | None, str, str, str], ...],
    ], ...
]:
    """Capture exact reference semantics for affected legacy table roots."""
    conn = _v5_reference_connection()
    try:
        return _v5_affected_legacy_structure(conn)
    finally:
        conn.close()


def _build_v5_legacy_autoindex_manifest() -> tuple[
    tuple[str, str, str, None], ...
]:
    """Capture legitimate implicit indexes on additive legacy tables."""
    conn = _v5_reference_connection()
    try:
        tables = tuple(dict.fromkeys(table for table, _, _ in V5_ADDITIVE_COLUMNS))
        marks = ",".join("?" for _ in tables)
        rows = tuple(
            tuple(row) for row in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema "  # nosec B608
                f"WHERE type='index' AND lower(tbl_name) IN ({marks}) "
                "AND sql IS NULL "
                "ORDER BY rowid",
                tables,
            )
        )
        if len(rows) != 1 or len({row[1].lower() for row in rows}) != len(rows):
            raise RuntimeError("invalid v5 affected-legacy autoindex inventory")
        return rows
    finally:
        conn.close()


def _sqlite_quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_ddl_tokens(
    sql: str, *, preserve_strings: bool = False,
) -> tuple[tuple[str, str], ...]:
    """Tokenize SQLite DDL for dependency and call-shape resolution."""
    tokens = []
    index = 0
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if char == "'":
            index += 1
            value = []
            while index < len(sql):
                if sql[index] != "'":
                    value.append(sql[index])
                    index += 1
                elif index + 1 < len(sql) and sql[index + 1] == "'":
                    value.append("'")
                    index += 2
                else:
                    index += 1
                    break
            else:
                raise ValueError("unterminated SQLite string")
            literal = "".join(value)
            tokens.append((
                "string", literal if preserve_strings else literal.lower(),
            ))
            continue
        if char == "-" and following == "-":
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline + 1
            continue
        if char == "/" and following == "*":
            end = sql.find("*/", index + 2)
            if end < 0:
                raise ValueError("unterminated SQLite comment")
            index = end + 2
            continue
        if char in ('"', "`", "["):
            closing = "]" if char == "[" else char
            index += 1
            identifier = []
            while index < len(sql):
                current = sql[index]
                if current != closing:
                    identifier.append(current)
                    index += 1
                elif (
                    closing != "]"
                    and index + 1 < len(sql)
                    and sql[index + 1] == closing
                ):
                    identifier.append(closing)
                    index += 2
                else:
                    index += 1
                    break
            else:
                raise ValueError("unterminated SQLite identifier")
            tokens.append(("identifier", "".join(identifier).lower()))
            continue
        if char == "_" or char.isalpha() or ord(char) >= 128:
            start = index
            index += 1
            while index < len(sql) and (
                sql[index] in "_$"
                or sql[index].isalnum()
                or ord(sql[index]) >= 128
            ):
                index += 1
            tokens.append(("identifier", sql[start:index].lower()))
            continue
        if char in "(),.;":
            tokens.append(("punctuation", char))
        elif not char.isspace():
            tokens.append(("value", char))
        index += 1
    return tuple(tokens)


def _sqlite_insert_dependencies(
    clone: sqlite3.Connection, sql: str,
) -> set[tuple[str, str]]:
    """Resolve explicit INSERT destinations after SQLite has accepted the DDL."""
    tokens = _sqlite_ddl_tokens(sql)
    dependencies = set()
    for position, token in enumerate(tokens):
        if token not in (
            ("identifier", "insert"), ("identifier", "replace"),
        ):
            continue
        cursor = position + 1
        if cursor < len(tokens) and tokens[cursor] == ("identifier", "or"):
            cursor += 2
        if cursor >= len(tokens) or tokens[cursor] != ("identifier", "into"):
            continue
        cursor += 1
        if cursor >= len(tokens) or tokens[cursor][0] not in (
            "identifier", "string",
        ):
            raise ValueError("malformed SQLite INSERT destination")
        table = tokens[cursor][1]
        cursor += 1
        if cursor + 1 < len(tokens) and tokens[cursor] == ("punctuation", "."):
            table = tokens[cursor + 1][1]
            cursor += 2
        if cursor < len(tokens) and tokens[cursor] == ("identifier", "as"):
            cursor += 1
            if cursor >= len(tokens) or tokens[cursor][0] not in (
                "identifier", "string",
            ):
                raise ValueError("malformed SQLite INSERT alias")
            cursor += 1
        columns = tuple(
            str(row[1]).lower() for row in clone.execute(
                f"PRAGMA table_info({_sqlite_quote_identifier(table)})"
            )
        )
        if cursor >= len(tokens) or tokens[cursor] != ("punctuation", "("):
            dependencies.update((table, column) for column in columns)
            continue
        cursor += 1
        while cursor < len(tokens) and tokens[cursor] != ("punctuation", ")"):
            if tokens[cursor][0] not in ("identifier", "string"):
                raise ValueError("malformed SQLite INSERT column list")
            dependencies.add((table, tokens[cursor][1]))
            cursor += 1
            if cursor < len(tokens) and tokens[cursor] == ("punctuation", ","):
                cursor += 1
    return dependencies


def _sqlite_trigger_event(
    sql: str, table: str,
) -> tuple[str, tuple[str, ...]]:
    """Return a trigger's declared operation and optional UPDATE OF columns."""
    tokens = _sqlite_ddl_tokens(sql)
    table_key = table.lower()
    on_position = None
    for position, token in enumerate(tokens):
        if token != ("identifier", "on") or position + 1 >= len(tokens):
            continue
        target_position = position + 1
        if (
            target_position + 2 < len(tokens)
            and tokens[target_position][0] in ("identifier", "string")
            and tokens[target_position + 1] == ("punctuation", ".")
        ):
            target_position += 2
        if (
            tokens[target_position][0] in ("identifier", "string")
            and tokens[target_position][1] == table_key
        ):
            on_position = position
            break
    if on_position is None:
        raise ValueError("malformed SQLite trigger target")

    for position in range(on_position - 1):
        if (
            tokens[position] != ("identifier", "update")
            or tokens[position + 1] != ("identifier", "of")
        ):
            continue
        event_tokens = tokens[position + 2:on_position]
        columns = []
        expect_identifier = True
        for token in event_tokens:
            if expect_identifier and token[0] in ("identifier", "string"):
                columns.append(token[1])
                expect_identifier = False
            elif not expect_identifier and token == ("punctuation", ","):
                expect_identifier = True
            else:
                raise ValueError("malformed SQLite UPDATE OF event")
        if not columns or expect_identifier:
            raise ValueError("malformed SQLite UPDATE OF event")
        return "update", tuple(columns)
    if on_position and tokens[on_position - 1] in (
        ("identifier", "insert"),
        ("identifier", "update"),
        ("identifier", "delete"),
    ):
        return tokens[on_position - 1][1], ()
    raise ValueError("malformed SQLite trigger event")


def _sqlite_trigger_update_of_dependencies(
    sql: str, table: str,
) -> set[tuple[str, str]]:
    """Return columns named by a trigger's explicit UPDATE OF event."""
    operation, columns = _sqlite_trigger_event(sql, table)
    if operation != "update":
        return set()
    return {(table.lower(), column) for column in columns}


def _sqlite_indexed_by_names(sql: str) -> set[str]:
    """Return exact index hints from syntactic SQLite INDEXED BY clauses."""
    tokens = _sqlite_ddl_tokens(sql)
    return {
        tokens[position + 2][1]
        for position in range(len(tokens) - 2)
        if tokens[position] == ("identifier", "indexed")
        and tokens[position + 1] == ("identifier", "by")
        and tokens[position + 2][0] in ("identifier", "string")
    }


def _sqlite_stored_ddl_type(sql: str) -> str:
    """Infer a sqlite_schema object's exact kind from its stored CREATE DDL."""
    tokens = _sqlite_ddl_tokens(sql)
    if not tokens or tokens[0] != ("identifier", "create"):
        raise ValueError("malformed stored SQLite DDL")
    cursor = 1
    if cursor < len(tokens) and tokens[cursor] == ("identifier", "virtual"):
        cursor += 1
        if cursor < len(tokens) and tokens[cursor] == (
            "identifier", "table",
        ):
            return "table"
        raise ValueError("malformed stored SQLite virtual-table DDL")
    if cursor < len(tokens) and tokens[cursor] == ("identifier", "unique"):
        cursor += 1
        if cursor < len(tokens) and tokens[cursor] == (
            "identifier", "index",
        ):
            return "index"
        raise ValueError("malformed stored SQLite unique-index DDL")
    if cursor < len(tokens) and tokens[cursor] in (
        ("identifier", "index"),
        ("identifier", "trigger"),
        ("identifier", "view"),
        ("identifier", "table"),
    ):
        return tokens[cursor][1]
    raise ValueError("malformed stored SQLite DDL kind")


def _sqlite_stored_table_name(sql: str) -> str:
    """Return the normalized table identifier declared by stored CREATE DDL."""
    tokens = _sqlite_ddl_tokens(sql)
    if not tokens or tokens[0] != ("identifier", "create"):
        raise ValueError("malformed stored SQLite table DDL")
    cursor = 1
    if cursor < len(tokens) and tokens[cursor] == ("identifier", "virtual"):
        cursor += 1
    if cursor >= len(tokens) or tokens[cursor] != ("identifier", "table"):
        raise ValueError("malformed stored SQLite table DDL")
    cursor += 1
    if tokens[cursor:cursor + 3] == (
        ("identifier", "if"),
        ("identifier", "not"),
        ("identifier", "exists"),
    ):
        cursor += 3
    if cursor >= len(tokens) or tokens[cursor][0] not in (
        "identifier", "string",
    ):
        raise ValueError("malformed stored SQLite table name")
    declared_name = tokens[cursor][1]
    cursor += 1
    if cursor < len(tokens) and tokens[cursor] == ("punctuation", "."):
        cursor += 1
        if cursor >= len(tokens) or tokens[cursor][0] not in (
            "identifier", "string",
        ):
            raise ValueError("malformed stored SQLite table name")
        declared_name = tokens[cursor][1]
    return declared_name


def _sqlite_function_calls(
    sql: str,
) -> dict[str, set[tuple[int, bool]]]:
    """Return function call arities and whether each call uses OVER."""
    tokens = _sqlite_ddl_tokens(sql)
    calls: dict[str, set[tuple[int, bool]]] = {}
    for position in range(len(tokens) - 1):
        if (
            tokens[position][0] != "identifier"
            or tokens[position + 1] != ("punctuation", "(")
        ):
            continue
        depth = 1
        cursor = position + 2
        commas = 0
        has_argument = False
        while cursor < len(tokens) and depth:
            token = tokens[cursor]
            if token == ("punctuation", "("):
                if depth == 1:
                    has_argument = True
                depth += 1
            elif token == ("punctuation", ")"):
                depth -= 1
            elif depth == 1 and token == ("punctuation", ","):
                commas += 1
            elif depth == 1:
                has_argument = True
            cursor += 1
        if depth:
            raise ValueError("unterminated SQLite function call")
        following = cursor
        if (
            following < len(tokens)
            and tokens[following] == ("identifier", "filter")
            and following + 1 < len(tokens)
            and tokens[following + 1] == ("punctuation", "(")
        ):
            following += 2
            filter_depth = 1
            while following < len(tokens) and filter_depth:
                if tokens[following] == ("punctuation", "("):
                    filter_depth += 1
                elif tokens[following] == ("punctuation", ")"):
                    filter_depth -= 1
                following += 1
            if filter_depth:
                raise ValueError("unterminated SQLite FILTER clause")
        is_window = (
            following < len(tokens)
            and tokens[following] == ("identifier", "over")
        )
        arity = commas + 1 if has_argument else 0
        calls.setdefault(tokens[position][1], set()).add((arity, is_window))
    return calls


def _execute_with_stubbed_functions(
    conn: sqlite3.Connection, statement: str, *, source_sql: str | None = None,
) -> list[sqlite3.Row] | list[tuple[object, ...]]:
    """Compile clone-only SQL, stubbing caller UDFs that are absent there."""
    calls = _sqlite_function_calls(source_sql or statement)
    scalar_stubs: dict[str, set[int]] = {}

    class StubWindowFunction:
        def step(self, *_args):
            pass

        def value(self):
            return 0

        def inverse(self, *_args):
            pass

        def finalize(self):
            return 0

    for _ in range(32):
        try:
            return conn.execute(statement).fetchall()
        except sqlite3.OperationalError as exc:
            prefix = "no such function: "
            message = str(exc)
            collation_prefix = "no such collation sequence: "
            if message.startswith(collation_prefix):
                collation = message[len(collation_prefix):]
                if not collation or len(collation) > 255 or "\x00" in collation:
                    raise
                conn.create_collation(
                    collation,
                    lambda left, right: (left > right) - (left < right),
                )
                continue
            window_suffix = "() may not be used as a window function"
            if message.endswith(window_suffix):
                function = message[:-len(window_suffix)]
                function_key = function.lower()
                window_arities = {
                    arity for arity, is_window in calls.get(function_key, set())
                    if is_window
                }
                if function_key not in scalar_stubs or not window_arities:
                    raise
                for arity in window_arities:
                    conn.create_window_function(
                        function, arity, StubWindowFunction,
                    )
                continue
            if not message.startswith(prefix):
                raise
            function = message[len(prefix):]
            if not function or "\x00" in function:
                raise
            function_key = function.lower()
            arities = {
                arity for arity, _ in calls.get(function_key, set())
            } or {-1}
            for arity in arities:
                conn.create_function(
                    function, arity, lambda *_args: 0, deterministic=True,
                )
            scalar_stubs.setdefault(function_key, set()).update(arities)
    raise sqlite3.OperationalError("too many missing SQLite functions")


def _v5_caller_function_specs(
    conn: sqlite3.Connection,
) -> tuple[tuple[str, str, int], ...]:
    """Read exact caller-defined SQLite function registrations."""
    specs = []
    for row in conn.execute("PRAGMA function_list"):
        if len(row) < 5 or row[1] != 0:
            continue
        name, kind, arity = row[0], row[2], row[4]
        if (
            type(name) is not str
            or not name
            or len(name) > 255
            or "\x00" in name
            or kind not in ("s", "a", "w")
            or type(arity) is not int
            or arity < -1
            or arity > 127
        ):
            raise ValueError("invalid SQLite function registry")
        specs.append((name, kind, arity))
    return tuple(specs)


def _install_v5_clone_functions(
    conn: sqlite3.Connection,
    specs: tuple[tuple[str, str, int], ...],
) -> None:
    """Install inert exact-form caller UDFs on a private clone."""
    class StubAggregate:
        def step(self, *_args):
            pass

        def finalize(self):
            return 0

    class StubWindowFunction(StubAggregate):
        def value(self):
            return 0

        def inverse(self, *_args):
            pass

    for name, kind, arity in specs:
        if kind == "s":
            conn.create_function(
                name, arity, lambda *_args: 0, deterministic=True,
            )
        elif kind == "a":
            conn.create_aggregate(name, arity, StubAggregate)
        else:
            conn.create_window_function(name, arity, StubWindowFunction)


def _sqlite_table_definition_segments(sql: str) -> tuple[str, ...]:
    """Split a stored CREATE TABLE body without flattening quoted expressions."""
    segments = []
    depth = 0
    segment_start = None
    index = 0
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if char == "-" and following == "-":
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline + 1
            continue
        if char == "/" and following == "*":
            end = sql.find("*/", index + 2)
            if end < 0:
                raise ValueError("unterminated SQLite table comment")
            index = end + 2
            continue
        if char in ("'", '"', "`", "["):
            closing = "]" if char == "[" else char
            index += 1
            while index < len(sql):
                if sql[index] != closing:
                    index += 1
                elif (
                    closing != "]"
                    and index + 1 < len(sql)
                    and sql[index + 1] == closing
                ):
                    index += 2
                else:
                    index += 1
                    break
            else:
                raise ValueError("unterminated SQLite table quote")
            continue
        if char == "(":
            depth += 1
            if depth == 1:
                segment_start = index + 1
        elif char == ")" and depth:
            depth -= 1
            if depth == 0:
                if segment_start is None:
                    raise ValueError("malformed SQLite table definition")
                segment = sql[segment_start:index].strip()
                if segment:
                    segments.append(segment)
                return tuple(segments)
        elif char == "," and depth == 1:
            if segment_start is None:
                raise ValueError("malformed SQLite table definition")
            segment = sql[segment_start:index].strip()
            if not segment:
                raise ValueError("empty SQLite table definition")
            segments.append(segment)
            segment_start = index + 1
        index += 1
    raise ValueError("malformed SQLite table definition")


def _sqlite_column_definition_tokens(
    sql: str, column: str,
) -> tuple[tuple[str, str], ...]:
    """Return one full column definition with insignificant text removed."""
    definitions = []
    column_key = column.lower()
    for segment in _sqlite_table_definition_segments(sql):
        tokens = _sqlite_ddl_tokens(segment, preserve_strings=True)
        if (
            tokens
            and tokens[0][0] in ("identifier", "string")
            and tokens[0][1].lower() == column_key
        ):
            definitions.append((
                ("identifier", tokens[0][1].lower()), *tokens[1:],
            ))
    if len(definitions) != 1:
        raise ValueError(f"column definition mismatch {column!r}")
    return definitions[0]


def _sqlite_generated_column_definitions(
    sql: str, generated_columns: set[str],
) -> tuple[tuple[str, str], ...]:
    """Return exact column definitions for generated columns in stored DDL."""
    definitions = {}
    for segment in _sqlite_table_definition_segments(sql):
        tokens = _sqlite_ddl_tokens(segment)
        if not tokens or tokens[0][0] not in ("identifier", "string"):
            continue
        name = tokens[0][1]
        has_generated_expression = any(
            tokens[position] == ("identifier", "as")
            and tokens[position + 1] == ("punctuation", "(")
            for position in range(len(tokens) - 1)
        )
        if name not in generated_columns or not has_generated_expression:
            continue
        if name in definitions:
            raise ValueError(f"duplicate generated column {name!r}")
        definitions[name] = segment
    if definitions.keys() != generated_columns:
        raise ValueError("generated column definition mismatch")
    return tuple(definitions.items())


def _sqlite_fts_external_content(
    sql: str,
) -> tuple[str, str | None, tuple[str, ...]] | None:
    """Return true FTS4/FTS5 external-content dependencies, if present."""
    tokens = _sqlite_ddl_tokens(sql)
    if len(tokens) < 6 or tokens[:3] != (
        ("identifier", "create"),
        ("identifier", "virtual"),
        ("identifier", "table"),
    ):
        return None
    # FTS3 treats content=... as a column declaration, not as the FTS4
    # external-content option. Including it here would invent a dependency.
    using_positions = [
        (position, tokens[position + 1][1])
        for position, token in enumerate(tokens)
        if token == ("identifier", "using")
        and position + 2 < len(tokens)
        and tokens[position + 1] in (
            ("identifier", "fts4"),
            ("identifier", "fts5"),
        )
        and tokens[position + 2] == ("punctuation", "(")
    ]
    if len(using_positions) != 1:
        return None
    _using_position, module = using_positions[0]
    options = {}
    fields = []
    for segment in _sqlite_table_definition_segments(sql):
        option = _sqlite_ddl_tokens(segment)
        if not option or option[0][0] not in ("identifier", "string"):
            continue
        if len(option) >= 2 and option[1] == ("value", "="):
            option_name = option[0][1]
            contextual_option = (
                option_name == "content"
                or module == "fts4" and option_name == "languageid"
                or module == "fts5" and option_name == "content_rowid"
            )
            if contextual_option:
                if (
                    len(option) != 3
                    or option[2][0] not in ("identifier", "string")
                ):
                    raise ValueError(
                        f"malformed {module.upper()} option {option_name!r}"
                    )
                option_value = option[2][1]
                if (
                    "\x00" in option_value
                    or "." in option_value
                    or not option_value and option_name != "content"
                ):
                    raise ValueError(
                        f"malformed {module.upper()} option {option_name!r}"
                    )
                if option_name in options:
                    raise ValueError(
                        f"duplicate {module.upper()} option "
                        f"{option_name!r}"
                    )
                options[option_name] = option_value
            continue
        if (
            module == "fts4"
            or len(option) == 1
            or len(option) == 2
            and option[1] == ("identifier", "unindexed")
        ):
            fields.append(option[0][1])
    content = options.get("content")
    if not content:
        return None
    if module == "fts4" and options.get("languageid"):
        fields.append(options["languageid"])
    return content, options.get("content_rowid"), tuple(fields)


def _v5_schema_skeleton_metadata(
    conn: sqlite3.Connection,
) -> tuple[
    tuple[
        tuple[
            str,
            tuple[str, ...],
            str | None,
            tuple[
                tuple[str, str, tuple[tuple[str, bool, str], ...]], ...
            ],
            bool,
            str | None,
            tuple[tuple[str, str], ...],
        ], ...
    ],
    tuple[tuple[object, ...], ...],
    tuple[tuple[object, ...], ...],
]:
    """Capture bounded schema shape without reading any table row payload."""
    tables = []
    for row in conn.execute("PRAGMA table_list"):
        schema, name, table_type, _columns, without_rowid = row[:5]
        if (
            schema != "main"
            or table_type == "view"
            or type(name) is not str
            or not name
            or name.lower().startswith("sqlite_")
        ):
            continue
        xinfo = tuple(
            tuple(column) for column in conn.execute(
                f"PRAGMA table_xinfo({_sqlite_quote_identifier(name)})"
            )
        )
        columns = tuple(
            str(column[1]) for column in xinfo if len(column) >= 7
        )
        primary_key = tuple(
            str(column[1]) for column in sorted(
                (column for column in xinfo if column[1] in columns and column[5]),
                key=lambda column: column[5],
            )
        )
        generated_columns = {
            str(column[1]).lower() for column in xinfo
            if len(column) >= 7 and column[6] in (2, 3)
        }
        exact_sql = None
        generated_definitions = ()
        if table_type == "table":
            stored = conn.execute(
                "SELECT sql FROM sqlite_schema "
                "WHERE type='table' AND name=?",
                (name,),
            ).fetchall()
            if len(stored) != 1 or type(stored[0][0]) is not str:
                raise ValueError(f"invalid schema table {name!r}")
            exact_sql = stored[0][0]
            generated_definitions = _sqlite_generated_column_definitions(
                exact_sql, generated_columns,
            )
        if not columns or (without_rowid and not primary_key):
            raise ValueError(f"invalid schema table {name!r}")
        implicit_indexes = []
        prefix = f"sqlite_autoindex_{name}_"
        for index in conn.execute(
            f"PRAGMA index_list({_sqlite_quote_identifier(name)})"
        ):
            if len(index) < 5 or index[3] not in ("u", "pk"):
                continue
            index_name = index[1]
            if (
                type(index_name) is not str
                or not index_name.startswith(prefix)
                or not index_name[len(prefix):].isdigit()
                or index[2] != 1
                or index[4] != 0
            ):
                raise ValueError(f"invalid schema index on {name!r}")
            key_columns = tuple(
                (str(detail[2]), bool(detail[3]), str(detail[4]))
                for detail in conn.execute(
                    f"PRAGMA index_xinfo({_sqlite_quote_identifier(index_name)})"
                )
                if len(detail) >= 6 and detail[5] == 1
            )
            if not key_columns or any(
                column not in columns or not collation
                for column, _descending, collation in key_columns
            ):
                raise ValueError(f"invalid schema index {index_name!r}")
            implicit_indexes.append((index_name, index[3], key_columns))
        implicit_indexes.sort(
            key=lambda index: int(index[0][len(prefix):])
        )
        has_primary_index = any(
            origin == "pk" for _index, origin, _columns in implicit_indexes
        )
        rowid_primary_key = None
        if primary_key and not has_primary_index:
            if (
                without_rowid
                or len(primary_key) != 1
                or next(
                    str(column[2]).upper() for column in xinfo
                    if str(column[1]) == primary_key[0]
                ) != "INTEGER"
            ):
                raise ValueError(f"invalid schema primary key {name!r}")
            rowid_primary_key = primary_key[0]
        tables.append((
            name, columns, rowid_primary_key, tuple(implicit_indexes),
            bool(without_rowid), exact_sql, generated_definitions,
        ))
    indexes = tuple(
        tuple(row) for row in conn.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE type='index' AND sql IS NOT NULL ORDER BY rowid"
        )
    )
    views = tuple(
        tuple(row) for row in conn.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE type='view' ORDER BY rowid"
        )
    )
    return tuple(tables), indexes, views


def _v5_schema_skeleton_clone(
    metadata: tuple[
        tuple[
            tuple[
                str,
                tuple[str, ...],
                str | None,
                tuple[
                    tuple[str, str, tuple[tuple[str, bool, str], ...]], ...
                ],
                bool,
                str | None,
                tuple[tuple[str, str], ...],
            ], ...
        ],
        tuple[tuple[object, ...], ...],
        tuple[tuple[object, ...], ...],
    ],
    candidate: tuple[object, ...],
    function_specs: tuple[tuple[str, str, int], ...],
) -> tuple[
    sqlite3.Connection,
    dict[tuple[str, str], set[tuple[str, str]]],
]:
    """Build an empty private schema sufficient to replay one candidate."""
    clone = sqlite3.connect(":memory:")
    try:
        _install_v5_clone_functions(clone, function_specs)
        clone.create_function(
            "p3_fee_sum", 1, p3_fee_sum_json, deterministic=True,
        )
        clone.execute("PRAGMA recursive_triggers=ON")
        tables, indexes, views = metadata
        generated_dependencies = {}
        for (
            name, columns, rowid_primary_key, implicit_indexes, without_rowid,
            exact_sql, generated_definitions,
        ) in tables:
            if exact_sql is not None:
                _execute_with_stubbed_functions(clone, exact_sql)
            else:
                definitions = []
                for column in columns:
                    definition = _sqlite_quote_identifier(column)
                    if column == rowid_primary_key:
                        definition += " INTEGER PRIMARY KEY"
                    definitions.append(definition)
                for _index_name, origin, key_columns in implicit_indexes:
                    terms = []
                    for column, descending, collation in key_columns:
                        term = (
                            f"{_sqlite_quote_identifier(column)} COLLATE "
                            f"{_sqlite_quote_identifier(collation)}"
                        )
                        if descending:
                            term += " DESC"
                        terms.append(term)
                    constraint = "PRIMARY KEY" if origin == "pk" else "UNIQUE"
                    definitions.append(f"{constraint}({','.join(terms)})")
                suffix = " WITHOUT ROWID" if without_rowid else ""
                _execute_with_stubbed_functions(
                    clone,
                    f"CREATE TABLE {_sqlite_quote_identifier(name)} "
                    f"({','.join(definitions)}){suffix}"
                )
            actual_implicit = {
                (index[1], index[3]) for index in clone.execute(
                    f"PRAGMA index_list({_sqlite_quote_identifier(name)})"
                )
                if len(index) >= 5 and index[3] in ("u", "pk")
            }
            expected_implicit = {
                (index_name, origin)
                for index_name, origin, _key_columns in implicit_indexes
            }
            if actual_implicit != expected_implicit:
                raise ValueError(f"malformed schema indexes on {name!r}")
            for generated_column, definition in generated_definitions:
                probe_name = "__v5_generated_column_probe"
                probe_columns = ",".join(
                    _sqlite_quote_identifier(column) for column in columns
                    if column.lower() != generated_column
                )
                events = []

                def authorizer(action, first, second, _database, _source):
                    events.append((action, first, second))
                    return sqlite3.SQLITE_OK

                clone.set_authorizer(authorizer)
                try:
                    _execute_with_stubbed_functions(
                        clone,
                        f"CREATE TEMP TABLE "
                        f"{_sqlite_quote_identifier(probe_name)} "
                        f"({probe_columns},{definition})",
                    )
                finally:
                    clone.set_authorizer(None)
                generated_dependencies[(name.lower(), generated_column)] = {
                    (name.lower(), second.lower())
                    for action, first, second in events
                    if action == sqlite3.SQLITE_READ
                    and isinstance(first, str)
                    and first.lower() == probe_name
                    and isinstance(second, str)
                }
                clone.execute(
                    f"DROP TABLE temp.{_sqlite_quote_identifier(probe_name)}"
                )

        candidate_type, candidate_name, _candidate_table, candidate_sql = candidate
        for index in indexes:
            if (
                candidate_type == "index"
                and isinstance(candidate_name, str)
                and isinstance(index[1], str)
                and index[1].lower() == candidate_name.lower()
            ):
                continue
            if type(index[3]) is not str or not index[3].strip():
                raise ValueError(f"malformed schema object {index[1]!r}")
            _execute_with_stubbed_functions(clone, index[3])
        for view in views:
            if (
                candidate_type == "view"
                and isinstance(candidate_name, str)
                and isinstance(view[1], str)
                and view[1].lower() == candidate_name.lower()
            ):
                continue
            if type(view[3]) is not str or not view[3].strip():
                raise ValueError(f"malformed schema object {view[1]!r}")
            _execute_with_stubbed_functions(clone, view[3])

        if (
            candidate_type in ("index", "trigger", "view")
            and type(candidate_name) is str
            and candidate_name
            and type(candidate_sql) is str
            and candidate_sql.strip()
        ):
            _execute_with_stubbed_functions(clone, candidate_sql)
        return clone, generated_dependencies
    except (sqlite3.Error, ValueError) as exc:
        clone.close()
        if isinstance(exc, ValueError):
            raise
        raise ValueError(f"malformed schema object {candidate[1]!r}") from exc


def _resolved_v5_schema_dependencies(
    clone: sqlite3.Connection,
    row: tuple[object, ...],
) -> set[tuple[str, str]]:
    """Replay one legacy-attached object and ask SQLite what it resolves."""
    schema_type, name, table, sql = row
    if (
        schema_type not in ("index", "trigger", "view")
        or type(name) is not str
        or not name
        or "\x00" in name
        or type(table) is not str
        or not table
        or "\x00" in table
        or type(sql) is not str
        or not sql.strip()
    ):
        raise ValueError(f"malformed schema object {name!r}")

    quoted_name = _sqlite_quote_identifier(name)
    dependencies: set[tuple[str, str]] = set()
    authorizer_events: list[tuple[int, str | None, str | None, str | None]] = []

    def authorizer(action, first, second, _database, source):
        authorizer_events.append((action, first, second, source))
        return sqlite3.SQLITE_OK

    try:
        clone.execute(f"DROP {schema_type.upper()} {quoted_name}")
        clone.set_authorizer(authorizer)
        try:
            _execute_with_stubbed_functions(clone, sql)
        finally:
            clone.set_authorizer(None)
        replayed = clone.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE lower(name)=lower(?)",
            (name,),
        ).fetchall()
    except sqlite3.Error as exc:
        clone.set_authorizer(None)
        raise ValueError(f"malformed schema object {name!r}") from exc
    if len(replayed) != 1 or tuple(replayed[0]) != row:
        raise ValueError(f"wrong schema object {name!r}")

    if schema_type == "index":
        dependencies.update(
            (first.lower(), second.lower())
            for action, first, second, _ in authorizer_events
            if action == sqlite3.SQLITE_READ
            and isinstance(first, str)
            and isinstance(second, str)
        )
        dependencies.update(_sqlite_insert_dependencies(clone, sql))
        return dependencies

    if schema_type == "view":
        view_events: list[
            tuple[int, str | None, str | None, str | None]
        ] = []

        def view_authorizer(action, first, second, _database, source):
            view_events.append((action, first, second, source))
            return sqlite3.SQLITE_OK

        clone.set_authorizer(view_authorizer)
        try:
            _execute_with_stubbed_functions(
                clone, f"EXPLAIN SELECT * FROM {quoted_name} LIMIT 0",  # nosec B608
                source_sql=sql,
            )
        except sqlite3.Error as exc:
            raise ValueError(f"malformed schema object {name!r}") from exc
        finally:
            clone.set_authorizer(None)
        dependencies.update(
            (first.lower(), second.lower())
            for action, first, second, source in view_events
            if action == sqlite3.SQLITE_READ
            and isinstance(first, str)
            and isinstance(second, str)
            and isinstance(source, str)
        )
        return dependencies

    for other_trigger in clone.execute(
        "SELECT name FROM sqlite_schema WHERE type='trigger' "
        "AND lower(name)<>lower(?)",
        (name,),
    ).fetchall():
        clone.execute(
            f"DROP TRIGGER {_sqlite_quote_identifier(other_trigger[0])}"
        )
    columns = {
        str(column[1]).lower(): str(column[1]) for column in clone.execute(
            f"PRAGMA table_info({_sqlite_quote_identifier(table)})"
        )
    }
    table_key = table.lower()
    table_list_row = next(
        (
            row for row in clone.execute("PRAGMA table_list")
            if str(row[0]).lower() == "main"
            and str(row[1]).lower() == table_key
        ),
        None,
    )
    if (
        table_list_row is not None
        and table_list_row[2] == "table"
        and table_list_row[4] == 0
    ):
        for rowid_alias in ("rowid", "_rowid_", "oid"):
            columns.setdefault(rowid_alias, rowid_alias)
    operation, event_columns = _sqlite_trigger_event(sql, table)
    if operation == "update" and any(
        column not in columns for column in event_columns
    ):
        raise ValueError(f"malformed schema object {name!r}")

    def probe(statement: str) -> None:
        events: list[tuple[int, str | None, str | None, str | None]] = []

        def probe_authorizer(action, first, second, _database, source):
            events.append((action, first, second, source))
            return sqlite3.SQLITE_OK

        clone.set_authorizer(probe_authorizer)
        try:
            _execute_with_stubbed_functions(clone, statement, source_sql=sql)
        finally:
            clone.set_authorizer(None)
        source_events = [
            event for event in events
            if isinstance(event[3], str)
        ]
        dependencies.update(
            (first.lower(), second.lower())
            for action, first, second, _ in source_events
            if action in (sqlite3.SQLITE_READ, sqlite3.SQLITE_UPDATE)
            and isinstance(first, str)
            and isinstance(second, str)
        )
        dependencies.update(
            (first.lower(), "*")
            for action, first, _, _ in source_events
            if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_DELETE)
            and isinstance(first, str)
        )
    quoted_table = _sqlite_quote_identifier(table)
    try:
        if operation == "insert":
            probe(f"EXPLAIN INSERT INTO {quoted_table} DEFAULT VALUES")
        elif operation == "update":
            update_columns = event_columns or tuple(columns)[:1]
            for column_key in update_columns:
                if column_key not in columns:
                    continue
                column = columns[column_key]
                quoted_column = _sqlite_quote_identifier(column)
                probe(
                    f"EXPLAIN UPDATE {quoted_table} "  # nosec B608
                    f"SET {quoted_column}={quoted_column}",
                )
        else:
            probe(f"EXPLAIN DELETE FROM {quoted_table}")  # nosec B608
    except sqlite3.DatabaseError as exc:
        clone.set_authorizer(None)
        raise ValueError(f"malformed schema object {name!r}") from exc
    dependencies.update(_sqlite_trigger_update_of_dependencies(sql, table))
    dependencies.update(_sqlite_insert_dependencies(clone, sql))
    return dependencies


def _attest_v5_schema_manifest(conn: sqlite3.Connection) -> None:
    """Read-only, fail-closed attestation of v5-owned schema and ALTER metadata."""
    expected = {
        row[1]: row for row in (
            *V5_SCHEMA_MANIFEST, *_V5_SCHEMA_AUTOINDEX_MANIFEST,
        )
    }
    if any(name != name.lower() for name in expected):
        raise RuntimeError("v5 schema inventory names must be normalized")
    names = tuple(expected)
    new_tables = tuple(name for name, _ in V5_TABLE_DDL)
    new_table_set = set(new_tables)
    v5_explicit_indexes = {name for name, _sql in V5_INDEX_DDL}
    additive_by_table: dict[str, set[str]] = {}
    for table, column, _ in V5_ADDITIVE_COLUMNS:
        additive_by_table.setdefault(table, set()).add(column)
    additive_pairs = {
        (table, column) for table, columns in additive_by_table.items()
        for column in columns
    }
    catalog_rows = [
        tuple(row) for row in conn.execute(
            "SELECT type,name,tbl_name,sql,lower(name),lower(tbl_name) "
            "FROM sqlite_schema ORDER BY rowid"
        )
    ]
    catalog_name_counts = {}
    for catalog_row in catalog_rows:
        if isinstance(catalog_row[4], str):
            catalog_name_counts[catalog_row[4]] = (
                catalog_name_counts.get(catalog_row[4], 0) + 1
            )
    legitimate_autoindexes = set()
    for (table,) in conn.execute(
        "SELECT name FROM sqlite_schema WHERE type='table'"
    ):
        if type(table) is not str or not table or "\x00" in table:
            continue
        for index in conn.execute(
            f"PRAGMA index_list({_sqlite_quote_identifier(table)})"
        ):
            if (
                len(index) >= 5
                and type(index[1]) is str
                and index[2] == 1
                and index[3] in ("u", "pk")
                and index[4] == 0
            ):
                expected_autoindex = ("index", index[1], table, None)
                if expected_autoindex in {
                    tuple(row) for row in conn.execute(
                        "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                        "WHERE name=?",
                        (index[1],),
                    )
                }:
                    legitimate_autoindexes.add(expected_autoindex)
    for catalog_row in catalog_rows:
        schema_type, name, table, sql = catalog_row[:4]
        if (
            type(name) is not str
            or not name
            or "\x00" in name
            or type(table) is not str
            or not table
            or "\x00" in table
        ):
            raise ValueError(f"malformed schema object {name!r}")
        if sql is None:
            if catalog_row[:4] not in legitimate_autoindexes:
                if name.lower() in expected:
                    if catalog_name_counts.get(name.lower(), 0) > 1:
                        raise ValueError(
                            f"duplicate schema object {name!r}"
                        )
                    raise ValueError(f"wrong schema object {name!r}")
                if (
                    table.lower() in new_table_set
                    or name.lower().startswith(("p3_", "canonical_"))
                ):
                    raise ValueError(
                        f"extra v5-owned schema object {name!r}"
                    )
                raise ValueError(f"malformed schema object {name!r}")
            continue
        if type(sql) is not str or not sql.strip():
            raise ValueError(f"malformed schema object {name!r}")
        try:
            ddl_type = _sqlite_stored_ddl_type(sql)
        except ValueError as exc:
            raise ValueError(f"malformed schema object {name!r}") from exc
        if schema_type != ddl_type:
            raise ValueError(f"malformed schema object {name!r}")
        if ddl_type == "table":
            try:
                declared_table = _sqlite_stored_table_name(sql)
            except ValueError as exc:
                raise ValueError(
                    f"malformed schema object {name!r}"
                ) from exc
            if name != table or declared_table != name.lower():
                raise ValueError(f"malformed schema object {name!r}")
    foreign_key_owned_tables = set()
    module_owned_tables = set()
    module_dependencies = {}
    for table, table_sql in conn.execute(
        "SELECT name,sql FROM sqlite_schema WHERE type='table'"
    ):
        if (
            type(table) is not str
            or table.lower() in new_table_set
            or table.lower() in additive_by_table
        ):
            continue
        for foreign_key in conn.execute(
            f"PRAGMA foreign_key_list({_sqlite_quote_identifier(table)})"
        ):
            target = (
                foreign_key[2].lower()
                if isinstance(foreign_key[2], str) else None
            )
            target_column = (
                foreign_key[4].lower()
                if isinstance(foreign_key[4], str) else None
            )
            if (
                target in new_table_set
                or (target, target_column) in additive_pairs
            ):
                foreign_key_owned_tables.add(table.lower())
                break
        try:
            module_target = (
                _sqlite_fts_external_content(table_sql)
                if isinstance(table_sql, str) else None
            )
        except ValueError as exc:
            raise ValueError(f"malformed schema object {table!r}") from exc
        if module_target is not None:
            content_table, content_rowid, fields = module_target
            dependencies = {
                (content_table, field) for field in fields
            }
            if content_rowid is not None:
                dependencies.add((content_table, content_rowid))
            module_dependencies[table.lower()] = dependencies
            if (
                content_table in new_table_set
                or dependencies & additive_pairs
            ):
                module_owned_tables.add(table.lower())
    legacy_autoindexes = {
        row[1].lower(): row for row in _V5_AFFECTED_LEGACY_AUTOINDEX_MANIFEST
    }
    rows = []
    legacy_candidates = []
    module_candidates = []
    for row in catalog_rows:
        name_key = row[4]
        table_key = row[5]
        owned = (
            name_key in expected
            or table_key in new_tables
            or table_key in foreign_key_owned_tables
            or table_key in module_owned_tables
            or (
                isinstance(name_key, str)
                and name_key.startswith(("p3_", "canonical_"))
                and not (
                    table_key in additive_by_table
                    and name_key in additive_by_table[table_key]
                )
            )
        )
        if not owned and name_key in legacy_autoindexes:
            if row[:4] != legacy_autoindexes[name_key]:
                raise ValueError(f"wrong schema object {name_key!r}")
            continue
        if (
            not owned
            and table_key in additive_by_table
            and row[0] == "table"
            and name_key == table_key
        ):
            continue
        if (
            not owned
            and row[0] == "table"
            and name_key == table_key
            and table_key in module_dependencies
        ):
            module_candidates.append(row[:4])
            continue
        if (
            not owned
            and isinstance(row[0], str)
            and row[0].lower() == "index"
            and isinstance(row[3], str)
        ):
            legacy_candidates.append(row[:4])
            continue
        if not owned and table_key in additive_by_table:
            legacy_candidates.append(row[:4])
            continue
        if (
            not owned
            and isinstance(row[0], str)
            and row[0].lower() in ("trigger", "view")
        ):
            legacy_candidates.append(row[:4])
            continue
        if owned:
            rows.append(row[:5])

    def validate_owned_rows() -> None:
        actual_by_name: dict[str, list[tuple[object, ...]]] = {}
        for owned_row in rows:
            actual_by_name.setdefault(str(owned_row[4]), []).append(owned_row[:4])
        duplicate = next(
            (
                name for name, matches in actual_by_name.items()
                if len(matches) != 1
            ),
            None,
        )
        if duplicate is not None:
            raise ValueError(f"duplicate schema object {duplicate!r}")
        wrong = next(
            (
                name for name, expected_row in expected.items()
                if name in actual_by_name
                and actual_by_name[name][0] != expected_row
            ),
            None,
        )
        if wrong is not None:
            raise ValueError(f"wrong schema object {wrong!r}")
        missing = next(
            (name for name in names if name not in actual_by_name), None,
        )
        if missing is not None:
            raise ValueError(f"missing schema object {missing!r}")
        extra = next(
            (name for name in actual_by_name if name not in expected), None,
        )
        if extra is not None:
            raise ValueError(f"extra v5-owned schema object {extra!r}")

    validate_owned_rows()
    if legacy_candidates or module_candidates:
        clone_function_specs = _v5_caller_function_specs(conn)
        try:
            skeleton_metadata = _v5_schema_skeleton_metadata(conn)
        except sqlite3.Error as exc:
            raise ValueError("malformed v5-affected schema") from exc
        generated_dependencies = None

        def expand_generated_dependencies(
            dependencies: set[tuple[str, str]],
        ) -> None:
            pending_dependencies = list(dependencies)
            while pending_dependencies:
                dependency = pending_dependencies.pop()
                for transitive in (generated_dependencies or {}).get(
                    dependency, set(),
                ):
                    if transitive not in dependencies:
                        dependencies.add(transitive)
                        pending_dependencies.append(transitive)

        for candidate in legacy_candidates:
            clone, generated_dependencies = _v5_schema_skeleton_clone(
                skeleton_metadata, candidate, clone_function_specs,
            )
            try:
                dependencies = _resolved_v5_schema_dependencies(clone, candidate)
                expand_generated_dependencies(dependencies)
                if (
                    dependencies & additive_pairs
                    or any(table in new_table_set for table, _ in dependencies)
                    or (
                        isinstance(candidate[3], str)
                        and _sqlite_indexed_by_names(candidate[3])
                        & v5_explicit_indexes
                    )
                ):
                    name_key = str(candidate[1]).lower()
                    rows.append((*candidate, name_key))
            finally:
                clone.close()
        if module_candidates and generated_dependencies is None:
            clone, generated_dependencies = _v5_schema_skeleton_clone(
                skeleton_metadata, module_candidates[0], clone_function_specs,
            )
            clone.close()
        for candidate in module_candidates:
            dependencies = set(
                module_dependencies[str(candidate[1]).lower()]
            )
            expand_generated_dependencies(dependencies)
            if (
                dependencies & additive_pairs
                or any(table in new_table_set for table, _ in dependencies)
            ):
                rows.append((*candidate, str(candidate[1]).lower()))
    validate_owned_rows()

    for (
        table, column, expected_info, expected_foreign_keys,
        expected_definition,
    ) in (
        _V5_ADDITIVE_COLUMN_CONTRACT
    ):
        actual_info = tuple(
            tuple(row) for row in conn.execute(f"PRAGMA table_info({table})")
            if row[1] == column
        )
        if actual_info != (expected_info,):
            raise ValueError(f"additive column mismatch {table}.{column}")
        actual_foreign_keys = tuple(
            _sqlite_foreign_key_semantics(tuple(row))
            for row in conn.execute(f"PRAGMA foreign_key_list({table})")
            if row[3] == column
        )
        normalized_expected_foreign_keys = tuple(
            _sqlite_foreign_key_semantics(row)
            for row in expected_foreign_keys
        )
        if actual_foreign_keys != normalized_expected_foreign_keys:
            raise ValueError(f"additive foreign key mismatch {table}.{column}")
        stored = conn.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
            (table,),
        ).fetchall()
        if len(stored) != 1 or type(stored[0][0]) is not str:
            raise ValueError(f"additive column mismatch {table}.{column}")
        try:
            actual_definition = _sqlite_column_definition_tokens(
                stored[0][0], column,
            )
        except ValueError as exc:
            raise ValueError(
                f"additive column definition mismatch {table}.{column}"
            ) from exc
        if actual_definition != expected_definition:
            raise ValueError(
                f"additive column definition mismatch {table}.{column}"
            )

    reference_legacy_structure = {
        row[0]: row for row in _V5_AFFECTED_LEGACY_STRUCTURE_CONTRACT
    }
    try:
        actual_legacy_structure = _v5_affected_legacy_structure(conn)
    except ValueError as exc:
        raise ValueError("malformed v5-affected schema") from exc
    for table, columns, dependencies, foreign_keys in actual_legacy_structure:
        reference = reference_legacy_structure.get(table)
        if reference is None:
            raise RuntimeError(f"missing reference structure for {table!r}")
        reference_columns = set(reference[1])
        if any(
            column.startswith(("p3_", "canonical_"))
            and column not in reference_columns
            for column in columns
        ):
            raise ValueError(
                f"extra v5-owned schema object {table!r}"
            )
        unexpected_dependencies = dependencies - reference[2]
        if (
            unexpected_dependencies & additive_pairs
            or any(
                dependency_table in new_table_set
                for dependency_table, _column in unexpected_dependencies
            )
        ):
            raise ValueError(
                f"extra v5-owned schema object {table!r}"
            )
        unexpected_foreign_keys = list(foreign_keys)
        for expected_foreign_key in reference[3]:
            if expected_foreign_key in unexpected_foreign_keys:
                unexpected_foreign_keys.remove(expected_foreign_key)
        if any(
            target in new_table_set
            or (target, target_column) in additive_pairs
            for (
                _sequence, target, _source, target_column,
                _on_update, _on_delete, _match,
            ) in unexpected_foreign_keys
        ):
            raise ValueError(
                f"extra v5-owned schema object {table!r}"
            )


LEDGER_TABLES = ("decisions", "paper_trades", "outcomes", "regime_log")
EVIDENCE_TABLES = ("wallet_pnl_events", "early_buyer_reads")


def _append_only_triggers(tables: tuple[str, ...] = LEDGER_TABLES) -> str:
    parts = []
    for t in tables:
        for op in ("UPDATE", "DELETE"):
            parts.append(
                f"CREATE TRIGGER IF NOT EXISTS {t}_no_{op.lower()} BEFORE {op} ON {t} "
                f"BEGIN SELECT RAISE(ABORT, 'append-only'); END;"
            )
    return "\n".join(parts)


def p3_fee_sum_json(value: str) -> float:
    class _Pairs(list[tuple[str,object]]):
        pass

    def reject_constant(token: str) -> object:
        raise ValueError(f"invalid JSON constant: {token}")

    if type(value) is not str or len(value) > 4096:
        raise ValueError("fees_json must be bounded text")
    pairs = json.loads(
        value, object_pairs_hook=_Pairs, parse_constant=reject_constant,
    )
    if not isinstance(pairs, _Pairs):
        raise ValueError("fees_json must be an object")
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)) or keys != sorted(keys):
        raise ValueError("fee keys must be unique and sorted")
    if any(type(key) is not str or not 1 <= len(key) <= 64 for key in keys):
        raise ValueError("invalid fee key")
    if any(type(item) not in (int, float) or not math.isfinite(item)
           or not 0.0 <= item <= 1e6 for _, item in pairs):
        raise ValueError("invalid fee value")
    obj = dict(pairs)
    if value != json.dumps(obj, sort_keys=True, separators=(',',':'), allow_nan=False):
        raise ValueError("fees_json is not canonical")
    total = math.fsum(item for _, item in pairs)
    if not math.isfinite(total) or not 0.0 <= total <= 1e100:
        raise ValueError("invalid fee total")
    return total


def _build_v6_curve_progress_reserve_contract() -> tuple[
    tuple[str, ...],
    tuple[tuple[str, tuple[object, ...], tuple[tuple[str, str], ...]], ...],
]:
    """Capture exact v5 token order and the four additive v6 definitions."""
    conn = _v5_reference_connection()
    try:
        base_names = tuple(
            row[1] for row in conn.execute("PRAGMA table_xinfo(tokens)")
        )
        contract = []
        for column, sql in SCHEMA_V6_ADDITIVE_COLUMNS:
            conn.execute(sql)
            rows = [
                tuple(row) for row in conn.execute("PRAGMA table_xinfo(tokens)")
                if row[1] == column
            ]
            stored = conn.execute(
                "SELECT sql FROM sqlite_schema "
                "WHERE type='table' AND name='tokens'"
            ).fetchall()
            if (
                len(rows) != 1
                or len(stored) != 1
                or type(stored[0][0]) is not str
            ):
                raise RuntimeError(
                    f"invalid v6 reference additive column tokens.{column}"
                )
            contract.append((
                column,
                rows[0],
                _sqlite_column_definition_tokens(stored[0][0], column),
            ))
        return base_names, tuple(contract)
    finally:
        conn.close()


def _attest_v6_curve_progress_reserve_schema(
    conn: sqlite3.Connection,
    *,
    allow_prefix: bool = False,
    attest_v5: bool = True,
) -> int | None:
    """Attest exact v5 token schema plus a complete or contiguous v6 prefix."""
    if attest_v5:
        _attest_v5_schema_manifest(conn)
    actual_rows = tuple(
        tuple(row) for row in conn.execute("PRAGMA table_xinfo(tokens)")
    )
    actual_names = tuple(row[1] for row in actual_rows)
    expected_v6_names = tuple(
        column for column, _info, _definition in _V6_CURVE_RESERVE_CONTRACT
    )
    if actual_names[:len(_V6_BASE_TOKEN_COLUMNS)] != _V6_BASE_TOKEN_COLUMNS:
        raise ValueError("v6 tokens base column mismatch")
    suffix = actual_names[len(_V6_BASE_TOKEN_COLUMNS):]
    if (
        suffix != expected_v6_names[:len(suffix)]
        or len(suffix) > len(expected_v6_names)
        or (not allow_prefix and suffix != expected_v6_names)
    ):
        raise ValueError("v6 curve reserve column inventory mismatch")

    stored = conn.execute(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name='tokens'"
    ).fetchall()
    if len(stored) != 1 or type(stored[0][0]) is not str:
        raise ValueError("v6 tokens definition mismatch")
    rows_by_name = {row[1]: row for row in actual_rows}
    for column, expected_info, expected_definition in (
        _V6_CURVE_RESERVE_CONTRACT[:len(suffix)]
    ):
        if rows_by_name.get(column) != expected_info:
            raise ValueError(f"v6 additive column mismatch tokens.{column}")
        try:
            actual_definition = _sqlite_column_definition_tokens(
                stored[0][0], column,
            )
        except ValueError as exc:
            raise ValueError(
                f"v6 additive column definition mismatch tokens.{column}"
            ) from exc
        if actual_definition != expected_definition:
            raise ValueError(
                f"v6 additive column definition mismatch tokens.{column}"
            )
    return len(suffix) if allow_prefix else None


V5_SCHEMA_MANIFEST = _build_v5_schema_manifest()
_V5_SCHEMA_AUTOINDEX_MANIFEST = _build_v5_autoindex_manifest()
_V5_ADDITIVE_COLUMN_CONTRACT = _build_v5_additive_column_contract()
_V5_AFFECTED_LEGACY_AUTOINDEX_MANIFEST = _build_v5_legacy_autoindex_manifest()
_V5_AFFECTED_LEGACY_STRUCTURE_CONTRACT = (
    _build_v5_affected_legacy_structure_contract()
)
(
    _V6_BASE_TOKEN_COLUMNS,
    _V6_CURVE_RESERVE_CONTRACT,
) = _build_v6_curve_progress_reserve_contract()


def _v5_performance_index_ddl_by_name() -> dict[str, str]:
    ddl_by_name = {}
    for name, sql in (*V5_INDEX_DDL, *V5_PERFORMANCE_INDEX_DDL):
        if name in ddl_by_name and ddl_by_name[name] != sql:
            raise RuntimeError(f"conflicting v5 index DDL {name}")
        ddl_by_name[name] = sql
    return ddl_by_name


def _attest_v5_performance_indexes(conn: sqlite3.Connection) -> None:
    """Read-only validation of every healer-managed v5 index."""
    ddl_by_name = _v5_performance_index_ddl_by_name()
    for name, (table, expected_columns, expected_partial) in (
        V5_PERFORMANCE_INDEX_CONTRACT.items()
    ):
        rows = conn.execute(
            "SELECT type, tbl_name, sql FROM sqlite_schema"
            " WHERE name = ? COLLATE NOCASE",
            (name,),
        ).fetchall()
        row = rows[0] if len(rows) == 1 else None

        expected_sql = ddl_by_name[name].replace(" IF NOT EXISTS", "", 1).rstrip(";")
        index_columns = tuple(
            (item["name"], item["desc"])
            for item in conn.execute(f"PRAGMA index_xinfo({name})")
            if item["key"] == 1
        )
        index_list_row = next(
            (item for item in conn.execute(f"PRAGMA index_list({table})")
             if item["name"] == name),
            None,
        )
        compatible = (
            row is not None
            and row["type"] == "index"
            and row["tbl_name"] == table
            and type(row["sql"]) is str
            and _sqlite_ddl_tokens(row["sql"], preserve_strings=True)
            == _sqlite_ddl_tokens(expected_sql, preserve_strings=True)
            and index_columns == expected_columns
            and index_list_row is not None
            and bool(index_list_row["partial"]) is expected_partial
        )
        if not compatible:
            raise RuntimeError(f"incompatible performance index {name}")


def _ensure_v5_performance_indexes(conn: sqlite3.Connection) -> None:
    """Transactionally create missing recovery indexes and reject impostors."""
    ddl_by_name = _v5_performance_index_ddl_by_name()
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        for name in V5_PERFORMANCE_INDEX_CONTRACT:
            rows = conn.execute(
                "SELECT type, tbl_name, sql FROM sqlite_schema"
                " WHERE name = ? COLLATE NOCASE",
                (name,),
            ).fetchall()
            if not rows:
                conn.execute(ddl_by_name[name])
                rows = conn.execute(
                    "SELECT type, tbl_name, sql FROM sqlite_schema"
                    " WHERE name = ? COLLATE NOCASE",
                    (name,),
                ).fetchall()
        _attest_v5_performance_indexes(conn)
        if owns_transaction:
            conn.commit()
    except BaseException:
        if owns_transaction:
            conn.rollback()
        raise


def assert_p3_buy_terminal_coverage(conn: sqlite3.Connection) -> None:
    """Fail closed when a canonical P3 BUY lacks one valid terminal graph."""

    def numeric(
        value: object, low: float, high: float, *, positive: bool = False,
    ) -> bool:
        return (
            type(value) in (int, float)
            and (value > low if positive else value >= low)
            and value <= high
            and math.isfinite(value)
        )

    def same(left: object, right: object) -> bool:
        if left is None or right is None:
            return left is right
        if type(left) in (int, float) and type(right) in (int, float):
            return left == right
        return type(left) is type(right) and left == right

    def hash_text(value: object) -> bool:
        return (
            type(value) is str
            and len(value) == 64
            and value == value.lower()
            and all(character in "0123456789abcdef" for character in value)
        )

    def nul_free_text(value: object) -> bool:
        return type(value) is str and "\x00" not in value

    def invalid(decision_id: object) -> RuntimeError:
        return RuntimeError(
            f"WATCH startup terminal coverage invalid for decision_id={decision_id}"
        )

    def strict_json_object(value: object) -> dict[str, object] | None:
        if type(value) is not str:
            return None

        def object_no_duplicates(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError("duplicate JSON key")
                result[key] = item
            return result

        def reject_constant(token: str) -> object:
            raise ValueError(f"invalid JSON constant: {token}")

        try:
            parsed = json.loads(
                value,
                object_pairs_hook=object_no_duplicates,
                parse_constant=reject_constant,
            )
            if type(parsed) is not dict:
                return None
            canonical = json.dumps(
                parsed, sort_keys=True, separators=(",", ":"), allow_nan=False,
            )
        except (TypeError, ValueError, RecursionError, json.JSONDecodeError):
            return None
        return parsed if value == canonical else None

    def valid_recheck_payload(
        execution: sqlite3.Row, decision: sqlite3.Row,
    ) -> bool:
        payload = strict_json_object(execution["recheck_payload_json"])
        if payload is None or set(payload) != {
            "decision_id", "attempt", "trigger", "trigger_report_id",
            "rechecked_at", "fill_event_at", "causal_target_report_id",
            "latest_target_report_id", "prior_inputs_hash", "target_snapshot",
            "verdict",
        }:
            return False
        verdict = payload.get("verdict")
        if type(verdict) is not dict or set(verdict) != {
            "status", "reason", "canonical_mint", "inputs_hash",
        }:
            return False
        if not hash_text(verdict.get("inputs_hash")):
            return False

        trigger = payload.get("trigger")
        trigger_report_id = payload.get("trigger_report_id")
        fill_event_at = payload.get("fill_event_at")
        snapshot = payload.get("target_snapshot")
        if trigger == "curve_progress":
            if (
                trigger_report_id is not None
                or not numeric(fill_event_at, 0.0, 4102444800.0)
                or type(snapshot) is not dict
                or set(snapshot) != {
                    "t_wall", "t_mono", "virtual_sol_reserves",
                    "virtual_token_reserves", "real_sol_reserves",
                    "real_token_reserves", "liquidity_sol", "spot_price_sol",
                    "progress_pct",
                }
                or not same(snapshot.get("t_wall"), fill_event_at)
                or not numeric(snapshot.get("t_wall"), 0.0, 4102444800.0)
                or not numeric(snapshot.get("t_mono"), 0.0, 1e100)
                or type(snapshot.get("virtual_sol_reserves")) is not int
                or snapshot["virtual_sol_reserves"] <= 0
                or type(snapshot.get("virtual_token_reserves")) is not int
                or snapshot["virtual_token_reserves"] <= 0
                or type(snapshot.get("real_sol_reserves")) is not int
                or snapshot["real_sol_reserves"] < 0
                or type(snapshot.get("real_token_reserves")) is not int
                or snapshot["real_token_reserves"] < 0
                or not numeric(snapshot.get("liquidity_sol"), 0.0, 1e100)
                or not numeric(
                    snapshot.get("spot_price_sol"), 0.0, 1e100, positive=True,
                )
                or not numeric(snapshot.get("progress_pct"), 0.0, 100.0)
                or fill_event_at >= execution["rechecked_at"]
            ):
                return False
        elif trigger == "safety_hard_fail":
            trigger_report = (
                conn.execute(
                    "SELECT mint,checked_at,hard_fails_json "
                    "FROM safety_reports WHERE id=?",
                    (trigger_report_id,),
                ).fetchone()
                if type(trigger_report_id) is int
                else None
            )
            try:
                trigger_hard_fails = (
                    json.loads(trigger_report["hard_fails_json"])
                    if trigger_report is not None
                    else None
                )
            except (TypeError, ValueError, RecursionError, json.JSONDecodeError):
                trigger_hard_fails = None
            if (
                type(trigger_report_id) is not int
                or trigger_report_id <= 0
                or fill_event_at is not None
                or snapshot is not None
                or trigger_report is None
                or trigger_report["mint"] != decision["mint"]
                or not numeric(
                    trigger_report["checked_at"], 0.0, 4102444800.0,
                )
                or trigger_report["checked_at"] >= execution["rechecked_at"]
                or type(trigger_hard_fails) is not list
                or not trigger_hard_fails
            ):
                return False
        else:
            return False

        payload_status = verdict.get("status")
        status_matches = (
            execution["recheck_status"] == "PASS"
            and payload_status == "CANONICAL"
        ) or (
            execution["recheck_status"] == "CANCEL"
            and payload_status in ("SUPPRESSED", "UNRESOLVED")
        )
        return (
            status_matches
            and same(execution["recheck_decision_id"], payload.get("decision_id"))
            and same(execution["recheck_attempt"], payload.get("attempt"))
            and same(execution["rechecked_at"], payload.get("rechecked_at"))
            and same(
                execution["causal_target_report_id"],
                payload.get("causal_target_report_id"),
            )
            and same(
                execution["latest_target_report_id"],
                payload.get("latest_target_report_id"),
            )
            and same(
                execution["recheck_prior_inputs_hash"],
                payload.get("prior_inputs_hash"),
            )
            and same(execution["recheck_reason"], verdict.get("reason"))
            and same(execution["canonical_mint"], verdict.get("canonical_mint"))
            and hash_text(execution["recheck_prior_inputs_hash"])
            and hash_text(execution["recheck_inputs_hash"])
            and hashlib.sha256(
                execution["recheck_payload_json"].encode()
            ).hexdigest() == execution["recheck_inputs_hash"]
        )

    last_id = 0
    while True:
        decisions = conn.execute(
            """SELECT d.id,d.at,d.mint,d.safety_report_id,
 typeof(d.mint) AS mint_type,
 length(trim(d.mint)) AS mint_trimmed_length,
 CASE WHEN json_valid(d.feature_vector_json)
      THEN json_extract(d.feature_vector_json,'$.canonical.planned_size_sol') END
      AS planned_size_sol,
 CASE WHEN json_valid(d.feature_vector_json)
      THEN json_extract(d.feature_vector_json,'$.canonical.inputs_hash') END
      AS prior_inputs_hash
FROM decisions AS d INDEXED BY decisions_p3_canonical_buy_idx
WHERE d.action='BUY'
  AND CASE WHEN json_valid(d.feature_vector_json)
           THEN json_extract(d.feature_vector_json,'$.canonical.status') END='CANONICAL'
  AND d.id>?
ORDER BY d.id
LIMIT 128""",
            (last_id,),
        ).fetchall()
        if not decisions:
            return

        for decision in decisions:
            decision_id = decision["id"]
            if (
                type(decision_id) is not int
                or decision_id <= last_id
                or not numeric(decision["at"], 0.0, 4102444800.0)
                or decision["mint_type"] != "text"
                or not nul_free_text(decision["mint"])
                or type(decision["mint_trimmed_length"]) is not int
                or not 1 <= decision["mint_trimmed_length"] <= 128
                or not numeric(
                    decision["planned_size_sol"], 0.0, 1e100, positive=True,
                )
                or not hash_text(decision["prior_inputs_hash"])
            ):
                raise invalid(decision_id)

            observations = conn.execute(
                """SELECT is_subject,is_canonical,eligible
FROM canonical_observations
INDEXED BY sqlite_autoindex_canonical_observations_1
WHERE decision_id=? AND mint=?
LIMIT 2""",
                (decision_id, decision["mint"]),
            ).fetchall()
            executions = conn.execute(
                """SELECT e.*,
 cr.decision_id AS recheck_decision_id,
 cr.attempt AS recheck_attempt,
 cr.rechecked_at,
 cr.status AS recheck_status,
 cr.reason AS recheck_reason,
 cr.canonical_mint,
 cr.causal_target_report_id,
 cr.latest_target_report_id,
 cr.prior_inputs_hash AS recheck_prior_inputs_hash,
 cr.recheck_inputs_hash,
 cr.payload_json AS recheck_payload_json,
 causal.mint AS causal_report_mint,
 causal.checked_at AS causal_report_checked_at,
 latest.mint AS latest_report_mint,
 latest.checked_at AS latest_report_checked_at,
 pt.decision_id AS trade_decision_id,
 pt.at AS trade_at,
 pt.mint AS trade_mint,
 pt.segment AS trade_segment,
 pt.side AS trade_side,
 pt.qty AS trade_qty,
 pt.quote_price AS trade_quote_price,
 pt.fill_price AS trade_fill_price,
 pt.fees_json AS trade_fees_json,
 pt.realism_grade AS trade_realism_grade,
 pt.canonical_recheck_id AS trade_recheck_id,
 pt.canonical_proof_hash AS trade_proof_hash,
 pt.p3_entry_execution_id AS trade_entry_execution_id,
 typeof(e.reason) AS execution_reason_type,
 length(trim(e.reason)) AS execution_reason_trimmed_length,
 typeof(pt.mint) AS trade_mint_type,
 length(trim(pt.mint)) AS trade_mint_trimmed_length,
 typeof(pt.segment) AS trade_segment_type,
 length(trim(pt.segment)) AS trade_segment_trimmed_length,
 typeof(pt.realism_grade) AS trade_realism_grade_type,
 length(pt.realism_grade) AS trade_realism_grade_length
FROM paper_entry_executions AS e
INDEXED BY sqlite_autoindex_paper_entry_executions_1
LEFT JOIN canonical_rechecks AS cr ON cr.id=e.canonical_recheck_id
LEFT JOIN safety_reports AS causal ON causal.id=cr.causal_target_report_id
LEFT JOIN safety_reports AS latest ON latest.id=cr.latest_target_report_id
LEFT JOIN paper_trades AS pt ON pt.id=e.paper_trade_id
WHERE e.decision_id=?
LIMIT 2""",
                (decision_id,),
            ).fetchall()
            if (
                len(observations) != 1
                or tuple(observations[0]) != (1, 1, 1)
                or len(executions) != 1
            ):
                raise invalid(decision_id)

            execution = executions[0]
            latest_recheck = conn.execute(
                """SELECT id
FROM canonical_rechecks INDEXED BY canonical_rechecks_decision_idx
WHERE decision_id=?
ORDER BY attempt DESC
LIMIT 1""",
                (decision_id,),
            ).fetchone()
            has_cancel = conn.execute(
                """SELECT 1
FROM canonical_rechecks INDEXED BY canonical_rechecks_decision_status_idx
WHERE decision_id=? AND status='CANCEL'
LIMIT 1""",
                (decision_id,),
            ).fetchone() is not None
            common_valid = (
                execution["decision_id"] == decision_id
                and numeric(execution["at"], 0.0, 4102444800.0)
                and execution["at"] > decision["at"]
                and execution["status"] in ("FILLED", "CANCELLED", "ABANDONED")
                and execution["execution_reason_type"] == "text"
                and nul_free_text(execution["reason"])
                and type(execution["execution_reason_trimmed_length"]) is int
                and execution["execution_reason_trimmed_length"] > 0
                and numeric(
                    execution["planned_size_sol"], 0.0, 1e100, positive=True,
                )
                and same(
                    execution["planned_size_sol"], decision["planned_size_sol"],
                )
            )
            if not common_valid:
                raise invalid(decision_id)

            recheck_is_latest = (
                latest_recheck is not None
                and latest_recheck["id"] == execution["canonical_recheck_id"]
            )
            recheck_is_linked = (
                execution["canonical_recheck_id"] is not None
                and execution["recheck_decision_id"] == decision_id
                and type(execution["recheck_attempt"]) is int
                and execution["recheck_attempt"] >= 1
                and numeric(execution["rechecked_at"], 0.0, 4102444800.0)
                and execution["at"] > execution["rechecked_at"]
                and execution["causal_target_report_id"]
                == decision["safety_report_id"]
                and execution["causal_report_mint"] == decision["mint"]
                and numeric(
                    execution["causal_report_checked_at"], 0.0, 4102444800.0,
                )
                and execution["causal_report_checked_at"] < decision["at"]
                and execution["causal_report_checked_at"]
                < execution["rechecked_at"]
                and execution["latest_report_mint"] == decision["mint"]
                and numeric(
                    execution["latest_report_checked_at"], 0.0, 4102444800.0,
                )
                and execution["latest_report_checked_at"]
                < execution["rechecked_at"]
                and execution["recheck_prior_inputs_hash"]
                == decision["prior_inputs_hash"]
                and hash_text(execution["recheck_prior_inputs_hash"])
                and hash_text(execution["recheck_inputs_hash"])
                and type(execution["recheck_payload_json"]) is str
                and hashlib.sha256(
                    execution["recheck_payload_json"].encode()
                ).hexdigest() == execution["recheck_inputs_hash"]
                and valid_recheck_payload(execution, decision)
                and recheck_is_latest
            )

            if execution["status"] == "FILLED":
                try:
                    fees = p3_fee_sum_json(execution["trade_fees_json"])
                except (
                    TypeError, ValueError, OverflowError, json.JSONDecodeError,
                ):
                    fees = None
                trade_valid = (
                    execution["reason"] == "filled"
                    and not has_cancel
                    and recheck_is_linked
                    and execution["recheck_status"] == "PASS"
                    and execution["recheck_reason"] == "canonical_selected"
                    and execution["canonical_mint"] == decision["mint"]
                    and execution["latest_target_report_id"]
                    == execution["causal_target_report_id"]
                    and execution["paper_trade_id"] is not None
                    and execution["trade_decision_id"] == decision_id
                    and execution["trade_mint"] == decision["mint"]
                    and execution["trade_side"] == "buy"
                    and execution["trade_mint_type"] == "text"
                    and nul_free_text(execution["trade_mint"])
                    and type(execution["trade_mint_trimmed_length"]) is int
                    and 1 <= execution["trade_mint_trimmed_length"] <= 128
                    and execution["trade_segment_type"] == "text"
                    and nul_free_text(execution["trade_segment"])
                    and type(execution["trade_segment_trimmed_length"]) is int
                    and 1 <= execution["trade_segment_trimmed_length"] <= 64
                    and numeric(execution["trade_at"], 0.0, 4102444800.0)
                    and execution["trade_at"] == execution["at"]
                    and numeric(
                        execution["trade_qty"], 0.0, 1e100, positive=True,
                    )
                    and numeric(
                        execution["trade_quote_price"], 0.0, 1e100, positive=True,
                    )
                    and numeric(
                        execution["trade_fill_price"], 0.0, 1e100, positive=True,
                    )
                    and execution["trade_fill_price"]
                    >= execution["trade_quote_price"]
                    and fees is not None
                    and numeric(fees, 0.0, 1e100)
                    and execution["trade_realism_grade_type"] == "text"
                    and nul_free_text(execution["trade_realism_grade"])
                    and type(execution["trade_realism_grade_length"]) is int
                    and 1 <= execution["trade_realism_grade_length"] <= 32
                    and execution["trade_recheck_id"]
                    == execution["canonical_recheck_id"]
                    and execution["trade_proof_hash"]
                    == execution["recheck_inputs_hash"]
                    and execution["trade_entry_execution_id"] is None
                    and math.isclose(
                        execution["planned_size_sol"],
                        execution["trade_qty"] * execution["trade_fill_price"],
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    )
                )
                if not trade_valid:
                    raise invalid(decision_id)
            elif execution["status"] == "CANCELLED":
                if not (
                    execution["paper_trade_id"] is None
                    and recheck_is_linked
                    and execution["recheck_status"] == "CANCEL"
                    and execution["reason"] == execution["recheck_reason"]
                ):
                    raise invalid(decision_id)
            else:
                before_fill = (
                    execution["paper_trade_id"] is None
                    and execution["canonical_recheck_id"] is None
                    and latest_recheck is None
                    and execution["reason"] == "restart_before_fill"
                )
                after_pass = (
                    execution["paper_trade_id"] is None
                    and recheck_is_linked
                    and execution["recheck_status"] == "PASS"
                    and execution["recheck_reason"] == "canonical_selected"
                    and execution["canonical_mint"] == decision["mint"]
                    and execution["latest_target_report_id"]
                    == execution["causal_target_report_id"]
                    and execution["reason"] == "restart_after_pass"
                    and not has_cancel
                )
                if not (before_fill or after_pass):
                    raise invalid(decision_id)

            last_id = decision_id


def assert_no_open_p3_positions(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        """SELECT decision_id
FROM p3_position_current INDEXED BY p3_position_current_open_idx
WHERE sold_qty<bought_qty
ORDER BY decision_id
LIMIT 1"""
    ).fetchone()
    if row is not None:
        raise RuntimeError(
            f"WATCH-only startup blocked by open P3 position decision_id={row['decision_id']}"
        )


def open_db(
    path: str | Path, *, migration_clock: Callable[[], float] = time.time,
) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.create_function("p3_fee_sum", 1, p3_fee_sum_json, deterministic=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA recursive_triggers=ON")
    foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()
    if foreign_keys is None or foreign_keys[0] != 1:
        conn.close()
        raise RuntimeError("SQLite foreign_keys unavailable")
    recursive_triggers = conn.execute("PRAGMA recursive_triggers").fetchone()
    if recursive_triggers is None or recursive_triggers[0] != 1:
        conn.close()
        raise RuntimeError("SQLite recursive_triggers unavailable")
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version > 6:
        conn.close()
        raise RuntimeError(f"unsupported database schema version {version}")
    conn.execute("PRAGMA journal_mode=WAL")
    if version < 1:
        # IF NOT EXISTS on every table/trigger is load-bearing: executescript
        # autocommits DDL as it runs, so a crash between the DDL and the
        # user_version stamp leaves tables present with user_version=0. The next
        # open must heal that half-state, not wedge on "table already exists".
        # The version gate is just an optimization; correctness = idempotent DDL.
        conn.executescript(SCHEMA_V1)
        conn.executescript(_append_only_triggers())
        conn.execute("PRAGMA user_version=1")
        conn.commit()
    if version < 2:
        try:
            conn.execute(SCHEMA_V2)
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise  # healing: re-adding after a crash between ALTER and stamp is a no-op
        conn.execute("PRAGMA user_version=2")
        conn.commit()
    if version < 3:
        try:
            conn.execute(SCHEMA_V3)
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise  # healing: re-adding after a crash between ALTER and stamp is a no-op
        conn.execute("PRAGMA user_version=3")
        conn.commit()
    if version < 4:
        conn.executescript(SCHEMA_V4)
        conn.executescript(_append_only_triggers(EVIDENCE_TABLES))
        conn.execute("PRAGMA user_version=4")
        conn.commit()
    if version < 5:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for _, sql in V5_TABLE_DDL:
                conn.execute(sql)
            _apply_v5_additive_columns(conn)
            for _, sql in V5_INDEX_DDL:
                conn.execute(sql)

            initialize_p3_causal_clock(conn, raw_now=migration_clock())
            _validate_v5_legacy_safety_reports(conn)
            _validate_v5_legacy_wallet_pnl_events(conn)
            _validate_v5_legacy_early_buyer_reads(conn)
            _validate_v5_legacy_creator_reputation_events(conn)
            _validate_v5_legacy_p3_trade_execution_graph(conn)
            _rebuild_v5_wallet_pnl_summary(conn)
            _rebuild_v5_creator_reputation_current(conn)
            _rebuild_v5_p3_position_current(conn)
            _rebuild_v5_canonical_pending_current(conn)

            for _, sql in V5_EXPLICIT_TRIGGER_DDL:
                conn.execute(sql)
            for sql in _v5_immutable_triggers():
                conn.execute(sql)
            _attest_v5_schema_manifest(conn)
            conn.execute("PRAGMA user_version=5")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    if version < 6:
        # V5 performance-only maintenance remains its own migration step.
        # The v6 transaction below creates no index.
        _ensure_v5_performance_indexes(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _attest_v5_performance_indexes(conn)
            prefix_length = _attest_v6_curve_progress_reserve_schema(
                conn, allow_prefix=True,
            )
            for _column, sql in SCHEMA_V6_ADDITIVE_COLUMNS[prefix_length:]:
                conn.execute(sql)
            _attest_v6_curve_progress_reserve_schema(conn, attest_v5=False)
            conn.execute("PRAGMA user_version=6")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    else:
        _attest_v5_performance_indexes(conn)
        _attest_v6_curve_progress_reserve_schema(conn)
    return conn


def upsert_token(conn: sqlite3.Connection, *, mint: str, created_at: float,
                 bonding_curve_key: str = "") -> None:
    conn.execute(
        "INSERT INTO tokens(mint, created_at, last_seen, bonding_curve_key)"
        " VALUES (?, ?, ?, ?)"
        " ON CONFLICT(mint) DO UPDATE SET last_seen = excluded.last_seen",
        (mint, created_at, created_at, bonding_curve_key))
    conn.commit()


def _load_legacy_token_metadata(value: object) -> dict[str, object]:
    if type(value) is not str:
        raise ValueError("invalid legacy token metadata")

    def object_no_duplicates(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    def reject_constant(token: str) -> object:
        raise ValueError(f"invalid JSON constant: {token}")

    try:
        metadata = json.loads(
            value,
            object_pairs_hook=object_no_duplicates,
            parse_constant=reject_constant,
        )
        if type(metadata) is not dict:
            raise ValueError("legacy token metadata is not an object")
        json.dumps(metadata, allow_nan=False)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid legacy token metadata") from exc
    return metadata


def upsert_token_identity(
    conn: sqlite3.Connection,
    *,
    mint: str,
    raw_ingested_at: float,
    bonding_curve_key: str,
    fields: Mapping[str, object],
) -> None:
    """Atomically persist first P3 identity and its causal ingestion time."""
    raw_wall = _validated_p3_causal_wall(raw_ingested_at)
    if type(mint) is not str or not mint.strip():
        raise ValueError("invalid token mint")
    if type(bonding_curve_key) is not str:
        raise ValueError("invalid bonding curve key")
    if not isinstance(fields, Mapping):
        raise ValueError("invalid token identity fields")

    tracked_fields = (
        "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
    )
    identity = {
        field: value if type(value := fields.get(field)) is str and value.strip() else ""
        for field in tracked_fields
    }

    if conn.in_transaction:
        raise RuntimeError("token identity upsert owns its transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT meta_json,p3_identity_ingested_at FROM tokens WHERE mint=?", (mint,)
        ).fetchone()
        if row is None or row["p3_identity_ingested_at"] is None:
            if row is not None:
                legacy_metadata = _load_legacy_token_metadata(row["meta_json"])
                identity = {
                    field: legacy_value
                    if type(legacy_value := legacy_metadata.get(field)) is str
                    and legacy_value.strip()
                    else value
                    for field, value in identity.items()
                }
            identity_t = allocate_p3_causal_wall(conn, raw_wall=raw_wall)
            metadata = {
                **identity,
                "identity_observed_at": {
                    field: identity_t for field, value in identity.items() if value
                },
                "identity_conflicts": [],
                "identity_conflict_observed_at": {},
            }
            metadata_json = json.dumps(
                metadata, sort_keys=True, separators=(",", ":"), allow_nan=False,
            )
            if row is None:
                conn.execute(
                    "INSERT INTO tokens("
                    "mint,created_at,last_seen,meta_json,bonding_curve_key,"
                    "p3_identity_ingested_at) VALUES (?,?,?,?,?,?)",
                    (
                        mint, raw_wall, raw_wall, metadata_json, bonding_curve_key,
                        identity_t,
                    ),
                )
            else:
                conn.execute(
                    "UPDATE tokens SET meta_json=?,"
                    "bonding_curve_key=CASE WHEN bonding_curve_key='' THEN ? "
                    "ELSE bonding_curve_key END,p3_identity_ingested_at=? WHERE mint=?",
                    (
                        metadata_json, bonding_curve_key, identity_t, mint,
                    ),
                )
        else:
            metadata = _load_legacy_token_metadata(row["meta_json"])
            expected_keys = set(tracked_fields) | {
                "identity_observed_at",
                "identity_conflicts",
                "identity_conflict_observed_at",
            }
            identity_t = _validated_p3_causal_wall(row["p3_identity_ingested_at"])
            observed_at = metadata.get("identity_observed_at")
            conflicts = metadata.get("identity_conflicts")
            conflict_observed_at = metadata.get("identity_conflict_observed_at")
            if (
                set(metadata) != expected_keys
                or any(type(metadata.get(field)) is not str for field in tracked_fields)
                or type(observed_at) is not dict
                or type(conflicts) is not list
                or type(conflict_observed_at) is not dict
                or any(type(field) is not str for field in conflicts)
                or conflicts != sorted(set(conflicts))
                or any(field not in tracked_fields for field in conflicts)
                or set(conflict_observed_at) != set(conflicts)
            ):
                raise ValueError("invalid p3 token metadata")
            expected_observed_fields = {
                field for field in tracked_fields if metadata[field].strip()
            }
            if (
                set(observed_at) != expected_observed_fields
                or any(
                    not metadata[field].strip() or field not in observed_at
                    for field in conflicts
                )
            ):
                raise ValueError("invalid p3 token metadata")
            causal_wall = _p3_causal_clock_last_wall(conn)
            try:
                observation_times = tuple(
                    _validated_p3_causal_wall(value)
                    for value in (*observed_at.values(), *conflict_observed_at.values())
                )
            except ValueError as exc:
                raise ValueError("invalid p3 token metadata") from exc
            if identity_t > causal_wall or any(
                not identity_t <= value <= causal_wall for value in observation_times
            ) or any(
                conflict_observed_at[field] <= observed_at[field] for field in conflicts
            ):
                raise ValueError("invalid p3 token metadata")

            field_normalizers = {
                "uri": normalize_uri,
                "website": normalize_website,
                "twitter": normalize_twitter,
                "telegram": normalize_telegram,
            }
            filled_fields = tuple(
                field
                for field in tracked_fields
                if not metadata[field].strip() and identity[field]
                and (
                    field not in field_normalizers
                    or field_normalizers[field](identity[field]) is not None
                )
            )
            new_conflicts = tuple(
                field
                for field in tracked_fields
                if metadata[field].strip()
                and identity[field]
                and field not in conflicts
                and (
                    (
                        field in ("name", "symbol")
                        and normalize_identity(metadata[field])
                        != normalize_identity(identity[field])
                    )
                    or (
                        field == "creator"
                        and metadata[field].strip() != identity[field].strip()
                    )
                    or (
                        field in field_normalizers
                        and field_normalizers[field](identity[field]) is not None
                        and field_normalizers[field](metadata[field])
                        != field_normalizers[field](identity[field])
                    )
                )
            )
            if filled_fields or new_conflicts:
                mutation_t = allocate_p3_causal_wall(conn, raw_wall=raw_wall)
                for field in filled_fields:
                    metadata[field] = identity[field]
                    observed_at[field] = mutation_t
                conflicts.extend(new_conflicts)
                conflicts.sort()
                for field in new_conflicts:
                    conflict_observed_at[field] = mutation_t
                metadata_json = json.dumps(
                    metadata, sort_keys=True, separators=(",", ":"), allow_nan=False,
                )
                conn.execute(
                    "UPDATE tokens SET meta_json=?,last_seen=? WHERE mint=?",
                    (metadata_json, raw_wall, mint),
                )
            else:
                conn.execute(
                    "UPDATE tokens SET last_seen=? WHERE mint=?", (raw_wall, mint),
                )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def get_token(conn: sqlite3.Connection, mint: str):
    return conn.execute("SELECT * FROM tokens WHERE mint = ?", (mint,)).fetchone()


class EvidenceIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ValidatedSafetyHolder:
    safety_report_id: int
    mint: str
    checked_at: float
    risk_score: float
    hard_fails: tuple[str, ...]
    safety_inputs_hash: str
    holder_evidence_id: int | None
    holder: HolderEvidenceDraft | None
    holder_unavailable_reason: str


@dataclass(frozen=True, slots=True)
class TerminalReputationResult:
    state: Literal["GRADUATED", "DEAD"]
    rugged: bool
    processed_at: float | None
    reputation_event_id: int | None
    reputation_outcome: Literal["GRADUATED", "RUGGED"] | None
    event_inserted: bool


@dataclass(frozen=True, slots=True)
class CreatorReputationResult:
    prior_successes: int
    prior_rugs: int
    selected_event_ids: tuple[int, ...]
    as_of: float
    unavailable_reason: str


def reputation_creator_eligible(value: object, *, conflicted: bool) -> bool:
    return (
        not conflicted
        and type(value) is str
        and value == value.strip(" ")
        and "\x00" not in value
        and 1 <= len(value) <= 128
    )


def validated_creator_reputation_current(
    conn: sqlite3.Connection,
    *,
    creator: str,
    candidate_mint: str,
    as_of: float,
    max_creator_history_mints: int,
) -> CreatorReputationResult:
    if not reputation_creator_eligible(creator, conflicted=False):
        raise ValueError("invalid creator")
    if type(candidate_mint) is not str or not candidate_mint.strip():
        raise ValueError("invalid candidate mint")
    validated_as_of = _validated_p3_causal_wall(as_of)
    if (
        type(max_creator_history_mints) is not int
        or not 1 <= max_creator_history_mints <= 500
    ):
        raise ValueError("invalid creator history limit")

    def unavailable(reason: str) -> CreatorReputationResult:
        return CreatorReputationResult(
            prior_successes=0,
            prior_rugs=0,
            selected_event_ids=(),
            as_of=validated_as_of,
            unavailable_reason=reason,
        )

    newest = conn.execute(
        """SELECT observed_at,event_id
FROM creator_reputation_current
WHERE creator=?
ORDER BY observed_at DESC,event_id DESC
LIMIT 1""",
        (creator,),
    ).fetchone()
    if newest is None:
        return unavailable("")
    newest_at, newest_event_id = newest
    try:
        validated_newest_at = _validated_p3_causal_wall(newest_at)
    except ValueError:
        return unavailable("creator_reputation_unavailable")
    if (
        type(newest_event_id) is not int
        or newest_event_id <= 0
        or validated_newest_at >= validated_as_of
    ):
        return unavailable("creator_reputation_unavailable")

    rows = conn.execute(
        """SELECT event_id,mint,creator,outcome,observed_at
FROM creator_reputation_current
WHERE creator=? AND mint<>?
ORDER BY observed_at,event_id
LIMIT ?""",
        (creator, candidate_mint, max_creator_history_mints + 1),
    ).fetchall()
    validated: list[tuple[int, str, str, float]] = []
    previous_order: tuple[float, int] | None = None
    for event_id, mint, row_creator, outcome, observed_at in rows:
        try:
            validated_at = _validated_p3_causal_wall(observed_at)
        except ValueError:
            return unavailable("creator_reputation_unavailable")
        order = (validated_at, event_id) if type(event_id) is int else None
        if (
            type(event_id) is not int
            or event_id <= 0
            or type(mint) is not str
            or not mint.strip()
            or mint == candidate_mint
            or row_creator != creator
            or not reputation_creator_eligible(row_creator, conflicted=False)
            or type(outcome) is not str
            or outcome not in ("GRADUATED", "RUGGED")
            or validated_at >= validated_as_of
            or previous_order is not None
            and order is not None
            and order <= previous_order
        ):
            return unavailable("creator_reputation_unavailable")
        validated.append((event_id, outcome, mint, validated_at))
        previous_order = order
    if len(validated) > max_creator_history_mints:
        return unavailable("creator_history_overflow")

    return CreatorReputationResult(
        prior_successes=sum(outcome == "GRADUATED" for _, outcome, _, _ in validated),
        prior_rugs=sum(outcome == "RUGGED" for _, outcome, _, _ in validated),
        selected_event_ids=tuple(event_id for event_id, _, _, _ in validated),
        as_of=validated_as_of,
        unavailable_reason="",
    )


def set_terminal_state_with_reputation(
    conn: sqlite3.Connection,
    *,
    mint: str,
    outcome: Literal["GRADUATED", "RUGGED"],
    raw_processed_at: float,
    creator: object,
    creator_conflicted: bool,
) -> TerminalReputationResult:
    if type(mint) is not str or not mint.strip():
        raise ValueError("invalid mint")
    if type(outcome) is not str or outcome not in ("GRADUATED", "RUGGED"):
        raise ValueError("invalid terminal outcome")
    raw_wall = _validated_p3_causal_wall(raw_processed_at)
    state = "GRADUATED" if outcome == "GRADUATED" else "DEAD"
    rugged = outcome == "RUGGED"

    conn.execute("BEGIN IMMEDIATE")
    try:
        token = conn.execute(
            "SELECT state,rugged FROM tokens WHERE mint=?", (mint,)
        ).fetchone()
        if token is None:
            raise ValueError("unknown token")
        token_state, token_rugged = token
        if (
            type(token_state) is not str
            or token_state not in (
                "FRESH", "CLIMBING", "TRENDING", "ESTABLISHED",
                "GRADUATED", "DEAD",
            )
            or type(token_rugged) is not int
            or token_rugged not in (0, 1)
        ):
            raise ValueError("invalid token state")
        prior_rows = conn.execute(
            "SELECT id,creator,outcome,observed_at "
            "FROM creator_reputation_events WHERE mint=? ORDER BY id",
            (mint,),
        ).fetchall()
        validated_prior_rows: list[tuple[int, str, str, float]] = []
        prior_outcomes: set[str] = set()
        prior_creator: str | None = None
        last_event_id = 0
        for event_id, event_creator, event_outcome, event_at in prior_rows:
            try:
                validated_event_at = _validated_p3_causal_wall(event_at)
            except ValueError as exc:
                raise ValueError("invalid terminal reputation evidence") from exc
            if (
                type(event_id) is not int
                or event_id <= last_event_id
                or not reputation_creator_eligible(
                    event_creator, conflicted=False
                )
                or type(event_outcome) is not str
                or event_outcome not in ("GRADUATED", "RUGGED")
                or event_outcome in prior_outcomes
                or (prior_creator is not None and event_creator != prior_creator)
            ):
                raise ValueError("invalid terminal reputation evidence")
            validated_prior_rows.append(
                (event_id, event_creator, event_outcome, validated_event_at)
            )
            prior_outcomes.add(event_outcome)
            prior_creator = event_creator
            last_event_id = event_id
        if len(validated_prior_rows) > 2 or (
            len(validated_prior_rows) == 2
            and (
                validated_prior_rows[0][2] != "GRADUATED"
                or validated_prior_rows[1][2] != "RUGGED"
                or validated_prior_rows[0][3] >= validated_prior_rows[1][3]
            )
        ):
            raise ValueError("invalid terminal reputation evidence")
        if outcome == "GRADUATED" and (
            token_rugged or "RUGGED" in prior_outcomes
        ):
            raise EvidenceIntegrityError("RUGGED reputation is terminal")
        same_outcome_rows = [
            row for row in validated_prior_rows if row[2] == outcome
        ]
        if len(same_outcome_rows) == 1 and not (
            outcome == "GRADUATED"
            and (token_rugged or any(row[2] == "RUGGED" for row in prior_rows))
        ):
            event_id, prior_creator, _, processed_at = same_outcome_rows[0]
            if (
                type(event_id) is not int
                or event_id <= 0
                or not reputation_creator_eligible(
                    prior_creator, conflicted=False
                )
            ):
                raise ValueError("invalid terminal reputation evidence")
            processed_at = _validated_p3_causal_wall(processed_at)
            conn.execute(
                "UPDATE tokens SET state=?,rugged=? WHERE mint=?",
                (state, int(rugged), mint),
            )
            conn.commit()
            return TerminalReputationResult(
                state=state,
                rugged=rugged,
                processed_at=processed_at,
                reputation_event_id=event_id,
                reputation_outcome=outcome,
                event_inserted=False,
            )
        state_only_duplicate = not prior_rows and (
            (outcome == "GRADUATED" and token_state == "GRADUATED" and not token_rugged)
            or (outcome == "RUGGED" and token_state == "DEAD" and token_rugged)
        )
        if state_only_duplicate:
            conn.execute(
                "UPDATE tokens SET state=?,rugged=? WHERE mint=?",
                (state, int(rugged), mint),
            )
            conn.commit()
            return TerminalReputationResult(
                state=state,
                rugged=rugged,
                processed_at=None,
                reputation_event_id=None,
                reputation_outcome=None,
                event_inserted=False,
            )
        if prior_rows:
            if (
                outcome != "RUGGED"
                or token_rugged
                or len(prior_rows) != 1
                or prior_rows[0][2] != "GRADUATED"
            ):
                raise ValueError("terminal reputation evidence already exists")
            prior_id, prior_creator, _, prior_observed_at = prior_rows[0]
            if (
                type(prior_id) is not int
                or prior_id <= 0
                or not reputation_creator_eligible(
                    prior_creator, conflicted=False
                )
            ):
                raise ValueError("invalid terminal reputation evidence")
            _validated_p3_causal_wall(prior_observed_at)
            selected_creator = prior_creator
            processed_at = allocate_p3_causal_wall(conn, raw_wall=raw_wall)
        else:
            processed_at = allocate_p3_causal_wall(conn, raw_wall=raw_wall)
            selected_creator = (
                creator
                if reputation_creator_eligible(
                    creator, conflicted=creator_conflicted
                )
                else None
            )
        conn.execute(
            "UPDATE tokens SET state=?,rugged=? WHERE mint=?",
            (state, int(rugged), mint),
        )
        if selected_creator is None:
            conn.commit()
            return TerminalReputationResult(
                state=state,
                rugged=rugged,
                processed_at=processed_at,
                reputation_event_id=None,
                reputation_outcome=None,
                event_inserted=False,
            )
        cursor = conn.execute(
            """INSERT INTO creator_reputation_events(
  mint,creator,outcome,observed_at)
VALUES(?,?,?,?)""",
            (mint, selected_creator, outcome, processed_at),
        )
        event_id = cursor.lastrowid
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return TerminalReputationResult(
        state=state,
        rugged=rugged,
        processed_at=processed_at,
        reputation_event_id=event_id,
        reputation_outcome=outcome,
        event_inserted=True,
    )


def set_token_state(conn: sqlite3.Connection, mint: str, state: str, *,
                    progress_pct: float | None = None,
                    last_seen: float | None = None) -> None:
    if state == "GRADUATED":
        raise ValueError(
            "GRADUATED is reserved for set_terminal_state_with_reputation"
        )
    sets, args = ["state = ?"], [state]
    if progress_pct is not None:
        sets.append("curve_progress = ?")
        args.append(progress_pct)
    if last_seen is not None:
        sets.append("last_seen = ?")
        args.append(last_seen)
    args.append(mint)
    conn.execute(
        f"UPDATE tokens SET {', '.join(sets)} WHERE mint = ?",  # nosec B608
        args,
    )
    conn.commit()


def tracked_tokens(conn: sqlite3.Connection, *, states: tuple[str, ...], limit: int):
    marks = ",".join("?" * len(states))
    return conn.execute(
        f"SELECT * FROM tokens WHERE state IN ({marks})"  # nosec B608
        " ORDER BY curve_progress DESC, created_at DESC LIMIT ?",
        (*states, limit)).fetchall()


def record_boot(conn: sqlite3.Connection, config_hash: str) -> int:
    cur = conn.execute(
        "INSERT INTO boots(at, config_hash) VALUES (?, ?)", (time.time(), config_hash)
    )
    conn.commit()
    return cur.lastrowid


def mark_clean_shutdown(conn: sqlite3.Connection, boot_id: int) -> None:
    conn.execute("UPDATE boots SET clean_shutdown = 1 WHERE id = ?", (boot_id,))
    conn.commit()


def save_safety_report(conn: sqlite3.Connection, *, mint: str,
                       raw_completed_at: float,
                       segment: str, hard_fails: list[str], risk_score: float,
                       results_json: str, inputs_hash: str) -> int:
    # `segment` and `results_json` are accepted to match the D7 gate's call shape but
    # are NOT YET persisted: v1 safety_reports has only id/mint/checked_at/
    # hard_fails_json/risk_score/inputs_hash (verified against SCHEMA_V1 above). Adding
    # columns for them would need a v4 migration, out of scope for D6 (rugged only).
    if conn.in_transaction:
        raise RuntimeError("childless safety report persistence owns its transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        raw_wall = _validated_p3_causal_wall(raw_completed_at)
        fence_p3_causal_wall(conn, observed_wall=raw_wall)
        report_t = allocate_p3_causal_wall(conn, raw_wall=raw_wall)
        cur = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash)"
            " VALUES (?, ?, ?, ?, ?)",
            (mint, report_t, json.dumps(hard_fails), risk_score, inputs_hash),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return cur.lastrowid


def save_safety_report_with_p3_evidence(
    conn: sqlite3.Connection, *, draft: Any,
) -> tuple[SafetyReport, int, int]:
    """Atomically persist one safety parent and its two required P3 children."""
    from memebot.safety.checks import CheckResult
    from memebot.safety.gate import SafetyReport

    if conn.in_transaction:
        raise RuntimeError("P3 safety report persistence owns its transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        raw_completed_at = _validated_p3_causal_wall(draft.raw_completed_at)
        holder = draft.holder
        holder_source_at = (
            None if holder.holder_observed_at is None
            else _validated_p3_causal_wall(holder.holder_observed_at)
        )
        early_buyer = draft.early_buyer
        early_source_at = (
            None if early_buyer.checked_at is None
            else _validated_p3_causal_wall(early_buyer.checked_at)
        )
        source_envelope = max(
            source_at
            for source_at in (raw_completed_at, holder_source_at, early_source_at)
            if source_at is not None
        )
        fence_p3_causal_wall(conn, observed_wall=source_envelope)
        report_t = allocate_p3_causal_wall(conn, raw_wall=source_envelope)
        hard_fails_json = json.dumps(
            list(draft.hard_fails),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        parent = conn.execute(
            "INSERT INTO safety_reports("
            "mint,checked_at,hard_fails_json,risk_score,inputs_hash) "
            "VALUES (?,?,?,?,?)",
            (
                draft.mint,
                report_t,
                hard_fails_json,
                draft.risk_score,
                draft.safety_inputs_hash,
            ),
        )
        report_id = parent.lastrowid

        holder_observed_at = (
            report_t if holder_source_at is None else holder_source_at
        )
        holder_row = conn.execute(
            "INSERT INTO holder_evidence("
            "safety_report_id,sampled_token_accounts,distinct_non_curve_owners,"
            "top10_non_curve_owner_share_pct,holder_observed_at,unavailable_reason,"
            "inputs_hash) VALUES (?,?,?,?,?,?,?)",
            (
                report_id,
                holder.sampled_token_accounts,
                holder.distinct_non_curve_owners,
                holder.top10_non_curve_owner_share_pct,
                holder_observed_at,
                holder.unavailable_reason,
                holder.inputs_hash,
            ),
        )
        holder_id = holder_row.lastrowid

        early_checked_at = (
            report_t if early_source_at is None else early_source_at
        )
        buyers_json = json.dumps(
            list(early_buyer.buyers),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        early_row = conn.execute(
            "INSERT INTO early_buyer_reads("
            "mint,checked_at,buyers_json,unavailable_reason,inputs_hash,"
            "safety_report_id) VALUES (?,?,?,?,?,?)",
            (
                draft.mint,
                early_checked_at,
                buyers_json,
                early_buyer.unavailable_reason,
                early_buyer.inputs_hash,
                report_id,
            ),
        )
        early_id = early_row.lastrowid

        raw_results = json.loads(draft.results_json)
        if type(raw_results) is not list or any(
            type(result) is not dict for result in raw_results
        ):
            raise ValueError("invalid safety results JSON")
        results = tuple(CheckResult(**result) for result in raw_results)
        report = SafetyReport(
            mint=draft.mint,
            checked_at=report_t,
            segment=draft.segment,
            hard_fails=tuple(draft.hard_fails),
            risk_score=float(draft.risk_score),
            results=results,
            inputs_hash=draft.safety_inputs_hash,
            report_id=report_id,
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return report, holder_id, early_id


def latest_safety_report(conn: sqlite3.Connection, mint: str):
    return conn.execute(
        "SELECT * FROM safety_reports WHERE mint = ? ORDER BY id DESC LIMIT 1",
        (mint,)).fetchone()


_P3_HOLDER_UNAVAILABLE_REASONS = frozenset({
    "holder_mint_supply_unavailable",
    "holder_accounts_unavailable",
    "holder_accounts_empty",
    "holder_owner_resolution_unavailable",
    "holder_owner_resolution_incomplete",
    "holder_curve_owner_unavailable",
    "holder_non_curve_owners_empty",
    "holder_evidence_malformed",
    "holder_check_not_run",
})

# The exact selector ABI has no config argument. Enforce the validated config
# domain here; the resolver applies its active safety_cfg buyer_limit.
_P3_EARLY_BUYER_LIMIT_MAX = 1000
_P3_EARLY_BUYER_UNAVAILABLE_REASONS = frozenset({
    "rpc_error",
    "no_signatures",
    "no_matching_buy_events",
    "missing_bonding_curve_key",
    "owner_resolution_incomplete",
    "reader_unavailable",
    "early_buyer_check_not_run",
    "early_buyer_evidence_malformed",
})


def _is_p3_hash(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _validated_safety_holder_from_row(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    expected_mint: str,
) -> ValidatedSafetyHolder:
    report_id = row["id"]
    mint = row["mint"]
    checked_at = row["checked_at"]
    risk_score = row["risk_score"]
    hard_fails_json = row["hard_fails_json"]
    inputs_hash = row["inputs_hash"]
    if mint != expected_mint:
        raise EvidenceIntegrityError("safety report mint mismatch")
    if (
        type(report_id) is not int
        or report_id <= 0
        or type(mint) is not str
        or not 1 <= len(mint.strip()) <= 128
        or type(checked_at) not in (int, float)
        or not math.isfinite(checked_at)
        or not 0.0 <= checked_at <= 4102444800.0
        or type(risk_score) not in (int, float)
        or not math.isfinite(risk_score)
        or not 0.0 <= risk_score <= 100.0
        or type(hard_fails_json) is not str
        or len(hard_fails_json) > 8192
        or not _is_p3_hash(inputs_hash)
    ):
        raise EvidenceIntegrityError("malformed safety report")

    def reject_constant(token: str) -> object:
        raise ValueError(f"invalid JSON constant: {token}")

    try:
        hard_fails_value = json.loads(
            hard_fails_json, parse_constant=reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EvidenceIntegrityError("malformed safety report") from exc
    if (
        type(hard_fails_value) is not list
        or any(
            type(reason) is not str or not reason.strip()
            for reason in hard_fails_value
        )
    ):
        raise EvidenceIntegrityError("malformed safety report")
    hard_fails = tuple(hard_fails_value)

    holder_rows = conn.execute(
        """SELECT id,safety_report_id,sampled_token_accounts,
distinct_non_curve_owners,top10_non_curve_owner_share_pct,
holder_observed_at,unavailable_reason,inputs_hash
FROM holder_evidence
WHERE safety_report_id=?
LIMIT 2""",
        (report_id,),
    ).fetchall()
    common = {
        "safety_report_id": report_id,
        "mint": mint,
        "checked_at": float(checked_at),
        "risk_score": float(risk_score),
        "hard_fails": hard_fails,
        "safety_inputs_hash": inputs_hash,
    }
    if not holder_rows:
        return ValidatedSafetyHolder(
            **common,
            holder_evidence_id=None,
            holder=None,
            holder_unavailable_reason="holder_evidence_missing",
        )
    holder_row = holder_rows[0]
    (
        holder_id,
        linked_report_id,
        sampled_accounts,
        distinct_owners,
        top10_share,
        holder_observed_at,
        unavailable_reason,
        holder_inputs_hash,
    ) = holder_row
    valid_common = (
        len(holder_rows) == 1
        and type(holder_id) is int
        and holder_id > 0
        and linked_report_id == report_id
        and type(holder_observed_at) in (int, float)
        and math.isfinite(holder_observed_at)
        and 0.0 <= holder_observed_at <= checked_at
        and type(unavailable_reason) is str
        and _is_p3_hash(holder_inputs_hash)
    )
    available = (
        unavailable_reason == ""
        and type(sampled_accounts) is int
        and sampled_accounts > 0
        and type(distinct_owners) is int
        and 1 <= distinct_owners <= sampled_accounts
        and type(top10_share) in (int, float)
        and math.isfinite(top10_share)
        and 0.0 <= top10_share <= 100.0
    )
    unavailable = (
        unavailable_reason in _P3_HOLDER_UNAVAILABLE_REASONS
        and unavailable_reason != ""
        and sampled_accounts is None
        and distinct_owners is None
        and top10_share is None
    )
    if not valid_common or not (available or unavailable):
        return ValidatedSafetyHolder(
            **common,
            holder_evidence_id=None,
            holder=None,
            holder_unavailable_reason="holder_evidence_malformed",
        )
    holder = HolderEvidenceDraft(
        sampled_token_accounts=sampled_accounts,
        distinct_non_curve_owners=distinct_owners,
        top10_non_curve_owner_share_pct=(
            None if top10_share is None else float(top10_share)
        ),
        holder_observed_at=float(holder_observed_at),
        unavailable_reason=unavailable_reason,
        inputs_hash=holder_inputs_hash,
    )
    return ValidatedSafetyHolder(
        **common,
        holder_evidence_id=holder_id,
        holder=holder,
        holder_unavailable_reason=unavailable_reason,
    )


def validated_report_by_id(
    conn: sqlite3.Connection, *, report_id: int, expected_mint: str,
) -> ValidatedSafetyHolder:
    if type(report_id) is not int or report_id <= 0:
        raise ValueError("invalid safety report ID")
    if (
        type(expected_mint) is not str
        or not 1 <= len(expected_mint.strip()) <= 128
    ):
        raise ValueError("invalid expected mint")
    row = conn.execute(
        """SELECT id,mint,checked_at,hard_fails_json,risk_score,inputs_hash
FROM safety_reports
WHERE id=?""",
        (report_id,),
    ).fetchone()
    if row is None:
        raise EvidenceIntegrityError("safety report missing")
    return _validated_safety_holder_from_row(
        conn, row=row, expected_mint=expected_mint,
    )


def validated_latest_report_as_of(
    conn: sqlite3.Connection, *, mint: str, as_of: float,
) -> ValidatedSafetyHolder | None:
    if type(mint) is not str or not 1 <= len(mint.strip()) <= 128:
        raise ValueError("invalid mint")
    validated_as_of = _validated_p3_causal_wall(as_of)
    row = conn.execute(
        """SELECT id,mint,checked_at,hard_fails_json,risk_score,inputs_hash
FROM safety_reports INDEXED BY safety_reports_mint_latest_idx
WHERE mint=?
ORDER BY id DESC LIMIT 1""",
        (mint,),
    ).fetchone()
    if row is None:
        return None
    try:
        validated = _validated_safety_holder_from_row(
            conn, row=row, expected_mint=mint,
        )
    except EvidenceIntegrityError:
        return None
    if validated.checked_at >= validated_as_of:
        return None
    return validated


def validated_early_buyer_for_report(
    conn: sqlite3.Connection,
    *,
    report_id: int,
    expected_mint: str,
    as_of: float,
) -> tuple[int, EarlyBuyerEvidenceDraft] | None:
    """Validate the early-buyer child of an already selected exact report."""
    if type(report_id) is not int or report_id <= 0:
        raise ValueError("invalid safety report ID")
    if (
        type(expected_mint) is not str
        or not 1 <= len(expected_mint.strip()) <= 128
    ):
        raise ValueError("invalid expected mint")
    validated_as_of = _validated_p3_causal_wall(as_of)
    try:
        report = validated_report_by_id(
            conn, report_id=report_id, expected_mint=expected_mint,
        )
    except EvidenceIntegrityError:
        return None
    report_checked_at = report.checked_at

    rows = conn.execute(
        """SELECT id,mint,checked_at,buyers_json,unavailable_reason,
inputs_hash,safety_report_id
FROM early_buyer_reads
WHERE safety_report_id=?
LIMIT 2""",
        (report_id,),
    ).fetchall()
    if len(rows) != 1:
        return None
    row = rows[0]
    early_id = row["id"]
    checked_at = row["checked_at"]
    buyers_json = row["buyers_json"]
    unavailable_reason = row["unavailable_reason"]
    inputs_hash = row["inputs_hash"]
    if (
        type(early_id) is not int
        or early_id <= 0
        or row["mint"] != expected_mint
        or row["safety_report_id"] != report_id
        or type(checked_at) not in (int, float)
        or not math.isfinite(checked_at)
        or not 0.0 <= checked_at <= report_checked_at
        or checked_at >= validated_as_of
        or type(buyers_json) is not str
        or len(buyers_json) > 8192
        or type(unavailable_reason) is not str
        or not _is_p3_hash(inputs_hash)
    ):
        return None

    def reject_constant(token: str) -> object:
        raise ValueError(f"invalid JSON constant: {token}")

    try:
        buyers_value = json.loads(
            buyers_json, parse_constant=reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if (
        type(buyers_value) is not list
        or any(
            type(buyer) is not str or not buyer.strip()
            for buyer in buyers_value
        )
        or len(buyers_value) != len(set(buyers_value))
        or buyers_json != json.dumps(
            buyers_value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    ):
        return None
    if unavailable_reason == "":
        if not 1 <= len(buyers_value) <= _P3_EARLY_BUYER_LIMIT_MAX:
            return None
    elif (
        unavailable_reason not in _P3_EARLY_BUYER_UNAVAILABLE_REASONS
        or buyers_value
    ):
        return None
    return (
        early_id,
        EarlyBuyerEvidenceDraft(
            checked_at=float(checked_at),
            buyers=tuple(buyers_value),
            unavailable_reason=unavailable_reason,
            inputs_hash=inputs_hash,
        ),
    )


def validated_smart_wallets_for_buyers(
    conn: sqlite3.Connection,
    *,
    buyers: Sequence[str],
    as_of: float,
    max_buyers: int,
    min_events: int,
    min_realized_pnl_sol: float,
) -> Mapping[str, Mapping[str, float | int]] | None:
    """Select smart wallets from bounded per-wallet operational summaries."""
    if type(max_buyers) is not int or not 1 <= max_buyers <= 1_000:
        raise ValueError("invalid buyer limit")
    if isinstance(buyers, (str, bytes)) or not isinstance(buyers, Sequence):
        raise ValueError("invalid buyers")
    buyer_count = len(buyers)
    if buyer_count > max_buyers:
        raise ValueError("invalid buyers")
    try:
        selected_buyers = tuple(buyers[index] for index in range(buyer_count))
    except IndexError as exc:
        raise ValueError("invalid buyers") from exc
    try:
        buyers[buyer_count]
    except IndexError:
        pass
    else:
        raise ValueError("invalid buyers")
    if (
        len(selected_buyers) != buyer_count
        or any(
            type(buyer) is not str or not 1 <= len(buyer.strip()) <= 128
            for buyer in selected_buyers
        )
        or len(selected_buyers) != len(set(selected_buyers))
    ):
        raise ValueError("invalid buyers")
    validated_as_of = _validated_p3_causal_wall(as_of)
    if type(min_events) is not int or min_events <= 0:
        raise ValueError("invalid minimum event count")
    if (
        type(min_realized_pnl_sol) not in (int, float)
        or not -1_000_000_000_000.0
        <= min_realized_pnl_sol
        <= 1_000_000_000_000.0
        or not math.isfinite(min_realized_pnl_sol)
    ):
        raise ValueError("invalid minimum realized PnL")

    smart_wallets: dict[str, Mapping[str, float | int]] = {}
    for buyer in selected_buyers:
        row = conn.execute(
            """SELECT s.wallet,s.event_count,s.realized_pnl_sol,s.last_at,
s.last_event_id,e.id,e.wallet,e.at,e.realized_pnl_sol
FROM wallet_pnl_summary AS s
LEFT JOIN wallet_pnl_events AS e ON e.id=s.last_event_id
WHERE s.wallet=?""",
            (buyer,),
        ).fetchone()
        if row is None:
            continue
        (
            wallet,
            event_count,
            realized_pnl_sol,
            last_at,
            last_event_id,
            event_id,
            event_wallet,
            event_at,
            event_realized_pnl_sol,
        ) = row
        if (
            wallet != buyer
            or type(wallet) is not str
            or not 1 <= len(wallet.strip()) <= 128
            or type(event_count) is not int
            or event_count <= 0
            or type(realized_pnl_sol) not in (int, float)
            or not math.isfinite(realized_pnl_sol)
            or not -1_000_000_000_000.0
            <= realized_pnl_sol
            <= 1_000_000_000_000.0
            or type(last_at) not in (int, float)
            or not math.isfinite(last_at)
            or not 0.0 <= last_at < validated_as_of
            or type(last_event_id) is not int
            or last_event_id <= 0
            or event_count > last_event_id
            or event_id != last_event_id
            or event_wallet != wallet
            or type(event_at) not in (int, float)
            or not math.isfinite(event_at)
            or not 0.0 <= event_at <= last_at
            or type(event_realized_pnl_sol) not in (int, float)
            or not math.isfinite(event_realized_pnl_sol)
            or not -1_000_000_000_000.0
            <= event_realized_pnl_sol
            <= 1_000_000_000_000.0
            or event_count == 1
            and (
                realized_pnl_sol != event_realized_pnl_sol
                or last_at != event_at
            )
        ):
            return None
        if (
            event_count >= min_events
            and realized_pnl_sol >= min_realized_pnl_sol
        ):
            smart_wallets[buyer] = {
                "events": event_count,
                "realized_pnl_sol": float(realized_pnl_sol),
            }
    return smart_wallets


@dataclass(frozen=True, slots=True)
class PendingSafetyPassPage:
    rows: tuple[sqlite3.Row, ...]
    next_before_id: int | None
    raw_overflow: bool
    exhausted: bool


def pending_safety_passes_for_scoring(
    conn: sqlite3.Connection, *, limit: int, scan_cap: int,
    before_id: int | None, now: float, stale_after_s: float,
) -> PendingSafetyPassPage:
    """Read one bounded newest-first raw safety page, then test eligibility."""
    if type(limit) is not int or limit <= 0:
        raise ValueError("limit must be a positive integer")
    if type(scan_cap) is not int or scan_cap <= 0:
        raise ValueError("scan_cap must be a positive integer")
    if before_id is not None and (type(before_id) is not int or before_id <= 0):
        raise ValueError("before_id must be a positive integer or None")
    if (type(now) not in (int, float) or not math.isfinite(now)
            or type(stale_after_s) not in (int, float)
            or not math.isfinite(stale_after_s) or stale_after_s < 0.0):
        raise ValueError("recovery time bounds must be finite")

    raw_limit = scan_cap + 1
    cursor_clause = "" if before_id is None else " AND sr.id < ?"
    cursor_args = () if before_id is None else (before_id,)
    rows = conn.execute(
        "WITH raw_page AS MATERIALIZED ("  # nosec B608
        " SELECT sr.* FROM safety_reports AS sr"
        " INDEXED BY safety_reports_pending_scoring_idx"
        " WHERE json_array_length(sr.hard_fails_json) = 0"
        f"{cursor_clause}"
        f" ORDER BY sr.id DESC LIMIT {raw_limit}"
        ") SELECT raw_page.*, CASE WHEN"
        " typeof(raw_page.checked_at) IN ('integer','real')"
        " AND raw_page.checked_at BETWEEN 0.0 AND 4102444800.0"
        " AND raw_page.checked_at >= ?"
        " AND EXISTS (SELECT 1 FROM tokens AS t"
        "             INDEXED BY sqlite_autoindex_tokens_1"
        "             WHERE t.mint = raw_page.mint"
        "             AND t.state = 'CLIMBING' AND t.rugged = 0)"
        " AND raw_page.id = (SELECT latest.id FROM safety_reports AS latest"
        "                    INDEXED BY safety_reports_mint_latest_idx"
        "                    WHERE latest.mint = raw_page.mint"
        "                    ORDER BY latest.id DESC LIMIT 1)"
        " AND NOT EXISTS (SELECT 1 FROM decisions AS d"
        "                 INDEXED BY decisions_climbing_mint_idx"
        "                 WHERE d.mint = raw_page.mint"
        "                 AND d.segment = 'CLIMBING')"
        " THEN 1 ELSE 0 END AS recovery_eligible"
        " FROM raw_page",
        (*cursor_args, float(now) - float(stale_after_s)),
    ).fetchall()
    rows.sort(key=lambda row: -row["id"])
    raw_overflow = len(rows) > scan_cap
    raw_window = rows[:scan_cap]
    eligible: list[sqlite3.Row] = []
    processed = 0
    for row in raw_window:
        processed += 1
        if row["recovery_eligible"]:
            eligible.append(row)
            if len(eligible) == limit:
                break

    has_unprocessed = processed < len(rows)
    exhausted = not has_unprocessed
    next_before_id = raw_window[processed - 1]["id"] if has_unprocessed else None
    return PendingSafetyPassPage(
        rows=tuple(eligible), next_before_id=next_before_id,
        raw_overflow=raw_overflow, exhausted=exhausted,
    )


def creator_rug_history(conn: sqlite3.Connection, creator: str) -> int:
    if not creator:
        return 0
    return conn.execute(
        "SELECT COUNT(*) FROM tokens WHERE rugged = 1"
        " AND json_extract(meta_json, '$.creator') = ?", (creator,)).fetchone()[0]


_P3_CANONICAL_SUBJECT_ONLY_REASONS = frozenset({
    "canonical_target_not_live",
    "canonical_identity_unavailable",
    "canonical_identity_conflict",
    "canonical_cluster_too_large",
})
_P3_CANONICAL_FULL_UNRESOLVED_REASONS = frozenset({
    "canonical_target_report_superseded",
    "canonical_holder_evidence_unavailable",
    "canonical_creator_history_overflow",
    "canonical_liquidity_unavailable",
})
_P3_START_UNAVAILABLE_REASONS = frozenset({
    "start_price_missing",
    "start_price_stale",
    "start_price_malformed",
})
_P3_RANKING_INPUT_KEYS = frozenset({
    "subject_mint",
    "target_report_id",
    "latest_target_report_id",
    "resolved_at",
    "cluster_key",
    "resolver_version",
    "weights_version",
    "config_hash",
    "counterfactual_horizons_s",
    "limits",
    "component_parameters",
    "weights_bps",
    "social_weights_bps",
    "candidates",
})
_P3_LIMIT_KEYS = frozenset({
    "max_cluster_candidates",
    "liquidity_max_age_s",
    "holder_max_age_s",
    "comparison_price_max_age_s",
})
_P3_COMPONENT_PARAMETER_KEYS = frozenset({
    "graduation_sol",
    "holder_owner_target",
    "top10_holder_max_pct",
    "token_decimals",
    "creator_reputation_as_of",
})
_P3_COMPONENT_KEYS = frozenset({
    "first_mover", "liquidity", "holder", "creator", "social",
})
_P3_SOCIAL_KEYS = frozenset({"uri", "website", "twitter", "telegram"})
_P3_SOCIAL_DIAGNOSTIC_KEYS = frozenset({
    "value", "present", "reuse", "cluster_conflict", "metadata_conflict",
})
_P3_CANDIDATE_KEYS = frozenset({
    "mint",
    "p3_identity_ingested_at",
    "state",
    "rugged",
    "normalized_name",
    "normalized_symbol",
    "creator",
    "identity_observed_at",
    "identity_conflicts",
    "eligible",
    "ineligible_reason",
    "safety_report_id",
    "safety_checked_at",
    "safety_inputs_hash",
    "safety_hard_fails",
    "safety_risk_score",
    "holder_evidence_id",
    "holder_inputs_hash",
    "holder_observed_at",
    "liquidity_source",
    "liquidity_observed_at",
    "raw",
    "components_ppm",
    "rank_points",
    "rank",
})
_P3_RAW_KEYS = frozenset({
    "liquidity_sol",
    "curve_progress_pct",
    "curve_snapshot",
    "sampled_token_accounts",
    "distinct_non_curve_owners",
    "top10_non_curve_owner_share_pct",
    "creator_prior_successes",
    "creator_prior_rugs",
    "creator_reputation_event_ids",
    "social",
})
_P3_CURVE_SNAPSHOT_KEYS = frozenset({
    "t_wall",
    "t_mono",
    "virtual_sol_reserves",
    "virtual_token_reserves",
    "real_sol_reserves",
    "real_token_reserves",
    "spot_price_sol",
})
_P3_IDENTITY_FIELDS = frozenset({
    "creator", "name", "symbol", "uri", "website", "twitter", "telegram",
})
_P3_INELIGIBLE_REASONS = frozenset({
    "canonical_identity_conflict",
    "canonical_safety_unavailable",
    "canonical_safety_hard_fail",
    "canonical_holder_evidence_unavailable",
    "canonical_liquidity_unavailable",
    "canonical_creator_history_overflow",
    "canonical_target_report_superseded",
    "canonical_internal_error",
})


def _p3_finite_number(
    value: object,
    *,
    name: str,
    minimum: float,
    maximum: float,
    strict_minimum: bool = False,
) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    numeric = float(value)
    if (
        numeric <= minimum if strict_minimum else numeric < minimum
    ) or numeric > maximum:
        raise ValueError(f"{name} is out of range")
    return numeric


def _p3_validate_payload_bound(feature_vector: Mapping[str, object]) -> int:
    """Reject oversized candidate populations before JSON copies are made."""
    canonical = feature_vector.get("canonical")
    if type(canonical) is not dict:
        raise ValueError("feature_vector must contain a canonical object")
    ranking_inputs = canonical.get("ranking_inputs")
    if (
        type(ranking_inputs) is not dict
        or set(ranking_inputs) != _P3_RANKING_INPUT_KEYS
    ):
        raise ValueError("canonical ranking_inputs must have exact keys")
    limits = ranking_inputs["limits"]
    if type(limits) is not dict or set(limits) != _P3_LIMIT_KEYS:
        raise ValueError("canonical limits must have exact keys")
    max_candidates = limits["max_cluster_candidates"]
    if type(max_candidates) is not int or not 1 <= max_candidates <= 500:
        raise ValueError("max_cluster_candidates must be in [1,500]")
    candidates = ranking_inputs["candidates"]
    if type(candidates) is not list:
        raise ValueError("canonical candidates must be a list")
    if len(candidates) > max_candidates:
        raise ValueError("canonical candidates exceed max_cluster_candidates")
    return max_candidates


def _p3_validate_ranking_configuration(
    ranking_inputs: Mapping[str, object], *, at: float,
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    int,
]:
    limits = ranking_inputs["limits"]
    parameters = ranking_inputs["component_parameters"]
    weights = ranking_inputs["weights_bps"]
    social_weights = ranking_inputs["social_weights_bps"]
    if type(limits) is not dict or set(limits) != _P3_LIMIT_KEYS:
        raise ValueError("canonical limits must have exact keys")
    if (
        type(parameters) is not dict
        or set(parameters) != _P3_COMPONENT_PARAMETER_KEYS
    ):
        raise ValueError("canonical component_parameters must have exact keys")
    if type(weights) is not dict or set(weights) != _P3_COMPONENT_KEYS:
        raise ValueError("canonical weights_bps must have exact keys")
    if (
        type(social_weights) is not dict
        or set(social_weights) != _P3_SOCIAL_KEYS
    ):
        raise ValueError("canonical social_weights_bps must have exact keys")

    max_candidates = limits["max_cluster_candidates"]
    if type(max_candidates) is not int or not 1 <= max_candidates <= 500:
        raise ValueError("max_cluster_candidates must be in [1,500]")
    for name in (
        "liquidity_max_age_s",
        "holder_max_age_s",
        "comparison_price_max_age_s",
    ):
        _p3_finite_number(
            limits[name],
            name=name,
            minimum=0.0,
            maximum=1e100,
            strict_minimum=True,
        )
    _p3_finite_number(
        parameters["graduation_sol"],
        name="graduation_sol",
        minimum=0.0,
        maximum=1e100,
        strict_minimum=True,
    )
    if parameters["holder_owner_target"] != 20:
        raise ValueError("holder_owner_target must be exactly 20")
    _p3_finite_number(
        parameters["top10_holder_max_pct"],
        name="top10_holder_max_pct",
        minimum=0.0,
        maximum=100.0,
        strict_minimum=True,
    )
    if (
        type(parameters["token_decimals"]) is not int
        or parameters["token_decimals"] < 0
    ):
        raise ValueError("token_decimals must be a nonnegative integer")
    if (
        type(parameters["creator_reputation_as_of"]) not in (int, float)
        or not math.isfinite(parameters["creator_reputation_as_of"])
        or parameters["creator_reputation_as_of"] != at
    ):
        raise ValueError("creator_reputation_as_of must equal decision T")

    # The pure units perform exact key, integer-BPS, and 10,000-sum checks.
    integer_rank_points(
        components={name: 0 for name in _P3_COMPONENT_KEYS},
        weights_bps=weights,
    )
    social_component(
        candidate_values={name: None for name in _P3_SOCIAL_KEYS},
        eligible_values=(),
        metadata_conflicts=(),
        social_weights_bps=social_weights,
    )
    return limits, parameters, weights, social_weights, max_candidates


def _p3_is_malformed_peer_identity(
    candidate: Mapping[str, object],
) -> bool:
    return (
        candidate["eligible"] is False
        and candidate["ineligible_reason"] == "canonical_internal_error"
        and candidate["normalized_name"] == ""
        and candidate["normalized_symbol"] == ""
        and candidate["creator"] == ""
        and candidate["identity_observed_at"] == {}
        and candidate["identity_conflicts"] == []
    )


def _p3_validate_candidate_base(
    candidate: dict[str, object],
    *,
    at: float,
    cluster_key: str,
    limits: Mapping[str, object],
) -> None:
    if set(candidate) != _P3_CANDIDATE_KEYS:
        raise ValueError("canonical candidate must have exact keys")
    mint = candidate["mint"]
    identity_at = candidate["p3_identity_ingested_at"]
    observed = candidate["identity_observed_at"]
    conflicts = candidate["identity_conflicts"]
    raw = candidate["raw"]
    components = candidate["components_ppm"]
    malformed_peer_identity = _p3_is_malformed_peer_identity(candidate)
    normal_identity = (
        type(candidate["normalized_name"]) is str
        and bool(candidate["normalized_name"])
        and normalize_identity(candidate["normalized_name"])
        == candidate["normalized_name"]
        and type(candidate["normalized_symbol"]) is str
        and bool(candidate["normalized_symbol"])
        and normalize_identity(candidate["normalized_symbol"])
        == candidate["normalized_symbol"]
        and (
            f"{candidate['normalized_name']}:{candidate['normalized_symbol']}"
            == cluster_key
        )
        and type(observed) is dict
        and {"name", "symbol"} <= set(observed) <= _P3_IDENTITY_FIELDS
    )
    if (
        type(mint) is not str
        or not 1 <= len(mint.strip()) <= 128
        or candidate["state"] not in ("FRESH", "CLIMBING")
        or type(candidate["rugged"]) is not int
        or candidate["rugged"] != 0
        or not (normal_identity or malformed_peer_identity)
        or type(candidate["creator"]) is not str
        or len(candidate["creator"]) > 65_536
        or type(candidate["eligible"]) is not bool
        or type(candidate["ineligible_reason"]) is not str
        or type(observed) is not dict
        or type(conflicts) is not list
        or any(
            type(field) is not str or field not in _P3_IDENTITY_FIELDS
            for field in conflicts
        )
        or conflicts != sorted(conflicts)
        or len(conflicts) != len(set(conflicts))
        or type(raw) is not dict
        or set(raw) != _P3_RAW_KEYS
        or type(components) is not dict
    ):
        raise ValueError("invalid canonical candidate diagnostics")
    _p3_finite_number(
        identity_at,
        name="p3_identity_ingested_at",
        minimum=0.0,
        maximum=at,
    )
    if identity_at >= at:
        raise ValueError("p3_identity_ingested_at must precede decision T")
    for field, observed_at in observed.items():
        if field not in _P3_IDENTITY_FIELDS:
            raise ValueError("identity_observed_at has an unknown field")
        _p3_finite_number(
            observed_at,
            name=f"identity_observed_at.{field}",
            minimum=0.0,
            maximum=at,
        )

    if not candidate["eligible"]:
        evidence_keys = (
            "safety_report_id",
            "safety_checked_at",
            "safety_inputs_hash",
            "safety_hard_fails",
            "safety_risk_score",
            "holder_evidence_id",
            "holder_inputs_hash",
            "holder_observed_at",
            "liquidity_source",
            "liquidity_observed_at",
        )
        if (
            candidate["ineligible_reason"] not in _P3_INELIGIBLE_REASONS
            or any(candidate[key] is not None for key in evidence_keys)
            or any(value is not None for value in raw.values())
            or components
            or candidate["rank_points"] is not None
            or candidate["rank"] is not None
        ):
            raise ValueError("invalid ineligible canonical candidate shape")
        return

    if candidate["ineligible_reason"] != "":
        raise ValueError("eligible canonical candidate has an ineligible reason")
    safety_report_id = candidate["safety_report_id"]
    holder_evidence_id = candidate["holder_evidence_id"]
    safety_checked_at = candidate["safety_checked_at"]
    holder_observed_at = candidate["holder_observed_at"]
    liquidity_observed_at = candidate["liquidity_observed_at"]
    if (
        type(safety_report_id) is not int
        or safety_report_id <= 0
        or type(holder_evidence_id) is not int
        or holder_evidence_id <= 0
        or not _is_p3_hash(candidate["safety_inputs_hash"])
        or candidate["safety_hard_fails"] != []
        or not _is_p3_hash(candidate["holder_inputs_hash"])
        or candidate["liquidity_source"] != "curve_snapshot"
        or set(components) != _P3_COMPONENT_KEYS
        or any(
            type(value) is not int or not 0 <= value <= 1_000_000
            for value in components.values()
        )
        or type(candidate["rank_points"]) is not int
        or not 0 <= candidate["rank_points"] <= 10_000_000_000
        or type(candidate["rank"]) is not int
        or candidate["rank"] <= 0
    ):
        raise ValueError("invalid eligible canonical evidence")
    _p3_finite_number(
        safety_checked_at,
        name="safety_checked_at",
        minimum=0.0,
        maximum=at,
    )
    if safety_checked_at >= at:
        raise ValueError("safety_checked_at must precede decision T")
    _p3_finite_number(
        holder_observed_at,
        name="holder_observed_at",
        minimum=0.0,
        maximum=float(safety_checked_at),
    )
    _p3_finite_number(
        liquidity_observed_at,
        name="liquidity_observed_at",
        minimum=0.0,
        maximum=at,
    )
    if (
        at - float(holder_observed_at) > limits["holder_max_age_s"]
        or at - float(liquidity_observed_at) > limits["liquidity_max_age_s"]
    ):
        raise ValueError("eligible canonical evidence is stale")
    _p3_finite_number(
        candidate["safety_risk_score"],
        name="safety_risk_score",
        minimum=0.0,
        maximum=100.0,
    )
    _p3_validate_eligible_raw(candidate, at=at)


def _p3_validate_eligible_raw(
    candidate: Mapping[str, object], *, at: float,
) -> None:
    raw = candidate["raw"]
    snapshot = raw["curve_snapshot"]
    social = raw["social"]
    if (
        type(snapshot) is not dict
        or set(snapshot) != _P3_CURVE_SNAPSHOT_KEYS
        or type(social) is not dict
        or set(social) != _P3_SOCIAL_KEYS
        or type(raw["sampled_token_accounts"]) is not int
        or raw["sampled_token_accounts"] <= 0
        or type(raw["distinct_non_curve_owners"]) is not int
        or not 1
        <= raw["distinct_non_curve_owners"]
        <= raw["sampled_token_accounts"]
        or type(raw["creator_prior_successes"]) is not int
        or raw["creator_prior_successes"] < 0
        or type(raw["creator_prior_rugs"]) is not int
        or raw["creator_prior_rugs"] < 0
    ):
        raise ValueError("invalid eligible canonical raw diagnostics")
    _p3_finite_number(
        raw["liquidity_sol"],
        name="liquidity_sol",
        minimum=0.0,
        maximum=1e100,
    )
    _p3_finite_number(
        raw["curve_progress_pct"],
        name="curve_progress_pct",
        minimum=0.0,
        maximum=100.0,
    )
    _p3_finite_number(
        raw["top10_non_curve_owner_share_pct"],
        name="top10_non_curve_owner_share_pct",
        minimum=0.0,
        maximum=100.0,
    )
    _p3_finite_number(
        snapshot["t_wall"],
        name="curve_snapshot.t_wall",
        minimum=0.0,
        maximum=at,
    )
    if (
        type(snapshot["t_mono"]) not in (int, float)
        or not math.isfinite(snapshot["t_mono"])
    ):
        raise ValueError("curve_snapshot.t_mono must be finite")
    reserves = (
        snapshot["virtual_sol_reserves"],
        snapshot["virtual_token_reserves"],
        snapshot["real_sol_reserves"],
        snapshot["real_token_reserves"],
    )
    if (
        any(type(value) is not int or value < 0 for value in reserves)
        or snapshot["virtual_sol_reserves"] == 0
        or snapshot["virtual_token_reserves"] == 0
        or snapshot["t_wall"] != candidate["liquidity_observed_at"]
        or raw["liquidity_sol"]
        != snapshot["real_sol_reserves"] / 1_000_000_000
    ):
        raise ValueError("invalid canonical curve snapshot diagnostics")
    _p3_finite_number(
        snapshot["spot_price_sol"],
        name="curve_snapshot.spot_price_sol",
        minimum=0.0,
        maximum=1e100,
        strict_minimum=True,
    )
    event_ids = raw["creator_reputation_event_ids"]
    if (
        type(event_ids) is not list
        or event_ids != sorted(set(event_ids))
        or any(type(value) is not int or value <= 0 for value in event_ids)
        or len(event_ids)
        != raw["creator_prior_successes"] + raw["creator_prior_rugs"]
    ):
        raise ValueError("invalid creator reputation diagnostics")
    for field, diagnostic in social.items():
        if (
            type(diagnostic) is not dict
            or set(diagnostic) != _P3_SOCIAL_DIAGNOSTIC_KEYS
            or (
                diagnostic["value"] is not None
                and (
                    type(diagnostic["value"]) is not str
                    or not diagnostic["value"]
                )
            )
            or type(diagnostic["present"]) is not bool
            or type(diagnostic["reuse"]) is not bool
            or type(diagnostic["cluster_conflict"]) is not bool
            or type(diagnostic["metadata_conflict"]) is not bool
        ):
            raise ValueError(f"invalid {field} social diagnostics")


def _p3_recompute_candidate_ranking(
    candidate_by_mint: Mapping[str, dict[str, object]],
    *,
    parameters: Mapping[str, object],
    weights: Mapping[str, object],
    social_weights: Mapping[str, object],
) -> list[object]:
    eligible = [
        candidate
        for candidate in candidate_by_mint.values()
        if candidate["eligible"]
    ]
    eligible_pairs = tuple(
        (candidate["p3_identity_ingested_at"], candidate["mint"])
        for candidate in eligible
    )
    social_values = tuple(
        {
            field: candidate["raw"]["social"][field]["value"]
            for field in _P3_SOCIAL_KEYS
        }
        for candidate in eligible
    )
    for candidate in eligible:
        candidate_social = {
            field: candidate["raw"]["social"][field]["value"]
            for field in _P3_SOCIAL_KEYS
        }
        conflicts = candidate["identity_conflicts"]
        for field in _P3_SOCIAL_KEYS:
            diagnostic = candidate["raw"]["social"][field]
            value = diagnostic["value"]
            present_values = [
                row[field] for row in social_values if row[field] is not None
            ]
            normalizer = {
                "uri": normalize_uri,
                "website": normalize_website,
                "twitter": normalize_twitter,
                "telegram": normalize_telegram,
            }[field]
            if (
                diagnostic["present"] is not (value is not None)
                or (
                    value is not None
                    and normalizer(value) != value
                )
                or diagnostic["reuse"]
                is not (
                    value is not None and present_values.count(value) > 1
                )
                or diagnostic["cluster_conflict"]
                is not (len(set(present_values)) > 1)
                or diagnostic["metadata_conflict"]
                is not (field in conflicts)
            ):
                raise ValueError("canonical social diagnostics disagree")
        raw = candidate["raw"]
        components = {
            "first_mover": first_mover_component(
                identity_ingested_at=candidate["p3_identity_ingested_at"],
                mint=candidate["mint"],
                eligible_pairs=eligible_pairs,
            ),
            "liquidity": liquidity_component(
                real_sol_locked=raw["liquidity_sol"],
                curve_progress_pct=raw["curve_progress_pct"],
                graduation_sol=parameters["graduation_sol"],
            ),
            "holder": holder_component(
                distinct_non_curve_owners=raw["distinct_non_curve_owners"],
                top10_share_pct=raw["top10_non_curve_owner_share_pct"],
                top10_holder_max_pct=parameters["top10_holder_max_pct"],
            ),
            "creator": creator_component(
                creator=(
                    candidate["creator"]
                    if reputation_creator_eligible(
                        candidate["creator"],
                        conflicted="creator" in conflicts,
                    )
                    else None
                ),
                creator_conflicted="creator" in conflicts,
                prior_successes=raw["creator_prior_successes"],
                prior_rugs=raw["creator_prior_rugs"],
            ),
            "social": social_component(
                candidate_values=candidate_social,
                eligible_values=social_values,
                metadata_conflicts=(
                    field for field in conflicts if field in _P3_SOCIAL_KEYS
                ),
                social_weights_bps=social_weights,
            ),
        }
        expected_ppm = {
            name: quantize_component(component)
            for name, component in components.items()
        }
        if candidate["components_ppm"] != expected_ppm:
            raise ValueError("canonical candidate components_ppm mismatch")
        expected_points = integer_rank_points(
            components=components, weights_bps=weights,
        )
        if candidate["rank_points"] != expected_points:
            raise ValueError("canonical candidate rank_points mismatch")
    return list(
        rank_eligible_candidates(
            tuple(
                {
                    "mint": candidate["mint"],
                    "p3_identity_ingested_at": candidate[
                        "p3_identity_ingested_at"
                    ],
                    "rank_points": candidate["rank_points"],
                }
                for candidate in eligible
            )
        )
    )


def _p3_canonical_json(value: object, *, name: str) -> str:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be canonical JSON data") from exc
    try:
        decoded = json.loads(encoded)
    except json.JSONDecodeError as exc:  # pragma: no cover - dumps made the bytes
        raise ValueError(f"{name} must be canonical JSON data") from exc
    if type(decoded) is not dict:
        raise ValueError(f"{name} must be a mapping")
    return encoded


def _p3_generation_identity(
    canonical: Mapping[str, object],
) -> tuple[str, str | None]:
    cluster_key = canonical.get("cluster_key")
    canonical_mint = canonical.get("canonical_mint")
    resolver_version = canonical.get("resolver_version")
    weights_version = canonical.get("weights_version")
    config_hash = canonical.get("config_hash")
    ranking_inputs = canonical.get("ranking_inputs")
    if (
        type(cluster_key) is not str
        or type(resolver_version) is not str
        or not resolver_version
        or type(weights_version) is not str
        or not weights_version
        or not _is_p3_hash(config_hash)
        or type(ranking_inputs) is not dict
    ):
        raise ValueError("invalid canonical generation identity")
    candidates = ranking_inputs.get("candidates")
    if type(candidates) is not list:
        raise ValueError("invalid canonical generation candidates")

    eligible: list[dict[str, object]] = []
    for candidate in candidates:
        if type(candidate) is not dict or type(candidate.get("eligible")) is not bool:
            raise ValueError("invalid canonical generation candidate")
        if not candidate["eligible"]:
            continue
        candidate_mint = candidate.get("mint")
        safety_report_id = candidate.get("safety_report_id")
        holder_evidence_id = candidate.get("holder_evidence_id")
        if (
            type(candidate_mint) is not str
            or not 1 <= len(candidate_mint.strip()) <= 128
            or type(safety_report_id) is not int
            or safety_report_id <= 0
            or type(holder_evidence_id) is not int
            or holder_evidence_id <= 0
        ):
            raise ValueError("invalid canonical generation candidate")
        eligible.append({
            "mint": candidate_mint,
            "safety_report_id": safety_report_id,
            "holder_evidence_id": holder_evidence_id,
        })

    if canonical_mint is None:
        if eligible:
            raise ValueError("eligible canonical generation requires a winner")
        computed_hash = None
    else:
        if type(canonical_mint) is not str or not 1 <= len(canonical_mint.strip()) <= 128:
            raise ValueError("invalid canonical generation winner")
        computed_hash = canonical_generation_hash(
            cluster_key=cluster_key,
            eligible=eligible,
            canonical_mint=canonical_mint,
            resolver_version=resolver_version,
            weights_version=weights_version,
            config_hash=config_hash,
        )
    signature = json.dumps(
        {
            "cluster_key": cluster_key,
            "eligible": sorted(eligible, key=lambda item: item["mint"]),
            "canonical_mint": canonical_mint,
            "resolver_version": resolver_version,
            "weights_version": weights_version,
            "config_hash": config_hash,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return signature, computed_hash


def _p3_prepare_canonical_decision(
    conn: sqlite3.Connection,
    *,
    at: float,
    mint: str,
    action: str,
    score: float,
    feature_vector: Mapping[str, object],
    safety_report_id: int,
    config_hash: str,
    generation_hash: str | None,
    score_status: str,
    score_weights_version: str,
    score_unavailable_reason: str,
    planned_position_size_sol: float,
) -> tuple[str, float, dict[str, object], dict[str, dict[str, object]], str]:
    if not isinstance(feature_vector, Mapping):
        raise ValueError("feature_vector must be a mapping")
    if type(mint) is not str or not 1 <= len(mint.strip()) <= 128:
        raise ValueError("mint must be non-empty bounded text")
    if action not in ("BUY", "SKIP"):
        raise ValueError("canonical decision action must be BUY or SKIP")
    if type(safety_report_id) is not int or safety_report_id <= 0:
        raise ValueError("safety_report_id must be a positive integer")
    if not _is_p3_hash(config_hash):
        raise ValueError("config_hash must be lowercase 64-hex")
    if (
        type(planned_position_size_sol) not in (int, float)
        or not math.isfinite(planned_position_size_sol)
        or not 0.0 < planned_position_size_sol <= 1e100
    ):
        raise ValueError("planned_position_size_sol must be finite and positive")
    if type(score_weights_version) is not str or not score_weights_version.strip():
        raise ValueError("score_weights_version must be non-empty text")
    if score_status == "VALID":
        if (
            type(score) not in (int, float)
            or not math.isfinite(score)
            or not 0.0 <= score <= 100.0
            or score_unavailable_reason != ""
        ):
            raise ValueError("invalid available score evidence")
        persisted_score = float(score)
    elif score_status == "UNAVAILABLE":
        if (
            type(score) not in (int, float)
            or not math.isfinite(score)
            or score != 0.0
            or score_unavailable_reason not in (
                "score_nonfinite", "score_exception",
            )
            or action != "SKIP"
        ):
            raise ValueError("invalid unavailable score evidence")
        persisted_score = 0.0
    else:
        raise ValueError("invalid score_status")

    bounded_candidates = _p3_validate_payload_bound(feature_vector)
    feature_json = _p3_canonical_json(
        dict(feature_vector), name="feature_vector"
    )
    prepared = json.loads(feature_json)
    canonical = prepared.get("canonical")
    if type(canonical) is not dict:
        raise ValueError("feature_vector must contain a canonical object")
    canonical["planned_size_sol"] = float(planned_position_size_sol)
    prepared["score_status"] = score_status
    prepared["score_weights_version"] = score_weights_version
    prepared["score_unavailable_reason"] = score_unavailable_reason

    expected_canonical_keys = {
        "resolver_version",
        "weights_version",
        "status",
        "reason",
        "resolved_at",
        "cluster_key",
        "cluster_size",
        "eligible_cluster_size",
        "canonical_mint",
        "rank",
        "rank_points",
        "generation_hash",
        "inputs_hash",
        "config_hash",
        "planned_size_sol",
        "ranking_order",
        "ranking_inputs",
    }
    if set(canonical) != expected_canonical_keys:
        raise ValueError("canonical payload has unexpected keys")
    if (
        type(canonical["resolver_version"]) is not str
        or not canonical["resolver_version"]
        or type(canonical["weights_version"]) is not str
        or not canonical["weights_version"]
        or canonical["config_hash"] != config_hash
        or canonical["generation_hash"] != generation_hash
        or not _is_p3_hash(canonical["inputs_hash"])
        or type(canonical["resolved_at"]) not in (int, float)
        or not math.isfinite(canonical["resolved_at"])
        or canonical["resolved_at"] != at
        or type(canonical["cluster_key"]) is not str
        or type(canonical["cluster_size"]) is not int
        or canonical["cluster_size"] < 0
        or type(canonical["eligible_cluster_size"]) is not int
        or not 0
        <= canonical["eligible_cluster_size"]
        <= canonical["cluster_size"]
    ):
        raise ValueError("invalid canonical payload scalar")

    ranking_inputs = canonical["ranking_inputs"]
    if (
        type(ranking_inputs) is not dict
        or set(ranking_inputs) != _P3_RANKING_INPUT_KEYS
    ):
        raise ValueError("canonical ranking_inputs must have exact keys")
    latest_target_report_id = ranking_inputs["latest_target_report_id"]
    if (
        ranking_inputs["subject_mint"] != mint
        or type(ranking_inputs["target_report_id"]) is not int
        or ranking_inputs["target_report_id"] != safety_report_id
        or (
            latest_target_report_id is not None
            and (
                type(latest_target_report_id) is not int
                or latest_target_report_id <= 0
            )
        )
        or type(ranking_inputs["resolved_at"]) not in (int, float)
        or not math.isfinite(ranking_inputs["resolved_at"])
        or ranking_inputs["resolved_at"] != at
        or ranking_inputs["cluster_key"] != canonical["cluster_key"]
        or ranking_inputs["resolver_version"] != canonical["resolver_version"]
        or ranking_inputs["weights_version"] != canonical["weights_version"]
        or ranking_inputs["config_hash"] != config_hash
    ):
        raise ValueError("canonical ranking_inputs disagree with decision")
    horizons = ranking_inputs["counterfactual_horizons_s"]
    if type(horizons) is not list or not 1 <= len(horizons) <= 32:
        raise ValueError("counterfactual horizons must contain 1 to 32 values")
    previous_horizon = 0.0
    for horizon in horizons:
        numeric_horizon = _p3_finite_number(
            horizon,
            name="counterfactual horizon",
            minimum=0.0,
            maximum=1e100,
            strict_minimum=True,
        )
        if numeric_horizon <= previous_horizon:
            raise ValueError("counterfactual horizons must be strictly increasing")
        previous_horizon = numeric_horizon
    (
        limits,
        parameters,
        weights,
        social_weights,
        max_candidates,
    ) = _p3_validate_ranking_configuration(ranking_inputs, at=at)
    if max_candidates != bounded_candidates:
        raise ValueError("max_cluster_candidates changed during preparation")
    ranking_json = json.dumps(
        ranking_inputs,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if hashlib.sha256(ranking_json.encode("utf-8")).hexdigest() != canonical[
        "inputs_hash"
    ]:
        raise ValueError("canonical inputs_hash mismatch")

    candidates = ranking_inputs["candidates"]
    if type(candidates) is not list or len(candidates) > max_candidates:
        raise ValueError("canonical candidates exceed max_cluster_candidates")
    candidate_by_mint: dict[str, dict[str, object]] = {}
    for candidate in candidates:
        if type(candidate) is not dict:
            raise ValueError("canonical candidate must be an object")
        _p3_validate_candidate_base(
            candidate,
            at=at,
            cluster_key=canonical["cluster_key"],
            limits=limits,
        )
        candidate_mint = candidate["mint"]
        if (
            candidate_mint in candidate_by_mint
            or (
                candidate_mint == mint
                and _p3_is_malformed_peer_identity(candidate)
            )
        ):
            raise ValueError("invalid canonical candidate identity")
        candidate_by_mint[candidate_mint] = candidate
    if list(candidate_by_mint) != sorted(candidate_by_mint):
        raise ValueError("canonical candidates must be raw-mint sorted")

    ranked = _p3_recompute_candidate_ranking(
        candidate_by_mint,
        parameters=parameters,
        weights=weights,
        social_weights=social_weights,
    )
    for result in ranked:
        candidate = candidate_by_mint[result.mint]
        if (
            candidate["rank"] != result.rank
            or candidate["rank_points"] != result.rank_points
        ):
            raise ValueError("canonical candidate rank mismatch")
    ranking_order = canonical["ranking_order"]
    if (
        type(ranking_order) is not list
        or ranking_order != [item.mint for item in ranked]
        or canonical["eligible_cluster_size"] != len(ranked)
    ):
        raise ValueError("canonical ranking order mismatch")
    winner = None if not ranked else ranked[0].mint
    if canonical["canonical_mint"] != winner:
        raise ValueError("canonical winner mismatch")

    status = canonical["status"]
    reason = canonical["reason"]
    subject_candidate = candidate_by_mint.get(mint)
    subject_only = (
        reason in _P3_CANONICAL_SUBJECT_ONLY_REASONS
        or (reason == "canonical_internal_error" and not candidates)
    )
    if subject_only:
        expected_cluster_size = (
            max_candidates + 1
            if reason == "canonical_cluster_too_large"
            else 0
        )
        valid_population = (
            not candidates
            and canonical["cluster_size"] == expected_cluster_size
            and canonical["eligible_cluster_size"] == 0
            and ranking_order == []
            and winner is None
        )
    else:
        valid_population = (
            1 <= len(candidates) <= max_candidates
            and canonical["cluster_size"] == len(candidates)
            and subject_candidate is not None
        )
    if not valid_population:
        raise ValueError("canonical cluster size/count mismatch")
    if status == "CANONICAL":
        valid_verdict = (
            reason == "canonical_selected"
            and not subject_only
            and winner == mint
            and type(canonical["rank"]) is int
            and canonical["rank"] == 1
            and type(canonical["rank_points"]) is int
            and subject_candidate is not None
            and canonical["rank_points"] == subject_candidate["rank_points"]
        )
    elif status == "SUPPRESSED":
        valid_verdict = (
            reason == "copycat_cluster"
            and not subject_only
            and winner is not None
            and winner != mint
            and subject_candidate is not None
            and subject_candidate["eligible"] is True
            and type(canonical["rank"]) is int
            and canonical["rank"] == subject_candidate["rank"]
            and canonical["rank"] > 1
            and type(canonical["rank_points"]) is int
            and canonical["rank_points"] == subject_candidate["rank_points"]
            and action == "SKIP"
        )
    elif status == "UNRESOLVED":
        valid_reason = (
            reason in _P3_CANONICAL_SUBJECT_ONLY_REASONS
            or reason in _P3_CANONICAL_FULL_UNRESOLVED_REASONS
            or reason == "canonical_internal_error"
        )
        valid_verdict = (
            valid_reason
            and canonical["rank"] is None
            and canonical["rank_points"] is None
            and action == "SKIP"
            and (
                subject_only
                or (
                    subject_candidate is not None
                    and subject_candidate["eligible"] is False
                    and subject_candidate["rank"] is None
                    and subject_candidate["rank_points"] is None
                    and winner != mint
                )
            )
        )
    else:
        valid_verdict = False
    if not valid_verdict:
        raise ValueError("invalid canonical verdict")

    generation_signature, computed_generation_hash = _p3_generation_identity(
        canonical
    )
    if generation_hash != computed_generation_hash:
        raise ValueError("canonical generation_hash mismatch")
    feature_json = json.dumps(
        prepared, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    _v5_canonical_pending_horizons(conn, feature_json)
    return (
        feature_json,
        persisted_score,
        canonical,
        candidate_by_mint,
        generation_signature,
    )


def _p3_validate_canonical_observations(
    *,
    at: float,
    mint: str,
    canonical: Mapping[str, object],
    candidate_by_mint: Mapping[str, Mapping[str, object]],
    observations: Sequence[CanonicalObservationDraft],
) -> tuple[CanonicalObservationDraft, ...]:
    max_candidates = canonical["ranking_inputs"]["limits"][
        "max_cluster_candidates"
    ]
    if (
        not isinstance(observations, Sequence)
        or isinstance(observations, (str, bytes))
    ):
        raise ValueError("observations must be a sequence of drafts")
    try:
        row_count = len(observations)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError("invalid canonical observation cardinality") from exc
    if not 1 <= row_count <= max_candidates:
        raise ValueError("canonical observation cardinality exceeds bound")
    try:
        rows = tuple(observations[index] for index in range(row_count))
    except (IndexError, KeyError, TypeError) as exc:
        raise ValueError("invalid canonical observation sequence") from exc
    seen_mints: set[str] = set()
    for row in rows:
        if not isinstance(row, CanonicalObservationDraft):
            raise ValueError("invalid canonical observation draft")
        if (
            type(row.mint) is not str
            or not 1 <= len(row.mint.strip()) <= 128
            or row.mint in seen_mints
            or type(row.is_subject) is not bool
            or type(row.is_canonical) is not bool
            or type(row.eligible) is not bool
        ):
            raise ValueError("invalid canonical observation identity")
        seen_mints.add(row.mint)
        if row.unavailable_reason == "":
            available = (
                type(row.start_price_sol) in (int, float)
                and math.isfinite(row.start_price_sol)
                and 0.0 < row.start_price_sol <= 1e100
                and type(row.price_observed_at) in (int, float)
                and math.isfinite(row.price_observed_at)
                and 0.0 <= row.price_observed_at <= at
            )
        else:
            available = (
                row.unavailable_reason in _P3_START_UNAVAILABLE_REASONS
                and row.start_price_sol is None
                and row.price_observed_at is None
            )
        if not available:
            raise ValueError("invalid canonical observation price evidence")

    subjects = [row for row in rows if row.is_subject]
    if len(subjects) != 1 or subjects[0].mint != mint:
        raise ValueError("canonical observation subject mismatch")
    canonical_mint = canonical["canonical_mint"]
    canonical_rows = [row for row in rows if row.is_canonical]
    if canonical_mint is None:
        if canonical_rows:
            raise ValueError("canonical observation winner mismatch")
    elif (
        len(canonical_rows) != 1
        or canonical_rows[0].mint != canonical_mint
        or not canonical_rows[0].eligible
    ):
        raise ValueError("canonical observation winner mismatch")

    reason = canonical["reason"]
    subject_only = (
        reason in _P3_CANONICAL_SUBJECT_ONLY_REASONS
        or (reason == "canonical_internal_error" and not candidate_by_mint)
    )
    if subject_only:
        if (
            len(rows) != 1
            or candidate_by_mint
            or rows[0].eligible
            or rows[0].is_canonical
        ):
            raise ValueError("canonical observation cardinality mismatch")
    else:
        if (
            len(rows) != canonical["cluster_size"]
            or len(rows) != len(candidate_by_mint)
            or seen_mints != set(candidate_by_mint)
        ):
            raise ValueError("canonical observation cardinality mismatch")
        for row in rows:
            if row.eligible is not candidate_by_mint[row.mint]["eligible"]:
                raise ValueError("canonical observation eligibility mismatch")
    return rows


def _p3_validate_existing_generation(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    generation_hash: str,
    generation_signature: str,
) -> None:
    first_decision_id = row["first_decision_id"]
    created_at = row["created_at"]
    if (
        row["generation_hash"] != generation_hash
        or type(first_decision_id) is not int
        or first_decision_id <= 0
        or type(created_at) not in (int, float)
        or not math.isfinite(created_at)
        or not 0.0 <= created_at <= 4102444800.0
    ):
        raise EvidenceIntegrityError("malformed canonical generation")
    first = conn.execute(
        "SELECT id,at,mint,segment,action,score,feature_vector_json,"
        "safety_report_id,config_hash FROM decisions WHERE id=?",
        (first_decision_id,),
    ).fetchone()
    if (
        first is None
        or first["id"] != first_decision_id
        or type(first["at"]) not in (int, float)
        or not math.isfinite(first["at"])
        or first["at"] != created_at
        or type(first["segment"]) is not str
        or not 1 <= len(first["segment"].strip()) <= 64
        or type(first["feature_vector_json"]) is not str
    ):
        raise EvidenceIntegrityError("malformed canonical generation decision")
    try:
        first_feature = json.loads(first["feature_vector_json"])
        if (
            type(first_feature) is not dict
            or first["feature_vector_json"]
            != json.dumps(
                first_feature,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        ):
            raise ValueError("noncanonical JSON")
        first_canonical = first_feature["canonical"]
        if (
            type(first_canonical) is not dict
            or first_canonical["generation_hash"] != generation_hash
            or first_canonical["resolved_at"] != created_at
            or first_canonical["config_hash"] != first["config_hash"]
        ):
            raise ValueError("scalar mismatch")
        (
            validated_feature_json,
            validated_score,
            validated_canonical,
            first_candidate_by_mint,
            first_signature,
        ) = _p3_prepare_canonical_decision(
            conn,
            at=first["at"],
            mint=first["mint"],
            action=first["action"],
            score=first["score"],
            feature_vector=first_feature,
            safety_report_id=first["safety_report_id"],
            config_hash=first["config_hash"],
            generation_hash=generation_hash,
            score_status=first_feature["score_status"],
            score_weights_version=first_feature["score_weights_version"],
            score_unavailable_reason=first_feature[
                "score_unavailable_reason"
            ],
            planned_position_size_sol=first_canonical["planned_size_sol"],
        )
        _, first_computed_hash = _p3_generation_identity(validated_canonical)
        if (
            validated_feature_json != first["feature_vector_json"]
            or validated_score != first["score"]
            or validated_canonical != first_canonical
        ):
            raise ValueError("first decision canonical bytes disagree")

        max_candidates = validated_canonical["ranking_inputs"]["limits"][
            "max_cluster_candidates"
        ]
        persisted_observations = conn.execute(
            "SELECT id,decision_id,mint,observed_at,is_subject,is_canonical,"
            "eligible,start_price_sol,price_observed_at,price_source,"
            "unavailable_reason FROM canonical_observations "
            "WHERE decision_id=? ORDER BY id LIMIT ?",
            (first_decision_id, max_candidates + 1),
        ).fetchall()
        if len(persisted_observations) > max_candidates:
            raise ValueError("first decision observation graph exceeds bound")
        first_observations: list[CanonicalObservationDraft] = []
        for observation in persisted_observations:
            unavailable_reason = observation["unavailable_reason"]
            expected_price_source = (
                "curve_snapshot" if unavailable_reason == "" else ""
            )
            if (
                type(observation["id"]) is not int
                or observation["id"] <= 0
                or observation["decision_id"] != first_decision_id
                or type(observation["observed_at"]) not in (int, float)
                or not math.isfinite(observation["observed_at"])
                or observation["observed_at"] != first["at"]
                or type(observation["is_subject"]) is not int
                or observation["is_subject"] not in (0, 1)
                or type(observation["is_canonical"]) is not int
                or observation["is_canonical"] not in (0, 1)
                or type(observation["eligible"]) is not int
                or observation["eligible"] not in (0, 1)
                or type(observation["price_source"]) is not str
                or observation["price_source"] != expected_price_source
                or type(unavailable_reason) is not str
            ):
                raise ValueError("malformed first decision observation scalar")
            first_observations.append(
                CanonicalObservationDraft(
                    mint=observation["mint"],
                    is_subject=bool(observation["is_subject"]),
                    is_canonical=bool(observation["is_canonical"]),
                    eligible=bool(observation["eligible"]),
                    start_price_sol=observation["start_price_sol"],
                    price_observed_at=observation["price_observed_at"],
                    unavailable_reason=unavailable_reason,
                )
            )
        _p3_validate_canonical_observations(
            at=first["at"],
            mint=first["mint"],
            canonical=validated_canonical,
            candidate_by_mint=first_candidate_by_mint,
            observations=tuple(first_observations),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EvidenceIntegrityError(
            "malformed canonical generation decision"
        ) from exc
    if (
        first_signature != generation_signature
        or first_computed_hash != generation_hash
    ):
        raise EvidenceIntegrityError("canonical generation evidence mismatch")


def _record_claimed_decision_with_canonical_observations(
    conn: sqlite3.Connection,
    *,
    allocated_at: float,
    at: float,
    mint: str,
    segment: str,
    action: str,
    score: float,
    feature_vector: Mapping[str, object],
    safety_report_id: int,
    config_hash: str,
    generation_hash: str | None,
    observations: Sequence[CanonicalObservationDraft],
    score_status: str,
    score_weights_version: str,
    score_unavailable_reason: str,
    planned_position_size_sol: float,
) -> tuple[int, tuple[int, ...], bool]:
    """Execute the already-claimed row-26 writer."""
    decision_at = _validated_p3_causal_wall(at)
    if decision_at != allocated_at:
        raise RuntimeError("decision at is not the transaction's allocated causal T")
    if type(segment) is not str or not 1 <= len(segment.strip()) <= 64:
        raise ValueError("segment must be non-empty bounded text")

    (
        feature_json,
        persisted_score,
        canonical,
        candidate_by_mint,
        generation_signature,
    ) = _p3_prepare_canonical_decision(
        conn,
        at=decision_at,
        mint=mint,
        action=action,
        score=score,
        feature_vector=feature_vector,
        safety_report_id=safety_report_id,
        config_hash=config_hash,
        generation_hash=generation_hash,
        score_status=score_status,
        score_weights_version=score_weights_version,
        score_unavailable_reason=score_unavailable_reason,
        planned_position_size_sol=planned_position_size_sol,
    )
    decision_cursor = conn.execute(
        "INSERT INTO decisions("
        "at,mint,segment,action,score,feature_vector_json,"
        "safety_report_id,config_hash"
        ") VALUES (?,?,?,?,?,?,?,?)",
        (
            decision_at,
            mint,
            segment,
            action,
            persisted_score,
            feature_json,
            safety_report_id,
            config_hash,
        ),
    )
    decision_id = decision_cursor.lastrowid
    if type(decision_id) is not int or decision_id <= 0:
        raise EvidenceIntegrityError("decision ID allocation failed")

    analysis_primary = False
    if generation_hash is not None:
        generation_row = conn.execute(
            "SELECT generation_hash,first_decision_id,created_at "
            "FROM canonical_generations WHERE generation_hash=?",
            (generation_hash,),
        ).fetchone()
        if generation_row is None:
            conn.execute(
                "INSERT INTO canonical_generations("
                "generation_hash,first_decision_id,created_at"
                ") VALUES (?,?,?)",
                (generation_hash, decision_id, decision_at),
            )
            analysis_primary = True
        else:
            _p3_validate_existing_generation(
                conn,
                row=generation_row,
                generation_hash=generation_hash,
                generation_signature=generation_signature,
            )

    observation_rows = _p3_validate_canonical_observations(
        at=decision_at,
        mint=mint,
        canonical=canonical,
        candidate_by_mint=candidate_by_mint,
        observations=observations,
    )
    observation_ids: list[int] = []
    for observation in observation_rows:
        available = observation.unavailable_reason == ""
        cursor = conn.execute(
            "INSERT INTO canonical_observations("
            "decision_id,mint,observed_at,is_subject,is_canonical,eligible,"
            "start_price_sol,price_observed_at,price_source,unavailable_reason"
            ") VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                decision_id,
                observation.mint,
                decision_at,
                int(observation.is_subject),
                int(observation.is_canonical),
                int(observation.eligible),
                observation.start_price_sol,
                observation.price_observed_at,
                "curve_snapshot" if available else "",
                observation.unavailable_reason,
            ),
        )
        observation_id = cursor.lastrowid
        if type(observation_id) is not int or observation_id <= 0:
            raise EvidenceIntegrityError("canonical observation ID allocation failed")
        observation_ids.append(observation_id)
    return decision_id, tuple(observation_ids), analysis_primary


def record_decision_with_canonical_observations(
    conn: sqlite3.Connection,
    *,
    at: float,
    mint: str,
    segment: str,
    action: str,
    score: float,
    feature_vector: Mapping[str, object],
    safety_report_id: int,
    config_hash: str,
    generation_hash: str | None,
    observations: Sequence[CanonicalObservationDraft],
    score_status: str,
    score_weights_version: str,
    score_unavailable_reason: str,
    planned_position_size_sol: float,
) -> tuple[int, tuple[int, ...], bool]:
    """Claim and complete the transaction's sole row-26 writer."""
    state = _P3_IMMEDIATE_STATES.get(conn)
    if not conn.in_transaction or state is None:
        raise RuntimeError(
            "record_decision_with_canonical_observations requires "
            "p3_immediate_transaction"
        )
    if state.owner_thread != get_ident():
        state.poisoned = True
        raise RuntimeError("p3 one-shot transaction owner thread mismatch")
    if state.poisoned or state.writer_claimed:
        state.poisoned = True
        raise RuntimeError("p3 one-shot writer already claimed")
    state.writer_claimed = True
    try:
        if state.allocated_at is None:
            raise RuntimeError(
                "decision at is not the transaction's allocated causal T"
            )
        result = _record_claimed_decision_with_canonical_observations(
            conn,
            allocated_at=state.allocated_at,
            at=at,
            mint=mint,
            segment=segment,
            action=action,
            score=score,
            feature_vector=feature_vector,
            safety_report_id=safety_report_id,
            config_hash=config_hash,
            generation_hash=generation_hash,
            observations=observations,
            score_status=score_status,
            score_weights_version=score_weights_version,
            score_unavailable_reason=score_unavailable_reason,
            planned_position_size_sol=planned_position_size_sol,
        )
    except BaseException:
        state.poisoned = True
        raise
    state.writer_completed = True
    return result


_P3_RECHECK_PAYLOAD_KEYS = frozenset({
    "decision_id",
    "attempt",
    "trigger",
    "trigger_report_id",
    "rechecked_at",
    "fill_event_at",
    "causal_target_report_id",
    "latest_target_report_id",
    "prior_inputs_hash",
    "target_snapshot",
    "verdict",
})
_P3_RECHECK_VERDICT_KEYS = frozenset({
    "status", "reason", "canonical_mint", "inputs_hash",
})
_P3_RECHECK_SNAPSHOT_KEYS = frozenset({
    "t_wall",
    "t_mono",
    "virtual_sol_reserves",
    "virtual_token_reserves",
    "real_sol_reserves",
    "real_token_reserves",
    "liquidity_sol",
    "spot_price_sol",
    "progress_pct",
})


def _prepare_canonical_recheck(
    conn: sqlite3.Connection,
    *,
    decision_id: int,
    attempt: int,
    rechecked_at: float,
    causal_target_report_id: int,
    latest_target_report_id: int | None,
    status: str,
    reason: str,
    canonical_mint: str | None,
    prior_inputs_hash: str,
    recheck_inputs_hash: str,
    payload: Mapping[str, object],
    _validated_latest_report: Mapping[str, object] | None = None,
) -> str:
    if type(decision_id) is not int or decision_id <= 0:
        raise ValueError("decision_id must be a positive integer")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("attempt must be a positive integer")
    at = _p3_finite_number(
        rechecked_at, name="rechecked_at", minimum=0.0, maximum=4102444800.0,
    )
    if type(causal_target_report_id) is not int or causal_target_report_id <= 0:
        raise ValueError("causal_target_report_id must be a positive integer")
    if latest_target_report_id is not None and (
        type(latest_target_report_id) is not int or latest_target_report_id <= 0
    ):
        raise ValueError("latest_target_report_id must be positive or None")
    if status not in ("PASS", "CANCEL"):
        raise ValueError("invalid canonical recheck status")
    if type(reason) is not str or not reason.strip() or "\x00" in reason:
        raise ValueError("canonical recheck reason must be non-empty text")
    if canonical_mint is not None and (
        type(canonical_mint) is not str
        or not 1 <= len(canonical_mint.strip()) <= 128
    ):
        raise ValueError("canonical_mint must be bounded text or None")
    if not _is_p3_hash(prior_inputs_hash) or not _is_p3_hash(recheck_inputs_hash):
        raise ValueError("canonical recheck hashes must be lowercase 64-hex")

    payload_json = _p3_canonical_json(payload, name="canonical recheck payload")
    decoded = json.loads(payload_json)
    if set(decoded) != _P3_RECHECK_PAYLOAD_KEYS:
        raise ValueError("canonical recheck payload has unexpected keys")
    verdict = decoded["verdict"]
    if type(verdict) is not dict or set(verdict) != _P3_RECHECK_VERDICT_KEYS:
        raise ValueError("canonical recheck verdict has unexpected keys")
    if not _is_p3_hash(verdict["inputs_hash"]):
        raise ValueError("canonical recheck verdict hash is invalid")
    if (
        decoded["decision_id"] != decision_id
        or decoded["attempt"] != attempt
        or decoded["rechecked_at"] != at
        or decoded["causal_target_report_id"] != causal_target_report_id
        or decoded["latest_target_report_id"] != latest_target_report_id
        or decoded["prior_inputs_hash"] != prior_inputs_hash
        or verdict["reason"] != reason
        or verdict["canonical_mint"] != canonical_mint
    ):
        raise ValueError("canonical recheck payload disagrees with scalar proof")
    expected_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    if expected_hash != recheck_inputs_hash:
        raise ValueError("canonical recheck inputs hash mismatch")

    trigger = decoded["trigger"]
    trigger_report_id = decoded["trigger_report_id"]
    fill_event_at = decoded["fill_event_at"]
    target_snapshot = decoded["target_snapshot"]
    if trigger == "curve_progress":
        if trigger_report_id is not None or type(target_snapshot) is not dict:
            raise ValueError("invalid curve-progress recheck trigger")
        event_at = _p3_finite_number(
            fill_event_at, name="fill_event_at", minimum=0.0,
            maximum=4102444800.0,
        )
        if event_at > at or set(target_snapshot) != _P3_RECHECK_SNAPSHOT_KEYS:
            raise ValueError("invalid canonical recheck target snapshot")
        if target_snapshot["t_wall"] != event_at:
            raise ValueError("canonical recheck snapshot time mismatch")
        for key in ("t_wall", "liquidity_sol", "spot_price_sol", "progress_pct"):
            _p3_finite_number(
                target_snapshot[key], name=f"target_snapshot.{key}",
                minimum=0.0, maximum=1e100,
                strict_minimum=key == "spot_price_sol",
            )
        _p3_finite_number(
            target_snapshot["t_mono"], name="target_snapshot.t_mono",
            minimum=-1e100, maximum=1e100,
        )
        for key in (
            "virtual_sol_reserves",
            "virtual_token_reserves",
            "real_sol_reserves",
            "real_token_reserves",
        ):
            value = target_snapshot[key]
            if type(value) is not int or value < 0 or (
                key.startswith("virtual_") and value == 0
            ):
                raise ValueError("invalid canonical recheck target reserves")
    elif trigger in ("safety_hard_fail", "restart_safety_hard_fail"):
        if (
            type(trigger_report_id) is not int
            or trigger_report_id <= 0
            or fill_event_at is not None
            or target_snapshot is not None
        ):
            raise ValueError("invalid safety recheck trigger")
    else:
        raise ValueError("invalid canonical recheck trigger")

    decision = conn.execute(
        "SELECT at,mint,action,safety_report_id,feature_vector_json "
        "FROM decisions WHERE id=?",
        (decision_id,),
    ).fetchone()
    if decision is None:
        raise ValueError("canonical recheck decision is unavailable")
    try:
        decision_canonical = json.loads(decision["feature_vector_json"])["canonical"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical recheck decision proof is malformed") from exc
    latest_report = _validated_latest_report
    if latest_report is None:
        latest_report = conn.execute(
            "SELECT id,checked_at FROM safety_reports WHERE mint=? "
            "ORDER BY id DESC LIMIT 1",
            (decision["mint"],),
        ).fetchone()
    common_valid = (
        type(decision_canonical) is dict
        and decision["action"] == "BUY"
        and decision_canonical.get("status") == "CANONICAL"
        and decision_canonical.get("inputs_hash") == prior_inputs_hash
        and decision["safety_report_id"] == causal_target_report_id
        and latest_report is not None
        and latest_report["id"] == latest_target_report_id
        and latest_report["checked_at"] < at
        and (trigger != "curve_progress" or fill_event_at >= decision["at"])
    )
    valid_pass = (
        common_valid
        and status == "PASS"
        and verdict["status"] == "CANONICAL"
        and reason == "canonical_selected"
        and canonical_mint == decision["mint"]
        and latest_target_report_id == causal_target_report_id
    )
    valid_cancel = (
        common_valid
        and status == "CANCEL"
        and verdict["status"] in ("SUPPRESSED", "UNRESOLVED")
    )
    if not (valid_pass or valid_cancel):
        raise ValueError("invalid canonical recheck verdict relationship")
    return payload_json


def record_canonical_recheck(
    conn: sqlite3.Connection,
    *,
    decision_id: int,
    attempt: int,
    rechecked_at: float,
    causal_target_report_id: int,
    latest_target_report_id: int | None,
    status: str,
    reason: str,
    canonical_mint: str | None,
    prior_inputs_hash: str,
    recheck_inputs_hash: str,
    payload: Mapping[str, object],
) -> int:
    """Append one exact pre-fill proof inside the caller's P3 transaction."""
    state = _P3_IMMEDIATE_STATES.get(conn)
    if not conn.in_transaction or state is None:
        raise RuntimeError("record_canonical_recheck requires p3_immediate_transaction")
    if state.owner_thread != get_ident():
        state.poisoned = True
        raise RuntimeError("p3 one-shot transaction owner thread mismatch")
    try:
        if state.poisoned or state.allocated_at is None:
            raise RuntimeError("canonical recheck transaction is not allocated")
        if rechecked_at != state.allocated_at:
            raise RuntimeError("rechecked_at is not the transaction causal T")
        payload_json = _prepare_canonical_recheck(
            conn,
            decision_id=decision_id,
            attempt=attempt,
            rechecked_at=rechecked_at,
            causal_target_report_id=causal_target_report_id,
            latest_target_report_id=latest_target_report_id,
            status=status,
            reason=reason,
            canonical_mint=canonical_mint,
            prior_inputs_hash=prior_inputs_hash,
            recheck_inputs_hash=recheck_inputs_hash,
            payload=payload,
        )
        existing = conn.execute(
            "SELECT * FROM canonical_rechecks WHERE decision_id=? AND attempt=?",
            (decision_id, attempt),
        ).fetchone()
        expected = (
            rechecked_at,
            causal_target_report_id,
            latest_target_report_id,
            status,
            reason,
            canonical_mint,
            prior_inputs_hash,
            recheck_inputs_hash,
            payload_json,
        )
        if existing is not None:
            actual = (
                existing["rechecked_at"],
                existing["causal_target_report_id"],
                existing["latest_target_report_id"],
                existing["status"],
                existing["reason"],
                existing["canonical_mint"],
                existing["prior_inputs_hash"],
                existing["recheck_inputs_hash"],
                existing["payload_json"],
            )
            if actual != expected:
                raise EvidenceIntegrityError("conflicting canonical recheck retry")
            state.writer_claimed = True
            state.writer_completed = True
            return existing["id"]
        if state.writer_claimed:
            raise RuntimeError("p3 one-shot writer already claimed")
        state.writer_claimed = True
        cursor = conn.execute(
            "INSERT INTO canonical_rechecks("
            "decision_id,attempt,rechecked_at,causal_target_report_id,"
            "latest_target_report_id,status,reason,canonical_mint,prior_inputs_hash,"
            "recheck_inputs_hash,payload_json"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                decision_id,
                attempt,
                rechecked_at,
                causal_target_report_id,
                latest_target_report_id,
                status,
                reason,
                canonical_mint,
                prior_inputs_hash,
                recheck_inputs_hash,
                payload_json,
            ),
        )
        recheck_id = cursor.lastrowid
        if type(recheck_id) is not int or recheck_id <= 0:
            raise EvidenceIntegrityError("canonical recheck ID allocation failed")
    except BaseException:
        state.poisoned = True
        raise
    state.writer_completed = True
    return recheck_id


def validate_p3_fill_notional(
    *, qty: float, fill_price: float, planned_size_sol: float,
) -> None:
    quantity = _p3_finite_number(
        qty, name="qty", minimum=0.0, maximum=1e100, strict_minimum=True,
    )
    price = _p3_finite_number(
        fill_price, name="fill_price", minimum=0.0, maximum=1e100,
        strict_minimum=True,
    )
    planned = _p3_finite_number(
        planned_size_sol, name="planned_size_sol", minimum=0.0,
        maximum=1e100, strict_minimum=True,
    )
    notional = quantity * price
    if (
        not math.isfinite(notional)
        or not 0.0 < notional <= 1e100
        or not math.isclose(
            notional, planned, rel_tol=1e-12, abs_tol=1e-12,
        )
    ):
        raise ValueError("P3 fill notional does not match planned size")


def record_canonical_paper_buy(
    conn: sqlite3.Connection,
    *,
    decision_id: int,
    recheck_id: int,
    raw_wall: float,
    mint: str,
    segment: str,
    qty: float,
    quote_price: float,
    fill_price: float,
    fees: Mapping[str, float],
    realism_grade: str,
    planned_size_sol: float,
) -> tuple[int, int]:
    """Atomically append the proof-bearing BUY and its FILLED terminal row."""
    if conn.in_transaction:
        raise RuntimeError("record_canonical_paper_buy owns its transaction")
    if type(decision_id) is not int or decision_id <= 0:
        raise ValueError("decision_id must be a positive integer")
    if type(recheck_id) is not int or recheck_id <= 0:
        raise ValueError("recheck_id must be a positive integer")
    if type(mint) is not str or not 1 <= len(mint.strip()) <= 128:
        raise ValueError("mint must be bounded text")
    if type(segment) is not str or not 1 <= len(segment.strip()) <= 64:
        raise ValueError("segment must be bounded text")
    if type(realism_grade) is not str or not 1 <= len(realism_grade) <= 32:
        raise ValueError("realism_grade must be bounded text")
    quote = _p3_finite_number(
        quote_price, name="quote_price", minimum=0.0, maximum=1e100,
        strict_minimum=True,
    )
    fill = _p3_finite_number(
        fill_price, name="fill_price", minimum=0.0, maximum=1e100,
        strict_minimum=True,
    )
    if fill < quote:
        raise ValueError("P3 BUY fill cannot be better than its quote")
    validate_p3_fill_notional(
        qty=qty, fill_price=fill, planned_size_sol=planned_size_sol,
    )
    if not isinstance(fees, Mapping):
        raise ValueError("fees must be a mapping")
    if any(type(key) is not str for key in fees):
        raise ValueError("fee keys must be text")
    try:
        fees_json = json.dumps(
            dict(fees), sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("fees must be canonical JSON data") from exc
    p3_fee_sum_json(fees_json)
    raw_t = _validated_p3_causal_wall(raw_wall)

    conn.execute("BEGIN IMMEDIATE")
    try:
        proof = conn.execute(
            "SELECT d.at AS decision_at,d.mint AS decision_mint,d.action,"
            "d.safety_report_id,d.feature_vector_json,"
            "cr.decision_id AS recheck_decision_id,cr.rechecked_at,cr.status,"
            "cr.reason,cr.canonical_mint,cr.causal_target_report_id,"
            "cr.latest_target_report_id,cr.prior_inputs_hash,cr.recheck_inputs_hash "
            "FROM decisions d JOIN canonical_rechecks cr ON cr.id=? "
            "WHERE d.id=?",
            (recheck_id, decision_id),
        ).fetchone()
        if proof is None:
            raise ValueError("canonical BUY proof is unavailable")
        try:
            canonical = json.loads(proof["feature_vector_json"])["canonical"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("canonical BUY decision proof is malformed") from exc
        latest_recheck = conn.execute(
            "SELECT id FROM canonical_rechecks WHERE decision_id=? "
            "ORDER BY attempt DESC,id DESC LIMIT 1",
            (decision_id,),
        ).fetchone()
        subject = conn.execute(
            "SELECT 1 FROM canonical_observations "
            "WHERE decision_id=? AND mint=? AND is_subject=1 "
            "AND is_canonical=1 AND eligible=1",
            (decision_id, mint),
        ).fetchone()
        if (
            proof["recheck_decision_id"] != decision_id
            or proof["decision_mint"] != mint
            or proof["action"] != "BUY"
            or type(canonical) is not dict
            or canonical.get("status") != "CANONICAL"
            or canonical.get("planned_size_sol") != planned_size_sol
            or canonical.get("inputs_hash") != proof["prior_inputs_hash"]
            or proof["status"] != "PASS"
            or proof["reason"] != "canonical_selected"
            or proof["canonical_mint"] != mint
            or proof["causal_target_report_id"] != proof["safety_report_id"]
            or proof["latest_target_report_id"] != proof["causal_target_report_id"]
            or latest_recheck is None
            or latest_recheck["id"] != recheck_id
            or subject is None
        ):
            raise EvidenceIntegrityError("canonical BUY proof graph is invalid")
        if conn.execute(
            "SELECT 1 FROM paper_entry_executions WHERE decision_id=?",
            (decision_id,),
        ).fetchone() is not None:
            raise EvidenceIntegrityError("canonical BUY already has a terminal execution")

        processed_at = allocate_p3_causal_wall(conn, raw_wall=raw_t)
        if processed_at <= proof["decision_at"] or processed_at <= proof["rechecked_at"]:
            raise ValueError("canonical BUY processing time is not causal")
        trade_cursor = conn.execute(
            "INSERT INTO paper_trades("
            "decision_id,at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade,canonical_recheck_id,canonical_proof_hash,p3_entry_execution_id"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
            (
                decision_id,
                processed_at,
                mint,
                segment,
                "buy",
                qty,
                quote,
                fill,
                fees_json,
                realism_grade,
                recheck_id,
                proof["recheck_inputs_hash"],
            ),
        )
        trade_id = trade_cursor.lastrowid
        execution_cursor = conn.execute(
            "INSERT INTO paper_entry_executions("
            "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id,"
            "paper_trade_id"
            ") VALUES (?,?,'FILLED','filled',?,?,?)",
            (
                decision_id,
                processed_at,
                planned_size_sol,
                recheck_id,
                trade_id,
            ),
        )
        execution_id = execution_cursor.lastrowid
        if (
            type(trade_id) is not int
            or trade_id <= 0
            or type(execution_id) is not int
            or execution_id <= 0
        ):
            raise EvidenceIntegrityError("canonical BUY graph ID allocation failed")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return trade_id, execution_id


def record_terminal_entry_execution(
    conn: sqlite3.Connection,
    *,
    decision_id: int,
    raw_wall: float,
    status: str,
    reason: str,
    recheck_id: int | None,
) -> int:
    """Append one CANCELLED or restart-ABANDONED terminal entry row."""
    if conn.in_transaction:
        raise RuntimeError("record_terminal_entry_execution owns its transaction")
    if type(decision_id) is not int or decision_id <= 0:
        raise ValueError("decision_id must be a positive integer")
    if status not in ("CANCELLED", "ABANDONED"):
        raise ValueError("terminal entry status must be CANCELLED or ABANDONED")
    if type(reason) is not str or not reason.strip():
        raise ValueError("reason must be non-empty text")
    if recheck_id is not None and (type(recheck_id) is not int or recheck_id <= 0):
        raise ValueError("recheck_id must be a positive integer or None")
    raw_t = _validated_p3_causal_wall(raw_wall)

    conn.execute("BEGIN IMMEDIATE")
    try:
        decision = conn.execute(
            "SELECT at,mint,action,feature_vector_json FROM decisions WHERE id=?",
            (decision_id,),
        ).fetchone()
        if decision is None:
            raise ValueError("terminal entry decision is unavailable")
        try:
            canonical = json.loads(decision["feature_vector_json"])["canonical"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("terminal entry decision proof is malformed") from exc
        if type(canonical) is not dict:
            raise ValueError("terminal entry decision proof is malformed")
        planned_size_sol = _p3_finite_number(
            canonical.get("planned_size_sol"),
            name="planned_size_sol",
            minimum=0.0,
            maximum=1e100,
            strict_minimum=True,
        )
        subject = conn.execute(
            "SELECT 1 FROM canonical_observations "
            "WHERE decision_id=? AND mint=? AND is_subject=1 "
            "AND is_canonical=1 AND eligible=1",
            (decision_id, decision["mint"]),
        ).fetchone()
        if (
            decision["action"] != "BUY"
            or canonical.get("status") != "CANONICAL"
            or subject is None
        ):
            raise EvidenceIntegrityError("terminal entry decision graph is invalid")

        existing = conn.execute(
            "SELECT id,status,reason,planned_size_sol,canonical_recheck_id,"
            "paper_trade_id FROM paper_entry_executions WHERE decision_id=?",
            (decision_id,),
        ).fetchone()
        if existing is not None:
            if (
                existing["status"] != status
                or existing["reason"] != reason
                or existing["planned_size_sol"] != planned_size_sol
                or existing["canonical_recheck_id"] != recheck_id
                or existing["paper_trade_id"] is not None
            ):
                raise EvidenceIntegrityError("conflicting terminal entry execution")
            execution_id = existing["id"]
            conn.commit()
            return execution_id

        latest_recheck = conn.execute(
            "SELECT id,rechecked_at,status,reason FROM canonical_rechecks "
            "WHERE decision_id=? ORDER BY attempt DESC,id DESC LIMIT 1",
            (decision_id,),
        ).fetchone()
        if status == "CANCELLED":
            if (
                latest_recheck is None
                or latest_recheck["id"] != recheck_id
                or latest_recheck["status"] != "CANCEL"
                or latest_recheck["reason"] != reason
            ):
                raise EvidenceIntegrityError("invalid CANCELLED entry execution proof")
        else:
            has_cancel = conn.execute(
                "SELECT 1 FROM canonical_rechecks "
                "WHERE decision_id=? AND status='CANCEL' LIMIT 1",
                (decision_id,),
            ).fetchone()
            before_fill = (
                recheck_id is None
                and latest_recheck is None
                and reason == "restart_before_fill"
            )
            after_pass = (
                latest_recheck is not None
                and recheck_id == latest_recheck["id"]
                and latest_recheck["status"] == "PASS"
                and reason == "restart_after_pass"
            )
            if has_cancel is not None or not (before_fill or after_pass):
                raise EvidenceIntegrityError("invalid ABANDONED entry execution proof")

        causal_floor = max(
            raw_t,
            float(decision["at"]),
            float(latest_recheck["rechecked_at"])
            if latest_recheck is not None and recheck_id == latest_recheck["id"]
            else 0.0,
        )
        processed_at = allocate_p3_causal_wall(
            conn, raw_wall=math.nextafter(causal_floor, math.inf),
        )
        if processed_at <= decision["at"] or (
            latest_recheck is not None
            and recheck_id == latest_recheck["id"]
            and processed_at <= latest_recheck["rechecked_at"]
        ):
            raise ValueError("terminal entry processing time is not causal")
        cursor = conn.execute(
            "INSERT INTO paper_entry_executions("
            "decision_id,at,status,reason,planned_size_sol,canonical_recheck_id,"
            "paper_trade_id) VALUES (?,?,?,?,?,?,NULL)",
            (
                decision_id,
                processed_at,
                status,
                reason,
                planned_size_sol,
                recheck_id,
            ),
        )
        execution_id = cursor.lastrowid
        if type(execution_id) is not int or execution_id <= 0:
            raise EvidenceIntegrityError("terminal entry execution ID allocation failed")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return execution_id


def _validated_restart_latest_safety(
    conn: sqlite3.Connection, *, mint: str, as_of: float,
) -> ValidatedSafetyHolder:
    row = conn.execute(
        "SELECT id FROM safety_reports INDEXED BY safety_reports_mint_latest_idx "
        "WHERE mint=? ORDER BY id DESC LIMIT 1",
        (mint,),
    ).fetchone()
    if row is None:
        raise EvidenceIntegrityError("restart latest safety evidence is missing")
    try:
        report = validated_report_by_id(
            conn, report_id=row["id"], expected_mint=mint,
        )
    except EvidenceIntegrityError as exc:
        raise EvidenceIntegrityError(
            "restart latest safety evidence is malformed"
        ) from exc
    if (
        report.checked_at >= as_of
        or report.holder_evidence_id is None
        or report.holder is None
        or validated_early_buyer_for_report(
            conn,
            report_id=report.safety_report_id,
            expected_mint=mint,
            as_of=as_of,
        ) is None
    ):
        raise EvidenceIntegrityError("restart latest safety evidence is unavailable")
    return report


def reconcile_unmatched_p3_buys(
    conn: sqlite3.Connection, *, raw_wall: float,
) -> int:
    """Fail-closed startup reconciliation for canonical BUY crash windows."""
    if conn.in_transaction:
        raise RuntimeError("reconcile_unmatched_p3_buys owns its transactions")
    raw_t = _validated_p3_causal_wall(raw_wall)
    unmatched = conn.execute(
        "SELECT d.id "
        "FROM decisions AS d "
        "JOIN canonical_observations AS o ON o.decision_id=d.id "
        "AND o.mint=d.mint AND o.is_subject=1 AND o.is_canonical=1 AND o.eligible=1 "
        "LEFT JOIN paper_entry_executions AS e ON e.decision_id=d.id "
        "WHERE d.action='BUY' AND e.id IS NULL ORDER BY d.id"
    ).fetchall()
    reconciled = 0
    for unmatched_decision in unmatched:
        conn.execute("BEGIN IMMEDIATE")
        try:
            restart_at = allocate_p3_causal_wall(conn, raw_wall=raw_t)
            decision = conn.execute(
                "SELECT d.id,d.mint,d.action,d.safety_report_id,"
                "d.feature_vector_json,e.id AS execution_id "
                "FROM decisions AS d "
                "JOIN canonical_observations AS o ON o.decision_id=d.id "
                "AND o.mint=d.mint AND o.is_subject=1 "
                "AND o.is_canonical=1 AND o.eligible=1 "
                "LEFT JOIN paper_entry_executions AS e ON e.decision_id=d.id "
                "WHERE d.id=?",
                (unmatched_decision["id"],),
            ).fetchone()
            if decision is None or decision["execution_id"] is not None:
                raise EvidenceIntegrityError(
                    "restart unmatched canonical decision graph changed"
                )
            try:
                canonical = json.loads(decision["feature_vector_json"])["canonical"]
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise EvidenceIntegrityError(
                    "restart canonical decision proof is malformed"
                ) from exc
            if (
                decision["action"] != "BUY"
                or type(canonical) is not dict
                or canonical.get("status") != "CANONICAL"
                or not _is_p3_hash(canonical.get("inputs_hash"))
                or type(decision["safety_report_id"]) is not int
                or decision["safety_report_id"] <= 0
            ):
                raise EvidenceIntegrityError(
                    "restart canonical decision proof is malformed"
                )

            latest_recheck = conn.execute(
                "SELECT * FROM canonical_rechecks WHERE decision_id=? "
                "ORDER BY attempt DESC,id DESC LIMIT 1",
                (decision["id"],),
            ).fetchone()
            latest_report = _validated_restart_latest_safety(
                conn, mint=decision["mint"], as_of=restart_at,
            )

            terminal_status: str
            terminal_reason: str
            terminal_recheck_id: int | None
            if (
                latest_report.hard_fails
                and latest_report.safety_report_id > decision["safety_report_id"]
            ):
                reason = "restart_safety_hard_fail"
                exact_retry = (
                    latest_recheck is not None
                    and latest_recheck["status"] == "CANCEL"
                    and latest_recheck["reason"] == reason
                    and latest_recheck["latest_target_report_id"]
                    == latest_report.safety_report_id
                )
                attempt = (
                    latest_recheck["attempt"]
                    if exact_retry
                    else 1 if latest_recheck is None
                    else latest_recheck["attempt"] + 1
                )
                rechecked_at = (
                    latest_recheck["rechecked_at"] if exact_retry else restart_at
                )
                payload = {
                    "decision_id": decision["id"],
                    "attempt": attempt,
                    "trigger": reason,
                    "trigger_report_id": latest_report.safety_report_id,
                    "rechecked_at": rechecked_at,
                    "fill_event_at": None,
                    "causal_target_report_id": decision["safety_report_id"],
                    "latest_target_report_id": latest_report.safety_report_id,
                    "prior_inputs_hash": canonical["inputs_hash"],
                    "target_snapshot": None,
                    "verdict": {
                        "status": "UNRESOLVED",
                        "reason": reason,
                        "canonical_mint": None,
                        "inputs_hash": latest_report.safety_inputs_hash,
                    },
                }
                encoded = json.dumps(
                    payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
                )
                recheck_inputs_hash = hashlib.sha256(encoded.encode()).hexdigest()
                payload_json = _prepare_canonical_recheck(
                    conn,
                    decision_id=decision["id"],
                    attempt=attempt,
                    rechecked_at=rechecked_at,
                    causal_target_report_id=decision["safety_report_id"],
                    latest_target_report_id=latest_report.safety_report_id,
                    status="CANCEL",
                    reason=reason,
                    canonical_mint=None,
                    prior_inputs_hash=canonical["inputs_hash"],
                    recheck_inputs_hash=recheck_inputs_hash,
                    payload=payload,
                    _validated_latest_report={
                        "id": latest_report.safety_report_id,
                        "checked_at": latest_report.checked_at,
                    },
                )
                expected = (
                    rechecked_at,
                    decision["safety_report_id"],
                    latest_report.safety_report_id,
                    "CANCEL",
                    reason,
                    None,
                    canonical["inputs_hash"],
                    recheck_inputs_hash,
                    payload_json,
                )
                if exact_retry:
                    actual = (
                        latest_recheck["rechecked_at"],
                        latest_recheck["causal_target_report_id"],
                        latest_recheck["latest_target_report_id"],
                        latest_recheck["status"],
                        latest_recheck["reason"],
                        latest_recheck["canonical_mint"],
                        latest_recheck["prior_inputs_hash"],
                        latest_recheck["recheck_inputs_hash"],
                        latest_recheck["payload_json"],
                    )
                    if actual != expected:
                        raise EvidenceIntegrityError(
                            "conflicting restart hard-fail recheck retry"
                        )
                    recheck_id = latest_recheck["id"]
                else:
                    cursor = conn.execute(
                        "INSERT INTO canonical_rechecks("
                        "decision_id,attempt,rechecked_at,causal_target_report_id,"
                        "latest_target_report_id,status,reason,canonical_mint,"
                        "prior_inputs_hash,recheck_inputs_hash,payload_json"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (decision["id"], attempt, *expected),
                    )
                    recheck_id = cursor.lastrowid
                    if type(recheck_id) is not int or recheck_id <= 0:
                        raise EvidenceIntegrityError(
                            "restart hard-fail recheck ID allocation failed"
                        )
                terminal_status = "CANCELLED"
                terminal_reason = reason
                terminal_recheck_id = recheck_id
            elif latest_recheck is None:
                terminal_status = "ABANDONED"
                terminal_reason = "restart_before_fill"
                terminal_recheck_id = None
            elif latest_recheck["status"] == "PASS":
                terminal_status = "ABANDONED"
                terminal_reason = "restart_after_pass"
                terminal_recheck_id = latest_recheck["id"]
            elif latest_recheck["status"] == "CANCEL":
                terminal_status = "CANCELLED"
                terminal_reason = latest_recheck["reason"]
                terminal_recheck_id = latest_recheck["id"]
            else:
                raise EvidenceIntegrityError(
                    "restart canonical recheck status is invalid"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

        record_terminal_entry_execution(
            conn,
            decision_id=unmatched_decision["id"],
            raw_wall=raw_t,
            status=terminal_status,
            reason=terminal_reason,
            recheck_id=terminal_recheck_id,
        )
        reconciled += 1
    return reconciled


_P3_FINAL_EXIT_REASONS = frozenset({
    "time_stop",
    "trailing_stop",
    "graduated",
    "dead",
    "graduated_no_price",
    "safety_flip",
    "stale",
    "restart_safety_hard_fail",
})


@dataclass(frozen=True, slots=True)
class RestoredP3Position:
    decision_id: int
    mint: str
    entry_execution_id: int
    entry_latest_target_report_id: int
    bought_qty: float
    sold_qty: float
    qty_remaining: float
    entry_price: float
    entry_at: float
    buy_notional_sol: float
    sell_proceeds_sol: float
    ladder_mask: int


def record_canonical_paper_sell(
    conn: sqlite3.Connection,
    *,
    decision_id: int,
    raw_wall: float,
    mint: str,
    segment: str,
    qty: float,
    quote_price: float,
    fill_price: float,
    fees: Mapping[str, float],
    realism_grade: str,
    exit_reason: str,
    ladder_index: int | None,
) -> tuple[int, int | None]:
    """Atomically append a proof-linked SELL and, when final, its outcome."""
    if conn.in_transaction:
        raise RuntimeError("record_canonical_paper_sell owns its transaction")
    if type(decision_id) is not int or decision_id <= 0:
        raise ValueError("decision_id must be a positive integer")
    if type(mint) is not str or not 1 <= len(mint.strip()) <= 128:
        raise ValueError("mint must be bounded text")
    if type(segment) is not str or not 1 <= len(segment.strip()) <= 64:
        raise ValueError("segment must be bounded text")
    if type(realism_grade) is not str or not 1 <= len(realism_grade) <= 32:
        raise ValueError("realism_grade must be bounded text")
    quantity = _p3_finite_number(
        qty, name="qty", minimum=0.0, maximum=1e100, strict_minimum=True,
    )
    quote = _p3_finite_number(
        quote_price, name="quote_price", minimum=0.0, maximum=1e100,
    )
    fill = _p3_finite_number(
        fill_price, name="fill_price", minimum=0.0, maximum=1e100,
    )
    if fill > quote:
        raise ValueError("P3 SELL fill cannot be better than its quote")
    if type(exit_reason) is not str or not exit_reason:
        raise ValueError("exit_reason must be non-empty text")
    if ladder_index is None:
        if exit_reason not in _P3_FINAL_EXIT_REASONS:
            raise ValueError("invalid final P3 exit reason")
    elif (
        type(ladder_index) is not int
        or not 0 <= ladder_index < 62
        or exit_reason != f"ladder_{ladder_index}"
    ):
        raise ValueError("invalid P3 ladder exit")
    if not isinstance(fees, Mapping) or any(type(key) is not str for key in fees):
        raise ValueError("fees must be a text-keyed mapping")
    try:
        fees_json = json.dumps(
            dict(fees), sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("fees must be canonical JSON data") from exc
    p3_fee_sum_json(fees_json)
    raw_t = _validated_p3_causal_wall(raw_wall)

    conn.execute("BEGIN IMMEDIATE")
    try:
        position = conn.execute(
            "SELECT pc.*,e.at AS entry_at,e.status AS entry_status,"
            "e.decision_id AS entry_decision_id,d.mint AS decision_mint,"
            "d.action,d.feature_vector_json "
            "FROM p3_position_current pc "
            "JOIN paper_entry_executions e ON e.id=pc.entry_execution_id "
            "JOIN decisions d ON d.id=pc.decision_id "
            "WHERE pc.decision_id=?",
            (decision_id,),
        ).fetchone()
        subject = conn.execute(
            "SELECT 1 FROM canonical_observations "
            "WHERE decision_id=? AND mint=? AND is_subject=1 "
            "AND is_canonical=1 AND eligible=1",
            (decision_id, mint),
        ).fetchone()
        if position is None:
            raise EvidenceIntegrityError("P3 SELL has no open FILLED entry")
        try:
            canonical = json.loads(position["feature_vector_json"])["canonical"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise EvidenceIntegrityError("P3 SELL decision proof is malformed") from exc
        if (
            position["entry_status"] != "FILLED"
            or position["entry_decision_id"] != decision_id
            or position["mint"] != mint
            or position["decision_mint"] != mint
            or position["action"] != "BUY"
            or type(canonical) is not dict
            or canonical.get("status") != "CANONICAL"
            or subject is None
        ):
            raise EvidenceIntegrityError("P3 SELL entry graph is invalid")
        open_qty = position["bought_qty"] - position["sold_qty"]
        if open_qty == 0.0:
            if ladder_index is not None:
                raise EvidenceIntegrityError("conflicting final P3 SELL retry")
            existing_rows = conn.execute(
                "SELECT pt.id AS sell_id,pt.at,pt.decision_id,pt.mint,pt.segment,"
                "pt.side,pt.qty,pt.quote_price,pt.fill_price,pt.fees_json,"
                "pt.realism_grade,pt.canonical_recheck_id,"
                "pt.canonical_proof_hash,pt.p3_entry_execution_id,"
                "o.id AS outcome_id,o.at AS outcome_at,o.ref_kind,o.ref_id,"
                "o.pnl_sol,o.detail_json,o.p3_exit_trade_id "
                "FROM outcomes AS o "
                "JOIN paper_trades AS pt ON pt.id=o.p3_exit_trade_id "
                "WHERE pt.decision_id=? AND pt.p3_entry_execution_id=? "
                "ORDER BY o.id LIMIT 2",
                (decision_id, position["entry_execution_id"]),
            ).fetchall()
            try:
                detail = (
                    json.loads(existing_rows[0]["detail_json"])
                    if len(existing_rows) == 1 else None
                )
            except (TypeError, json.JSONDecodeError):
                detail = None
            existing = existing_rows[0] if len(existing_rows) == 1 else None
            exact_retry = (
                existing is not None
                and existing["at"] == position["last_trade_at"]
                and existing["decision_id"] == decision_id
                and existing["mint"] == mint
                and existing["segment"] == segment
                and existing["side"] == "sell"
                and existing["qty"] == quantity
                and existing["quote_price"] == quote
                and existing["fill_price"] == fill
                and existing["fees_json"] == fees_json
                and existing["realism_grade"] == realism_grade
                and existing["canonical_recheck_id"] is None
                and existing["canonical_proof_hash"] is None
                and existing["p3_entry_execution_id"]
                == position["entry_execution_id"]
                and existing["outcome_at"] == existing["at"]
                and existing["ref_kind"] == "trade"
                and existing["ref_id"] == existing["sell_id"]
                and existing["p3_exit_trade_id"] == existing["sell_id"]
                and existing["pnl_sol"]
                == position["sell_proceeds_sol"] - position["buy_notional_sol"]
                and type(detail) is dict
                and detail.get("reason") == exit_reason
                and detail.get("grade") == realism_grade
                and detail.get("hold_s") == existing["at"] - position["entry_at"]
                and set(detail) == {"reason", "hold_s", "grade"}
            )
            if not exact_retry:
                raise EvidenceIntegrityError("conflicting final P3 SELL retry")
            conn.commit()
            return existing["sell_id"], existing["outcome_id"]
        if open_qty < 0.0 or quantity > open_qty:
            raise ValueError("P3 SELL quantity exceeds the open position")
        if ladder_index is not None:
            ladder_bit = 1 << ladder_index
            if quantity >= open_qty or position["ladder_mask"] & ladder_bit:
                raise EvidenceIntegrityError("invalid or repeated P3 ladder exit")
        elif quantity != open_qty:
            raise ValueError("final P3 SELL must close the full open position")

        processed_at = allocate_p3_causal_wall(conn, raw_wall=raw_t)
        if processed_at <= position["entry_at"] or processed_at <= position["last_trade_at"]:
            raise ValueError("P3 SELL processing time is not causal")
        trade_cursor = conn.execute(
            "INSERT INTO paper_trades("
            "decision_id,at,mint,segment,side,qty,quote_price,fill_price,fees_json,"
            "realism_grade,canonical_recheck_id,canonical_proof_hash,"
            "p3_entry_execution_id) VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,?)",
            (
                decision_id,
                processed_at,
                mint,
                segment,
                "sell",
                quantity,
                quote,
                fill,
                fees_json,
                realism_grade,
                position["entry_execution_id"],
            ),
        )
        sell_id = trade_cursor.lastrowid
        if type(sell_id) is not int or sell_id <= 0:
            raise EvidenceIntegrityError("P3 SELL ID allocation failed")

        outcome_id = None
        if ladder_index is not None:
            conn.execute(
                "UPDATE p3_position_current SET ladder_mask=ladder_mask | ? "
                "WHERE decision_id=?",
                (ladder_bit, decision_id),
            )
        else:
            closed = conn.execute(
                "SELECT bought_qty,sold_qty,buy_notional_sol,sell_proceeds_sol "
                "FROM p3_position_current WHERE decision_id=?",
                (decision_id,),
            ).fetchone()
            if closed is None or closed["sold_qty"] != closed["bought_qty"]:
                raise EvidenceIntegrityError("final P3 SELL did not close its position")
            pnl_sol = closed["sell_proceeds_sol"] - closed["buy_notional_sol"]
            detail_json = json.dumps(
                {
                    "grade": realism_grade,
                    "hold_s": processed_at - position["entry_at"],
                    "reason": exit_reason,
                },
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            outcome_cursor = conn.execute(
                "INSERT INTO outcomes("
                "at,ref_kind,ref_id,pnl_sol,detail_json,p3_exit_trade_id"
                ") VALUES (?,'trade',?,?,?,?)",
                (processed_at, sell_id, pnl_sol, detail_json, sell_id),
            )
            outcome_id = outcome_cursor.lastrowid
            if type(outcome_id) is not int or outcome_id <= 0:
                raise EvidenceIntegrityError("P3 outcome ID allocation failed")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return sell_id, outcome_id


def list_open_p3_filled_positions(
    conn: sqlite3.Connection,
    *,
    max_open_positions: int,
) -> list[RestoredP3Position]:
    """Load the bounded P3 open-position summary without scanning trade history."""
    if type(max_open_positions) is not int or max_open_positions <= 0:
        raise ValueError("max_open_positions must be a positive integer")
    rows = conn.execute(
        "SELECT pc.*,e.status,e.decision_id AS execution_decision_id,e.at AS entry_at,"
        "e.paper_trade_id,e.canonical_recheck_id,"
        "d.mint AS decision_mint,d.action,d.feature_vector_json,"
        "pt.decision_id AS trade_decision_id,pt.mint AS trade_mint,pt.side,"
        "pt.qty AS entry_qty,pt.quote_price AS entry_quote_price,"
        "pt.fill_price AS entry_price,pt.at AS trade_at,"
        "pt.fees_json,pt.canonical_recheck_id AS trade_recheck_id,"
        "pt.p3_entry_execution_id AS trade_entry_execution_id,"
        "cr.status AS recheck_status,cr.latest_target_report_id,"
        "cr.recheck_inputs_hash,pt.canonical_proof_hash "
        "FROM p3_position_current AS pc INDEXED BY p3_position_current_open_idx "
        "JOIN paper_entry_executions AS e ON e.id=pc.entry_execution_id "
        "JOIN decisions AS d ON d.id=pc.decision_id "
        "JOIN paper_trades AS pt ON pt.id=e.paper_trade_id "
        "JOIN canonical_rechecks AS cr ON cr.id=e.canonical_recheck_id "
        "WHERE pc.sold_qty<pc.bought_qty ORDER BY pc.decision_id LIMIT ?",
        (max_open_positions + 1,),
    ).fetchall()
    if len(rows) > max_open_positions:
        raise EvidenceIntegrityError("open P3 position limit exceeded")
    restored: list[RestoredP3Position] = []
    seen_mints: set[str] = set()
    for row in rows:
        try:
            canonical = json.loads(row["feature_vector_json"])["canonical"]
            fee_sum = p3_fee_sum_json(row["fees_json"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise EvidenceIntegrityError("malformed open P3 position") from exc
        numeric_domain = (
            type(row["bought_qty"]) in (int, float)
            and math.isfinite(row["bought_qty"])
            and 0.0 < row["bought_qty"] <= 1e100
            and type(row["sold_qty"]) in (int, float)
            and math.isfinite(row["sold_qty"])
            and 0.0 <= row["sold_qty"] < row["bought_qty"]
            and type(row["entry_qty"]) in (int, float)
            and math.isfinite(row["entry_qty"])
            and 0.0 < row["entry_qty"] <= 1e100
            and type(row["entry_quote_price"]) in (int, float)
            and math.isfinite(row["entry_quote_price"])
            and 0.0 < row["entry_quote_price"] <= 1e100
            and type(row["entry_price"]) in (int, float)
            and math.isfinite(row["entry_price"])
            and row["entry_quote_price"] <= row["entry_price"] <= 1e100
            and type(row["buy_notional_sol"]) in (int, float)
            and math.isfinite(row["buy_notional_sol"])
            and 0.0 < row["buy_notional_sol"] <= 1e100
            and type(row["sell_proceeds_sol"]) in (int, float)
            and math.isfinite(row["sell_proceeds_sol"])
            and -1e100 <= row["sell_proceeds_sol"] <= 1e100
            and type(row["ladder_mask"]) is int
            and 0 <= row["ladder_mask"] <= 4611686018427387903
            and type(row["entry_at"]) in (int, float)
            and math.isfinite(row["entry_at"])
            and 0.0 <= row["entry_at"] <= 4102444800.0
            and type(row["last_trade_at"]) in (int, float)
            and math.isfinite(row["last_trade_at"])
            and 0.0 <= row["last_trade_at"] <= 4102444800.0
        )
        expected_buy_notional = (
            row["entry_qty"] * row["entry_price"] + fee_sum
            if numeric_domain else math.nan
        )
        financials_match = (
            math.isfinite(expected_buy_notional)
            and row["buy_notional_sol"] == expected_buy_notional
        )
        valid = (
            numeric_domain
            and financials_match
            and row["status"] == "FILLED"
            and row["execution_decision_id"] == row["decision_id"]
            and row["decision_mint"] == row["mint"] == row["trade_mint"]
            and row["action"] == "BUY"
            and type(canonical) is dict
            and canonical.get("status") == "CANONICAL"
            and row["trade_decision_id"] == row["decision_id"]
            and row["side"] == "buy"
            and row["entry_qty"] == row["bought_qty"]
            and row["trade_at"] == row["entry_at"]
            and row["trade_recheck_id"] == row["canonical_recheck_id"]
            and row["trade_entry_execution_id"] is None
            and row["recheck_status"] == "PASS"
            and row["canonical_proof_hash"] == row["recheck_inputs_hash"]
            and row["latest_target_report_id"] is not None
            and row["sold_qty"] >= 0.0
            and row["sold_qty"] < row["bought_qty"]
            and row["last_trade_at"] >= row["entry_at"]
            and row["mint"] not in seen_mints
        )
        if not valid:
            raise EvidenceIntegrityError("invalid open P3 position graph")
        seen_mints.add(row["mint"])
        restored.append(RestoredP3Position(
            decision_id=row["decision_id"],
            mint=row["mint"],
            entry_execution_id=row["entry_execution_id"],
            entry_latest_target_report_id=row["latest_target_report_id"],
            bought_qty=row["bought_qty"],
            sold_qty=row["sold_qty"],
            qty_remaining=row["bought_qty"] - row["sold_qty"],
            entry_price=row["entry_price"],
            entry_at=row["entry_at"],
            buy_notional_sol=row["buy_notional_sol"],
            sell_proceeds_sol=row["sell_proceeds_sol"],
            ladder_mask=row["ladder_mask"],
        ))
    return restored


def record_decision(conn: sqlite3.Connection, *, at: float, mint: str, segment: str,
                    action: str, score: float, feature_vector: dict, config_hash: str,
                    safety_report_id: int | None = None) -> int:
    if "canonical" in feature_vector:
        raise ValueError(
            "canonical payloads require p3_immediate_transaction and "
            "record_decision_with_canonical_observations"
        )
    cur = conn.execute(
        "INSERT INTO decisions(at, mint, segment, action, score, feature_vector_json,"
        " safety_report_id, config_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (at, mint, segment, action, score, _json_ledger.dumps(feature_vector),
         safety_report_id, config_hash))
    conn.commit()
    return cur.lastrowid


def decision_exists(conn: sqlite3.Connection, *, mint: str, segment: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM decisions WHERE mint = ? AND segment = ? LIMIT 1",
        (mint, segment),
    ).fetchone() is not None


def list_pending_canonical_observations(
    conn: sqlite3.Connection,
    *,
    horizons: Sequence[float],
    limit_plus_one: int,
) -> list[sqlite3.Row]:
    """Return incomplete observation rows carrying their decision-time horizons."""
    if type(limit_plus_one) is not int or limit_plus_one <= 0:
        raise ValueError("limit_plus_one must be a positive integer")
    if not isinstance(horizons, Sequence):
        raise ValueError("invalid supplied canonical horizon tuple")
    try:
        supplied_horizon_count = len(horizons)
    except (TypeError, OverflowError) as exc:
        raise ValueError("invalid supplied canonical horizon tuple") from exc
    if not 1 <= supplied_horizon_count <= 32:
        raise ValueError("invalid supplied canonical horizon tuple")
    try:
        supplied_horizons = tuple(
            islice(iter(horizons), supplied_horizon_count + 1)
        )
    except (TypeError, OverflowError) as exc:
        raise ValueError("invalid supplied canonical horizon tuple") from exc
    previous_horizon: int | float | None = None
    if len(supplied_horizons) != supplied_horizon_count:
        raise ValueError("invalid supplied canonical horizon tuple")
    for horizon in supplied_horizons:
        try:
            valid_horizon = (
                type(horizon) in (int, float)
                and math.isfinite(horizon)
                and horizon > 0.0
                and (
                    previous_horizon is None
                    or horizon > previous_horizon
                )
            )
        except OverflowError:
            valid_horizon = False
        if not valid_horizon:
            raise ValueError("invalid supplied canonical horizon tuple")
        previous_horizon = horizon
    rows = conn.execute(
        """SELECT o.*,cp.horizons_json,cp.full_mask,cp.completed_mask
FROM canonical_pending_current AS cp INDEXED BY canonical_pending_incomplete_idx
JOIN canonical_observations o NOT INDEXED ON o.id=cp.observation_id
JOIN decisions d ON d.id=cp.decision_id AND d.id=o.decision_id
WHERE cp.completed_mask<>cp.full_mask
ORDER BY cp.observation_id
LIMIT :limit_plus_one""",
        {"limit_plus_one": limit_plus_one},
    ).fetchall()
    for row in rows:
        decision = conn.execute(
            "SELECT feature_vector_json FROM decisions WHERE id=?",
            (row["decision_id"],),
        ).fetchone()
        if decision is None:
            raise EvidenceIntegrityError("malformed canonical pending horizons")
        try:
            decision_json, _ = _v5_canonical_pending_horizons(
                conn, decision["feature_vector_json"],
            )
        except ValueError as exc:
            raise EvidenceIntegrityError(
                "malformed canonical pending horizons"
            ) from exc
        if row["horizons_json"] != decision_json:
            raise EvidenceIntegrityError("malformed canonical pending horizons")

        def reject_constant(value: str) -> object:
            raise ValueError(f"invalid JSON constant: {value}")

        try:
            exact_decision_horizons = json.loads(
                decision_json,
                parse_constant=reject_constant,
            )
        except (TypeError, ValueError) as exc:
            raise EvidenceIntegrityError(
                "malformed canonical pending horizons"
            ) from exc
        exact_previous: int | float | None = None
        exact_valid = (
            type(exact_decision_horizons) is list
            and 1 <= len(exact_decision_horizons) <= 32
        )
        if exact_valid:
            for exact_horizon in exact_decision_horizons:
                try:
                    exact_value_valid = (
                        type(exact_horizon) in (int, float)
                        and math.isfinite(exact_horizon)
                        and exact_horizon > 0.0
                        and (
                            exact_previous is None
                            or exact_horizon > exact_previous
                        )
                    )
                except OverflowError:
                    exact_value_valid = False
                if not exact_value_valid:
                    exact_valid = False
                    break
                exact_previous = exact_horizon
        if not exact_valid:
            raise EvidenceIntegrityError("malformed canonical pending horizons")
        if tuple(exact_decision_horizons) != supplied_horizons:
            raise EvidenceIntegrityError(
                "canonical pending horizon config drift"
            )
    return rows


def canonical_outcome_exists(
    conn: sqlite3.Connection, *, observation_id: int, horizon_s: float,
) -> bool:
    if type(observation_id) is not int or observation_id <= 0:
        raise ValueError("observation_id must be a positive integer")
    horizon = _p3_finite_number(
        horizon_s,
        name="horizon_s",
        minimum=0.0,
        maximum=1e100,
        strict_minimum=True,
    )
    return conn.execute(
        "SELECT 1 FROM outcomes WHERE ref_kind='canonical_observation' "
        "AND ref_id=? AND json_extract(detail_json,'$.horizon_s')=? LIMIT 1",
        (observation_id, horizon),
    ).fetchone() is not None


def record_canonical_observation_outcome(
    conn: sqlite3.Connection,
    *,
    raw_wall: float,
    observation_id: int,
    horizon_s: float,
    forward_return_pct: float | None,
    price0: float,
    price0_observed_at: float,
    price_now: float | None,
    price_now_observed_at: float | None,
    terminal: str | None,
    unavailable_reason: str,
) -> int:
    """Append one strictly shaped canonical-observation return measurement."""
    if conn.in_transaction:
        raise RuntimeError("canonical outcome persistence owns its transaction")
    raw_t = _validated_p3_causal_wall(raw_wall)
    if type(observation_id) is not int or observation_id <= 0:
        raise ValueError("observation_id must be a positive integer")
    horizon = _p3_finite_number(
        horizon_s,
        name="horizon_s",
        minimum=0.0,
        maximum=1e100,
        strict_minimum=True,
    )
    start_price = _p3_finite_number(
        price0,
        name="price0",
        minimum=0.0,
        maximum=1e100,
        strict_minimum=True,
    )
    start_price_at = _p3_finite_number(
        price0_observed_at,
        name="price0_observed_at",
        minimum=0.0,
        maximum=4102444800.0,
    )
    if terminal is not None and (
        type(terminal) is not str
        or terminal not in ("DEAD", "STALE", "GRADUATED")
    ):
        raise ValueError("invalid canonical outcome terminal")
    if type(unavailable_reason) is not str:
        raise ValueError("invalid canonical outcome unavailable reason")

    if unavailable_reason == "":
        measured_return = _p3_finite_number(
            forward_return_pct,
            name="forward_return_pct",
            minimum=-1e100,
            maximum=1e100,
        )
        current_price = _p3_finite_number(
            price_now,
            name="price_now",
            minimum=0.0,
            maximum=1e100,
        )
        current_price_at = _p3_finite_number(
            price_now_observed_at,
            name="price_now_observed_at",
            minimum=0.0,
            maximum=4102444800.0,
        )
        if terminal in ("DEAD", "STALE"):
            if current_price != 0.0 or measured_return != -100.0:
                raise ValueError("invalid zero-terminal canonical outcome")
        else:
            if current_price <= 0.0:
                raise ValueError("available canonical outcome price must be positive")
            expected_return = 100.0 * (
                current_price - start_price
            ) / start_price
            if (
                not math.isfinite(expected_return)
                or not math.isclose(
                    measured_return,
                    expected_return,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
            ):
                raise ValueError("canonical outcome return arithmetic mismatch")
    elif unavailable_reason in ("journal_replay_gap", "graduated_no_price"):
        if (
            forward_return_pct is not None
            or price_now is not None
            or price_now_observed_at is not None
            or (
                unavailable_reason == "journal_replay_gap"
                and terminal is not None
            )
            or (
                unavailable_reason == "graduated_no_price"
                and terminal != "GRADUATED"
            )
        ):
            raise ValueError("invalid unavailable canonical outcome")
        measured_return = None
        current_price = None
        current_price_at = None
    else:
        raise ValueError("invalid canonical outcome unavailable reason")

    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            """SELECT o.id,o.decision_id,o.observed_at,o.eligible,
       o.start_price_sol,o.price_observed_at,o.price_source,o.unavailable_reason,
       d.feature_vector_json,cp.decision_id,cp.horizons_json,
       cp.full_mask,cp.completed_mask
FROM canonical_observations AS o
JOIN decisions AS d ON d.id=o.decision_id
JOIN canonical_pending_current AS cp ON cp.observation_id=o.id
WHERE o.id=?""",
            (observation_id,),
        ).fetchone()
        if row is None:
            raise EvidenceIntegrityError("canonical observation unavailable")
        observed_at = row[2]
        persisted_price0 = row[4]
        persisted_price0_at = row[5]
        if (
            type(row[0]) is not int
            or row[0] != observation_id
            or type(row[1]) is not int
            or row[1] <= 0
            or type(observed_at) not in (int, float)
            or not math.isfinite(observed_at)
            or not 0.0 <= observed_at <= 4102444800.0
            or type(row[3]) is not int
            or row[3] != 1
            or type(persisted_price0) not in (int, float)
            or not math.isfinite(persisted_price0)
            or not 0.0 < persisted_price0 <= 1e100
            or type(persisted_price0_at) not in (int, float)
            or not math.isfinite(persisted_price0_at)
            or not 0.0 <= persisted_price0_at <= observed_at
            or row[6] != "curve_snapshot"
            or row[7] != ""
            or row[9] != row[1]
        ):
            raise EvidenceIntegrityError("malformed canonical observation")
        if (
            start_price != persisted_price0
            or start_price_at != persisted_price0_at
        ):
            raise ValueError("canonical outcome start evidence mismatch")
        persisted_horizons_json, persisted_horizons = (
            _v5_canonical_pending_horizons(conn, row[8])
        )
        expected_full_mask = (1 << len(persisted_horizons)) - 1
        if (
            row[10] != persisted_horizons_json
            or type(row[11]) is not int
            or row[11] != expected_full_mask
            or type(row[12]) is not int
            or not 0 <= row[12] <= expected_full_mask
        ):
            raise EvidenceIntegrityError("malformed canonical pending state")
        if horizon not in persisted_horizons:
            raise ValueError("horizon_s is not in persisted decision horizons")
        if (
            current_price_at is not None
            and not observed_at <= current_price_at <= observed_at + horizon
        ):
            raise ValueError("canonical outcome source time is outside horizon")
        duplicate = conn.execute(
            "SELECT id FROM outcomes WHERE ref_kind='canonical_observation' "
            "AND ref_id=? AND json_extract(detail_json,'$.horizon_s')=? LIMIT 1",
            (observation_id, horizon),
        ).fetchone()
        if duplicate is not None:
            raise EvidenceIntegrityError("duplicate canonical observation outcome")

        outcome_at = allocate_p3_causal_wall(conn, raw_wall=raw_t)
        if outcome_at <= observed_at or outcome_at < observed_at + horizon:
            raise ValueError("canonical outcome is not due")
        detail_json = json.dumps(
            {
                "horizon_s": horizon,
                "forward_return_pct": measured_return,
                "price0": start_price,
                "price0_observed_at": start_price_at,
                "price_now": current_price,
                "price_now_observed_at": current_price_at,
                "terminal": terminal,
                "unavailable_reason": unavailable_reason,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        cursor = conn.execute(
            "INSERT INTO outcomes(at,ref_kind,ref_id,pnl_sol,detail_json) "
            "VALUES (?,'canonical_observation',?,0.0,?)",
            (outcome_at, observation_id, detail_json),
        )
        outcome_id = cursor.lastrowid
        if type(outcome_id) is not int or outcome_id <= 0:
            raise EvidenceIntegrityError("canonical outcome ID allocation failed")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return outcome_id


def record_paper_trade(conn: sqlite3.Connection, *, decision_id: int, at: float, mint: str,
                       segment: str, side: str, qty: float, quote_price: float,
                       fill_price: float, fees: dict, realism_grade: str) -> int:
    if conn.execute(
        "SELECT 1 FROM canonical_observations WHERE decision_id=? LIMIT 1",
        (decision_id,),
    ).fetchone() is not None:
        raise ValueError("canonical P3 trades require strict P3 trade helpers")
    cur = conn.execute(
        "INSERT INTO paper_trades(decision_id, at, mint, segment, side, qty, quote_price,"
        " fill_price, fees_json, realism_grade) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (decision_id, at, mint, segment, side, qty, quote_price, fill_price,
         _json_ledger.dumps(fees), realism_grade))
    conn.commit()
    return cur.lastrowid


def record_outcome(conn: sqlite3.Connection, *, at: float, ref_kind: str, ref_id: int,
                   pnl_sol: float, detail: dict) -> int:
    if ref_kind == "canonical_observation" or (
        ref_kind == "trade"
        and conn.execute(
            "SELECT 1 FROM paper_trades pt "
            "JOIN canonical_observations o ON o.decision_id=pt.decision_id "
            "WHERE pt.id=? LIMIT 1",
            (ref_id,),
        ).fetchone()
        is not None
    ):
        raise ValueError("canonical P3 outcomes require strict P3 outcome helpers")
    cur = conn.execute(
        "INSERT INTO outcomes(at, ref_kind, ref_id, pnl_sol, detail_json)"
        " VALUES (?, ?, ?, ?, ?)",
        (at, ref_kind, ref_id, pnl_sol, _json_ledger.dumps(detail)))
    conn.commit()
    return cur.lastrowid


def open_positions_from_ledger(
    conn: sqlite3.Connection, *, legacy_only: bool = False,
) -> list[dict]:
    """Reconstruct still-open paper positions: per DECISION (entry cycle), sum(buy qty) -
    sum(sell qty) > 0. Used to restore in-memory positions after a restart (deploys restart
    the service).

    Aggregated by decision_id, NOT mint (N1 fix): a mint can have multiple entry cycles
    over its lifetime (P4 re-enters graduated tokens), so an EARLIER decision on a mint
    may be fully closed while a LATER decision on the SAME mint is still open. Keying on
    mint alone would net qty_remaining across both cycles while reporting the first buy's
    decision_id/entry_price -- reconcile() then re-queries paper_trades WHERE decision_id =
    <that wrong id>, restoring the closed cycle's original_qty/buy_notional/entry_price
    against the open cycle's qty_remaining, and every subsequent exit silently corrupts
    _realized_pnl under the wrong decision. Keying on decision_id keeps each entry cycle
    strictly separate, so a closed decision A and an open decision B on the same mint
    correctly return ONE row (B's), never mixed."""
    if type(legacy_only) is not bool:
        raise ValueError("legacy_only must be boolean")
    rows = conn.execute(
        "SELECT pt.mint,pt.side,pt.qty,pt.fill_price,pt.at,pt.decision_id "
        "FROM paper_trades AS pt WHERE pt.segment='CLIMBING' "
        "AND (?=0 OR NOT EXISTS (SELECT 1 FROM canonical_observations AS o "
        "WHERE o.decision_id=pt.decision_id)) ORDER BY pt.at ASC",
        (int(legacy_only),),
    ).fetchall()
    agg: dict[int, dict] = {}
    for r in rows:
        pos = agg.setdefault(r["decision_id"], {
            "mint": r["mint"], "qty_remaining": 0.0, "entry_price": None,
            "entry_at": None, "decision_id": r["decision_id"]})
        if r["side"] == "buy":
            pos["qty_remaining"] += r["qty"]
            if pos["entry_price"] is None:      # first buy defines the entry
                pos["entry_price"] = r["fill_price"]
                pos["entry_at"] = r["at"]
        else:
            pos["qty_remaining"] -= r["qty"]
    return [p for p in agg.values() if p["qty_remaining"] > 1e-9]


def list_decisions_for_counterfactual(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """All decisions (traded or not) — the counterfactual tracker resumes forward-return
    tracking from these after a restart. Reads the decision-time price from the feature
    vector's `spot_price_sol` key (Task 8/10 write it there)."""
    return conn.execute(
        "SELECT id, at, mint, feature_vector_json FROM decisions"
        " WHERE segment = 'CLIMBING' ORDER BY at ASC").fetchall()


def record_wallet_pnl_event(conn: sqlite3.Connection, *, at: float, wallet: str, mint: str,
                            realized_pnl_sol: float, source: str, detail: dict) -> int:
    """Append one realized wallet-PnL observation used by the P2 smart-money snapshot.

    Events are append-only evidence. `smart_wallets_snapshot(before_at=...)` is the
    no-lookahead guard: it only aggregates events strictly before a candidate decision.
    """
    if conn.in_transaction:
        raise RuntimeError("wallet PnL persistence owns its transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        observed_at = _validated_p3_causal_wall(at)
        fence_p3_causal_wall(conn, observed_wall=observed_at)
        cur = conn.execute(
            "INSERT INTO wallet_pnl_events("
            "at, wallet, mint, realized_pnl_sol, source, detail_json)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                observed_at,
                wallet,
                mint,
                realized_pnl_sol,
                source,
                _json_ledger.dumps(detail),
            ),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return cur.lastrowid


def smart_wallets_snapshot(conn: sqlite3.Connection, *, before_at: float, min_events: int,
                           min_realized_pnl_sol: float) -> dict[str, dict]:
    """Return wallets that qualified as smart money BEFORE a decision timestamp.

    The strict `at < before_at` predicate is load-bearing: a wallet's result on the
    candidate token can never leak into the feature vector for that same candidate.
    """
    rows = conn.execute(
        "SELECT wallet, COUNT(*) AS events, SUM(realized_pnl_sol) AS total_realized_pnl_sol"
        " FROM wallet_pnl_events WHERE at < ? GROUP BY wallet"
        " HAVING COUNT(*) >= ? AND SUM(realized_pnl_sol) >= ?",
        (before_at, min_events, min_realized_pnl_sol)).fetchall()
    return {r["wallet"]: {"events": r["events"],
                          "realized_pnl_sol": r["total_realized_pnl_sol"]} for r in rows}


def record_early_buyer_read(conn: sqlite3.Connection, *, mint: str, checked_at: float,
                            buyers: tuple[str, ...] | list[str], unavailable_reason: str,
                            inputs_hash: str) -> int:
    if conn.in_transaction:
        raise RuntimeError("early-buyer persistence owns its transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        observed_at = _validated_p3_causal_wall(checked_at)
        fence_p3_causal_wall(conn, observed_wall=observed_at)
        cur = conn.execute(
            "INSERT INTO early_buyer_reads(mint, checked_at, buyers_json, unavailable_reason,"
            " inputs_hash) VALUES (?, ?, ?, ?, ?)",
            (
                mint,
                observed_at,
                _json_ledger.dumps(list(buyers)),
                unavailable_reason,
                inputs_hash,
            ),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return cur.lastrowid


def latest_early_buyer_read(conn: sqlite3.Connection, mint: str):
    return conn.execute(
        "SELECT * FROM early_buyer_reads WHERE mint = ? ORDER BY checked_at DESC, id DESC LIMIT 1",
        (mint,)).fetchone()
