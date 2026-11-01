"""SQLite target: DDL + connection helper.

Current-state tables (customers, orders) hold ONE row per business key (latest version wins).
*_history tables are append-only: every valid version ever seen, so change history is never
silently overwritten. Operational tables: etl_watermark, file_log, pipeline_runs, rejects.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS customers (
  customer_id        TEXT PRIMARY KEY,
  name               TEXT,
  email              TEXT,
  country            TEXT,
  is_deleted         INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0,1)),
  source_updated_at  TEXT NOT NULL,
  record_hash        TEXT NOT NULL,
  source_file        TEXT NOT NULL,
  first_run_id       TEXT NOT NULL,
  last_run_id        TEXT NOT NULL,
  loaded_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS customers_history (
  customer_id        TEXT NOT NULL,
  source_updated_at  TEXT NOT NULL,
  record_hash        TEXT NOT NULL,
  op                 TEXT NOT NULL CHECK (op IN ('U','D')),
  name               TEXT,
  email              TEXT,
  country            TEXT,
  run_id             TEXT NOT NULL,
  source_file        TEXT NOT NULL,
  loaded_at          TEXT NOT NULL,
  PRIMARY KEY (customer_id, source_updated_at, record_hash)
);
CREATE TABLE IF NOT EXISTS orders (
  order_id           TEXT PRIMARY KEY,
  customer_id        TEXT REFERENCES customers(customer_id),
  status             TEXT CHECK (status IN ('NEW','PAID','SHIPPED','CANCELLED')),
  amount_minor       INTEGER CHECK (amount_minor >= 0),
  currency           TEXT CHECK (currency IN ('INR','USD','EUR')),
  order_ts           TEXT,
  order_date         TEXT,
  is_deleted         INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0,1)),
  source_updated_at  TEXT NOT NULL,
  record_hash        TEXT NOT NULL,
  source_file        TEXT NOT NULL,
  first_run_id       TEXT NOT NULL,
  last_run_id        TEXT NOT NULL,
  loaded_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_history (
  order_id           TEXT NOT NULL,
  source_updated_at  TEXT NOT NULL,
  record_hash        TEXT NOT NULL,
  op                 TEXT NOT NULL CHECK (op IN ('U','D')),
  customer_id        TEXT,
  status             TEXT,
  amount_minor       INTEGER,
  currency           TEXT,
  order_ts           TEXT,
  order_date         TEXT,
  run_id             TEXT NOT NULL,
  source_file        TEXT NOT NULL,
  loaded_at          TEXT NOT NULL,
  PRIMARY KEY (order_id, source_updated_at, record_hash)
);
CREATE INDEX IF NOT EXISTS ix_orders_customer ON orders(customer_id);

CREATE TABLE IF NOT EXISTS etl_watermark (
  entity       TEXT PRIMARY KEY,
  watermark    TEXT NOT NULL,        -- max source_updated_at of committed, accepted rows
  run_id       TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_log (
  source_file    TEXT NOT NULL,
  file_sha256    TEXT NOT NULL,
  entity         TEXT NOT NULL,
  status         TEXT NOT NULL CHECK (status IN ('SUCCESS','FAILED_SCHEMA','FAILED_ERROR')),
  run_id         TEXT NOT NULL,
  rows_read      INTEGER NOT NULL DEFAULT 0,
  rows_inserted  INTEGER NOT NULL DEFAULT 0,
  rows_updated   INTEGER NOT NULL DEFAULT 0,
  rows_deleted   INTEGER NOT NULL DEFAULT 0,
  rows_stale     INTEGER NOT NULL DEFAULT 0,
  rows_duplicate INTEGER NOT NULL DEFAULT 0,
  rows_rejected  INTEGER NOT NULL DEFAULT 0,
  note           TEXT,
  processed_at   TEXT NOT NULL,
  PRIMARY KEY (source_file, file_sha256)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id             TEXT PRIMARY KEY,
  started_at         TEXT NOT NULL,
  ended_at           TEXT,
  status             TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),
  files_discovered   INTEGER DEFAULT 0,
  files_processed    INTEGER DEFAULT 0,
  files_skipped      INTEGER DEFAULT 0,
  files_failed       INTEGER DEFAULT 0,
  rows_read          INTEGER DEFAULT 0,
  rows_inserted      INTEGER DEFAULT 0,
  rows_updated       INTEGER DEFAULT 0,
  rows_deleted       INTEGER DEFAULT 0,
  rows_stale         INTEGER DEFAULT 0,
  rows_duplicate     INTEGER DEFAULT 0,
  rows_rejected      INTEGER DEFAULT 0,
  rejects_by_reason  TEXT,
  duration_seconds   REAL,
  error_message      TEXT,
  config_json        TEXT
);
CREATE TABLE IF NOT EXISTS rejects (
  reject_id     TEXT PRIMARY KEY,     -- sha1(entity|file|line|reason)[:16] -> reruns do not duplicate
  run_id        TEXT NOT NULL,
  entity        TEXT NOT NULL,
  source_file   TEXT NOT NULL,
  line_no       INTEGER NOT NULL,
  reason_code   TEXT NOT NULL,
  reason_detail TEXT NOT NULL,
  raw_record    TEXT NOT NULL,
  rejected_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rejects_reason ON rejects(reason_code);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)  # explicit BEGIN/COMMIT control
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(DDL)
    return conn
