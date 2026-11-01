"""Orchestration: discover files -> per-file atomic merge -> watermark -> run metrics.

Key guarantees
  * One SQLite transaction per source file: data + history + rejects + file ledger + watermark
    commit together or not at all (crash-safe; the watermark can never run ahead of the data).
  * Merge is idempotent: key + source_updated_at version comparison, history PK, deterministic
    reject ids. Re-running the same or an overlapping file cannot create duplicates.
  * Files are processed parents-first (customers, then orders) and in name order.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import entities as E
from .config import TS_FORMAT, Config, iso
from .db import connect
from .reader import SchemaError, read_batch

log = logging.getLogger("ingest")

COUNTERS = ("rows_read", "rows_inserted", "rows_updated", "rows_deleted",
            "rows_stale", "rows_duplicate", "rows_rejected")


class InjectedFailure(RuntimeError):
    """Raised on purpose (Config.inject_failure_file) to demonstrate crash recovery."""


@dataclass
class FileStats:
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_stale: int = 0
    rows_duplicate: int = 0
    rows_rejected: int = 0
    resurrected: int = 0  # informational subset of rows_updated
    rejects_by_reason: Counter = field(default_factory=Counter)
    max_ts: Optional[str] = None
    note: str = ""


@dataclass
class RunSummary:
    run_id: str
    status: str = "RUNNING"
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: float = 0.0
    files_discovered: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    counters: Dict[str, int] = field(default_factory=lambda: {c: 0 for c in COUNTERS})
    rejects_by_reason: Counter = field(default_factory=Counter)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k not in ("counters", "rejects_by_reason")}
        d.update(self.counters)
        d["rejects_by_reason"] = dict(sorted(self.rejects_by_reason.items()))
        return d


# ------------------------------------------------------------------------------------------
def discover_files(source_dir: Path) -> List[Tuple[str, Path]]:
    found: List[Tuple[int, str, Path]] = []
    for p in sorted(Path(source_dir).glob("*.csv")):
        m = re.match(r"([a-z]+)_", p.name)
        if not m or m.group(1) not in E.ENTITIES:
            log.warning("ignoring file with unknown entity prefix: %s", p.name)
            continue
        found.append((E.ENTITY_ORDER.index(m.group(1)), p.name, p))
    found.sort(key=lambda t: (t[0], t[1]))
    return [(E.ENTITY_ORDER[i], p) for i, _, p in found]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _chunks(items, n):
    items = list(items)
    for i in range(0, len(items), max(1, n)):
        yield items[i:i + max(1, n)]


def _fetch_state(conn, spec, keys, chunk) -> Dict[str, dict]:
    state: Dict[str, dict] = {}
    for part in _chunks(sorted(keys), chunk):
        q = ",".join("?" * len(part))
        for row in conn.execute(f"SELECT * FROM {spec.table} WHERE {spec.key_col} IN ({q})", part):
            state[row[spec.key_col]] = dict(row)
    return state


def _fetch_existing_ids(conn, table, key_col, ids, chunk) -> set:
    found = set()
    for part in _chunks(sorted(ids), chunk):
        q = ",".join("?" * len(part))
        found.update(r[0] for r in conn.execute(f"SELECT {key_col} FROM {table} WHERE {key_col} IN ({q})", part))
    return found


def _reject_tuple(run_id, entity, file_name, line_no, reason, detail, raw, ts):
    rid = hashlib.sha1(f"{entity}|{file_name}|{line_no}|{reason}".encode()).hexdigest()[:16]
    return (rid, run_id, entity, file_name, line_no, reason, detail[:500], json.dumps(raw, ensure_ascii=False)[:2000], ts)


def _apply(spec, cur: Optional[dict], r: E.Record, run_id, file_name, loaded_at) -> dict:
    new = dict(cur) if cur else {spec.key_col: r.key, "first_run_id": run_id, **{c: None for c in spec.attr_cols}}
    if r.op == "U":
        new.update(r.values)
        new["is_deleted"] = 0
    else:  # tombstone: soft delete, keep last known attributes
        new["is_deleted"] = 1
    new.update(source_updated_at=r.ts, record_hash=r.record_hash, source_file=file_name,
               last_run_id=run_id, loaded_at=loaded_at)
    return new


# ------------------------------------------------------------------------------------------
def process_file(conn: sqlite3.Connection, cfg: Config, spec: E.EntitySpec, path: Path, sha: str,
                 run_id: str, now_ts: str, latest_allowed_ts: str) -> FileStats:
    file_name = path.name
    batch = read_batch(path, spec)  # may raise SchemaError (nothing written yet)
    stats = FileStats(rows_read=len(batch.rows) + len(batch.malformed))
    if batch.extra_columns:
        stats.note = f"schema drift tolerated: extra columns {batch.extra_columns} ignored"
        log.warning("[%s] %s: %s", run_id, file_name, stats.note)

    rejects: list = []

    def reject(line_no, reason, detail, raw):
        stats.rows_rejected += 1
        stats.rejects_by_reason[reason] += 1
        rejects.append(_reject_tuple(run_id, spec.name, file_name, line_no, reason, detail, raw, now_ts))

    valid: List[E.Record] = []
    for line_no, fields, detail in batch.malformed:
        reject(line_no, E.MALFORMED_ROW, detail, fields)
    for line_no, raw in batch.rows:
        try:
            valid.append(E.validate_row(spec, raw, line_no, latest_allowed_ts))
        except E.RowError as exc:
            reject(line_no, exc.reason, exc.detail, raw)

    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT watermark FROM etl_watermark WHERE entity=?", (spec.name,)).fetchone()
        watermark = row["watermark"] if row else None
        cutoff = None
        if watermark and not cfg.full_refresh:
            wm_dt = datetime.strptime(watermark, TS_FORMAT).replace(tzinfo=timezone.utc)
            cutoff = iso(wm_dt - timedelta(hours=cfg.lookback_hours))

        # --- referential integrity (child -> parent) ---------------------------------------
        if spec.parent:
            fk, parent_name = spec.parent
            parent = E.ENTITIES[parent_name]
            ids = {r.values[fk] for r in valid if r.op == "U"}
            known = _fetch_existing_ids(conn, parent.table, parent.key_col, ids, cfg.lookup_chunk)
            kept = []
            by_line = dict(batch.rows)
            for r in valid:
                if r.op == "U" and r.values[fk] not in known:
                    reject(r.line_no, E.INVALID_REFERENCE,
                           f"{fk} {r.values[fk]} not found in {parent.table}", by_line[r.line_no])
                else:
                    kept.append(r)
            valid = kept

        # --- merge: apply versions in (updated_at, line) order against current state -------
        state = _fetch_state(conn, spec, {r.key for r in valid}, cfg.lookup_chunk)
        by_line = dict(batch.rows)
        valid.sort(key=lambda r: (r.ts, r.line_no))
        history, dirty = [], set()
        for r in valid:
            cur = state.get(r.key)
            cur_ts = cur["source_updated_at"] if cur else None
            if cutoff and r.ts < cutoff and (cur is None or r.ts > cur_ts):
                reject(r.line_no, E.LATE_BEYOND_LOOKBACK,
                       f"updated_at {r.ts} older than watermark {watermark} minus {cfg.lookback_hours}h",
                       by_line[r.line_no])
                continue
            if cur is not None and r.ts == cur_ts and r.record_hash != cur["record_hash"]:
                reject(r.line_no, E.DUPLICATE_CONFLICT,
                       f"same key and updated_at as an already-loaded version but different content",
                       by_line[r.line_no])
                continue
            history.append((r.key, r.ts, r.record_hash, r.op, *[r.values.get(c) for c in spec.attr_cols],
                            run_id, file_name, now_ts))
            stats.max_ts = r.ts if stats.max_ts is None or r.ts > stats.max_ts else stats.max_ts
            if cur is None or r.ts > cur_ts:
                if cur is None:
                    stats.rows_inserted += r.op == "U"
                    stats.rows_deleted += r.op == "D"
                else:
                    stats.rows_updated += r.op == "U"
                    stats.rows_deleted += r.op == "D"
                    stats.resurrected += (r.op == "U" and cur["is_deleted"] == 1)
                state[r.key] = _apply(spec, cur, r, run_id, file_name, now_ts)
                dirty.add(r.key)
            elif r.ts == cur_ts:
                stats.rows_duplicate += 1
            else:
                stats.rows_stale += 1  # late / out-of-order older version: history only

        # --- write ---------------------------------------------------------------------------
        n_attr = len(spec.attr_cols)
        conn.executemany(
            f"INSERT OR IGNORE INTO {spec.history_table} ({spec.key_col}, source_updated_at, record_hash, op, "
            f"{', '.join(spec.attr_cols)}, run_id, source_file, loaded_at) VALUES ({','.join('?' * (4 + n_attr + 3))})",
            history)
        cols = [spec.key_col, *spec.attr_cols, "is_deleted", "source_updated_at", "record_hash",
                "source_file", "first_run_id", "last_run_id", "loaded_at"]
        updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in (spec.key_col, "first_run_id"))
        conn.executemany(
            f"INSERT INTO {spec.table} ({', '.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
            f"ON CONFLICT({spec.key_col}) DO UPDATE SET {updates}",
            [tuple(state[k][c] for c in cols) for k in sorted(dirty)])
        conn.executemany("INSERT OR IGNORE INTO rejects VALUES (?,?,?,?,?,?,?,?,?)", rejects)
        conn.execute(
            "INSERT OR REPLACE INTO file_log (source_file, file_sha256, entity, status, run_id, rows_read, "
            "rows_inserted, rows_updated, rows_deleted, rows_stale, rows_duplicate, rows_rejected, note, processed_at) "
            "VALUES (?,?,?,'SUCCESS',?,?,?,?,?,?,?,?,?,?)",
            (file_name, sha, spec.name, run_id, stats.rows_read, stats.rows_inserted, stats.rows_updated,
             stats.rows_deleted, stats.rows_stale, stats.rows_duplicate, stats.rows_rejected, stats.note, now_ts))
        if stats.max_ts:
            conn.execute(
                "INSERT INTO etl_watermark (entity, watermark, run_id, updated_at) VALUES (?,?,?,?) "
                "ON CONFLICT(entity) DO UPDATE SET watermark=MAX(watermark, excluded.watermark), "
                "run_id=excluded.run_id, updated_at=excluded.updated_at",
                (spec.name, stats.max_ts, run_id, now_ts))
        if cfg.inject_failure_file and cfg.inject_failure_file in file_name:
            raise InjectedFailure(f"injected failure before commit of {file_name}")
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return stats


def _log_failed_file(conn, name, sha, entity, status, run_id, note, now_ts):
    conn.execute(
        "INSERT OR REPLACE INTO file_log (source_file, file_sha256, entity, status, run_id, note, processed_at) "
        "VALUES (?,?,?,?,?,?,?)", (name, sha, entity, status, run_id, note[:500], now_ts))


def run_pipeline(cfg: Config) -> RunSummary:
    conn = connect(cfg.db_path)
    t0 = time.perf_counter()
    started = cfg.now()
    n_prev = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
    run_id = cfg.run_id or f"run_{started.strftime('%Y%m%dT%H%M%SZ')}_{n_prev + 1:03d}"
    now_ts = iso(started)
    latest_allowed = iso(started + timedelta(hours=cfg.max_future_skew_hours))
    summary = RunSummary(run_id=run_id, started_at=now_ts)
    conn.execute("INSERT INTO pipeline_runs (run_id, started_at, status, config_json) VALUES (?,?,?,?)",
                 (run_id, now_ts, "RUNNING", json.dumps(cfg.to_json())))
    log.info("[%s] start source=%s db=%s", run_id, cfg.source_dir, cfg.db_path)

    files = discover_files(cfg.source_dir)
    summary.files_discovered = len(files)
    for entity, path in files:
        spec = E.ENTITIES[entity]
        sha = sha256_file(path)
        prior = conn.execute("SELECT status FROM file_log WHERE source_file=? AND file_sha256=?",
                             (path.name, sha)).fetchone()
        if prior and prior["status"] in ("SUCCESS", "FAILED_SCHEMA") and not cfg.full_refresh:
            summary.files_skipped += 1
            lvl = logging.WARNING if prior["status"] == "FAILED_SCHEMA" else logging.INFO
            log.log(lvl, "[%s] skip %s (already %s, same checksum)%s", run_id, path.name, prior["status"],
                    " - STILL QUARANTINED, needs a corrected file" if lvl == logging.WARNING else "")
            continue
        try:
            st = process_file(conn, cfg, spec, path, sha, run_id, now_ts, latest_allowed)
        except SchemaError as exc:
            _log_failed_file(conn, path.name, sha, entity, "FAILED_SCHEMA", run_id, str(exc), now_ts)
            summary.files_failed += 1
            log.error("[%s] quarantined %s: %s", run_id, path.name, exc)
            continue
        except Exception as exc:  # unexpected: roll back (done), record, stop; restart resumes here
            _log_failed_file(conn, path.name, sha, entity, "FAILED_ERROR", run_id, repr(exc), now_ts)
            summary.files_failed += 1
            summary.error = f"{path.name}: {exc!r}"
            log.error("[%s] FAILED on %s: %r (transaction rolled back; rerun resumes here)", run_id, path.name, exc)
            break
        summary.files_processed += 1
        for c in COUNTERS:
            summary.counters[c] += getattr(st, c)
        summary.rejects_by_reason.update(st.rejects_by_reason)
        log.info("[%s] %s: read=%d ins=%d upd=%d del=%d stale=%d dup=%d rej=%d", run_id, path.name,
                 st.rows_read, st.rows_inserted, st.rows_updated, st.rows_deleted, st.rows_stale,
                 st.rows_duplicate, st.rows_rejected)

    summary.status = "FAILED" if summary.error else ("PARTIAL" if summary.files_failed else "SUCCESS")
    summary.duration_seconds = round(time.perf_counter() - t0, 3)
    summary.ended_at = iso(cfg.now())
    c = summary.counters
    conn.execute(
        "UPDATE pipeline_runs SET ended_at=?, status=?, files_discovered=?, files_processed=?, files_skipped=?, "
        "files_failed=?, rows_read=?, rows_inserted=?, rows_updated=?, rows_deleted=?, rows_stale=?, "
        "rows_duplicate=?, rows_rejected=?, rejects_by_reason=?, duration_seconds=?, error_message=? WHERE run_id=?",
        (summary.ended_at, summary.status, summary.files_discovered, summary.files_processed, summary.files_skipped,
         summary.files_failed, c["rows_read"], c["rows_inserted"], c["rows_updated"], c["rows_deleted"],
         c["rows_stale"], c["rows_duplicate"], c["rows_rejected"], json.dumps(dict(summary.rejects_by_reason)),
         summary.duration_seconds, summary.error, run_id))
    if cfg.metrics_dir:
        Path(cfg.metrics_dir).mkdir(parents=True, exist_ok=True)
        (Path(cfg.metrics_dir) / f"{run_id}.json").write_text(json.dumps(summary.to_dict(), indent=2))
    log.info("[%s] end status=%s files(processed=%d skipped=%d failed=%d) %s in %.2fs", run_id, summary.status,
             summary.files_processed, summary.files_skipped, summary.files_failed, dict(c), summary.duration_seconds)
    conn.close()
    return summary
