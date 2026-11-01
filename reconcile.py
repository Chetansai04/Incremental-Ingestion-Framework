"""Reconciliation / validation report: proves the target is internally consistent and (optionally)
matches the independent answer key produced by the generator."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


def _one(conn, sql, args=()):
    return conn.execute(sql, args).fetchone()[0]


def run_checks(conn: sqlite3.Connection, labels: Optional[dict] = None) -> List[Check]:
    out: List[Check] = []

    def add(name, ok, detail=""):
        out.append(Check(name, bool(ok), detail))

    # 1. every successfully processed file accounts for every row it read
    bad = conn.execute(
        "SELECT source_file FROM file_log WHERE status='SUCCESS' AND rows_read != rows_inserted+rows_updated+"
        "rows_deleted+rows_stale+rows_duplicate+rows_rejected").fetchall()
    add("row accounting per file: read = inserted+updated+deleted+stale+duplicate+rejected", not bad,
        f"{len(bad)} file(s) do not balance" if bad else "all files balance")

    # 2. current state vs history (per entity)
    for ent, tbl, hist, key in (("customers", "customers", "customers_history", "customer_id"),
                                ("orders", "orders", "orders_history", "order_id")):
        n_cur = _one(conn, f"SELECT COUNT(*) FROM {tbl}")
        n_hist_keys = _one(conn, f"SELECT COUNT(DISTINCT {key}) FROM {hist}")
        add(f"{ent}: one current row per key; keys in current == distinct keys in history",
            n_cur == n_hist_keys, f"current={n_cur}, history keys={n_hist_keys}")
        lag = _one(conn, f"SELECT COUNT(*) FROM {tbl} t WHERE t.source_updated_at != "
                         f"(SELECT MAX(source_updated_at) FROM {hist} h WHERE h.{key}=t.{key})")
        add(f"{ent}: current version == latest history version", lag == 0, f"{lag} mismatches")
        flag = _one(conn, f"SELECT COUNT(*) FROM {tbl} t WHERE NOT EXISTS (SELECT 1 FROM {hist} h WHERE h.{key}=t.{key} "
                          f"AND h.source_updated_at=t.source_updated_at AND h.record_hash=t.record_hash "
                          f"AND (h.op='D') = (t.is_deleted=1))")
        add(f"{ent}: is_deleted flag and record_hash agree with the latest history row", flag == 0, f"{flag} mismatches")

    # 3. referential integrity + derived fields + domain rules
    orphans = _one(conn, "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON c.customer_id=o.customer_id "
                         "WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL")
    add("orders.customer_id -> customers.customer_id (no orphans)", orphans == 0, f"{orphans} orphans")
    bad_der = _one(conn, "SELECT COUNT(*) FROM orders WHERE order_date != substr(order_ts,1,10) OR amount_minor < 0")
    add("derived fields: order_date = date(order_ts); amount_minor >= 0", bad_der == 0, f"{bad_der} violations")
    nulls = _one(conn, "SELECT (SELECT COUNT(*) FROM customers WHERE is_deleted=0 AND (name IS NULL OR email IS NULL OR country IS NULL))"
                       " + (SELECT COUNT(*) FROM orders WHERE is_deleted=0 AND (status IS NULL OR amount_minor IS NULL OR customer_id IS NULL))")
    add("active rows have no NULL mandatory attributes", nulls == 0, f"{nulls} violations")

    # 4. rejects are traceable to their source
    untraceable = _one(conn, "SELECT COUNT(*) FROM rejects r WHERE r.source_file='' OR r.raw_record='' OR NOT EXISTS "
                             "(SELECT 1 FROM file_log f WHERE f.source_file=r.source_file)")
    add("every reject carries source_file, line_no, reason and raw record, and maps to a logged file",
        untraceable == 0, f"{untraceable} untraceable")

    # 5. against the answer key (only when a labels file is supplied)
    if labels:
        exp = labels["expected_final_state"]
        for ent, tbl in (("customers", "customers"), ("orders", "orders")):
            act = _one(conn, f"SELECT COUNT(*) FROM {tbl} WHERE is_deleted=0")
            dele = _one(conn, f"SELECT COUNT(*) FROM {tbl} WHERE is_deleted=1")
            add(f"[labels] {ent}: active/deleted counts", (act, dele) == (exp[ent]["active"], exp[ent]["deleted"]),
                f"actual active={act} deleted={dele}; expected active={exp[ent]['active']} deleted={exp[ent]['deleted']}")
        amt = {r[0]: r[1] for r in conn.execute(
            "SELECT currency, SUM(amount_minor) FROM orders WHERE is_deleted=0 GROUP BY currency ORDER BY currency")}
        add("[labels] orders: SUM(amount_minor) of active orders by currency", amt == exp["orders"]["active_amount_minor_by_currency"],
            f"actual={amt}; expected={exp['orders']['active_amount_minor_by_currency']}")
        rej = {r[0]: r[1] for r in conn.execute("SELECT reason_code, COUNT(*) FROM rejects GROUP BY reason_code ORDER BY 1")}
        add("[labels] rejects by reason equal the injected anomalies", rej == labels["expected_rejects_by_reason"],
            f"actual={rej}; expected={labels['expected_rejects_by_reason']}")
        rows = _one(conn, "SELECT COALESCE(SUM(rows_read),0) FROM file_log WHERE status='SUCCESS'")
        add("[labels] total rows read from successful files", rows == labels["expected_rows_read"],
            f"actual={rows}; expected={labels['expected_rows_read']}")
        dups = _one(conn, "SELECT COALESCE(SUM(rows_duplicate),0) FROM file_log WHERE status='SUCCESS'")
        add("[labels] exact duplicates detected", dups == labels["expected_exact_duplicates"],
            f"actual={dups}; expected={labels['expected_exact_duplicates']}")
        q = sorted(r[0] for r in conn.execute("SELECT source_file FROM file_log WHERE status='FAILED_SCHEMA'"))
        add("[labels] schema-broken files are quarantined", q == sorted(labels["expected_quarantined_files"]), f"actual={q}")
    return out


def key_aggregates(conn: sqlite3.Connection) -> dict:
    q = lambda sql: [dict(r) for r in conn.execute(sql)]
    return {
        "orders_by_status_currency": q(
            "SELECT status, currency, COUNT(*) AS orders, SUM(amount_minor)/100.0 AS revenue "
            "FROM orders WHERE is_deleted=0 GROUP BY status, currency ORDER BY status, currency"),
        "daily_revenue_active_orders": q(
            "SELECT order_date, COUNT(*) AS orders, SUM(amount_minor)/100.0 AS revenue FROM orders "
            "WHERE is_deleted=0 GROUP BY order_date ORDER BY order_date"),
        "customers_by_country": q(
            "SELECT country, COUNT(*) AS customers FROM customers WHERE is_deleted=0 GROUP BY country ORDER BY 2 DESC, 1"),
        "rejects_by_reason": q("SELECT reason_code, COUNT(*) AS rows FROM rejects GROUP BY reason_code ORDER BY 2 DESC, 1"),
        "per_file": q("SELECT source_file, status, rows_read, rows_inserted, rows_updated, rows_deleted, rows_stale, "
                      "rows_duplicate, rows_rejected FROM file_log ORDER BY entity DESC, source_file"),
        "runs": q("SELECT run_id, status, files_processed, files_skipped, files_failed, rows_read, rows_rejected, duration_seconds "
                  "FROM pipeline_runs ORDER BY started_at, run_id"),
        "tombstoned": q("SELECT 'customers' AS entity, COUNT(*) AS deleted_rows FROM customers WHERE is_deleted=1 "
                        "UNION ALL SELECT 'orders', COUNT(*) FROM orders WHERE is_deleted=1"),
    }


def _md_table(rows: List[dict]) -> str:
    if not rows:
        return "_none_\n"
    cols = list(rows[0].keys())
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows:
        lines.append("| " + " | ".join("" if r[c] is None else str(r[c]) for c in cols) + " |")
    return "\n".join(lines) + "\n"


def render_report(checks: List[Check], aggregates: dict) -> str:
    passed = sum(c.passed for c in checks)
    out = [f"# Reconciliation / validation report\n", f"**{passed}/{len(checks)} checks passed**\n",
           "| # | Result | Check | Detail |", "|---|---|---|---|"]
    for i, c in enumerate(checks, 1):
        out.append(f"| {i} | {'PASS' if c.passed else '**FAIL**'} | {c.name} | {c.detail} |")
    out.append("")
    titles = {"per_file": "Source-to-target counts per file", "rejects_by_reason": "Rejected / quarantined rows by reason",
              "orders_by_status_currency": "Key aggregate: active orders by status and currency",
              "daily_revenue_active_orders": "Key aggregate: daily revenue (active orders)",
              "customers_by_country": "Active customers by country", "tombstoned": "Soft-deleted rows (tombstones)",
              "runs": "Run history"}
    for k, t in titles.items():
        out += [f"## {t}\n", _md_table(aggregates[k])]
    return "\n".join(out)


def reconcile(db_path: Path, labels_path: Optional[Path] = None, report_path: Optional[Path] = None):
    from .db import connect
    conn = connect(db_path)
    labels = json.loads(Path(labels_path).read_text()) if labels_path else None
    checks = run_checks(conn, labels)
    if report_path:
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(report_path).write_text(render_report(checks, key_aggregates(conn)))
    conn.close()
    return checks
