"""Command line interface:  python -m ingest {generate,run,reconcile,report,benchmark}"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .benchmark import run_benchmark
from .config import Config
from .generator import generate
from .pipeline import run_pipeline
from .reconcile import key_aggregates, reconcile, render_report, run_checks


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ingest", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create reproducible synthetic input + answer-key labels")
    g.add_argument("--out", required=True, type=Path)
    g.add_argument("--customers", type=int, default=40)
    g.add_argument("--orders", type=int, default=120)
    g.add_argument("--days", type=int, default=5)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--start-date", default="2024-01-01")

    r = sub.add_parser("run", help="run the incremental load (safe to re-run)")
    r.add_argument("--source", required=True, type=Path, help="directory with <entity>_*.csv batch files")
    r.add_argument("--db", required=True, type=Path)
    r.add_argument("--lookback-hours", type=int, default=48)
    r.add_argument("--full-refresh", action="store_true", help="re-merge everything; ignore ledger + lookback rule")
    r.add_argument("--metrics-dir", type=Path)
    r.add_argument("--as-of", help="override 'now' (ISO-8601, UTC) - used for the future-timestamp check")
    r.add_argument("--inject-failure-file", help="demo: crash just before COMMIT for matching file names")

    c = sub.add_parser("reconcile", help="run consistency + reconciliation checks")
    c.add_argument("--db", required=True, type=Path)
    c.add_argument("--labels", type=Path)
    c.add_argument("--report", type=Path)

    s = sub.add_parser("report", help="print key aggregates / run history")
    s.add_argument("--db", required=True, type=Path)

    b = sub.add_parser("benchmark", help="measure throughput, compare row-by-row vs batched lookups")
    b.add_argument("--customers", type=int, default=20000)
    b.add_argument("--orders", type=int, default=100000)
    b.add_argument("--out", type=Path)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    if args.cmd == "generate":
        labels = generate(args.out, args.customers, args.orders, args.days, args.seed, args.start_date)
        print(json.dumps({"input_dir": str(args.out / "input"), "labels": str(args.out / "labels" / "labels.json"),
                          "expected_rows_read": labels["expected_rows_read"]}, indent=2))
        return 0
    if args.cmd == "run":
        now = (lambda: datetime.fromisoformat(args.as_of.replace("Z", "+00:00")).astimezone(timezone.utc)) \
            if args.as_of else None
        cfg = Config(args.source, args.db, lookback_hours=args.lookback_hours, full_refresh=args.full_refresh,
                     metrics_dir=args.metrics_dir, inject_failure_file=args.inject_failure_file)
        if now:
            cfg.now = now
        summary = run_pipeline(cfg)
        print(json.dumps(summary.to_dict(), indent=2))
        return {"SUCCESS": 0, "PARTIAL": 2, "FAILED": 1}[summary.status]
    if args.cmd == "reconcile":
        checks = reconcile(args.db, args.labels, args.report)
        for ch in checks:
            print(f"[{'PASS' if ch.passed else 'FAIL'}] {ch.name} :: {ch.detail}")
        ok = all(ch.passed for ch in checks)
        print(f"\n{sum(c.passed for c in checks)}/{len(checks)} checks passed")
        return 0 if ok else 1
    if args.cmd == "report":
        from .db import connect
        conn = connect(args.db)
        print(render_report(run_checks(conn), key_aggregates(conn)))
        return 0
    if args.cmd == "benchmark":
        res = run_benchmark(args.customers, args.orders, out_json=args.out)
        print(json.dumps(res, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
