"""Simple performance measurement + one optimisation (batched key lookups vs row-by-row)."""
from __future__ import annotations

import json
import shutil
import tempfile
import time
from pathlib import Path

from .config import Config
from .generator import generate
from .pipeline import run_pipeline


def run_benchmark(n_customers: int = 20000, n_orders: int = 100000, days: int = 7, seed: int = 42,
                  out_json: Path | None = None) -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="ingest_bench_"))
    try:
        t = time.perf_counter()
        generate(tmp / "data", n_customers, n_orders, days, seed)
        gen_s = time.perf_counter() - t
        results = {"dataset": {"customers": n_customers, "orders": n_orders, "days": days, "seed": seed,
                               "generate_seconds": round(gen_s, 2)}, "runs": []}
        for label, chunk in (("baseline: row-by-row lookups (lookup_chunk=1)", 1),
                             ("optimised: batched IN-list lookups (lookup_chunk=500)", 500)):
            db = tmp / f"bench_{chunk}.db"
            summary = run_pipeline(Config(tmp / "data" / "input", db, lookup_chunk=chunk))
            rows = summary.counters["rows_read"]
            results["runs"].append({"mode": label, "status": summary.status, "rows_read": rows,
                                    "seconds": summary.duration_seconds,
                                    "rows_per_second": round(rows / summary.duration_seconds) if summary.duration_seconds else None})
        rerun = run_pipeline(Config(tmp / "data" / "input", tmp / "bench_500.db"))
        results["noop_rerun_of_same_input"] = {"status": rerun.status, "files_skipped": rerun.files_skipped,
                                               "rows_read": rerun.counters["rows_read"], "seconds": rerun.duration_seconds}
        base, opt = results["runs"][0]["seconds"], results["runs"][1]["seconds"]
        results["speedup_x"] = round(base / opt, 2) if opt else None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    if out_json:
        Path(out_json).parent.mkdir(parents=True, exist_ok=True)
        Path(out_json).write_text(json.dumps(results, indent=2))
    return results
