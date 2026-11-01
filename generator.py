"""Deterministic synthetic data generator (fixed seed) + independent expected-answer labels.

Input files go to <out>/input/, labels go to <out>/labels/labels.json (kept separate on purpose).
The labels are computed from the generator's own model of the truth (max updated_at per key among
the *valid* events, plus the exact number of deliberately injected bad rows per reason code); they do
not use the pipeline code, so they act as an independent oracle.
"""
from __future__ import annotations

import csv
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from .config import iso
from .entities import (BUSINESS_RULE_VIOLATION, DUPLICATE_CONFLICT, FUTURE_TIMESTAMP, INVALID_REFERENCE,
                       INVALID_TIMESTAMP, INVALID_VALUE, LATE_BEYOND_LOOKBACK, MALFORMED_ROW,
                       MISSING_REQUIRED_FIELD)

CUST_HEADER = ["customer_id", "name", "email", "country", "updated_at", "op"]
ORD_HEADER = ["order_id", "customer_id", "status", "amount", "currency", "order_ts", "updated_at", "op"]
FIRST = ["Asha", "Ravi", "Meera", "Karthik", "Divya", "Arjun", "Sneha", "Vikram", "Anita", "Rohit", "Priya", "Kiran"]
LAST = ["Reddy", "Sharma", "Naidu", "Iyer", "Patel", "Gupta", "Rao", "Menon", "Singh", "Das"]
COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AE"]
FLOW = ["NEW", "PAID", "SHIPPED"]


def _fmt_amount(minor: int) -> str:
    return f"{minor // 100}.{minor % 100:02d}"


def generate(out_dir: Path, n_customers: int = 40, n_orders: int = 120, days: int = 5, seed: int = 42,
             start_date: str = "2024-01-01", p_late: float = 0.30) -> dict:
    assert days >= 5, "need >= 5 daily batches so the watermark has moved > 48h past day 0 (beyond-lookback rows)"
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    in_dir, lab_dir = out_dir / "input", out_dir / "labels"
    in_dir.mkdir(parents=True, exist_ok=True)
    lab_dir.mkdir(parents=True, exist_ok=True)
    for old in in_dir.glob("*.csv"):
        old.unlink()
    base = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)

    def ts(day: int) -> str:
        return iso(base + timedelta(days=day, seconds=rng.randrange(60, 86000)))

    # files[(entity, day)] -> list of rows (lists of str). Tracks truth separately.
    files: Dict[tuple, List[list]] = defaultdict(list)
    final_events: Dict[tuple, dict] = {}  # (entity, key) -> final event (max ts)
    all_events: List[dict] = []

    def place(entity, key, day, op, attrs, ev_ts, row, delayable=True):
        # late delivery: 1 or 2 batches (<= 48h, inside the default lookback). A 2-batch delay lets an older
        # version arrive AFTER a newer one was already loaded => exercises the 'stale' path.
        delay = rng.choice([1, 2]) if (delayable and rng.random() < p_late) else 0
        batch_day = min(day + delay, days - 1)
        ev = dict(entity=entity, key=key, day=day, ts=ev_ts, op=op, attrs=attrs, batch=batch_day, row=row)
        files[(entity, batch_day)].append(row)
        all_events.append(ev)
        cur = final_events.get((entity, key))
        if cur is None or ev_ts > cur["ts"]:
            final_events[(entity, key)] = ev

    # ---- customers: created on day 0 (never delayed), then updates / deletes / resurrections -----
    for i in range(1, n_customers + 1):
        cid = f"C{i:06d}"
        attrs = dict(name=f"{rng.choice(FIRST)} {rng.choice(LAST)}", country=rng.choice(COUNTRIES))
        attrs["email"] = f"user{i}@example.com"
        t = ts(0)
        place("customers", cid, 0, "U", dict(attrs), t, [cid, attrs["name"], attrs["email"], attrs["country"], t, "U"],
              delayable=False)
        later = list(range(1, days))
        used = sorted(rng.sample(later, rng.choice([0, 0, 1, 1, 2]))) if later else []
        deleted = False
        for d in used:
            t = ts(d)
            if not deleted and rng.random() < 0.12:
                place("customers", cid, d, "D", {}, t, [cid, "", "", "", t, "D"])
                deleted = True
            else:
                attrs = dict(attrs)
                if rng.random() < 0.5:
                    attrs["email"] = f"user{i}.{d}@example.com"
                else:
                    attrs["country"] = rng.choice(COUNTRIES)
                place("customers", cid, d, "U", dict(attrs), t,
                      [cid, attrs["name"], attrs["email"], attrs["country"], t, "U"])
                deleted = False

    # ---- orders -------------------------------------------------------------------------------
    for i in range(1, n_orders + 1):
        oid = f"O{i:08d}"
        cid = f"C{rng.randint(1, n_customers):06d}"
        d0 = rng.randrange(days)
        order_ts = ts(d0)
        minor = rng.randint(100, 500000)
        cur = dict(customer_id=cid, status="NEW", amount_minor=minor, currency=rng.choice(["INR", "INR", "USD", "EUR"]))
        row = lambda a, t, op="U": [oid, cid, a["status"], _fmt_amount(a["amount_minor"]), a["currency"], order_ts, t, op]
        place("orders", oid, d0, "U", dict(cur), order_ts, row(cur, order_ts))
        stage, deleted = 0, False
        pool = list(range(d0 + 1, days))
        used = sorted(rng.sample(pool, min(len(pool), rng.choice([0, 1, 1, 2]))))
        for d in used:
            t = ts(d)
            if not deleted and rng.random() < 0.05:
                place("orders", oid, d, "D", {}, t, [oid, "", "", "", "", "", t, "D"])
                deleted = True
                continue
            cur = dict(cur)
            if rng.random() < 0.08:
                cur["status"] = "CANCELLED"
            else:
                stage = min(stage + 1, 2)
                cur["status"] = FLOW[stage]
            deleted = False
            place("orders", oid, d, "U", dict(cur), t, row(cur, t))

    # ---- deliberately injected anomalies (fresh keys => they never touch the valid-truth model) ---
    n_bad = max(2, round(n_orders * 0.02))
    expected_rejects: Counter = Counter()
    rows_extra = 0
    labels_notes = {}

    def put_bad(entity, row, reason, day=None):
        d = rng.randrange(days) if day is None else day
        files[(entity, d)].append(row)
        expected_rejects[reason] += 1

    seq = {"c": 800001, "o": 80000001, "lc": 900001, "lo": 90000001}
    def cid_new(kind="c"):
        v = seq[kind]; seq[kind] += 1; return f"C{v:06d}"
    def oid_new(kind="o"):
        v = seq[kind]; seq[kind] += 1; return f"O{v:08d}"

    for _ in range(n_bad):
        d = rng.randrange(days)
        c = cid_new(); t = ts(d)
        put_bad("customers", [c, "Bad Row", "a@b.com"], MALFORMED_ROW)                       # too few fields
        c = cid_new(); put_bad("customers", [c, "Bad Row", "a@b.com", "IN", t, "U", "x", "y"], MALFORMED_ROW)  # 8 fields: malformed even with the drift column
        c = cid_new(); put_bad("customers", [c, "No Email", "", "IN", t, "U"], MISSING_REQUIRED_FIELD)
        c = cid_new(); put_bad("customers", [c, "Bad Email", "not-an-email", "IN", t, "U"], INVALID_VALUE)
        c = cid_new(); put_bad("customers", [c, "Bad Country", "x@y.com", "India", t, "U"], INVALID_VALUE)
        c = cid_new(); put_bad("customers", [c, "Bad Time", "x@y.com", "IN", "2024-13-45 99:99", "U"], INVALID_TIMESTAMP)
        c = cid_new(); put_bad("customers", [c, "Future", "x@y.com", "IN", "2099-01-01T00:00:00Z", "U"], FUTURE_TIMESTAMP)
        o = oid_new(); cc = f"C{rng.randint(1, n_customers):06d}"; ot = ts(d)
        put_bad("orders", [o, cc, "PAID", "10.00"], MALFORMED_ROW)
        o = oid_new(); put_bad("orders", [o, cc, "", "10.00", "INR", ot, ot, "U"], MISSING_REQUIRED_FIELD)
        o = oid_new(); put_bad("orders", [o, cc, "PAID", "-5.00", "INR", ot, ot, "U"], INVALID_VALUE)
        o = oid_new(); put_bad("orders", [o, cc, "REFUNDED", "5.00", "INR", ot, ot, "U"], INVALID_VALUE)
        o = oid_new(); put_bad("orders", [o, cc, "PAID", "abc", "INR", ot, ot, "U"], INVALID_VALUE)
        o = oid_new(); put_bad("orders", [o, cc, "PAID", "5.00", "INR", "yesterday", ot, "U"], INVALID_TIMESTAMP)
        o = oid_new(); put_bad("orders", [o, cc, "PAID", "5.00", "INR", ot, "2099-01-01T00:00:00Z", "U"], FUTURE_TIMESTAMP)
        o = oid_new()
        early = iso(datetime.strptime(ot, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) - timedelta(hours=3))
        put_bad("orders", [o, cc, "PAID", "5.00", "INR", ot, early, "U"], BUSINESS_RULE_VIOLATION)
        o = oid_new(); put_bad("orders", [o, "C999999", "PAID", "5.00", "INR", ot, ot, "U"], INVALID_REFERENCE)

    # beyond-lookback: brand-new keys whose updated_at is from day 0 but arrive in the last batch
    last = days - 1
    for _ in range(max(2, n_bad // 2)):
        c = cid_new("lc"); t = ts(0)
        put_bad("customers", [c, "Very Late", "late@example.com", "IN", t, "U"], LATE_BEYOND_LOOKBACK, day=last)
        o = oid_new("lo"); ot = ts(0)
        put_bad("orders", [o, f"C{rng.randint(1, n_customers):06d}", "NEW", "9.99", "USD", ot, ot, "U"],
                LATE_BEYOND_LOOKBACK, day=last)

    # rows are shuffled inside each file (=> out-of-order data) BEFORE duplicates are injected, so that every
    # duplicate/conflict copy sits after its original (ties are applied in file order).
    for key_ in sorted(files):
        rng.shuffle(files[key_])

    # duplicates: copies of the *final* event of a key, placed in the same file right after the original
    n_exact = n_conflict = 0
    for entity, header_len in (("customers", 6), ("orders", 8)):
        finals = [e for (ent, _), e in sorted(final_events.items()) if ent == entity]
        for ev in rng.sample(finals, min(len(finals), n_bad)):
            rows = files[(entity, ev["batch"])]
            idx = rows.index(ev["row"])
            rows.insert(idx + 1, list(ev["row"]))
            n_exact += 1
        upserts = [e for e in finals if e["op"] == "U"]
        for ev in rng.sample(upserts, min(len(upserts), max(1, n_bad // 2))):
            rows = files[(entity, ev["batch"])]
            idx = rows.index(ev["row"])
            alt = list(ev["row"])
            if entity == "customers":
                alt[1] = alt[1] + " (conflict)"
            else:
                alt[3] = _fmt_amount(int(alt[3].replace(".", "")) + 1)
            rows.insert(idx + 1, alt)
            expected_rejects[DUPLICATE_CONFLICT] += 1

    # ---- write files (rows shuffled inside each file => out-of-order data) ------------------------
    data_rows = 0
    drift_file = None
    for (entity, day), rows in sorted(files.items()):
        name = f"{entity}_{(base + timedelta(days=day)).strftime('%Y%m%d')}_b{day + 1:02d}.csv"
        header = list(CUST_HEADER if entity == "customers" else ORD_HEADER)
        extra = entity == "customers" and day == 2
        with open(in_dir / name, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header + (["loyalty_tier"] if extra else []))
            for r in rows:
                # only well-formed rows get the extra column; injected malformed rows stay malformed
                w.writerow(r + ["GOLD"] if (extra and len(r) == len(header)) else r)
        if extra:
            drift_file = name
        data_rows += len(rows)

    # empty batch (header only), zero-byte batch, and a file with a missing required column
    stamp = (base + timedelta(days=last)).strftime("%Y%m%d")
    (in_dir / f"customers_{stamp}_b90_empty.csv").write_text(",".join(CUST_HEADER) + "\n")
    (in_dir / f"orders_{stamp}_b91_zero_bytes.csv").write_text("")
    with open(in_dir / f"customers_{stamp}_b92_bad_schema.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["customer_id", "name", "country", "updated_at", "op"])  # 'email' column missing
        w.writerow(["C700001", "Schema Break", "IN", ts(last), "U"])

    # ---- expected answers (independent of pipeline code) ---------------------------------------------
    exp = {"customers": {"active": 0, "deleted": 0},
           "orders": {"active": 0, "deleted": 0, "active_amount_minor_by_currency": Counter()}}
    for (entity, _), ev in final_events.items():
        if ev["op"] == "D":
            exp[entity]["deleted"] += 1
        else:
            exp[entity]["active"] += 1
            if entity == "orders":
                exp["orders"]["active_amount_minor_by_currency"][ev["attrs"]["currency"]] += ev["attrs"]["amount_minor"]
    exp["orders"]["active_amount_minor_by_currency"] = dict(sorted(exp["orders"]["active_amount_minor_by_currency"].items()))
    labels = {
        "generator": {"seed": seed, "customers": n_customers, "orders": n_orders, "days": days,
                      "start_date": start_date, "p_late": p_late},
        "expected_final_state": exp,
        "expected_rejects_by_reason": dict(sorted(expected_rejects.items())),
        "expected_exact_duplicates": n_exact,
        "expected_rows_read": data_rows,
        "expected_quarantined_files": ["customers_%s_b92_bad_schema.csv" % stamp],
        "schema_drift_file_with_extra_column": drift_file,
        "note": "Labels are the answer key; they are never read by the pipeline.",
    }
    (lab_dir / "labels.json").write_text(json.dumps(labels, indent=2))
    return labels
