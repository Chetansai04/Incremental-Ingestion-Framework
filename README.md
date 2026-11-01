# Incremental Ingestion Framework (watermark + idempotent merge)

A small, tested, locally runnable data-engineering pipeline. It ingests incremental CSV extracts from a
source system into a SQLite "warehouse" and is safe to **rerun**, handles **late-arriving / out-of-order
records**, **deletes (tombstones)** and **recovery after partial failure** without ever duplicating target data.

* Python 3.9+, **standard library only** (SQLite ships with Python). `pytest` is needed only to run the tests.
* No credentials, no external services, no real data: everything is synthetic and seeded.

## Quick start (clean environment)

```bash
python3 -m venv .venv && source .venv/bin/activate     # optional
pip install -r requirements.txt                          # just pytest
make test        # 57 automated tests (~1 s)
make demo        # generate sample data -> crash mid-run -> restart -> rerun -> reconcile
make bench       # throughput on ~258k rows (~1 min)
```

Without `make`:

```bash
export PYTHONPATH=src
python -m ingest generate  --out sample_data --customers 40 --orders 120 --days 5 --seed 42
python -m ingest run       --source sample_data/input --db sample_output/warehouse.db --metrics-dir sample_output/metrics
python -m ingest reconcile --db sample_output/warehouse.db --labels sample_data/labels/labels.json --report sample_output/reconciliation_report.md
python -m ingest report    --db sample_output/warehouse.db
sqlite3 sample_output/warehouse.db < sql/sample_queries.sql      # or: python scripts/render_sample_queries.py
```

For a 100k+ row run: `python -m ingest generate --out big --customers 20000 --orders 100000 --days 7`.
`run` exit codes: `0` success, `2` partial (a file was quarantined), `1` failed (crash, rolled back; just rerun).

## Architecture / flow

```
 source extracts (CSV batch files, may overlap, arrive late / out of order)
   customers_YYYYMMDD_bNN.csv   orders_YYYYMMDD_bNN.csv
                │
                ▼  discover (parents first: customers → orders; name order)
        ┌─ file ledger check ── same name + SHA-256 already SUCCESS/FAILED_SCHEMA? → skip
        ▼
   read + header check ──missing required column──▶ file quarantined (FAILED_SCHEMA), run continues
        │ extra columns → tolerated + logged as schema drift
        ▼
   row validation (types, enums, ranges, ISO-8601, future-date guard)  ──bad──▶ rejects (reason, file, line, raw row)
        ▼
   referential check (order.customer_id exists)                         ──bad──▶ rejects (INVALID_REFERENCE)
        ▼
   merge in (updated_at, line) order against current state:
        new key → insert │ newer version → update │ older version → "stale" (history only)
        same version, same content → duplicate (no-op) │ same version, other content → DUPLICATE_CONFLICT reject
        older than watermark−lookback and NEW info → LATE_BEYOND_LOOKBACK reject (quarantine)
        op = D → soft delete (is_deleted = 1, last known attributes kept)
        ▼
   ONE TRANSACTION per file:  upsert current table + append history + rejects + file ledger + advance watermark
        ▼  COMMIT (or ROLLBACK on any error → nothing is half-applied)
   pipeline_runs row: run_id, start/end, per-outcome counts, rejects by reason, status, error
```

Target tables: `customers`, `orders` (current state, one row per key), `customers_history`, `orders_history`
(append-only, every valid version), `rejects` (quarantine), `etl_watermark`, `file_log`, `pipeline_runs`.
Full column list: [`docs/data_dictionary.md`](docs/data_dictionary.md). Trade-offs: [`docs/design_tradeoffs.md`](docs/design_tradeoffs.md).

## How each requirement is met

| Requirement | Where / how |
|---|---|
| Incremental via timestamps / watermarks | `etl_watermark` per entity = max `updated_at` of committed, accepted rows; drives the late-data rule (`watermark − lookback`). Row versions are compared on `updated_at`. |
| Reruns safe | file ledger (name + SHA-256) skips finished files; the merge itself is idempotent (version comparison, history PK `(key, updated_at, hash)`, deterministic `reject_id`). Tested: rerun, overlapping extract, `--full-refresh`. |
| Late-arriving / out-of-order | rows sorted by `(updated_at, line)` inside a file; older version arriving after a newer one is **stale** → history only, never overwrites current. New info older than `watermark − lookback` (default 48 h) is quarantined and can be replayed with `--full-refresh`. |
| Deletes / tombstones | `op = D` rows soft-delete (`is_deleted = 1`); delete is versioned too: an older upsert cannot resurrect, a newer upsert does. History keeps every version. Tombstone for an unseen key is stored so a later-arriving older upsert cannot win. |
| Recovery after partial failure | one transaction per file; watermark and ledger commit atomically with the data. Crash → rollback → restart resumes at the first unfinished file. Demonstrated with `--inject-failure-file` (see `make demo`) and tested to equal a clean run. |
| Expose failures / data quality | `rejects` table (reason code, file, line, raw record), `file_log` (per-file counts, schema drift notes, quarantined files), `pipeline_runs` metrics + JSON per run, WARNING/ERROR logs with `run_id`. |
| Measurable outputs | per-file and per-run counts `read = inserted + updated + deleted + stale + duplicate + rejected` (checked), reconciliation report, performance numbers. |
| Validation / acceptance criteria | `python -m ingest reconcile` → 18 checks incl. PK/FK integrity, current-vs-history agreement, derived fields, traceable rejects, and an independent answer key (`labels.json`). |

## Assumptions (also in the data dictionary)

* Each source row is a **full snapshot** of the entity at `updated_at` (not a delta). Tombstones need only key + `updated_at`.
* `updated_at` is set by the source, is in UTC (or carries an offset) and increases for each change of a key.
* Same key + same `updated_at` + different content is a source defect → the first loaded version wins, the other is rejected (`DUPLICATE_CONFLICT`).
* Customers are delivered before the orders that reference them (customers files are always processed first).
* Money is stored as integer minor units (`amount_minor`) to avoid floating point error.

## Naming conventions

`snake_case` tables/columns; keys are business keys (`C000001`, `O00000001`) kept verbatim; timestamps are ISO-8601 UTC
(`YYYY-MM-DDTHH:MM:SSZ`); `run_id = run_<UTC start>_<seq>`; `source_file` + `line_no` on every reject; `reject_id =
sha1(entity|file|line|reason)[:16]`; `record_hash = sha256` of the canonical business fields (deterministic).
Input files: `<entity>_<YYYYMMDD>_b<NN>[_note].csv`.

## Retry, restart, backfill

* **Retry / restart:** just run the same command again. Completed files are skipped by checksum, a failed file is retried.
* **Corrected file:** drop it under a new name or fix it in place (new checksum ⇒ reprocessed); a quarantined schema-broken file keeps
  logging a WARNING until its content changes.
* **Backfill / replay of quarantined late rows:** `python -m ingest run ... --full-refresh` re-merges every file ignoring the ledger
  and the lookback rule. Because the merge is idempotent this only adds what is missing (tested).

## Sample input / output (committed)

| Path | Content |
|---|---|
| `sample_data/input/*.csv` | 13 small input files (normal rows, updates, tombstones, duplicates, late + out-of-order rows, malformed rows, schema drift, empty/zero-byte batch, broken-schema file) |
| `sample_data/labels/labels.json` | answer key, kept **separate** from the input |
| `sample_output/warehouse.db` | resulting SQLite target |
| `sample_output/reconciliation_report.md` | 18/18 checks + source-to-target counts + key aggregates |
| `sample_output/sample_query_results.md` | results of `sql/sample_queries.sql` |
| `sample_output/metrics/*.json`, `demo_transcript.txt` | per-run metrics; crash → restart → rerun transcript |
| `sample_output/test_results.txt`, `performance.json` | pytest output; benchmark |

## Tests (`tests/`, 57 tests)

Unit: validation rules, CSV reader. Pipeline scenarios: happy path, empty input/batch, malformed rows, null required fields, duplicates
(exact + conflicting), rerun, overlapping extract, changed file, late/out-of-order data, beyond-lookback, tombstones (delete, no
resurrection by stale data, resurrection by newer data, tombstone-before-create), invalid reference, schema change (extra / missing column),
future-dated timestamp, crash-before-commit + restart equals clean run, daily increments equal one big run. End-to-end: generated data vs the
independent answer key. (A sweep over 20 seeds × 4 sizes also matched the key on all 80 datasets.)

## Known limitations / next steps

See the design note. Highlights: SQLite single-writer; file-based source only; a whole file is one transaction (very large files would
benefit from chunked checkpoints); malformed quoting that spans lines can swallow following lines; rejects of quarantined late rows are not
auto-marked resolved after a replay.
