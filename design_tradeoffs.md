# Design / trade-off note

## Core idea
Every source row is a full snapshot with a source-side `updated_at`. The target keeps, per business key, the version with the highest
`updated_at`, and keeps every version in an append-only history. Correctness rests on three properties: (1) the merge is **idempotent**,
(2) it is **order-independent** for different versions of a key, (3) data, watermark and file ledger **commit atomically**. Together they make
reruns, overlaps, late data and crashes safe without needing "exactly once" delivery.

## Decisions and why
| Decision | Reason | Cost / alternative |
|---|---|---|
| Compare row versions by `updated_at`, not by arrival order | late or out-of-order rows can never overwrite newer data | needs a trustworthy source timestamp (assumption) |
| Watermark = max accepted `updated_at`, **plus a lookback window** (48 h) instead of a hard `> watermark` filter | a strict filter silently loses late rows; the lookback turns "late" into a classification | rows later than the window need replay (`--full-refresh`); a larger window = more audit noise |
| Late rows beyond the window that carry *new* information are **quarantined**, not applied or dropped | surprising rewrites of closed history should be a human decision; nothing is lost | extra operator step |
| Soft deletes + versioned tombstones, history kept | analysts can exclude deleted rows, audit can see them; an older upsert cannot resurrect a deleted key and a newer one can | tombstone-only rows have NULL attributes |
| One SQLite transaction per file (data + history + rejects + ledger + watermark) | crash safety; watermark can never run ahead of data | a very large file is all-or-nothing (next step: chunked checkpoints keyed by file + line) |
| File ledger on `(name, SHA-256)` | cheap no-op reruns, and a corrected file is reprocessed automatically | a deliberately identical re-drop is skipped (use `--full-refresh`) |
| Fail-fast on unexpected errors, continue past schema-broken files | an unexpected error may leave later files dependent (parents before children); a schema break is a data problem isolated to one file | a quarantined customers file can cause `INVALID_REFERENCE` rejects downstream (replayable) |
| Same key + same version + different content → first wins, second rejected | deterministic regardless of retries | which one is right is a business question; the raw row is in `rejects` |
| Integer minor units for money; derived `order_date` | no float drift; simple date aggregates | – |
| SQLite + stdlib | zero setup, reviewer can run it anywhere, real transactions and constraints | single writer; production would use a warehouse `MERGE` with the same logic |
| Independent answer key (`labels.json`) generated separately from pipeline code | catches pipeline and generator bugs (it caught one while building) | generator must stay in sync with rules |

## Failure & recovery behaviour
* Crash at any point → open transaction rolls back; `file_log` records `FAILED_ERROR`; the run is `FAILED` (exit 1). Rerun skips committed files and retries the failed one. Tested equal to a clean run.
* Schema-broken file → `FAILED_SCHEMA`, run `PARTIAL` (exit 2); skipped on later runs until its content changes, with a WARNING each run.
* Retry = rerun. Restart = rerun. Backfill / replay = `--full-refresh` (idempotent).

## Performance (measured in this environment)
Dataset: 20k customers + 100k orders over 7 daily batches = 257,936 input rows (incl. injected bad rows), single process, SQLite.
* Full load ≈ **12 s (~21k rows/s)**; **no-op rerun of the same input ≈ 0.02 s** (ledger skip) — the main "incremental" win.
* Profiling (cProfile, ~50k rows) showed ~46 % of time in row validation (ISO timestamp parsing, JSON-based hashing), ~17 % in `executemany`, only ~4 % in key lookups.
* **Optimisation applied:** fast path for already-canonical UTC timestamps and a cheaper hash payload: same 258k-row dataset **14.1 s → 11.9 s (≈15 %)** end-to-end.
* Batched key lookups (`IN (...)` × 500) vs row-by-row: only 1.06–1.19× here because SQLite lookups are local and cheap; the benefit grows with a networked database. Kept because it also bounds memory per round-trip.
* Reasonable next steps: parse files in parallel processes, chunked commits, `executemany` in larger batches with `WITHOUT ROWID` tables.
(`python -m ingest benchmark` reproduces; numbers vary by machine.)

## Known limitations
* Source is local CSV files; no API/cloud connector (a thin adapter producing the same `Batch` would plug in).
* Unbalanced quotes inside a CSV can swallow following lines (standard CSV parsing); such rows surface as malformed/short rows.
* Watermark is per entity, not per partition; a source that rewrites history with older `updated_at` needs `--full-refresh`.
* After a `--full-refresh` replay, old `rejects` rows are not marked resolved (they stay as an audit trail).
* No concurrent runs: SQLite `BEGIN IMMEDIATE` serialises writers; a second concurrent run would wait/fail rather than corrupt.
* Timestamps with sub-second precision are accepted but truncated to seconds in the normalised form.

## What I would do next
Chunked checkpoints for huge files; alerting thresholds on reject rate/volume anomalies; a `resolved_by_run_id` on rejects; Parquet/warehouse
target adapter using the same version-comparison `MERGE`; property-based tests (Hypothesis) over random event interleavings.
