# Data dictionary

Conventions: `snake_case`; timestamps ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ` (offsets in the source are converted to UTC);
keys are business keys from the source; every target row carries `source_file` and `run_id`/`last_run_id`.

## 1. Source schemas (CSV, header row required, column order free, extra columns tolerated)

### `customers_*.csv`  — grain: one row per `customer_id` per change
| Column | Type | Required | Rule |
|---|---|---|---|
| customer_id | text | yes | `C` + 6 digits; business key |
| name | text | yes on `U` | whitespace collapsed |
| email | text | yes on `U` | lower-cased, must look like `x@y.z` |
| country | text | yes on `U` | ISO-3166 alpha-2, upper-cased |
| updated_at | timestamp | yes | ISO-8601 with `Z`/offset; not later than run time + 24 h |
| op | text | yes | `U` upsert, `D` delete (tombstone: only key + `updated_at` needed) |

### `orders_*.csv` — grain: one row per `order_id` per change; many orders → one customer
| Column | Type | Required | Rule |
|---|---|---|---|
| order_id | text | yes | `O` + 8 digits; business key |
| customer_id | text | yes on `U` | `C` + 6 digits; must exist in `customers` (soft-deleted customers count as existing) |
| status | text | yes on `U` | `NEW`, `PAID`, `SHIPPED`, `CANCELLED` (case-insensitive input, stored upper) |
| amount | decimal text | yes on `U` | finite, `>= 0`, at most 2 decimals |
| currency | text | yes on `U` | `INR`, `USD`, `EUR` |
| order_ts | timestamp | yes on `U` | when the order was created |
| updated_at | timestamp | yes | version timestamp; business rule `updated_at >= order_ts` |
| op | text | yes | `U` / `D` |

## 2. Target tables

### `customers` / `orders` (current state; one row per key)
| Column | Type | Null | Notes |
|---|---|---|---|
| customer_id / order_id | TEXT | PK | business key |
| name, email, country | TEXT | yes (only for tombstone-only keys) | |
| customer_id (orders) | TEXT | FK → customers.customer_id | |
| status, currency | TEXT | CHECK constraints | |
| amount_minor | INTEGER | `>= 0` | **derived**: `amount × 100` (minor units) |
| order_ts | TEXT | | |
| order_date | TEXT | | **derived**: `substr(order_ts, 1, 10)` |
| is_deleted | INTEGER | NOT NULL, 0/1 | 1 = soft-deleted by tombstone; last known attributes retained |
| source_updated_at | TEXT | NOT NULL | version currently held |
| record_hash | TEXT | NOT NULL | sha256 of canonical business fields; used to detect duplicate vs conflict |
| source_file | TEXT | NOT NULL | file that supplied the current version |
| first_run_id / last_run_id | TEXT | NOT NULL | run that first inserted / last changed the row |
| loaded_at | TEXT | NOT NULL | run clock at last change |

### `customers_history` / `orders_history` (append-only)
PK `(key, source_updated_at, record_hash)` ⇒ re-inserting a version is a no-op. Columns: key, `source_updated_at`, `record_hash`,
`op` (`U`/`D`), the attribute columns of that version (NULL for tombstones), `run_id`, `source_file`, `loaded_at`.
Contains every accepted version including stale (late) ones and tombstones; never contains rejected rows.

### `rejects` (quarantine)
| Column | Notes |
|---|---|
| reject_id (PK) | `sha1(entity|file|line|reason)[:16]` – deterministic, so reprocessing never duplicates |
| run_id, entity, source_file, line_no | trace back to the exact source line |
| reason_code, reason_detail | see below |
| raw_record | JSON of the original fields |
| rejected_at | run clock |

### Reason codes
`MALFORMED_ROW` (field count ≠ header) · `MISSING_REQUIRED_FIELD` · `INVALID_VALUE` (format/enum/range) · `INVALID_TIMESTAMP` ·
`FUTURE_TIMESTAMP` (> run time + 24 h) · `BUSINESS_RULE_VIOLATION` (`updated_at < order_ts`) · `INVALID_REFERENCE` (unknown customer) ·
`DUPLICATE_CONFLICT` (same key+version, different content) · `LATE_BEYOND_LOOKBACK` (new information older than watermark − lookback).

### Operational tables
| Table | Purpose / key columns |
|---|---|
| `etl_watermark` | PK `entity`; `watermark` = max `updated_at` of committed, accepted rows (monotonic: `MAX(old,new)`) |
| `file_log` | PK `(source_file, file_sha256)`; `status` SUCCESS / FAILED_SCHEMA / FAILED_ERROR; per-file `rows_read, rows_inserted, rows_updated, rows_deleted, rows_stale, rows_duplicate, rows_rejected`; `note` (e.g. schema drift) |
| `pipeline_runs` | PK `run_id`; `started_at, ended_at, status` (RUNNING/SUCCESS/PARTIAL/FAILED), file and row counters, `rejects_by_reason` (JSON), `duration_seconds`, `error_message`, `config_json` |

### Row outcome definitions (mutually exclusive; `rows_read` = their sum)
`inserted` new key · `updated` newer version of a known key (incl. resurrection) · `deleted` tombstone applied · `stale` older version, history only ·
`duplicate` same version and content as already loaded · `rejected` any reason above.

## 3. Keys, relationships, rules
* `orders.customer_id → customers.customer_id` (many-to-one), enforced by validation **and** a SQLite FOREIGN KEY.
* Uniqueness: one current row per key (PK); one history row per `(key, updated_at, hash)`.
* Nullability: attributes mandatory for `U` rows; NULL only on rows that exist solely because of a tombstone.
* Derived fields: `amount_minor`, `order_date`, `record_hash`, `is_deleted`.
