"""Entity specifications + row-level validation / normalisation rules.

Two source entities:
  customers (grain: one row per customer_id)
  orders    (grain: one row per order_id; many orders -> one customer)

Every source row is a full-row snapshot of the entity at `updated_at`, with
op = 'U' (upsert) or 'D' (delete / tombstone). Tombstones only need key + updated_at.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional, Tuple

from .config import TS_FORMAT

# ---- reason codes (stable, documented in docs/data_dictionary.md) -------------------------
MALFORMED_ROW = "MALFORMED_ROW"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
INVALID_VALUE = "INVALID_VALUE"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
INVALID_REFERENCE = "INVALID_REFERENCE"
DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"
LATE_BEYOND_LOOKBACK = "LATE_BEYOND_LOOKBACK"

ORDER_STATUSES = ("NEW", "PAID", "SHIPPED", "CANCELLED")
CURRENCIES = ("INR", "USD", "EUR")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class RowError(Exception):
    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Record:
    key: str
    op: str  # 'U' | 'D'
    ts: str  # normalised updated_at (UTC ISO-8601)
    values: dict  # normalised attribute columns (empty for tombstones)
    record_hash: str
    line_no: int


@dataclass(frozen=True)
class EntitySpec:
    name: str
    table: str
    history_table: str
    key_col: str
    key_pattern: "re.Pattern[str]"
    attr_cols: Tuple[str, ...]  # target attribute columns (incl. derived ones)
    source_cols: Tuple[str, ...]  # columns accepted from the source
    required_cols: Tuple[str, ...]  # must exist in the file header (else file is quarantined)
    required_on_upsert: Tuple[str, ...]  # must be non-empty when op = 'U'
    normalise: Callable[[dict, str], dict]
    parent: Optional[Tuple[str, str]] = None  # (fk column, parent entity name)


def parse_ts(value: str) -> str:
    v = value.strip()
    # Fast path (profiled hotspot): already canonical 'YYYY-MM-DDTHH:MM:SSZ' -> validate, keep as is.
    if len(v) == 20 and v[10] == "T" and v[19] == "Z":
        try:
            datetime.fromisoformat(v[:19])
            return v
        except ValueError:
            raise RowError(INVALID_TIMESTAMP, f"not ISO-8601: {value!r}")
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        raise RowError(INVALID_TIMESTAMP, f"not ISO-8601: {value!r}")
    if dt.tzinfo is None:
        raise RowError(INVALID_TIMESTAMP, f"timezone designator required: {value!r}")
    return dt.astimezone(timezone.utc).strftime(TS_FORMAT)


def _norm_customer(raw: dict, ts: str) -> dict:
    email = raw["email"].lower()
    if not EMAIL_RE.match(email):
        raise RowError(INVALID_VALUE, f"email malformed: {raw['email']!r}")
    country = raw["country"].upper()
    if not re.fullmatch(r"[A-Z]{2}", country):
        raise RowError(INVALID_VALUE, f"country must be ISO-3166 alpha-2: {raw['country']!r}")
    return {"name": " ".join(raw["name"].split()), "email": email, "country": country}


def _norm_order(raw: dict, ts: str) -> dict:
    if not re.fullmatch(r"C\d{6}", raw["customer_id"]):
        raise RowError(INVALID_VALUE, f"customer_id format: {raw['customer_id']!r}")
    status = raw["status"].upper()
    if status not in ORDER_STATUSES:
        raise RowError(INVALID_VALUE, f"status not in {ORDER_STATUSES}: {raw['status']!r}")
    currency = raw["currency"].upper()
    if currency not in CURRENCIES:
        raise RowError(INVALID_VALUE, f"currency not in {CURRENCIES}: {raw['currency']!r}")
    try:
        amount = Decimal(raw["amount"])
    except InvalidOperation:
        raise RowError(INVALID_VALUE, f"amount not numeric: {raw['amount']!r}")
    if not amount.is_finite() or amount < 0 or amount.as_tuple().exponent < -2:
        raise RowError(INVALID_VALUE, f"amount must be a finite non-negative number with <=2 decimals: {raw['amount']!r}")
    order_ts = parse_ts(raw["order_ts"])
    if ts < order_ts:
        raise RowError(BUSINESS_RULE_VIOLATION, f"updated_at {ts} is before order_ts {order_ts}")
    return {
        "customer_id": raw["customer_id"],
        "status": status,
        "amount_minor": int((amount * 100).to_integral_value()),  # derived: integer minor units
        "currency": currency,
        "order_ts": order_ts,
        "order_date": order_ts[:10],  # derived
    }


CUSTOMERS = EntitySpec(
    name="customers", table="customers", history_table="customers_history",
    key_col="customer_id", key_pattern=re.compile(r"C\d{6}"),
    attr_cols=("name", "email", "country"),
    source_cols=("customer_id", "name", "email", "country", "updated_at", "op"),
    required_cols=("customer_id", "name", "email", "country", "updated_at", "op"),
    required_on_upsert=("name", "email", "country"),
    normalise=_norm_customer,
)

ORDERS = EntitySpec(
    name="orders", table="orders", history_table="orders_history",
    key_col="order_id", key_pattern=re.compile(r"O\d{8}"),
    attr_cols=("customer_id", "status", "amount_minor", "currency", "order_ts", "order_date"),
    source_cols=("order_id", "customer_id", "status", "amount", "currency", "order_ts", "updated_at", "op"),
    required_cols=("order_id", "customer_id", "status", "amount", "currency", "order_ts", "updated_at", "op"),
    required_on_upsert=("customer_id", "status", "amount", "currency", "order_ts"),
    normalise=_norm_order,
    parent=("customer_id", "customers"),
)

# Processing order matters: parents before children (referential integrity).
ENTITIES = {"customers": CUSTOMERS, "orders": ORDERS}
ENTITY_ORDER = ("customers", "orders")


def record_hash(entity: str, key: str, op: str, ts: str, values: dict) -> str:
    # cheaper than json.dumps (profiled hotspot); \x1f cannot appear in validated fields
    payload = "\x1f".join([entity, key, op, ts, *[f"{k}={values[k]}" for k in sorted(values)]])
    return hashlib.sha256(payload.encode()).hexdigest()


def validate_row(spec: EntitySpec, raw: dict, line_no: int, latest_allowed_ts: str) -> Record:
    """Validate + normalise one raw row. Raises RowError with a reason code."""
    raw = {k: (v or "").strip() for k, v in raw.items()}
    key = raw.get(spec.key_col, "")
    if not key:
        raise RowError(MISSING_REQUIRED_FIELD, f"{spec.key_col} is empty")
    if not spec.key_pattern.fullmatch(key):
        raise RowError(INVALID_VALUE, f"{spec.key_col} format: {key!r}")
    op = raw.get("op", "").upper()
    if op not in ("U", "D"):
        raise RowError(INVALID_VALUE, f"op must be 'U' or 'D': {raw.get('op')!r}")
    if not raw.get("updated_at"):
        raise RowError(MISSING_REQUIRED_FIELD, "updated_at is empty")
    ts = parse_ts(raw["updated_at"])
    if ts > latest_allowed_ts:
        raise RowError(FUTURE_TIMESTAMP, f"updated_at {ts} is after allowed {latest_allowed_ts}")
    values: dict = {}
    if op == "U":
        for col in spec.required_on_upsert:
            if not raw.get(col):
                raise RowError(MISSING_REQUIRED_FIELD, f"{col} is empty")
        values = spec.normalise(raw, ts)
    return Record(key, op, ts, values, record_hash(spec.name, key, op, ts, values), line_no)
