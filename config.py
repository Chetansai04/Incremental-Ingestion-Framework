"""Run configuration. Everything tunable lives here (no hidden globals)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # ISO-8601, UTC, lexicographically sortable


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(TS_FORMAT)


@dataclass
class Config:
    source_dir: Path
    db_path: Path
    # Rows older than (watermark - lookback) that carry NEW information are quarantined
    # as LATE_BEYOND_LOOKBACK instead of silently rewriting old history.
    lookback_hours: int = 48
    # updated_at further in the future than now + skew is rejected (protects the watermark).
    max_future_skew_hours: int = 24
    # Keys fetched per SELECT ... IN (...) lookup. 1 == naive row-by-row (used by benchmark).
    lookup_chunk: int = 500
    # Ignore file ledger + lookback rule: re-merge everything (safe because merge is idempotent).
    full_refresh: bool = False
    # Fault injection for tests/demos: raise right before COMMIT for files whose name contains this.
    inject_failure_file: Optional[str] = None
    metrics_dir: Optional[Path] = None
    now: Callable[[], datetime] = utc_now
    run_id: Optional[str] = None

    def to_json(self) -> dict:
        return {
            "source_dir": str(self.source_dir),
            "db_path": str(self.db_path),
            "lookback_hours": self.lookback_hours,
            "max_future_skew_hours": self.max_future_skew_hours,
            "lookup_chunk": self.lookup_chunk,
            "full_refresh": self.full_refresh,
        }
