"""CSV batch reader: separates well-formed rows from malformed lines and checks the header."""
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple

from .entities import EntitySpec


class SchemaError(Exception):
    """File-level problem (e.g. required column missing). The whole file is quarantined."""


@dataclass
class Batch:
    rows: List[Tuple[int, dict]] = field(default_factory=list)  # (line_no, {col: value})
    malformed: List[Tuple[int, list, str]] = field(default_factory=list)  # (line_no, fields, detail)
    extra_columns: List[str] = field(default_factory=list)  # tolerated schema drift
    header: List[str] = field(default_factory=list)


def read_batch(path: Path, spec: EntitySpec) -> Batch:
    batch = Batch()
    if path.stat().st_size == 0:  # zero-byte file == empty batch, not an error
        return batch
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        try:
            header = [h.strip().lower() for h in next(reader)]
        except StopIteration:
            return batch
        if len(set(header)) != len(header):
            raise SchemaError(f"duplicate column names in header: {header}")
        missing = [c for c in spec.required_cols if c not in header]
        if missing:
            raise SchemaError(f"missing required columns {missing}; header was {header}")
        batch.header = header
        batch.extra_columns = [c for c in header if c not in spec.source_cols]
        for fields in reader:
            line_no = reader.line_num
            if not fields:  # blank line
                continue
            if len(fields) != len(header):
                batch.malformed.append(
                    (line_no, fields, f"expected {len(header)} fields, found {len(fields)}")
                )
                continue
            batch.rows.append((line_no, dict(zip(header, fields))))
    return batch
