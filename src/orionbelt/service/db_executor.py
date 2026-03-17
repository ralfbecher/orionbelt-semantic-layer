"""Database query execution service.

Bridges the CompilationPipeline output to actual database execution
via ob-flight-extension's db_router. Used by POST /v1/query/execute.
"""

from __future__ import annotations

import base64
import contextlib
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


class ExecutionUnavailableError(Exception):
    """Raised when query execution is not available (missing package or config)."""


class ExecutionError(Exception):
    """Raised when database execution fails."""


@dataclass
class ColumnMeta:
    """Metadata for a single result column."""

    name: str
    type_hint: str  # "string" | "number" | "datetime" | "binary"


@dataclass
class ExecutionResult:
    """Result of a database query execution."""

    columns: list[ColumnMeta] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0


def _map_type_code(type_code: Any) -> str:
    """Map a PEP 249 type code to a simple string type hint."""
    try:
        from ob_driver_core.type_codes import BINARY, DATETIME, NUMBER, STRING

        if type_code == NUMBER or type_code is NUMBER:
            return "number"
        if type_code == STRING or type_code is STRING:
            return "string"
        if type_code == DATETIME or type_code is DATETIME:
            return "datetime"
        if type_code == BINARY or type_code is BINARY:
            return "binary"
    except ImportError:
        pass
    return "string"


def _serialize_value(val: Any) -> Any:
    """Convert a Python value to a JSON-serializable type."""
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bytes):
        return base64.b64encode(val).decode("ascii")
    return str(val)


def _serialize_row(row: Any) -> list[Any]:
    """Convert a result row to a list of JSON-serializable values."""
    return [_serialize_value(v) for v in row]


def execute_sql(sql: str, *, dialect: str) -> ExecutionResult:
    """Execute SQL against the configured vendor database.

    The SQL is expected to include a LIMIT clause (enforced by the caller).

    Raises:
        ExecutionUnavailableError: if ob-flight-extension or vendor driver
            is not installed, or credentials are missing.
        ExecutionError: if the database connection or query fails.
    """
    try:
        from ob_flight.db_router import connect as db_connect
    except ImportError:
        raise ExecutionUnavailableError(
            "ob-flight-extension package is not installed. "
            "Install with: uv sync --extra flight"
        ) from None

    t0 = time.monotonic()
    conn = None
    cursor = None
    try:
        conn = db_connect(dialect)
        cursor = conn.cursor()
        cursor.execute(sql)

        # Build column metadata from cursor.description
        columns: list[ColumnMeta] = []
        if cursor.description:
            for col_desc in cursor.description:
                name = col_desc[0]
                type_code = col_desc[1]
                columns.append(ColumnMeta(name=name, type_hint=_map_type_code(type_code)))

        raw_rows = cursor.fetchall()
        rows = [_serialize_row(r) for r in raw_rows]
        elapsed_ms = (time.monotonic() - t0) * 1000

        return ExecutionResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=round(elapsed_ms, 2),
        )
    except ExecutionUnavailableError:
        raise
    except KeyError as exc:
        raise ExecutionUnavailableError(str(exc)) from None
    except Exception as exc:
        raise ExecutionError(f"Database execution failed: {exc}") from exc
    finally:
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()
