"""PEP 249 Cursor wrapping a clickhouse-connect Client for ClickHouse.

clickhouse-connect is **not** DB-API 2.0 — it exposes a ``Client`` with
``.query()`` / ``.command()`` methods and a ``QueryResult`` object.  This class
adapts that interface to PEP 249 so that OBML-aware code (and plain SQL) can
use standard cursor semantics.

Each ``execute()`` fetches the entire result set into memory (client-side
buffering).
"""

from __future__ import annotations

from typing import Any

from ob_clickhouse.compiler import compile_obml, is_obml, parse_obml
from ob_clickhouse.exceptions import NotSupportedError, ProgrammingError
from ob_clickhouse.type_codes import CH_TYPE_MAP, STRING


class Cursor:
    """DB-API 2.0 cursor wrapping a clickhouse-connect Client.

    The underlying ``Client.query()`` returns a ``QueryResult`` with
    ``.result_rows``, ``.column_names``, and ``.column_types``.  This cursor
    stores the full result set after each ``execute()`` and exposes it through
    the standard ``fetch*()`` methods.
    """

    arraysize: int = 1

    def __init__(
        self,
        client: Any,
        *,
        ob_api_url: str = "http://localhost:8000",
        ob_timeout: int = 30,
    ) -> None:
        self._client = client
        self._closed = False
        self._ob_api_url = ob_api_url
        self._ob_timeout = ob_timeout
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self._description: (
            tuple[tuple[str, Any, None, None, None, None, None], ...] | None
        ) = None
        self._rowcount: int = -1

    # -- PEP 249 attributes --------------------------------------------------

    @property
    def description(
        self,
    ) -> tuple[tuple[str, Any, None, None, None, None, None], ...] | None:
        """PEP 249 cursor description — 7-item tuples per column."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows produced by the last ``execute()``."""
        return self._rowcount

    @property
    def lastrowid(self) -> None:
        """ClickHouse does not expose lastrowid."""
        return None

    # -- Internal helpers -----------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cursor is closed.")

    def _resolve_sql(self, operation: str) -> str:
        """Compile OBML to SQL or return plain SQL unchanged."""
        if not is_obml(operation):
            return operation
        obml = parse_obml(operation)
        return compile_obml(
            obml,
            dialect="clickhouse",
            ob_api_url=self._ob_api_url,
            ob_timeout=self._ob_timeout,
        )

    def _build_description(self, result: Any) -> None:
        """Build PEP 249 description from a clickhouse-connect QueryResult."""
        names: list[str] = result.column_names
        types: list[Any] = result.column_types
        cols: list[tuple[str, Any, None, None, None, None, None]] = []
        for i, name in enumerate(names):
            # column_types are ClickHouseType objects; str() gives e.g.
            # "Decimal(18,2)", "Nullable(Int64)".  Strip Nullable wrapper
            # and parenthesised params for the base-type lookup.
            type_str = str(types[i]) if i < len(types) else ""
            # Unwrap Nullable(...)
            if type_str.startswith("Nullable(") and type_str.endswith(")"):
                type_str = type_str[len("Nullable(") : -1]
            # Strip parameters — e.g. Decimal(18,2) → Decimal
            base = type_str.split("(")[0]
            type_code = CH_TYPE_MAP.get(base, STRING)
            cols.append((name, type_code, None, None, None, None, None))
        self._description = tuple(cols) if cols else None

    # -- PEP 249 execute methods ----------------------------------------------

    def execute(self, operation: str, parameters: Any = None) -> Cursor:
        """Execute a query — OBML YAML or plain SQL."""
        self._check_open()
        sql = self._resolve_sql(operation)
        if parameters is not None:
            result = self._client.query(sql, parameters=parameters)
        else:
            result = self._client.query(sql)
        self._rows = [tuple(r) for r in result.result_rows]
        self._pos = 0
        self._rowcount = len(self._rows)
        self._build_description(result)
        return self

    def executemany(self, operation: str, seq_of_parameters: Any) -> None:
        """Execute against all parameter sequences.

        OBML queries are not supported with executemany — raises NotSupportedError.
        """
        self._check_open()
        if is_obml(operation):
            raise NotSupportedError("executemany() is not supported for OBML queries.")
        for params in seq_of_parameters:
            self._client.query(operation, parameters=params)
        self._description = None
        self._rows = []
        self._pos = 0
        self._rowcount = -1

    # -- PEP 249 fetch methods ------------------------------------------------

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row."""
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next *size* rows."""
        self._check_open()
        n = size if size is not None else self.arraysize
        rows = self._rows[self._pos : self._pos + n]
        self._pos += len(rows)
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
        self._check_open()
        rows = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rows

    # -- PEP 249 no-ops -------------------------------------------------------

    def setinputsizes(self, sizes: Any) -> None:
        """No-op — required by PEP 249."""

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """No-op — required by PEP 249."""

    # -- Lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Close the cursor.

        Does **not** close the underlying Client — the Client is owned by the
        Connection and shared across cursors.
        """
        if not self._closed:
            self._closed = True

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
