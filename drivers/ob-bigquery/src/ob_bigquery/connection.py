"""PEP 249 Connection wrapping ``google.cloud.bigquery.Client``."""

from __future__ import annotations

from typing import Any

from ob_bigquery.cursor import Cursor
from ob_bigquery.exceptions import ProgrammingError


class Connection:
    """DB-API 2.0 connection that wraps a native BigQuery client.

    OBML queries are compiled to SQL via the OrionBelt REST API
    (single-model mode, ``/v1/query/sql`` shortcut).
    """

    def __init__(
        self,
        native: Any,
        *,
        ob_api_url: str = "http://localhost:8000",
        ob_timeout: int = 30,
    ) -> None:
        self._native = native
        self._closed = False
        self._ob_api_url = ob_api_url
        self._ob_timeout = ob_timeout

    def _check_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Connection is closed.")

    def cursor(self) -> Cursor:
        """Return a new Cursor for this connection."""
        self._check_open()
        return Cursor(
            self._native,
            ob_api_url=self._ob_api_url,
            ob_timeout=self._ob_timeout,
        )

    def commit(self) -> None:
        """No-op — BigQuery auto-commits all statements."""

    def rollback(self) -> None:
        """No-op — BigQuery does not support transactions via this driver."""

    def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            self._native.close()
            self._closed = True

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
