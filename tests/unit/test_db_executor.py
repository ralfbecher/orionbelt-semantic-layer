"""Unit tests for the db_executor service module."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from orionbelt.service.db_executor import (
    ExecutionError,
    ExecutionResult,
    ExecutionUnavailableError,
    _map_type_code,
    _serialize_row,
    _serialize_value,
    execute_sql,
)


class TestSerializeValue:
    def test_none(self) -> None:
        assert _serialize_value(None) is None

    def test_string(self) -> None:
        assert _serialize_value("hello") == "hello"

    def test_int(self) -> None:
        assert _serialize_value(42) == 42

    def test_float(self) -> None:
        assert _serialize_value(3.14) == 3.14

    def test_bool(self) -> None:
        assert _serialize_value(True) is True

    def test_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert _serialize_value(dt) == "2024-01-15T10:30:00"

    def test_date(self) -> None:
        d = date(2024, 6, 1)
        assert _serialize_value(d) == "2024-06-01"

    def test_decimal(self) -> None:
        assert _serialize_value(Decimal("99.95")) == 99.95

    def test_bytes(self) -> None:
        assert _serialize_value(b"\x00\x01\x02") == "AAEC"

    def test_other_type(self) -> None:
        assert _serialize_value({"key": "val"}) == "{'key': 'val'}"


class TestSerializeRow:
    def test_mixed_row(self) -> None:
        row = ("US", 42, Decimal("100.5"), None, datetime(2024, 1, 1))
        result = _serialize_row(row)
        assert result == ["US", 42, 100.5, None, "2024-01-01T00:00:00"]


class TestMapTypeCode:
    def test_number_type(self) -> None:
        from ob_driver_core.type_codes import NUMBER

        assert _map_type_code(NUMBER) == "number"

    def test_string_type(self) -> None:
        from ob_driver_core.type_codes import STRING

        assert _map_type_code(STRING) == "string"

    def test_datetime_type(self) -> None:
        from ob_driver_core.type_codes import DATETIME

        assert _map_type_code(DATETIME) == "datetime"

    def test_binary_type(self) -> None:
        from ob_driver_core.type_codes import BINARY

        assert _map_type_code(BINARY) == "binary"

    def test_unknown_defaults_to_string(self) -> None:
        assert _map_type_code("unknown") == "string"

    def test_none_defaults_to_string(self) -> None:
        assert _map_type_code(None) == "string"


class TestExecuteSql:
    def test_import_error_raises_unavailable(self) -> None:
        with (
            patch.dict("sys.modules", {"ob_flight": None, "ob_flight.db_router": None}),
            pytest.raises(ExecutionUnavailableError, match="ob-flight-extension"),
        ):
            execute_sql("SELECT 1", dialect="duckdb")

    def test_successful_execution(self) -> None:
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("country", "STRING", None, None, None, None, None),
            ("revenue", "NUMBER", None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [("US", 100), ("UK", 200)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch(
            "ob_flight.db_router.connect", return_value=mock_conn, create=True
        ):
            result = execute_sql("SELECT country, revenue FROM t", dialect="duckdb")

        assert isinstance(result, ExecutionResult)
        assert result.row_count == 2
        assert len(result.columns) == 2
        assert result.columns[0].name == "country"
        assert result.rows == [["US", 100], ["UK", 200]]
        assert result.execution_time_ms >= 0
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_db_error_raises_execution_error(self) -> None:
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.execute.side_effect = RuntimeError("connection refused")

        with (
            patch("ob_flight.db_router.connect", return_value=mock_conn, create=True),
            pytest.raises(ExecutionError, match="connection refused"),
        ):
            execute_sql("SELECT 1", dialect="duckdb")

    def test_unsupported_dialect_raises_unavailable(self) -> None:
        with (
            patch(
                "ob_flight.db_router.connect",
                side_effect=KeyError("Unsupported dialect: 'mysql'"),
                create=True,
            ),
            pytest.raises(ExecutionUnavailableError, match="mysql"),
        ):
            execute_sql("SELECT 1", dialect="mysql")

    def test_cursor_and_conn_closed_on_error(self) -> None:
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("boom")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with (
            patch("ob_flight.db_router.connect", return_value=mock_conn, create=True),
            pytest.raises(ExecutionError),
        ):
            execute_sql("SELECT 1", dialect="duckdb")

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
