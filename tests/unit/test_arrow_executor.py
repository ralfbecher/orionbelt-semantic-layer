"""Unit tests for ``_arrow_to_rows`` — driver-agnostic.

Fabricates the exact Arrow extension type ADBC postgres emits for
``NUMERIC`` columns without requiring a live database. Pinned here so a
regression in the string→Decimal normalisation surfaces in the fast unit
suite, not only in the optional ``-m adbc`` integration tests.
"""

from __future__ import annotations

import pyarrow as pa

from orionbelt.service.db_executor import (
    _arrow_to_rows,
    _is_string_stored_numeric_arrow_type,
)


def _opaque_numeric_table(values: list[str]) -> pa.Table:
    """Build a one-column Arrow table with the ADBC-postgres-style
    ``opaque[storage_type=string, type_name=numeric]`` extension type."""
    opaque = pa.opaque(pa.string(), type_name="numeric", vendor_name="PostgreSQL")
    arr = pa.array(values, type=pa.string()).cast(opaque)
    return pa.table({"revenue": arr})


class TestIsStringStoredNumericArrowType:
    def test_recognises_postgres_numeric_opaque(self) -> None:
        opaque = pa.opaque(pa.string(), type_name="numeric", vendor_name="PostgreSQL")
        assert _is_string_stored_numeric_arrow_type(opaque) is True

    def test_recognises_decimal_type_name(self) -> None:
        opaque = pa.opaque(pa.string(), type_name="DECIMAL", vendor_name="X")
        assert _is_string_stored_numeric_arrow_type(opaque) is True

    def test_rejects_string_type(self) -> None:
        # Plain string column — not an extension, not numeric.
        assert _is_string_stored_numeric_arrow_type(pa.string()) is False

    def test_rejects_decimal128(self) -> None:
        # decimal128 is the "happy" path; pydict already yields Decimal.
        assert _is_string_stored_numeric_arrow_type(pa.decimal128(18, 2)) is False

    def test_rejects_string_extension_with_non_numeric_type_name(self) -> None:
        opaque = pa.opaque(pa.string(), type_name="json", vendor_name="X")
        assert _is_string_stored_numeric_arrow_type(opaque) is False


class TestArrowToRowsStringNumericNormalisation:
    def test_string_numeric_is_parsed_to_float(self) -> None:
        """The string cell from ADBC's opaque[string] type is parsed to
        Decimal in ``_arrow_to_rows``; ``_serialize_value`` then converts
        to float for downstream JSON-friendly output."""
        table = _opaque_numeric_table(["2045134942.09"])
        rows = _arrow_to_rows(table)
        assert len(rows) == 1
        cell = rows[0][0]
        assert isinstance(cell, float)
        assert cell == 2045134942.09

    def test_unparseable_string_passes_through(self) -> None:
        """Garbage in the string column doesn't crash — falls back to the
        original string. Defensive: prefer surfacing a string to the user
        over a 500."""
        table = _opaque_numeric_table(["not-a-number"])
        rows = _arrow_to_rows(table)
        # _serialize_value sees the original string and returns it unchanged.
        assert rows[0][0] == "not-a-number"

    def test_plain_decimal128_unchanged(self) -> None:
        """The non-ADBC path (pydict already yields ``Decimal``) is
        unaffected — _serialize_value still converts Decimal → float."""
        from decimal import Decimal

        arr = pa.array([Decimal("123.45")], type=pa.decimal128(18, 2))
        table = pa.table({"x": arr})
        rows = _arrow_to_rows(table)
        assert rows[0][0] == 123.45
        assert isinstance(rows[0][0], float)
