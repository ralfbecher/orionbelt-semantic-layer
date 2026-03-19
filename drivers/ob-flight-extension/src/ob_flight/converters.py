"""Arrow conversion utilities for Flight SQL results."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from ob_driver_core.type_codes import BINARY, DATETIME, NUMBER, STRING


def pep249_type_to_arrow(type_code: Any) -> pa.DataType:
    """Map a PEP 249 type code object to an Arrow data type.

    Uses equality comparison — PEP 249 type objects support __eq__ for membership testing.
    """
    if type_code == NUMBER:
        return pa.float64()
    if type_code == STRING:
        return pa.utf8()
    if type_code == DATETIME:
        return pa.timestamp("us")
    if type_code == BINARY:
        return pa.binary()
    return pa.utf8()  # fallback


def schema_from_description(
    description: tuple[tuple[str, Any, ...], ...],
) -> pa.Schema:
    """Build an Arrow Schema from PEP 249 cursor.description.

    Each description entry is a 7-tuple: (name, type_code, ...).
    """
    fields = []
    for col in description:
        name = col[0]
        type_code = col[1]
        arrow_type = pep249_type_to_arrow(type_code)
        fields.append(pa.field(name, arrow_type))
    return pa.schema(fields)


def rows_to_batch(
    rows: list[tuple[Any, ...]],
    schema: pa.Schema,
) -> pa.RecordBatch:
    """Convert a list of row tuples to an Arrow RecordBatch.

    Transposes row-major data to column-major for Arrow.
    """
    if not rows:
        return pa.RecordBatch.from_pydict(
            {field.name: [] for field in schema}, schema=schema
        )
    n_cols = len(schema)
    columns: dict[str, list[Any]] = {schema.field(i).name: [] for i in range(n_cols)}
    for row in rows:
        for i in range(n_cols):
            col_name = schema.field(i).name
            columns[col_name].append(row[i] if i < len(row) else None)
    return pa.RecordBatch.from_pydict(columns, schema=schema)


def cursor_to_batches(
    cursor: Any,
    schema: pa.Schema,
    batch_size: int = 1024,
) -> list[pa.RecordBatch]:
    """Fetch all rows from a PEP 249 cursor as Arrow RecordBatches.

    Fetches in chunks of batch_size to manage memory.
    Returns a list of RecordBatches (not a generator, for simplicity).
    """
    batches: list[pa.RecordBatch] = []
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        batches.append(rows_to_batch(rows, schema))
    return batches
