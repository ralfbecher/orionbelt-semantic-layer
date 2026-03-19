"""Flight catalog — maps OrionBelt model metadata to Flight schema info."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.flight as flight


# Map OBML abstract types to Arrow types
_OBML_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.utf8(),
    "int": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "timestamp": pa.timestamp("us"),
}


def object_to_schema(data_object: Any) -> pa.Schema:
    """Build an Arrow schema from a SemanticModel data object.

    Expects data_object to have .columns dict where each column has
    .label (str) and .abstract_type (str).
    """
    fields: list[pa.Field] = []
    if hasattr(data_object, "columns") and data_object.columns:
        for col_name, col in data_object.columns.items():
            abstract_type = getattr(col, "abstract_type", "string") or "string"
            arrow_type = _OBML_TYPE_MAP.get(abstract_type, pa.utf8())
            label = getattr(col, "label", col_name) or col_name
            fields.append(pa.field(label, arrow_type))
    return pa.schema(fields)


def model_to_flight_infos(
    model: Any,
    model_id: str,
) -> list[flight.FlightInfo]:
    """Convert a SemanticModel to a list of FlightInfo entries.

    One FlightInfo per data object — makes them browsable as "tables"
    in DBeaver's schema tree.
    """
    infos: list[flight.FlightInfo] = []
    if not hasattr(model, "data_objects") or not model.data_objects:
        return infos
    for obj_name, obj in model.data_objects.items():
        schema = object_to_schema(obj)
        descriptor = flight.FlightDescriptor.for_path(model_id, obj_name)
        info = flight.FlightInfo(schema, descriptor, [], -1, -1)
        infos.append(info)
    return infos
