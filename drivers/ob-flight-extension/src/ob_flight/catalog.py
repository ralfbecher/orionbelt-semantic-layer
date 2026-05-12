"""Flight catalog — maps OrionBelt model metadata to Flight schema info."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.flight as flight


# Map OBML abstract types to Arrow types. Covers the full OBML DataType enum.
_OBML_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.utf8(),
    "json": pa.utf8(),
    "int": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "time": pa.utf8(),
    "time_tz": pa.utf8(),
    "timestamp": pa.timestamp("us"),
    "timestamp_tz": pa.timestamp("us", tz="UTC"),
}


def _obml_type_to_arrow(type_name: str | None) -> pa.DataType:
    """Map an OBML type name to an Arrow type, defaulting to utf8."""
    if not type_name:
        return pa.utf8()
    return _OBML_TYPE_MAP.get(type_name, pa.utf8())


# ---------------------------------------------------------------------------
# Virtual metadata table schemas
# ---------------------------------------------------------------------------

DIMENSIONS_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("data_object", pa.utf8()),
        pa.field("column", pa.utf8()),
        pa.field("type", pa.utf8()),
        pa.field("time_grain", pa.utf8()),
        pa.field("description", pa.utf8()),
    ]
)

MEASURES_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("aggregation", pa.utf8()),
        pa.field("expression", pa.utf8()),
        pa.field("type", pa.utf8()),
        pa.field("columns", pa.utf8()),
        pa.field("description", pa.utf8()),
    ]
)

METRICS_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("metric_type", pa.utf8()),
        pa.field("expression", pa.utf8()),
        pa.field("measure", pa.utf8()),
        pa.field("description", pa.utf8()),
    ]
)

VIRTUAL_TABLES: dict[str, pa.Schema] = {
    "_dimensions": DIMENSIONS_SCHEMA,
    "_measures": MEASURES_SCHEMA,
    "_metrics": METRICS_SCHEMA,
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
            arrow_type = _obml_type_to_arrow(getattr(abstract_type, "value", str(abstract_type)))
            label = getattr(col, "label", col_name) or col_name
            fields.append(pa.field(label, arrow_type))
    return pa.schema(fields)


def model_virtual_table_name(model: Any) -> str:
    """Stable virtual-table name for a model.

    Per ``design/PLAN_flight_natural_sql.md`` §3.1, every model is exposed
    as exactly one virtual table. The server side stamps ``_ob_model_id``
    on the model when it pulls it from the SessionManager — that's the
    source of truth. Falls back to ``model.label`` / ``model.name`` for
    tests that hand-build a model without the session-id stamp, and finally
    to ``"sales_model"``.
    """
    # Check ``__dict__`` directly so MagicMock auto-attrs don't masquerade as
    # a real stamp. Only Pydantic-side-channeled values survive this check.
    if hasattr(model, "__dict__"):
        stamped = model.__dict__.get("_ob_model_id")
        if isinstance(stamped, str) and stamped:
            return stamped
    for attr in ("label", "name"):
        v = getattr(model, attr, None)
        if isinstance(v, str) and v:
            return v
    return "sales_model"


def model_to_virtual_table_schema(model: Any) -> pa.Schema:
    """Build the virtual-table Arrow schema for a model.

    Columns are the union of dimensions + measures + metrics, typed by each
    artefact's ``result_type``. This is the schema BI tools see when they
    pick from the catalog tree.
    """
    fields: list[pa.Field] = []
    if hasattr(model, "dimensions") and model.dimensions:
        for label, dim in model.dimensions.items():
            display = getattr(dim, "label", label) or label
            rt = getattr(dim, "result_type", None)
            rt_name = getattr(rt, "value", None) or "string"
            fields.append(pa.field(display, _obml_type_to_arrow(rt_name)))
    if hasattr(model, "measures") and model.measures:
        for label, meas in model.measures.items():
            display = getattr(meas, "label", label) or label
            rt = getattr(meas, "result_type", None)
            rt_name = getattr(rt, "value", None) or "float"
            fields.append(pa.field(display, _obml_type_to_arrow(rt_name)))
    if hasattr(model, "metrics") and model.metrics:
        for label, met in model.metrics.items():
            display = getattr(met, "label", label) or label
            # Metrics have no result_type; default to float (matches OBML default
            # for ratio/derived metrics).
            fields.append(pa.field(display, pa.float64()))
    return pa.schema(fields)


# ---------------------------------------------------------------------------
# Virtual metadata table data builders
# ---------------------------------------------------------------------------


def build_dimensions_data(model: Any) -> pa.Table:
    """Build a queryable table of all dimensions in the semantic model."""
    names: list[str] = []
    data_objects: list[str] = []
    columns: list[str] = []
    types: list[str] = []
    time_grains: list[str | None] = []
    descriptions: list[str | None] = []

    if hasattr(model, "dimensions") and model.dimensions:
        for dim_name, dim in model.dimensions.items():
            names.append(getattr(dim, "label", dim_name) or dim_name)
            data_objects.append(getattr(dim, "view", "") or "")
            columns.append(getattr(dim, "column", "") or "")
            rt = getattr(dim, "result_type", None)
            types.append(rt.value if hasattr(rt, "value") else str(rt or "string"))
            tg = getattr(dim, "time_grain", None)
            time_grains.append(tg.value if hasattr(tg, "value") else None)
            descriptions.append(getattr(dim, "description", None))

    return pa.table(
        {
            "name": names,
            "data_object": data_objects,
            "column": columns,
            "type": types,
            "time_grain": time_grains,
            "description": descriptions,
        },
        schema=DIMENSIONS_SCHEMA,
    )


def build_measures_data(model: Any) -> pa.Table:
    """Build a queryable table of all measures in the semantic model."""
    names: list[str] = []
    aggregations: list[str] = []
    expressions: list[str | None] = []
    types: list[str] = []
    columns_list: list[str] = []
    descriptions: list[str | None] = []

    if hasattr(model, "measures") and model.measures:
        for meas_name, meas in model.measures.items():
            names.append(getattr(meas, "label", meas_name) or meas_name)
            aggregations.append(getattr(meas, "aggregation", "") or "")
            expressions.append(getattr(meas, "expression", None))
            rt = getattr(meas, "result_type", None)
            types.append(rt.value if hasattr(rt, "value") else str(rt or "float"))
            cols = getattr(meas, "columns", []) or []
            col_strs = []
            for c in cols:
                v = getattr(c, "view", "") or ""
                col = getattr(c, "column", "") or ""
                col_strs.append(f"{v}.{col}" if v else col)
            columns_list.append(", ".join(col_strs))
            descriptions.append(getattr(meas, "description", None))

    return pa.table(
        {
            "name": names,
            "aggregation": aggregations,
            "expression": expressions,
            "type": types,
            "columns": columns_list,
            "description": descriptions,
        },
        schema=MEASURES_SCHEMA,
    )


def build_metrics_data(model: Any) -> pa.Table:
    """Build a queryable table of all metrics in the semantic model."""
    names: list[str] = []
    metric_types: list[str] = []
    expressions: list[str | None] = []
    measures: list[str | None] = []
    descriptions: list[str | None] = []

    if hasattr(model, "metrics") and model.metrics:
        for met_name, met in model.metrics.items():
            names.append(getattr(met, "label", met_name) or met_name)
            mt = getattr(met, "type", None)
            metric_types.append(mt.value if hasattr(mt, "value") else str(mt or "derived"))
            expressions.append(getattr(met, "expression", None))
            measures.append(getattr(met, "measure", None))
            descriptions.append(getattr(met, "description", None))

    return pa.table(
        {
            "name": names,
            "metric_type": metric_types,
            "expression": expressions,
            "measure": measures,
            "description": descriptions,
        },
        schema=METRICS_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Flight info builders
# ---------------------------------------------------------------------------


def model_to_flight_infos(
    model: Any,
    model_id: str,
    *,
    expose_data_objects: bool = False,
) -> list[flight.FlightInfo]:
    """Convert a SemanticModel to a list of FlightInfo entries.

    By default (``expose_data_objects=False``) only the semantic virtual
    table and ``_dimensions / _measures / _metrics`` virtual tables are
    listed — see ``design/PLAN_flight_natural_sql.md`` §3.5. Set
    ``expose_data_objects=True`` to also list each data object (used when
    ``flight_allow_data_object_sql`` is on, for raw column passthrough).
    """
    infos: list[flight.FlightInfo] = []
    if not hasattr(model, "data_objects") or not model.data_objects:
        return infos

    # Always-on: the semantic virtual table (the canonical query surface).
    vt_name = model_virtual_table_name(model)
    vt_schema = model_to_virtual_table_schema(model)
    if len(vt_schema) > 0:
        descriptor = flight.FlightDescriptor.for_path(model_id, vt_name)
        infos.append(flight.FlightInfo(vt_schema, descriptor, [], -1, -1))

    # Opt-in: data-object pass-through tables.
    if expose_data_objects:
        for obj_name, obj in model.data_objects.items():
            schema = object_to_schema(obj)
            descriptor = flight.FlightDescriptor.for_path(model_id, obj_name)
            info = flight.FlightInfo(schema, descriptor, [], -1, -1)
            infos.append(info)

    # Virtual metadata tables — always present.
    for meta_name, meta_schema in VIRTUAL_TABLES.items():
        descriptor = flight.FlightDescriptor.for_path(model_id, meta_name)
        info = flight.FlightInfo(meta_schema, descriptor, [], -1, -1)
        infos.append(info)
    return infos
