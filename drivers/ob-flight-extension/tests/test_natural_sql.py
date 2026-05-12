"""End-to-end smoke tests for the Flight natural-SQL surface.

Tests use the real OrionBelt parser/compiler/translator but mock the
SessionManager so no warehouse round-trip happens. Covers:

* virtual table listed in catalog
* CMD_GET_COLUMNS returns dim/measure/metric labels
* Semantic QL → translated → compiled (no DB)
* raw SQL rejected when flag off
* GROUP BY silently ignored in semantic mode
* measure reference in WHERE routed to HAVING

Spec: design/PLAN_flight_natural_sql.md.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow.flight as flight
import pytest

from ob_flight.catalog import (
    model_to_flight_infos,
    model_to_virtual_table_schema,
    model_virtual_table_name,
)
from ob_flight.flight_sql import build_columns_table, build_tables_table
from ob_flight.server import OBFlightServer
from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver

# Inline a small OBML model — independent of the main project's fixtures.
_MODEL_YAML = """\
version: 1.0
dataObjects:
  Customers:
    code: CUSTOMERS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Customer ID:
        code: CUSTOMER_ID
        abstractType: string
      Country:
        code: COUNTRY
        abstractType: string
  Orders:
    code: ORDERS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Order ID:
        code: ORDER_ID
        abstractType: string
      Order Customer ID:
        code: CUSTOMER_ID
        abstractType: string
      Amount:
        code: AMOUNT
        abstractType: float
        numClass: additive
    joins:
      - joinType: many-to-one
        joinTo: Customers
        columnsFrom: [Order Customer ID]
        columnsTo: [Customer ID]
dimensions:
  Customer Country:
    dataObject: Customers
    column: Country
    resultType: string
measures:
  Total Revenue:
    columns:
      - dataObject: Orders
        column: Amount
    resultType: float
    aggregation: sum
"""


@pytest.fixture
def model():
    loader = TrackedLoader()
    raw, source_map = loader.load_string(_MODEL_YAML)
    resolver = ReferenceResolver()
    m, result = resolver.resolve(raw, source_map)
    assert result.valid
    # The server stamps _ob_model_id on the model after pulling it from
    # the SessionManager — mirror that here so the virtual-table name
    # resolver returns the BI-friendly default the tests expect.
    m.__dict__["_ob_model_id"] = "sample_model"
    return m


def _make_server(model: object, **kwargs: object) -> OBFlightServer:
    """Build a server with a SessionManager mock that returns ``model``.

    The mock returns ``model_id="sample_model"`` so the server's
    ``_get_model`` stamps the same virtual-table id the fixture uses.
    """
    store = MagicMock()
    store.list_models.return_value = [MagicMock(model_id="sample_model")]
    store.get_model.return_value = model
    mgr = MagicMock()
    mgr.get_store.return_value = store

    # Avoid binding a real gRPC socket — the tests only exercise the
    # translation/classification methods.
    server = OBFlightServer.__new__(OBFlightServer)
    server._session_manager = mgr
    server._default_dialect = "duckdb"
    server._batch_size = 1024
    server._allow_raw_sql = bool(kwargs.get("allow_raw_sql", False))
    server._allow_data_object_sql = bool(kwargs.get("allow_data_object_sql", False))
    import threading

    server._lock = threading.Lock()
    server._pending = {}
    server._prepared = {}
    server._pending_ttl = 300
    return server


# --- catalog ------------------------------------------------------------------


class TestVirtualTableInCatalog:
    def test_virtual_table_is_first_entry(self, model) -> None:
        infos = model_to_flight_infos(model, "default")
        assert infos[0].descriptor.path[-1] == b"sample_model"

    def test_data_objects_hidden_by_default(self, model) -> None:
        infos = model_to_flight_infos(model, "default")
        labels = {info.descriptor.path[-1] for info in infos}
        assert b"Customers" not in labels
        assert b"Orders" not in labels

    def test_data_objects_shown_when_opted_in(self, model) -> None:
        infos = model_to_flight_infos(model, "default", expose_data_objects=True)
        labels = {info.descriptor.path[-1] for info in infos}
        assert b"Customers" in labels
        assert b"Orders" in labels

    def test_virtual_table_schema_lists_dims_and_measures(self, model) -> None:
        schema = model_to_virtual_table_schema(model)
        names = [f.name for f in schema]
        assert "Customer Country" in names
        assert "Total Revenue" in names


class TestBuildTablesTable:
    def test_lists_virtual_table_first(self, model) -> None:
        t = build_tables_table(model)
        table_names = t.column("table_name").to_pylist()
        assert table_names[0] == "sample_model"
        # Default: data objects are hidden, only virtual metadata follows.
        assert "Customers" not in table_names
        assert "Orders" not in table_names

    def test_metadata_views_have_view_type(self, model) -> None:
        t = build_tables_table(model)
        rows = list(
            zip(
                t.column("table_name").to_pylist(),
                t.column("table_type").to_pylist(),
                strict=True,
            )
        )
        for name, type_ in rows:
            if name in {"_dimensions", "_measures", "_metrics"}:
                assert type_ == "VIEW"


class TestBuildColumnsTable:
    def test_includes_dim_and_measure(self, model) -> None:
        t = build_columns_table(model)
        cols = t.column("column_name").to_pylist()
        assert "Customer Country" in cols
        assert "Total Revenue" in cols

    def test_data_object_columns_hidden_by_default(self, model) -> None:
        t = build_columns_table(model)
        tables = set(t.column("table_name").to_pylist())
        assert "Customers" not in tables
        assert "Orders" not in tables

    def test_data_object_columns_opt_in(self, model) -> None:
        t = build_columns_table(model, expose_data_objects=True)
        tables = set(t.column("table_name").to_pylist())
        assert "Customers" in tables


# --- translator + governance --------------------------------------------------


class TestClassifySQL:
    def test_virtual_table_is_semantic(self, model) -> None:
        server = _make_server(model)
        mode = server._classify_sql(
            'SELECT "Customer Country", "Total Revenue" FROM sample_model', model
        )
        assert mode == "semantic"

    def test_data_object_target(self, model) -> None:
        server = _make_server(model)
        mode = server._classify_sql('SELECT * FROM "Customers"', model)
        assert mode == "data_object"

    def test_raw_target(self, model) -> None:
        server = _make_server(model)
        mode = server._classify_sql("SELECT 1 FROM other_thing", model)
        assert mode == "raw"


class TestPrepareSQL:
    def test_semantic_compiles(self, model) -> None:
        server = _make_server(model)
        sql, dialect, _m, schema = server._prepare_sql(
            'SELECT "Customer Country", "Total Revenue" FROM sample_model'
        )
        assert "SELECT" in sql.upper()
        assert dialect == "duckdb"
        assert schema is not None
        names = [f.name for f in schema]
        assert "Customer Country" in names
        assert "Total Revenue" in names

    def test_semantic_group_by_ignored(self, model) -> None:
        server = _make_server(model)
        sql, *_ = server._prepare_sql(
            'SELECT "Customer Country", "Total Revenue" FROM sample_model '
            'GROUP BY "Customer Country"'
        )
        # The translator silently drops the explicit GROUP BY and the
        # planner re-injects it from the SELECT dims.
        assert "GROUP BY" in sql.upper()

    def test_semantic_measure_in_where_routes_to_having(self, model) -> None:
        server = _make_server(model)
        sql, *_ = server._prepare_sql(
            'SELECT "Customer Country", "Total Revenue" FROM sample_model '
            'WHERE "Total Revenue" > 1000'
        )
        assert "HAVING" in sql.upper()

    def test_raw_sql_rejected_by_default(self, model) -> None:
        server = _make_server(model, allow_raw_sql=False)
        with pytest.raises(flight.FlightServerError, match="RAW_SQL_DISABLED"):
            server._prepare_sql("SELECT * FROM information_schema.tables")

    def test_raw_sql_allowed_when_flag_on(self, model) -> None:
        server = _make_server(model, allow_raw_sql=True)
        sql, _d, _m, schema = server._prepare_sql("SELECT 1 FROM information_schema.tables")
        assert "SELECT" in sql.upper()
        # No schema hint for raw pass-through — caller falls back to DB probe.
        assert schema is None

    def test_data_object_rejected_by_default(self, model) -> None:
        server = _make_server(model, allow_data_object_sql=False)
        with pytest.raises(flight.FlightServerError, match="DATA_OBJECT_SQL_DISABLED"):
            server._prepare_sql('SELECT * FROM "Customers"')

    def test_data_object_allowed_when_flag_on(self, model) -> None:
        server = _make_server(model, allow_data_object_sql=True)
        sql, *_ = server._prepare_sql('SELECT * FROM "Customers"')
        # Rewritten to the physical code
        assert "CUSTOMERS" in sql.upper()

    def test_translator_error_surfaces_as_flight_error(self, model) -> None:
        server = _make_server(model)
        with pytest.raises(
            flight.FlightServerError, match="OrionBelt Semantic QL translation failed"
        ):
            server._prepare_sql('SELECT "Bogus" FROM sample_model')


class TestSchemaProbeShortcut:
    def test_semantic_query_returns_arrow_schema_without_db(self, model) -> None:
        server = _make_server(model)
        _sql, _d, _m, schema = server._prepare_sql(
            'SELECT "Customer Country", "Total Revenue" FROM sample_model '
            "WHERE \"Customer Country\" = 'US'"
        )
        # Result schema should be inferred from the model — no DB needed.
        assert schema is not None
        assert schema.field(0).name == "Customer Country"
        assert schema.field(1).name == "Total Revenue"

    def test_rollup_adds_grouping_flag_columns(self, model) -> None:
        server = _make_server(model)
        _sql, _d, _m, schema = server._prepare_sql(
            'SELECT "Customer Country", "Total Revenue" FROM sample_model WITH ROLLUP'
        )
        assert schema is not None
        names = [f.name for f in schema]
        assert "_g_Customer Country" in names
