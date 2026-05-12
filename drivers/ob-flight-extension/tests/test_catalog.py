"""Tests for Flight catalog (model -> FlightInfo conversion)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from ob_flight.catalog import (
    build_dimensions_data,
    build_measures_data,
    build_metrics_data,
    model_to_flight_infos,
    object_to_schema,
)


class TestObjectToSchema:
    def test_basic_columns(self):
        col1 = MagicMock()
        col1.label = "Region"
        col1.abstract_type = "string"
        col2 = MagicMock()
        col2.label = "Amount"
        col2.abstract_type = "float"

        obj = MagicMock()
        obj.columns = {"Region": col1, "Amount": col2}

        schema = object_to_schema(obj)
        assert len(schema) == 2
        assert schema.field(0).name == "Region"
        assert schema.field(0).type == pa.utf8()
        assert schema.field(1).name == "Amount"
        assert schema.field(1).type == pa.float64()

    def test_int_type(self):
        col = MagicMock()
        col.label = "Count"
        col.abstract_type = "int"
        obj = MagicMock()
        obj.columns = {"Count": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.int64()

    def test_datetime_type(self):
        col = MagicMock()
        col.label = "Created"
        col.abstract_type = "datetime"
        obj = MagicMock()
        obj.columns = {"Created": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.timestamp("us")

    def test_unknown_type_fallback(self):
        col = MagicMock()
        col.label = "Data"
        col.abstract_type = "custom_type"
        obj = MagicMock()
        obj.columns = {"Data": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.utf8()

    def test_no_columns(self):
        obj = MagicMock()
        obj.columns = {}
        schema = object_to_schema(obj)
        assert len(schema) == 0

    def test_no_columns_attr(self):
        obj = MagicMock(spec=[])  # no attributes
        schema = object_to_schema(obj)
        assert len(schema) == 0

    def test_none_abstract_type_defaults_to_string(self):
        col = MagicMock()
        col.label = "Name"
        col.abstract_type = None
        obj = MagicMock()
        obj.columns = {"Name": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.utf8()

    def test_boolean_type(self):
        col = MagicMock()
        col.label = "Active"
        col.abstract_type = "boolean"
        obj = MagicMock()
        obj.columns = {"Active": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.bool_()

    def test_date_type(self):
        col = MagicMock()
        col.label = "OrderDate"
        col.abstract_type = "date"
        obj = MagicMock()
        obj.columns = {"OrderDate": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.date32()

    def test_timestamp_type(self):
        col = MagicMock()
        col.label = "Modified"
        col.abstract_type = "timestamp"
        obj = MagicMock()
        obj.columns = {"Modified": col}
        schema = object_to_schema(obj)
        assert schema.field(0).type == pa.timestamp("us")

    def test_label_fallback_to_col_name(self):
        col = MagicMock()
        col.label = None
        col.abstract_type = "string"
        obj = MagicMock()
        obj.columns = {"my_column": col}
        schema = object_to_schema(obj)
        assert schema.field(0).name == "my_column"


def _make_model_with_dim(name: str = "sales_model") -> MagicMock:
    """Build a model mock with one dim + one measure → produces a non-empty
    virtual table schema. See PLAN_flight_natural_sql.md §3.5."""
    dim = MagicMock()
    dim.label = "Region"
    dim.result_type = MagicMock(value="string")
    dim.time_grain = None
    dim.description = None
    dim.column = "region"
    dim.view = "Sales"

    meas = MagicMock()
    meas.label = "Total Sales"
    meas.aggregation = "sum"
    meas.expression = None
    meas.result_type = MagicMock(value="float")
    meas.columns = []
    meas.description = None

    col = MagicMock()
    col.label = "X"
    col.abstract_type = MagicMock(value="string")
    obj = MagicMock()
    obj.columns = {"X": col}

    model = MagicMock()
    model.label = name
    model.id = name
    model.name = name
    model.data_objects = {"Sales": obj}
    model.dimensions = {"Region": dim}
    model.measures = {"Total Sales": meas}
    model.metrics = {}
    return model


class TestModelToFlightInfos:
    def test_default_hides_data_objects(self):
        """Default: virtual table + _dimensions/_measures/_metrics = 4."""
        model = _make_model_with_dim()
        infos = model_to_flight_infos(model, "test-model")
        assert len(infos) == 4
        # First info is the semantic virtual table
        assert infos[0].descriptor.path == [b"test-model", b"sales_model"]

    def test_expose_data_objects(self):
        """expose_data_objects=True: virtual table + each data object + 3 meta."""
        model = _make_model_with_dim()
        infos = model_to_flight_infos(model, "test-model", expose_data_objects=True)
        assert len(infos) == 5  # vt + Sales + 3 meta
        labels = {info.descriptor.path[-1] for info in infos}
        assert b"sales_model" in labels
        assert b"Sales" in labels

    def test_no_data_objects(self):
        model = MagicMock()
        model.data_objects = {}
        infos = model_to_flight_infos(model, "m1")
        assert len(infos) == 0

    def test_no_data_objects_attr(self):
        model = MagicMock(spec=[])
        infos = model_to_flight_infos(model, "m1")
        assert len(infos) == 0

    def test_virtual_tables_included(self):
        model = _make_model_with_dim()
        infos = model_to_flight_infos(model, "m1")
        vt_paths = {info.descriptor.path[-1] for info in infos}
        assert b"_dimensions" in vt_paths
        assert b"_measures" in vt_paths
        assert b"_metrics" in vt_paths

    def test_virtual_table_schema_has_dims_and_measures(self):
        """The semantic virtual table exposes dims + measures + metrics."""
        from ob_flight.catalog import model_to_virtual_table_schema

        model = _make_model_with_dim()
        schema = model_to_virtual_table_schema(model)
        names = [f.name for f in schema]
        assert "Region" in names
        assert "Total Sales" in names


class TestBuildDimensionsData:
    def test_basic(self):
        dim = MagicMock()
        dim.label = "Region"
        dim.view = "Orders"
        dim.column = "region"
        dim.result_type = MagicMock(value="string")
        dim.time_grain = None
        dim.description = "Sales region"

        model = MagicMock()
        model.dimensions = {"Region": dim}

        table = build_dimensions_data(model)
        assert len(table) == 1
        assert table.column("name")[0].as_py() == "Region"
        assert table.column("data_object")[0].as_py() == "Orders"
        assert table.column("column")[0].as_py() == "region"
        assert table.column("type")[0].as_py() == "string"
        assert table.column("description")[0].as_py() == "Sales region"

    def test_with_time_grain(self):
        dim = MagicMock()
        dim.label = "Order Date"
        dim.view = "Orders"
        dim.column = "order_date"
        dim.result_type = MagicMock(value="date")
        dim.time_grain = MagicMock(value="month")
        dim.description = None

        model = MagicMock()
        model.dimensions = {"Order Date": dim}

        table = build_dimensions_data(model)
        assert table.column("time_grain")[0].as_py() == "month"

    def test_empty_model(self):
        model = MagicMock()
        model.dimensions = {}
        table = build_dimensions_data(model)
        assert len(table) == 0

    def test_no_dimensions_attr(self):
        model = MagicMock(spec=[])
        table = build_dimensions_data(model)
        assert len(table) == 0


class TestBuildMeasuresData:
    def test_basic(self):
        col_ref = MagicMock()
        col_ref.view = "Orders"
        col_ref.column = "amount"

        meas = MagicMock()
        meas.label = "Total Sales"
        meas.aggregation = "sum"
        meas.expression = None
        meas.result_type = MagicMock(value="float")
        meas.columns = [col_ref]
        meas.description = "Sum of sales"

        model = MagicMock()
        model.measures = {"Total Sales": meas}

        table = build_measures_data(model)
        assert len(table) == 1
        assert table.column("name")[0].as_py() == "Total Sales"
        assert table.column("aggregation")[0].as_py() == "sum"
        assert table.column("columns")[0].as_py() == "Orders.amount"
        assert table.column("description")[0].as_py() == "Sum of sales"

    def test_empty_model(self):
        model = MagicMock()
        model.measures = {}
        table = build_measures_data(model)
        assert len(table) == 0


class TestBuildMetricsData:
    def test_basic(self):
        met = MagicMock()
        met.label = "Return Rate"
        met.type = MagicMock(value="derived")
        met.expression = "{[Total Returns]} / {[Total Sales]}"
        met.measure = None
        met.description = "Rate of returns"

        model = MagicMock()
        model.metrics = {"Return Rate": met}

        table = build_metrics_data(model)
        assert len(table) == 1
        assert table.column("name")[0].as_py() == "Return Rate"
        assert table.column("metric_type")[0].as_py() == "derived"
        assert table.column("expression")[0].as_py() == "{[Total Returns]} / {[Total Sales]}"

    def test_empty_model(self):
        model = MagicMock()
        model.metrics = {}
        table = build_metrics_data(model)
        assert len(table) == 0
