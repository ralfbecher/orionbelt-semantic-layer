"""Tests for Flight catalog (model -> FlightInfo conversion)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from ob_flight.catalog import model_to_flight_infos, object_to_schema


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


class TestModelToFlightInfos:
    def test_basic(self):
        col = MagicMock()
        col.label = "ID"
        col.abstract_type = "int"

        obj = MagicMock()
        obj.columns = {"ID": col}

        model = MagicMock()
        model.data_objects = {"Orders": obj}

        infos = model_to_flight_infos(model, "test-model")
        assert len(infos) == 1
        info = infos[0]
        assert info.descriptor.path == [b"test-model", b"Orders"]

    def test_multiple_objects(self):
        col = MagicMock()
        col.label = "X"
        col.abstract_type = "string"

        obj1 = MagicMock()
        obj1.columns = {"X": col}
        obj2 = MagicMock()
        obj2.columns = {"X": col}

        model = MagicMock()
        model.data_objects = {"A": obj1, "B": obj2}

        infos = model_to_flight_infos(model, "m1")
        assert len(infos) == 2

    def test_no_data_objects(self):
        model = MagicMock()
        model.data_objects = {}
        infos = model_to_flight_infos(model, "m1")
        assert len(infos) == 0

    def test_no_data_objects_attr(self):
        model = MagicMock(spec=[])
        infos = model_to_flight_infos(model, "m1")
        assert len(infos) == 0

    def test_schema_preserved_in_flight_info(self):
        col1 = MagicMock()
        col1.label = "Name"
        col1.abstract_type = "string"
        col2 = MagicMock()
        col2.label = "Amount"
        col2.abstract_type = "float"

        obj = MagicMock()
        obj.columns = {"Name": col1, "Amount": col2}

        model = MagicMock()
        model.data_objects = {"Sales": obj}

        infos = model_to_flight_infos(model, "m1")
        schema = infos[0].schema
        assert len(schema) == 2
        assert schema.field(0).name == "Name"
        assert schema.field(1).name == "Amount"
