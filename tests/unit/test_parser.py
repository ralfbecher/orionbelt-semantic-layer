"""Tests for YAML parser, resolver, and validator."""

from __future__ import annotations

from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver
from orionbelt.parser.validator import SemanticValidator
from tests.conftest import SALES_MODEL_DIR, SAMPLE_MODEL_YAML


class TestTrackedLoader:
    def test_load_string(self, loader: TrackedLoader) -> None:
        raw, source_map = loader.load_string(SAMPLE_MODEL_YAML)
        assert "dataObjects" in raw
        assert "dimensions" in raw
        assert "measures" in raw
        assert raw["version"] == 1.0

    def test_load_string_empty(self, loader: TrackedLoader) -> None:
        raw, source_map = loader.load_string("")
        assert raw == {}

    def test_source_map_has_positions(self, loader: TrackedLoader) -> None:
        raw, source_map = loader.load_string(SAMPLE_MODEL_YAML)
        # Should have position info for dataObjects, dimensions, measures
        assert len(source_map.paths) > 0

    def test_load_model_file(self, loader: TrackedLoader) -> None:
        raw, source_map = loader.load(SALES_MODEL_DIR / "model.yaml")
        assert "dataObjects" in raw
        assert "Orders" in raw["dataObjects"]
        assert "Customers" in raw["dataObjects"]

    def test_data_objects_have_columns(self, loader: TrackedLoader) -> None:
        raw, _ = loader.load_string(SAMPLE_MODEL_YAML)
        orders = raw["dataObjects"]["Orders"]
        assert "Order ID" in orders["columns"]
        assert orders["columns"]["Amount"]["abstractType"] == "float"


class TestReferenceResolver:
    def test_resolve_valid_model(self, resolver: ReferenceResolver) -> None:
        loader = TrackedLoader()
        raw, source_map = loader.load_string(SAMPLE_MODEL_YAML)
        model, result = resolver.resolve(raw, source_map)
        assert result.valid
        assert len(model.data_objects) == 2
        assert len(model.dimensions) == 1
        assert len(model.measures) == 3

    def test_resolve_dimension_references(self, resolver: ReferenceResolver) -> None:
        loader = TrackedLoader()
        raw, source_map = loader.load_string(SAMPLE_MODEL_YAML)
        model, result = resolver.resolve(raw, source_map)
        dim = model.dimensions["Customer Country"]
        assert dim.view == "Customers"
        assert dim.column == "Country"

    def test_unknown_data_object_error(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  Orders:
    code: ORDERS
    database: DB
    schema: SCH
    columns:
      ID:
        code: ID
        abstractType: string
dimensions:
  Bad Dim:
    dataObject: NonExistent
    column: Foo
    resultType: string
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        assert not result.valid
        assert any(e.code == "UNKNOWN_DATA_OBJECT" for e in result.errors)

    def test_unknown_column_error(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  Orders:
    code: ORDERS
    database: DB
    schema: SCH
    columns:
      ID:
        code: ID
        abstractType: string
dimensions:
  Bad Dim:
    dataObject: Orders
    column: NonExistent
    resultType: string
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        assert not result.valid
        assert any(e.code == "UNKNOWN_COLUMN" for e in result.errors)

    def test_resolve_sales_model(self) -> None:
        loader = TrackedLoader()
        resolver = ReferenceResolver()
        raw, source_map = loader.load(SALES_MODEL_DIR / "model.yaml")
        model, result = resolver.resolve(raw, source_map)
        assert result.valid, f"Errors: {[e.message for e in result.errors]}"
        assert "Orders" in model.data_objects
        assert "Revenue" in model.measures
        assert "Customer Country" in model.dimensions

    def test_resolve_dimension_data_object(self) -> None:
        loader = TrackedLoader()
        resolver = ReferenceResolver()
        raw, source_map = loader.load(SALES_MODEL_DIR / "model.yaml")
        model, result = resolver.resolve(raw, source_map)
        # Product Category uses dataObject + field
        assert "Product Category" in model.dimensions
        dim = model.dimensions["Product Category"]
        assert dim.view == "Products"
        assert dim.column == "Category"


class TestSemanticValidator:
    def test_valid_model(self, sales_model) -> None:
        validator = SemanticValidator()
        errors = validator.validate(sales_model)
        assert len(errors) == 0

    def test_duplicate_identifier_across_sections(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  Orders:
    code: ORDERS
    database: DB
    schema: SCH
    columns:
      id:
        code: ID
        abstractType: string
dimensions:
  Orders:
    dataObject: Orders
    column: id
    resultType: string
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        assert any(e.code == "DUPLICATE_IDENTIFIER" for e in errors)

    def test_cyclic_join_detection(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  A:
    code: A
    database: DB
    schema: SCH
    columns:
      id:
        code: ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: B
        columnsFrom: [id]
        columnsTo: [id]
  B:
    code: B
    database: DB
    schema: SCH
    columns:
      id:
        code: ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: A
        columnsFrom: [id]
        columnsTo: [id]
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        assert any(e.code == "CYCLIC_JOIN" for e in errors)

    def test_unknown_join_target(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  A:
    code: A
    database: DB
    schema: SCH
    columns:
      id:
        code: ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: NonExistent
        columnsFrom: [id]
        columnsTo: [id]
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        assert any(e.code == "UNKNOWN_JOIN_TARGET" for e in errors)

    def test_join_column_count_mismatch(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  A:
    code: A
    database: DB
    schema: SCH
    columns:
      id1:
        code: ID1
        abstractType: string
      id2:
        code: ID2
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: B
        columnsFrom: [id1, id2]
        columnsTo: [id1]
  B:
    code: B
    database: DB
    schema: SCH
    columns:
      id1:
        code: ID1
        abstractType: string
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        assert any(e.code == "JOIN_COLUMN_COUNT_MISMATCH" for e in errors)

    def test_multipath_join_detection(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  A:
    code: A
    database: DB
    schema: SCH
    columns:
      a_id:
        code: A_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: B
        columnsFrom: [a_id]
        columnsTo: [b_id]
      - joinType: many-to-one
        joinTo: D
        columnsFrom: [a_id]
        columnsTo: [d_id]
  B:
    code: B
    database: DB
    schema: SCH
    columns:
      b_id:
        code: B_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: C
        columnsFrom: [b_id]
        columnsTo: [c_id]
  C:
    code: C
    database: DB
    schema: SCH
    columns:
      c_id:
        code: C_ID
        abstractType: string
  D:
    code: D
    database: DB
    schema: SCH
    columns:
      d_id:
        code: D_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: C
        columnsFrom: [d_id]
        columnsTo: [c_id]
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        multipath_errors = [e for e in errors if e.code == "MULTIPATH_JOIN"]
        assert len(multipath_errors) == 1
        assert "A" in multipath_errors[0].message
        assert "C" in multipath_errors[0].message

    def test_no_multipath_in_tree(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  A:
    code: A
    database: DB
    schema: SCH
    columns:
      a_id:
        code: A_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: B
        columnsFrom: [a_id]
        columnsTo: [b_id]
      - joinType: many-to-one
        joinTo: C
        columnsFrom: [a_id]
        columnsTo: [c_id]
  B:
    code: B
    database: DB
    schema: SCH
    columns:
      b_id:
        code: B_ID
        abstractType: string
  C:
    code: C
    database: DB
    schema: SCH
    columns:
      c_id:
        code: C_ID
        abstractType: string
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        assert not any(e.code == "MULTIPATH_JOIN" for e in errors)

    def test_multipath_longer_paths(self, resolver: ReferenceResolver) -> None:
        yaml_content = """\
version: 1.0
dataObjects:
  A:
    code: A
    database: DB
    schema: SCH
    columns:
      a_id:
        code: A_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: B
        columnsFrom: [a_id]
        columnsTo: [b_id]
      - joinType: many-to-one
        joinTo: E
        columnsFrom: [a_id]
        columnsTo: [e_id]
  B:
    code: B
    database: DB
    schema: SCH
    columns:
      b_id:
        code: B_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: C
        columnsFrom: [b_id]
        columnsTo: [c_id]
  C:
    code: C
    database: DB
    schema: SCH
    columns:
      c_id:
        code: C_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: D
        columnsFrom: [c_id]
        columnsTo: [d_id]
  D:
    code: D
    database: DB
    schema: SCH
    columns:
      d_id:
        code: D_ID
        abstractType: string
  E:
    code: E
    database: DB
    schema: SCH
    columns:
      e_id:
        code: E_ID
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: D
        columnsFrom: [e_id]
        columnsTo: [d_id]
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        multipath_errors = [e for e in errors if e.code == "MULTIPATH_JOIN"]
        assert len(multipath_errors) == 1
        assert "A" in multipath_errors[0].message
        assert "D" in multipath_errors[0].message

    def test_no_multipath_direct_plus_indirect(self, resolver: ReferenceResolver) -> None:
        """Direct join + indirect path is valid snowflake — not ambiguous."""
        yaml_content = """\
version: 1.0
dataObjects:
  Purchases:
    code: purchases
    database: DB
    schema: SCH
    columns:
      purchase_id:
        code: purchase_id
        abstractType: string
      purchase_product:
        code: purchase_product
        abstractType: string
      purchase_supplier:
        code: purchase_supplier
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: Products
        columnsFrom: [purchase_product]
        columnsTo: [product_id]
      - joinType: many-to-one
        joinTo: Suppliers
        columnsFrom: [purchase_supplier]
        columnsTo: [supplier_id]
  Products:
    code: products
    database: DB
    schema: SCH
    columns:
      product_id:
        code: product_id
        abstractType: string
      product_supplier:
        code: product_supplier
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: Suppliers
        columnsFrom: [product_supplier]
        columnsTo: [supplier_id]
  Suppliers:
    code: suppliers
    database: DB
    schema: SCH
    columns:
      supplier_id:
        code: supplier_id
        abstractType: string
"""
        loader = TrackedLoader()
        raw, source_map = loader.load_string(yaml_content)
        model, result = resolver.resolve(raw, source_map)
        validator = SemanticValidator()
        errors = validator.validate(model)
        assert not any(e.code == "MULTIPATH_JOIN" for e in errors)
