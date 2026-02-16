"""Tests for the compilation pipeline."""

from __future__ import annotations

import pytest

from orionbelt.ast.nodes import BinaryOp, ColumnRef, Literal, RelativeDateRange
from orionbelt.compiler.pipeline import CompilationPipeline
from orionbelt.compiler.resolution import (
    QueryResolver,
    ResolutionError,
    _parse_metric_formula,
    _tokenize_formula,
)
from orionbelt.compiler.star import StarSchemaPlanner
from orionbelt.models.query import FilterOperator, QueryFilter, QueryObject, QuerySelect
from orionbelt.models.semantic import SemanticModel
from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver
from tests.conftest import SAMPLE_MODEL_YAML


def _load_model(yaml_content: str = SAMPLE_MODEL_YAML) -> SemanticModel:
    loader = TrackedLoader()
    resolver = ReferenceResolver()
    raw, source_map = loader.load_string(yaml_content)
    model, result = resolver.resolve(raw, source_map)
    assert result.valid, f"Model errors: {[e.message for e in result.errors]}"
    return model


class TestQueryResolver:
    def test_resolve_simple_query(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert len(resolved.dimensions) == 1
        assert resolved.dimensions[0].name == "Customer Country"
        assert resolved.dimensions[0].object_name == "Customers"
        assert len(resolved.measures) == 1
        assert resolved.measures[0].name == "Total Revenue"

    def test_resolve_unknown_dimension_error(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["NonExistent"],
                measures=["Total Revenue"],
            ),
        )
        with pytest.raises(ResolutionError) as exc_info:
            resolver.resolve(query, model)
        assert any("NonExistent" in e.message for e in exc_info.value.errors)

    def test_resolve_unknown_measure_error(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["NonExistent"],
            ),
        )
        with pytest.raises(ResolutionError) as exc_info:
            resolver.resolve(query, model)
        assert any("NonExistent" in e.message for e in exc_info.value.errors)

    def test_resolve_base_object_selection(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        # Orders has joins, so should be base object
        assert resolved.base_object == "Orders"

    def test_resolve_relative_filter(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
            where=[
                QueryFilter(
                    field="Customer Country",
                    op=FilterOperator.RELATIVE,
                    value={"unit": "day", "count": 7, "direction": "past"},
                )
            ],
        )
        resolved = resolver.resolve(query, model)
        assert len(resolved.where_filters) == 1
        assert isinstance(resolved.where_filters[0].expression, RelativeDateRange)

    def test_resolve_with_limit(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
            limit=50,
        )
        resolved = resolver.resolve(query, model)
        assert resolved.limit == 50


class TestStarSchemaPlanner:
    def test_plan_simple_query(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        planner = StarSchemaPlanner()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        plan = planner.plan(resolved, model)
        assert plan.ast is not None
        assert len(plan.ast.columns) == 2  # 1 dimension + 1 measure
        assert plan.ast.from_ is not None
        assert len(plan.ast.group_by) == 1


class TestCompilationPipeline:
    def test_compile_postgres(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        result = pipeline.compile(query, model, "postgres")
        assert "SELECT" in result.sql
        assert "GROUP BY" in result.sql
        assert result.dialect == "postgres"
        assert "Customer Country" in result.resolved.dimensions
        assert "Total Revenue" in result.resolved.measures

    def test_compile_snowflake(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        result = pipeline.compile(query, model, "snowflake")
        assert "SELECT" in result.sql
        assert result.dialect == "snowflake"

    def test_compile_clickhouse(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        result = pipeline.compile(query, model, "clickhouse")
        assert "SELECT" in result.sql
        assert result.dialect == "clickhouse"

    def test_compile_with_limit(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
            limit=1000,
        )
        result = pipeline.compile(query, model, "postgres")
        assert "LIMIT 1000" in result.sql

    def test_compile_resolved_info(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        result = pipeline.compile(query, model, "postgres")
        assert "Orders" in result.resolved.fact_tables
        assert "Customer Country" in result.resolved.dimensions
        assert "Total Revenue" in result.resolved.measures


class TestFormulaParser:
    """Tests for the metric formula tokenizer and parser."""

    def test_tokenize_simple_division(self) -> None:
        tokens = _tokenize_formula("{[Revenue]} / {[Order Count]}")
        assert len(tokens) == 3
        assert tokens[0].kind == "ref" and tokens[0].value == "Revenue"
        assert tokens[1].kind == "op" and tokens[1].value == "/"
        assert tokens[2].kind == "ref" and tokens[2].value == "Order Count"

    def test_tokenize_with_numbers(self) -> None:
        tokens = _tokenize_formula("{[Revenue]} * 100")
        assert len(tokens) == 3
        assert tokens[2].kind == "number" and tokens[2].value == "100"

    def test_tokenize_with_parens(self) -> None:
        tokens = _tokenize_formula("({[A]} + {[B]}) * {[C]}")
        assert tokens[0].kind == "lparen"
        assert tokens[4].kind == "rparen"

    def test_tokenize_unclosed_ref_raises(self) -> None:
        with pytest.raises(ValueError):
            _tokenize_formula("{[Revenue} / {[Order Count]}")

    def test_parse_simple_division(self) -> None:
        tokens = _tokenize_formula("{[Revenue]} / {[Count]}")
        ast = _parse_metric_formula(tokens)
        assert isinstance(ast, BinaryOp)
        assert ast.op == "/"
        assert isinstance(ast.left, ColumnRef) and ast.left.name == "Revenue"
        assert isinstance(ast.right, ColumnRef) and ast.right.name == "Count"

    def test_parse_precedence_multiply_before_add(self) -> None:
        # a + b * c → a + (b * c)
        tokens = _tokenize_formula("{[A]} + {[B]} * {[C]}")
        ast = _parse_metric_formula(tokens)
        assert isinstance(ast, BinaryOp) and ast.op == "+"
        assert isinstance(ast.left, ColumnRef) and ast.left.name == "A"
        assert isinstance(ast.right, BinaryOp) and ast.right.op == "*"

    def test_parse_parentheses_override_precedence(self) -> None:
        # (a + b) * c
        tokens = _tokenize_formula("({[A]} + {[B]}) * {[C]}")
        ast = _parse_metric_formula(tokens)
        assert isinstance(ast, BinaryOp) and ast.op == "*"
        assert isinstance(ast.left, BinaryOp) and ast.left.op == "+"
        assert isinstance(ast.right, ColumnRef) and ast.right.name == "C"

    def test_parse_numeric_literal(self) -> None:
        tokens = _tokenize_formula("{[Revenue]} / 100")
        ast = _parse_metric_formula(tokens)
        assert isinstance(ast, BinaryOp) and ast.op == "/"
        assert isinstance(ast.right, Literal) and ast.right.value == 100

    def test_parse_float_literal(self) -> None:
        tokens = _tokenize_formula("{[Revenue]} * 1.5")
        ast = _parse_metric_formula(tokens)
        assert isinstance(ast, BinaryOp) and ast.op == "*"
        assert isinstance(ast.right, Literal) and ast.right.value == 1.5

    def test_parse_multi_word_measure_name(self) -> None:
        tokens = _tokenize_formula("{[Total Revenue]} / {[Total Order Count]}")
        ast = _parse_metric_formula(tokens)
        assert isinstance(ast, BinaryOp)
        assert isinstance(ast.left, ColumnRef) and ast.left.name == "Total Revenue"
        assert isinstance(ast.right, ColumnRef) and ast.right.name == "Total Order Count"


class TestTotalResolution:
    """Tests for total measure resolution."""

    def test_total_flag_propagated(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Grand Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert len(resolved.measures) == 1
        assert resolved.measures[0].total is True

    def test_non_total_flag_default(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert resolved.measures[0].total is False

    def test_has_totals_true(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Grand Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert resolved.has_totals is True

    def test_has_totals_false(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert resolved.has_totals is False

    def test_has_totals_via_metric_component(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Revenue Share"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert resolved.has_totals is True


class TestMetricResolution:
    """Tests for metric resolution via QueryResolver."""

    def test_resolve_metric_produces_ast(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Revenue per Order"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert len(resolved.measures) == 1
        metric = resolved.measures[0]
        assert metric.name == "Revenue per Order"
        assert metric.component_measures == ["Total Revenue", "Order Count"]
        assert isinstance(metric.expression, BinaryOp)
        assert metric.expression.op == "/"

    def test_resolve_metric_populates_components(self) -> None:
        model = _load_model()
        resolver = QueryResolver()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Revenue per Order"],
            ),
        )
        resolved = resolver.resolve(query, model)
        assert "Total Revenue" in resolved.metric_components
        assert "Order Count" in resolved.metric_components
        assert resolved.metric_components["Total Revenue"].aggregation == "sum"
        assert resolved.metric_components["Order Count"].aggregation == "count"


class TestStarSchemaMetric:
    """Tests for star schema planner with metrics."""

    def test_metric_compiles_to_valid_sql(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Revenue per Order"],
            ),
        )
        result = pipeline.compile(query, model, "postgres")
        sql = result.sql
        assert "SELECT" in sql
        assert "GROUP BY" in sql
        # Should contain SUM and COUNT, not _ref_ placeholders
        assert "_ref_" not in sql
        assert "SUM" in sql.upper()
        assert "COUNT" in sql.upper()
        assert "Revenue per Order" in sql

    def test_metric_with_regular_measure(self) -> None:
        model = _load_model()
        pipeline = CompilationPipeline()
        query = QueryObject(
            select=QuerySelect(
                dimensions=["Customer Country"],
                measures=["Total Revenue", "Revenue per Order"],
            ),
        )
        result = pipeline.compile(query, model, "postgres")
        sql = result.sql
        assert "Total Revenue" in sql
        assert "Revenue per Order" in sql
        assert "_ref_" not in sql
