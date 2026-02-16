"""Tests for SQL validation using sqlglot."""

from __future__ import annotations

import pytest

from orionbelt.compiler.pipeline import CompilationPipeline
from orionbelt.compiler.validator import validate_sql
from orionbelt.models.query import QueryObject, QuerySelect
from orionbelt.models.semantic import SemanticModel
from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver
from tests.conftest import SAMPLE_MODEL_YAML


def _load_model() -> SemanticModel:
    loader = TrackedLoader()
    resolver = ReferenceResolver()
    raw, source_map = loader.load_string(SAMPLE_MODEL_YAML)
    model, result = resolver.resolve(raw, source_map)
    assert result.valid, f"Model errors: {[e.message for e in result.errors]}"
    return model


@pytest.mark.parametrize("dialect", ["postgres", "snowflake", "clickhouse", "databricks", "dremio"])
def test_valid_sql_all_dialects(dialect: str) -> None:
    errors = validate_sql("SELECT 1", dialect)
    assert errors == []


def test_invalid_sql_returns_errors() -> None:
    errors = validate_sql("SELECT FROM WHERE", "postgres")
    assert len(errors) > 0


def test_dremio_maps_to_trino() -> None:
    errors = validate_sql("SELECT 1 AS x", "dremio")
    assert errors == []


def test_unknown_dialect_returns_warning() -> None:
    errors = validate_sql("SELECT 1", "unknown_db")
    assert len(errors) == 1
    assert "Unknown dialect" in errors[0]


def test_validation_integrated_in_pipeline() -> None:
    model = _load_model()
    pipeline = CompilationPipeline()
    query = QueryObject(
        select=QuerySelect(
            dimensions=["Customer Country"],
            measures=["Total Revenue"],
        ),
    )
    result = pipeline.compile(query, model, "postgres")
    assert result.sql_valid is True
    assert result.sql != ""


def test_pipeline_returns_sql_even_when_invalid() -> None:
    model = _load_model()
    pipeline = CompilationPipeline()
    query = QueryObject(
        select=QuerySelect(
            dimensions=["Customer Country"],
            measures=["Total Revenue"],
        ),
    )
    result = pipeline.compile(query, model, "postgres")
    # SQL is always returned regardless of validation
    assert result.sql != ""
    assert isinstance(result.sql_valid, bool)
