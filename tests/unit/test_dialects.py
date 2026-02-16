"""Tests for SQL dialect system."""

from __future__ import annotations

import pytest

from orionbelt.ast.builder import QueryBuilder, col, eq, lit
from orionbelt.ast.nodes import (
    AliasedExpr,
    BinaryOp,
    CaseExpr,
    Cast,
    ColumnRef,
    FunctionCall,
    InList,
    IsNull,
    Literal,
    OrderByItem,
    RelativeDateRange,
    Select,
    Star,
    WindowFunction,
)
from orionbelt.dialect import DialectRegistry
from orionbelt.dialect.clickhouse import ClickHouseDialect
from orionbelt.dialect.databricks import DatabricksDialect
from orionbelt.dialect.dremio import DremioDialect
from orionbelt.dialect.postgres import PostgresDialect
from orionbelt.dialect.registry import UnsupportedDialectError
from orionbelt.dialect.snowflake import SnowflakeDialect
from orionbelt.models.semantic import TimeGrain


class TestDialectRegistry:
    def test_available_dialects(self) -> None:
        available = DialectRegistry.available()
        assert "postgres" in available
        assert "snowflake" in available
        assert "clickhouse" in available
        assert "dremio" in available
        assert "databricks" in available

    def test_get_postgres(self) -> None:
        dialect = DialectRegistry.get("postgres")
        assert isinstance(dialect, PostgresDialect)

    def test_get_snowflake(self) -> None:
        dialect = DialectRegistry.get("snowflake")
        assert isinstance(dialect, SnowflakeDialect)

    def test_unsupported_dialect_error(self) -> None:
        with pytest.raises(UnsupportedDialectError) as exc_info:
            DialectRegistry.get("oracle")
        assert "oracle" in str(exc_info.value)
        assert "postgres" in str(exc_info.value)


class TestPostgresDialect:
    @pytest.fixture
    def dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def test_name(self, dialect: PostgresDialect) -> None:
        assert dialect.name == "postgres"

    def test_capabilities(self, dialect: PostgresDialect) -> None:
        assert dialect.capabilities.supports_cte is True
        assert dialect.capabilities.supports_qualify is False
        assert dialect.capabilities.supports_ilike is True

    def test_quote_identifier(self, dialect: PostgresDialect) -> None:
        assert dialect.quote_identifier("name") == '"name"'
        assert dialect.quote_identifier('has"quote') == '"has""quote"'

    def test_compile_simple_select(self, dialect: PostgresDialect) -> None:
        ast = QueryBuilder().select(Star()).from_("orders").build()
        sql = dialect.compile(ast)
        assert "SELECT *" in sql
        assert "FROM orders" in sql

    def test_compile_with_alias(self, dialect: PostgresDialect) -> None:
        ast = (
            QueryBuilder()
            .select(AliasedExpr(expr=col("name"), alias="customer_name"))
            .from_("customers", alias="c")
            .build()
        )
        sql = dialect.compile(ast)
        assert '"customer_name"' in sql
        assert '"c"' in sql

    def test_compile_aggregation(self, dialect: PostgresDialect) -> None:
        ast = (
            QueryBuilder()
            .select(
                col("country", "c"),
                AliasedExpr(
                    expr=FunctionCall(name="SUM", args=[col("amount", "o")]),
                    alias="total",
                ),
            )
            .from_("orders", alias="o")
            .join("customers", on=eq(col("customer_id", "o"), col("id", "c")), alias="c")
            .group_by(col("country", "c"))
            .order_by(col("total"), desc=True)
            .limit(100)
            .build()
        )
        sql = dialect.compile(ast)
        assert "SELECT" in sql
        assert "SUM" in sql
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql
        assert "DESC" in sql
        assert "LIMIT 100" in sql
        assert "LEFT JOIN" in sql

    def test_compile_where(self, dialect: PostgresDialect) -> None:
        ast = (
            QueryBuilder()
            .select(Star())
            .from_("t")
            .where(BinaryOp(left=col("status"), op="=", right=lit("active")))
            .build()
        )
        sql = dialect.compile(ast)
        assert "WHERE" in sql
        assert "'active'" in sql

    def test_compile_in_list(self, dialect: PostgresDialect) -> None:
        expr = InList(
            expr=col("status"),
            values=[lit("a"), lit("b")],
        )
        sql = dialect.compile_expr(expr)
        assert "IN" in sql
        assert "'a'" in sql

    def test_compile_is_null(self, dialect: PostgresDialect) -> None:
        expr = IsNull(expr=col("deleted_at"))
        sql = dialect.compile_expr(expr)
        assert "IS NULL" in sql

    def test_compile_is_not_null(self, dialect: PostgresDialect) -> None:
        expr = IsNull(expr=col("email"), negated=True)
        sql = dialect.compile_expr(expr)
        assert "IS NOT NULL" in sql

    def test_compile_case(self, dialect: PostgresDialect) -> None:
        expr = CaseExpr(
            when_clauses=[(eq(col("status"), lit("active")), lit("Yes"))],
            else_clause=lit("No"),
        )
        sql = dialect.compile_expr(expr)
        assert "CASE" in sql
        assert "WHEN" in sql
        assert "THEN" in sql
        assert "ELSE" in sql
        assert "END" in sql

    def test_compile_cast(self, dialect: PostgresDialect) -> None:
        expr = Cast(expr=col("age"), type_name="INTEGER")
        sql = dialect.compile_expr(expr)
        assert "CAST" in sql
        assert "INTEGER" in sql

    def test_time_grain(self, dialect: PostgresDialect) -> None:
        result = dialect.render_time_grain(col("order_date"), TimeGrain.MONTH)
        assert isinstance(result, FunctionCall)
        assert result.name == "date_trunc"

    def test_compile_null_literal(self, dialect: PostgresDialect) -> None:
        assert dialect.compile_expr(Literal.null()) == "NULL"

    def test_compile_boolean_literals(self, dialect: PostgresDialect) -> None:
        assert dialect.compile_expr(Literal.boolean(True)) == "TRUE"
        assert dialect.compile_expr(Literal.boolean(False)) == "FALSE"

    def test_compile_distinct_function(self, dialect: PostgresDialect) -> None:
        f = FunctionCall(name="COUNT", args=[col("id")], distinct=True)
        sql = dialect.compile_expr(f)
        assert "DISTINCT" in sql


class TestSnowflakeDialect:
    @pytest.fixture
    def dialect(self) -> SnowflakeDialect:
        return SnowflakeDialect()

    def test_name(self, dialect: SnowflakeDialect) -> None:
        assert dialect.name == "snowflake"

    def test_capabilities(self, dialect: SnowflakeDialect) -> None:
        assert dialect.capabilities.supports_qualify is True
        assert dialect.capabilities.supports_time_travel is True

    def test_quote_identifier(self, dialect: SnowflakeDialect) -> None:
        assert dialect.quote_identifier("col") == '"col"'

    def test_time_grain(self, dialect: SnowflakeDialect) -> None:
        result = dialect.render_time_grain(col("dt"), TimeGrain.MONTH)
        assert isinstance(result, FunctionCall)
        assert result.name == "DATE_TRUNC"

    def test_string_contains(self, dialect: SnowflakeDialect) -> None:
        result = dialect.render_string_contains(col("name"), lit("foo"))
        assert isinstance(result, FunctionCall)
        assert result.name == "CONTAINS"


class TestClickHouseDialect:
    @pytest.fixture
    def dialect(self) -> ClickHouseDialect:
        return ClickHouseDialect()

    def test_name(self, dialect: ClickHouseDialect) -> None:
        assert dialect.name == "clickhouse"

    def test_time_grain_month(self, dialect: ClickHouseDialect) -> None:
        result = dialect.render_time_grain(col("dt"), TimeGrain.MONTH)
        assert isinstance(result, FunctionCall)
        assert result.name == "toStartOfMonth"

    def test_time_grain_year(self, dialect: ClickHouseDialect) -> None:
        result = dialect.render_time_grain(col("dt"), TimeGrain.YEAR)
        assert isinstance(result, FunctionCall)
        assert result.name == "toStartOfYear"

    def test_cast_to_int(self, dialect: ClickHouseDialect) -> None:
        result = dialect.render_cast(col("val"), "INT")
        assert isinstance(result, FunctionCall)
        assert result.name == "toInt64"


class TestDatabricksDialect:
    @pytest.fixture
    def dialect(self) -> DatabricksDialect:
        return DatabricksDialect()

    def test_name(self, dialect: DatabricksDialect) -> None:
        assert dialect.name == "databricks"

    def test_backtick_quoting(self, dialect: DatabricksDialect) -> None:
        assert dialect.quote_identifier("col") == "`col`"
        assert dialect.quote_identifier("has`tick") == "`has``tick`"


class TestDremioDialect:
    @pytest.fixture
    def dialect(self) -> DremioDialect:
        return DremioDialect()

    def test_name(self, dialect: DremioDialect) -> None:
        assert dialect.name == "dremio"

    def test_capabilities(self, dialect: DremioDialect) -> None:
        assert dialect.capabilities.supports_arrays is False
        assert dialect.capabilities.supports_ilike is False


class TestCrossDialectConsistency:
    """Ensure the same query produces valid SQL across all dialects."""

    def _build_test_query(self) -> Select:
        return (
            QueryBuilder()
            .select(
                col("country"),
                AliasedExpr(
                    expr=FunctionCall(name="SUM", args=[col("amount")]),
                    alias="total",
                ),
            )
            .from_("orders")
            .where(BinaryOp(left=col("status"), op="=", right=lit("active")))
            .group_by(col("country"))
            .order_by(col("total"), desc=True)
            .limit(10)
            .build()
        )

    @pytest.mark.parametrize(
        "dialect_name", ["postgres", "snowflake", "clickhouse", "dremio", "databricks"]
    )
    def test_all_dialects_produce_valid_sql(self, dialect_name: str) -> None:
        ast = self._build_test_query()
        dialect = DialectRegistry.get(dialect_name)
        sql = dialect.compile(ast)
        # All dialects should produce SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
        assert "SELECT" in sql
        assert "FROM" in sql
        assert "WHERE" in sql
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql
        assert "LIMIT" in sql
        assert "SUM" in sql


class TestWindowFunctionRendering:
    """Test window function rendering across all dialects."""

    @pytest.mark.parametrize(
        "dialect_name", ["postgres", "snowflake", "clickhouse", "dremio", "databricks"]
    )
    def test_sum_over_empty(self, dialect_name: str) -> None:
        """SUM(x) OVER () — grand total."""
        dialect = DialectRegistry.get(dialect_name)
        wf = WindowFunction(func_name="SUM", args=[ColumnRef(name="amount")])
        sql = dialect.compile_expr(wf)
        assert "SUM(" in sql
        assert "OVER ()" in sql

    @pytest.mark.parametrize(
        "dialect_name", ["postgres", "snowflake", "clickhouse", "dremio", "databricks"]
    )
    def test_count_distinct_over_empty(self, dialect_name: str) -> None:
        """COUNT(DISTINCT x) OVER ()."""
        dialect = DialectRegistry.get(dialect_name)
        wf = WindowFunction(
            func_name="COUNT",
            args=[ColumnRef(name="id")],
            distinct=True,
        )
        sql = dialect.compile_expr(wf)
        assert "COUNT(DISTINCT" in sql
        assert "OVER ()" in sql

    @pytest.mark.parametrize(
        "dialect_name", ["postgres", "snowflake", "clickhouse", "dremio", "databricks"]
    )
    def test_with_partition_by(self, dialect_name: str) -> None:
        """SUM(x) OVER (PARTITION BY dept)."""
        dialect = DialectRegistry.get(dialect_name)
        wf = WindowFunction(
            func_name="SUM",
            args=[ColumnRef(name="amount")],
            partition_by=[ColumnRef(name="dept")],
        )
        sql = dialect.compile_expr(wf)
        assert "SUM(" in sql
        assert "PARTITION BY" in sql
        assert "OVER (" in sql

    def test_with_order_by(self) -> None:
        """ROW_NUMBER() OVER (ORDER BY salary DESC)."""
        dialect = DialectRegistry.get("postgres")
        wf = WindowFunction(
            func_name="ROW_NUMBER",
            args=[],
            order_by=[OrderByItem(expr=ColumnRef(name="salary"), desc=True)],
        )
        sql = dialect.compile_expr(wf)
        assert "ROW_NUMBER()" in sql
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_with_partition_and_order(self) -> None:
        """SUM(x) OVER (PARTITION BY dept ORDER BY hire_date ASC)."""
        dialect = DialectRegistry.get("postgres")
        wf = WindowFunction(
            func_name="SUM",
            args=[ColumnRef(name="salary")],
            partition_by=[ColumnRef(name="dept")],
            order_by=[OrderByItem(expr=ColumnRef(name="hire_date"))],
        )
        sql = dialect.compile_expr(wf)
        assert "PARTITION BY" in sql
        assert "ORDER BY" in sql


@pytest.mark.parametrize(
    ("dialect_name", "expected_date_fn", "expected_add_fn"),
    [
        ("postgres", "CURRENT_DATE", "INTERVAL"),
        ("snowflake", "CURRENT_DATE()", "DATEADD('day'"),
        ("clickhouse", "today()", "addDays"),
        ("databricks", "current_date()", "date_add("),
        ("dremio", "CURRENT_DATE", "DATE_ADD"),
    ],
)
def test_relative_date_range_compiles(
    dialect_name: str, expected_date_fn: str, expected_add_fn: str
) -> None:
    dialect = DialectRegistry.get(dialect_name)
    expr = RelativeDateRange(
        column=ColumnRef(name="order_date"),
        unit="day",
        count=7,
        direction="past",
        include_current=True,
    )
    sql = dialect.compile_expr(expr)
    assert "order_date" in sql
    assert expected_date_fn in sql
    assert expected_add_fn in sql
