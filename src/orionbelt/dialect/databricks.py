"""Databricks SQL dialect implementation."""

from __future__ import annotations

from orionbelt.ast.nodes import Cast, Expr, FunctionCall, Literal
from orionbelt.dialect.base import Dialect, DialectCapabilities
from orionbelt.dialect.registry import DialectRegistry
from orionbelt.models.semantic import TimeGrain


@DialectRegistry.register
class DatabricksDialect(Dialect):
    """Databricks SQL dialect — Spark SQL semantics, backtick identifiers."""

    @property
    def name(self) -> str:
        return "databricks"

    @property
    def capabilities(self) -> DialectCapabilities:
        return DialectCapabilities(
            supports_cte=True,
            supports_qualify=False,
            supports_arrays=True,
            supports_window_filters=False,
            supports_ilike=False,
        )

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    def render_time_grain(self, column: Expr, grain: TimeGrain) -> Expr:
        return FunctionCall(name="date_trunc", args=[Literal.string(grain.value), column])

    def render_cast(self, expr: Expr, target_type: str) -> Expr:
        return Cast(expr=expr, type_name=target_type)

    def render_string_contains(self, column: Expr, pattern: Expr) -> Expr:
        from orionbelt.ast.nodes import BinaryOp

        return BinaryOp(
            left=FunctionCall(name="lower", args=[column]),
            op="LIKE",
            right=BinaryOp(
                left=BinaryOp(
                    left=Literal.string("%"),
                    op="||",
                    right=FunctionCall(name="lower", args=[pattern]),
                ),
                op="||",
                right=Literal.string("%"),
            ),
        )

    def current_date_sql(self) -> str:
        return "current_date()"

    def date_add_sql(self, date_sql: str, unit: str, count: int) -> str:
        if unit == "day":
            return f"date_add({date_sql}, {count})"
        if unit == "week":
            return f"date_add({date_sql}, {count * 7})"
        if unit == "month":
            return f"add_months({date_sql}, {count})"
        if unit == "year":
            return f"add_months({date_sql}, {count * 12})"
        raise ValueError(f"Unsupported unit '{unit}' for Databricks")
