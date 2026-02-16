"""Snowflake dialect implementation."""

from __future__ import annotations

from orionbelt.ast.nodes import Cast, Expr, FunctionCall, Literal, UnionAll
from orionbelt.dialect.base import Dialect, DialectCapabilities
from orionbelt.dialect.registry import DialectRegistry
from orionbelt.models.semantic import TimeGrain


@DialectRegistry.register
class SnowflakeDialect(Dialect):
    """Snowflake dialect — QUALIFY, case-sensitive identifiers, semi-structured types."""

    @property
    def name(self) -> str:
        return "snowflake"

    @property
    def capabilities(self) -> DialectCapabilities:
        return DialectCapabilities(
            supports_cte=True,
            supports_qualify=True,
            supports_arrays=True,
            supports_window_filters=True,
            supports_ilike=True,
            supports_time_travel=True,
            supports_semi_structured=True,
        )

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def render_time_grain(self, column: Expr, grain: TimeGrain) -> Expr:
        return FunctionCall(name="DATE_TRUNC", args=[Literal.string(grain.value), column])

    def render_cast(self, expr: Expr, target_type: str) -> Expr:
        return Cast(expr=expr, type_name=target_type)

    def render_string_contains(self, column: Expr, pattern: Expr) -> Expr:
        return FunctionCall(name="CONTAINS", args=[column, pattern])

    def current_date_sql(self) -> str:
        return "CURRENT_DATE()"

    def date_add_sql(self, date_sql: str, unit: str, count: int) -> str:
        unit_sql = unit.lower()
        return f"DATEADD('{unit_sql}', {count}, {date_sql})"

    def _compile_multi_field_count(self, args: list[Expr], distinct: bool) -> str:
        """Snowflake supports native multi-arg COUNT(col1, col2)."""
        args_sql = ", ".join(self.compile_expr(a) for a in args)
        if distinct:
            return f"COUNT(DISTINCT {args_sql})"
        return f"COUNT({args_sql})"

    def compile_union_all(self, node: UnionAll) -> str:
        """Snowflake uses UNION ALL BY NAME to match columns by name."""
        return "\nUNION ALL BY NAME\n".join(self.compile_select(q) for q in node.queries)
