"""ClickHouse dialect implementation."""

from __future__ import annotations

from orionbelt.ast.nodes import BinaryOp, Cast, Expr, FunctionCall, Literal, OrderByItem
from orionbelt.dialect.base import Dialect, DialectCapabilities
from orionbelt.dialect.registry import DialectRegistry
from orionbelt.models.semantic import TimeGrain

_GRAIN_FUNCTIONS: dict[TimeGrain, str] = {
    TimeGrain.YEAR: "toStartOfYear",
    TimeGrain.QUARTER: "toStartOfQuarter",
    TimeGrain.MONTH: "toStartOfMonth",
    TimeGrain.WEEK: "toMonday",
    TimeGrain.DAY: "toDate",
    TimeGrain.HOUR: "toStartOfHour",
    TimeGrain.MINUTE: "toStartOfMinute",
    TimeGrain.SECOND: "toStartOfSecond",
}


@DialectRegistry.register
class ClickHouseDialect(Dialect):
    """ClickHouse dialect — custom date functions, aggregation differences."""

    _ABSTRACT_TYPE_MAP: dict[str, str] = {
        "string": "String",
        "json": "String",
        "int": "Int64",
        "float": "Float64",
        "date": "Date",
        "time": "String",
        "time_tz": "String",
        "timestamp": "DateTime",
        "timestamp_tz": "DateTime",
        "boolean": "Bool",
    }

    def format_table_ref(self, database: str, schema: str, code: str) -> str:
        """ClickHouse: two-part ``schema.code`` (OBML schema maps to CH database)."""
        return f"{schema}.{code}"

    @property
    def name(self) -> str:
        return "clickhouse"

    @property
    def capabilities(self) -> DialectCapabilities:
        return DialectCapabilities(
            supports_cte=True,
            supports_qualify=False,
            supports_arrays=True,
            supports_window_filters=False,
            supports_ilike=True,
        )

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def render_time_grain(self, column: Expr, grain: TimeGrain) -> Expr:
        func_name = _GRAIN_FUNCTIONS.get(grain)
        if func_name:
            return FunctionCall(name=func_name, args=[column])
        return column

    def render_cast(self, expr: Expr, target_type: str) -> Expr:
        # ClickHouse uses toType functions for common casts
        type_map: dict[str, str] = {
            "INT": "toInt64",
            "INTEGER": "toInt64",
            "FLOAT": "toFloat64",
            "STRING": "toString",
            "DATE": "toDate",
        }
        func_name = type_map.get(target_type.upper())
        if func_name:
            return FunctionCall(name=func_name, args=[expr])
        return Cast(expr=expr, type_name=target_type)

    def render_string_contains(self, column: Expr, pattern: Expr) -> Expr:
        return BinaryOp(
            left=column,
            op="ILIKE",
            right=BinaryOp(
                left=BinaryOp(left=Literal.string("%"), op="||", right=pattern),
                op="||",
                right=Literal.string("%"),
            ),
        )

    _FUNCTION_NAME_MAP: dict[str, str] = {
        "ANY_VALUE": "any",
    }

    def _map_function_name(self, name: str) -> str:
        return self._FUNCTION_NAME_MAP.get(name.upper(), name)

    def _compile_mode(self, args: list[Expr]) -> str:
        """ClickHouse: topK(1)(col)[1] — returns the most frequent value."""
        col_sql = self.compile_expr(args[0]) if args else "NULL"
        return f"topK(1)({col_sql})[1]"

    def _compile_listagg(
        self,
        args: list[Expr],
        distinct: bool,
        order_by: list[OrderByItem],
        separator: str | None,
    ) -> str:
        """ClickHouse: arrayStringConcat([arraySort](groupArray/groupUniqArray(col)), sep).

        ClickHouse does not support ORDER BY inside aggregate functions.
        Only self-ordering (sorting the aggregated column) is supported via arraySort.
        Cross-column ordering raises an error.
        """
        sep = separator if separator is not None else ","
        col_sql = self.compile_expr(args[0]) if args else "''"
        escaped_sep = sep.replace("'", "''")
        group_fn = "groupUniqArray" if distinct else "groupArray"
        inner = f"{group_fn}({col_sql})"
        if order_by:
            ob_expr = order_by[0]
            ob_sql = self.compile_expr(ob_expr.expr)
            if ob_sql != col_sql:
                raise ValueError(
                    f"ClickHouse LISTAGG does not support ORDER BY on a different column "
                    f"(aggregated: {col_sql}, order by: {ob_sql})"
                )
            sort_fn = "arrayReverseSort" if ob_expr.desc else "arraySort"
            inner = f"{sort_fn}({inner})"
        return f"arrayStringConcat({inner}, '{escaped_sep}')"

    def current_date_sql(self) -> str:
        return "today()"

    def date_add_sql(self, date_sql: str, unit: str, count: int) -> str:
        funcs: dict[str, str] = {
            "day": "addDays",
            "week": "addWeeks",
            "month": "addMonths",
            "year": "addYears",
        }
        func = funcs.get(unit)
        if func is None:
            raise ValueError(f"Unsupported unit '{unit}' for ClickHouse")
        return f"{func}({date_sql}, {count})"
