"""Wrapper CTE for cumulative (running/rolling/grain-to-date) metrics.

Cumulative metrics are window functions applied to already-aggregated measures,
ordered by a time dimension. Three core patterns:

| Pattern        | SQL Frame                                           |
|----------------|-----------------------------------------------------|
| Running total  | ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW    |
| Rolling window | ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW          |
| Grain-to-date  | PARTITION BY TRUNC(grain) + ROWS UNBOUNDED PRECEDING |

The wrapper follows the same CTE pattern as ``total_wrap.py``:
the planner output becomes a base CTE, and an outer query applies
the cumulative window functions.
"""

from __future__ import annotations

from orionbelt.ast.nodes import (
    CTE,
    AliasedExpr,
    ColumnRef,
    Expr,
    From,
    FunctionCall,
    OrderByItem,
    Select,
    WindowFrame,
    WindowFunction,
)
from orionbelt.compiler.resolution import ResolvedMeasure, ResolvedQuery
from orionbelt.models.semantic import CumulativeAggType, GrainToDate

# Map CumulativeAggType → SQL window function name
_CUMULATIVE_AGG_MAP: dict[CumulativeAggType, str] = {
    CumulativeAggType.SUM: "SUM",
    CumulativeAggType.AVG: "AVG",
    CumulativeAggType.MIN: "MIN",
    CumulativeAggType.MAX: "MAX",
    CumulativeAggType.COUNT: "COUNT",
}

# Map GrainToDate → DATE_TRUNC grain string
_GRAIN_TRUNC_MAP: dict[GrainToDate, str] = {
    GrainToDate.YEAR: "year",
    GrainToDate.QUARTER: "quarter",
    GrainToDate.MONTH: "month",
    GrainToDate.WEEK: "week",
}


def _build_cumulative_window(
    measure: ResolvedMeasure,
    time_dim_name: str,
) -> Expr:
    """Build the window function expression for a cumulative metric."""
    func_name = _CUMULATIVE_AGG_MAP[measure.cumulative_type]
    base_ref = ColumnRef(name=measure.cumulative_measure or measure.name)
    time_ref = ColumnRef(name=time_dim_name)
    order_by = [OrderByItem(expr=time_ref)]

    if measure.cumulative_grain_to_date is not None:
        # Grain-to-date: PARTITION BY DATE_TRUNC(grain, time_dim), unbounded frame
        grain = _GRAIN_TRUNC_MAP[measure.cumulative_grain_to_date]
        partition_expr = FunctionCall(
            name="DATE_TRUNC",
            args=[ColumnRef(name=grain), time_ref],
        )
        return WindowFunction(
            func_name=func_name,
            args=[base_ref],
            partition_by=[partition_expr],
            order_by=order_by,
            frame=WindowFrame(
                mode="ROWS",
                start="UNBOUNDED PRECEDING",
                end="CURRENT ROW",
            ),
        )

    if measure.cumulative_window is not None:
        # Rolling window: ROWS BETWEEN (window-1) PRECEDING AND CURRENT ROW
        preceding = measure.cumulative_window - 1
        return WindowFunction(
            func_name=func_name,
            args=[base_ref],
            order_by=order_by,
            frame=WindowFrame(
                mode="ROWS",
                start=f"{preceding} PRECEDING",
                end="CURRENT ROW",
            ),
        )

    # Running total (unbounded): ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    return WindowFunction(
        func_name=func_name,
        args=[base_ref],
        order_by=order_by,
        frame=WindowFrame(
            mode="ROWS",
            start="UNBOUNDED PRECEDING",
            end="CURRENT ROW",
        ),
    )


def wrap_with_cumulative(ast: Select, resolved: ResolvedQuery) -> Select:
    """Wrap a planner AST with a CTE + outer query for cumulative metrics.

    If no cumulative metrics are present, returns ``ast`` unchanged.
    """
    if not resolved.has_cumulative:
        return ast

    # Find time dimension names used by cumulative metrics
    cumulative_measures: list[ResolvedMeasure] = [m for m in resolved.measures if m.is_cumulative]

    # --- Build base CTE columns from the planner's AST ---
    # We need to decompose cumulative metrics into their base measure components
    # in the base CTE, then apply window functions in the outer query.
    direct_measure_names = {m.name for m in resolved.measures if not m.component_measures}
    cumulative_names = {m.name for m in cumulative_measures}

    base_columns: list[Expr] = []
    for col_node in ast.columns:
        alias = _get_alias(col_node)
        if alias and alias in cumulative_names:
            # Replace cumulative metric with its base measure component
            cum_metric = next(m for m in cumulative_measures if m.name == alias)
            comp_name = cum_metric.cumulative_measure
            if comp_name and comp_name not in direct_measure_names:
                comp = resolved.metric_components.get(comp_name)
                if comp:
                    # Only add the component if not already present
                    already_in_base = any(_get_alias(c) == comp_name for c in base_columns)
                    if not already_in_base:
                        base_columns.append(AliasedExpr(expr=comp.expression, alias=comp.name))
            # If the base measure is already a direct measure, it's already in the columns
        else:
            base_columns.append(col_node)

    # --- Build base CTE ---
    base_cte_query = Select(
        columns=base_columns,
        from_=ast.from_,
        joins=ast.joins,
        where=ast.where,
        group_by=ast.group_by,
        having=ast.having,
        order_by=[],
        limit=None,
        offset=None,
        ctes=[],
    )

    cte_name = "cumulative_base"
    base_cte = CTE(name=cte_name, query=base_cte_query)

    # --- Build outer SELECT ---
    outer_columns: list[Expr] = []

    # Dimensions: pass-through
    for dim in resolved.dimensions:
        outer_columns.append(AliasedExpr(expr=ColumnRef(name=dim.name), alias=dim.name))

    # Measures and metrics
    for m in resolved.measures:
        if m.is_cumulative:
            # Cumulative metric: build window function
            assert m.cumulative_time_dimension is not None
            window_expr = _build_cumulative_window(m, m.cumulative_time_dimension)
            outer_columns.append(AliasedExpr(expr=window_expr, alias=m.name))
        else:
            # Regular measure or derived metric: pass-through
            outer_columns.append(AliasedExpr(expr=ColumnRef(name=m.name), alias=m.name))

    # --- ORDER BY remapping ---
    outer_order_by = []
    for ob in ast.order_by:
        remapped = _remap_order_by_expr(ob.expr)
        outer_order_by.append(OrderByItem(expr=remapped, desc=ob.desc, nulls_last=ob.nulls_last))

    # --- Assemble final Select ---
    all_ctes = list(ast.ctes) + [base_cte]

    return Select(
        columns=outer_columns,
        from_=From(source=cte_name, alias=cte_name),
        joins=[],
        where=None,
        group_by=[],
        having=None,
        order_by=outer_order_by,
        limit=ast.limit,
        offset=ast.offset,
        ctes=all_ctes,
    )


def _get_alias(expr: Expr) -> str | None:
    """Extract the alias from an AliasedExpr, or None."""
    if isinstance(expr, AliasedExpr):
        return expr.alias
    return None


def _remap_order_by_expr(expr: Expr) -> Expr:
    """Remap table-qualified column refs to alias-only for the outer query."""
    if isinstance(expr, ColumnRef) and expr.table is not None:
        return ColumnRef(name=expr.name)
    return expr
