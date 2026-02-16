"""Star schema planner: single fact table with dimension joins → AST."""

from __future__ import annotations

from dataclasses import dataclass

from orionbelt.ast.builder import QueryBuilder
from orionbelt.ast.nodes import (
    AliasedExpr,
    BinaryOp,
    ColumnRef,
    Expr,
    Select,
)
from orionbelt.compiler.graph import JoinGraph
from orionbelt.compiler.resolution import ResolvedMeasure, ResolvedQuery
from orionbelt.models.semantic import SemanticModel


def _substitute_measure_refs(
    expr: Expr,
    components: dict[str, ResolvedMeasure],
) -> Expr:
    """Walk a metric AST tree and replace ColumnRef placeholders with aggregate expressions."""
    if isinstance(expr, ColumnRef) and expr.table is None and expr.name in components:
        return components[expr.name].expression
    if isinstance(expr, BinaryOp):
        new_left = _substitute_measure_refs(expr.left, components)
        new_right = _substitute_measure_refs(expr.right, components)
        if new_left is not expr.left or new_right is not expr.right:
            return BinaryOp(left=new_left, op=expr.op, right=new_right)
    return expr


@dataclass
class QueryPlan:
    """A planned query ready for AST construction."""

    ast: Select


class StarSchemaPlanner:
    """Plans star-schema queries: single fact base with dimension joins."""

    def plan(self, resolved: ResolvedQuery, model: SemanticModel) -> QueryPlan:
        builder = QueryBuilder()
        graph = JoinGraph(model)

        base_object = model.data_objects.get(resolved.base_object)
        if not base_object:
            return QueryPlan(ast=builder.build())

        base_alias = resolved.base_object

        # SELECT: dimensions
        for dim in resolved.dimensions:
            col = ColumnRef(name=dim.source_column, table=dim.object_name)
            if dim.grain:
                # Time grain will be applied by dialect, for now use column directly
                builder.select(AliasedExpr(expr=col, alias=dim.name))
            else:
                builder.select(AliasedExpr(expr=col, alias=dim.name))

        # SELECT: measures (aggregated) — for metrics, substitute component refs
        for measure in resolved.measures:
            if measure.component_measures:
                substituted = _substitute_measure_refs(
                    measure.expression, resolved.metric_components
                )
                builder.select(AliasedExpr(expr=substituted, alias=measure.name))
            else:
                builder.select(AliasedExpr(expr=measure.expression, alias=measure.name))

        # FROM: base fact table
        builder.from_(base_object.qualified_code, alias=base_alias)

        # JOINs: dimension tables
        for step in resolved.join_steps:
            target_object = model.data_objects.get(step.to_object)
            if not target_object:
                continue
            on_expr = graph.build_join_condition(step)
            builder.join(
                table=target_object.qualified_code,
                on=on_expr,
                join_type=step.join_type,
                alias=step.to_object,
            )

        # WHERE
        for wf in resolved.where_filters:
            builder.where(wf.expression)

        # GROUP BY (all dimension columns)
        for dim in resolved.dimensions:
            col = ColumnRef(name=dim.source_column, table=dim.object_name)
            builder.group_by(col)

        # HAVING
        for hf in resolved.having_filters:
            builder.having(hf.expression)

        # ORDER BY
        for expr, desc in resolved.order_by_exprs:
            builder.order_by(expr, desc=desc)

        # LIMIT
        if resolved.limit is not None:
            builder.limit(resolved.limit)

        return QueryPlan(ast=builder.build())
