"""CFL (Composite Fact Layer) planner: conformed dimensions + fact stitching."""

from __future__ import annotations

from orionbelt.ast.builder import QueryBuilder
from orionbelt.ast.nodes import (
    CTE,
    AliasedExpr,
    BinaryOp,
    Cast,
    ColumnRef,
    Expr,
    FunctionCall,
    Literal,
    Select,
    UnionAll,
)
from orionbelt.compiler.graph import JoinGraph
from orionbelt.compiler.resolution import ResolvedMeasure, ResolvedQuery
from orionbelt.compiler.star import QueryPlan
from orionbelt.models.semantic import SemanticModel


class FanoutError(Exception):
    """Raised when CFL planning detects a grain incompatibility (fanout)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class CFLPlanner:
    """Plans Composite Fact Layer queries: conformed dimensions + fact stitching.

    Uses a UNION ALL strategy:
    1. Each fact leg SELECTs conformed dimensions + its own measures (NULL for others)
    2. UNION ALL combines the legs into a single CTE
    3. Outer query aggregates over the union, grouping by conformed dimensions
    """

    def plan(self, resolved: ResolvedQuery, model: SemanticModel) -> QueryPlan:
        """Plan a CFL query."""
        self._validate_fanout(resolved, model)

        # Group measures by their source object
        measures_by_object, cross_fact = self._group_measures_by_object(resolved, model)

        if len(measures_by_object) <= 1 and not cross_fact:
            # Single fact — delegate to star schema
            from orionbelt.compiler.star import StarSchemaPlanner

            return StarSchemaPlanner().plan(resolved, model)

        # Multi-fact: UNION ALL strategy
        return self._plan_union_all(resolved, model, measures_by_object, cross_fact)

    def _validate_fanout(self, resolved: ResolvedQuery, model: SemanticModel) -> None:
        """Validate that grain is compatible and no fanout will occur."""
        errors: list[str] = []

        for dim in resolved.dimensions:
            if dim.object_name not in model.data_objects:
                errors.append(
                    f"Dimension '{dim.name}' references unknown data object '{dim.object_name}'"
                )

        if errors:
            raise FanoutError("; ".join(errors))

    def _group_measures_by_object(
        self,
        resolved: ResolvedQuery,
        model: SemanticModel,
    ) -> tuple[dict[str, list[ResolvedMeasure]], list[ResolvedMeasure]]:
        """Group measures by their primary source object.

        Returns ``(groups, cross_fact)`` where *cross_fact* contains
        multi-field measures whose fields span multiple objects.
        For metrics, expand their component measures into the grouping
        instead of the metric itself.  Cross-fact measures ensure every
        involved object has a leg, but are not assigned to any single
        group — their individual fields are distributed per-leg by
        ``_plan_union_all``.
        """
        groups: dict[str, list[ResolvedMeasure]] = {}
        cross_fact: list[ResolvedMeasure] = []
        seen: set[str] = set()

        for measure in resolved.measures:
            if measure.component_measures:
                # Metric: add each component measure to its source object
                for comp_name in measure.component_measures:
                    if comp_name in seen:
                        continue
                    seen.add(comp_name)
                    comp = resolved.metric_components.get(comp_name)
                    if comp is None:
                        continue
                    model_measure = model.measures.get(comp_name)
                    if model_measure and model_measure.columns:
                        obj_name = model_measure.columns[0].view or resolved.base_object
                    else:
                        obj_name = resolved.base_object
                    groups.setdefault(obj_name, []).append(comp)
            else:
                if measure.name in seen:
                    continue
                seen.add(measure.name)
                model_measure = model.measures.get(measure.name)
                if not model_measure or not model_measure.columns:
                    groups.setdefault(resolved.base_object, []).append(measure)
                    continue

                # Collect all distinct source objects for this measure
                field_objects = {f.view for f in model_measure.columns if f.view}
                if len(field_objects) > 1:
                    # Cross-fact multi-field measure: ensure each
                    # involved object has a leg, but don't assign
                    # the measure to any single group.
                    cross_fact.append(measure)
                    for obj in field_objects:
                        groups.setdefault(obj, [])
                else:
                    obj_name = model_measure.columns[0].view or resolved.base_object
                    groups.setdefault(obj_name, []).append(measure)

        return groups, cross_fact

    @staticmethod
    def _is_multi_field(measure: ResolvedMeasure) -> bool:
        """Check if a measure has multiple field args (e.g. COUNT(a, b))."""
        return isinstance(measure.expression, FunctionCall) and len(measure.expression.args) > 1

    @staticmethod
    def _multi_field_cte_alias(measure_name: str, idx: int) -> str:
        """CTE column name for the *idx*-th field of a multi-field measure."""
        return f"{measure_name}__f{idx}"

    @staticmethod
    def _unwrap_aggregation(measure: ResolvedMeasure) -> Expr:
        """Extract the inner expression from an aggregated measure.

        For FunctionCall(SUM, [inner]) → returns inner.
        Falls back to the full expression if not a FunctionCall.
        """
        if isinstance(measure.expression, FunctionCall) and measure.expression.args:
            return measure.expression.args[0]
        return measure.expression

    def _build_outer_metric_expr(
        self,
        metric: ResolvedMeasure,
        resolved: ResolvedQuery,
    ) -> Expr:
        """Build the outer query expression for a metric.

        Walks the metric's AST tree and replaces each ColumnRef(measure_name)
        with ``AGG("measure_name")`` using the component measure's aggregation.
        """
        return self._substitute_outer_refs(metric.expression, resolved)

    def _substitute_outer_refs(self, expr: Expr, resolved: ResolvedQuery) -> Expr:
        """Recursively substitute measure refs with outer aggregations."""
        if isinstance(expr, ColumnRef) and expr.table is None:
            comp = resolved.metric_components.get(expr.name)
            if comp:
                agg = comp.aggregation.upper()
                distinct = False
                if agg == "COUNT_DISTINCT":
                    agg = "COUNT"
                    distinct = True
                if isinstance(comp.expression, FunctionCall) and comp.expression.distinct:
                    distinct = True
                return FunctionCall(
                    name=agg,
                    args=[ColumnRef(name=comp.name)],
                    distinct=distinct,
                )
        if isinstance(expr, BinaryOp):
            new_left = self._substitute_outer_refs(expr.left, resolved)
            new_right = self._substitute_outer_refs(expr.right, resolved)
            if new_left is not expr.left or new_right is not expr.right:
                return BinaryOp(left=new_left, op=expr.op, right=new_right)
        return expr

    def _build_outer_concat_count(
        self,
        measure_name: str,
        n_fields: int,
        agg: str,
        distinct: bool,
    ) -> Expr:
        """Build ``COUNT(DISTINCT CAST(f0 AS VARCHAR) || '|' || ...)`` for the outer query."""
        parts: list[Expr] = [
            Cast(
                expr=ColumnRef(name=self._multi_field_cte_alias(measure_name, i)),
                type_name="VARCHAR",
            )
            for i in range(n_fields)
        ]
        concat: Expr = parts[0]
        for part in parts[1:]:
            concat = BinaryOp(
                left=concat,
                op="||",
                right=BinaryOp(
                    left=Literal.string("|"),
                    op="||",
                    right=part,
                ),
            )
        return FunctionCall(name=agg, args=[concat], distinct=distinct)

    def _plan_union_all(
        self,
        resolved: ResolvedQuery,
        model: SemanticModel,
        measures_by_object: dict[str, list[ResolvedMeasure]],
        cross_fact: list[ResolvedMeasure] | None = None,
    ) -> QueryPlan:
        """UNION ALL strategy: stack fact legs with NULL padding, aggregate outside."""
        graph = JoinGraph(model, use_path_names=resolved.use_path_names or None)

        # Collect all measures across all objects + cross-fact measures
        all_measures: list[ResolvedMeasure] = []
        for measures in measures_by_object.values():
            all_measures.extend(measures)
        if cross_fact:
            all_measures.extend(cross_fact)

        # Build one SELECT per fact object
        union_legs: list[Select] = []
        for obj_name, measures in measures_by_object.items():
            leg_builder = QueryBuilder()
            this_measure_names = {m.name for m in measures}

            # SELECT conformed dimensions
            for dim in resolved.dimensions:
                col = ColumnRef(name=dim.source_column, table=dim.object_name)
                leg_builder.select(AliasedExpr(expr=col, alias=dim.name))

            # SELECT this fact's measures (raw expressions, no aggregation)
            # and NULL for the other facts' measures.
            # Multi-field measures expand into one CTE column per field.
            for m in all_measures:
                if self._is_multi_field(m):
                    assert isinstance(m.expression, FunctionCall)
                    for i, arg in enumerate(m.expression.args):
                        alias = self._multi_field_cte_alias(m.name, i)
                        # Each field goes into the leg that owns its table
                        arg_table = arg.table if isinstance(arg, ColumnRef) else None
                        if arg_table == obj_name:
                            leg_builder.select(AliasedExpr(expr=arg, alias=alias))
                        else:
                            leg_builder.select(AliasedExpr(expr=Literal.null(), alias=alias))
                elif m.name in this_measure_names:
                    leg_builder.select(AliasedExpr(expr=self._unwrap_aggregation(m), alias=m.name))
                else:
                    leg_builder.select(AliasedExpr(expr=Literal.null(), alias=m.name))

            # FROM fact object
            obj = model.data_objects.get(obj_name)
            if obj:
                leg_builder.from_(obj.qualified_code, alias=obj_name)

            # JOINs for this fact's dimensions
            required = {dim.object_name for dim in resolved.dimensions}
            if obj_name in required:
                required = required - {obj_name}
            steps = graph.find_join_path({obj_name}, required | {obj_name})
            for step in steps:
                target_object = model.data_objects.get(step.to_object)
                if target_object:
                    on_expr = graph.build_join_condition(step)
                    leg_builder.join(
                        table=target_object.qualified_code,
                        on=on_expr,
                        join_type=step.join_type,
                        alias=step.to_object,
                    )

            union_legs.append(leg_builder.build())

        # Create the UNION ALL CTE
        cte_name = "composite_01"
        union_cte = CTE(name=cte_name, query=UnionAll(queries=union_legs))

        # Build outer query: aggregate over the composite CTE
        outer_builder = QueryBuilder()

        # SELECT dimensions
        for dim in resolved.dimensions:
            outer_builder.select(
                AliasedExpr(
                    expr=ColumnRef(name=dim.name),
                    alias=dim.name,
                )
            )

        # SELECT aggregated measures and metrics
        # First, add all component measures (from UNION ALL legs)
        seen_measure_names: set[str] = set()
        for m in all_measures:
            seen_measure_names.add(m.name)
            agg = m.aggregation.upper()
            distinct = False
            if agg == "COUNT_DISTINCT":
                agg = "COUNT"
                distinct = True
            if isinstance(m.expression, FunctionCall) and m.expression.distinct:
                distinct = True

            if self._is_multi_field(m):
                # Multi-field: concat CTE columns in outer query
                assert isinstance(m.expression, FunctionCall)
                n_fields = len(m.expression.args)
                agg_expr: Expr = self._build_outer_concat_count(m.name, n_fields, agg, distinct)
            else:
                agg_expr = FunctionCall(
                    name=agg,
                    args=[ColumnRef(name=m.name)],
                    distinct=distinct,
                )
            outer_builder.select(AliasedExpr(expr=agg_expr, alias=m.name))

        # Then, add metric expressions that combine component measures
        for m in resolved.measures:
            if m.component_measures and m.name not in seen_measure_names:
                metric_expr = self._build_outer_metric_expr(m, resolved)
                outer_builder.select(AliasedExpr(expr=metric_expr, alias=m.name))

        outer_builder.from_(cte_name, alias=cte_name)

        # GROUP BY dimensions
        for dim in resolved.dimensions:
            outer_builder.group_by(ColumnRef(name=dim.name))

        # ORDER BY and LIMIT
        for expr, desc in resolved.order_by_exprs:
            outer_builder.order_by(expr, desc=desc)
        if resolved.limit is not None:
            outer_builder.limit(resolved.limit)

        outer_select = outer_builder.build()

        # Attach CTE
        final = Select(
            columns=outer_select.columns,
            from_=outer_select.from_,
            joins=outer_select.joins,
            where=outer_select.where,
            group_by=outer_select.group_by,
            having=outer_select.having,
            order_by=outer_select.order_by,
            limit=outer_select.limit,
            ctes=[union_cte],
        )

        return QueryPlan(ast=final)
