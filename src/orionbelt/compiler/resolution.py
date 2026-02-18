"""Phase 1: Resolve semantic references to physical expressions."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from orionbelt.ast.nodes import (
    BinaryOp,
    ColumnRef,
    Expr,
    FunctionCall,
    Literal,
    RelativeDateRange,
)
from orionbelt.compiler.graph import JoinGraph, JoinStep
from orionbelt.models.errors import SemanticError
from orionbelt.models.query import (
    DimensionRef,
    FilterOperator,
    QueryFilter,
    QueryObject,
    UsePathName,
)
from orionbelt.models.semantic import Measure, Metric, SemanticModel, TimeGrain


@dataclass
class ResolvedDimension:
    """A resolved dimension with its physical column reference."""

    name: str
    object_name: str
    column_name: str
    source_column: str
    grain: TimeGrain | None = None


@dataclass
class ResolvedMeasure:
    """A resolved measure with its aggregate expression."""

    name: str
    aggregation: str
    expression: Expr
    is_expression: bool = False
    component_measures: list[str] = field(default_factory=list)
    total: bool = False


@dataclass
class ResolvedFilter:
    """A resolved filter with physical expression."""

    expression: Expr
    is_aggregate: bool = False


@dataclass
class ResolvedQuery:
    """Result of query resolution — ready for SQL planning."""

    dimensions: list[ResolvedDimension] = field(default_factory=list)
    measures: list[ResolvedMeasure] = field(default_factory=list)
    base_object: str = ""
    required_objects: set[str] = field(default_factory=set)
    join_steps: list[JoinStep] = field(default_factory=list)
    where_filters: list[ResolvedFilter] = field(default_factory=list)
    having_filters: list[ResolvedFilter] = field(default_factory=list)
    order_by_exprs: list[tuple[Expr, bool]] = field(default_factory=list)
    limit: int | None = None
    warnings: list[str] = field(default_factory=list)
    requires_cfl: bool = False
    measure_source_objects: set[str] = field(default_factory=set)
    metric_components: dict[str, ResolvedMeasure] = field(default_factory=dict)
    use_path_names: list[UsePathName] = field(default_factory=list)

    @property
    def fact_tables(self) -> list[str]:
        if self.measure_source_objects:
            return sorted(self.measure_source_objects)
        return [self.base_object] if self.base_object else []

    @property
    def has_totals(self) -> bool:
        """Check if any measure (direct or metric component) uses total."""
        for m in self.measures:
            if m.total:
                return True
            for comp_name in m.component_measures:
                comp = self.metric_components.get(comp_name)
                if comp and comp.total:
                    return True
        return False


@dataclass
class _Token:
    """A token from metric formula tokenization."""

    kind: str  # "ref", "number", "op", "lparen", "rparen"
    value: str


def _tokenize_formula(formula: str) -> list[_Token]:
    """Tokenize a metric formula like ``{[Revenue]} / {[Order Count]}``."""
    tokens: list[_Token] = []
    i = 0
    while i < len(formula):
        ch = formula[i]
        if ch in " \t\n":
            i += 1
        elif ch == "{" and i + 1 < len(formula) and formula[i + 1] == "[":
            # {[Measure Name]} reference
            end = formula.find("]}", i + 2)
            if end == -1:
                raise ValueError("Unclosed {[...]} reference in metric formula")
            tokens.append(_Token(kind="ref", value=formula[i + 2 : end]))
            i = end + 2
        elif ch in "0123456789" or (
            ch == "." and i + 1 < len(formula) and formula[i + 1].isdigit()
        ):
            j = i
            while j < len(formula) and (formula[j].isdigit() or formula[j] == "."):
                j += 1
            tokens.append(_Token(kind="number", value=formula[i:j]))
            i = j
        elif ch in "+-*/":
            tokens.append(_Token(kind="op", value=ch))
            i += 1
        elif ch == "(":
            tokens.append(_Token(kind="lparen", value="("))
            i += 1
        elif ch == ")":
            tokens.append(_Token(kind="rparen", value=")"))
            i += 1
        else:
            i += 1
    return tokens


def _parse_metric_formula(tokens: list[_Token]) -> Expr:
    """Recursive descent parser for metric formulas with correct precedence.

    Grammar:
        expr     → term (('+' | '-') term)*
        term     → factor (('*' | '/') factor)*
        factor   → '(' expr ')' | NUMBER | MEASURE_REF
    """
    pos = [0]  # mutable index

    def _peek() -> _Token | None:
        return tokens[pos[0]] if pos[0] < len(tokens) else None

    def _advance() -> _Token:
        tok = tokens[pos[0]]
        pos[0] += 1
        return tok

    def _parse_factor() -> Expr:
        tok = _peek()
        if tok is None:
            return Literal.number(0)
        if tok.kind == "lparen":
            _advance()  # consume '('
            node = _parse_expr()
            if _peek() and _peek().kind == "rparen":  # type: ignore[union-attr]
                _advance()  # consume ')'
            return node
        if tok.kind == "number":
            _advance()
            val = float(tok.value) if "." in tok.value else int(tok.value)
            return Literal.number(val)
        if tok.kind == "ref":
            _advance()
            return ColumnRef(name=tok.value)
        _advance()
        return Literal.number(0)

    def _parse_term() -> Expr:
        left = _parse_factor()
        while _peek() and _peek().kind == "op" and _peek().value in "*/":  # type: ignore[union-attr]
            op_tok = _advance()
            right = _parse_factor()
            left = BinaryOp(left=left, op=op_tok.value, right=right)
        return left

    def _parse_expr() -> Expr:
        left = _parse_term()
        while _peek() and _peek().kind == "op" and _peek().value in "+-":  # type: ignore[union-attr]
            op_tok = _advance()
            right = _parse_term()
            left = BinaryOp(left=left, op=op_tok.value, right=right)
        return left

    return _parse_expr()


class QueryResolver:
    """Resolves a QueryObject + SemanticModel into a ResolvedQuery."""

    def resolve(self, query: QueryObject, model: SemanticModel) -> ResolvedQuery:
        errors: list[SemanticError] = []
        result = ResolvedQuery(
            limit=query.limit,
            use_path_names=list(query.use_path_names),
        )

        # Build global column lookup: col_name → (object_name, source_column)
        global_columns: dict[str, tuple[str, str]] = {}
        for obj_name, obj in model.data_objects.items():
            for col_name, col_obj in obj.columns.items():
                global_columns[col_name] = (obj_name, col_obj.code)

        # 1. Resolve dimensions
        for dim_str in query.select.dimensions:
            dim_ref = DimensionRef.parse(dim_str)
            resolved_dim = self._resolve_dimension(dim_ref, model, errors)
            if resolved_dim:
                result.dimensions.append(resolved_dim)
                result.required_objects.add(resolved_dim.object_name)

        # 2. Resolve measures and track their source objects
        for measure_name in query.select.measures:
            resolved_meas = self._resolve_measure(
                measure_name, model, global_columns, errors, result
            )
            if resolved_meas:
                result.measures.append(resolved_meas)
                # Collect all source objects for this measure/metric
                source_objs = self._get_measure_source_objects(measure_name, model, global_columns)
                result.measure_source_objects.update(source_objs)
                result.required_objects.update(source_objs)

        # 3. Determine base object (the one with most joins / most measures)
        result.base_object = self._select_base_object(result, model)
        if result.base_object:
            result.required_objects.add(result.base_object)

        # Detect multi-fact: if measures come from multiple source objects, use CFL
        if len(result.measure_source_objects) > 1:
            result.requires_cfl = True

        # 4. Validate usePathNames before building join graph
        for upn in query.use_path_names:
            if upn.source not in model.data_objects:
                errors.append(
                    SemanticError(
                        code="UNKNOWN_DATA_OBJECT",
                        message=(f"usePathNames references unknown data object '{upn.source}'"),
                        path="usePathNames",
                    )
                )
                continue
            if upn.target not in model.data_objects:
                errors.append(
                    SemanticError(
                        code="UNKNOWN_DATA_OBJECT",
                        message=(f"usePathNames references unknown data object '{upn.target}'"),
                        path="usePathNames",
                    )
                )
                continue
            # Check that a secondary join with this pathName exists for the pair
            source_obj = model.data_objects[upn.source]
            found = False
            for join in source_obj.joins:
                if (
                    join.join_to == upn.target
                    and join.secondary
                    and join.path_name == upn.path_name
                ):
                    found = True
                    break
            if not found:
                errors.append(
                    SemanticError(
                        code="UNKNOWN_PATH_NAME",
                        message=(
                            f"No secondary join with pathName '{upn.path_name}' "
                            f"from '{upn.source}' to '{upn.target}'"
                        ),
                        path="usePathNames",
                    )
                )

        # 5. Resolve join paths
        graph = JoinGraph(model, use_path_names=query.use_path_names or None)
        if result.base_object and len(result.required_objects) > 1:
            result.join_steps = graph.find_join_path({result.base_object}, result.required_objects)

        # 6. Classify filters
        for qf in query.where:
            resolved_filter = self._resolve_filter(qf, model, is_having=False, errors=errors)
            if resolved_filter:
                result.where_filters.append(resolved_filter)

        for qf in query.having:
            resolved_filter = self._resolve_filter(qf, model, is_having=True, errors=errors)
            if resolved_filter:
                result.having_filters.append(resolved_filter)

        # 7. Resolve order by
        for ob in query.order_by:
            expr = self._resolve_order_by_field(ob.field, result, model)
            if expr:
                result.order_by_exprs.append((expr, ob.direction == "desc"))

        if errors:
            raise ResolutionError(errors)

        return result

    def _resolve_dimension(
        self,
        ref: DimensionRef,
        model: SemanticModel,
        errors: list[SemanticError],
    ) -> ResolvedDimension | None:
        """Resolve a dimension reference to its physical column."""
        dim = model.dimensions.get(ref.name)
        if dim is None:
            errors.append(
                SemanticError(
                    code="UNKNOWN_DIMENSION",
                    message=f"Unknown dimension '{ref.name}'",
                    path="select.dimensions",
                )
            )
            return None

        obj_name = dim.view
        col_name = dim.column
        obj = model.data_objects.get(obj_name)
        if obj is None:
            errors.append(
                SemanticError(
                    code="UNKNOWN_DATA_OBJECT",
                    message=f"Dimension '{ref.name}' references unknown data object '{obj_name}'",
                )
            )
            return None

        vf = obj.columns.get(col_name)
        source_col = vf.code if vf else col_name

        return ResolvedDimension(
            name=ref.name,
            object_name=obj_name,
            column_name=col_name,
            source_column=source_col,
            grain=ref.grain or dim.time_grain,
        )

    def _resolve_measure(
        self,
        name: str,
        model: SemanticModel,
        global_columns: dict[str, tuple[str, str]],
        errors: list[SemanticError],
        result: ResolvedQuery | None = None,
    ) -> ResolvedMeasure | None:
        """Resolve a measure name to its aggregate expression."""
        measure = model.measures.get(name)
        if measure is None:
            # Check metrics
            metric = model.metrics.get(name)
            if metric:
                return self._resolve_metric(name, metric, model, global_columns, errors, result)
            errors.append(
                SemanticError(
                    code="UNKNOWN_MEASURE",
                    message=f"Unknown measure '{name}'",
                    path="select.measures",
                )
            )
            return None

        expr = self._build_measure_expr(measure.label, measure, model, global_columns)
        return ResolvedMeasure(
            name=name,
            aggregation=measure.aggregation,
            expression=expr,
            is_expression=measure.expression is not None,
            total=measure.total,
        )

    def _build_measure_expr(
        self,
        name: str,
        measure: Measure,
        model: SemanticModel,
        global_columns: dict[str, tuple[str, str]],
    ) -> Expr:
        """Build the aggregate expression for a measure."""
        if measure.expression:
            return self._expand_expression(measure, model, global_columns)

        # Build column references for all columns
        args: list[Expr] = []
        if measure.columns:
            for ref in measure.columns:
                obj_name = ref.view or ""
                col_name = ref.column or ""
                obj = model.data_objects.get(obj_name)
                source = obj.columns[col_name].code if obj and col_name in obj.columns else col_name
                args.append(ColumnRef(name=source, table=obj_name))
        if not args:
            args = [Literal.number(1)]

        agg = measure.aggregation.upper()
        distinct = measure.distinct
        if agg == "COUNT_DISTINCT":
            agg = "COUNT"
            distinct = True

        return FunctionCall(
            name=agg,
            args=args,
            distinct=distinct,
        )

    def _expand_expression(
        self,
        measure: Measure,
        model: SemanticModel,
        global_columns: dict[str, tuple[str, str]],
    ) -> Expr:
        """Expand a measure expression into AST nodes.

        Handles {[Column]} placeholders — column names are globally unique.
        """

        formula = measure.expression or ""
        agg = measure.aggregation.upper()

        # Replace {[Column]} with column references
        named_refs = re.findall(r"\{\[([^\]]+)\]\}", formula)
        for col_name in named_refs:
            if col_name in global_columns:
                obj_name, source = global_columns[col_name]
                formula = formula.replace(f"{{[{col_name}]}}", f"{obj_name}.{source}")

        # Wrap the whole formula in the aggregation function as raw SQL
        from orionbelt.ast.nodes import RawSQL

        distinct = measure.distinct
        if agg == "COUNT_DISTINCT":
            agg = "COUNT"
            distinct = True

        return FunctionCall(
            name=agg,
            args=[RawSQL(sql=formula)],
            distinct=distinct,
        )

    def _resolve_metric(
        self,
        name: str,
        metric: Metric,
        model: SemanticModel,
        global_columns: dict[str, tuple[str, str]],
        errors: list[SemanticError],
        result: ResolvedQuery | None = None,
    ) -> ResolvedMeasure | None:
        """Resolve a metric to its combined expression.

        Parses the formula into a proper AST tree and resolves each
        component measure so that planners can substitute them later.
        """
        formula = metric.expression

        # Extract and resolve each component measure
        component_names = re.findall(r"\{\[([^\]]+)\]\}", formula)
        for comp_name in component_names:
            if result is not None and comp_name not in result.metric_components:
                comp = self._resolve_measure(comp_name, model, global_columns, errors, result)
                if comp:
                    result.metric_components[comp_name] = comp

        # Parse the formula into an AST tree
        try:
            tokens = _tokenize_formula(formula)
            parsed_expr = _parse_metric_formula(tokens)
        except Exception as exc:
            errors.append(
                SemanticError(
                    code="INVALID_METRIC_EXPRESSION",
                    message=f"Metric '{name}' has invalid expression: {exc}",
                    path=f"metrics.{name}.expression",
                )
            )
            return None

        return ResolvedMeasure(
            name=name,
            aggregation="",
            expression=parsed_expr,
            component_measures=component_names,
            is_expression=True,
        )

    def _get_measure_source_objects(
        self,
        name: str,
        model: SemanticModel,
        global_columns: dict[str, tuple[str, str]],
    ) -> set[str]:
        """Extract all source data objects for a measure or metric."""
        result: set[str] = set()

        # Check simple measures first
        measure = model.measures.get(name)
        if measure:
            # Columns-based measure
            for cref in measure.columns:
                if cref.view:
                    result.add(cref.view)
            # Expression-based measure: extract {[Column]} references
            if measure.expression:
                col_refs = re.findall(r"\{\[([^\]]+)\]\}", measure.expression)
                for col_name in col_refs:
                    if col_name in global_columns:
                        obj_name, _ = global_columns[col_name]
                        result.add(obj_name)
            return result

        # Check metrics: {[Measure Name]} references → recurse
        metric = model.metrics.get(name)
        if metric:
            measure_refs = re.findall(r"\{\[([^\]]+)\]\}", metric.expression)
            for ref_name in measure_refs:
                result.update(self._get_measure_source_objects(ref_name, model, global_columns))

        return result

    def _select_base_object(self, result: ResolvedQuery, model: SemanticModel) -> str:
        """Select the base (fact) object — prefer measure source objects with most joins."""
        # Priority 1: measure source objects (true fact tables) — pick the one with most joins
        if result.measure_source_objects:
            best = ""
            best_joins = -1
            for obj_name in sorted(result.measure_source_objects):
                obj = model.data_objects.get(obj_name)
                n = len(obj.joins) if obj else 0
                if n > best_joins:
                    best = obj_name
                    best_joins = n
            if best:
                return best

        # Priority 2: any required object with joins defined
        for obj_name in sorted(result.required_objects):
            obj = model.data_objects.get(obj_name)
            if obj and obj.joins:
                return obj_name

        # Fallback: first required object
        if result.required_objects:
            return next(iter(sorted(result.required_objects)))
        if model.data_objects:
            return next(iter(model.data_objects))
        return ""

    def _resolve_filter(
        self,
        qf: QueryFilter,
        model: SemanticModel,
        is_having: bool,
        errors: list[SemanticError],
    ) -> ResolvedFilter | None:
        """Resolve a query filter to a physical expression."""
        # Try to find the field in dimensions first
        dim = model.dimensions.get(qf.field)
        if dim:
            obj_name = dim.view
            col_name = dim.column
            obj = model.data_objects.get(obj_name)
            source = obj.columns[col_name].code if obj and col_name in obj.columns else col_name
            col_expr: Expr = ColumnRef(name=source, table=obj_name)
        else:
            col_expr = ColumnRef(name=qf.field)

        filter_expr = self._build_filter_expr(col_expr, qf, errors)
        if filter_expr is None:
            return None
        return ResolvedFilter(expression=filter_expr, is_aggregate=is_having)

    def _build_filter_expr(
        self, col: Expr, qf: QueryFilter, errors: list[SemanticError]
    ) -> Expr | None:
        """Build a filter expression from operator and value."""
        from orionbelt.ast.nodes import InList, IsNull

        op = qf.op
        val = qf.value

        match op:
            case FilterOperator.EQUALS | FilterOperator.EQ:
                return BinaryOp(left=col, op="=", right=Literal(value=val))
            case FilterOperator.NOT_EQUALS | FilterOperator.NEQ:
                return BinaryOp(left=col, op="<>", right=Literal(value=val))
            case FilterOperator.GT | FilterOperator.GREATER:
                return BinaryOp(left=col, op=">", right=Literal(value=val))
            case FilterOperator.GTE | FilterOperator.GREATER_EQ:
                return BinaryOp(left=col, op=">=", right=Literal(value=val))
            case FilterOperator.LT | FilterOperator.LESS:
                return BinaryOp(left=col, op="<", right=Literal(value=val))
            case FilterOperator.LTE | FilterOperator.LESS_EQ:
                return BinaryOp(left=col, op="<=", right=Literal(value=val))
            case FilterOperator.IN_LIST | FilterOperator.IN:
                vals: list[Expr] = (
                    [Literal(value=v) for v in val]
                    if isinstance(val, list)
                    else [Literal(value=val)]
                )
                return InList(expr=col, values=vals)
            case FilterOperator.NOT_IN_LIST | FilterOperator.NOT_IN:
                not_vals: list[Expr] = (
                    [Literal(value=v) for v in val]
                    if isinstance(val, list)
                    else [Literal(value=val)]
                )
                return InList(expr=col, values=not_vals, negated=True)
            case FilterOperator.SET | FilterOperator.IS_NOT_NULL:
                return IsNull(expr=col, negated=True)
            case FilterOperator.NOT_SET | FilterOperator.IS_NULL:
                return IsNull(expr=col, negated=False)
            case FilterOperator.CONTAINS:
                return BinaryOp(
                    left=col,
                    op="LIKE",
                    right=Literal.string(f"%{val}%"),
                )
            case FilterOperator.NOT_CONTAINS:
                return BinaryOp(
                    left=col,
                    op="NOT LIKE",
                    right=Literal.string(f"%{val}%"),
                )
            case FilterOperator.STARTS_WITH:
                return BinaryOp(
                    left=col,
                    op="LIKE",
                    right=Literal.string(f"{val}%"),
                )
            case FilterOperator.ENDS_WITH:
                return BinaryOp(
                    left=col,
                    op="LIKE",
                    right=Literal.string(f"%{val}"),
                )
            case FilterOperator.LIKE:
                return BinaryOp(left=col, op="LIKE", right=Literal.string(str(val)))
            case FilterOperator.NOT_LIKE:
                return BinaryOp(left=col, op="NOT LIKE", right=Literal.string(str(val)))
            case FilterOperator.BETWEEN:
                from orionbelt.ast.nodes import Between

                if isinstance(val, list) and len(val) >= 2:
                    return Between(
                        expr=col,
                        low=Literal(value=val[0]),
                        high=Literal(value=val[1]),
                    )
                return BinaryOp(left=col, op="=", right=Literal(value=val))
            case FilterOperator.NOT_BETWEEN:
                from orionbelt.ast.nodes import Between

                if isinstance(val, list) and len(val) >= 2:
                    return Between(
                        expr=col,
                        low=Literal(value=val[0]),
                        high=Literal(value=val[1]),
                        negated=True,
                    )
                return BinaryOp(left=col, op="<>", right=Literal(value=val))
            case FilterOperator.RELATIVE:
                relative = self._parse_relative_filter(val, errors, field=qf.field)
                if relative is None:
                    return None
                return RelativeDateRange(
                    column=col,
                    unit=relative["unit"],
                    count=relative["count"],
                    direction=relative["direction"],
                    include_current=relative["include_current"],
                )
            case _:
                errors.append(
                    SemanticError(
                        code="INVALID_FILTER_OPERATOR",
                        message=f"Unsupported filter operator '{op}'",
                        path="filters",
                    )
                )
                return None

    def _parse_relative_filter(
        self, value: object, errors: list[SemanticError], field: str
    ) -> dict[str, object] | None:
        if not isinstance(value, dict):
            errors.append(
                SemanticError(
                    code="INVALID_RELATIVE_FILTER",
                    message=(
                        f"Relative filter for '{field}' must be an object "
                        "with keys {unit, count, direction?, include_current?}"
                    ),
                    path="filters",
                )
            )
            return None

        unit = value.get("unit")
        count = value.get("count")
        direction = value.get("direction", "past")
        include_current = value.get("include_current", value.get("includeCurrent", True))

        if not isinstance(unit, str):
            errors.append(
                SemanticError(
                    code="INVALID_RELATIVE_FILTER",
                    message=f"Relative filter for '{field}' requires string 'unit'",
                    path="filters",
                )
            )
            return None
        unit = unit.lower()
        if unit not in {"day", "week", "month", "year"}:
            errors.append(
                SemanticError(
                    code="INVALID_RELATIVE_FILTER",
                    message=f"Relative filter for '{field}' has unsupported unit '{unit}'",
                    path="filters",
                )
            )
            return None
        if not isinstance(count, int) or count <= 0:
            errors.append(
                SemanticError(
                    code="INVALID_RELATIVE_FILTER",
                    message=f"Relative filter for '{field}' requires positive integer 'count'",
                    path="filters",
                )
            )
            return None
        if direction not in {"past", "future"}:
            errors.append(
                SemanticError(
                    code="INVALID_RELATIVE_FILTER",
                    message=(f"Relative filter for '{field}' has invalid direction '{direction}'"),
                    path="filters",
                )
            )
            return None
        if not isinstance(include_current, bool):
            errors.append(
                SemanticError(
                    code="INVALID_RELATIVE_FILTER",
                    message=(f"Relative filter for '{field}' has non-boolean include_current"),
                    path="filters",
                )
            )
            return None

        return {
            "unit": unit,
            "count": count,
            "direction": direction,
            "include_current": include_current,
        }

    def _resolve_order_by_field(
        self,
        field_name: str,
        result: ResolvedQuery,
        model: SemanticModel,
    ) -> Expr | None:
        """Resolve an order-by field to its expression."""
        # Check if it's a resolved dimension
        for dim in result.dimensions:
            if dim.name == field_name:
                return ColumnRef(name=dim.source_column, table=dim.object_name)

        # Check if it's a resolved measure
        for meas in result.measures:
            if meas.name == field_name:
                return meas.expression

        return ColumnRef(name=field_name)


class ResolutionError(Exception):
    """Raised when query resolution encounters errors."""

    def __init__(self, errors: list[SemanticError]) -> None:
        self.errors = errors
        messages = "; ".join(e.message for e in errors)
        super().__init__(f"Resolution errors: {messages}")
