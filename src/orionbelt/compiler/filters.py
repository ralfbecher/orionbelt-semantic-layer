"""Filter expression builder — converts QueryFilter to AST expressions."""

from __future__ import annotations

from typing import TypedDict

from orionbelt.ast.nodes import (
    Between,
    BinaryOp,
    Expr,
    InList,
    IsNull,
    Literal,
    RelativeDateRange,
)
from orionbelt.models.errors import SemanticError
from orionbelt.models.query import FilterOperator, QueryFilter


class RelativeFilterParsed(TypedDict):
    unit: str
    count: int
    direction: str
    include_current: bool


def build_filter_expr(col: Expr, qf: QueryFilter, errors: list[SemanticError]) -> Expr | None:
    """Build a filter expression from operator and value."""
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
                [Literal(value=v) for v in val] if isinstance(val, list) else [Literal(value=val)]
            )
            return InList(expr=col, values=vals)
        case FilterOperator.NOT_IN_LIST | FilterOperator.NOT_IN:
            not_vals: list[Expr] = (
                [Literal(value=v) for v in val] if isinstance(val, list) else [Literal(value=val)]
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
            if isinstance(val, list) and len(val) >= 2:
                return Between(
                    expr=col,
                    low=Literal(value=val[0]),
                    high=Literal(value=val[1]),
                )
            return BinaryOp(left=col, op="=", right=Literal(value=val))
        case FilterOperator.NOT_BETWEEN:
            if isinstance(val, list) and len(val) >= 2:
                return Between(
                    expr=col,
                    low=Literal(value=val[0]),
                    high=Literal(value=val[1]),
                    negated=True,
                )
            return BinaryOp(left=col, op="<>", right=Literal(value=val))
        case FilterOperator.RELATIVE:
            relative = parse_relative_filter(val, errors, field=qf.field)
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


def parse_relative_filter(
    value: object, errors: list[SemanticError], field: str
) -> RelativeFilterParsed | None:
    """Parse and validate a relative date filter value."""
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
                message=f"Relative filter for '{field}' has invalid direction '{direction}'",
                path="filters",
            )
        )
        return None
    if not isinstance(include_current, bool):
        errors.append(
            SemanticError(
                code="INVALID_RELATIVE_FILTER",
                message=f"Relative filter for '{field}' has non-boolean include_current",
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
