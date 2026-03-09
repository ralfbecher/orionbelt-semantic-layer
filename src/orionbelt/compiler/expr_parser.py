"""Generic expression tokenizer and recursive descent parser.

Handles two expression syntaxes:
- Metric formulas: ``{[Measure Name]}`` references → ``ColumnRef(name=...)``
- Measure expressions: ``{[DataObject].[Column]}`` references → ``ColumnRef(name=..., table=...)``

Both share the same arithmetic grammar:
    expr   → term (('+' | '-') term)*
    term   → factor (('*' | '/') factor)*
    factor → '(' expr ')' | NUMBER | REF
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from orionbelt.ast.nodes import BinaryOp, ColumnRef, Expr, Literal

if TYPE_CHECKING:
    from orionbelt.models.semantic import SemanticModel


@dataclass
class _Token:
    """A token from expression tokenization."""

    kind: str  # "ref", "colref", "number", "op", "lparen", "rparen"
    value: str


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

_MEASURE_REF_PATTERN = re.compile(r"\{\[([^\]]+)\]\.\[([^\]]+)\]\}", re.DOTALL)


def _tokenize_common(formula: str, tokens: list[_Token], start: int) -> int:
    """Tokenize a common token (number, operator, paren) at position *start*.

    Returns the new position after the consumed token, or *start* if no
    common token was found (caller should skip the character).
    """
    ch = formula[start]
    if ch in " \t\n":
        return start + 1
    if ch in "0123456789" or (
        ch == "." and start + 1 < len(formula) and formula[start + 1].isdigit()
    ):
        j = start
        while j < len(formula) and (formula[j].isdigit() or formula[j] == "."):
            j += 1
        tokens.append(_Token(kind="number", value=formula[start:j]))
        return j
    if ch in "+-*/":
        tokens.append(_Token(kind="op", value=ch))
        return start + 1
    if ch == "(":
        tokens.append(_Token(kind="lparen", value="("))
        return start + 1
    if ch == ")":
        tokens.append(_Token(kind="rparen", value=")"))
        return start + 1
    return start + 1  # skip unrecognised


def tokenize_metric_formula(formula: str) -> list[_Token]:
    """Tokenize a metric formula with ``{[Measure Name]}`` references."""
    tokens: list[_Token] = []
    i = 0
    while i < len(formula):
        ch = formula[i]
        if ch == "{" and i + 1 < len(formula) and formula[i + 1] == "[":
            end = formula.find("]}", i + 2)
            if end == -1:
                raise ValueError("Unclosed {[...]} reference in metric formula")
            ref_name = formula[i + 2 : end]
            if "{[" in ref_name:
                raise ValueError("Unclosed {[...]} reference in metric formula")
            tokens.append(_Token(kind="ref", value=ref_name))
            i = end + 2
        else:
            i = _tokenize_common(formula, tokens, i)
    return tokens


def tokenize_measure_expression(formula: str, model: SemanticModel) -> list[_Token]:
    """Tokenize a measure expression with ``{[DataObject].[Column]}`` references.

    Column references are resolved to physical names and stored as ``"colref"``
    tokens carrying ``table\\0column`` in their ``value``.
    """
    tokens: list[_Token] = []
    i = 0
    while i < len(formula):
        ch = formula[i]
        if ch == "{" and i + 1 < len(formula) and formula[i + 1] == "[":
            m = _MEASURE_REF_PATTERN.match(formula, i)
            if m:
                obj_name, col_name = m.group(1), m.group(2)
                obj = model.data_objects.get(obj_name)
                source = obj.columns[col_name].code if obj and col_name in obj.columns else col_name
                tokens.append(_Token(kind="colref", value=f"{obj_name}\0{source}"))
                i = m.end()
            else:
                i += 1
        else:
            i = _tokenize_common(formula, tokens, i)
    return tokens


# ---------------------------------------------------------------------------
# Parsing (recursive descent, shared by both expression types)
# ---------------------------------------------------------------------------


def parse_expression(tokens: list[_Token]) -> Expr:
    """Parse tokens into an AST with correct operator precedence.

    Handles ``"ref"`` tokens (metric formula → unqualified ColumnRef) and
    ``"colref"`` tokens (measure expression → qualified ColumnRef) uniformly.
    """
    pos = [0]

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
            _advance()
            node = _parse_expr()
            if _peek() and _peek().kind == "rparen":  # type: ignore[union-attr]
                _advance()
            return node
        if tok.kind == "number":
            _advance()
            val = float(tok.value) if "." in tok.value else int(tok.value)
            return Literal.number(val)
        if tok.kind == "ref":
            _advance()
            return ColumnRef(name=tok.value)
        if tok.kind == "colref":
            _advance()
            table, column = tok.value.split("\0", 1)
            return ColumnRef(name=column, table=table)
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
