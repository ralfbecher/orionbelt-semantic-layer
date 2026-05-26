"""Tests for CASE / IN / BETWEEN / IS NULL / LIKE in computed-column
expression parser (v2.7.3+, issue #77).

Pre-v2.7.3 the parser silently dropped tokens it couldn't handle, so
``CASE WHEN x THEN y END`` compiled to the string literal ``'CASE'``
with no error and ``SUM('CASE')`` ended up in the SQL.
"""

from __future__ import annotations

import pytest

from orionbelt.ast.nodes import Between, BinaryOp, CaseExpr, InList, IsNull, Literal
from orionbelt.compiler.expr_parser import parse_expression, tokenize_measure_expression
from orionbelt.compiler.pipeline import CompilationPipeline
from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver

_MODEL_YAML = """\
version: 1.0
dataObjects:
  Financial:
    code: financial
    columns:
      Default Status:
        code: dflt_stts
        abstractType: string
      Outstanding Nominal Amount:
        code: otstndng_nmnl_amnt
        abstractType: float
        numClass: additive
      Off Balance Sheet Amount:
        code: off_blnc_sht_amnt
        abstractType: float
        numClass: additive
      Credit Exposure Amount:
        expression: '{Outstanding Nominal Amount} + {Off Balance Sheet Amount}'
        abstractType: float
        numClass: additive
      Defaulted Credit Exposure Base Amount:
        expression: >-
          CASE WHEN {Default Status} NOT IN ('11', '14')
          THEN {Credit Exposure Amount} ELSE 0 END
        abstractType: float
        numClass: additive
dimensions:
  Default Status:
    dataObject: Financial
    column: Default Status
    resultType: string
measures:
  Defaulted Credit Exposure Amount:
    columns:
      - dataObject: Financial
        column: Defaulted Credit Exposure Base Amount
    aggregation: sum
    resultType: float
"""


def _load_model():
    loader = TrackedLoader()
    raw, sm = loader.load_string(_MODEL_YAML)
    model, vr = ReferenceResolver().resolve(raw, sm)
    assert vr.valid, vr.errors
    return model


class TestCaseExpression:
    def test_case_compiles_to_case_expr(self):
        model = _load_model()
        # Use qualified refs so the tokenizer doesn't need the
        # ``{ColumnName}`` rewrite step (that happens during recursive
        # tokenisation, not direct parser calls).
        tokens = tokenize_measure_expression(
            "CASE WHEN {[Financial].[Default Status]} NOT IN ('11', '14') "
            "THEN {[Financial].[Outstanding Nominal Amount]} "
            "ELSE 0 END",
            model,
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, CaseExpr)
        assert len(ast.when_clauses) == 1
        assert isinstance(ast.when_clauses[0][0], InList)
        assert ast.when_clauses[0][0].negated is True
        assert isinstance(ast.else_clause, Literal)

    def test_issue_77_repro_compiles_to_real_sql(self):
        """The exact #77 repro must produce SUM(CASE …), not SUM('CASE')."""
        from orionbelt.models.query import QueryObject, QuerySelect

        model = _load_model()
        q = QueryObject(
            select=QuerySelect(measures=["Defaulted Credit Exposure Amount"]),
        )
        result = CompilationPipeline().compile(q, model, "postgres")
        assert "'CASE'" not in result.sql, f"Silent-drop regression: {result.sql}"
        assert "CASE" in result.sql
        assert "WHEN" in result.sql
        assert "NOT IN" in result.sql
        assert "ELSE" in result.sql
        assert "END" in result.sql

    def test_case_with_else(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "CASE WHEN {[Financial].[Default Status]} = '11' THEN 1 ELSE 0 END", model
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, CaseExpr)
        assert ast.else_clause is not None

    def test_case_without_else(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "CASE WHEN {[Financial].[Default Status]} = '11' THEN 1 END", model
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, CaseExpr)
        assert ast.else_clause is None

    def test_case_multiple_whens(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "CASE "
            "WHEN {[Financial].[Default Status]} = '11' THEN 1 "
            "WHEN {[Financial].[Default Status]} = '14' THEN 2 "
            "ELSE 0 END",
            model,
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, CaseExpr)
        assert len(ast.when_clauses) == 2


class TestPostfixPredicates:
    def test_in_list(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "{[Financial].[Default Status]} IN ('11', '14')", model
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, InList)
        assert ast.negated is False
        assert len(ast.values) == 2

    def test_not_in(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "{[Financial].[Default Status]} NOT IN ('11', '14')", model
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, InList)
        assert ast.negated is True

    def test_between(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "{[Financial].[Outstanding Nominal Amount]} BETWEEN 0 AND 100", model
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, Between)
        assert ast.negated is False

    def test_not_between(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "{[Financial].[Outstanding Nominal Amount]} NOT BETWEEN 0 AND 100", model
        )
        ast = parse_expression(tokens)
        assert isinstance(ast, Between)
        assert ast.negated is True

    def test_is_null(self):
        model = _load_model()
        tokens = tokenize_measure_expression("{[Financial].[Default Status]} IS NULL", model)
        ast = parse_expression(tokens)
        assert isinstance(ast, IsNull)
        assert ast.negated is False

    def test_is_not_null(self):
        model = _load_model()
        tokens = tokenize_measure_expression("{[Financial].[Default Status]} IS NOT NULL", model)
        ast = parse_expression(tokens)
        assert isinstance(ast, IsNull)
        assert ast.negated is True

    def test_like(self):
        model = _load_model()
        tokens = tokenize_measure_expression("{[Financial].[Default Status]} LIKE '1%'", model)
        ast = parse_expression(tokens)
        assert isinstance(ast, BinaryOp)
        assert ast.op == "LIKE"

    def test_not_like(self):
        model = _load_model()
        tokens = tokenize_measure_expression("{[Financial].[Default Status]} NOT LIKE '1%'", model)
        ast = parse_expression(tokens)
        assert isinstance(ast, BinaryOp)
        assert ast.op == "NOT LIKE"


class TestParserStrictness:
    """The pre-v2.7.3 parser silently dropped tokens it couldn't parse,
    producing garbage SQL. Now malformed expressions error loudly."""

    def test_dangling_tokens_raise(self):
        model = _load_model()
        # ``1 + 2 unexpected`` — the bare ident after ``2`` has no role.
        tokens = tokenize_measure_expression("1 + 2 garbage", model)
        # `garbage` becomes a bare-ident literal followed by no operator —
        # parser sees the literal as ``_parse_factor`` second factor of
        # nothing. Actually this parses OK as 1 + 2, then `garbage` is
        # leftover.
        with pytest.raises(ValueError, match="Unexpected token"):
            parse_expression(tokens)

    def test_unterminated_case_raises(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "CASE WHEN {[Financial].[Default Status]} = '11' THEN 1", model
        )
        with pytest.raises(ValueError, match="Unterminated CASE"):
            parse_expression(tokens)

    def test_case_when_without_then_raises(self):
        model = _load_model()
        tokens = tokenize_measure_expression("CASE WHEN {[Financial].[Default Status]} END", model)
        with pytest.raises(ValueError, match="THEN"):
            parse_expression(tokens)

    def test_in_without_parens_raises(self):
        model = _load_model()
        tokens = tokenize_measure_expression("{[Financial].[Default Status]} IN '11'", model)
        with pytest.raises(ValueError, match="IN must be followed by"):
            parse_expression(tokens)

    def test_between_without_and_raises(self):
        model = _load_model()
        tokens = tokenize_measure_expression(
            "{[Financial].[Outstanding Nominal Amount]} BETWEEN 0 100", model
        )
        with pytest.raises(ValueError, match="BETWEEN"):
            parse_expression(tokens)

    def test_is_without_null_raises(self):
        model = _load_model()
        tokens = tokenize_measure_expression("{[Financial].[Default Status]} IS '11'", model)
        with pytest.raises(ValueError, match="IS predicate"):
            parse_expression(tokens)

    def test_missing_closing_paren_raises(self):
        model = _load_model()
        tokens = tokenize_measure_expression("(1 + 2", model)
        with pytest.raises(ValueError, match="closing"):
            parse_expression(tokens)


class TestDialectRendering:
    """The repro must compile to syntactically-plausible SQL on every dialect."""

    @pytest.mark.parametrize(
        "dialect",
        [
            "postgres",
            "mysql",
            "duckdb",
            "clickhouse",
            "snowflake",
            "bigquery",
            "databricks",
            "dremio",
        ],
    )
    def test_case_renders_on_all_dialects(self, dialect):
        from orionbelt.models.query import QueryObject, QuerySelect

        model = _load_model()
        q = QueryObject(
            select=QuerySelect(measures=["Defaulted Credit Exposure Amount"]),
        )
        result = CompilationPipeline().compile(q, model, dialect)
        sql = result.sql
        assert "'CASE'" not in sql, f"{dialect}: silent-drop regression"
        assert "CASE" in sql
        assert "WHEN" in sql
        assert "END" in sql
