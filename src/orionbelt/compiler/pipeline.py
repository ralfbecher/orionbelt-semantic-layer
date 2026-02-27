"""Orchestrates the full compilation pipeline: Query → Resolution → Planning → AST → SQL."""

from __future__ import annotations

from dataclasses import dataclass, field

from orionbelt.compiler.cfl import CFLPlanner
from orionbelt.compiler.codegen import CodeGenerator
from orionbelt.compiler.fanout import detect_fanout
from orionbelt.compiler.resolution import QueryResolver
from orionbelt.compiler.star import StarSchemaPlanner
from orionbelt.compiler.total_wrap import wrap_with_totals
from orionbelt.compiler.validator import validate_sql
from orionbelt.dialect.registry import DialectRegistry
from orionbelt.models.query import QueryObject
from orionbelt.models.semantic import SemanticModel


@dataclass
class ResolvedInfo:
    """Summary of what was resolved during compilation."""

    fact_tables: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    measures: list[str] = field(default_factory=list)


@dataclass
class CompilationResult:
    """The result of compiling a query to SQL."""

    sql: str
    dialect: str
    resolved: ResolvedInfo
    warnings: list[str] = field(default_factory=list)
    sql_valid: bool = True


class CompilationPipeline:
    """Orchestrates: Query → Resolution → Planning → AST → SQL."""

    def __init__(self) -> None:
        self._resolver = QueryResolver()
        self._star_planner = StarSchemaPlanner()
        self._cfl_planner = CFLPlanner()

    def compile(
        self,
        query: QueryObject,
        model: SemanticModel,
        dialect_name: str,
    ) -> CompilationResult:
        """Compile a query to SQL for the specified dialect."""
        # Phase 1: Resolution
        resolved = self._resolver.resolve(query, model)

        # Phase 1.5: Fanout detection (skip for CFL — each fact queried independently)
        if not resolved.requires_cfl:
            detect_fanout(resolved, model)

        # Create dialect early so planners can use dialect-aware table formatting
        dialect = DialectRegistry.get(dialect_name)
        qualify_table = lambda obj: dialect.format_table_ref(  # noqa: E731
            obj.database, obj.schema_name, obj.code
        )

        # Phase 2: Planning (star schema or CFL)
        if resolved.requires_cfl:
            plan = self._cfl_planner.plan(resolved, model, qualify_table=qualify_table)
        else:
            plan = self._star_planner.plan(resolved, model, qualify_table=qualify_table)

        # Phase 2.5: Wrap with totals CTE if needed
        wrapped_ast = wrap_with_totals(plan.ast, resolved)

        # Phase 3: Dialect-specific SQL rendering
        codegen = CodeGenerator(dialect)
        sql = codegen.generate(wrapped_ast)

        # Phase 4: SQL validation (non-blocking)
        validation_errors = validate_sql(sql, dialect_name)
        sql_valid = len(validation_errors) == 0
        warnings = resolved.warnings
        if not sql_valid:
            warnings = warnings + [f"SQL validation: {e}" for e in validation_errors]

        return CompilationResult(
            sql=sql,
            dialect=dialect_name,
            resolved=ResolvedInfo(
                fact_tables=resolved.fact_tables,
                dimensions=[d.name for d in resolved.dimensions],
                measures=[m.name for m in resolved.measures],
            ),
            warnings=warnings,
            sql_valid=sql_valid,
        )
