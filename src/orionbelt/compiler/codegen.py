"""Phase 3: AST → SQL string via dialect rendering."""

from __future__ import annotations

from orionbelt.ast.nodes import Select
from orionbelt.dialect.base import Dialect


class CodeGenerator:
    """Generates SQL from AST using a dialect."""

    def __init__(self, dialect: Dialect) -> None:
        self._dialect = dialect

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    def generate(self, ast: Select) -> str:
        """Generate SQL string from AST using the configured dialect."""
        return self._dialect.compile(ast)
