"""Dialect plugin registry — discover and register dialect implementations."""

from __future__ import annotations

from orionbelt.dialect.base import Dialect


class UnsupportedDialectError(Exception):
    """Raised when a requested dialect is not registered."""

    def __init__(self, name: str, available: list[str]) -> None:
        self.dialect_name = name
        self.available = available
        super().__init__(f"Unsupported dialect '{name}'. Available: {', '.join(available)}")


class DialectRegistry:
    """Registry for SQL dialect plugins."""

    _dialects: dict[str, type[Dialect]] = {}

    @classmethod
    def register(cls, dialect_class: type[Dialect]) -> type[Dialect]:
        """Register a dialect class. Can be used as a decorator."""
        # Instantiate to read the name property
        instance = dialect_class()
        cls._dialects[instance.name] = dialect_class
        return dialect_class

    @classmethod
    def get(cls, name: str) -> Dialect:
        """Get an instance of the named dialect."""
        if name not in cls._dialects:
            raise UnsupportedDialectError(name, available=cls.available())
        return cls._dialects[name]()

    @classmethod
    def available(cls) -> list[str]:
        """List registered dialect names."""
        return sorted(cls._dialects.keys())

    @classmethod
    def reset(cls) -> None:
        """Clear all registered dialects (for testing)."""
        cls._dialects.clear()
