"""PEP 249 DB-API 2.0 exception hierarchy.

Hierarchy (per PEP 249):
    StandardError (builtin Exception)
    ├── Warning
    └── Error
        ├── InterfaceError
        └── DatabaseError
            ├── DataError
            ├── OperationalError
            ├── IntegrityError
            ├── InternalError
            ├── ProgrammingError
            └── NotSupportedError
"""

from __future__ import annotations


class Warning(Exception):  # noqa: A001
    """Exception raised for important warnings (e.g. data truncation)."""


class Error(Exception):
    """Base class for all DB-API errors."""


class InterfaceError(Error):
    """Exception for errors related to the database interface, not the database itself."""


class DatabaseError(Error):
    """Exception for errors related to the database."""


class DataError(DatabaseError):
    """Exception for errors due to problems with processed data."""


class OperationalError(DatabaseError):
    """Exception for errors related to the database's operation."""


class IntegrityError(DatabaseError):
    """Exception for errors related to database integrity."""


class InternalError(DatabaseError):
    """Exception for internal database errors."""


class ProgrammingError(DatabaseError):
    """Exception for programming errors (e.g. bad SQL, table not found)."""


class NotSupportedError(DatabaseError):
    """Exception for optional features not supported by the database."""
