"""ob-postgres — OrionBelt Semantic Layer driver for PostgreSQL (PEP 249 DB-API 2.0).

Requires the OrionBelt REST API running in single-model mode (MODEL_FILE set).
OBML queries are compiled to SQL via ``POST /v1/query/sql``.

Usage::

    import ob_postgres

    conn = ob_postgres.connect(dbname="mydb", user="me", password="secret")
    with conn.cursor() as cur:
        cur.execute("select:\\n  dimensions:\\n    - Region\\n  measures:\\n    - Revenue")
        print(cur.fetchall())
"""

from __future__ import annotations

from typing import Any

import psycopg2

from ob_postgres.connection import Connection
from ob_postgres.exceptions import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

# PEP 249 module-level constants
apilevel = "2.0"
threadsafety = 1  # threads may share the module but not connections
paramstyle = "format"  # psycopg2 uses %s placeholders


def connect(
    dsn: str | None = None,
    *,
    host: str = "localhost",
    port: int = 5432,
    dbname: str = "postgres",
    user: str | None = None,
    password: str | None = None,
    sslmode: str | None = None,
    options: dict[str, Any] | None = None,
    # OrionBelt parameters
    ob_api_url: str = "http://localhost:8000",
    ob_timeout: int = 30,
) -> Connection:
    """Open a PostgreSQL connection with OBML support.

    Parameters
    ----------
    dsn : str, optional
        Full libpq connection string (overrides individual params).
    host : str
        PostgreSQL host (default: ``localhost``).
    port : int
        PostgreSQL port (default: ``5432``).
    dbname : str
        Database name (default: ``postgres``).
    user : str, optional
        Username.
    password : str, optional
        Password.
    sslmode : str, optional
        SSL mode (``disable``, ``require``, ``verify-full``, etc.).
    options : dict, optional
        Extra libpq connection options.
    ob_api_url : str
        OrionBelt REST API URL (must be running in single-model mode).
    ob_timeout : int
        HTTP timeout in seconds for OBML compilation.
    """
    connect_kwargs: dict[str, Any] = {}
    if dsn is not None:
        connect_kwargs["dsn"] = dsn
    else:
        connect_kwargs["host"] = host
        connect_kwargs["port"] = port
        connect_kwargs["dbname"] = dbname
        if user is not None:
            connect_kwargs["user"] = user
        if password is not None:
            connect_kwargs["password"] = password
        if sslmode is not None:
            connect_kwargs["sslmode"] = sslmode
    if options:
        connect_kwargs["options"] = options

    native = psycopg2.connect(**connect_kwargs)
    return Connection(
        native,
        ob_api_url=ob_api_url,
        ob_timeout=ob_timeout,
    )


__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
