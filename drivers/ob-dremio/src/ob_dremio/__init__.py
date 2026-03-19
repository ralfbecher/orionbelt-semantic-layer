"""ob-dremio — OrionBelt Semantic Layer driver for Dremio (PEP 249 DB-API 2.0).

Requires the OrionBelt REST API running in single-model mode (MODEL_FILE set).
OBML queries are compiled to SQL via ``POST /v1/query/sql``.

Dremio is accessed via Arrow Flight protocol using ``pyarrow.flight``.

Usage::

    import ob_dremio

    conn = ob_dremio.connect(host="localhost", username="user", password="pass")
    with conn.cursor() as cur:
        cur.execute("select:\\n  dimensions:\\n    - Region\\n  measures:\\n    - Revenue")
        print(cur.fetchall())
"""

from __future__ import annotations

from ob_dremio.connection import Connection
from ob_dremio.exceptions import (
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
paramstyle = "qmark"  # Dremio SQL uses ? placeholders


def connect(
    *,
    host: str = "localhost",
    port: int = 32010,
    username: str | None = None,
    password: str | None = None,
    tls: bool = False,
    # OrionBelt parameters
    ob_api_url: str = "http://localhost:8000",
    ob_timeout: int = 30,
) -> Connection:
    """Open a Dremio connection via Arrow Flight with OBML support.

    Parameters
    ----------
    host : str
        Dremio host (default: ``localhost``).
    port : int
        Arrow Flight port (default: ``32010``).
    username : str, optional
        Dremio username for authentication.
    password : str, optional
        Dremio password for authentication.
    tls : bool
        Use TLS for the Flight connection (default: ``False``).
    ob_api_url : str
        OrionBelt REST API URL (must be running in single-model mode).
    ob_timeout : int
        HTTP timeout in seconds for OBML compilation.
    """
    import pyarrow.flight

    scheme = "grpc+tls" if tls else "grpc"
    location = f"{scheme}://{host}:{port}"
    client = pyarrow.flight.FlightClient(location)

    if username is not None and password is not None:
        token_pair = client.authenticate_basic_token(username, password)
        # Store the bearer token as default call options via a header factory.
        # pyarrow Flight uses call options for auth headers on each RPC.
        options = pyarrow.flight.FlightCallOptions(
            headers=[token_pair],
        )
        # Wrap the client so that default options are applied.
        # For simplicity, we store the options on the client object.
        client._ob_call_options = options  # type: ignore[attr-defined]

    return Connection(
        client,
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
