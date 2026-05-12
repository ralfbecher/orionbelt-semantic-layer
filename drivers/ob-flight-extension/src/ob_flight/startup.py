"""Flight server lifecycle management — daemon thread startup/shutdown."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger("ob_flight.startup")

_server: Any = None
_thread: threading.Thread | None = None


def _env_flag(name: str) -> bool:
    """Read a boolean env var (case-insensitive 'true'/'1')."""
    return os.getenv(name, "").lower() in ("1", "true", "yes")


def start_flight_background(
    *,
    session_manager: Any = None,
    port: int | None = None,
    auth_handler: Any = None,
    default_dialect: str | None = None,
    allow_raw_sql: bool | None = None,
    allow_data_object_sql: bool | None = None,
) -> threading.Thread:
    """Launch the Flight SQL server in a daemon thread.

    Governance defaults follow ``design/PLAN_flight_natural_sql.md``: raw
    SQL pass-through and FROM-<data-object-label> are both off by default.
    Operators opt in via ``FLIGHT_ALLOW_RAW_SQL`` /
    ``FLIGHT_ALLOW_DATA_OBJECT_SQL`` (or the explicit kwargs).
    """
    global _server, _thread

    from ob_flight.server import OBFlightServer

    if auth_handler is None:
        from ob_flight.auth import create_auth_handler

        auth_handler = create_auth_handler()

    if port is None:
        port = int(os.getenv("FLIGHT_PORT", "8815"))

    if default_dialect is None:
        default_dialect = os.getenv("DB_VENDOR", "duckdb")
    location = f"grpc://0.0.0.0:{port}"

    if allow_raw_sql is None:
        allow_raw_sql = _env_flag("FLIGHT_ALLOW_RAW_SQL")
    if allow_data_object_sql is None:
        allow_data_object_sql = _env_flag("FLIGHT_ALLOW_DATA_OBJECT_SQL")

    _server = OBFlightServer(
        location,
        auth_handler=auth_handler,
        session_manager=session_manager,
        default_dialect=default_dialect,
        allow_raw_sql=allow_raw_sql,
        allow_data_object_sql=allow_data_object_sql,
    )

    _thread = threading.Thread(
        target=_server.serve,
        name="ob-flight-server",
        daemon=True,
    )
    _thread.start()
    logger.info("Flight SQL server started on port %d (dialect=%s)", port, default_dialect)
    return _thread


def stop_flight_server() -> None:
    """Shutdown the Flight server, with a timeout to avoid blocking forever.

    pyarrow's FlightServerBase.shutdown() blocks until all in-flight requests
    finish.  DBeaver (and other JDBC clients) keep idle connections open, so
    shutdown() can hang indefinitely.  We call it from a helper thread and
    join with a short timeout so the main process can exit cleanly.
    """
    global _server, _thread
    if _server is not None:
        server = _server
        # Call shutdown() from a helper thread — it blocks and we don't want
        # to stall the FastAPI lifespan finalizer.
        shutdown_thread = threading.Thread(target=_shutdown_safely, args=(server,), daemon=True)
        shutdown_thread.start()
        shutdown_thread.join(timeout=3)
        # Wait for the serve() thread to finish too
        if _thread is not None and _thread.is_alive():
            _thread.join(timeout=2)
        _server = None
        _thread = None
        logger.info("Flight SQL server stopped")


def _shutdown_safely(server: Any) -> None:
    """Call server.shutdown() in a thread-safe way."""
    try:
        server.shutdown()
    except Exception:
        pass
