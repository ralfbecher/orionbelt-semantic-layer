"""Flight server lifecycle management — daemon thread startup/shutdown."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger("ob_flight.startup")

_server: Any = None
_thread: threading.Thread | None = None


def start_flight_background(
    *,
    session_manager: Any = None,
    port: int | None = None,
    auth_handler: Any = None,
) -> threading.Thread:
    """Launch the Flight SQL server in a daemon thread.

    Parameters
    ----------
    session_manager : SessionManager
        The shared SessionManager from the FastAPI lifespan.
    port : int, optional
        gRPC port (default: FLIGHT_PORT env var or 8815).
    auth_handler : ServerAuthHandler, optional
        Auth handler (default: created from FLIGHT_AUTH_MODE env var).
    """
    global _server, _thread

    from ob_flight.server import OBFlightServer

    if auth_handler is None:
        from ob_flight.auth import create_auth_handler

        auth_handler = create_auth_handler()

    if port is None:
        port = int(os.getenv("FLIGHT_PORT", "8815"))

    default_dialect = os.getenv("DB_VENDOR", "duckdb")
    location = f"grpc://0.0.0.0:{port}"

    _server = OBFlightServer(
        location,
        auth_handler=auth_handler,
        session_manager=session_manager,
        default_dialect=default_dialect,
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
    """Shutdown the Flight server gracefully."""
    global _server, _thread
    if _server is not None:
        try:
            _server.shutdown()
        except Exception:
            pass  # server may already be stopped
        _server = None
        _thread = None
        logger.info("Flight SQL server stopped")
