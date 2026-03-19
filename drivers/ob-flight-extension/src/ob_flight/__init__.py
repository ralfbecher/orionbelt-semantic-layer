"""ob-flight-extension — Arrow Flight SQL server for OrionBelt Semantic Layer.

Adds Arrow Flight SQL protocol to the orionbelt-api FastAPI server.
Runs as a daemon thread in the same process — no HTTP hop for compilation.

Usage: Set FLIGHT_ENABLED=true when starting orionbelt-api.

Note: server and startup are NOT imported at package level to avoid
pulling in pyarrow.flight (heavy gRPC init) when only db_router is needed.
Import them explicitly: ``from ob_flight.startup import start_flight_background``
"""

from __future__ import annotations

__all__ = [
    "OBFlightServer",
    "start_flight_background",
    "stop_flight_server",
]


def __getattr__(name: str) -> object:
    """Lazy imports for server and startup — avoids pyarrow.flight at import time."""
    if name == "OBFlightServer":
        from ob_flight.server import OBFlightServer

        return OBFlightServer
    if name == "start_flight_background":
        from ob_flight.startup import start_flight_background

        return start_flight_background
    if name == "stop_flight_server":
        from ob_flight.startup import stop_flight_server

        return stop_flight_server
    raise AttributeError(f"module 'ob_flight' has no attribute {name!r}")
