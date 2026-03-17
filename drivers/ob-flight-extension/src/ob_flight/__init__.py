"""ob-flight-extension — Arrow Flight SQL server for OrionBelt Semantic Layer.

Adds Arrow Flight SQL protocol to the orionbelt-api FastAPI server.
Runs as a daemon thread in the same process — no HTTP hop for compilation.

Usage: Set FLIGHT_ENABLED=true when starting orionbelt-api.
"""

from __future__ import annotations

from ob_flight.server import OBFlightServer
from ob_flight.startup import start_flight_background, stop_flight_server

__all__ = [
    "OBFlightServer",
    "start_flight_background",
    "stop_flight_server",
]
