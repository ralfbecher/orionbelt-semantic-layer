"""OrionBelt compilation bridge for DB-API 2.0 drivers.

Compiles OBML query dicts to SQL by calling the OrionBelt REST API.
Assumes the API is running in single-model mode (MODEL_FILE set).
Uses the ``/v1/query/sql`` shortcut endpoint — no session or model ID required.
"""

from __future__ import annotations

import logging
from typing import Any

from ob_driver_core.exceptions import OperationalError, ProgrammingError

logger = logging.getLogger(__name__)


def compile_obml(
    obml: dict[str, Any],
    *,
    dialect: str,
    ob_api_url: str = "http://localhost:8000",
    ob_timeout: int = 30,
) -> str:
    """Compile an OBML query dict to SQL via the OrionBelt REST API.

    Calls ``POST /v1/query/sql?dialect=...`` which auto-resolves the
    single session and model in single-model mode.
    """
    import httpx

    url = f"{ob_api_url.rstrip('/')}/v1/query/sql"
    try:
        response = httpx.post(
            url,
            params={"dialect": dialect},
            json=obml,
            timeout=ob_timeout,
        )
    except httpx.ConnectError as exc:
        raise OperationalError(f"OB API unavailable at {ob_api_url}: {exc}") from exc

    if not response.is_success:
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text
        if response.status_code < 500:
            raise ProgrammingError(f"OB compile error: {detail}")
        raise OperationalError(f"OB API error: {detail}")

    return response.json()["sql"]  # type: ignore[no-any-return]
