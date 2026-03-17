"""Dependency injection for FastAPI — SessionManager singleton."""

from __future__ import annotations

from orionbelt.service.session_manager import SessionManager

_session_manager: SessionManager | None = None
_disable_session_list: bool = False
_single_model_mode: bool = False
_preload_model_yaml: str | None = None
_flight_info: dict[str, object] | None = None
_query_default_limit: int = 1000


def init_session_manager(
    manager: SessionManager,
    *,
    disable_session_list: bool = False,
    preload_model_yaml: str | None = None,
    flight_info: dict[str, object] | None = None,
    query_default_limit: int = 1000,
) -> None:
    """Set the global SessionManager (called at app startup)."""
    global _session_manager, _disable_session_list  # noqa: PLW0603
    global _single_model_mode, _preload_model_yaml, _flight_info  # noqa: PLW0603
    global _query_default_limit  # noqa: PLW0603
    _session_manager = manager
    _disable_session_list = disable_session_list
    _single_model_mode = preload_model_yaml is not None
    _preload_model_yaml = preload_model_yaml
    _flight_info = flight_info
    _query_default_limit = query_default_limit


def get_session_manager() -> SessionManager:
    """FastAPI ``Depends`` provider for SessionManager."""
    if _session_manager is None:
        raise RuntimeError("SessionManager not initialised — call init_session_manager() first")
    return _session_manager


def is_session_list_disabled() -> bool:
    """Return True when the GET /sessions endpoint is suppressed."""
    return _disable_session_list


def is_single_model_mode() -> bool:
    """Return True when a MODEL_FILE is configured (no model upload/removal)."""
    return _single_model_mode


def get_preload_model_yaml() -> str | None:
    """Return the OBML YAML to pre-load into new sessions, or None."""
    return _preload_model_yaml


def get_flight_info() -> dict[str, object] | None:
    """Return Flight SQL settings dict, or None if Flight is not enabled."""
    return _flight_info


def get_query_default_limit() -> int:
    """Return the default row limit for query execution."""
    return _query_default_limit


def reset_session_manager() -> None:
    """Clear the global SessionManager (for tests)."""
    global _session_manager, _disable_session_list  # noqa: PLW0603
    global _single_model_mode, _preload_model_yaml, _flight_info  # noqa: PLW0603
    global _query_default_limit  # noqa: PLW0603
    _session_manager = None
    _disable_session_list = False
    _single_model_mode = False
    _preload_model_yaml = None
    _flight_info = None
    _query_default_limit = 1000
