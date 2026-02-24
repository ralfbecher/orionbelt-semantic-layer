"""Dependency injection for FastAPI — SessionManager singleton."""

from __future__ import annotations

from orionbelt.service.session_manager import SessionManager

_session_manager: SessionManager | None = None
_disable_session_list: bool = False


def init_session_manager(
    manager: SessionManager, *, disable_session_list: bool = False
) -> None:
    """Set the global SessionManager (called at app startup)."""
    global _session_manager, _disable_session_list  # noqa: PLW0603
    _session_manager = manager
    _disable_session_list = disable_session_list


def get_session_manager() -> SessionManager:
    """FastAPI ``Depends`` provider for SessionManager."""
    if _session_manager is None:
        raise RuntimeError("SessionManager not initialised — call init_session_manager() first")
    return _session_manager


def is_session_list_disabled() -> bool:
    """Return True when the GET /sessions endpoint is suppressed."""
    return _disable_session_list


def reset_session_manager() -> None:
    """Clear the global SessionManager (for tests)."""
    global _session_manager, _disable_session_list  # noqa: PLW0603
    _session_manager = None
    _disable_session_list = False
