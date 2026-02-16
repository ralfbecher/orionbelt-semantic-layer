"""Dependency injection for FastAPI — SessionManager singleton."""

from __future__ import annotations

from orionbelt.service.session_manager import SessionManager

_session_manager: SessionManager | None = None


def init_session_manager(manager: SessionManager) -> None:
    """Set the global SessionManager (called at app startup)."""
    global _session_manager  # noqa: PLW0603
    _session_manager = manager


def get_session_manager() -> SessionManager:
    """FastAPI ``Depends`` provider for SessionManager."""
    if _session_manager is None:
        raise RuntimeError("SessionManager not initialised — call init_session_manager() first")
    return _session_manager


def reset_session_manager() -> None:
    """Clear the global SessionManager (for tests)."""
    global _session_manager  # noqa: PLW0603
    _session_manager = None
