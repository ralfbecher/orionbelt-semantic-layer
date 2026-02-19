"""FastAPI application factory for OrionBelt Semantic Layer."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from orionbelt import __version__
from orionbelt.api.deps import init_session_manager, reset_session_manager
from orionbelt.api.middleware import RequestTimingMiddleware
from orionbelt.api.routers import dialects, sessions
from orionbelt.api.schemas import HealthResponse
from orionbelt.service.session_manager import SessionManager
from orionbelt.settings import Settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Start/stop the SessionManager alongside the application."""
    settings: Settings = app.state.settings
    mgr = SessionManager(
        ttl_seconds=settings.session_ttl_seconds,
        cleanup_interval=settings.session_cleanup_interval,
    )
    mgr.start()
    init_session_manager(mgr)
    try:
        yield
    finally:
        mgr.stop()
        reset_session_manager()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if settings is None:
        settings = Settings()

    app = FastAPI(
        title="OrionBelt Semantic Layer",
        description="Compiles YAML semantic models into analytical SQL across multiple dialects.",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings

    # Middleware
    app.add_middleware(RequestTimingMiddleware)

    # Session-scoped endpoints
    app.include_router(sessions.router, prefix="/sessions", tags=["sessions"])

    app.include_router(dialects.router, prefix="/dialects", tags=["dialects"])

    @app.get("/health", response_model=HealthResponse, tags=["health"])
    async def health() -> HealthResponse:
        return HealthResponse(status="ok", version=__version__)

    return app


def main() -> None:
    """Run the REST API server using settings from environment / .env file."""
    settings = Settings()

    logging.basicConfig(level=settings.log_level.upper())
    logger = logging.getLogger("orionbelt.api")
    logger.info(
        "OrionBelt API Server v%s starting (host=%s, port=%d)",
        __version__, settings.api_server_host, settings.api_server_port,
    )

    uvicorn.run(
        "orionbelt.api.app:create_app",
        factory=True,
        host=settings.api_server_host,
        port=settings.api_server_port,
        log_level=settings.log_level.lower(),
    )
