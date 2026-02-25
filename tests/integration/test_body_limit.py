"""Regression tests for RequestBodyLimitMiddleware."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from orionbelt.api.app import create_app
from orionbelt.api.deps import init_session_manager, reset_session_manager
from orionbelt.service.session_manager import SessionManager
from orionbelt.settings import Settings


@pytest.fixture
def app():
    settings = Settings(session_ttl_seconds=3600, session_cleanup_interval=9999)
    application = create_app(settings=settings)
    mgr = SessionManager(
        ttl_seconds=settings.session_ttl_seconds,
        cleanup_interval=settings.session_cleanup_interval,
    )
    init_session_manager(mgr)
    yield application
    reset_session_manager()


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestInvalidContentLength:
    """Non-integer Content-Length must not cause a 500."""

    async def test_non_integer_content_length(self, client: AsyncClient) -> None:
        """Invalid Content-Length (non-int) falls through to streaming, not 500."""
        response = await client.post(
            "/health",
            content=b"small body",
            headers={"content-length": "not-a-number"},
        )
        # Should NOT be 500 — the request either succeeds or gets a 4xx
        assert response.status_code != 500

    async def test_negative_content_length(self, client: AsyncClient) -> None:
        response = await client.post(
            "/health",
            content=b"small body",
            headers={"content-length": "-1"},
        )
        assert response.status_code != 500


class TestChunkedOverLimit:
    """Chunked (no Content-Length) body over limit is still rejected."""

    async def test_chunked_body_over_default_limit(self, client: AsyncClient) -> None:
        """Body exceeding 1 MB default limit without Content-Length header."""
        oversized = b"x" * (1 * 1024 * 1024 + 1)
        response = await client.post(
            "/sessions",
            content=oversized,
            headers={"transfer-encoding": "chunked"},
        )
        assert response.status_code == 413
        assert "too large" in response.json()["detail"]
