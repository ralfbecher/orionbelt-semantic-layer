"""Middleware: tenant isolation, auth, rate limiting, tracing."""

from __future__ import annotations

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

# Body size limits
_MODEL_PATHS = ("/models", "/validate")
_MAX_BODY_MODEL = 5 * 1024 * 1024  # 5 MB for model load/validate
_MAX_BODY_DEFAULT = 1 * 1024 * 1024  # 1 MB for everything else


class RequestTimingMiddleware(BaseHTTPMiddleware):
    """Add X-Request-Duration header with processing time."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        duration_ms = (time.monotonic() - start) * 1000
        response.headers["X-Request-Duration-Ms"] = f"{duration_ms:.1f}"
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add standard security headers to every response."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'"
        return response


class RequestBodyLimitMiddleware(BaseHTTPMiddleware):
    """Reject request bodies that exceed size limits.

    Model load and validate endpoints allow up to 5 MB; all other
    endpoints are capped at 1 MB.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        limit = _MAX_BODY_MODEL if path.endswith(_MODEL_PATHS) else _MAX_BODY_DEFAULT

        content_length = request.headers.get("content-length")
        if content_length is not None and int(content_length) > limit:
            return JSONResponse(
                status_code=413,
                content={"detail": f"Request body too large (max {limit // (1024 * 1024)} MB)"},
            )

        return await call_next(request)
