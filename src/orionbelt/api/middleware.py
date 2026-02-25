"""Middleware: tenant isolation, auth, rate limiting, tracing."""

from __future__ import annotations

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

# Body size limits (also enforced by Cloud Armor — keep in sync with
# infra/apply-cloud-armor.sh rules 103/106)
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
        # Gradio UI requires inline scripts/styles and external fonts —
        # use a relaxed CSP for /ui paths, strict for API endpoints.
        if request.url.path.startswith("/ui"):
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; "
                "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com; "
                "connect-src 'self'; "
                "img-src 'self' data:; "
                "frame-ancestors 'none'"
            )
        else:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; frame-ancestors 'none'"
            )
        return response


class RequestBodyLimitMiddleware(BaseHTTPMiddleware):
    """Reject request bodies that exceed size limits.

    Model load and validate endpoints allow up to 5 MB; all other
    endpoints are capped at 1 MB.

    Two checks are performed:
    1. **Content-Length header** — cheap early rejection (also enforced at
       the Cloud Armor layer in front of the load balancer).
    2. **Streaming byte count** — reads the body via ``request.stream()``
       and aborts as soon as the limit is exceeded, avoiding buffering an
       arbitrarily large payload into memory.  The consumed bytes are
       cached on ``request._body`` so downstream handlers can still use
       ``await request.body()``.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        limit = _MAX_BODY_MODEL if path.endswith(_MODEL_PATHS) else _MAX_BODY_DEFAULT
        limit_mb = limit // (1024 * 1024)

        # Fast path: check Content-Length header first
        content_length = request.headers.get("content-length")
        if content_length is not None and int(content_length) > limit:
            return JSONResponse(
                status_code=413,
                content={"detail": f"Request body too large (max {limit_mb} MB)"},
            )

        # Stream actual bytes — abort early if limit exceeded
        if request.method in ("POST", "PUT", "PATCH"):
            chunks: list[bytes] = []
            total = 0
            async for chunk in request.stream():
                total += len(chunk)
                if total > limit:
                    return JSONResponse(
                        status_code=413,
                        content={
                            "detail": f"Request body too large (max {limit_mb} MB)"
                        },
                    )
                chunks.append(chunk)
            # Cache consumed body so downstream can call request.body()
            request._body = b"".join(chunks)  # noqa: SLF001

        return await call_next(request)
