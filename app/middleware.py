"""
Timing middleware — logs every request with response time.
Add to main.py:

    from app.middleware import TimingMiddleware
    app.add_middleware(TimingMiddleware)
"""

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

log = logging.getLogger(__name__)


class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000

        # Color-code by speed: fast=OK, medium=WARN, slow=ERROR
        if elapsed_ms < 200:
            log.info("%-6.0f ms  %s %s", elapsed_ms, request.method, request.url.path)
        elif elapsed_ms < 1000:
            log.warning(
                "%-6.0f ms  %s %s", elapsed_ms, request.method, request.url.path
            )
        else:
            log.error(
                "%-6.0f ms  %s %s  ← SLOW", elapsed_ms, request.method, request.url.path
            )

        # Add timing header so you can see it in browser DevTools Network tab
        response.headers["X-Response-Time"] = f"{elapsed_ms:.0f}ms"
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add conservative browser security headers without breaking the embeddable widget."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault(
            "Permissions-Policy",
            "camera=(), microphone=(), geolocation=(self)",
        )
        return response
