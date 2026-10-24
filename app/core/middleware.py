"""Sets a correlation id before any handler runs, so an INTERNAL error can
return it to the caller and log it on the same line."""

from __future__ import annotations

from uuid import uuid4

from starlette.types import ASGIApp, Receive, Scope, Send


def new_correlation_id() -> str:
    """Short enough to read back over voice, wide enough not to collide."""
    return uuid4().hex[:12]


class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            # Request.state reads scope["state"]; setdefault matches how
            # Starlette itself initialises it.
            scope.setdefault("state", {})["correlation_id"] = new_correlation_id()
        await self.app(scope, receive, send)


__all__ = ["CorrelationIdMiddleware", "new_correlation_id"]
