"""A ceiling on the website's reads. Its one caller makes a handful of calls
per build, so far more than that is a misuse or a leaked token."""

from __future__ import annotations

import time
from collections import deque

from app.core.config import settings
from app.core.errors import api_error

WINDOW_SECONDS = 60.0
_calls: deque[float] = deque()


async def within_limit() -> None:
    """Refuses a call once the last minute holds the setting's number of calls.
    Async, so it runs on the event loop and two calls never count at once."""
    now = time.monotonic()
    while _calls and now - _calls[0] >= WINDOW_SECONDS:
        _calls.popleft()
    if len(_calls) >= settings.public_requests_per_minute:
        raise api_error(
            code="RATE_LIMITED",
            message="Too many requests. Try again in a minute.",
            status_code=429,
            retryable=True,
        )
    _calls.append(now)


__all__ = ["WINDOW_SECONDS", "within_limit"]
