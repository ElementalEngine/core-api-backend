"""The website's answers, kept for a few minutes. The site rebuilds once a day
and reads each answer once, so a repeat inside the window costs no database
work, however many calls arrive."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

from app.core.config import settings

_answers: dict[tuple[str, ...], tuple[float, Any]] = {}


async def cached[T](key: tuple[str, ...], compute: Callable[[], Awaitable[T]]) -> T:
    """The answer stored under the key if it is younger than the setting, else
    a fresh one. A failure is never stored."""
    now = time.monotonic()
    hit = _answers.get(key)
    if hit is not None and now - hit[0] < settings.public_cache_seconds:
        answer: T = hit[1]
        return answer
    fresh = await compute()
    _answers[key] = (now, fresh)
    return fresh


__all__ = ["cached"]
