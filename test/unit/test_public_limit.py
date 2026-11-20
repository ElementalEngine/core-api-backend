from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from app.core.config import settings
from app.features.public import limit
from app.features.public.router import router


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(settings, "public_requests_per_minute", 3)
    limit._calls.clear()
    yield
    limit._calls.clear()


def _call() -> int | None:
    try:
        asyncio.run(limit.within_limit())
    except HTTPException as exc:
        return exc.status_code
    return None


def test_calls_up_to_the_setting_pass_and_the_next_is_refused():
    assert [_call() for _ in range(4)] == [None, None, None, 429]


def test_the_refusal_is_retryable_in_the_error_envelope():
    for _ in range(3):
        _call()
    with pytest.raises(HTTPException) as exc:
        asyncio.run(limit.within_limit())
    error = exc.value.detail["error"]
    assert (error["code"], error["retryable"]) == ("RATE_LIMITED", True)


def test_calls_older_than_the_window_no_longer_count(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(limit.time, "monotonic", lambda: clock[0])
    for _ in range(3):
        _call()
    clock[0] += limit.WINDOW_SECONDS
    assert _call() is None


def test_the_token_is_checked_before_a_call_is_counted():
    gates = [d.dependency.__name__ for d in router.dependencies]
    assert gates == ["require_website_token", "within_limit"]
