"""D92's catch-all: an unhandled error is INTERNAL/500, never a 503 costume.

503 says "transient, try again". A bug is not transient, and the current
handlers that dress one as 503 invite a client to retry it forever. The
correlation id is what makes the 500 traceable without leaking str(exc).

The structural test always runs; the functional one needs httpx.

What should break this file: returning str(exc) to the client, marking
INTERNAL retryable, or dropping the middleware that mints the id.
"""

from __future__ import annotations

import logging

import pytest

from app.core.errors import INTERNAL_MESSAGE, unhandled_exception_handler
from app.core.middleware import CorrelationIdMiddleware, new_correlation_id


def test_main_registers_the_catch_all_and_the_middleware():
    from app.main import app

    assert app.exception_handlers.get(Exception) is unhandled_exception_handler
    assert any(m.cls is CorrelationIdMiddleware for m in app.user_middleware)


def test_correlation_ids_are_short_and_distinct():
    ids = {new_correlation_id() for _ in range(500)}
    assert len(ids) == 500
    assert all(len(i) == 12 for i in ids)


def _client():
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.add_exception_handler(Exception, unhandled_exception_handler)  # type: ignore[arg-type]
    app.add_middleware(CorrelationIdMiddleware)

    @app.get("/boom")
    async def boom():
        raise RuntimeError("mongo password is hunter2")

    return TestClient(app, raise_server_exceptions=False)


def test_unhandled_error_returns_the_internal_envelope():
    res = _client().get("/boom")

    assert res.status_code == 500
    error = res.json()["detail"]["error"]
    assert error["code"] == "INTERNAL"
    assert error["retryable"] is False
    assert error["message"] == INTERNAL_MESSAGE
    assert len(error["correlation_id"]) == 12


def test_the_exception_text_never_reaches_the_client():
    assert "hunter2" not in _client().get("/boom").text


def test_the_same_id_is_returned_and_logged(caplog):
    with caplog.at_level(logging.ERROR, logger="app.core.errors"):
        returned = _client().get("/boom").json()["detail"]["error"]["correlation_id"]

    logged = [r for r in caplog.records if "Unhandled error" in r.getMessage()]
    assert len(logged) == 1
    assert f"correlation_id={returned}" in logged[0].getMessage()
