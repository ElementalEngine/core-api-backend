from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from app.api.health import readyz


class _Admin:
    async def command(self, name: str) -> dict[str, int]:
        await asyncio.sleep(0.01)
        return {"ok": 1}


class _Client:
    admin = _Admin()


def test_readyz_times_the_database_ping():
    body = asyncio.run(readyz(_Client()))  # type: ignore[arg-type]
    assert body == {"status": "ready", "db_ms": body["db_ms"]}
    assert body["db_ms"] >= 10


class _Down:
    class admin:
        @staticmethod
        async def command(name: str) -> dict[str, int]:
            raise RuntimeError("mongodb://user:secret@cluster-dev.example/?x")


def test_a_database_that_does_not_answer_is_a_retryable_503_without_its_error_text():
    with pytest.raises(HTTPException) as caught:
        asyncio.run(readyz(_Down()))  # type: ignore[arg-type]
    error = caught.value.detail["error"]
    assert (caught.value.status_code, error["code"], error["retryable"]) == (
        503,
        "UNAVAILABLE",
        True,
    )
    assert "secret" not in str(caught.value.detail)
