"""Unit tests for the Mito service-token dependency"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from pydantic import SecretStr

from app.core import dependencies
from app.core.dependencies import require_mito_token

TOKEN = "mito-test-token"


def _set_token(monkeypatch, value: str) -> None:
    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(value))


def test_unconfigured_token_answers_503_misconfigured(monkeypatch):
    _set_token(monkeypatch, "")
    with pytest.raises(HTTPException) as exc_info:
        require_mito_token(authorization=f"Bearer {TOKEN}")
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["error"]["code"] == "MITO_SERVICE_MISCONFIGURED"


def test_valid_bearer_passes(monkeypatch):
    _set_token(monkeypatch, TOKEN)
    assert require_mito_token(authorization=f"Bearer {TOKEN}") is None


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "",
        "Bearer",
        "Bearer ",
        "Bearer wrong-token",
        f"Basic {TOKEN}",
        TOKEN,  # missing scheme
    ],
)
def test_missing_or_invalid_bearer_answers_401(monkeypatch, authorization):
    _set_token(monkeypatch, TOKEN)
    with pytest.raises(HTTPException) as exc_info:
        require_mito_token(authorization=authorization)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["error"]["code"] == "UNAUTHORIZED"
