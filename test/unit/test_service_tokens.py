from __future__ import annotations

import pytest
from fastapi import HTTPException
from pydantic import SecretStr

from app.core import dependencies
from app.core.dependencies import require_any_service_token, require_mito_token
from app.features.civdata.router import router as civ_data_router

MITO = "mito-test-token"
LJ = "lj-test-token"
AUTH = "auth-test-token"


def _set_tokens(monkeypatch, mito=MITO, lj=LJ, auth=AUTH) -> None:
    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(mito))
    monkeypatch.setattr(dependencies.settings, "lj_service_token", SecretStr(lj))
    monkeypatch.setattr(dependencies.settings, "auth_service_token", SecretStr(auth))


@pytest.mark.parametrize("token", [MITO, LJ, AUTH])
def test_every_configured_service_token_is_admitted(monkeypatch, token):
    _set_tokens(monkeypatch)
    assert require_any_service_token(authorization=f"Bearer {token}") is None


@pytest.mark.parametrize("gate", [require_any_service_token, require_mito_token])
@pytest.mark.parametrize(
    "authorization",
    [None, "", "Bearer", "Bearer ", "Bearer wrong-token", f"Basic {MITO}", MITO],
)
def test_a_missing_or_wrong_bearer_answers_401(monkeypatch, gate, authorization):
    _set_tokens(monkeypatch)
    with pytest.raises(HTTPException) as exc_info:
        gate(authorization=authorization)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["error"]["code"] == "UNAUTHORIZED"


def test_no_token_configured_answers_503(monkeypatch):
    _set_tokens(monkeypatch, mito="", lj="", auth="")
    with pytest.raises(HTTPException) as exc_info:
        require_any_service_token(authorization=f"Bearer {MITO}")
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["error"]["code"] == "SERVICE_TOKENS_MISCONFIGURED"


def test_gate_runs_before_the_edition_is_validated(monkeypatch):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    _set_tokens(monkeypatch)
    app = FastAPI()
    app.include_router(civ_data_router)
    client = TestClient(app, raise_server_exceptions=False)

    denied = client.get("/api/v2/civ-data/civ6")
    assert denied.status_code == 401
    assert denied.json()["detail"]["error"]["code"] == "UNAUTHORIZED"

    # An unknown edition is still 401, not 422: a caller without a token
    # learns nothing about which editions exist.
    assert client.get("/api/v2/civ-data/civ5").status_code == 401

    # With a token the gate admits the request; it then fails on the missing
    # Mongo dependency, which is what proves it got past the gate.
    admitted = client.get(
        "/api/v2/civ-data/civ6", headers={"authorization": f"Bearer {LJ}"}
    )
    assert admitted.status_code != 401


def _set_mito(monkeypatch, value: str) -> None:
    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(value))


def test_unconfigured_token_answers_503_misconfigured(monkeypatch):
    _set_mito(monkeypatch, "")
    with pytest.raises(HTTPException) as exc_info:
        require_mito_token(authorization=f"Bearer {MITO}")
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["error"]["code"] == "MITO_SERVICE_MISCONFIGURED"


def test_valid_bearer_passes(monkeypatch):
    _set_mito(monkeypatch, MITO)
    assert require_mito_token(authorization=f"Bearer {MITO}") is None
