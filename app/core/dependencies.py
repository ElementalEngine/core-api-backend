from __future__ import annotations

from fastapi import Header, HTTPException, Request, status
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from app.core.config import settings
from app.core.errors import AppDependencyError, ErrorDetail, ErrorResponse
from app.core.security import constant_time_equals


def get_database(request: Request) -> AsyncMongoClient:
    client = getattr(request.app.state, "mongodb_client", None)
    if client is None:
        raise AppDependencyError("Mongo client not initialized")
    return client


def get_mongo_database(request: Request) -> AsyncDatabase:
    database = getattr(request.app.state, "mongodb", None)
    if database is None:
        raise AppDependencyError("Mongo database not initialized")
    return database


def _require_bearer(
    authorization: str | None,
    *,
    configured: str,
    misconfig_code: str,
    misconfig_message: str,
) -> None:
    if not configured:
        payload = ErrorResponse(
            error=ErrorDetail(
                code=misconfig_code, message=misconfig_message, retryable=False
            )
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=payload.model_dump()
        )
    scheme, _, token = (authorization or "").partition(" ")
    if (
        scheme.lower() != "bearer"
        or not token
        or not constant_time_equals(token, configured)
    ):
        payload = ErrorResponse(
            error=ErrorDetail(
                code="UNAUTHORIZED",
                message="Missing or invalid service authorization.",
                retryable=False,
            )
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=payload.model_dump()
        )


def require_lj_token(authorization: str | None = Header(default=None)) -> None:
    _require_bearer(
        authorization,
        configured=settings.lj_service_token.get_secret_value(),
        misconfig_code="LJ_SERVICE_MISCONFIGURED",
        misconfig_message="LJ service token is not configured on the backend.",
    )


def require_service_token(authorization: str | None = Header(default=None)) -> None:
    _require_bearer(
        authorization,
        configured=settings.auth_service_token.get_secret_value(),
        misconfig_code="AUTH_SERVICE_MISCONFIGURED",
        misconfig_message="Auth service token is not configured on the backend.",
    )


def require_mito_token(authorization: str | None = Header(default=None)) -> None:
    _require_bearer(
        authorization,
        configured=settings.mito_service_token.get_secret_value(),
        misconfig_code="MITO_SERVICE_MISCONFIGURED",
        misconfig_message="Mito service token is not configured on the backend.",
    )


def require_activity_token(authorization: str | None = Header(default=None)) -> None:
    """The Activity server's own credential, separate from Mito's by D94.

    These seven routes submit picks, cancel a draft, and read any lobby AS ANY
    ACTOR through D73's per-caller censoring. A shared gate would hand Mite
    that reach and the Activity the ability to claim posts -- strictly wider
    than D17 locked. Not the same case as `require_any_service_token`, which
    widens only over read-only reference data (D96).

    Unset until S10 provisions the credential, in which case these routes
    answer 503 rather than admitting anyone else's token.
    """
    _require_bearer(
        authorization,
        configured=settings.activity_service_token.get_secret_value(),
        misconfig_code="ACTIVITY_SERVICE_MISCONFIGURED",
        misconfig_message="Activity service token is not configured on the backend.",
    )


def require_any_service_token(authorization: str | None = Header(default=None)) -> None:
    """Accept any configured service token.

    Read-only reference data every consumer needs identically, so there is no
    authority to leak by widening the gate (D96). Naming one bot's token here
    would mean re-editing it for each consumer that legitimately reads it.

    The Activity is admitted here (C8) because it drafts from the leader and
    civ tables. That is the whole of what widening buys it -- the lobby routes
    take their own gate, per D94.
    """
    configured = [
        settings.mito_service_token.get_secret_value(),
        settings.lj_service_token.get_secret_value(),
        settings.auth_service_token.get_secret_value(),
        settings.activity_service_token.get_secret_value(),
    ]
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() == "bearer" and token:
        for candidate in configured:
            if candidate and constant_time_equals(token, candidate):
                return
    if not any(configured):
        payload = ErrorResponse(
            error=ErrorDetail(
                code="SERVICE_TOKENS_MISCONFIGURED",
                message="No service token is configured on the backend.",
                retryable=False,
            )
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=payload.model_dump()
        )
    payload = ErrorResponse(
        error=ErrorDetail(
            code="UNAUTHORIZED",
            message="Missing or invalid service authorization.",
            retryable=False,
        )
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail=payload.model_dump()
    )
