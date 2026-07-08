from __future__ import annotations

from fastapi import Header, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import settings
from app.core.errors import AppDependencyError
from app.core.security import constant_time_equals
from app.shared.schemas.common import ErrorDetail, ErrorResponse


def get_database(request: Request) -> AsyncIOMotorClient:
    client = getattr(request.app.state, "mongodb_client", None)
    if client is None:
        raise AppDependencyError("Mongo client not initialized")
    return client


def get_mongo_database(request: Request) -> AsyncIOMotorDatabase:
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
        payload = ErrorResponse(error=ErrorDetail(code=misconfig_code, message=misconfig_message, retryable=False))
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=payload.model_dump())
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token or not constant_time_equals(token, configured):
        payload = ErrorResponse(error=ErrorDetail(code="UNAUTHORIZED", message="Missing or invalid service authorization.", retryable=False))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=payload.model_dump())

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
