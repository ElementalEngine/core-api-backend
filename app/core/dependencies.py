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


def require_service_token(authorization: str | None = Header(default=None)) -> None:
    configured = settings.auth_service_token.get_secret_value()
    if not configured:
        payload = ErrorResponse(error=ErrorDetail(code="AUTH_SERVICE_MISCONFIGURED", message="Auth service token is not configured on the backend.", retryable=False))
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=payload.model_dump())
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token or not constant_time_equals(token, configured):
        payload = ErrorResponse(error=ErrorDetail(code="UNAUTHORIZED", message="Missing or invalid service authorization.", retryable=False))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=payload.model_dump())
