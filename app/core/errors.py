from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pymongo.errors import ConnectionFailure
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Any | None = None
    retryable: bool | None = None
    correlation_id: str | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


class AppDependencyError(RuntimeError):
    """Raised when core app state is unavailable."""


# Fixed, and never str(exc): an unhandled error must not leak its own text
# to a client. The correlation id is how it is traced instead.
INTERNAL_MESSAGE = (
    "Something went wrong on our side. Quote the correlation id if you report this."
)


def _error_envelope(
    *,
    code: str,
    message: str,
    details: Any | None = None,
    retryable: bool,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    payload = ErrorResponse(
        error=ErrorDetail(
            code=code,
            message=message,
            details=details,
            retryable=retryable,
            correlation_id=correlation_id,
        )
    )
    return payload.model_dump()


def api_error(
    *,
    code: str,
    message: str,
    status_code: int,
    retryable: bool = False,
    details: Any | None = None,
) -> HTTPException:
    """The error envelope: a closed set of codes, with INTERNAL and a correlation
    id for anything unhandled."""
    return HTTPException(
        status_code=status_code,
        detail=_error_envelope(
            code=code, message=message, details=details, retryable=retryable
        ),
    )


def invalid_request(message: str) -> HTTPException:
    return api_error(code="INVALID_REQUEST", message=message, status_code=400)


def forbidden(message: str) -> HTTPException:
    return api_error(code="FORBIDDEN", message=message, status_code=403)


def not_found(message: str) -> HTTPException:
    return api_error(code="NOT_FOUND", message=message, status_code=404)


def conflict(message: str, details: Any | None = None) -> HTTPException:
    """409 with code CONFLICT."""
    return api_error(code="CONFLICT", message=message, status_code=409, details=details)


async def request_validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "detail": _error_envelope(
                code="INVALID_REQUEST",
                message="The request failed validation.",
                details={"errors": jsonable_encoder(exc.errors())},
                retryable=False,
            )
        },
    )


# Codes for the errors the framework raises itself, such as an unknown route.
FRAMEWORK_CODES = {401: "UNAUTHORIZED", 403: "FORBIDDEN", 404: "NOT_FOUND"}


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """Our errors pass as raised; the framework's own get the envelope too."""
    detail: Any = exc.detail
    if not isinstance(detail, dict):
        detail = _error_envelope(
            code=FRAMEWORK_CODES.get(exc.status_code, "INVALID_REQUEST"),
            message=str(detail),
            retryable=False,
        )
    return JSONResponse(
        status_code=exc.status_code, content={"detail": detail}, headers=exc.headers
    )


async def app_dependency_exception_handler(
    request: Request, exc: AppDependencyError | ConnectionFailure
) -> JSONResponse:
    logger.error("App dependency unavailable: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "detail": _error_envelope(
                code="UNAVAILABLE",
                message="A backend dependency is unavailable right now. Please try again.",
                retryable=True,
            )
        },
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all: INTERNAL / 500 / retryable false, never 503."""
    correlation_id = str(getattr(request.state, "correlation_id", "") or "")
    logger.exception(
        "Unhandled error correlation_id=%s method=%s path=%s",
        correlation_id,
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": _error_envelope(
                code="INTERNAL",
                message=INTERNAL_MESSAGE,
                retryable=False,
                correlation_id=correlation_id,
            )
        },
    )
