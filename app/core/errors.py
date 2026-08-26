from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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
    return {"detail": payload.model_dump()}


def api_error(
    *, code: str, message: str, status_code: int, retryable: bool = False
) -> HTTPException:
    """Raise through D92's envelope. S6 uses three codes on /api/v2/matches;
    S7 adds the closed enum, the INTERNAL catch-all and correlation_id."""
    return HTTPException(
        status_code=status_code,
        detail=_error_envelope(code=code, message=message, retryable=retryable)[
            "detail"
        ],
    )


def invalid_request(message: str) -> HTTPException:
    return api_error(code="INVALID_REQUEST", message=message, status_code=400)


def forbidden(message: str) -> HTTPException:
    return api_error(code="FORBIDDEN", message=message, status_code=403)


def not_found(message: str) -> HTTPException:
    return api_error(code="NOT_FOUND", message=message, status_code=404)


async def request_validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=_error_envelope(
            code="INVALID_REQUEST",
            message="The request failed validation.",
            details={"errors": jsonable_encoder(exc.errors())},
            retryable=False,
        ),
    )


async def app_dependency_exception_handler(
    request: Request, exc: AppDependencyError
) -> JSONResponse:
    logger.error("App dependency unavailable: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=_error_envelope(
            code="UNAVAILABLE",
            message="A backend dependency is unavailable right now. Please try again.",
            retryable=True,
        ),
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """D92's catch-all: INTERNAL / 500 / retryable false, never 503.

    503 is reserved for Mongo unreachable and the dependency gate. A bug is
    not a transient fault, and dressing one as a retryable outage invites the
    client to retry it forever.
    """
    correlation_id = str(getattr(request.state, "correlation_id", "") or "")
    logger.exception(
        "Unhandled error correlation_id=%s method=%s path=%s",
        correlation_id,
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=_error_envelope(
            code="INTERNAL",
            message=INTERNAL_MESSAGE,
            retryable=False,
            correlation_id=correlation_id,
        ),
    )
