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


def _error_envelope(
    *,
    code: str,
    message: str,
    details: Any | None = None,
    retryable: bool,
) -> dict[str, Any]:
    payload = ErrorResponse(
        error=ErrorDetail(
            code=code,
            message=message,
            details=details,
            retryable=retryable,
            correlation_id=None,
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


async def request_validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=_error_envelope(
            code="VALIDATION_ERROR",
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
            code="DEPENDENCY_UNAVAILABLE",
            message="A backend dependency is unavailable right now. Please try again.",
            retryable=True,
        ),
    )
