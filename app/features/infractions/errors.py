from __future__ import annotations

import logging

from fastapi import Request, status as http_status
from fastapi.responses import JSONResponse

from app.core.errors import ErrorDetail, ErrorResponse

logger = logging.getLogger(__name__)


class InfractionError(Exception):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        status_code: int,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable


class NoSuspension(InfractionError):
    def __init__(self) -> None:
        super().__init__(
            code="NOT_SUSPENDED",
            message="This member is not suspended.",
            status_code=http_status.HTTP_409_CONFLICT,
        )


class StillSuspended(InfractionError):
    def __init__(self) -> None:
        super().__init__(
            code="STILL_SUSPENDED",
            message="This member is suspended. End the suspension before changing tiers.",
            status_code=http_status.HTTP_409_CONFLICT,
        )


class SuspensionHeld(InfractionError):
    def __init__(self) -> None:
        super().__init__(
            code="SUSPENSION_HELD",
            message="This member is held at the ban threshold until staff ban or unsuspend.",
            status_code=http_status.HTTP_409_CONFLICT,
        )


class ChangeConflict(InfractionError):
    def __init__(self) -> None:
        super().__init__(
            code="CONFLICT",
            message="The record changed while this was applied. Nothing was written.",
            status_code=http_status.HTTP_409_CONFLICT,
            retryable=True,
        )


async def infraction_error_handler(
    request: Request, exc: InfractionError
) -> JSONResponse:
    """A v2 refusal in the envelope, with the request's correlation id."""
    correlation_id = getattr(request.state, "correlation_id", None)
    logger.info(
        "Infraction refusal code=%s path=%s correlation_id=%s",
        exc.code,
        request.url.path,
        correlation_id,
    )
    error = ErrorDetail(
        code=exc.code,
        message=exc.message,
        retryable=exc.retryable,
        correlation_id=correlation_id,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": ErrorResponse(error=error).model_dump(mode="json")},
    )
