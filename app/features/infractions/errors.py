from __future__ import annotations

from fastapi import HTTPException
from fastapi import status as http_status

from app.core.errors import ErrorDetail, ErrorResponse


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


class NotSuspendedError(InfractionError):
    def __init__(self, discord_id: str) -> None:
        super().__init__(
            code="USER_NOT_SUSPENDED",
            message=f"User {discord_id} has no active suspension to modify.",
            status_code=http_status.HTTP_400_BAD_REQUEST,
        )


def to_http_exception(error: InfractionError) -> HTTPException:
    payload = ErrorResponse(
        error=ErrorDetail(
            code=error.code,
            message=error.message,
            retryable=error.retryable,
        )
    )
    return HTTPException(status_code=error.status_code, detail=payload.model_dump())
