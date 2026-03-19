from __future__ import annotations

from fastapi import HTTPException, status

from app.shared.schemas.common import ErrorDetail, ErrorResponse


class AppDependencyError(RuntimeError):
    """Raised when core app state is unavailable."""


def service_unavailable(detail: str) -> HTTPException:
    payload = ErrorResponse(error=ErrorDetail(code="service_unavailable", message=detail))
    return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=payload.model_dump())
