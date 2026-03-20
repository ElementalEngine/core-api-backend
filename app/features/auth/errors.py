from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status

from app.shared.schemas.common import ErrorDetail, ErrorResponse


class AuthError(Exception):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        status_code: int,
        retryable: bool = False,
        details: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.details = details


class AuthConfigurationError(AuthError):
    def __init__(self, message: str) -> None:
        super().__init__(
            code="AUTH_MISCONFIGURED",
            message=message,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )


class SessionNotFoundError(AuthError):
    def __init__(self, session_id: str) -> None:
        super().__init__(
            code="REGISTRATION_SESSION_NOT_FOUND",
            message="Registration session was not found.",
            status_code=status.HTTP_404_NOT_FOUND,
            details={"session_id": session_id},
        )


class SessionExpiredError(AuthError):
    def __init__(self, session_id: str) -> None:
        super().__init__(
            code="REGISTRATION_SESSION_EXPIRED",
            message="Registration session expired. Please start again.",
            status_code=status.HTTP_410_GONE,
            details={"session_id": session_id},
        )


class AccountLookupNotFoundError(AuthError):
    def __init__(self, *, field: str, value: str) -> None:
        super().__init__(
            code="ACCOUNT_NOT_FOUND",
            message="No linked registration account was found.",
            status_code=status.HTTP_404_NOT_FOUND,
            details={field: value},
        )


class LinkedAccountFetchError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="DISCORD_LINKED_ACCOUNT_FETCH_FAILED",
            message="We could not read your Discord linked accounts. Please try again.",
            status_code=status.HTTP_502_BAD_GATEWAY,
            retryable=True,
        )


class LinkedAccountNotFoundError(AuthError):
    def __init__(self, platform: str) -> None:
        message = {
            "steam": "No Steam account was found in your Discord linked accounts.",
            "epic": "We could not confirm a linked Epic account on your Discord profile.",
            "xbox": "We could not confirm a linked Xbox account on your Discord profile.",
        }.get(platform, "No supported linked account was found.")
        super().__init__(
            code="DISCORD_LINKED_ACCOUNT_NOT_FOUND",
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"platform": platform},
        )


class ManualRegistrationRequiredError(AuthError):
    def __init__(self, platform: str, account_name: str | None = None) -> None:
        message = {
            "epic": (
                "Your Epic account is linked to Discord, but automatic Epic registration "
                "is not supported. Please contact staff for manual registration or use Steam auth."
            ),
            "xbox": (
                "Your Xbox account is linked to Discord, but automatic Xbox registration "
                "is not supported. Please contact staff for manual registration or use Steam auth."
            ),
        }.get(platform, "Automatic registration is not supported for this account type.")
        super().__init__(
            code=f"{platform.upper()}_MANUAL_REQUIRED",
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"platform": platform, "account_name": account_name},
        )


class DiscordOAuthError(AuthError):
    def __init__(self, message: str = "Failed to complete Discord authentication.") -> None:
        super().__init__(
            code="DISCORD_OAUTH_FAILED",
            message=message,
            status_code=status.HTTP_502_BAD_GATEWAY,
            retryable=True,
        )


class AlreadyRegisteredError(AuthError):
    def __init__(self, game: str) -> None:
        super().__init__(
            code="ALREADY_REGISTERED",
            message=f"You are already registered for {game.upper()}.",
            status_code=status.HTTP_409_CONFLICT,
            details={"game": game},
        )


class InvalidStateError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="INVALID_AUTH_STATE",
            message="The registration link is invalid or has already been used.",
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class SessionStateConflictError(AuthError):
    def __init__(self, session_id: str, status_value: str) -> None:
        super().__init__(
            code="INVALID_AUTH_STATE",
            message="The registration link is invalid or has already been used.",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"session_id": session_id, "status": status_value},
        )



def to_http_exception(error: AuthError) -> HTTPException:
    payload = ErrorResponse(
        error=ErrorDetail(
            code=error.code,
            message=error.message,
            details=error.details,
            retryable=error.retryable,
        )
    )
    return HTTPException(status_code=error.status_code, detail=payload.model_dump())
