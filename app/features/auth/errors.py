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


class SessionNotValidatedError(AuthError):
    def __init__(self, session_id: str, status_value: str) -> None:
        super().__init__(
            code="REGISTRATION_SESSION_NOT_READY",
            message="Registration session is not ready to complete yet.",
            status_code=status.HTTP_409_CONFLICT,
            details={"session_id": session_id, "status": status_value},
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
            "steam": "No Steam account was found in your Discord linked accounts. Check Discord Settings → Connections and try again.",
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


class DiscordUserMismatchError(AuthError):
    def __init__(self, *, session_user_id: str, request_user_id: str) -> None:
        super().__init__(
            code="DISCORD_USER_MISMATCH",
            message="The authenticated Discord account did not match the registration session.",
            status_code=status.HTTP_403_FORBIDDEN,
            details={"session_user_id": session_user_id, "request_user_id": request_user_id},
        )


class OperationNotFoundError(AuthError):
    def __init__(self, operation_id: str) -> None:
        super().__init__(
            code="REGISTRATION_OPERATION_NOT_FOUND",
            message="Registration operation was not found.",
            status_code=status.HTTP_404_NOT_FOUND,
            details={"operation_id": operation_id},
        )


class OperationStateConflictError(AuthError):
    def __init__(self, operation_id: str, status_value: str) -> None:
        super().__init__(
            code="REGISTRATION_OPERATION_STATE_CONFLICT",
            message="Registration operation is no longer pending.",
            status_code=status.HTTP_409_CONFLICT,
            details={"operation_id": operation_id, "status": status_value},
        )


class SteamProfilePrivateError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="STEAM_PROFILE_PRIVATE",
            message="Your Steam profile must be public and your playtime must be visible to register automatically.",
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class SteamOwnershipMissingError(AuthError):
    def __init__(self, game: str) -> None:
        super().__init__(
            code="STEAM_OWNERSHIP_MISSING",
            message=f"Your Steam account does not appear to own {game.upper()}.",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"game": game},
        )


class SteamPlaytimeBelowThresholdError(AuthError):
    def __init__(self, *, game: str, required_minutes: int, actual_minutes: int) -> None:
        super().__init__(
            code="STEAM_PLAYTIME_BELOW_THRESHOLD",
            message=(
                f"Your playtime does not meet the requirement for {game.upper()}. "
                f"Required: {required_minutes} minutes. Your playtime: {actual_minutes} minutes."
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
            details={
                "game": game,
                "required_minutes": required_minutes,
                "actual_minutes": actual_minutes,
            },
        )


class SteamValidationError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="STEAM_API_FAILURE",
            message="We could not verify your Steam account right now. Please try again.",
            status_code=status.HTTP_502_BAD_GATEWAY,
            retryable=True,
        )


class SteamIdConflictError(AuthError):
    def __init__(self, *, steam_id: str, existing_discord_id: str) -> None:
        super().__init__(
            code="STEAM_ID_CONFLICT",
            message="This Steam account is already linked to another Discord account. Please contact staff if you believe this is a mistake.",
            status_code=status.HTTP_409_CONFLICT,
            details={"steam_id": steam_id, "existing_discord_id": existing_discord_id},
        )


class LinkedAccountConflictError(AuthError):
    def __init__(self, *, platform: str, account_id: str, existing_discord_id: str) -> None:
        super().__init__(
            code="LINKED_ACCOUNT_CONFLICT",
            message="This linked account is already connected to another Discord account. Please contact staff if you believe this is a mistake.",
            status_code=status.HTTP_409_CONFLICT,
            details={"platform": platform, "account_id": account_id, "existing_discord_id": existing_discord_id},
        )


class DiscordSteamConflictError(AuthError):
    def __init__(self, *, discord_user_id: str, existing_steam_id: str) -> None:
        super().__init__(
            code="DISCORD_ID_CONFLICT",
            message="Your Discord account is already linked to a different Steam account. Please contact staff to resolve the conflict.",
            status_code=status.HTTP_409_CONFLICT,
            details={"discord_user_id": discord_user_id, "existing_steam_id": existing_steam_id},
        )


class RankRoleEligibilityError(AuthError):
    def __init__(self, discord_user_id: str) -> None:
        super().__init__(
            code="RANK_ROLE_NOT_ELIGIBLE",
            message="You must complete registration before you can add a ranked role.",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"discord_user_id": discord_user_id},
        )


def to_http_exception(error: AuthError) -> HTTPException:
    payload = ErrorResponse(
        detail=ErrorDetail(
            error={
                "code": error.code,
                "message": error.message,
                "details": error.details,
                "retryable": error.retryable,
                "correlation_id": None,
            }
        )
    )
    return HTTPException(status_code=error.status_code, detail=payload.detail.model_dump())
