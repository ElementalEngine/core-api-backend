from __future__ import annotations

import logging
from typing import Any

from fastapi import Request, status
from fastapi.responses import JSONResponse

from app.core.errors import ErrorDetail, ErrorResponse

logger = logging.getLogger(__name__)


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
        label = {
            "discord_id": f"Discord ID {value}",
            "linked_account_id": f"linked account ID {value}",
        }.get(field, value)
        super().__init__(
            code="ACCOUNT_NOT_FOUND",
            message=f"No account record was found for {label}.",
            status_code=status.HTTP_404_NOT_FOUND,
            details={field: value},
        )


class LinkedAccountFetchError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="DISCORD_LINKED_ACCOUNT_FETCH_FAILED",
            message="We could not read your Discord linked accounts right now. Please try again.",
            status_code=status.HTTP_502_BAD_GATEWAY,
            retryable=True,
        )


class LinkedAccountNotFoundError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="DISCORD_LINKED_ACCOUNT_NOT_FOUND",
            message=(
                "No linked Steam account was found on your Discord profile. "
                "Open Discord Settings → Connections, add Steam, enable Display on profile, then try again."
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"platform": "steam"},
        )


class DiscordOAuthError(AuthError):
    def __init__(
        self, message: str = "Failed to complete Discord authentication."
    ) -> None:
        super().__init__(
            code="DISCORD_OAUTH_FAILED",
            message=message,
            status_code=status.HTTP_502_BAD_GATEWAY,
            retryable=True,
        )


class SignInCancelledError(AuthError):
    """The player stopped on Discord's page: nothing failed, so not retryable."""

    def __init__(self) -> None:
        super().__init__(
            code="DISCORD_OAUTH_FAILED",
            message="Discord sign-in was cancelled or did not finish. Please start again.",
            status_code=status.HTTP_400_BAD_REQUEST,
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


class DiscordUserMismatchError(AuthError):
    def __init__(self, *, session_user_id: str, request_user_id: str) -> None:
        super().__init__(
            code="DISCORD_USER_MISMATCH",
            message="The authenticated Discord account did not match the registration session.",
            status_code=status.HTTP_403_FORBIDDEN,
            details={
                "session_user_id": session_user_id,
                "request_user_id": request_user_id,
            },
        )


class SteamProfilePrivateError(AuthError):
    def __init__(self, *, game: str) -> None:
        super().__init__(
            code="STEAM_PROFILE_PRIVATE",
            message=(
                f"We could not verify {game.upper()} because your Steam game details or playtime are hidden. "
                "Make your Steam profile public, set Game details to Public, then try again."
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"game": game},
        )


class SteamOwnershipMissingError(AuthError):
    def __init__(self, game: str) -> None:
        super().__init__(
            code="STEAM_OWNERSHIP_MISSING",
            message=(
                f"Your linked Steam account was found, but {game.upper()} does not appear to be owned on that account. "
                "Check that the correct Steam account is linked in Discord."
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"game": game},
        )


class SteamPlaytimeBelowThresholdError(AuthError):
    def __init__(
        self, *, game: str, required_minutes: int, actual_minutes: int
    ) -> None:
        super().__init__(
            code="STEAM_PLAYTIME_BELOW_THRESHOLD",
            message=(
                f"Your linked Steam account owns {game.upper()}, but it does not meet the minimum playtime requirement yet. "
                f"Required: {required_minutes} minutes. Found: {actual_minutes} minutes. If 0 hours are found: Make sure 'Always keep my total playing time private . . .' is unchecked."
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
    def __init__(
        self, *, platform: str, account_id: str, existing_discord_id: str
    ) -> None:
        super().__init__(
            code="LINKED_ACCOUNT_CONFLICT",
            message="This linked account is already connected to another Discord account. Please contact staff if you believe this is a mistake.",
            status_code=status.HTTP_409_CONFLICT,
            details={
                "platform": platform,
                "account_id": account_id,
                "existing_discord_id": existing_discord_id,
            },
        )


class DiscordSteamConflictError(AuthError):
    def __init__(self, *, discord_user_id: str, existing_steam_id: str) -> None:
        super().__init__(
            code="DISCORD_ID_CONFLICT",
            message="Your Discord account is already linked to a different Steam account. Please contact staff to resolve the conflict.",
            status_code=status.HTTP_409_CONFLICT,
            details={
                "discord_user_id": discord_user_id,
                "existing_steam_id": existing_steam_id,
            },
        )


class RankRoleEligibilityError(AuthError):
    def __init__(self, discord_user_id: str) -> None:
        super().__init__(
            code="RANK_ROLE_NOT_ELIGIBLE",
            message="You must complete registration before you can add a ranked role.",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"discord_user_id": discord_user_id},
        )


class RankRoleNeedsSteamError(AuthError):
    def __init__(self, discord_user_id: str) -> None:
        super().__init__(
            code="RANK_ROLE_NEEDS_STEAM",
            message=(
                "A second rank role needs a Steam registration verified by the bot. "
                "Please ask staff to add it."
            ),
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"discord_user_id": discord_user_id},
        )


class StaffRequiredError(AuthError):
    def __init__(self) -> None:
        super().__init__(
            code="FORBIDDEN",
            message="Only staff may register a player by hand.",
            status_code=status.HTTP_403_FORBIDDEN,
        )


async def auth_error_handler(request: Request, exc: AuthError) -> JSONResponse:
    """An auth refusal in the envelope, with the request's correlation id."""
    correlation_id = getattr(request.state, "correlation_id", None)
    logger.info(
        "Auth refusal code=%s path=%s correlation_id=%s",
        exc.code,
        request.url.path,
        correlation_id,
    )
    error = ErrorDetail(
        code=exc.code,
        message=exc.message,
        details=exc.details,
        retryable=exc.retryable,
        correlation_id=correlation_id,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": ErrorResponse(error=error).model_dump(mode="json")},
    )
