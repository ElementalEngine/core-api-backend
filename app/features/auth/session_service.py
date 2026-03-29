from __future__ import annotations

from datetime import datetime, timedelta, timezone
from secrets import token_urlsafe
from urllib.parse import urlencode

from app.core.config import settings
from app.features.auth.constants import DISCORD_OAUTH_AUTHORIZE_URL, DISCORD_OAUTH_SCOPES
from app.features.auth.enums import RegistrationPlatform, RegistrationSessionStatus, SupportedGame
from app.features.auth.errors import (
    AlreadyRegisteredError,
    AuthConfigurationError,
    DiscordUserMismatchError,
    SessionExpiredError,
    SessionNotFoundError,
    SessionNotValidatedError,
    SessionStateConflictError,
)
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import (
    CreateRegistrationSessionRequest,
    RegistrationSessionResponse,
    RegistrationSessionStatusResponse,
)


class SessionService:
    def __init__(self, repository: AuthRepository) -> None:
        self._repository = repository

    async def create_registration_session(
        self,
        payload: CreateRegistrationSessionRequest,
    ) -> RegistrationSessionResponse:
        existing = await self._repository.get_user_by_discord_id(payload.discord_user_id)
        regs = (existing or {}).get("registrations") or {}
        if payload.game.value in regs:
            raise AlreadyRegisteredError(payload.game.value)

        if not settings.auth_discord_client_id or not settings.auth_discord_redirect_uri:
            raise AuthConfigurationError(
                "Discord OAuth is not configured for auth registration."
            )

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=settings.auth_session_ttl_minutes)
        session_id = token_urlsafe(24)
        state_token = token_urlsafe(32)

        await self._repository.insert_registration_session(
            {
                "session_id": session_id,
                "state_token": state_token,
                "discord_user_id": payload.discord_user_id,
                "game": payload.game.value,
                "platform": payload.platform.value,
                "status": RegistrationSessionStatus.PENDING_AUTH.value,
                "expires_at": expires_at,
                "created_at": now,
                "updated_at": now,
            }
        )
        await self._repository.append_audit_event(
            {
                "action": "registration_session_created",
                "discord_user_id": payload.discord_user_id,
                "game": payload.game.value,
                "platform": payload.platform.value,
                "session_id": session_id,
            }
        )
        return RegistrationSessionResponse(
            session_id=session_id,
            authorize_url=self._build_authorize_url(state_token),
            expires_at=expires_at,
        )

    async def get_registration_session_status(
        self,
        session_id: str,
    ) -> RegistrationSessionStatusResponse:
        session = await self._repository.get_registration_session(session_id)
        if session is None:
            raise SessionNotFoundError(session_id)
        session = await self._coerce_expired_session(session, raise_on_expired=False)

        raw_game = session.get("game")
        raw_platform = session.get("platform")
        details: dict[str, object] = {}
        if session.get("validated_account_name"):
            details["linked_account_name"] = str(session["validated_account_name"])
        if session.get("oauth_username_snapshot"):
            details["username"] = str(session["oauth_username_snapshot"])
        if session.get("oauth_display_name_snapshot"):
            details["display_name"] = str(session["oauth_display_name_snapshot"])

        return RegistrationSessionStatusResponse(
            session_id=session_id,
            status=RegistrationSessionStatus(str(session.get("status", RegistrationSessionStatus.PENDING_AUTH.value))),
            game=SupportedGame(str(raw_game)) if isinstance(raw_game, str) else None,
            platform=RegistrationPlatform(str(raw_platform)) if isinstance(raw_platform, str) else None,
            expires_at=session.get("expires_at"),
            linked_account_id=str(session["validated_account_id"]) if session.get("validated_account_id") else None,
            linked_account_name=str(session["validated_account_name"]) if session.get("validated_account_name") else None,
            oauth_username_snapshot=str(session["oauth_username_snapshot"]) if session.get("oauth_username_snapshot") else None,
            oauth_display_name_snapshot=str(session["oauth_display_name_snapshot"]) if session.get("oauth_display_name_snapshot") else None,
            oauth_locale=str(session["oauth_locale"]) if session.get("oauth_locale") else None,
            oauth_verified=session.get("oauth_verified") if isinstance(session.get("oauth_verified"), bool) else None,
            oauth_mfa_enabled=session.get("oauth_mfa_enabled") if isinstance(session.get("oauth_mfa_enabled"), bool) else None,
            oauth_premium_type=int(session["oauth_premium_type"]) if isinstance(session.get("oauth_premium_type"), int) else None,
            failure_code=session.get("failure_code"),
            failure_message=session.get("failure_message"),
            details=details,
        )

    async def load_session_by_state(self, state_token: str) -> dict[str, object]:
        session = await self._repository.get_registration_session_by_state(state_token)
        if session is None:
            raise SessionNotFoundError("oauth_state")
        session = await self._coerce_expired_session(session, raise_on_expired=True)

        session_id = str(session["session_id"])
        status_value = str(session.get("status", RegistrationSessionStatus.PENDING_AUTH.value))
        if status_value in {
            RegistrationSessionStatus.VALIDATED.value,
            RegistrationSessionStatus.FAILED.value,
            RegistrationSessionStatus.EXPIRED.value,
            RegistrationSessionStatus.COMPLETED.value,
        }:
            raise SessionStateConflictError(session_id=session_id, status_value=status_value)

        return session

    async def load_session_for_completion(
        self,
        *,
        session_id: str,
        discord_user_id: str,
    ) -> dict[str, object]:
        session = await self._repository.get_registration_session(session_id)
        if session is None:
            raise SessionNotFoundError(session_id)
        session = await self._coerce_expired_session(session, raise_on_expired=True)

        if str(session.get("discord_user_id", "")) != discord_user_id:
            raise DiscordUserMismatchError(
                session_user_id=str(session.get("discord_user_id", "")),
                request_user_id=discord_user_id,
            )

        status_value = str(session.get("status", RegistrationSessionStatus.PENDING_AUTH.value))
        if status_value != RegistrationSessionStatus.VALIDATED.value:
            raise SessionNotValidatedError(session_id, status_value)
        return session

    async def mark_validating(self, session_id: str) -> None:
        await self._repository.update_registration_session(
            session_id,
            {
                "status": RegistrationSessionStatus.VALIDATING.value,
                "updated_at": datetime.now(timezone.utc),
            },
        )

    async def mark_validated(
        self,
        session_id: str,
        *,
        linked_account_id: str,
        linked_account_name: str | None,
        oauth_username_snapshot: str | None = None,
        oauth_display_name_snapshot: str | None = None,
        oauth_locale: str | None = None,
        oauth_verified: bool | None = None,
        oauth_mfa_enabled: bool | None = None,
        oauth_premium_type: int | None = None,
    ) -> None:
        await self._repository.update_registration_session(
            session_id,
            {
                "status": RegistrationSessionStatus.VALIDATED.value,
                "validated_account_id": linked_account_id,
                "validated_account_name": linked_account_name,
                "oauth_username_snapshot": oauth_username_snapshot,
                "oauth_display_name_snapshot": oauth_display_name_snapshot,
                "oauth_locale": oauth_locale,
                "oauth_verified": oauth_verified,
                "oauth_mfa_enabled": oauth_mfa_enabled,
                "oauth_premium_type": oauth_premium_type,
                "failure_code": None,
                "failure_message": None,
                "updated_at": datetime.now(timezone.utc),
            },
        )

    async def mark_failed(
        self,
        session_id: str,
        *,
        failure_code: str,
        failure_message: str,
        extra: dict[str, object] | None = None,
    ) -> None:
        changes = {
            "status": RegistrationSessionStatus.FAILED.value,
            "failure_code": failure_code,
            "failure_message": failure_message,
            "updated_at": datetime.now(timezone.utc),
        }
        if extra:
            changes.update(extra)
        await self._repository.update_registration_session(session_id, changes)

    async def _coerce_expired_session(
        self,
        session: dict[str, object],
        *,
        raise_on_expired: bool,
    ) -> dict[str, object]:
        session_id = str(session["session_id"])
        expires_at = session.get("expires_at")
        normalized_expires_at = (
            self._normalize_utc_datetime(expires_at)
            if isinstance(expires_at, datetime)
            else None
        )
        status_value = str(session.get("status", RegistrationSessionStatus.PENDING_AUTH.value))
        if (
            normalized_expires_at is not None
            and normalized_expires_at <= datetime.now(timezone.utc)
            and status_value
            not in {
                RegistrationSessionStatus.EXPIRED.value,
                RegistrationSessionStatus.COMPLETED.value,
                RegistrationSessionStatus.FAILED.value,
            }
        ):
            status_value = RegistrationSessionStatus.EXPIRED.value
            await self._repository.update_registration_session(
                session_id,
                {
                    "status": status_value,
                    "failure_code": "REGISTRATION_SESSION_EXPIRED",
                    "failure_message": "Registration session expired. Please start again.",
                    "updated_at": datetime.now(timezone.utc),
                },
            )
            session = {
                **session,
                "status": status_value,
                "failure_code": "REGISTRATION_SESSION_EXPIRED",
                "failure_message": "Registration session expired. Please start again.",
            }
        if raise_on_expired and status_value == RegistrationSessionStatus.EXPIRED.value:
            raise SessionExpiredError(session_id)
        return session

    @staticmethod
    def _normalize_utc_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _build_authorize_url(state_token: str) -> str:
        params = {
            "client_id": settings.auth_discord_client_id,
            "redirect_uri": settings.auth_discord_redirect_uri,
            "response_type": "code",
            "scope": " ".join(DISCORD_OAUTH_SCOPES),
            "prompt": "consent",
            "state": state_token,
        }
        return f"{DISCORD_OAUTH_AUTHORIZE_URL}?{urlencode(params)}"
