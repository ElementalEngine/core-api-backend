from __future__ import annotations

from datetime import datetime, timedelta, timezone
from secrets import token_urlsafe
from urllib.parse import urlencode

from app.core.config import settings
from app.features.auth.constants import DISCORD_OAUTH_AUTHORIZE_URL, DISCORD_OAUTH_SCOPES
from app.features.auth.enums import RegistrationSessionStatus
from app.features.auth.errors import (
    AlreadyRegisteredError,
    AuthConfigurationError,
    SessionExpiredError,
    SessionNotFoundError,
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

        expires_at = session.get("expires_at")
        status_value = session.get("status", RegistrationSessionStatus.PENDING_AUTH.value)
        if (
            isinstance(expires_at, datetime)
            and expires_at <= datetime.now(timezone.utc)
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

        return RegistrationSessionStatusResponse(
            session_id=session_id,
            status=RegistrationSessionStatus(status_value),
            expires_at=expires_at,
            failure_code=session.get("failure_code"),
            failure_message=session.get("failure_message"),
        )

    async def load_session_by_state(self, state_token: str) -> dict[str, object]:
        session = await self._repository.get_registration_session_by_state(state_token)
        if session is None:
            raise SessionNotFoundError("state")

        session_id = str(session["session_id"])
        expires_at = session.get("expires_at")
        if isinstance(expires_at, datetime) and expires_at <= datetime.now(timezone.utc):
            await self._repository.update_registration_session(
                session_id,
                {
                    "status": RegistrationSessionStatus.EXPIRED.value,
                    "failure_code": "REGISTRATION_SESSION_EXPIRED",
                    "failure_message": "Registration session expired. Please start again.",
                    "updated_at": datetime.now(timezone.utc),
                },
            )
            raise SessionExpiredError(session_id)

        status_value = str(session.get("status", RegistrationSessionStatus.PENDING_AUTH.value))
        if status_value in {
            RegistrationSessionStatus.VALIDATED.value,
            RegistrationSessionStatus.FAILED.value,
            RegistrationSessionStatus.EXPIRED.value,
            RegistrationSessionStatus.COMPLETED.value,
        }:
            raise SessionStateConflictError(session_id=session_id, status_value=status_value)

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
    ) -> None:
        await self._repository.update_registration_session(
            session_id,
            {
                "status": RegistrationSessionStatus.VALIDATED.value,
                "validated_account_id": linked_account_id,
                "validated_account_name": linked_account_name,
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
