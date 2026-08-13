import asyncio
import importlib
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from app.features.auth.enums import (
    RegistrationPlatform,
    RegistrationSessionStatus,
    SupportedGame,
)
from app.features.auth.errors import (
    LinkedAccountNotFoundError,
    ManualRegistrationRequiredError,
)
from app.features.auth.oauth_service import DiscordOAuthService
import app.features.auth.router as auth_router
from app.features.auth.registration_service import RegistrationService
from app.features.auth.schemas import CreateRegistrationSessionRequest


class FakeRepo:
    def __init__(self):
        self.user_doc = None
        self.sessions = {}

    async def get_user_by_discord_id(self, discord_id: str):
        return self.user_doc

    async def get_user_by_steam_id(self, steam_id: str):
        return self.user_doc

    async def get_user_by_linked_account(self, platform: str, account_id: str):
        return self.user_doc

    async def insert_registration_session(self, doc):
        self.sessions[doc["session_id"]] = dict(doc)
        self.sessions[f"state:{doc['state_token']}"] = self.sessions[doc["session_id"]]

    async def get_registration_session(self, session_id: str):
        return self.sessions.get(session_id)

    async def get_registration_session_by_state(self, state_token: str):
        return self.sessions.get(f"state:{state_token}")

    async def update_registration_session(self, session_id: str, changes):
        if session_id not in self.sessions:
            return False
        self.sessions[session_id].update(dict(changes))
        return True


def test_session_service_creates_pending_session(monkeypatch):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI",
        "https://example.com/oauth/discord/callback",
    )
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg
    import app.features.auth.session_service as session_module

    importlib.reload(cfg)
    importlib.reload(session_module)
    SessionService = session_module.SessionService

    repo = FakeRepo()
    service = SessionService(repo)

    response = asyncio.run(
        service.create_registration_session(
            CreateRegistrationSessionRequest(
                discord_user_id="123456",
                game=SupportedGame.CIV6,
            )
        )
    )
    assert response.session_id
    assert "discord.com/oauth2/authorize" in response.authorize_url

    status = asyncio.run(service.get_registration_session_status(response.session_id))
    assert status.status is RegistrationSessionStatus.PENDING_AUTH


@pytest.mark.parametrize(
    "platform",
    [RegistrationPlatform.EPIC, RegistrationPlatform.TWOK, RegistrationPlatform.XBOX],
)
def test_session_service_rejects_non_steam_oauth_sessions(monkeypatch, platform):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI", "https://example.com/oauth/discord/callback"
    )
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg
    import app.features.auth.session_service as session_module

    importlib.reload(cfg)
    importlib.reload(session_module)
    SessionService = session_module.SessionService

    repo = FakeRepo()
    service = SessionService(repo)

    with pytest.raises(ManualRegistrationRequiredError):
        asyncio.run(
            service.create_registration_session(
                CreateRegistrationSessionRequest(
                    discord_user_id="123456",
                    game=SupportedGame.CIV7,
                    platform=platform,
                )
            )
        )

    assert not repo.sessions


def test_session_service_coerces_naive_expiry(monkeypatch):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI",
        "https://example.com/oauth/discord/callback",
    )
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg
    import app.features.auth.session_service as session_module

    importlib.reload(cfg)
    importlib.reload(session_module)
    SessionService = session_module.SessionService

    repo = FakeRepo()
    repo.sessions["sess-1"] = {
        "session_id": "sess-1",
        "status": RegistrationSessionStatus.PENDING_AUTH.value,
        "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(
            tzinfo=None
        ),
    }

    status = asyncio.run(SessionService(repo).get_registration_session_status("sess-1"))

    assert status.status is RegistrationSessionStatus.EXPIRED
    assert repo.sessions["sess-1"]["status"] == RegistrationSessionStatus.EXPIRED.value


def test_oauth_service_picks_expected_connection():
    picked = DiscordOAuthService._pick_connection(
        [
            {"id": "1", "type": "steam", "name": "steam-user"},
            {"id": "2", "type": "epicgames", "name": "epic-user"},
        ],
        RegistrationPlatform.EPIC,
    )
    assert picked["id"] == "2"


def test_oauth_service_raises_when_connection_missing():
    with pytest.raises(LinkedAccountNotFoundError):
        DiscordOAuthService._pick_connection([], RegistrationPlatform.XBOX)


def test_registration_service_manual_required():
    with pytest.raises(ManualRegistrationRequiredError):
        RegistrationService.manual_required_for_platform(
            RegistrationPlatform.EPIC,
            account_name="epic-user",
        )


def test_session_status_includes_validated_account_details(monkeypatch):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI", "https://example.com/oauth/discord/callback"
    )
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg
    import app.features.auth.session_service as session_module

    importlib.reload(cfg)
    importlib.reload(session_module)
    SessionService = session_module.SessionService

    repo = FakeRepo()
    repo.sessions["sess-2"] = {
        "session_id": "sess-2",
        "status": RegistrationSessionStatus.VALIDATED.value,
        "game": SupportedGame.CIV6.value,
        "platform": RegistrationPlatform.STEAM.value,
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        "validated_account_id": "76561198000000000",
        "validated_account_name": "Project Cisco",
        "oauth_username_snapshot": "cisco",
        "oauth_display_name_snapshot": "Cisco",
        "oauth_locale_snapshot": "en-GB",
        "oauth_verified_snapshot": False,
        "oauth_mfa_enabled_snapshot": True,
    }

    status = asyncio.run(SessionService(repo).get_registration_session_status("sess-2"))

    assert status.game is SupportedGame.CIV6
    assert status.platform is RegistrationPlatform.STEAM
    assert status.linked_account_name == "Project Cisco"
    assert status.discord_locale == "en-GB"
    assert status.discord_mfa_enabled is True


def test_session_service_coerces_unknown_status_to_failed(monkeypatch):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI", "https://example.com/oauth/discord/callback"
    )
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg
    import app.features.auth.session_service as session_module

    importlib.reload(cfg)
    importlib.reload(session_module)
    SessionService = session_module.SessionService

    repo = FakeRepo()
    repo.sessions["sess-unknown"] = {
        "session_id": "sess-unknown",
        "status": "not_a_real_status",
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
    }

    status = asyncio.run(
        SessionService(repo).get_registration_session_status("sess-unknown")
    )

    assert status.status is RegistrationSessionStatus.FAILED


def test_oauth_callback_denial_returns_discord_oauth_failed(monkeypatch):
    class CallbackRepo:
        async def get_registration_session_by_state(self, state_token: str):
            assert state_token == "state-1"
            return {"session_id": "sess-1"}

    class CallbackSessionService:
        def __init__(self, repository):
            self.repository = repository
            self.calls = []

        async def mark_failed(
            self,
            session_id: str,
            *,
            failure_code: str,
            failure_message: str,
            extra=None,
        ):
            self.calls.append(
                {
                    "session_id": session_id,
                    "failure_code": failure_code,
                    "failure_message": failure_message,
                    "extra": extra,
                }
            )

    repo = CallbackRepo()
    session_service = CallbackSessionService(repo)

    monkeypatch.setattr(auth_router, "_repo", lambda db: repo)
    monkeypatch.setattr(
        auth_router, "SessionService", lambda repository: session_service
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            auth_router.discord_oauth_callback(
                code=None, state="state-1", error="access_denied", db=None
            )
        )

    response = exc_info.value
    assert response.status_code == 502
    assert response.detail["error"]["code"] == "DISCORD_OAUTH_FAILED"
    assert session_service.calls == [
        {
            "session_id": "sess-1",
            "failure_code": "DISCORD_OAUTH_FAILED",
            "failure_message": "Discord authentication was cancelled or denied. Please start again.",
            "extra": {"oauth_error": "access_denied"},
        }
    ]


def test_manual_required_for_2k_raises():
    with pytest.raises(ManualRegistrationRequiredError):
        RegistrationService.manual_required_for_platform(RegistrationPlatform.TWOK)


def test_manual_not_required_for_steam():
    # Should not raise.
    RegistrationService.manual_required_for_platform(RegistrationPlatform.STEAM)
