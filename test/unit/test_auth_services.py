import asyncio
import importlib
from datetime import datetime, timedelta, timezone

import pytest

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
from app.features.auth.registration_service import RegistrationService
from app.features.auth.schemas import CreateRegistrationSessionRequest


class FakeRepo:
    def __init__(self):
        self.user_doc = None
        self.sessions = {}
        self.audit = []

    async def get_user_by_discord_id(self, discord_id: str):
        return self.user_doc

    async def get_user_by_steam_id(self, steam_id: str):
        return self.user_doc

    async def insert_registration_session(self, doc):
        self.sessions[doc["session_id"]] = dict(doc)
        self.sessions[f"state:{doc['state_token']}"] = self.sessions[doc["session_id"]]

    async def append_audit_event(self, doc):
        self.audit.append(dict(doc))

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

    importlib.reload(cfg)
    from app.features.auth.session_service import SessionService

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




def test_session_service_coerces_naive_expiry(monkeypatch):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI",
        "https://example.com/oauth/discord/callback",
    )
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg

    importlib.reload(cfg)
    from app.features.auth.session_service import SessionService

    repo = FakeRepo()
    repo.sessions["sess-1"] = {
        "session_id": "sess-1",
        "status": RegistrationSessionStatus.PENDING_AUTH.value,
        "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(tzinfo=None),
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
    monkeypatch.setenv("AUTH_DISCORD_REDIRECT_URI", "https://example.com/oauth/discord/callback")
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg

    importlib.reload(cfg)
    from app.features.auth.session_service import SessionService

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
        "oauth_locale": "en-GB",
        "oauth_verified": False,
        "oauth_mfa_enabled": True,
        "oauth_premium_type": 0,
    }

    status = asyncio.run(SessionService(repo).get_registration_session_status("sess-2"))

    assert status.game is SupportedGame.CIV6
    assert status.platform is RegistrationPlatform.STEAM
    assert status.linked_account_name == "Project Cisco"
    assert status.oauth_locale == "en-GB"
    assert status.oauth_mfa_enabled is True
