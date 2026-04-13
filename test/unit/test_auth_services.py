from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from app.features.auth.enums import RegistrationPlatform, RegistrationSessionStatus, SupportedGame
from app.features.auth.errors import LinkedAccountNotFoundError, ManualRegistrationRequiredError
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

    async def get_user_by_linked_account(self, platform: str, account_id: str):
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


class FakeIndexCollection:
    def __init__(self):
        self.index_calls = []

    async def create_index(self, keys, **kwargs):
        self.index_calls.append((keys, kwargs))


class FakeDatabase(dict):
    def __getitem__(self, name):
        if name not in self:
            self[name] = FakeIndexCollection()
        return dict.__getitem__(self, name)


class FakeClient(dict):
    def __getitem__(self, name):
        if name not in self:
            self[name] = FakeDatabase()
        return dict.__getitem__(self, name)


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


def test_session_status_invalid_status_defaults_to_failed(monkeypatch):
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "123")
    monkeypatch.setenv("AUTH_DISCORD_REDIRECT_URI", "https://example.com/oauth/discord/callback")
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret")

    import app.core.config as cfg

    importlib.reload(cfg)
    from app.features.auth.session_service import SessionService

    repo = FakeRepo()
    repo.sessions["sess-bad"] = {
        "session_id": "sess-bad",
        "status": "corrupted_status",
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
    }

    status = asyncio.run(SessionService(repo).get_registration_session_status("sess-bad"))

    assert status.status is RegistrationSessionStatus.FAILED


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


def test_auth_repository_ensures_expected_indexes():
    from app.features.auth.repository import AuthRepository

    client = FakeClient()
    asyncio.run(AuthRepository(client).ensure_indexes())

    operation_calls = client["auth"]["registration_operations"].index_calls
    user_calls = client["server_members"]["users"].index_calls

    assert any(call[1].get("name") == "auth_operation_completed_ttl" for call in operation_calls)
    assert any(call[1].get("name") == "auth_user_discord_id_idx" for call in user_calls)
    assert any(call[1].get("name") == "auth_user_linked_platform_account_idx" for call in user_calls)


def test_callback_denied_persists_denial_and_returns_auth_error(monkeypatch):
    import app.features.auth.router as auth_router

    class FakeRepository:
        async def get_registration_session_by_state(self, state_token: str):
            return {"session_id": "sess-1"} if state_token == "state-1" else None

    class FakeSessionService:
        def __init__(self, repository):
            self.calls = []

        async def mark_failed(self, session_id: str, *, failure_code: str, failure_message: str, extra=None):
            self.calls.append(
                {
                    "session_id": session_id,
                    "failure_code": failure_code,
                    "failure_message": failure_message,
                    "extra": extra,
                }
            )

    fake_session_service = FakeSessionService(None)
    monkeypatch.setattr(auth_router, "_repo", lambda db: FakeRepository())
    monkeypatch.setattr(auth_router, "SessionService", lambda repository: fake_session_service)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(auth_router.discord_oauth_callback(code=None, state="state-1", error="access_denied", db=None))

    assert exc.value.status_code == 400
    assert exc.value.detail["error"]["code"] == "DISCORD_OAUTH_DENIED"
    assert fake_session_service.calls[0]["failure_code"] == "DISCORD_OAUTH_DENIED"


def test_callback_mismatch_marks_failed_with_context(monkeypatch):
    import app.features.auth.router as auth_router

    class FakeRepository:
        async def get_registration_session_by_state(self, state_token: str):
            return {
                "session_id": "sess-1",
                "state_token": "state-1",
                "discord_user_id": "111",
                "platform": RegistrationPlatform.STEAM.value,
                "game": SupportedGame.CIV6.value,
            } if state_token == "state-1" else None

    class FakeSessionService:
        def __init__(self, repository):
            self.failed_calls = []

        async def load_session_by_state(self, state_token: str):
            return {
                "session_id": "sess-1",
                "state_token": "state-1",
                "discord_user_id": "111",
                "platform": RegistrationPlatform.STEAM.value,
                "game": SupportedGame.CIV6.value,
            }

        async def mark_validating(self, session_id: str):
            return None

        async def mark_validated(self, *args, **kwargs):
            raise AssertionError('mark_validated should not be reached for mismatch flow')

        async def mark_failed(self, session_id: str, *, failure_code: str, failure_message: str, extra=None):
            self.failed_calls.append(
                {
                    "session_id": session_id,
                    "failure_code": failure_code,
                    "failure_message": failure_message,
                    "extra": extra or {},
                }
            )

    class FakeOAuthService:
        async def fetch_identity_and_connection(self, *, code: str, platform):
            return (
                {"id": "222", "username": "other-user", "global_name": "Other User"},
                {"id": "76561198000000000", "name": "Steam Name"},
            )

    class FakeSteamService:
        async def validate_linked_account(self, *, steam_id: str, game: str):
            return {"steam_id": steam_id, "game": game}

    fake_repo = FakeRepository()
    fake_session_service = FakeSessionService(fake_repo)

    monkeypatch.setattr(auth_router, "_repo", lambda db: fake_repo)
    monkeypatch.setattr(auth_router, "SessionService", lambda repository: fake_session_service)
    monkeypatch.setattr(auth_router, "DiscordOAuthService", lambda: FakeOAuthService())
    monkeypatch.setattr(auth_router, "SteamService", lambda: FakeSteamService())

    with pytest.raises(HTTPException) as exc:
        asyncio.run(auth_router.discord_oauth_callback(code="code-1", state="state-1", error=None, db=None))

    assert exc.value.status_code == 403
    assert exc.value.detail["error"]["code"] == "DISCORD_USER_MISMATCH"
    assert fake_session_service.failed_calls[0]["failure_code"] == "DISCORD_USER_MISMATCH"
    assert fake_session_service.failed_calls[0]["extra"]["validated_account_id"] == "76561198000000000"
