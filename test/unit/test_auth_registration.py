import asyncio
from datetime import datetime, timezone

import pytest
from pydantic import SecretStr

import app.features.auth.steam_service as steam_module
from app.features.auth.enums import (
    RegistrationOperationStatus,
    RegistrationSessionStatus,
    RoleIntent,
    SupportedGame,
)
from app.features.auth.errors import (
    AlreadyRegisteredError,
    DiscordSteamConflictError,
    SteamIdConflictError,
    SteamOwnershipMissingError,
    SteamPlaytimeBelowThresholdError,
    SteamProfilePrivateError,
)
from app.features.auth.operation_service import OperationService
from app.features.auth.registration_service import RegistrationService
from app.features.auth.schemas import FinalizeRegistrationOperationRequest
from app.features.auth.steam_service import SteamService


class FakeRepo:
    def __init__(self):
        self.users_by_discord = {}
        self.users_by_steam = {}
        self.users_by_linked = {}
        self.operations = {}
        self.sessions = {}
        self.audit = []
        self.upserts = []

    async def get_user_by_discord_id(self, discord_id: str):
        return self.users_by_discord.get(discord_id)

    async def get_user_by_steam_id(self, steam_id: str):
        return self.users_by_steam.get(steam_id)

    async def get_user_by_linked_account(self, platform: str, account_id: str):
        return self.users_by_linked.get((platform, account_id))

    async def insert_registration_operation(self, doc):
        self.operations[doc["operation_id"]] = dict(doc)

    async def get_registration_operation(self, operation_id: str):
        return self.operations.get(operation_id)

    async def update_registration_operation(self, operation_id: str, changes):
        self.operations[operation_id].update(dict(changes))
        return True

    async def update_registration_session(self, session_id: str, changes):
        self.sessions.setdefault(session_id, {}).update(dict(changes))
        return True

    async def append_audit_event(self, doc):
        self.audit.append(dict(doc))

    async def upsert_registered_user(self, **kwargs):
        self.upserts.append(dict(kwargs))


@pytest.mark.parametrize(
    ("existing_discord", "existing_steam", "expected_error"),
    [
        (
            {"discord_id": "1", "steam_id": "other", "registrations": {}},
            None,
            DiscordSteamConflictError,
        ),
        (
            None,
            {"discord_id": "2", "steam_id": "steam-1", "registrations": {}},
            SteamIdConflictError,
        ),
        (
            {"discord_id": "1", "steam_id": "steam-1", "registrations": {"civ6": {}}},
            {"discord_id": "1", "steam_id": "steam-1", "registrations": {"civ6": {}}},
            AlreadyRegisteredError,
        ),
    ],
)
def test_registration_conflicts(existing_discord, existing_steam, expected_error):
    repo = FakeRepo()
    if existing_discord is not None:
        repo.users_by_discord["1"] = existing_discord
    if existing_steam is not None:
        repo.users_by_steam["steam-1"] = existing_steam

    service = RegistrationService(repo)

    with pytest.raises(expected_error):
        asyncio.run(
            service.assert_registration_conflicts(
                discord_user_id="1",
                platform=RegistrationPlatform.STEAM,
                account_id="steam-1",
                game=SupportedGame.CIV6.value,
            )
        )


def test_operation_finalize_success_upserts_user_and_completes_session():
    repo = FakeRepo()
    repo.operations["op-1"] = {
        "operation_id": "op-1",
        "status": RegistrationOperationStatus.PENDING.value,
        "source_session_id": "sess-1",
        "discord_user_id": "123",
        "linked_platform": RegistrationPlatform.STEAM.value,
        "linked_account_id": "765",
        "steam_id": "765",
        "game": SupportedGame.CIV6.value,
        "type": "registration",
        "username_snapshot": "user",
        "display_name_snapshot": "User",
        "ownership_verified_at": datetime.now(timezone.utc),
        "playtime_minutes": 333,
    }

    asyncio.run(
        OperationService(repo).finalize_operation(
            "op-1",
            FinalizeRegistrationOperationRequest(
                result="succeeded",
                applied_role_intents=[RoleIntent.GRANT_CIV6_RANK],
                failure_code=None,
                failure_message=None,
            ),
        )
    )

    assert repo.upserts and repo.upserts[0]["discord_user_id"] == "123"
    assert repo.operations["op-1"]["status"] == RegistrationOperationStatus.SUCCEEDED.value
    assert repo.sessions["sess-1"]["status"] == RegistrationSessionStatus.COMPLETED.value


def test_operation_finalize_failure_marks_session_failed():
    repo = FakeRepo()
    repo.operations["op-2"] = {
        "operation_id": "op-2",
        "status": RegistrationOperationStatus.PENDING.value,
        "source_session_id": "sess-2",
        "discord_user_id": "123",
        "linked_platform": RegistrationPlatform.STEAM.value,
        "linked_account_id": "765",
        "steam_id": "765",
        "game": SupportedGame.CIV7.value,
    }

    asyncio.run(
        OperationService(repo).finalize_operation(
            "op-2",
            FinalizeRegistrationOperationRequest(
                result="failed",
                applied_role_intents=[],
                failure_code="ROLE_SYNC_FAILED",
                failure_message="Failed to assign role.",
            ),
        )
    )

    assert not repo.upserts
    assert repo.operations["op-2"]["status"] == RegistrationOperationStatus.FAILED.value
    assert repo.sessions["sess-2"]["status"] == RegistrationSessionStatus.FAILED.value
    assert repo.sessions["sess-2"]["failure_code"] == "ROLE_SYNC_FAILED"


def test_steam_validation_private_profile(monkeypatch):
    async def fake_owned_games(*, steam_id: str, app_id: int):
        return {"response": {}}

    object.__setattr__(steam_module.settings, "auth_steam_api_key", SecretStr("test-key"))
    service = SteamService()
    monkeypatch.setattr(service, "_get_owned_games", fake_owned_games)

    with pytest.raises(SteamProfilePrivateError):
        asyncio.run(service.validate_linked_account(steam_id="1", game=SupportedGame.CIV6.value))


def test_steam_validation_missing_game(monkeypatch):
    async def fake_owned_games(*, steam_id: str, app_id: int):
        return {"response": {"games": [{"appid": 1, "playtime_forever": 999}]}}

    object.__setattr__(steam_module.settings, "auth_steam_api_key", SecretStr("test-key"))
    service = SteamService()
    monkeypatch.setattr(service, "_get_owned_games", fake_owned_games)

    with pytest.raises(SteamOwnershipMissingError):
        asyncio.run(service.validate_linked_account(steam_id="1", game=SupportedGame.CIV6.value))


def test_steam_validation_playtime_threshold(monkeypatch):
    async def fake_owned_games(*, steam_id: str, app_id: int):
        return {"response": {"games": [{"appid": app_id, "playtime_forever": 10}]}}

    object.__setattr__(steam_module.settings, "auth_steam_api_key", SecretStr("test-key"))
    service = SteamService()
    monkeypatch.setattr(service, "_get_owned_games", fake_owned_games)

    with pytest.raises(SteamPlaytimeBelowThresholdError):
        asyncio.run(service.validate_linked_account(steam_id="1", game=SupportedGame.CIV7.value))
