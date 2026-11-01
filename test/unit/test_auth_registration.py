import asyncio

import pytest
from pydantic import SecretStr

import app.features.auth.steam_service as steam_module
from app.features.auth.enums import (
    ManualRegistrationChoice,
    RegistrationPlatform,
    RoleIntent,
    SupportedGame,
)
from app.features.auth.errors import (
    AlreadyRegisteredError,
    DiscordSteamConflictError,
    LinkedAccountConflictError,
    SteamIdConflictError,
    SteamOwnershipMissingError,
    SteamPlaytimeBelowThresholdError,
    SteamProfilePrivateError,
)
from app.features.auth.registration_v2 import refuse_clash, role_intents
from app.features.auth.steam_service import SteamService


class FakeRepo:
    def __init__(self):
        self.users_by_discord = {}
        self.users_by_steam = {}
        self.users_by_linked = {}

    async def get_user_by_discord_id(self, discord_id: str):
        return self.users_by_discord.get(discord_id)

    async def get_user_by_steam_id(self, steam_id: str):
        return self.users_by_steam.get(steam_id)

    async def get_user_by_linked_account(self, platform: str, account_id: str):
        return self.users_by_linked.get((platform, account_id))


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
        (
            {"discord_id": "1", "linked_platform": "2k", "linked_account_id": "2k-1"},
            None,
            LinkedAccountConflictError,
        ),
    ],
)
def test_registration_conflicts(existing_discord, existing_steam, expected_error):
    repo = FakeRepo()
    if existing_discord is not None:
        repo.users_by_discord["1"] = existing_discord
    if existing_steam is not None:
        repo.users_by_steam["steam-1"] = existing_steam

    with pytest.raises(expected_error):
        asyncio.run(
            refuse_clash(
                repo,
                discord_user_id="1",
                platform=RegistrationPlatform.STEAM,
                account_id="steam-1",
                game=SupportedGame.CIV6.value,
            )
        )


def test_a_2k_account_held_by_another_player_is_a_linked_account_clash():
    repo = FakeRepo()
    repo.users_by_linked[("2k", "2k-1")] = {"discord_id": "2"}
    with pytest.raises(LinkedAccountConflictError) as caught:
        asyncio.run(
            refuse_clash(
                repo,
                discord_user_id="1",
                platform=RegistrationPlatform.TWOK,
                account_id="2k-1",
                game=SupportedGame.CIV7.value,
            )
        )
    assert caught.value.details["existing_discord_id"] == "2"


def test_steam_validation_private_profile(monkeypatch):
    async def fake_owned_games(*, steam_id: str, app_id: int):
        return {"response": {}}

    object.__setattr__(
        steam_module.settings, "auth_steam_api_key", SecretStr("test-key")
    )
    service = SteamService()
    monkeypatch.setattr(service, "_get_owned_games", fake_owned_games)

    with pytest.raises(SteamProfilePrivateError):
        asyncio.run(
            service.validate_linked_account(steam_id="1", game=SupportedGame.CIV6.value)
        )


def test_steam_validation_missing_game(monkeypatch):
    async def fake_owned_games(*, steam_id: str, app_id: int):
        return {"response": {"games": [{"appid": 1, "playtime_forever": 999}]}}

    object.__setattr__(
        steam_module.settings, "auth_steam_api_key", SecretStr("test-key")
    )
    service = SteamService()
    monkeypatch.setattr(service, "_get_owned_games", fake_owned_games)

    with pytest.raises(SteamOwnershipMissingError):
        asyncio.run(
            service.validate_linked_account(steam_id="1", game=SupportedGame.CIV6.value)
        )


def test_steam_validation_playtime_threshold(monkeypatch):
    async def fake_owned_games(*, steam_id: str, app_id: int):
        return {"response": {"games": [{"appid": app_id, "playtime_forever": 10}]}}

    object.__setattr__(
        steam_module.settings, "auth_steam_api_key", SecretStr("test-key")
    )
    service = SteamService()
    monkeypatch.setattr(service, "_get_owned_games", fake_owned_games)

    with pytest.raises(SteamPlaytimeBelowThresholdError):
        asyncio.run(
            service.validate_linked_account(steam_id="1", game=SupportedGame.CIV7.value)
        )


@pytest.mark.parametrize(
    ("choice", "exp_platform", "exp_method"),
    [
        (ManualRegistrationChoice.STEAM, "steam", "admin_staff_attested"),
        (
            ManualRegistrationChoice.STEAM_FAMILY_SHARE,
            "steam",
            "admin_steam_family_share",
        ),
        (ManualRegistrationChoice.TWOK, "2k", "admin_staff_attested"),
    ],
)
def test_a_manual_choice_maps_to_a_platform_and_method(
    choice, exp_platform, exp_method
):
    platform, method = choice.resolved()
    assert (platform.value, method.value) == (exp_platform, exp_method)


@pytest.mark.parametrize(
    "game,platform,expected",
    [
        (
            SupportedGame.CIV6,
            RegistrationPlatform.STEAM,
            [
                RoleIntent.GRANT_CIV6_RANK,
                RoleIntent.GRANT_NOVICE,
                RoleIntent.GRANT_SERVER_NEWS,
                RoleIntent.GRANT_CIV6_NEWS,
                RoleIntent.GRANT_PC_STEAM,
                RoleIntent.REMOVE_NON_VERIFIED,
            ],
        ),
        (
            SupportedGame.CIV6,
            RegistrationPlatform.TWOK,
            [
                RoleIntent.GRANT_CIV6_RANK,
                RoleIntent.GRANT_NOVICE,
                RoleIntent.GRANT_SERVER_NEWS,
                RoleIntent.GRANT_CIV6_NEWS,
                RoleIntent.GRANT_2K_CROSSPLATFORM,
                RoleIntent.REMOVE_NON_VERIFIED,
            ],
        ),
        (
            SupportedGame.CIV7,
            RegistrationPlatform.STEAM,
            [
                RoleIntent.GRANT_CIV7_RANK,
                RoleIntent.GRANT_NOVICE,
                RoleIntent.GRANT_SERVER_NEWS,
                RoleIntent.GRANT_CIV7_NEWS,
                RoleIntent.GRANT_PC_STEAM,
                RoleIntent.REMOVE_NON_VERIFIED,
            ],
        ),
        (
            SupportedGame.CIV7,
            RegistrationPlatform.TWOK,
            [
                RoleIntent.GRANT_CIV7_RANK,
                RoleIntent.GRANT_NOVICE,
                RoleIntent.GRANT_SERVER_NEWS,
                RoleIntent.GRANT_CIV7_NEWS,
                RoleIntent.GRANT_2K_CROSSPLATFORM,
                RoleIntent.REMOVE_NON_VERIFIED,
            ],
        ),
    ],
)
def test_build_registration_role_intents_matrix(game, platform, expected):
    assert role_intents(game, platform) == expected
