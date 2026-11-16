"""One record per member and one member per linked account: the unique indexes on
`users`, and a write that loses the race answered as the clash check would."""

from __future__ import annotations

import asyncio

import pytest
from pymongo.errors import DuplicateKeyError

from app.features.auth.enums import (
    RegistrationMethod,
    RegistrationPlatform,
    SupportedGame,
)
from app.features.auth.errors import AlreadyRegisteredError, SteamIdConflictError
from app.features.auth.registration_v2 import RegistrationFlow
from app.features.auth.repository import AuthRepository

ME = "100000000000000001"
OTHER = "200000000000000002"
STEAM = "76561198000000001"


class _Indexes:
    def __init__(self):
        self.made = {}

    async def create_index(self, keys, **options):
        self.made[options["name"]] = (keys, options)


def test_users_indexes_are_unique():
    repo = AuthRepository.__new__(AuthRepository)
    repo._users = _Indexes()
    asyncio.run(repo.ensure_indexes())
    made = repo._users.made
    assert made["auth_user_discord_id_unique_idx"][1]["unique"] is True
    keys, options = made["auth_user_linked_account_unique_idx"]
    assert keys == [("linked_platform", 1), ("linked_account_id", 1)]
    assert options["unique"] is True
    assert options["partialFilterExpression"] == {
        "linked_account_id": {"$type": "string", "$gt": ""}
    }
    assert "auth_user_discord_id_idx" not in made
    assert "auth_user_linked_platform_account_idx" not in made
    assert "auth_user_steam_id_idx" not in made


class _Users:
    """Empty when checked; by the write, another record holds what was asked."""

    def __init__(self, written):
        self.written = written
        self.by_discord = None
        self.by_account = None

    async def get_user_by_discord_id(self, discord_id):
        return self.by_discord

    async def get_user_by_linked_account(self, platform, account_id):
        return self.by_account

    async def upsert_registered_user(self, **fields):
        self.by_discord, self.by_account = self.written
        raise DuplicateKeyError("E11000 duplicate key")


def _register(users):
    flow = RegistrationFlow.__new__(RegistrationFlow)
    flow._users = users
    flow._client = None
    return flow._register(
        discord_user_id=ME,
        game=SupportedGame.CIV6,
        platform=RegistrationPlatform.STEAM,
        method=RegistrationMethod.OAUTH_STEAM_API,
        account_id=STEAM,
        account_name=None,
        username=None,
        display_name=None,
    )


def test_account_taken_between_check_and_write_is_a_clash():
    other = {
        "discord_id": OTHER,
        "linked_platform": "steam",
        "linked_account_id": STEAM,
    }
    with pytest.raises(SteamIdConflictError):
        asyncio.run(_register(_Users((None, other))))


def test_own_second_sign_in_is_already_registered():
    mine = {
        "discord_id": ME,
        "linked_platform": "steam",
        "linked_account_id": STEAM,
        "registrations": {"civ6": {"method": "oauth_steam_api"}},
    }
    with pytest.raises(AlreadyRegisteredError):
        asyncio.run(_register(_Users((mine, mine))))
