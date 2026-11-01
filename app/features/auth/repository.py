from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from pymongo import ASCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_USERS, DB_SERVER_MEMBERS
from app.features.auth.enums import RegistrationPlatform

# A lookup lists at most this many records for one id.
LOOKUP_LIMIT = 25


class AuthRepository:
    """The players' records in `users`."""

    def __init__(self, client: AsyncMongoClient) -> None:
        self._users: AsyncCollection = client[DB_SERVER_MEMBERS][COL_USERS]

    async def ensure_indexes(self) -> None:
        await self._users.create_index(
            [("discord_id", ASCENDING)],
            name="auth_user_discord_id_idx",
        )
        await self._users.create_index(
            [("linked_account_id", ASCENDING)],
            name="auth_user_linked_account_id_idx",
        )
        await self._users.create_index(
            [("linked_platform", ASCENDING), ("linked_account_id", ASCENDING)],
            name="auth_user_linked_platform_account_idx",
        )
        await self._users.create_index(
            [("steam_id", ASCENDING)],
            sparse=True,
            name="auth_user_steam_id_idx",
        )

    async def get_user_by_discord_id(self, discord_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"discord_id": discord_id})

    async def find_users_by_discord_id(self, discord_id: str) -> list[dict[str, Any]]:
        cursor = (
            self._users.find({"discord_id": discord_id})
            .sort(
                [
                    ("linked_platform", ASCENDING),
                    ("linked_account_id", ASCENDING),
                    ("steam_id", ASCENDING),
                ]
            )
            .limit(LOOKUP_LIMIT)
        )
        return await cursor.to_list(length=LOOKUP_LIMIT)

    async def get_user_by_steam_id(self, steam_id: str) -> dict[str, Any] | None:
        return await self._users.find_one(
            {
                "$or": [
                    {"steam_id": steam_id},
                    {"linked_platform": "steam", "linked_account_id": steam_id},
                ]
            }
        )

    async def find_users_by_linked_account_id(
        self, linked_account_id: str
    ) -> list[dict[str, Any]]:
        cursor = (
            self._users.find(
                {
                    "$or": [
                        {"linked_account_id": linked_account_id},
                        {"steam_id": linked_account_id},
                    ]
                }
            )
            .sort(
                [
                    ("discord_id", ASCENDING),
                    ("linked_platform", ASCENDING),
                    ("linked_account_id", ASCENDING),
                ]
            )
            .limit(LOOKUP_LIMIT)
        )
        return await cursor.to_list(length=LOOKUP_LIMIT)

    async def get_user_by_linked_account(
        self, platform: str, account_id: str
    ) -> dict[str, Any] | None:
        return await self._users.find_one(
            {"linked_platform": platform, "linked_account_id": account_id}
        )

    async def upsert_registered_user(
        self,
        *,
        discord_user_id: str,
        discord_username: str | None,
        display_name: str | None,
        linked_platform: str,
        linked_account_id: str,
        linked_account_name: str | None,
        game: str,
        method: str,
        ownership_verified_at: datetime | None,
        playtime_minutes: int | None,
        entry_extra: Mapping[str, Any] | None = None,
    ) -> None:
        now = datetime.now(UTC)
        existing = await self._users.find_one(
            {"discord_id": discord_user_id},
            {
                "discord_username": 1,
                "user_name": 1,
                "display_name": 1,
                "linked_platform": 1,
                "linked_account_id": 1,
                "linked_account_name": 1,
                "steam_id": 1,
                "server_registered_at": 1,
                "first_registered_at": 1,
                "created_at": 1,
                "registrations": 1,
                "__v": 1,
            },
        )

        registration_doc = _build_registration_entry(
            now=now,
            method=method,
            ownership_verified_at=ownership_verified_at,
            playtime_minutes=playtime_minutes,
        )
        if entry_extra:
            registration_doc.update(entry_extra)

        if existing is None:
            await self._users.insert_one(
                _build_new_user_document(
                    discord_user_id=discord_user_id,
                    discord_username=discord_username,
                    display_name=display_name,
                    linked_platform=linked_platform,
                    linked_account_id=linked_account_id,
                    linked_account_name=linked_account_name,
                    game=game,
                    registration_doc=registration_doc,
                    server_registered_at=now,
                )
            )
            return

        await self._users.update_one(
            {"discord_id": discord_user_id},
            _build_existing_user_update(
                existing=existing,
                discord_user_id=discord_user_id,
                discord_username=discord_username,
                display_name=display_name,
                linked_platform=linked_platform,
                linked_account_id=linked_account_id,
                linked_account_name=linked_account_name,
                game=game,
                registration_doc=registration_doc,
            ),
        )


def _build_registration_entry(
    *,
    now: datetime,
    method: str,
    ownership_verified_at: datetime | None,
    playtime_minutes: int | None,
) -> dict[str, Any]:
    return {
        "method": method,
        "registered_at": now,
        "ownership_verified_at": ownership_verified_at,
        "playtime_minutes": playtime_minutes,
    }


def _build_new_user_document(
    *,
    discord_user_id: str,
    discord_username: str | None,
    display_name: str | None,
    linked_platform: str,
    linked_account_id: str,
    linked_account_name: str | None,
    game: str,
    registration_doc: Mapping[str, Any],
    server_registered_at: datetime,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "_id": ObjectId(),
        "discord_id": discord_user_id,
    }
    if discord_username:
        document["discord_username"] = discord_username
    if display_name:
        document["display_name"] = display_name
    document["linked_account_id"] = linked_account_id
    if linked_account_name:
        document["linked_account_name"] = linked_account_name
    document["linked_platform"] = linked_platform
    document["registrations"] = {game: dict(registration_doc)}
    document["server_registered_at"] = server_registered_at
    document["__v"] = 0
    return document


def _build_existing_user_update(
    *,
    existing: Mapping[str, Any],
    discord_user_id: str,
    discord_username: str | None,
    display_name: str | None,
    linked_platform: str,
    linked_account_id: str,
    linked_account_name: str | None,
    game: str,
    registration_doc: Mapping[str, Any],
) -> dict[str, Any]:
    registration_key = f"registrations.{game}"
    raw_username = existing.get("discord_username") or existing.get("user_name")
    existing_username = str(raw_username) if raw_username else None
    current_registration = (existing.get("registrations") or {}).get(game)

    material_changed = any(
        [
            existing_username != discord_username,
            existing.get("display_name") != display_name,
            existing.get("linked_platform") != linked_platform,
            existing.get("linked_account_id") != linked_account_id,
            existing.get("linked_account_name") != linked_account_name,
            current_registration != dict(registration_doc),
        ]
    )

    set_payload: dict[str, Any] = {
        "discord_id": discord_user_id,
        "linked_platform": linked_platform,
        "linked_account_id": linked_account_id,
        registration_key: dict(registration_doc),
    }
    unset_payload: dict[str, str] = {
        "steam_id": "",
        "steam_name": "",
        "locale": "",
        "verified": "",
        "mfa_enabled": "",
        "auth_version": "",
        "updated_at": "",
        "created_at": "",
        "user_name": "",
        "first_registered_at": "",
    }

    if discord_username:
        set_payload["discord_username"] = discord_username
    else:
        unset_payload["discord_username"] = ""

    if display_name:
        set_payload["display_name"] = display_name
    else:
        unset_payload["display_name"] = ""

    if linked_account_name:
        set_payload["linked_account_name"] = linked_account_name
    else:
        unset_payload["linked_account_name"] = ""

    existing_server_registered_at = _resolve_server_registered_at(existing)
    if existing_server_registered_at is not None:
        set_payload["server_registered_at"] = existing_server_registered_at

    update_doc: dict[str, Any] = {
        "$set": set_payload,
        "$unset": unset_payload,
    }
    if material_changed:
        update_doc["$inc"] = {"__v": 1}
    return update_doc


def _resolve_server_registered_at(
    existing: Mapping[str, Any] | None,
) -> datetime | None:
    if not existing:
        return None

    explicit = existing.get("server_registered_at")
    if isinstance(explicit, datetime):
        return explicit

    historical = existing.get("first_registered_at") or existing.get("created_at")
    if isinstance(historical, datetime):
        return historical

    registrations = existing.get("registrations") or {}
    candidates: list[datetime] = []
    if isinstance(registrations, Mapping):
        for value in registrations.values():
            if isinstance(value, Mapping):
                registered_at = value.get("registered_at")
                if isinstance(registered_at, datetime):
                    candidates.append(registered_at)
    return min(candidates) if candidates else None


# Readers of a record, which older writers kept under other field names.
def platform_of(doc: Mapping[str, Any]) -> RegistrationPlatform | None:
    if doc.get("linked_platform"):
        return RegistrationPlatform(str(doc["linked_platform"]))
    if doc.get("steam_id"):
        return RegistrationPlatform.STEAM
    return None


def account_id_of(doc: Mapping[str, Any]) -> str | None:
    if doc.get("linked_account_id"):
        return str(doc["linked_account_id"])
    if doc.get("steam_id"):
        return str(doc["steam_id"])
    return None


def account_name_of(doc: Mapping[str, Any]) -> str | None:
    if doc.get("linked_account_name"):
        return str(doc["linked_account_name"])
    if doc.get("steam_name"):
        return str(doc["steam_name"])
    return None


def username_of(doc: Mapping[str, Any]) -> str | None:
    if doc.get("discord_username"):
        return str(doc["discord_username"])
    if doc.get("user_name"):
        return str(doc["user_name"])
    return None


def display_name_of(doc: Mapping[str, Any]) -> str | None:
    return str(doc["display_name"]) if doc.get("display_name") else None
