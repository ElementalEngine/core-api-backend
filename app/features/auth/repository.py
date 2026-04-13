from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING

from app.features.auth.constants import (
    AUTH_DB_NAME,
    AUTH_OPERATION_RETENTION_SECONDS,
    COL_REGISTRATION_OPERATIONS,
    COL_REGISTRATION_SESSIONS,
)
from app.shared.persistence.mongo_queries import COL_USERS, DB_SERVER_MEMBERS


class AuthRepository:
    def __init__(self, client: AsyncIOMotorClient) -> None:
        auth_db = client[AUTH_DB_NAME]
        members = client[DB_SERVER_MEMBERS]
        self._sessions: AsyncIOMotorCollection = auth_db[COL_REGISTRATION_SESSIONS]
        self._operations: AsyncIOMotorCollection = auth_db[COL_REGISTRATION_OPERATIONS]
        self._users: AsyncIOMotorCollection = members[COL_USERS]

    async def ensure_indexes(self) -> None:
        await self._sessions.create_index(
            [("session_id", ASCENDING)],
            unique=True,
            name="auth_session_id_uq",
        )
        await self._sessions.create_index(
            [("state_token", ASCENDING)],
            unique=True,
            name="auth_state_token_uq",
        )
        await self._sessions.create_index(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="auth_session_ttl",
        )
        await self._sessions.create_index(
            [("discord_user_id", ASCENDING)],
            name="auth_session_discord_id_idx",
        )
        await self._operations.create_index(
            [("operation_id", ASCENDING)],
            unique=True,
            name="auth_operation_id_uq",
        )
        await self._operations.create_index(
            [("discord_user_id", ASCENDING)],
            name="auth_operation_discord_id_idx",
        )
        await self._operations.create_index(
            [("completed_at", ASCENDING)],
            expireAfterSeconds=AUTH_OPERATION_RETENTION_SECONDS,
            partialFilterExpression={"completed_at": {"$exists": True}},
            name="auth_operation_completed_ttl",
        )
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

    async def get_user_by_steam_id(self, steam_id: str) -> dict[str, Any] | None:
        return await self._users.find_one(
            {
                "$or": [
                    {"steam_id": steam_id},
                    {"linked_platform": "steam", "linked_account_id": steam_id},
                ]
            }
        )

    async def get_user_by_linked_account(self, platform: str, account_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"linked_platform": platform, "linked_account_id": account_id})

    async def insert_registration_session(self, doc: Mapping[str, Any]) -> None:
        await self._sessions.insert_one(dict(doc))

    async def get_registration_session(self, session_id: str) -> dict[str, Any] | None:
        return await self._sessions.find_one({"session_id": session_id})

    async def get_registration_session_by_state(self, state_token: str) -> dict[str, Any] | None:
        return await self._sessions.find_one({"state_token": state_token})

    async def update_registration_session(self, session_id: str, changes: Mapping[str, Any]) -> bool:
        res = await self._sessions.update_one({"session_id": session_id}, {"$set": dict(changes)})
        return res.matched_count == 1

    async def insert_registration_operation(self, doc: Mapping[str, Any]) -> None:
        await self._operations.insert_one(dict(doc))

    async def get_registration_operation(self, operation_id: str) -> dict[str, Any] | None:
        return await self._operations.find_one({"operation_id": operation_id})

    async def update_registration_operation(self, operation_id: str, changes: Mapping[str, Any]) -> bool:
        res = await self._operations.update_one({"operation_id": operation_id}, {"$set": dict(changes)})
        return res.matched_count == 1

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
    ) -> None:
        now = datetime.now(timezone.utc)
        registration_key = f"registrations.{game}"
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

        registration_doc = {
            "method": method,
            "registered_at": now,
            "ownership_verified_at": ownership_verified_at,
            "playtime_minutes": playtime_minutes,
        }

        existing_username = None
        if existing:
            raw_username = existing.get("discord_username") or existing.get("user_name")
            existing_username = str(raw_username) if raw_username else None

        current_registration = ((existing or {}).get("registrations") or {}).get(game)
        material_changed = existing is None or any(
            [
                existing_username != discord_username,
                (existing or {}).get("display_name") != display_name,
                (existing or {}).get("linked_platform") != linked_platform,
                (existing or {}).get("linked_account_id") != linked_account_id,
                (existing or {}).get("linked_account_name") != linked_account_name,
                current_registration != registration_doc,
            ]
        )

        set_payload: dict[str, Any] = {
            "discord_id": discord_user_id,
            "linked_platform": linked_platform,
            "linked_account_id": linked_account_id,
            registration_key: registration_doc,
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

        server_registered_at = self._resolve_server_registered_at(existing, now)
        if server_registered_at:
            set_payload["server_registered_at"] = server_registered_at
        else:
            unset_payload["server_registered_at"] = ""

        if material_changed:
            set_payload["__v"] = int(existing.get("__v", 0)) + 1 if existing else 1

        update_doc: dict[str, Any] = {"$set": set_payload}
        if unset_payload:
            update_doc["$unset"] = unset_payload
        await self._users.update_one({"discord_id": discord_user_id}, update_doc, upsert=True)

    @staticmethod
    def _resolve_server_registered_at(existing: Mapping[str, Any] | None, fallback: datetime) -> datetime:
        if not existing:
            return fallback

        for key in ("server_registered_at", "first_registered_at", "created_at"):
            value = existing.get(key)
            if isinstance(value, datetime):
                return value

        registrations = existing.get("registrations") or {}
        if isinstance(registrations, dict):
            candidates = [
                value.get("registered_at")
                for value in registrations.values()
                if isinstance(value, dict) and isinstance(value.get("registered_at"), datetime)
            ]
            if candidates:
                return min(candidates)

        return fallback
