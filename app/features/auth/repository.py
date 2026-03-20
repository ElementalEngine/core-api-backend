from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING

from app.features.auth.constants import (
    AUTH_DB_NAME,
    COL_AUDIT_EVENTS,
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
        self._audit_events: AsyncIOMotorCollection = auth_db[COL_AUDIT_EVENTS]
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
        await self._audit_events.create_index(
            [("created_at", ASCENDING)],
            name="auth_audit_created_idx",
        )

    async def get_user_by_discord_id(self, discord_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"discord_id": discord_id})

    async def get_user_by_steam_id(self, steam_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"steam_id": steam_id})

    async def upsert_registered_user(
        self,
        *,
        discord_user_id: str,
        steam_id: str,
        game: str,
        username_snapshot: str | None,
        display_name_snapshot: str | None,
        method: str,
        ownership_verified_at: datetime,
        playtime_minutes: int,
    ) -> None:
        now = datetime.now(timezone.utc)
        registration_doc = {
            "status": "active",
            "method": method,
            "registered_at": now,
            "ownership_verified_at": ownership_verified_at,
            "playtime_minutes": playtime_minutes,
        }
        await self._users.update_one(
            {"discord_id": discord_user_id},
            {
                "$set": {
                    "steam_id": steam_id,
                    "user_name": username_snapshot,
                    "display_name": display_name_snapshot,
                    f"registrations.{game}": registration_doc,
                    "auth_version": 2,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "discord_id": discord_user_id,
                    "created_at": now,
                },
            },
            upsert=True,
        )

    async def insert_registration_session(self, doc: Mapping[str, Any]) -> None:
        await self._sessions.insert_one(dict(doc))

    async def get_registration_session(self, session_id: str) -> dict[str, Any] | None:
        return await self._sessions.find_one({"session_id": session_id})

    async def get_registration_session_by_state(self, state_token: str) -> dict[str, Any] | None:
        return await self._sessions.find_one({"state_token": state_token})

    async def update_registration_session(
        self,
        session_id: str,
        changes: Mapping[str, Any],
    ) -> bool:
        res = await self._sessions.update_one(
            {"session_id": session_id},
            {"$set": dict(changes)},
        )
        return res.matched_count == 1

    async def insert_registration_operation(self, doc: Mapping[str, Any]) -> None:
        await self._operations.insert_one(dict(doc))

    async def get_registration_operation(self, operation_id: str) -> dict[str, Any] | None:
        return await self._operations.find_one({"operation_id": operation_id})

    async def update_registration_operation(
        self,
        operation_id: str,
        changes: Mapping[str, Any],
    ) -> bool:
        res = await self._operations.update_one(
            {"operation_id": operation_id},
            {"$set": dict(changes)},
        )
        return res.matched_count == 1

    async def append_audit_event(self, doc: Mapping[str, Any]) -> None:
        payload = {**dict(doc)}
        payload.setdefault("created_at", datetime.now(timezone.utc))
        await self._audit_events.insert_one(payload)
