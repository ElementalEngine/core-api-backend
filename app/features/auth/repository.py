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
        await self._audit_events.create_index(
            [("created_at", ASCENDING)],
            name="auth_audit_created_idx",
        )

    async def get_user_by_discord_id(self, discord_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"discord_id": discord_id})

    async def get_user_by_steam_id(self, steam_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"steam_id": steam_id})

    async def insert_registration_session(self, doc: Mapping[str, Any]) -> None:
        await self._sessions.insert_one(dict(doc))

    async def get_registration_session(self, session_id: str) -> dict[str, Any] | None:
        return await self._sessions.find_one({"session_id": session_id})

    async def get_registration_session_by_state(
        self,
        state_token: str,
    ) -> dict[str, Any] | None:
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

    async def append_audit_event(self, doc: Mapping[str, Any]) -> None:
        payload = {**dict(doc)}
        payload.setdefault("created_at", datetime.now(timezone.utc))
        await self._audit_events.insert_one(payload)
