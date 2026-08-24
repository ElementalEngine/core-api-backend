from __future__ import annotations

from typing import Any, Dict, Optional

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_USERS, DB_SERVER_MEMBERS


class MongoQueries:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client

        sm = client[DB_SERVER_MEMBERS]
        self._users: AsyncCollection = sm[COL_USERS]

    async def start_session(self) -> AsyncClientSession:
        return self._client.start_session()

    async def get_user_by_discord_id(self, discord_id: str) -> Optional[Dict[str, Any]]:
        return await self._users.find_one({"discord_id": discord_id})

    async def get_user_by_steam_id(self, steam_id: str) -> Optional[Dict[str, Any]]:
        return await self._users.find_one(
            {
                "$or": [
                    {"steam_id": steam_id},
                    {"linked_platform": "steam", "linked_account_id": steam_id},
                ]
            }
        )
