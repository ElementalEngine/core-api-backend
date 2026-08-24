from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from bson.int64 import Int64
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo import ASCENDING, DESCENDING

from app.core.constants import COL_USERS, DB_SERVER_MEMBERS
from app.features.ratings.scope import stats_collection_name, stats_db_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeaderboardResult:
    rows: List[Dict[str, Any]]
    last_updated: Optional[datetime]


class MongoQueries:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client

        sm = client[DB_SERVER_MEMBERS]
        self._users: AsyncCollection = sm[COL_USERS]

    # -------------------- infra --------------------

    async def start_session(self) -> AsyncClientSession:
        return self._client.start_session()

    # -------------------- users --------------------

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

    # -------------------- stats tables --------------------

    def _stats_collection(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        match_type: str,
        is_cloud: bool,
        is_combined: bool,
    ) -> AsyncCollection:
        db = self._client[
            stats_db_name(civ_version=civ_version, is_seasonal=is_seasonal)
        ]
        return db[
            stats_collection_name(
                match_type=match_type, is_cloud=is_cloud, is_combined=is_combined
            )
        ]

    async def get_player_stat_doc(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        match_type: str,
        is_cloud: bool,
        is_combined: bool,
        discord_id: str,
        session: Optional[AsyncClientSession] = None,
    ) -> Optional[Dict[str, Any]]:
        col = self._stats_collection(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud,
            is_combined=is_combined,
        )
        return await col.find_one({"_id": Int64(discord_id)}, session=session)

    async def get_player_stat_docs_batch(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        match_type: str,
        is_cloud: bool,
        is_combined: bool,
        discord_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Batch fetch stat docs by discord id.

        Returns mapping: discord_id -> doc for ids that exist.
        Missing ids are simply absent.
        """
        if not discord_ids:
            return {}

        col = self._stats_collection(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud,
            is_combined=is_combined,
        )

        ids = [Int64(did) for did in discord_ids]
        cursor = col.find({"_id": {"$in": ids}})
        docs = await cursor.to_list(length=len(ids))

        out: Dict[str, Dict[str, Any]] = {}
        for d in docs:
            did = d.get("_id")
            if did is None:
                continue
            out[str(int(did))] = d
        return out

    async def upsert_player_stat_doc(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        match_type: str,
        is_cloud: bool,
        is_combined: bool,
        discord_id: str,
        doc: Mapping[str, Any],
        session: AsyncClientSession | None = None,
    ) -> None:
        col = self._stats_collection(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud,
            is_combined=is_combined,
        )
        await col.replace_one(
            {"_id": Int64(discord_id)}, dict(doc), upsert=True, session=session
        )

    async def get_leaderboard(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        match_type: str,
        is_cloud: bool,
        is_combined: bool,
        min_games: int = 3,
        limit: int = 100,
    ) -> LeaderboardResult:
        col = self._stats_collection(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud,
            is_combined=is_combined,
        )

        last = await col.find_one(
            {}, sort=[("lastModified", DESCENDING)], projection={"lastModified": 1}
        )
        last_updated = (last or {}).get("lastModified")

        cursor = (
            col.find({"games": {"$gte": min_games}})
            .sort({"mu": DESCENDING, "sigma": ASCENDING})
            .limit(limit)
        )
        rows = await cursor.to_list(length=limit)
        return LeaderboardResult(rows=rows, last_updated=last_updated)
