from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from bson import ObjectId
from bson.int64 import Int64
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo import ASCENDING, DESCENDING


logger = logging.getLogger(__name__)


# ---- DB / collection names (single source of truth) ----

DB_MATCH_REPORTER = "match_reporter"
COL_PENDING_MATCHES = "pending_matches"
COL_VALIDATED_MATCHES = "validated_matches"

DB_SERVER_MEMBERS = "server_members"
COL_USERS = "users"
COL_SUBS = "subs"

DB_CIV6_LIFETIME = "civ6_lifetime_stats"
DB_CIV7_LIFETIME = "civ7_lifetime_stats"
DB_CIV6_SEASON = "civ6_season_stats"
DB_CIV7_SEASON = "civ7_season_stats"


@dataclass(frozen=True)
class LeaderboardResult:
    rows: List[Dict[str, Any]]
    last_updated: Optional[datetime]


class MongoQueries:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client

        mr = client[DB_MATCH_REPORTER]
        self._pending: AsyncCollection = mr[COL_PENDING_MATCHES]
        self._validated: AsyncCollection = mr[COL_VALIDATED_MATCHES]

        sm = client[DB_SERVER_MEMBERS]
        self._users: AsyncCollection = sm[COL_USERS]
        self._subs: AsyncCollection = sm[COL_SUBS]

    # -------------------- infra --------------------

    async def start_session(self) -> AsyncClientSession:
        return self._client.start_session()

    async def ping(self) -> bool:
        # Motor returns a dict like {"ok": 1.0}
        res = await self._client.admin.command("ping")
        return bool(res.get("ok"))

    async def db_stats(self) -> Dict[str, Any]:
        return await self._client.admin.command("dbstats")

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

    # -------------------- pending matches --------------------

    async def find_pending_by_hash(
        self, save_file_hash: str
    ) -> Optional[Dict[str, Any]]:
        return await self._pending.find_one({"save_file_hash": save_file_hash})

    async def find_pending_by_id(self, oid: ObjectId) -> Optional[Dict[str, Any]]:
        return await self._pending.find_one({"_id": oid})

    async def find_validated_by_id(self, oid: ObjectId) -> Optional[Dict[str, Any]]:
        return await self._validated.find_one({"_id": oid})

    async def insert_pending_match(
        self, match_doc: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> ObjectId:
        res = await self._pending.insert_one(dict(match_doc), session=session)
        return res.inserted_id

    async def update_pending_match_set(
        self,
        oid: ObjectId,
        changes: Mapping[str, Any],
        *,
        session: AsyncClientSession | None = None,
    ) -> bool:
        res = await self._pending.update_one(
            {"_id": oid}, {"$set": dict(changes)}, session=session
        )
        return res.matched_count == 1

    async def replace_pending_match(
        self,
        oid: ObjectId,
        match_doc: Mapping[str, Any],
        *,
        session: AsyncClientSession | None = None,
    ) -> bool:
        res = await self._pending.replace_one(
            {"_id": oid}, dict(match_doc), session=session
        )
        return res.matched_count == 1

    async def delete_pending_match(
        self, oid: ObjectId, *, session: AsyncClientSession | None = None
    ) -> bool:
        res = await self._pending.delete_one({"_id": oid}, session=session)
        return res.deleted_count == 1

    async def delete_validated_match(
        self, oid: ObjectId, *, session: AsyncClientSession | None = None
    ) -> bool:
        res = await self._validated.delete_one({"_id": oid}, session=session)
        return res.deleted_count == 1

    # -------------------- validated matches --------------------

    async def insert_validated_match(
        self, match_doc: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> ObjectId:
        res = await self._validated.insert_one(dict(match_doc), session=session)
        return res.inserted_id

    # -------------------- subs --------------------

    async def inc_subs_in(
        self, discord_id: str, *, session: AsyncClientSession | None = None
    ) -> None:
        await self._subs.update_one(
            {"_id": Int64(discord_id)},
            {"$inc": {"subs_in": 1}},
            upsert=True,
            session=session,
        )

    async def dec_subs_in(
        self, discord_id: str, *, session: AsyncClientSession | None = None
    ) -> None:
        await self._subs.update_one(
            {"_id": Int64(discord_id)},
            {"$inc": {"subs_in": -1}},
            upsert=True,
            session=session,
        )

    # -------------------- stats tables --------------------

    def _stats_db_name(self, *, civ_version: str, is_seasonal: bool) -> str:
        if civ_version == "civ6":
            return DB_CIV6_SEASON if is_seasonal else DB_CIV6_LIFETIME
        return DB_CIV7_SEASON if is_seasonal else DB_CIV7_LIFETIME

    def _stats_collection_name(
        self, *, match_type: str, is_cloud: bool, is_combined: bool
    ) -> str:
        prefix = "pbc_" if is_cloud else "rt_"

        if is_combined:
            return f"{prefix}combined"

        mt = match_type.strip().lower()
        # Accept legacy alias but keep internal naming as 'teamer'.
        if mt == "team":
            mt = "teamer"

        if mt not in {"ffa", "teamer", "duel"}:
            raise ValueError(
                f"Unexpected match_type: {match_type!r} (expected ffa|teamer|duel)"
            )

        return f"{prefix}{mt}"

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
            self._stats_db_name(civ_version=civ_version, is_seasonal=is_seasonal)
        ]
        return db[
            self._stats_collection_name(
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
    ) -> Optional[Dict[str, Any]]:
        col = self._stats_collection(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud,
            is_combined=is_combined,
        )
        return await col.find_one({"_id": Int64(discord_id)})

    # Returns nothing; the annotation is wrong and no caller reads it (S7).
    async def reset_player_stat_doc(  # type: ignore[return]
        self,
        *,
        civ_version: str,
        is_cloud: bool,
        discord_id: str,
    ) -> Optional[Dict[str, Any]]:
        session = await self.start_session()
        async with session:
            async with await session.start_transaction():
                try:
                    stat_reset = {
                        "civ_version": civ_version,
                        "is_cloud": is_cloud,
                        "discord_id": discord_id,
                        "stat_reset": True,
                    }
                    await self.insert_validated_match(stat_reset, session=session)
                    for match_type in ["ffa", "teamer", "duel"]:
                        for is_combined in [False, True]:
                            for is_seasonal in [False, True]:
                                await self._stats_collection(
                                    civ_version=civ_version,
                                    is_seasonal=is_seasonal,
                                    match_type=match_type,
                                    is_cloud=is_cloud,
                                    is_combined=is_combined,
                                ).delete_one(
                                    {"_id": Int64(discord_id)}, session=session
                                )
                    await session.commit_transaction()
                except Exception as e:
                    logger.exception("Transaction failed while writing to DB; aborting")
                    await session.abort_transaction()
                    raise ValueError(f"An error occured during writing to DB: {e}")

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
