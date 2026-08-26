from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from collections.abc import Mapping, Sequence

from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection

from app.core.coerce import as_float
from app.core.config import settings
from app.core.constants import COL_RATING_EVENTS, COL_STAT_RESETS, GAMES_DB
from app.features.ratings.events import build_reset_event
from app.features.ratings.scope import (
    stat_scope,
    stats_collection_name,
    stats_db_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeaderboardResult:
    rows: list[dict[str, Any]]
    last_updated: datetime | None


class RatingsRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client
        self._events: AsyncCollection = client[GAMES_DB][COL_RATING_EVENTS]
        self._resets: AsyncCollection = client[GAMES_DB][COL_STAT_RESETS]

    async def ensure_indexes(self) -> None:
        # Reset events carry no match_id. Without the partial filter a unique
        # index treats every missing value as one null and collides them on a
        # player's second reset.
        await self._events.create_index(
            [
                ("match_id", ASCENDING),
                ("player_id", ASCENDING),
                ("scope", ASCENDING),
                ("event_type", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={"match_id": {"$exists": True}},
            name="rating_events_match_event_uq",
        )
        await self._events.create_index(
            [
                ("player_id", ASCENDING),
                ("scope", ASCENDING),
                ("match_created_at", DESCENDING),
            ],
            name="rating_events_player_history_idx",
        )

    async def insert_events(
        self,
        events: Sequence[dict[str, Any]],
        *,
        session: AsyncClientSession,
    ) -> None:
        # session is required: a ledger write outside the transaction that
        # moved the rating is the failure this collection exists to detect.
        if not events:
            return
        await self._events.insert_many(list(events), session=session)

    # -------------------- stat documents --------------------

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
        session: AsyncClientSession | None = None,
    ) -> dict[str, Any] | None:
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
        discord_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
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

        out: dict[str, dict[str, Any]] = {}
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

    async def reset_player_stats(
        self, *, civ_version: str, is_cloud: bool, discord_id: str
    ) -> None:
        occurred_at = datetime.now(UTC)
        pid = Int64(discord_id)

        # is_combined ignores match_type, so the twelve combinations name only
        # eight documents. One event per distinct scope, or a reset counts its
        # combined movement three times.
        scopes = sorted(
            {
                stat_scope(
                    civ_version=civ_version,
                    is_seasonal=is_seasonal,
                    match_type=match_type,
                    is_cloud=is_cloud,
                    is_combined=is_combined,
                )
                for match_type in ("ffa", "teamer", "duel")
                for is_combined in (False, True)
                for is_seasonal in (False, True)
            }
        )

        session = self._client.start_session()
        async with session:
            async with await session.start_transaction():
                try:
                    # Its own collection since Entry 6, so no marker flag
                    # and a real date rather than one hidden in the ObjectId.
                    await self._resets.insert_one(
                        {
                            "occurred_at": occurred_at,
                            "civ_version": civ_version,
                            "is_cloud": is_cloud,
                            "discord_id": discord_id,
                        },
                        session=session,
                    )

                    events = []
                    for scope in scopes:
                        db_name, col_name = scope.split(".")
                        col = self._client[db_name][col_name]
                        doc = await col.find_one({"_id": pid}, session=session)
                        await col.delete_one({"_id": pid}, session=session)
                        # A scope with no document still gets an event: the
                        # player's effective rating is ts_mu either way, and
                        # the chain stays continuous for reconciliation.
                        events.append(
                            build_reset_event(
                                occurred_at=occurred_at,
                                discord_id=discord_id,
                                scope=scope,
                                mu_before=as_float(
                                    doc.get("mu") if doc else None, settings.ts_mu
                                ),
                                mu_after=settings.ts_mu,
                                sigma_before=as_float(
                                    doc.get("sigma") if doc else None, settings.ts_sigma
                                ),
                                sigma_after=settings.ts_sigma,
                            )
                        )

                    await self.insert_events(events, session=session)
                    await session.commit_transaction()
                except Exception:
                    logger.exception("Stat reset transaction failed; aborting")
                    await session.abort_transaction()
                    raise

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


__all__ = ["LeaderboardResult", "RatingsRepository"]
