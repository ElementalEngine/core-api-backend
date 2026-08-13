from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Dict, Sequence

from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection

from app.core.coerce import as_float
from app.core.config import settings
from app.core.constants import COL_RATING_EVENTS, GAMES_DB
from app.features.ratings.events import build_reset_event
from app.shared.persistence.mongo_queries import COL_VALIDATED_MATCHES, stat_scope

logger = logging.getLogger(__name__)


class RatingsRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client
        self._events: AsyncCollection = client[GAMES_DB][COL_RATING_EVENTS]
        self._validated: AsyncCollection = client[GAMES_DB][COL_VALIDATED_MATCHES]

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
        events: Sequence[Dict[str, Any]],
        *,
        session: AsyncClientSession,
    ) -> None:
        # session is required: a ledger write outside the transaction that
        # moved the rating is the failure this collection exists to detect.
        if not events:
            return
        await self._events.insert_many(list(events), session=session)

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
                    await self._validated.insert_one(
                        {
                            "civ_version": civ_version,
                            "is_cloud": is_cloud,
                            "discord_id": discord_id,
                            "stat_reset": True,
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


__all__ = ["RatingsRepository"]
