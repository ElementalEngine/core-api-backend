from __future__ import annotations

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_RATING_EVENTS, GAMES_DB


class RatingsRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._events: AsyncCollection = client[GAMES_DB][COL_RATING_EVENTS]

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


__all__ = ["RatingsRepository"]
