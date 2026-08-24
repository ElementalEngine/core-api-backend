from __future__ import annotations

from typing import Dict, List, Optional

from pymongo import AsyncMongoClient

from app.features.ratings.repository import RatingsRepository
from app.shared.persistence.mongo_queries import MongoQueries


class StatsRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._queries = MongoQueries(client)
        self._ratings = RatingsRepository(client)

    async def get_player_stat_docs_batch(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        match_type: str,
        is_cloud: bool,
        is_combined: bool,
        discord_ids: List[str],
    ) -> Dict[str, Dict[str, object]]:
        return await self._queries.get_player_stat_docs_batch(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud,
            is_combined=is_combined,
            discord_ids=discord_ids,
        )

    async def reset_player_stat_doc(
        self,
        *,
        civ_version: str,
        is_cloud: bool,
        discord_id: str,
    ) -> Optional[Dict[str, object]]:
        await self._ratings.reset_player_stats(
            civ_version=civ_version,
            is_cloud=is_cloud,
            discord_id=discord_id,
        )
        return None
