"""The leaderboard read, carved out of MatchService (D114).

It touches no match collection: the rows come from a stat collection via
RatingsRepository, and the only match-domain thing about it is the route it
is served from. Both the v1 and the v2 handler call it directly, so nothing
delegates through MatchService to reach it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pymongo import AsyncMongoClient

from app.core.coerce import as_float, as_int
from app.features.ratings.repository import RatingsRepository

MIN_GAMES = 3
LIMIT = 100
CLOUD_VALUES = {"pbc", "cloud", "true", "1"}


class LeaderboardService:
    def __init__(self, client: AsyncMongoClient) -> None:
        self.ratings = RatingsRepository(client)

    async def get_leaderboard(
        self,
        match_type: str,
        is_cloud: str,
        is_seasonal: bool,
        is_combined: bool,
        civ_version: str,
    ) -> Dict[str, Any]:
        is_cloud_game = str(is_cloud).strip().lower() in CLOUD_VALUES

        lb = await self.ratings.get_leaderboard(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud_game,
            is_combined=is_combined,
            min_games=MIN_GAMES,
            limit=LIMIT,
        )

        out: List[Dict[str, Any]] = []
        for idx, row in enumerate(lb.rows or [], start=1):
            did = str(row.get("_id"))
            mu = as_float(row.get("mu"), 0.0)
            games = as_int(row.get("games"), 0)
            out.append(
                {
                    "rank": idx,
                    "discord_id": did,
                    "mu": mu,
                    "sigma": as_float(row.get("sigma"), 0.0),
                    "games": games,
                    # Backwards-compatible aliases for older clients.
                    "rating": int(round(mu)),
                    "games_played": games,
                    "wins": as_int(row.get("wins"), 0),
                    "first": as_int(row.get("first"), 0),
                }
            )
        last_updated_ts = (
            int(lb.last_updated.timestamp())
            if isinstance(lb.last_updated, datetime)
            else 0
        )
        return {"rankings": out, "last_updated": last_updated_ts}


__all__ = ["LeaderboardService"]
