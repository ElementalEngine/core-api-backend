"""The v2 stats read shape: one field renamed, nothing else changed.

`wins` counts games where the rating went up, which is about half of all
games in every mode by construction -- it is not a win count. Measured
across 340k games: in duel `wins == first` exactly, in teamer they agree
99.87% of the time, and in FFA they are five times apart. So the two
counters answer different questions only in FFA, and only `first` ever
answers "how often did this player win" (D164, phase3-sequence section 4
item 70).

C2 requires a v2 read surface to state which counter it is showing, so v2
says `rating_gains` and keeps `first` unchanged. `mu` stays an int here:
StatsService rounds it in _doc_to_row, and a mapper cannot recover
precision that is already gone (section 4 item 95).
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.features.stats.schemas import StatRow, StatSet, UserStatsResponse


class StatRowV2(BaseModel):
    mu: int
    sigma: float
    games: int
    first: int
    rating_gains: int
    subbedIn: int = 0
    subbedOut: int = 0
    lastModified: datetime | None = None

    @classmethod
    def from_v1(cls, row: StatRow) -> StatRowV2:
        fields = row.model_dump()
        return cls(rating_gains=fields.pop("wins"), **fields)


class StatSetV2(BaseModel):
    ffa: StatRowV2 | None = None
    teamer: StatRowV2 | None = None
    duel: StatRowV2 | None = None

    @classmethod
    def from_v1(cls, stats: StatSet) -> StatSetV2:
        return cls(
            ffa=StatRowV2.from_v1(stats.ffa) if stats.ffa else None,
            teamer=StatRowV2.from_v1(stats.teamer) if stats.teamer else None,
            duel=StatRowV2.from_v1(stats.duel) if stats.duel else None,
        )


class UserStatsResponseV2(BaseModel):
    discord_id: str
    civ_version: str
    game_type: str
    lifetime: StatSetV2
    season: StatSetV2

    @classmethod
    def from_v1(cls, response: UserStatsResponse) -> UserStatsResponseV2:
        return cls(
            discord_id=response.discord_id,
            civ_version=response.civ_version,
            game_type=response.game_type,
            lifetime=StatSetV2.from_v1(response.lifetime),
            season=StatSetV2.from_v1(response.season),
        )


class BatchStatsResponseV2(BaseModel):
    civ_version: str
    game_type: str
    results: list[UserStatsResponseV2]


__all__ = [
    "BatchStatsResponseV2",
    "StatRowV2",
    "StatSetV2",
    "UserStatsResponseV2",
]
