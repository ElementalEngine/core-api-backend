"""Response models for the v2 stats surface.

`wins` answers how often a player won, which is not the same question as how
often their rating went up. The two diverge in free-for-all, where placing
second can still gain rating.

Governed by D164.
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


class ResetStatsResponseV2(UserStatsResponseV2):
    """A reset's answer: the stats as they stood before it, and as they start
    again after it -- every mode that had a row, back at the starting rating."""

    after_lifetime: StatSetV2
    after_season: StatSetV2

    @classmethod
    def of(
        cls, before: UserStatsResponse, *, mu: float, sigma: float
    ) -> ResetStatsResponseV2:
        fresh = StatRowV2(mu=round(mu), sigma=sigma, games=0, first=0, rating_gains=0)

        def restarted(stats: StatSet) -> StatSetV2:
            return StatSetV2(
                ffa=fresh if stats.ffa else None,
                teamer=fresh if stats.teamer else None,
                duel=fresh if stats.duel else None,
            )

        v2 = UserStatsResponseV2.from_v1(before)
        return cls(
            **v2.model_dump(),
            after_lifetime=restarted(before.lifetime),
            after_season=restarted(before.season),
        )


class BatchStatsResponseV2(BaseModel):
    civ_version: str
    game_type: str
    results: list[UserStatsResponseV2]


class HistoryGame(BaseModel):
    """One approved game, as the player saw it. A placement of 0 is first;
    in a team game it is the team's place."""

    match_id: str
    reported_at: datetime
    game_mode: str
    map_type: str
    turn: int | None = None
    civ: str | None = None
    leader: str | None = None
    placement: int | None = None
    sides: int
    delta: int
    subbed_in: bool = False
    subbed_out: bool = False


class HistoryPage(BaseModel):
    discord_id: str
    civ_version: str
    game_type: str
    page: int
    page_size: int
    has_more: bool
    games: list[HistoryGame]


class MapRecord(BaseModel):
    """Games on a map, those whose rating went up, and first places."""

    map_type: str
    games: int
    wins: int
    firsts: int


class CivRecord(BaseModel):
    """A civ6 leader token, or a civ7 civ token, and how it went."""

    token: str
    games: int
    wins: int
    firsts: int


class StatsSummary(BaseModel):
    """Games by map and the most played civs, since the last reset if any.
    A win is a game whose rating went up; a first is first place."""

    discord_id: str
    civ_version: str
    game_type: str
    since: datetime | None = None
    maps: list[MapRecord]
    civs: list[CivRecord]


__all__ = [
    "BatchStatsResponseV2",
    "CivRecord",
    "HistoryGame",
    "HistoryPage",
    "MapRecord",
    "ResetStatsResponseV2",
    "StatRowV2",
    "StatSetV2",
    "StatsSummary",
    "UserStatsResponseV2",
]
