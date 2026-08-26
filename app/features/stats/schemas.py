from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class StatRow(BaseModel):
    mu: int
    sigma: float
    games: int
    wins: int
    first: int
    subbedIn: int = 0
    subbedOut: int = 0
    lastModified: datetime | None = None


class StatSet(BaseModel):
    ffa: StatRow | None = None
    teamer: StatRow | None = None
    duel: StatRow | None = None


class UserStatsResponse(BaseModel):
    discord_id: str
    civ_version: str
    game_type: str
    lifetime: StatSet
    season: StatSet


class BatchStatsRequest(BaseModel):
    civ_version: str
    game_type: str
    discord_ids: list[str]


class BatchStatsResponse(BaseModel):
    civ_version: str
    game_type: str
    results: list[UserStatsResponse]


class TeamGenRequest(BaseModel):
    civ_version: str
    game_type: str
    discord_ids: list[str]


class TeamGenResponse(BaseModel):
    civ_version: str
    game_type: str
    game_quality: float
    teams: list[list[str]]


__all__ = [
    "BatchStatsRequest",
    "BatchStatsResponse",
    "StatRow",
    "StatSet",
    "TeamGenRequest",
    "TeamGenResponse",
    "UserStatsResponse",
]
