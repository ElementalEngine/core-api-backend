from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class StatRow(BaseModel):
    mu: int
    sigma: float
    games: int
    wins: int
    first: int
    subbedIn: int = 0
    subbedOut: int = 0
    lastModified: Optional[datetime] = None


class StatSet(BaseModel):
    ffa: Optional[StatRow] = None
    teamer: Optional[StatRow] = None
    duel: Optional[StatRow] = None


class UserStatsResponse(BaseModel):
    discord_id: str
    civ_version: str
    game_type: str
    lifetime: StatSet
    season: StatSet


class BatchStatsRequest(BaseModel):
    civ_version: str
    game_type: str
    discord_ids: List[str]


class BatchStatsResponse(BaseModel):
    civ_version: str
    game_type: str
    results: List[UserStatsResponse]


class TeamGenRequest(BaseModel):
    civ_version: str
    game_type: str
    discord_ids: List[str]


class TeamGenResponse(BaseModel):
    civ_version: str
    game_type: str
    game_quality: float
    teams: List[List[str]]


__all__ = [
    "BatchStatsRequest",
    "BatchStatsResponse",
    "StatRow",
    "StatSet",
    "TeamGenRequest",
    "TeamGenResponse",
    "UserStatsResponse",
]
