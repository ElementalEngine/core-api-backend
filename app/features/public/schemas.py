"""What the website's read routes answer. Players appear by name only, never
by their Discord id."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

LadderKey = Literal["season-ffa", "season-teamer", "season-duel", "cloud"]


class Overview(BaseModel):
    """League totals: everyone who has registered, and every approved match."""

    updated_at: datetime
    registered_players: int
    matches_played: int


class LadderRow(BaseModel):
    rank: int
    display_name: str
    rating: int
    mu: float
    sigma: float
    games: int
    wins: int
    first: int
    last_active_at: datetime | None


class Ladder(BaseModel):
    key: LadderKey
    total: int
    rows: list[LadderRow]


class Ladders(BaseModel):
    """An edition's ladders: this season's per mode, and cloud over all time."""

    updated_at: datetime
    edition: str
    season_label: str | None
    season_started_at: datetime | None
    min_games: int
    ladders: list[Ladder]


__all__ = ["Ladder", "LadderKey", "LadderRow", "Ladders", "Overview"]
