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


StatsMode = Literal["ffa", "teamer", "duel"]
PickKind = Literal["leader", "civ"]


class MapGames(BaseModel):
    map: str
    games: int


class DraftBan(BaseModel):
    token: str
    name: str
    kind: PickKind
    bans: int


class PickRecord(BaseModel):
    """A leader or civ this season. A win is a game whose rating went up;
    win_rate is null below the season stats' minimum games."""

    token: str
    name: str
    picks: int
    wins: int
    firsts: int
    win_rate: float | None


class MapPick(BaseModel):
    """avg_placement counts first place as 1."""

    token: str
    name: str
    games: int
    firsts: int
    avg_placement: float


class MapPicks(BaseModel):
    map: str
    kind: PickKind
    rows: list[MapPick]


class ModeStats(BaseModel):
    """One mode's season: civs are listed on Civ 7 only, and map_picks on
    FFA only."""

    mode: StatsMode
    games: int
    maps: list[MapGames]
    draft_bans: list[DraftBan]
    leaders: list[PickRecord]
    civs: list[PickRecord]
    map_picks: list[MapPicks]


class SeasonStats(BaseModel):
    """An edition's open season, counted from its approved real-time games
    and the bans of its drafts."""

    updated_at: datetime
    edition: str
    season_label: str
    season_started_at: datetime
    min_games: int
    map_top_n: int
    modes: list[ModeStats]


__all__ = [
    "DraftBan",
    "Ladder",
    "LadderKey",
    "LadderRow",
    "Ladders",
    "MapGames",
    "MapPick",
    "MapPicks",
    "ModeStats",
    "Overview",
    "PickKind",
    "PickRecord",
    "SeasonStats",
    "StatsMode",
]
