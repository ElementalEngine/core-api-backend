from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.features.matches.models import ContestReport


class PlayerSchema(BaseModel):
    steam_id: str | None = None
    user_name: str | None = None
    civ: str
    team: int
    leader: str | None = None
    player_alive: bool | None = None
    discord_id: str | None = None
    placement: int | None = None
    quit: bool
    delta: float = 0.0
    season_delta: float | None = None
    combined_delta: float | None = None
    is_sub: bool = False
    subbed_out: bool = False
    # The in-game score when the save was read, where the save records it.
    score: int | None = None


class AffectedPlayerRating(BaseModel):
    discord_id: str
    rating_mu: float


class MatchResponse(BaseModel):
    match_id: str
    game: str
    turn: int
    age: str | None = None
    map_type: str
    game_mode: str
    is_cloud: bool
    players: list[PlayerSchema]
    parser_version: str
    discord_messages_id_list: list[str]
    created_at: datetime
    approved_at: datetime | None = None
    approver_discord_id: str | None = None
    flagged: bool
    flagged_by: str | None = None
    save_file_hash: str
    reporter_discord_id: str
    contest_report_list: list[ContestReport]
    affected_players: list[AffectedPlayerRating] | None = None
    # The season it was approved in, else the open one, such as "Season 6".
    season_label: str | None = None
    # The approved result's post in the history channel, where one was recorded.
    history_message_id: str | None = None


class PlayerLeaderboard(BaseModel):
    discord_id: str
    rating: int
    games_played: int
    wins: int
    first: int


class LeaderboardRankingResponse(BaseModel):
    rankings: list[PlayerLeaderboard]
    last_updated: int


__all__ = [
    "AffectedPlayerRating",
    "ContestReport",
    "LeaderboardRankingResponse",
    "MatchResponse",
    "PlayerLeaderboard",
    "PlayerSchema",
]
