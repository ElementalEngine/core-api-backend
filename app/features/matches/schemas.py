from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


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


class ContestReport(BaseModel):
    contestor_discord_id: str
    reason: str


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


class MatchUpdate(BaseModel):
    match_id: str
    players: list[PlayerSchema] | None = None
    confirmed: bool | None = None
    flagged: bool | None = None
    flagged_by: str | None = None


class SetPlayerOrder(BaseModel):
    match_id: str
    player_order: str
    discord_message_id: str


class ChangeOrder(BaseModel):
    match_id: str
    new_order: str
    discord_message_id: str


class DeletePendingMatch(BaseModel):
    match_id: str


class TriggerQuit(BaseModel):
    match_id: str
    quitter_discord_id: str
    discord_message_id: str


class AppendDiscordMessageID(BaseModel):
    match_id: str
    discord_message_id: list[str]


class AssignDiscordId(BaseModel):
    match_id: str
    player_id: str
    player_discord_id: str
    discord_message_id: str


class AssignDiscordIdAll(BaseModel):
    match_id: str
    discord_id_list: list[str]
    discord_message_id: str


class AssignSub(BaseModel):
    match_id: str
    sub_in_id: str
    sub_out_discord_id: str
    discord_message_id: str


class RemoveSub(BaseModel):
    match_id: str
    sub_out_id: str
    discord_message_id: str


class ContestReportRequest(BaseModel):
    match_id: str
    contestor_discord_id: str
    reason: str
    discord_message_id: str


class RevertMatchRequest(BaseModel):
    match_id: str


class ApproveMatch(BaseModel):
    match_id: str
    approver_discord_id: str


class GetLeaderboardRequest(BaseModel):
    game: str
    game_type: str
    game_mode: str
    is_seasonal: bool
    is_combined: bool


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
    "AppendDiscordMessageID",
    "ApproveMatch",
    "AssignDiscordId",
    "AssignDiscordIdAll",
    "AssignSub",
    "ChangeOrder",
    "ContestReport",
    "ContestReportRequest",
    "DeletePendingMatch",
    "GetLeaderboardRequest",
    "LeaderboardRankingResponse",
    "MatchResponse",
    "MatchUpdate",
    "PlayerLeaderboard",
    "PlayerSchema",
    "RemoveSub",
    "RevertMatchRequest",
    "SetPlayerOrder",
    "TriggerQuit",
]
