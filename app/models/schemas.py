from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class PlayerSchema(BaseModel):
    steam_id: Optional[str] = None
    user_name: Optional[str] = None
    civ: str
    team: int
    leader: Optional[str] = None
    player_alive: Optional[bool] = None
    discord_id: Optional[str] = None
    placement: Optional[int] = None
    quit: bool
    delta: float = 0.0
    season_delta: Optional[float] = None
    combined_delta: Optional[float] = None
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
    age: Optional[str] = None
    map_type: str
    game_mode: str
    is_cloud: bool
    players: List[PlayerSchema]
    parser_version: str
    discord_messages_id_list: List[str]
    created_at: datetime
    approved_at: Optional[datetime] = None
    approver_discord_id: Optional[str] = None
    flagged: bool
    flagged_by: Optional[str] = None
    save_file_hash: str
    reporter_discord_id: str
    contest_report_list: List[ContestReport]

    # Optional: returned on approval to allow the bot to update rank roles.
    affected_players: Optional[List[AffectedPlayerRating]] = None

class MatchUpdate(BaseModel):
    match_id: str
    players: Optional[List[PlayerSchema]] = None
    confirmed: Optional[bool] = None
    flagged: Optional[bool] = None
    flagged_by: Optional[str] = None

class SetPlayerOrder(BaseModel):
    match_id: str
    player_order: str # The players in order with their discord id. e.g. "Calcifer Cisco Canuck ..." separated by spaces and specify ties with "TIE"
    discord_message_id: str

class ChangeOrder(BaseModel):
    match_id: str
    new_order: str # The order of players as a string, e.g. "1 2 3 4" separated by spaces
    discord_message_id: str

class DeletePendingMatch(BaseModel):
    match_id: str

class TriggerQuit(BaseModel):
    match_id: str
    quitter_discord_id: str
    discord_message_id: str

class AppendDiscordMessageID(BaseModel):
    match_id: str
    discord_message_id: List[str]

class AssignDiscordId(BaseModel):
    match_id: str
    player_id: str
    player_discord_id: str
    discord_message_id: str

class AssignDiscordIdAll(BaseModel):
    match_id: str
    discord_id_list: List[str]
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
    rankings: List[PlayerLeaderboard]
    last_updated: int

# -------------------- stats API --------------------

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