from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime

class CivCount(BaseModel):
    civ: str
    count: int

class StatModel(BaseModel):
    index: int
    id: int  # discord_id
    mu: float
    sigma: float
    games: int
    wins: int
    first: int
    subbedIn: int
    subbedOut: int
    civs: Optional[Dict[str, int]] = None
    lastModified: datetime = Field(default_factory=datetime.utcnow)

class PlayerModel(BaseModel):
    steam_id: Optional[str] = None
    user_name: Optional[str] = None
    civ: str
    team: int
    leader: Optional[str] = None
    player_alive: Optional[bool] = None
    discord_id: Optional[str] = None
    placement: Optional[int] = None
    quit: bool = False
    delta: float = 0.0
    season_delta: Optional[float] = None
    combined_delta: Optional[float] = None
    is_sub: bool = False
    subbed_out: bool = False

class MatchModel(BaseModel):
    game: str  # parsers return "civ6" or "civ7"
    turn: int
    age: Optional[str] = None
    map_type: str
    game_mode: str  # allow "", "FFA", "Teamer", "Duel"
    is_cloud: bool
    players: List[PlayerModel]
    parser_version: str
    discord_messages_id_list: List[str]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    approved_at: Optional[datetime] = None
    approver_discord_id: Optional[str] = None
    flagged: bool = False
    flagged_by: Optional[str] = None
    save_file_hash: str
    reporter_discord_id: str