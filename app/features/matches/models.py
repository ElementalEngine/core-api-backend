from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class StatModel(BaseModel):
    """Stat row used for ranking/stats calculations.

    Canonical identifier across the codebase is discord_id (string). Stats collections
    historically used Mongo _id equal to the Discord ID (often numeric). This model
    accepts either shape and normalizes.
    """

    index: int

    # Canonical outward identifier.
    discord_id: str

    # Back-compat: some call sites / docs refer to the stats document id.
    # We keep it optional and derive from discord_id where possible.
    id: Optional[int] = None

    mu: float
    sigma: float
    games: int
    wins: int
    first: int

    # Subbing fields are not present in older docs; keep safe defaults.
    subbedIn: int = 0
    subbedOut: int = 0

    # Civs map is historically {"CivName": <int games>}.
    civs: Optional[Dict[str, Any]] = None

    lastModified: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="before")
    @classmethod
    def _normalize_ids(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        # Accept Mongo-style `_id` or legacy `id`.
        raw_id = data.get("id")
        if raw_id is None and "_id" in data:
            raw_id = data.get("_id")

        # Accept legacy `discord_id`.
        raw_discord_id = data.get("discord_id")
        if raw_discord_id is None and raw_id is not None:
            raw_discord_id = str(raw_id)

        if raw_discord_id is not None:
            data["discord_id"] = str(raw_discord_id)

        # Best-effort int id (used when writing stats docs).
        if raw_id is None and raw_discord_id is not None:
            try:
                data["id"] = int(str(raw_discord_id))
            except TypeError, ValueError:
                data["id"] = None
        elif raw_id is not None:
            try:
                data["id"] = int(str(raw_id))
            except TypeError, ValueError:
                data["id"] = None

        # Defaults for older docs.
        data.setdefault("subbedIn", 0)
        data.setdefault("subbedOut", 0)
        return data


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


class ContestReport(BaseModel):
    contestor_discord_id: str
    reason: str


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
    contest_report_list: List[ContestReport]


__all__ = ["StatModel", "PlayerModel", "ContestReport", "MatchModel"]
