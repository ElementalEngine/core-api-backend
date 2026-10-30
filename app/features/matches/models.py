from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from pydantic import BaseModel, Field, model_validator


class StatModel(BaseModel):
    """Stat row used for ranking/stats calculations."""

    index: int
    discord_id: str
    # The stat document's own id: the Discord id as an int.
    id: int | None = None

    mu: float
    sigma: float
    games: int
    wins: int
    first: int

    # Older documents have no sub counters.
    subbedIn: int = 0
    subbedOut: int = 0

    # Games per civ token, and below per leader token: Civ7 picks the two
    # independently, so they are different questions.
    civs: dict[str, Any] | None = None

    leaders: dict[str, Any] | None = None

    lastModified: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="before")
    @classmethod
    def _normalize_ids(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        # The id comes as `id` or as Mongo's `_id`; the Discord id follows from it.
        raw_id = data.get("id")
        if raw_id is None and "_id" in data:
            raw_id = data.get("_id")

        raw_discord_id = data.get("discord_id")
        if raw_discord_id is None and raw_id is not None:
            raw_discord_id = str(raw_id)

        if raw_discord_id is not None:
            data["discord_id"] = str(raw_discord_id)

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
        return data


class PlayerModel(BaseModel):
    steam_id: str | None = None
    user_name: str | None = None
    civ: str
    team: int
    leader: str | None = None
    player_alive: bool | None = None
    discord_id: str | None = None
    placement: int | None = None
    quit: bool = False
    delta: float = 0.0
    season_delta: float | None = None
    combined_delta: float | None = None
    is_sub: bool = False
    subbed_out: bool = False
    # The in-game score when the save was read, where the save records it.
    score: int | None = None


class ContestReport(BaseModel):
    contestor_discord_id: str
    reason: str


class MatchModel(BaseModel):
    game: str  # parsers return "civ6" or "civ7"
    turn: int
    age: str | None = None
    map_type: str
    game_mode: str  # allow "", "FFA", "Teamer", "Duel"
    is_cloud: bool
    players: list[PlayerModel]
    parser_version: str
    discord_messages_id_list: list[str]
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    approved_at: datetime | None = None
    approver_discord_id: str | None = None
    flagged: bool = False
    flagged_by: str | None = None
    save_file_hash: str
    save_bytes_sha256: str | None = None
    reporter_discord_id: str
    contest_report_list: list[ContestReport]

    @model_validator(mode="before")
    @classmethod
    def _v1_gaps(cls, data: Any) -> Any:
        """Matches v1 approved can lack a contest list and hold a null
        created_at; the report time is the match id's own timestamp."""
        if not isinstance(data, dict):
            return data
        data = dict(data)
        if data.get("contest_report_list") is None:
            data["contest_report_list"] = []
        if not isinstance(data.get("created_at"), datetime):
            oid = data.get("_id")
            if isinstance(oid, ObjectId):
                data["created_at"] = oid.generation_time
            else:
                data.pop("created_at", None)
        return data


__all__ = ["ContestReport", "MatchModel", "PlayerModel", "StatModel"]
