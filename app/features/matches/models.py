from __future__ import annotations

from datetime import datetime
from typing import Any

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
    id: int | None = None

    mu: float
    sigma: float
    games: int
    wins: int
    first: int

    # Subbing fields are not present in older docs; keep safe defaults.
    subbedIn: int = 0
    subbedOut: int = 0

    # Civs map is historically {"CivName": <int games>}.
    civs: dict[str, Any] | None = None

    # Same shape, keyed on the leader token. Civ7 picks a leader and a civ
    # independently, so the two tallies are different questions.
    leaders: dict[str, Any] | None = None

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
    created_at: datetime = Field(default_factory=datetime.utcnow)
    approved_at: datetime | None = None
    approver_discord_id: str | None = None
    flagged: bool = False
    flagged_by: str | None = None
    save_file_hash: str
    # Entry 12: the byte hash. Optional because 35,941 existing
    # documents have none -- and it must never be written empty, since
    # the partial index filters on {$exists: true} and would collide
    # every such document. D83, D133.
    save_bytes_sha256: str | None = None
    reporter_discord_id: str
    contest_report_list: list[ContestReport]


__all__ = ["ContestReport", "MatchModel", "PlayerModel", "StatModel"]
