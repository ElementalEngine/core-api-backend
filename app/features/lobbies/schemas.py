"""The v2 lobbies wire shapes.

Pydantic answers "is this JSON the right shape"; `modes.py` answers "is this
a lobby that can exist". Both surface as INVALID_REQUEST -- 422 from the
registered validation handler, 400 from `invalid_request` -- so the split
costs the caller nothing and keeps every mode rule in one file.

`game_type` is a plain str, not a Literal, deliberately: it is a mode rule
and `resolve_shape` owns it. `edition` is a Literal because it is a plain
enum that no mode rule touches.

Governed by D73.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CreateLobbyRequest(BaseModel):
    """Mite's create call. Authoritative guild, channel, host and roster."""

    model_config = ConfigDict(extra="forbid")

    guild_id: str = Field(min_length=1, max_length=32)
    channel_id: str = Field(min_length=1, max_length=32)
    host_discord_id: str = Field(min_length=1, max_length=32)
    edition: Literal["civ6", "civ7"]
    game_type: str
    number_teams: int | None = None
    team_size: int | None = None
    # Teamers choose between standard and cwc here rather than on the ballot:
    # cwc needs captains identified before the ban phase, and a ballot answer
    # arrives too late to seat them.
    draft_mode: Literal["standard", "cwc"] | None = None
    roster: list[str] = Field(default_factory=list, max_length=99)
    instance_id: str | None = Field(default=None, max_length=64)
    starting_age: Literal["AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN"] | None = (
        Field(default=None)
    )

    @model_validator(mode="after")
    def _draft_mode_suits_the_game_type(self) -> CreateLobbyRequest:
        if self.game_type != "teamer":
            if self.draft_mode is not None:
                raise ValueError("only a teamer chooses its draft mode at creation")
            return self
        if self.draft_mode is None:
            raise ValueError("a teamer must choose standard or cwc")
        if self.draft_mode == "cwc":
            # The pick order alternates between exactly two sides, and an odd
            # team size is a league rule rather than a limit of the table.
            if self.number_teams != 2:
                raise ValueError("cwc needs exactly two teams")
            if not self.team_size or self.team_size % 2:
                raise ValueError("cwc needs an even team size")
        return self


class SeatAction(StrEnum):
    PLACE = "place"
    LEAVE = "leave"


class ChangeSeatRequest(BaseModel):
    """One seat change. `place` covers self-place, move and host rearrange."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    action: SeatAction
    seat_index: int | None = Field(default=None, ge=0)
    team: int | None = Field(default=None, ge=0)
    # Absent means the actor. Naming somebody else is the host's rearrange
    # and is refused for anyone else.
    discord_id: str | None = Field(default=None, min_length=1, max_length=32)

    @model_validator(mode="after")
    def _fields_match_the_action(self) -> ChangeSeatRequest:
        if self.action is SeatAction.PLACE and self.seat_index is None:
            raise ValueError("seat_index is required to place a seat")
        if self.action is SeatAction.LEAVE and (
            self.seat_index is not None or self.team is not None
        ):
            raise ValueError("leaving takes no seat_index and no team")
        return self


class SubmitBallotRequest(BaseModel):
    """One seat's settings ballot."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    selections: dict[str, str] = Field(min_length=1)


class SubmitBansRequest(BaseModel):
    """One seat's bans. The whole set, not a delta."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    leader_keys: list[str] = Field(default_factory=list, max_length=40)
    civ_keys: list[str] = Field(default_factory=list, max_length=40)


class SubmitPickRequest(BaseModel):
    """One seat's pick, from the pool that seat was dealt."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    token: str | None = Field(default=None, min_length=1, max_length=64)
    civ_token: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def _at_least_one_token(self) -> SubmitPickRequest:
        if self.token is None and self.civ_token is None:
            raise ValueError("a pick needs a token or a civ_token")
        return self


__all__ = [
    "ChangeSeatRequest",
    "CreateLobbyRequest",
    "SeatAction",
    "SubmitBallotRequest",
    "SubmitBansRequest",
    "SubmitPickRequest",
]
