"""The v2 lobbies wire shapes.

Pydantic answers "is this JSON the right shape"; `modes.py` answers "is this
a lobby that can exist". Both surface as INVALID_REQUEST -- 422 from the
registered validation handler, 400 from `invalid_request` -- so the split
costs the caller nothing and keeps every mode rule in one file.

⚠ `game_type` is a plain str, not a Literal, deliberately: it is a mode rule
and `resolve_shape` owns it. `edition` is a Literal because it is a plain
enum that no mode rule touches.

⚠ There is no response model. D73's projection decides the response shape
per recipient, and a model would have to make every censored field Optional
-- which would resurrect a hidden `ballot` or `pool` as `null` instead of
absent, contradicting the projection's own tests. The cost is that these
responses carry no schema in openapi.json, alongside section 4 item 96.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CreateLobbyRequest(BaseModel):
    """Mite's create call. Authoritative guild, channel, host and roster.

    `extra="forbid"` so a camelCase field name is refused rather than
    silently dropped and then reported as a missing required field.
    """

    model_config = ConfigDict(extra="forbid")

    guild_id: str = Field(min_length=1, max_length=32)
    channel_id: str = Field(min_length=1, max_length=32)
    host_discord_id: str = Field(min_length=1, max_length=32)
    edition: Literal["civ6", "civ7"]
    game_type: str
    number_teams: int | None = None
    team_size: int | None = None
    # Voice members. A creation-time INPUT, not a stored field: it seats
    # people and is then discarded. Keeping it would mean enforcing voice
    # membership as eligibility, and voice membership churns constantly --
    # the Discord channel already scopes who can see the lobby.
    #
    # ⚠ Seated only when the roster FITS. Fifteen people in voice and a 3v3
    # opens empty: seating an arbitrary first six excludes people by list
    # order, and this is exactly where self-selection matters (D75).
    #
    # May be empty -- a host outside voice gets a lobby nobody is in yet,
    # which is legal and is why the seat index carries the $exists clause
    # (D175, Correction 73b).
    roster: list[str] = Field(default_factory=list, max_length=99)
    # Discord's Activity instance. A diagnostic attribute only: the lobby
    # key is core-api's own id and the Activity resolves by channel, never
    # by this (spec section 2). Omitted from the document when absent.
    instance_id: str | None = Field(default=None, max_length=64)


class SeatAction(StrEnum):
    PLACE = "place"
    LEAVE = "leave"


class ChangeSeatRequest(BaseModel):
    """One seat change. `place` covers self-place, move and host rearrange.

    ⚠ Two actions rather than four. Moving IS placing at a different index,
    and a host rearrange is placing aimed at somebody else -- so one action
    plus an optional target covers all four of C5's verbs with one code path
    and one call to `validate_seats`.

    ⚠ `place` states the WHOLE desired position. Omitting `team` means no
    side, not "keep the side you had": distinguishing the two would need a
    sentinel, and a seat move that silently retains a team is the kind of
    quiet action O-19b's compaction bug was made of.
    """

    model_config = ConfigDict(extra="forbid")

    # D77's optimistic concurrency. Revision starts at 1, so 0 is a client
    # bug rather than "I have nothing".
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


__all__ = ["ChangeSeatRequest", "CreateLobbyRequest", "SeatAction"]
