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
    # ⚠ civ7 only, and NOT a ballot question -- Mite's slash command carries
    # it, so it must arrive at creation or the civ ban cap has nothing to key
    # on. civ-data's vocabulary, not Mite v1's `Antiquity_Age` (D196): every
    # `CivRow.age_pool` reads `AGE_ANTIQUITY`, and a filter against the wrong
    # spelling matches nothing, leaves no civ bannable, and raises nothing.
    starting_age: Literal["AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN"] | None = (
        Field(default=None)
    )


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


class SubmitBallotRequest(BaseModel):
    """One seat's settings ballot.

    ⚠ The whole ballot, not a delta. A seat re-submitting replaces what it
    had, so a client that dropped an answer cannot leave a stale one behind
    -- and "has this seat answered question X" stays a single lookup rather
    than a merge of every submission it ever made.

    Selections are `option_id`, or `a|b` where the question allows more than
    one (D191). Ids are checked against the catalogue, so a question or option
    the ballot invents is a 400 rather than a vote nothing counts.
    """

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    selections: dict[str, str] = Field(min_length=1)


class SubmitBansRequest(BaseModel):
    """One seat's bans. The whole set, not a delta.

    ⚠ Both lists may be empty -- a seat banning nothing has still SUBMITTED,
    and the phase advances on all-submitted. Distinguishing "banned nothing"
    from "has not banned" is why the seat stores `bans` as a document rather
    than two bare lists (`bans is not None` is the submitted test).

    Tokens are civ-data's, and are checked against it: leaders against the
    edition's whole set, civs against the STARTING AGE's pool only. A civ
    outside the chosen age is a real token for a civ not in the game, and
    banning it would burn one of three slots (D196).
    """

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    leader_keys: list[str] = Field(default_factory=list, max_length=40)
    civ_keys: list[str] = Field(default_factory=list, max_length=40)


class SubmitPickRequest(BaseModel):
    """One seat's pick, from the pool that seat was dealt.

    ⚠ **A pick is final once made (O-34).** The refusal is on the seat's own
    state, NOT on `revision`: a revision guard permits exactly what the rule
    forbids -- read at revision 5, change your mind, write at revision 5, and
    nothing has moved so the guard is satisfied. Same shape as D176's `$ne`,
    which is a clause about the document's content rather than its version.

    `civ_token` is civ7 only; civ6 drafts leaders alone.
    """

    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    # ⚠ Both optional, at least one required. Snake on civ7 runs a leader
    # round then a civ round, so the civ-round request carries `civ_token`
    # ALONE -- a required `token` would force the client to resend a leader
    # it has already locked, and the per-field lock would refuse it. The
    # schema has to match the two-round shape, not the other way round.
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
