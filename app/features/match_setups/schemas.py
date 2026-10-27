"""What Mite sends for /draft and /lobby, and what it gets back."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_DRAFTS = {"civ6": 14, "civ7": 10}


class DraftRequest(BaseModel):
    """How many pools, their sizes, the age and the host's bans."""

    model_config = ConfigDict(extra="forbid")

    host_id: str = Field(min_length=1, max_length=32)
    edition: Literal["civ6", "civ7"]
    game_type: Literal["ffa", "teamer", "duel"]
    drafts: int = Field(ge=2, le=14)
    leaders: int = Field(ge=1)
    # civ7 only, as is the starting age; no age means every age.
    civs: int | None = Field(default=None, ge=1)
    starting_age: Literal["AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN"] | None = (
        None
    )
    leader_bans: list[str] = Field(default_factory=list, max_length=100)
    civ_bans: list[str] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def _suits_the_edition(self) -> DraftRequest:
        if self.drafts > MAX_DRAFTS[self.edition]:
            raise ValueError(
                f"{self.edition} deals at most {MAX_DRAFTS[self.edition]} drafts"
            )
        if self.game_type == "duel" and self.drafts != 2:
            raise ValueError("a duel is two drafts")
        if self.edition == "civ6" and (
            self.civs is not None or self.starting_age is not None or self.civ_bans
        ):
            raise ValueError("civ6 deals leaders only")
        if self.edition == "civ7" and self.civs is None:
            raise ValueError("civ7 needs a civ count")
        return self


class DealtPool(BaseModel):
    leaders: list[str]
    civs: list[str]


class DealtBans(BaseModel):
    leader: list[str]
    civ: list[str]


class DraftDeal(BaseModel):
    pools: list[DealtPool]
    bans: DealtBans


LobbyShape = Literal[
    "ffa-8",
    "ffa-10",
    "ffa-12",
    "ffa-14",
    "duel",
    "teamer-2v2",
    "teamer-3v3",
    "teamer-4v4",
    "teamer-5v5",
    "teamer-2v2v2v2",
]


class LobbyRequest(BaseModel):
    """/lobby host. The host is the acting player."""

    model_config = ConfigDict(extra="forbid")

    shape: LobbyShape
    draft_mode: Literal["standard", "random"]
    guild_id: str = Field(min_length=1, max_length=32)
    channel_id: str = Field(min_length=1, max_length=32)
    steam_link: str | None = Field(default=None, max_length=120)


class LobbyLive(BaseModel):
    """The post that opens the lobby to players, edited in place from then on."""

    message_id: str = Field(min_length=1, max_length=32)


class LobbyEdit(BaseModel):
    """The host's whole edit: the size, every setting fixed (the rest are
    voted), every leader banned, and players moved or removed."""

    model_config = ConfigDict(extra="forbid")

    shape: LobbyShape
    settings: dict[str, str]
    bans: list[str] = Field(max_length=100)
    teams: dict[str, int] = Field(default_factory=dict)
    removed: list[str] = Field(default_factory=list, max_length=14)


# The players who backed one option or one ban: at most a full lobby.
Voters = Annotated[list[str], Field(max_length=14)]


class VotePlace(BaseModel):
    """Where the vote is posted: its channel or thread, and the message of each
    open setting and of the bans."""

    model_config = ConfigDict(extra="forbid")

    channel_id: str = Field(min_length=1, max_length=32)
    messages: dict[str, str] = Field(max_length=30)


class LobbyBallot(BaseModel):
    """The players' reactions: per setting, each option's voters, and per
    leader, the players banning it."""

    model_config = ConfigDict(extra="forbid")

    settings: dict[str, dict[str, Voters]] = Field(max_length=30)
    bans: dict[str, Voters] = Field(max_length=100)


class LobbyVote(BaseModel):
    ends_at: datetime
    channel_id: str | None
    messages: dict[str, str]
    finished: list[str]


class LobbyPlayer(BaseModel):
    discord_id: str
    team: int | None


class ChosenSetting(BaseModel):
    id: str
    value: str
    by: str


class SetupBan(BaseModel):
    token: str
    kind: str
    by: str


class LobbyPool(BaseModel):
    """A player's draft, or a team's when `player_id` is empty."""

    player_id: str | None
    team: int | None
    leaders: list[str]


class LobbyView(BaseModel):
    id: str
    shape: LobbyShape
    mode: str
    seats: int
    teams: int
    draft_mode: str
    status: Literal["open", "done", "cancelled"]
    live: bool
    host_id: str
    guild_id: str
    channel_id: str
    message_id: str | None
    steam_link: str | None
    players: list[LobbyPlayer]
    settings: list[ChosenSetting]
    bans: list[SetupBan]
    pools: list[LobbyPool]
    vote: LobbyVote | None
    cancel_reason: str | None
    updated_at: datetime


class SettingOption(BaseModel):
    id: str
    label: str
    emoji: str


class SettingQuestion(BaseModel):
    id: str
    title: str
    options: list[SettingOption]
    default_option_id: str


__all__ = [
    "MAX_DRAFTS",
    "ChosenSetting",
    "DealtBans",
    "DealtPool",
    "DraftDeal",
    "DraftRequest",
    "LobbyBallot",
    "LobbyEdit",
    "LobbyLive",
    "LobbyPlayer",
    "LobbyPool",
    "LobbyRequest",
    "LobbyShape",
    "LobbyView",
    "LobbyVote",
    "SettingOption",
    "SettingQuestion",
    "SetupBan",
    "VotePlace",
]
