"""What Mite sends for /draft, /lobby and /vote, and what it gets back."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.core.constants import Edition

MAX_DRAFTS = {"civ6": 14, "civ7": 10}
StartingAge = Literal["AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN"]


class DraftRequest(BaseModel):
    """How many pools, their sizes, the age and the host's bans."""

    model_config = ConfigDict(extra="forbid")

    host_id: str = Field(min_length=1, max_length=32)
    edition: Edition
    game_type: Literal["ffa", "teamer", "duel"]
    drafts: int = Field(ge=2, le=14)
    leaders: int = Field(ge=1)
    # civ7 only, as is the starting age; no age means every age.
    civs: int | None = Field(default=None, ge=1)
    starting_age: StartingAge | None = None
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
    "teamer-2v2v2",
    "teamer-2v2v2v2",
]


class LobbyRequest(BaseModel):
    """/lobby host. The host is the acting player; the settings start blank."""

    model_config = ConfigDict(extra="forbid")

    shape: LobbyShape
    guild_id: str = Field(min_length=1, max_length=32)
    channel_id: str = Field(min_length=1, max_length=32)
    steam_link: str | None = Field(default=None, max_length=120)
    message: str | None = Field(default=None, max_length=200)
    # Pings the server's LFG role when the lobby opens.
    lfg: bool = False


class LobbyLive(BaseModel):
    """The post that opens the lobby to players, edited in place from then on."""

    message_id: str = Field(min_length=1, max_length=32)


class LobbyEdit(BaseModel):
    """The host's whole edit: the size, the settings with the draft mode (all
    of them once live), every leader banned, and players moved or removed."""

    model_config = ConfigDict(extra="forbid")

    shape: LobbyShape
    settings: dict[str, str]
    bans: list[str] = Field(max_length=100)
    teams: dict[str, int] = Field(default_factory=dict)
    removed: list[str] = Field(default_factory=list, max_length=14)


# The players who backed one option or one ban: at most a full lobby.
Voters = Annotated[list[str], Field(max_length=14)]


class VotePlace(BaseModel):
    """Where the bans vote, then the blind picks, are posted: the channel and
    the one message."""

    model_config = ConfigDict(extra="forbid")

    channel_id: str = Field(min_length=1, max_length=32)
    messages: dict[str, str] = Field(max_length=1)


class LobbyBallot(BaseModel):
    """The players' reactions: per leader, the players banning it."""

    model_config = ConfigDict(extra="forbid")

    bans: dict[str, Voters] = Field(max_length=100)


class LobbyCancel(BaseModel):
    """A lobby or vote is cancelled by its code, by its host or by staff."""

    model_config = ConfigDict(extra="forbid")

    guild_id: str = Field(min_length=1, max_length=32)
    code: str = Field(min_length=1, max_length=8)


class LeaderPick(BaseModel):
    """A blind pick: a leader, and a civ where the pool has civs."""

    model_config = ConfigDict(extra="forbid")

    leader: str = Field(min_length=1, max_length=64)
    civ: str | None = Field(default=None, min_length=1, max_length=64)


class LobbyVote(BaseModel):
    channel_id: str | None
    messages: dict[str, str]
    finished: list[str]


class BlindPick(BaseModel):
    player_id: str
    leader: str
    civ: str | None = None
    by: Literal["player", "auto"]


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
    civs: list[str] = []


class SetupView(BaseModel):
    id: str
    teams: int
    draft_mode: str | None
    status: Literal["open", "done", "cancelled"]
    deadline: datetime | None
    code: str
    host_id: str
    guild_id: str
    channel_id: str
    players: list[LobbyPlayer]
    settings: list[ChosenSetting]
    bans: list[SetupBan]
    pools: list[LobbyPool]
    picks: list[BlindPick]
    cancel_reason: str | None
    updated_at: datetime


class LobbyView(SetupView):
    shape: LobbyShape
    mode: str
    seats: int
    phase: Literal["setup", "live", "voting", "picking"]
    message_id: str | None
    steam_link: str | None
    message: str | None = None
    lfg: bool = False
    vote: LobbyVote | None


class VoteRequest(BaseModel):
    """/vote: the players from a voice channel, the game, and the bans its
    starter chose; everything else is voted on."""

    model_config = ConfigDict(extra="forbid")

    edition: Edition
    mode: Literal["ffa", "duel", "teamer"]
    # civ7 only; none means every age.
    age: StartingAge | None = None
    guild_id: str = Field(min_length=1, max_length=32)
    channel_id: str = Field(min_length=1, max_length=32)
    players: list[str] = Field(min_length=2, max_length=14)
    leader_bans: list[str] = Field(default_factory=list, max_length=100)
    civ_bans: list[str] = Field(default_factory=list, max_length=100)


class VoteMessages(BaseModel):
    """Where the vote is posted: the channel, and its status, setting and ban
    messages by name."""

    model_config = ConfigDict(extra="forbid")

    channel_id: str = Field(min_length=1, max_length=32)
    messages: dict[str, str] = Field(min_length=1, max_length=25)


class VoteBallot(BaseModel):
    """The players' reactions: per setting, the players backing each option;
    per leader or civ, the players banning it."""

    model_config = ConfigDict(extra="forbid")

    settings: dict[str, dict[str, Voters]] = Field(default_factory=dict, max_length=40)
    bans: dict[str, Voters] = Field(default_factory=dict, max_length=200)


class VoteView(SetupView):
    edition: Edition
    mode: Literal["ffa", "duel", "teamer"]
    age: StartingAge | None
    phase: Literal["voting", "picking"]
    vote: LobbyVote


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
    "BlindPick",
    "ChosenSetting",
    "DealtBans",
    "DealtPool",
    "DraftDeal",
    "DraftRequest",
    "LeaderPick",
    "LobbyBallot",
    "LobbyCancel",
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
    "SetupView",
    "StartingAge",
    "VoteBallot",
    "VoteMessages",
    "VotePlace",
    "VoteRequest",
    "VoteView",
]
