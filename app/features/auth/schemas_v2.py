from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    StringConstraints,
    model_validator,
)

from app.core.errors import ErrorDetail
from app.features.auth.enums import (
    ManualRegistrationChoice,
    RegistrationMethod,
    RegistrationPlatform,
    RoleIntent,
    SupportedGame,
)


def _blank_is_none(value: object) -> object:
    return value.strip() or None if isinstance(value, str) else value


DiscordId = Annotated[
    str, StringConstraints(strip_whitespace=True, pattern=r"^[0-9]{17,20}$")
]
AccountId = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
]
Name = Annotated[
    Annotated[str, StringConstraints(max_length=128)] | None,
    BeforeValidator(_blank_is_none),
]
SessionStatus = Literal[
    "pending_auth",
    "validating",
    "validated",
    "completing",
    "completed",
    "failed",
    "expired",
]
AccountFlag = Literal["young_discord", "young_steam", "unknown_steam_age"]
STEAM_ID = re.compile(r"^7656119[0-9]{10}$")


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SessionStart(_Body):
    discord_user_id: DiscordId
    game: SupportedGame


class SessionComplete(_Body):
    discord_user_id: DiscordId


class RankRoleBody(_Body):
    discord_user_id: DiscordId
    game: SupportedGame


class ManualRegistrationBody(_Body):
    subject_discord_id: DiscordId
    game: SupportedGame
    platform: ManualRegistrationChoice
    platform_account_id: AccountId
    platform_account_name: Name = None
    discord_username: Name = None
    discord_display_name: Name = None
    reason: Annotated[
        Annotated[str, StringConstraints(max_length=500)] | None,
        BeforeValidator(_blank_is_none),
    ] = None

    @model_validator(mode="after")
    def _a_steam_link_is_a_steam_id(self) -> ManualRegistrationBody:
        steam = self.platform is not ManualRegistrationChoice.TWOK
        if steam and not STEAM_ID.match(self.platform_account_id):
            raise ValueError("a Steam account id is 17 digits starting 7656119")
        return self


class TwoKRegistrationBody(_Body):
    """Civ7 on 2K, registered by the player without staff."""

    discord_user_id: DiscordId
    platform_account_id: AccountId
    platform_account_name: Name = None
    discord_username: Name = None
    discord_display_name: Name = None


class SessionStarted(BaseModel):
    session_id: str
    authorize_url: str
    expires_at: datetime


class SessionState(BaseModel):
    session_id: str
    status: SessionStatus
    expires_at: datetime
    game: SupportedGame
    linked_account_id: str | None = None
    linked_account_name: str | None = None
    discord_username: str | None = None
    discord_display_name: str | None = None
    discord_locale: str | None = None
    discord_verified: bool | None = None
    discord_mfa_enabled: bool | None = None
    failure: ErrorDetail | None = None


class Registration(BaseModel):
    discord_user_id: str
    game: SupportedGame
    linked_platform: RegistrationPlatform
    linked_account_id: str
    linked_account_name: str | None = None
    registration_method: RegistrationMethod
    role_intents: list[RoleIntent]
    flags: list[AccountFlag] = []


class Standing(BaseModel):
    """An account's record with Lady Justice."""

    suspended: bool
    ends: datetime | None = None
    active_category: str | None = None
    tiers: dict[str, int]
    at_ban_threshold: bool


class RegistrationEntry(BaseModel):
    game: SupportedGame
    method: str | None = None
    registered_at: datetime | None = None
    flags: list[AccountFlag] = []
    actor_discord_id: str | None = None
    reason: str | None = None


class AccountRecord(BaseModel):
    discord_id: str
    discord_username: str | None = None
    discord_display_name: str | None = None
    linked_platform: RegistrationPlatform | None = None
    linked_account_id: str | None = None
    linked_account_name: str | None = None
    registrations: list[RegistrationEntry]
    standing: Standing | None = None


class AccountLookup(BaseModel):
    """Every record that matches; more than one is worth a look."""

    records: list[AccountRecord]
