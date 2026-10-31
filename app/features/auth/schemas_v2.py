from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, StringConstraints

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
AccountFlag = Literal["young_discord", "young_steam"]


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
