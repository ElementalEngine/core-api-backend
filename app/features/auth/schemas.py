from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.features.auth.enums import (
    ManualRegistrationChoice,
    RegistrationMethod,
    RegistrationOperationStatus,
    RegistrationPlatform,
    RegistrationSessionStatus,
    RoleIntent,
    SupportedGame,
)


class _DiscordUserIdModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discord_user_id: str = Field(min_length=1)

    @field_validator("discord_user_id")
    @classmethod
    def _normalize_discord_user_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("discord_user_id must not be blank")
        return normalized


class CreateRegistrationSessionRequest(_DiscordUserIdModel):
    game: SupportedGame
    platform: RegistrationPlatform = RegistrationPlatform.STEAM


class CompleteRegistrationSessionRequest(_DiscordUserIdModel):
    pass


class RankRoleRequest(_DiscordUserIdModel):
    game: SupportedGame


class ManualRegistrationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor_discord_id: str = Field(min_length=1)
    subject_discord_id: str = Field(min_length=1)
    platform: ManualRegistrationChoice
    platform_account_id: str = Field(
        min_length=1, validation_alias=AliasChoices("platform_account_id", "account_id")
    )
    game: SupportedGame
    reason: str | None = Field(default=None, max_length=500)
    platform_account_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("platform_account_name", "account_name"),
    )
    discord_username: str | None = None
    discord_display_name: str | None = None

    @field_validator("actor_discord_id", "subject_discord_id", "platform_account_id")
    @classmethod
    def _normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized

    @field_validator(
        "reason", "platform_account_name", "discord_username", "discord_display_name"
    )
    @classmethod
    def _normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class SelfServiceRegistrationRequest(BaseModel):
    """Civ7 non-Steam self-service registration (currently 2K only).

    The Auth Bot supplies the Discord identity fields automatically. game/platform are
    accepted but the backend enforces game=civ7 + platform=2k (see ManualRegistrationService).
    """

    model_config = ConfigDict(extra="forbid")

    discord_user_id: str = Field(min_length=1)
    game: SupportedGame
    platform: RegistrationPlatform
    platform_account_id: str = Field(
        min_length=1, validation_alias=AliasChoices("platform_account_id", "account_id")
    )
    platform_account_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("platform_account_name", "account_name"),
    )
    discord_username: str | None = None
    discord_display_name: str | None = None

    @field_validator("discord_user_id", "platform_account_id")
    @classmethod
    def _normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized

    @field_validator(
        "platform_account_name", "discord_username", "discord_display_name"
    )
    @classmethod
    def _normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class FinalizeRegistrationOperationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    result: Literal["succeeded", "failed"]
    applied_role_intents: list[RoleIntent] = Field(default_factory=list)
    failure_code: str | None = None
    failure_message: str | None = None

    @model_validator(mode="after")
    def _validate_failed_payload(self) -> "FinalizeRegistrationOperationRequest":
        if self.result == "failed" and (
            not self.failure_code or not self.failure_message
        ):
            raise ValueError(
                "failure_code and failure_message are required when result is failed"
            )
        return self


class RegistrationSessionResponse(BaseModel):
    session_id: str
    authorize_url: str
    expires_at: datetime


class RegistrationSessionStatusResponse(BaseModel):
    session_id: str
    status: RegistrationSessionStatus
    expires_at: datetime | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    game: SupportedGame | None = None
    platform: RegistrationPlatform | None = None
    linked_account_id: str | None = None
    linked_account_name: str | None = None
    discord_username: str | None = None
    discord_display_name: str | None = None
    discord_locale: str | None = None
    discord_verified: bool | None = None
    discord_mfa_enabled: bool | None = None


class RegistrationOperationResponse(BaseModel):
    operation_id: str
    status: RegistrationOperationStatus | None = None
    discord_user_id: str
    steam_id: str
    steam_name: str | None = None
    linked_platform: RegistrationPlatform | None = None
    linked_account_id: str | None = None
    linked_account_name: str | None = None
    registration_method: RegistrationMethod | None = None
    game: SupportedGame
    role_intents: list[RoleIntent]


class RegistrationSummary(BaseModel):
    game: SupportedGame
    method: str | None = None
    registered_at: datetime | None = None


class LinkedAccountLookupHit(BaseModel):
    linked_platform: RegistrationPlatform | None = None
    linked_account_id: str
    linked_account_name: str | None = None
    registrations: list[RegistrationSummary] = Field(default_factory=list)


class DiscordLookupResponse(BaseModel):
    discord_id: str
    discord_username: str | None = None
    discord_display_name: str | None = None
    linked_accounts: list[LinkedAccountLookupHit] = Field(default_factory=list)


class DiscordAccountLookupHit(BaseModel):
    discord_id: str
    discord_username: str | None = None
    discord_display_name: str | None = None
    registrations: list[RegistrationSummary] = Field(default_factory=list)


class LinkedAccountLookupResponse(BaseModel):
    linked_account_id: str
    linked_account_name: str | None = None
    linked_platform: RegistrationPlatform | None = None
    discord_accounts: list[DiscordAccountLookupHit] = Field(default_factory=list)


class DiscordOAuthCallbackResult(BaseModel):
    session_id: str
    status: RegistrationSessionStatus
    platform: RegistrationPlatform
    linked_account_id: str | None = None
    linked_account_name: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
