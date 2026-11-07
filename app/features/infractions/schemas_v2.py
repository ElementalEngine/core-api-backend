from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

from app.features.infractions.models import FlatType, TierCategory

DiscordId = Annotated[str, StringConstraints(pattern=r"^[0-9]{17,20}$")]
Reason = Annotated[str, StringConstraints(max_length=1000)]
Roles = Annotated[list[DiscordId], Field(max_length=100)]


class InfractionBody(BaseModel):
    reason: Reason | None = None
    suspended_roles: Roles = Field(default_factory=list)


class DaysBody(BaseModel):
    days: int = Field(ge=-365, le=365, strict=True)

    @field_validator("days")
    @classmethod
    def _not_zero(cls, days: int) -> int:
        if days == 0:
            raise ValueError("days must not be 0")
        return days


class PendingBody(BaseModel):
    punishment_type: TierCategory | FlatType
    reason: Reason | None = None


class ReasonBody(BaseModel):
    reason: Reason | None = None


class RemoveTierBody(BaseModel):
    category: TierCategory


class RolesBody(BaseModel):
    suspended_roles: Roles = Field(default_factory=list)


class TierResult(BaseModel):
    discord_id: str
    category: TierCategory
    tier: int
    days_added: int
    ends: datetime | None
    suspended: bool
    held: bool
    is_ban_threshold: bool
    is_warning_only: bool


class FlatResult(BaseModel):
    discord_id: str
    type: FlatType
    days_added: int
    ends: datetime


class DaysResult(BaseModel):
    discord_id: str
    days: int
    ends: datetime
    suspended: bool


class TierRemoved(BaseModel):
    discord_id: str
    category: TierCategory
    tier: int
    was_changed: bool


class SuspensionCleared(BaseModel):
    """A suspension ended, with the roles the bot should give back; `suspended`
    is the record after the call, so a repeat can tell it ended or never did."""

    discord_id: str
    cleared: bool
    suspended: bool
    suspended_roles: list[str]


class DueSuspensions(BaseModel):
    discord_ids: list[str]
    next_ends: datetime | None


class PendingApplied(BaseModel):
    discord_id: str
    applied: bool
    punishment_type: str | None
    reason: str | None
    ends: datetime | None
    held: bool
    # Whether the member wears the lagger tag on the record, so a rejoin gets the role back.
    lagger_tagged: bool


class SuspensionRecord(BaseModel):
    """A member's record read as the next write would see it, nothing written;
    a member without one reads as a zero record."""

    discord_id: str
    suspended: bool
    ends: datetime | None
    held: bool
    active_category: str | None
    tiers: dict[TierCategory, int]
    lagger_tagged: bool


class Lagger(BaseModel):
    discord_id: str
    strikes: int
    tagged: bool


class Laggers(BaseModel):
    members: list[Lagger]
