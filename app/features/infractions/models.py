from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Final, Literal

from pydantic import BaseModel, Field


# ─── Enums ───────────────────────────────────────────────────────────────────

class TierCategory(str, Enum):
    quit = "quit"
    minor = "minor"
    moderate = "moderate"
    major = "major"
    extreme = "extreme"


class FlatType(str, Enum):
    smurf = "smurf"
    oversub = "oversub"
    comp = "comp"


# ─── Sub-document (matches IInfraction shape in Mongoose exactly) ─────────────

class InfractionRecord(BaseModel):
    tier: int = 0
    decays: datetime | None = None


# ─── Main suspension document ─────────────────────────────────────────────────

class SuspensionDocument(BaseModel):
    discord_id: str
    suspended: bool = False
    ends: datetime | None = None
    suspendedRoles: list[str] = []          # camelCase — matches live collection
    quit: InfractionRecord = Field(default_factory=InfractionRecord)
    minor: InfractionRecord = Field(default_factory=InfractionRecord)
    moderate: InfractionRecord = Field(default_factory=InfractionRecord)
    major: InfractionRecord = Field(default_factory=InfractionRecord)
    extreme: InfractionRecord = Field(default_factory=InfractionRecord)
    active_category: TierCategory | Literal["flat"] | None = None  # additive — None on old docs is safe


# ─── Pending suspension document ─────────────────────────────────────────────

class PendingSuspensionDocument(BaseModel):
    id: str = Field(alias="_id")            # discord_id stored as _id
    punishment_type: str                    # tier category name or flat type name
    reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ─── Request shapes ───────────────────────────────────────────────────────────

class RecordTierInfractionRequest(BaseModel):
    reason: str | None = None
    suspended_roles: list[str] = Field(default_factory=list)


class RecordFlatSuspensionRequest(BaseModel):
    reason: str | None = None
    suspended_roles: list[str] = Field(default_factory=list)


class ModifyDaysRequest(BaseModel):
    days: int = Field(..., ge=-365, le=365)


class RemoveTierRequest(BaseModel):
    category: TierCategory


class UnsuspendRequest(BaseModel):
    reason: str | None = None


class CreatePendingSuspensionRequest(BaseModel):
    punishment_type: str
    reason: str | None = None


# ─── Response shapes ─────────────────────────────────────────────────────────

class TierInfractionResponse(BaseModel):
    discord_id: str
    category: TierCategory
    tier: int
    days_added: int
    ends: datetime | None
    suspended: bool
    is_ban_threshold: bool
    is_warning_only: bool
    active_category: TierCategory | None


class FlatSuspensionResponse(BaseModel):
    discord_id: str
    type: FlatType
    days_added: int
    ends: datetime
    suspended: bool


class ModifyDaysResponse(BaseModel):
    discord_id: str
    days_delta: int         # signed
    new_ends: datetime


class RemoveTierResponse(BaseModel):
    discord_id: str
    category: TierCategory
    new_tier: int
    new_decays: datetime | None
    was_changed: bool


class SuspensionRecordResponse(BaseModel):
    discord_id: str
    suspended: bool
    ends: datetime | None
    suspended_roles: list[str]
    active_category: TierCategory | Literal["flat"] | None


class ActiveSuspension(BaseModel):
    discord_id: str
    ends: datetime


class PendingSuspensionResponse(BaseModel):
    discord_id: str
    punishment_type: str
    reason: str | None


__all__: Final[list[str]] = [
    "TierCategory",
    "FlatType",
    "InfractionRecord",
    "SuspensionDocument",
    "PendingSuspensionDocument",
    "RecordTierInfractionRequest",
    "RecordFlatSuspensionRequest",
    "ModifyDaysRequest",
    "RemoveTierRequest",
    "UnsuspendRequest",
    "CreatePendingSuspensionRequest",
    "TierInfractionResponse",
    "FlatSuspensionResponse",
    "ModifyDaysResponse",
    "RemoveTierResponse",
    "SuspensionRecordResponse",
    "ActiveSuspension",
    "PendingSuspensionResponse",
]
