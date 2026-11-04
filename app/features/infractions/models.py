from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field

# ─── Enums ───────────────────────────────────────────────────────────────────


class TierCategory(StrEnum):
    quit = "quit"
    minor = "minor"
    moderate = "moderate"
    major = "major"
    extreme = "extreme"


class FlatType(StrEnum):
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
    suspendedRoles: list[str] = []  # camelCase — matches live collection
    quit: InfractionRecord = Field(default_factory=InfractionRecord)
    minor: InfractionRecord = Field(default_factory=InfractionRecord)
    moderate: InfractionRecord = Field(default_factory=InfractionRecord)
    major: InfractionRecord = Field(default_factory=InfractionRecord)
    extreme: InfractionRecord = Field(default_factory=InfractionRecord)
    active_category: TierCategory | Literal["flat"] | None = (
        None  # additive — None on old docs is safe
    )
