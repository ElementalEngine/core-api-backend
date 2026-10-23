from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from app.features.matches.schemas import MatchResponse
from app.features.matches.validation import SeatPatch


class SeatPatchIn(BaseModel):
    """One seat's requested changes. Absent means unchanged."""

    seat: int
    placement: int | None = None
    discord_id: str | None = None
    quit: bool | None = None
    sub_out: str | None = None
    leader: str | None = None
    civ: str | None = None

    @model_validator(mode="after")
    def _only_sub_out_is_nullable(self) -> SeatPatchIn:
        for name in ("placement", "discord_id", "quit", "leader", "civ"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(
                    f"{name} may not be null; omit it to leave it unchanged"
                )
        return self


class PlayersPatch(BaseModel):
    players: list[SeatPatchIn] = Field(default_factory=list)

    def fill_civs(self, leader_civs: Mapping[str, str | None]) -> None:
        """A leader set without a civ brings its own, where the civ data names one."""
        for entry in self.players:
            civ = leader_civs.get(entry.leader) if entry.leader is not None else None
            if entry.civ is None and civ:
                entry.civ = civ

    def to_seat_patches(self) -> list[SeatPatch]:
        """Wire model to domain patch."""
        out: list[SeatPatch] = []
        for entry in self.players:
            declared = entry.model_fields_set
            fields: dict[str, Any] = {"seat": entry.seat}
            for name in ("placement", "discord_id", "quit", "sub_out", "leader", "civ"):
                if name in declared:
                    fields[name] = getattr(entry, name)
            out.append(SeatPatch(**fields))
        return out


class ContestBody(BaseModel):
    reason: str


class UploadResponse(MatchResponse):
    """A response model drops undeclared keys, so the duplicate flags are declared."""

    repeated: bool = False
    repeated_by: Literal["file", "composition"] | None = None


class SubQuotaResponse(BaseModel):
    discord_id: str
    count: int
    limit: int
    window_days: int


__all__ = [
    "ContestBody",
    "PlayersPatch",
    "SeatPatchIn",
    "SubQuotaResponse",
    "UploadResponse",
]
