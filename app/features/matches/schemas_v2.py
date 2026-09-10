from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

from app.features.matches.validation import SeatPatch


class SeatPatchIn(BaseModel):
    """One seat's requested changes. Absent means unchanged."""

    seat: int
    placement: int | None = None
    discord_id: str | None = None
    quit: bool | None = None
    sub_out: str | None = None

    @model_validator(mode="after")
    def _only_sub_out_is_nullable(self) -> SeatPatchIn:
        for name in ("placement", "discord_id", "quit"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(
                    f"{name} may not be null; omit it to leave it unchanged"
                )
        return self


class PlayersPatch(BaseModel):
    players: list[SeatPatchIn] = Field(default_factory=list)

    def to_seat_patches(self) -> list[SeatPatch]:
        """Wire model to domain patch."""
        out: list[SeatPatch] = []
        for entry in self.players:
            declared = entry.model_fields_set
            fields: dict[str, Any] = {"seat": entry.seat}
            for name in ("placement", "discord_id", "quit", "sub_out"):
                if name in declared:
                    fields[name] = getattr(entry, name)
            out.append(SeatPatch(**fields))
        return out


class ContestBody(BaseModel):
    reason: str


__all__ = ["ContestBody", "PlayersPatch", "SeatPatchIn"]
