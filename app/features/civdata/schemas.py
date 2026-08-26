from __future__ import annotations

from pydantic import BaseModel


class LeaderRow(BaseModel):
    token: str
    name: str
    # civ6 only: many-to-one, and null where no save has shown the pair yet.
    civ: str | None = None


class CivRow(BaseModel):
    token: str
    name: str
    age_pool: str


class CivDataResponse(BaseModel):
    edition: str
    # Global, not per-edition: the lobby stamps one value (D96).
    leader_data_version: int
    leaders: list[LeaderRow]
    # Empty for civ6, which is leaders only.
    civs: list[CivRow]


__all__ = ["CivDataResponse", "CivRow", "LeaderRow"]
