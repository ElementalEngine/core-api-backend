from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class LeaderRow(BaseModel):
    token: str
    name: str
    # civ6 only: many-to-one, and null where no save has shown the pair yet.
    civ: Optional[str] = None


class CivRow(BaseModel):
    token: str
    name: str
    age_pool: str


class CivDataResponse(BaseModel):
    edition: str
    # Global, not per-edition: the lobby stamps one value (D96).
    leader_data_version: int
    leaders: List[LeaderRow]
    # Empty for civ6, which is leaders only.
    civs: List[CivRow]


__all__ = ["CivDataResponse", "CivRow", "LeaderRow"]
