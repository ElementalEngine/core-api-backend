from __future__ import annotations

from pydantic import BaseModel


class LeaderRow(BaseModel):
    token: str
    # `name` is the emoji's id name; `display` is what a player reads.
    name: str
    display: str | None = None
    # A Discord application emoji: the portrait the league already sees in
    # embeds, served from Discord's CDN at up to 128px.
    emoji_id: str | None = None
    # civ6 only: many-to-one, and null where no save has shown the pair yet.
    civ: str | None = None
    # civ6 only: the civ's label, set even where `civ` is not yet mapped.
    civ_display: str | None = None


class CivRow(BaseModel):
    token: str
    name: str
    display: str | None = None
    age_pool: str
    emoji_id: str | None = None


class CivDataResponse(BaseModel):
    edition: str
    leader_data_version: int
    leaders: list[LeaderRow]
    # Empty for civ6, which is leaders only.
    civs: list[CivRow]


__all__ = ["CivDataResponse", "CivRow", "LeaderRow"]
