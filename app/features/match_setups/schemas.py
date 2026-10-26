"""/draft: what Mite asks for and the deal it gets back."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_DRAFTS = {"civ6": 14, "civ7": 10}


class DraftRequest(BaseModel):
    """How many pools, their sizes, the age and the host's bans."""

    model_config = ConfigDict(extra="forbid")

    host_id: str = Field(min_length=1, max_length=32)
    edition: Literal["civ6", "civ7"]
    game_type: Literal["ffa", "teamer", "duel"]
    drafts: int = Field(ge=2, le=14)
    leaders: int = Field(ge=1)
    # civ7 only, as is the starting age; no age means every age.
    civs: int | None = Field(default=None, ge=1)
    starting_age: Literal["AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN"] | None = (
        None
    )
    leader_bans: list[str] = Field(default_factory=list, max_length=100)
    civ_bans: list[str] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def _suits_the_edition(self) -> DraftRequest:
        if self.drafts > MAX_DRAFTS[self.edition]:
            raise ValueError(
                f"{self.edition} deals at most {MAX_DRAFTS[self.edition]} drafts"
            )
        if self.game_type == "duel" and self.drafts != 2:
            raise ValueError("a duel is two drafts")
        if self.edition == "civ6" and (
            self.civs is not None or self.starting_age is not None or self.civ_bans
        ):
            raise ValueError("civ6 deals leaders only")
        if self.edition == "civ7" and self.civs is None:
            raise ValueError("civ7 needs a civ count")
        return self


class DealtPool(BaseModel):
    leaders: list[str]
    civs: list[str]


class DealtBans(BaseModel):
    leader: list[str]
    civ: list[str]


class DraftDeal(BaseModel):
    pools: list[DealtPool]
    bans: DealtBans


__all__ = ["MAX_DRAFTS", "DealtBans", "DealtPool", "DraftDeal", "DraftRequest"]
