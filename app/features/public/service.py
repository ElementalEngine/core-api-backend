"""The website's totals and ladders. Each ladder is every ranked player, best
first, named as they registered."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, NamedTuple

from pymongo import AsyncMongoClient

from app.core.coerce import as_float, as_int
from app.core.config import settings
from app.core.constants import Edition
from app.features.public.repository import NAME_FIELDS, PublicRepository
from app.features.public.schemas import (
    Ladder,
    LadderKey,
    LadderRow,
    Ladders,
    Overview,
)
from app.features.ratings.repository import RatingsRepository
from app.features.seasons.repository import SeasonNotSeededError, SeasonsRepository

UNREGISTERED = "Unregistered player"


class LadderSource(NamedTuple):
    key: LadderKey
    is_seasonal: bool
    is_cloud: bool
    # "combined" is every mode together.
    match_type: str


# This season's real-time ladder per mode, and cloud games of every mode over
# all time: cloud games have no season.
LADDERS: tuple[LadderSource, ...] = (
    LadderSource("season-ffa", True, False, "ffa"),
    LadderSource("season-teamer", True, False, "teamer"),
    LadderSource("season-duel", True, False, "duel"),
    LadderSource("cloud", False, True, "combined"),
)


def display_name(user: Mapping[str, Any] | None) -> str:
    """The name a player registered with, or UNREGISTERED without a record."""
    if user:
        for field in NAME_FIELDS:
            if name := str(user.get(field) or "").strip():
                return name
    return UNREGISTERED


def ladder_rows(
    docs: Sequence[Mapping[str, Any]], users: Mapping[str, Mapping[str, Any]]
) -> list[LadderRow]:
    """Stat documents, already in ladder order, as ranked and named rows."""
    rows: list[LadderRow] = []
    for rank, doc in enumerate(docs, start=1):
        mu = as_float(doc.get("mu"), settings.ts_mu)
        last = doc.get("lastModified")
        rows.append(
            LadderRow(
                rank=rank,
                display_name=display_name(users.get(str(doc["_id"]))),
                rating=round(mu),
                mu=mu,
                sigma=as_float(doc.get("sigma"), settings.ts_sigma),
                games=as_int(doc.get("games"), 0),
                wins=as_int(doc.get("wins"), 0),
                first=as_int(doc.get("first"), 0),
                last_active_at=last if isinstance(last, datetime) else None,
            )
        )
    return rows


async def overview(db: AsyncMongoClient) -> Overview:
    updated_at = datetime.now(UTC)
    repo = PublicRepository(db)
    return Overview(
        updated_at=updated_at,
        registered_players=await repo.registered_players(),
        matches_played=await repo.matches_played(),
    )


async def _open_season(db: AsyncMongoClient, edition: Edition) -> dict[str, Any] | None:
    try:
        return await SeasonsRepository(db).get_current_season(edition)
    except SeasonNotSeededError:
        return None


async def ladders(db: AsyncMongoClient, edition: Edition) -> Ladders:
    updated_at = datetime.now(UTC)
    season = await _open_season(db, edition)
    ratings = RatingsRepository(db)
    docs_by_key: dict[LadderKey, list[dict[str, Any]]] = {}
    for source in LADDERS:
        result = await ratings.get_leaderboard(
            civ_version=edition,
            is_seasonal=source.is_seasonal,
            match_type=source.match_type,
            is_cloud=source.is_cloud,
            is_combined=source.match_type == "combined",
            min_games=settings.ladder_min_games,
        )
        docs_by_key[source.key] = result.rows

    ids = sorted({str(doc["_id"]) for docs in docs_by_key.values() for doc in docs})
    users = await PublicRepository(db).names_by_discord_id(ids)
    return Ladders(
        updated_at=updated_at,
        edition=edition,
        season_label=season["label"] if season else None,
        season_started_at=season["started_at"] if season else None,
        min_games=settings.ladder_min_games,
        ladders=[
            Ladder(key=key, total=len(docs), rows=ladder_rows(docs, users))
            for key, docs in docs_by_key.items()
        ],
    )


__all__ = [
    "LADDERS",
    "UNREGISTERED",
    "display_name",
    "ladder_rows",
    "ladders",
    "overview",
]
