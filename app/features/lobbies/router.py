"""The v2 lobbies surface: two routers on one prefix, gated separately (D94).

⚠ Both routers declare `prefix="/api/v2/lobbies"` and resolve as ONE ordered
route table, so registration order decides which gate a path meets.
`mite_router` is included first in `app/api/router.py` -- C5 section 6b.

⚠ Within `activity_router`, the literal `/active` is declared BEFORE the
browse route: a parameterised path registered first would swallow it. There
is no `/{id}` route yet -- it arrives at CP5, and this ordering already
accommodates it.

Handlers catch only what they can name. `except Exception` on a v2 route is
what D92's catch-all exists to replace.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, status
from pymongo import AsyncMongoClient

from app.core.dependencies import (
    get_database,
    require_activity_token,
    require_mito_token,
)
from app.core.errors import conflict, invalid_request
from app.features.lobbies.modes import InvalidLobbyShape
from app.features.lobbies.repository import LobbyInsertRefused, LobbyRepository
from app.features.lobbies.schemas import CreateLobbyRequest
from app.features.lobbies.service import LobbyService
from app.features.seasons.repository import SeasonsRepository

REFUSAL_MESSAGES = {
    "one_active_lobby_per_channel": (
        "This channel already has an open lobby. Cancel it or use another channel."
    ),
    "one_active_seat_per_player": (
        "Someone in the roster is already seated in another open lobby."
    ),
}

mite_router = APIRouter(
    prefix="/api/v2/lobbies",
    tags=["lobbies"],
    dependencies=[Depends(require_mito_token)],
)

activity_router = APIRouter(
    prefix="/api/v2/lobbies",
    tags=["lobbies"],
    dependencies=[Depends(require_activity_token)],
)


def _service(db: AsyncMongoClient) -> LobbyService:
    return LobbyService(LobbyRepository(db), SeasonsRepository(db))


@mite_router.post("", status_code=status.HTTP_201_CREATED)
async def create_lobby(
    body: CreateLobbyRequest,
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    try:
        return await _service(db).create(body)
    except InvalidLobbyShape as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyInsertRefused as exc:
        # Both D71 invariants are 409. The index name says which, and the
        # message says it in words a host can act on.
        raise conflict(REFUSAL_MESSAGES.get(exc.index, str(exc))) from exc


@activity_router.get("/active")
async def resolve_active(
    guild_id: str = Query(min_length=1),
    channel_id: str = Query(min_length=1),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any] | None:
    """One open lobby or none, by the D71 index."""
    return await _service(db).resolve_active(guild_id, channel_id)


@activity_router.get("")
async def browse_lobbies(
    guild_id: str = Query(min_length=1),
    edition: str | None = Query(default=None),
    game_type: str | None = Query(default=None),
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Open lobbies for a guild (D180).

    ⚠ `guild_id` is required, never defaulted: a service token is
    per-service, not per-guild, so an unfiltered read would expose every
    lobby on the deployment to any holder of it.
    """
    return await _service(db).browse(guild_id, edition, game_type)


__all__ = ["activity_router", "mite_router"]
