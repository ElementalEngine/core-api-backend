"""The v2 lobbies surface: two routers on one prefix, gated separately.

The activity router serves the in-Discord app and requires an actor header on
every route; the mite router serves the bot and passes no viewer, since the
bot holds no seat. Both mount under `/api/v2/lobbies`, and declaration order
matters -- a literal path registered after a parameterised one is swallowed
by it.

Handlers translate service exceptions into the error envelope. Anything not
translated here reaches the catch-all and becomes a 500.

Governed by D90, D94, D186.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, Query, Response, status
from pymongo import AsyncMongoClient

from app.core.dependencies import (
    actor_discord_id,
    get_database,
    require_activity_token,
    require_mito_token,
)
from app.core.errors import conflict, forbidden, invalid_request, not_found
from app.features.civdata.repository import CivDataRepository
from app.features.lobbies.modes import InvalidLobbyShape, InvalidSeating
from app.features.lobbies.repository import LobbyInsertRefused, LobbyRepository
from app.features.lobbies.schemas import (
    ChangeSeatRequest,
    CreateLobbyRequest,
    SubmitBallotRequest,
    SubmitBansRequest,
    SubmitPickRequest,
)
from app.features.lobbies.service import (
    InvalidLobbyId,
    LobbyNotFound,
    LobbyService,
    NotSeated,
    NotTheHost,
    NotYourTurn,
    PickIsFinal,
    SeatChangeRefused,
)
from app.features.seasons.repository import SeasonsRepository

REFUSAL_MESSAGES = {
    "one_active_lobby_per_channel": (
        "This channel already has an open lobby. Cancel it or use another channel."
    ),
    "one_active_seat_per_player": (
        "Someone in the roster is already seated in another open lobby."
    ),
}

logger = logging.getLogger(__name__)

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
    return LobbyService(
        LobbyRepository(db), SeasonsRepository(db), CivDataRepository(db)
    )


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
        raise conflict(REFUSAL_MESSAGES.get(exc.index, str(exc))) from exc


@activity_router.get("/active")
async def resolve_active(
    guild_id: str = Query(min_length=1),
    channel_id: str = Query(min_length=1),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any] | None:
    """One open lobby or none, by the index."""
    return await _service(db).resolve_active(guild_id, channel_id, actor)


@activity_router.get("")
async def browse_lobbies(
    guild_id: str = Query(min_length=1),
    edition: str | None = Query(default=None),
    game_type: str | None = Query(default=None),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Open lobbies for a guild."""
    return await _service(db).browse(guild_id, actor, edition, game_type)


@activity_router.get("/{lobby_id}", response_model=None)
async def read_lobby(
    lobby_id: str,
    since: int | None = Query(default=None, ge=1),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any] | Response:
    """One lobby, censored for the caller, revision-gated."""
    try:
        snapshot = await _service(db).read(lobby_id, actor, since)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    if snapshot is None:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return snapshot


@activity_router.patch("/{lobby_id}/seats", response_model=None)
async def change_seat(
    lobby_id: str,
    request: ChangeSeatRequest = Body(),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """
    Self-place, leave, move and host rearrange (C5), returning the updated censored
    snapshot so the caller never waits for a poll tick.
    """
    try:
        lobby = await _service(db).change_seat(lobby_id, actor, request)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    except NotTheHost as exc:
        raise forbidden(str(exc)) from exc
    except InvalidSeating as exc:
        raise invalid_request(str(exc)) from exc
    except SeatChangeRefused as exc:
        logger.warning(
            "seat change refused. lobby=%s actor=%s expected=%s current=%s",
            lobby_id,
            actor,
            exc.expected,
            exc.current,
        )
        raise conflict(
            str(exc),
            details={
                "expected_revision": exc.expected,
                "current_revision": exc.current,
            },
        ) from exc
    logger.info(
        "seat changed. lobby=%s actor=%s expected=%s current=%s",
        lobby_id,
        actor,
        request.expected_revision,
        lobby["revision"],
    )
    return lobby


@activity_router.post("/{lobby_id}/start", response_model=None)
async def start_lobby(
    lobby_id: str,
    expected_revision: int = Body(embed=True, ge=1),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Close seating, open the settings vote."""
    try:
        lobby = await _service(db).start(lobby_id, actor, expected_revision)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    except NotTheHost as exc:
        raise forbidden(str(exc)) from exc
    except SeatChangeRefused as exc:
        logger.warning(
            "start refused. lobby=%s actor=%s expected=%s current=%s",
            lobby_id,
            actor,
            exc.expected,
            exc.current,
        )
        raise conflict(
            str(exc),
            details={
                "expected_revision": exc.expected,
                "current_revision": exc.current,
            },
        ) from exc
    logger.info(
        "lobby started. lobby=%s actor=%s expected=%s current=%s",
        lobby_id,
        actor,
        expected_revision,
        lobby["revision"],
    )
    return lobby


@activity_router.put("/{lobby_id}/votes", response_model=None)
async def submit_ballot(
    lobby_id: str,
    request: SubmitBallotRequest = Body(),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """One seat's settings ballot, resolving the phase on the last one."""
    try:
        lobby = await _service(db).submit_ballot(lobby_id, actor, request)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    except NotSeated as exc:
        raise forbidden(str(exc)) from exc
    except InvalidSeating as exc:
        raise invalid_request(str(exc)) from exc
    except SeatChangeRefused as exc:
        logger.warning(
            "ballot refused. lobby=%s actor=%s expected=%s current=%s",
            lobby_id,
            actor,
            exc.expected,
            exc.current,
        )
        raise conflict(
            str(exc),
            details={
                "expected_revision": exc.expected,
                "current_revision": exc.current,
            },
        ) from exc
    logger.info(
        "ballot submitted. lobby=%s actor=%s expected=%s current=%s phase=%s",
        lobby_id,
        actor,
        request.expected_revision,
        lobby["revision"],
        lobby["phase"],
    )
    return lobby


@activity_router.put("/{lobby_id}/bans", response_model=None)
async def submit_bans(
    lobby_id: str,
    request: SubmitBansRequest = Body(),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """One seat's bans, resolving the phase on the last submission."""
    try:
        lobby = await _service(db).submit_bans(lobby_id, actor, request)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    except NotSeated as exc:
        raise forbidden(str(exc)) from exc
    except InvalidSeating as exc:
        raise invalid_request(str(exc)) from exc
    except SeatChangeRefused as exc:
        logger.warning(
            "bans refused. lobby=%s actor=%s expected=%s current=%s",
            lobby_id,
            actor,
            exc.expected,
            exc.current,
        )
        raise conflict(
            str(exc),
            details={
                "expected_revision": exc.expected,
                "current_revision": exc.current,
            },
        ) from exc
    logger.info(
        "bans submitted. lobby=%s actor=%s expected=%s current=%s phase=%s",
        lobby_id,
        actor,
        request.expected_revision,
        lobby["revision"],
        lobby["phase"],
    )
    return lobby


@activity_router.put("/{lobby_id}/picks", response_model=None)
async def submit_pick(
    lobby_id: str,
    request: SubmitPickRequest = Body(),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """One seat's pick, completing the lobby on the last one."""
    try:
        lobby = await _service(db).submit_pick(lobby_id, actor, request)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    except NotSeated as exc:
        raise forbidden(str(exc)) from exc
    except NotYourTurn as exc:
        raise forbidden(str(exc)) from exc
    except PickIsFinal as exc:
        raise conflict(str(exc)) from exc
    except InvalidSeating as exc:
        raise invalid_request(str(exc)) from exc
    except SeatChangeRefused as exc:
        logger.warning(
            "pick refused. lobby=%s actor=%s expected=%s current=%s",
            lobby_id,
            actor,
            exc.expected,
            exc.current,
        )
        raise conflict(
            str(exc),
            details={
                "expected_revision": exc.expected,
                "current_revision": exc.current,
            },
        ) from exc
    logger.info(
        "pick submitted. lobby=%s actor=%s expected=%s current=%s phase=%s",
        lobby_id,
        actor,
        request.expected_revision,
        lobby["revision"],
        lobby["phase"],
    )
    return lobby


@activity_router.post("/{lobby_id}/cancel", response_model=None)
async def cancel_lobby(
    lobby_id: str,
    expected_revision: int = Body(embed=True, ge=1),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host ends the lobby. Frees the channel and every seat at once."""
    try:
        lobby = await _service(db).cancel(lobby_id, actor, expected_revision)
    except InvalidLobbyId as exc:
        raise invalid_request(str(exc)) from exc
    except LobbyNotFound as exc:
        raise not_found("Lobby not found") from exc
    except NotTheHost as exc:
        raise forbidden(str(exc)) from exc
    except SeatChangeRefused as exc:
        raise conflict(
            str(exc),
            details={
                "expected_revision": exc.expected,
                "current_revision": exc.current,
            },
        ) from exc
    logger.info(
        "lobby cancelled. lobby=%s actor=%s current=%s",
        lobby_id,
        actor,
        lobby["revision"],
    )
    return lobby


@mite_router.post("/claim-post", response_model=None)
async def claim_post(
    response: Response,
    guild_id: str = Query(min_length=1, max_length=32),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any] | None:
    """Claim the oldest unposted finished lobby, or 204 when there is none."""
    lobby = await _service(db).claim_post(guild_id)
    if lobby is None:
        response.status_code = status.HTTP_204_NO_CONTENT
        return None
    logger.info("lobby claimed for posting. lobby=%s guild=%s", lobby["_id"], guild_id)
    return lobby


__all__ = ["activity_router", "mite_router"]
