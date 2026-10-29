"""Mite's match setup routes: /draft's deal, /lobby from host to draft, and
what /vote's routes share with them."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, status
from pymongo import AsyncMongoClient

from app.core.constants import Edition
from app.core.dependencies import (
    actor_discord_id,
    actor_is_staff,
    get_database,
    require_mito_token,
)
from app.core.errors import api_error, conflict, forbidden, invalid_request, not_found
from app.features.civdata.repository import CivDataRepository
from app.features.match_setups.bans import ban_caps
from app.features.match_setups.deal import DraftRefused, checked_bans, deal_draft
from app.features.match_setups.lobby import (
    CANCELLED,
    DONE,
    IDLE_LIMIT,
    SETUP_IDLE_LIMIT,
    LobbyRefused,
    NotTheHost,
    ballot_counted,
    cancel_reason,
    edited,
    finished,
    joined,
    left,
    lobby_code,
    new_lobby,
    picked,
    revealed,
    started,
    tallied,
    vote_placed,
    went_live,
)
from app.features.match_setups.questions import UnknownBallot, questions_for
from app.features.match_setups.record import draft_setup
from app.features.match_setups.repository import (
    CodeTaken,
    MatchSetupRepository,
    SeatedElsewhere,
)
from app.features.match_setups.schemas import (
    DraftDeal,
    DraftRequest,
    LeaderPick,
    LobbyBallot,
    LobbyCancel,
    LobbyEdit,
    LobbyLive,
    LobbyRequest,
    LobbyView,
    SettingQuestion,
    VotePlace,
)
from app.features.seasons.repository import SeasonNotSeededError, SeasonsRepository

router = APIRouter(
    prefix="/api/v2/match-setups",
    tags=["match-setups"],
    dependencies=[Depends(require_mito_token)],
)

# A change read and written in the same moment as another is tried again.
WRITE_ATTEMPTS = 3
SEATED_ELSEWHERE = "you are already in another open lobby"


async def open_season_id(db: AsyncMongoClient, edition: str) -> Any:
    """The season a game counts towards; none before seasons are seeded."""
    try:
        return (await SeasonsRepository(db).get_current_season(edition))["_id"]
    except SeasonNotSeededError:
        return None


async def civ_data_for(db: AsyncMongoClient, edition: str) -> dict[str, Any]:
    civ_data = await CivDataRepository(db).fetch(edition)
    if civ_data["leader_data_version"] is None:
        raise api_error(
            code="CIV_DATA_NOT_SEEDED",
            message=f"civ_data holds no documents for {edition}.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return civ_data


def lobby_view(lobby: Mapping[str, Any]) -> dict[str, Any]:
    return {**lobby, "id": str(lobby["_id"])}


def _try_again(message: str) -> Exception:
    return api_error(
        code="CONFLICT",
        message=message,
        status_code=status.HTTP_409_CONFLICT,
        retryable=True,
    )


async def civ6_leaders(db: AsyncMongoClient) -> list[str]:
    return [row["token"] for row in (await civ_data_for(db, "civ6"))["leaders"]]


async def read_setup(
    db: AsyncMongoClient, setup_id: str, command: str = "lobby"
) -> dict[str, Any]:
    setup = await MatchSetupRepository(db).lobby(setup_id, command)
    if setup is None:
        raise not_found(f"there is no such {command}")
    return setup


async def change_setup(
    db: AsyncMongoClient,
    setup_id: str,
    change: Callable[[dict[str, Any]], dict[str, Any]],
    command: str = "lobby",
) -> dict[str, Any]:
    """Read the lobby (or vote), work out the change from what was read, and
    write it only if nobody changed it in between. One it closes is stamped."""
    for _ in range(WRITE_ATTEMPTS):
        setup = await read_setup(db, setup_id, command)
        try:
            changes = change(setup)
        except NotTheHost as exc:
            raise forbidden(str(exc)) from exc
        except LobbyRefused as exc:
            raise conflict(str(exc)) from exc
        now = datetime.now(UTC)
        if changes.get("status") in (DONE, CANCELLED):
            changes = {**changes, "closed_at": now}
        try:
            written = await MatchSetupRepository(db).update_lobby(setup, changes, now)
        except SeatedElsewhere as exc:
            raise conflict(SEATED_ELSEWHERE) from exc
        if written is not None:
            return lobby_view(written)
    raise _try_again(f"the {command} changed while you acted")


async def insert_coded(
    db: AsyncMongoClient, make: Callable[[str], dict[str, Any]], command: str
) -> dict[str, Any]:
    """A new lobby or vote, under a code no other open one here has."""
    for _ in range(WRITE_ATTEMPTS):
        try:
            setup = make(lobby_code())
        except LobbyRefused as exc:
            raise invalid_request(str(exc)) from exc
        try:
            setup["_id"] = await MatchSetupRepository(db).insert(setup)
        except SeatedElsewhere as exc:
            raise conflict(SEATED_ELSEWHERE) from exc
        except CodeTaken:
            continue
        return lobby_view(setup)
    raise _try_again(f"no free {command} code was found")


async def cancel_by_code(
    db: AsyncMongoClient, body: LobbyCancel, actor: str, staff: bool, command: str
) -> dict[str, Any]:
    """The host, or staff, cancel an open lobby or vote by its code."""
    code = body.code.strip().upper()
    found = await MatchSetupRepository(db).lobby_by_code(body.guild_id, code, command)
    if found is None:
        raise not_found(f"no open {command} here has that code")

    def cancelled(setup: dict[str, Any]) -> dict[str, Any]:
        reason = cancel_reason(setup, actor, staff)
        return {"status": CANCELLED, "cancel_reason": reason, "deadline": None}

    return await change_setup(db, str(found["_id"]), cancelled, command)


@router.post("/draft", response_model=DraftDeal)
async def deal_draft_and_record(
    request: DraftRequest, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    """/draft: deal the pools, then record the setup."""
    civ_data = await civ_data_for(db, request.edition)
    try:
        deal = deal_draft(civ_data, request)
    except DraftRefused as exc:
        raise invalid_request(str(exc)) from exc
    season_id = await open_season_id(db, request.edition)
    now = datetime.now(UTC)
    await MatchSetupRepository(db).insert(draft_setup(request, deal, season_id, now))
    return deal


@router.get("/settings/{edition}/{game_type}", response_model=list[SettingQuestion])
async def settings_questions(
    edition: Edition, game_type: Literal["ffa", "duel", "teamer"]
) -> list[dict[str, Any]]:
    """Every setting a game is set up with, draft mode last."""
    try:
        return questions_for(edition, game_type)
    except UnknownBallot as exc:
        raise not_found(str(exc)) from exc


@router.post("/lobbies", status_code=status.HTTP_201_CREATED, response_model=LobbyView)
async def host_lobby(
    request: LobbyRequest,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A lobby in setup, under a code no other open lobby here has."""

    def made(code: str) -> dict[str, Any]:
        return new_lobby(
            **request.model_dump(), host_id=actor, code=code, now=datetime.now(UTC)
        )

    return await insert_coded(db, made, "lobby")


@router.get("/lobbies", response_model=list[LobbyView])
async def open_lobbies(
    guild_id: str = Query(min_length=1, max_length=32),
    host_id: str | None = Query(default=None, min_length=1, max_length=32),
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """The lobbies players can see; with a host, that host's own, in setup or
    not."""
    found = await MatchSetupRepository(db).open_lobbies(guild_id, host_id)
    return [lobby_view(lobby) for lobby in found]


@router.post("/lobbies/close-idle", response_model=list[LobbyView])
async def close_idle_lobbies(
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Close every lobby idle past its limit, so the bot can remove their posts;
    one left in setup, past the shorter one."""
    now = datetime.now(UTC)
    closed = await MatchSetupRepository(db).close_idle(
        now - IDLE_LIMIT, now - SETUP_IDLE_LIMIT, now
    )
    return [lobby_view(lobby) for lobby in closed]


@router.get("/lobbies/due", response_model=list[LobbyView])
async def lobbies_due(
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Countdowns, votes and blind picks past their time, for the bot to move
    on."""
    due = await MatchSetupRepository(db).due(datetime.now(UTC))
    return [lobby_view(lobby) for lobby in due]


@router.post("/lobbies/cancel", response_model=LobbyView)
async def cancel_lobby(
    body: LobbyCancel,
    actor: str = Depends(actor_discord_id),
    staff: bool = Depends(actor_is_staff),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host, or staff, cancel an open lobby by its code."""
    return await cancel_by_code(db, body, actor, staff, "lobby")


@router.get("/lobbies/{lobby_id}", response_model=LobbyView)
async def read_lobby(
    lobby_id: str, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    return lobby_view(await read_setup(db, lobby_id))


@router.post("/lobbies/{lobby_id}/live", response_model=LobbyView)
async def open_lobby(
    lobby_id: str,
    body: LobbyLive,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host confirms the setup; its post opens the lobby to players."""

    def opened(lobby: dict[str, Any]) -> dict[str, Any]:
        questions = questions_for(lobby["edition"], lobby["mode"])
        return went_live(lobby, actor, body.message_id, questions)

    return await change_setup(db, lobby_id, opened)


@router.post("/lobbies/{lobby_id}/join", response_model=LobbyView)
async def join_lobby(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A player sits down; the last seat starts the countdown to the vote."""
    now = datetime.now(UTC)
    return await change_setup(db, lobby_id, lambda lobby: joined(lobby, actor, now))


@router.post("/lobbies/{lobby_id}/leave", response_model=LobbyView)
async def leave_lobby(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    return await change_setup(db, lobby_id, lambda lobby: left(lobby, actor))


@router.put("/lobbies/{lobby_id}/edit", response_model=LobbyView)
async def edit_lobby(
    lobby_id: str,
    body: LobbyEdit,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host's edit, saved whole."""
    cap = ban_caps("civ6", None)["leader"]
    try:
        bans = checked_bans(body.bans, await civ6_leaders(db), cap, "leader")
    except DraftRefused as exc:
        raise invalid_request(str(exc)) from exc

    def saved(lobby: dict[str, Any]) -> dict[str, Any]:
        return edited(
            lobby,
            actor,
            shape=body.shape,
            questions=questions_for(lobby["edition"], lobby["mode"]),
            settings=body.settings,
            bans=bans,
            teams=body.teams,
            removed=body.removed,
            now=datetime.now(UTC),
        )

    return await change_setup(db, lobby_id, saved)


@router.post("/lobbies/{lobby_id}/start", response_model=LobbyView)
async def start_lobby(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host starts, or the bot once a full lobby's countdown ends: the
    seats lock and the bans vote opens."""
    now = datetime.now(UTC)
    return await change_setup(db, lobby_id, lambda lobby: started(lobby, actor, now))


@router.put("/lobbies/{lobby_id}/vote", response_model=LobbyView)
async def place_vote(
    lobby_id: str,
    body: VotePlace,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Where the bot posted the vote, acting as the host."""
    return await change_setup(
        db,
        lobby_id,
        lambda lobby: vote_placed(lobby, actor, body.channel_id, body.messages),
    )


@router.post("/lobbies/{lobby_id}/finished", response_model=LobbyView)
async def finish_voting(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    return await change_setup(db, lobby_id, lambda lobby: finished(lobby, actor))


@router.post("/lobbies/{lobby_id}/ballot", response_model=LobbyView)
async def count_ballot(
    lobby_id: str,
    body: LobbyBallot,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The votes so far, sent by the bot as the host while the players vote: a
    leader reaching a majority is banned at once and stays banned."""
    leaders = await civ6_leaders(db)
    return await change_setup(
        db,
        lobby_id,
        lambda lobby: ballot_counted(
            lobby, actor, ban_votes=body.bans, leaders=leaders
        ),
    )


@router.post("/lobbies/{lobby_id}/tally", response_model=LobbyView)
async def tally_vote(
    lobby_id: str,
    body: LobbyBallot,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Count the bans and deal the draft. The bot counts, acting as the host,
    once all finish or the time is up."""
    leaders = await civ6_leaders(db)
    season_id = await open_season_id(db, "civ6")

    def counted(lobby: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        changes = tallied(lobby, actor, ban_votes=body.bans, leaders=leaders, now=now)
        return {**changes, "season_id": season_id}

    return await change_setup(db, lobby_id, counted)


@router.post("/lobbies/{lobby_id}/pick", response_model=LobbyView)
async def pick_leader(
    lobby_id: str,
    body: LeaderPick,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A player's blind pick."""
    return await change_setup(
        db, lobby_id, lambda lobby: picked(lobby, actor, body.leader)
    )


@router.post("/lobbies/{lobby_id}/reveal", response_model=LobbyView)
async def reveal_picks(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The blind picks revealed. The bot reveals, acting as the host, once all
    have picked or the time is up."""
    return await change_setup(db, lobby_id, lambda lobby: revealed(lobby, actor))


__all__ = [
    "cancel_by_code",
    "change_setup",
    "civ_data_for",
    "insert_coded",
    "lobby_view",
    "open_season_id",
    "read_setup",
    "router",
]
