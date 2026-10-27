"""Mite's match setup routes: /draft's deal, and /lobby from host to draft."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, status
from pymongo import AsyncMongoClient

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
    LobbyRefused,
    NotTheHost,
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


async def _open_season_id(db: AsyncMongoClient, edition: str) -> Any:
    """The season a game counts towards; none before seasons are seeded."""
    try:
        return (await SeasonsRepository(db).get_current_season(edition))["_id"]
    except SeasonNotSeededError:
        return None


async def _civ_data(db: AsyncMongoClient, edition: str) -> dict[str, Any]:
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


async def _change(
    db: AsyncMongoClient,
    lobby_id: str,
    change: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Read the lobby, work out the change from what was read, and write it
    only if nobody changed the lobby in between."""
    setups = MatchSetupRepository(db)
    for _ in range(WRITE_ATTEMPTS):
        lobby = await setups.lobby(lobby_id)
        if lobby is None:
            raise not_found("there is no such lobby")
        try:
            changes = change(lobby)
        except NotTheHost as exc:
            raise forbidden(str(exc)) from exc
        except LobbyRefused as exc:
            raise conflict(str(exc)) from exc
        try:
            written = await setups.update_lobby(lobby, changes, datetime.now(UTC))
        except SeatedElsewhere as exc:
            raise conflict(SEATED_ELSEWHERE) from exc
        if written is not None:
            return lobby_view(written)
    raise api_error(
        code="CONFLICT",
        message="the lobby changed while you acted",
        status_code=status.HTTP_409_CONFLICT,
        retryable=True,
    )


@router.post("/draft", response_model=DraftDeal)
async def deal_draft_and_record(
    request: DraftRequest, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    """/draft: deal the pools, then record the setup."""
    civ_data = await _civ_data(db, request.edition)
    try:
        deal = deal_draft(civ_data, request)
    except DraftRefused as exc:
        raise invalid_request(str(exc)) from exc
    season_id = await _open_season_id(db, request.edition)
    now = datetime.now(UTC)
    await MatchSetupRepository(db).insert(draft_setup(request, deal, season_id, now))
    return deal


@router.get("/settings/{edition}/{game_type}", response_model=list[SettingQuestion])
async def settings_questions(
    edition: Literal["civ6", "civ7"], game_type: Literal["ffa", "duel", "teamer"]
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
    for _ in range(WRITE_ATTEMPTS):
        try:
            lobby = new_lobby(
                **request.model_dump(),
                host_id=actor,
                code=lobby_code(),
                now=datetime.now(UTC),
            )
        except LobbyRefused as exc:
            raise invalid_request(str(exc)) from exc
        try:
            lobby["_id"] = await MatchSetupRepository(db).insert(lobby)
        except SeatedElsewhere as exc:
            raise conflict(SEATED_ELSEWHERE) from exc
        except CodeTaken:
            continue
        return lobby_view(lobby)
    raise api_error(
        code="CONFLICT",
        message="no free lobby code was found",
        status_code=status.HTTP_409_CONFLICT,
        retryable=True,
    )


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
    """Close every lobby idle past the limit, so the bot can mark their posts."""
    now = datetime.now(UTC)
    closed = await MatchSetupRepository(db).close_idle(now - IDLE_LIMIT, now)
    return [lobby_view(lobby) for lobby in closed]


@router.get("/lobbies/due", response_model=list[LobbyView])
async def lobbies_due(
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Votes and blind picks past their time, for the bot to close."""
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
    code = body.code.strip().upper()
    found = await MatchSetupRepository(db).lobby_by_code(body.guild_id, code)
    if found is None:
        raise not_found("no open lobby here has that code")

    def cancelled(lobby: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": CANCELLED,
            "cancel_reason": cancel_reason(lobby, actor, staff),
            "deadline": None,
            "closed_at": datetime.now(UTC),
        }

    return await _change(db, str(found["_id"]), cancelled)


@router.get("/lobbies/{lobby_id}", response_model=LobbyView)
async def read_lobby(
    lobby_id: str, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    lobby = await MatchSetupRepository(db).lobby(lobby_id)
    if lobby is None:
        raise not_found("there is no such lobby")
    return lobby_view(lobby)


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

    return await _change(db, lobby_id, opened)


@router.post("/lobbies/{lobby_id}/join", response_model=LobbyView)
async def join_lobby(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A player sits down; the last seat starts the bans vote."""
    now = datetime.now(UTC)
    return await _change(db, lobby_id, lambda lobby: joined(lobby, actor, now))


@router.post("/lobbies/{lobby_id}/leave", response_model=LobbyView)
async def leave_lobby(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    return await _change(db, lobby_id, lambda lobby: {"players": left(lobby, actor)})


@router.put("/lobbies/{lobby_id}/edit", response_model=LobbyView)
async def edit_lobby(
    lobby_id: str,
    body: LobbyEdit,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host's edit, saved whole."""
    leaders = [row["token"] for row in (await _civ_data(db, "civ6"))["leaders"]]
    cap = ban_caps("civ6", None)["leader"]
    try:
        bans = checked_bans(body.bans, leaders, cap, "leader")
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
        )

    return await _change(db, lobby_id, saved)


@router.post("/lobbies/{lobby_id}/start", response_model=LobbyView)
async def start_lobby(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host starts before it fills: the seats lock and the bans vote opens."""
    now = datetime.now(UTC)
    return await _change(db, lobby_id, lambda lobby: started(lobby, actor, now))


@router.put("/lobbies/{lobby_id}/vote", response_model=LobbyView)
async def place_vote(
    lobby_id: str,
    body: VotePlace,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Where the bot posted the vote, acting as the host."""
    return await _change(
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
    return await _change(db, lobby_id, lambda lobby: finished(lobby, actor))


@router.post("/lobbies/{lobby_id}/tally", response_model=LobbyView)
async def tally_vote(
    lobby_id: str,
    body: LobbyBallot,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Count the bans and deal the draft. The bot counts, acting as the host,
    once all finish or the time is up."""
    leaders = [row["token"] for row in (await _civ_data(db, "civ6"))["leaders"]]
    season_id = await _open_season_id(db, "civ6")

    def counted(lobby: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        changes = tallied(lobby, actor, ban_votes=body.bans, leaders=leaders, now=now)
        closed = {"closed_at": now} if changes.get("status") == DONE else {}
        return {**changes, "season_id": season_id, **closed}

    return await _change(db, lobby_id, counted)


@router.post("/lobbies/{lobby_id}/pick", response_model=LobbyView)
async def pick_leader(
    lobby_id: str,
    body: LeaderPick,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A player's blind pick."""
    return await _change(db, lobby_id, lambda lobby: picked(lobby, actor, body.leader))


@router.post("/lobbies/{lobby_id}/reveal", response_model=LobbyView)
async def reveal_picks(
    lobby_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The blind picks revealed. The bot reveals, acting as the host, once all
    have picked or the time is up."""

    def shown(lobby: dict[str, Any]) -> dict[str, Any]:
        return {**revealed(lobby, actor), "closed_at": datetime.now(UTC)}

    return await _change(db, lobby_id, shown)


__all__ = ["router"]
