from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Query, Response, UploadFile
from pymongo import AsyncMongoClient

from app.core.config import settings
from app.core.dependencies import (
    actor_discord_id,
    actor_is_staff,
    get_database,
    require_mito_token,
)
from app.core.errors import api_error, forbidden, invalid_request, not_found
from app.features.civdata.repository import CivDataRepository
from app.features.matches.auto_approve import auto_approve
from app.features.matches.editing import EditingService
from app.features.matches.errors import (
    ApprovingError,
    InvalidIDError,
    MatchServiceError,
    NotFoundError,
    ParseError,
    RetryLaterError,
)
from app.features.matches.ingest import IngestService
from app.features.matches.leaderboard import LeaderboardService
from app.features.matches.schemas import LeaderboardRankingResponse, MatchResponse
from app.features.matches.schemas_v2 import (
    ContestBody,
    HistoryPost,
    PlayersPatch,
    SubQuotaResponse,
    UploadResponse,
)
from app.features.matches.service import MatchService
from app.features.matches.sub_quota import sub_quota
from app.features.matches.upload import read_capped
from app.features.seasons.repository import SeasonNotSeededError, SeasonsRepository

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v2",
    tags=["matches-v2"],
    dependencies=[Depends(require_mito_token)],
)


async def _load(svc: MatchService, match_id: str) -> dict[str, Any]:
    try:
        return await svc.get_match(match_id)
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except InvalidIDError as exc:
        raise invalid_request(str(exc)) from exc


async def _named(db: AsyncMongoClient, doc: dict[str, Any]) -> dict[str, Any]:
    """The match with its season's name: the one it was approved in, else the
    edition's open season."""
    doc["season_label"] = await SeasonsRepository(db).label_for(
        str(doc.get("game", "")).lower(), doc.get("season_id")
    )
    return doc


def _require_reporter(doc: dict[str, Any], actor: str, is_staff: bool) -> None:
    """Reporter -> core-api, staff -> Mite. Two guard clauses, no policy layer."""
    if is_staff:
        return
    if doc.get("reporter_discord_id") != actor:
        raise forbidden("Only the reporter or staff may edit this match.")


def _retry_later(exc: MatchServiceError) -> Exception:
    return api_error(code="CONFLICT", message=str(exc), status_code=409, retryable=True)


def _require_player(doc: dict[str, Any], actor: str) -> None:
    """Only a player in the match contests it; staff edit or remove instead."""
    if not any(p.get("discord_id") == actor for p in doc.get("players", [])):
        raise forbidden("Only a player in this match may contest it.")


@router.post("/matches", response_model=UploadResponse)
async def upload_match(
    file: UploadFile = File(...),
    is_cloud: str = Form(...),
    discord_message_id: str = Form(...),
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    # A form field, so checked here rather than in the schema the bots share.
    if is_cloud not in ("0", "1"):
        raise invalid_request("is_cloud must be 0 or 1.")
    raw = await read_capped(file)
    svc = MatchService(db)
    try:
        created = await IngestService(svc).create_from_save(
            raw, actor, is_cloud == "1", discord_message_id
        )
    except ParseError as exc:
        logger.warning("Save not parsed: %s", exc)
        raise invalid_request("That file could not be read as a Civ save.") from exc
    except MatchServiceError as exc:
        raise invalid_request(str(exc)) from exc
    logger.info("Stored match %s", created["match_id"])
    return await _named(db, created)


# Declared before /matches/{id}: a literal path at the same depth as a
# parameterised one is captured by whichever registers first.
@router.get("/matches/leaderboard", response_model=LeaderboardRankingResponse)
async def get_leaderboard(
    game: str,
    game_type: str,
    game_mode: str,
    is_seasonal: bool = False,
    is_combined: bool = False,
    db: AsyncMongoClient = Depends(get_database),
) -> Any:
    svc = LeaderboardService(db)
    try:
        return await svc.get_leaderboard(
            match_type=game_mode,
            is_cloud=game_type,
            is_seasonal=is_seasonal,
            is_combined=is_combined,
            civ_version=game,
        )
    except ValueError as exc:
        raise invalid_request(str(exc)) from exc
    except NotFoundError as exc:
        raise not_found(str(exc)) from exc
    except MatchServiceError as exc:
        raise invalid_request(str(exc)) from exc


# A literal path too, so it is also declared before /matches/{id}.
@router.get("/matches/sub-quota", response_model=SubQuotaResponse)
async def get_sub_quota(
    discord_id: str = Query(pattern=r"^\d{15,20}$"),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    # Ids are stored as Int64; a larger number is no Discord id.
    if int(discord_id) >= 2**63:
        raise invalid_request("That is not a Discord id.")
    return await sub_quota(MatchService(db).q, discord_id)


@router.get("/matches/{match_id}", response_model=MatchResponse)
async def get_match(match_id: str, db=Depends(get_database)) -> dict[str, Any]:
    return await _named(db, await _load(MatchService(db), match_id))


async def _require_known_civs(
    db: AsyncMongoClient, doc: dict[str, Any], body: PlayersPatch
) -> None:
    """A leader or civ set by hand must be one the civ data knows, as that kind.
    Civ6 has no civ list, so a civ6 leader brings the civ it belongs to."""
    if all(e.leader is None and e.civ is None for e in body.players):
        return
    data = await CivDataRepository(db).fetch(str(doc.get("game", "")).lower())
    leaders = {row["token"] for row in data["leaders"]}
    civs = {row["token"] for row in data["civs"]}
    unknown = sorted(
        {
            e.leader
            for e in body.players
            if e.leader is not None and e.leader not in leaders
        }
        | {e.civ for e in body.players if e.civ is not None and e.civ not in civs}
    )
    if unknown:
        raise invalid_request(f"Unknown leader or civ: {', '.join(unknown)}.")
    body.fill_civs({row["token"]: row.get("civ") for row in data["leaders"]})


@router.patch("/matches/{match_id}/players", response_model=MatchResponse)
async def patch_players(
    match_id: str,
    body: PlayersPatch,
    actor: str = Depends(actor_discord_id),
    is_staff: bool = Depends(actor_is_staff),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    svc = MatchService(db)
    doc = await _load(svc, match_id)
    _require_reporter(doc, actor, is_staff)
    if is_staff:  # for anyone else, the validator refuses a leader or civ
        await _require_known_civs(db, doc, body)
    try:
        patched = await EditingService(svc).patch_players(
            match_id, body.to_seat_patches(), actor_is_staff=is_staff
        )
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except ApprovingError as exc:
        raise _retry_later(exc) from exc
    except MatchServiceError as exc:
        raise invalid_request(str(exc)) from exc
    return await _named(db, patched)


@router.post("/matches/{match_id}/approve", response_model=MatchResponse)
async def approve_match(
    match_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    svc = MatchService(db)
    try:
        approved = await svc.approve_match(match_id, actor)
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except SeasonNotSeededError as exc:
        raise not_found(str(exc)) from exc
    except ApprovingError as exc:
        raise _retry_later(exc) from exc
    except RetryLaterError as exc:
        raise _retry_later(exc) from exc
    except (InvalidIDError, MatchServiceError) as exc:
        raise invalid_request(str(exc)) from exc
    return await _named(db, approved)


@router.post("/matches/{match_id}/contest", response_model=MatchResponse)
async def contest_match(
    match_id: str,
    body: ContestBody,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    svc = MatchService(db)
    _require_player(await _load(svc, match_id), actor)
    try:
        contested = await EditingService(svc).contest_report(
            match_id, actor, body.reason
        )
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except ApprovingError as exc:
        raise _retry_later(exc) from exc
    except MatchServiceError as exc:
        raise invalid_request(str(exc)) from exc
    return await _named(db, contested)


@router.delete("/matches/{match_id}/contest", response_model=MatchResponse)
async def withdraw_contest(
    match_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A player takes back their own contest."""
    svc = MatchService(db)
    try:
        withdrawn = await EditingService(svc).withdraw_contest(match_id, actor)
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except ApprovingError as exc:
        raise _retry_later(exc) from exc
    except MatchServiceError as exc:
        raise invalid_request(str(exc)) from exc
    return await _named(db, withdrawn)


@router.post("/matches/auto-approve", response_model=list[MatchResponse])
async def auto_approve_due(
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Approves, as the actor, every report that needs no staff 48 hours after
    its upload."""
    approved = await auto_approve(
        MatchService(db),
        CivDataRepository(db),
        actor,
        datetime.now(UTC),
        start=settings.auto_approve_start,
    )
    return [await _named(db, match) for match in approved]


@router.put("/matches/{match_id}/history-post", status_code=204)
async def record_history_post(
    match_id: str, body: HistoryPost, db: AsyncMongoClient = Depends(get_database)
) -> Response:
    """The approved result's post, so a revert can redraw it."""
    svc = MatchService(db)
    try:
        found = await svc.q.set_history_message(svc._to_oid(match_id), body.message_id)
    except InvalidIDError as exc:
        raise invalid_request(str(exc)) from exc
    if not found:
        raise not_found("Match not found")
    return Response(status_code=204)


@router.post("/matches/{match_id}/revert", response_model=MatchResponse)
async def revert_match(match_id: str, db=Depends(get_database)) -> dict[str, Any]:
    svc = MatchService(db)
    try:
        reverted = await svc.revert_match(match_id)
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except RetryLaterError as exc:
        raise _retry_later(exc) from exc
    except (InvalidIDError, MatchServiceError) as exc:
        raise invalid_request(str(exc)) from exc
    return await _named(db, reverted)


@router.delete("/matches/{match_id}", response_model=MatchResponse)
async def delete_match(
    match_id: str,
    actor: str = Depends(actor_discord_id),
    is_staff: bool = Depends(actor_is_staff),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    svc = MatchService(db)
    _require_reporter(await _load(svc, match_id), actor, is_staff)
    try:
        deleted = await EditingService(svc).delete_pending_match(match_id)
    except NotFoundError as exc:
        raise not_found("Match not found") from exc
    except ApprovingError as exc:
        raise _retry_later(exc) from exc
    except MatchServiceError as exc:
        raise invalid_request(str(exc)) from exc
    return await _named(db, deleted)
