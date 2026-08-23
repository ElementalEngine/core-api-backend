from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, File, Form, Header, UploadFile

from app.core.dependencies import get_database, require_mito_token
from app.core.errors import api_error
from app.features.matches.editing import EditingService
from app.features.matches.errors import (
    InvalidIDError,
    MatchServiceError,
    NotFoundError,
    ParseError,
)
from app.features.matches.ingest import IngestService
from app.features.matches.router import _read_capped
from app.features.matches.schemas import LeaderboardRankingResponse, MatchResponse
from app.features.matches.schemas_v2 import ContestBody, PlayersPatch
from app.features.matches.service import MatchService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v2",
    tags=["matches-v2"],
    dependencies=[Depends(require_mito_token)],
)


def actor_discord_id(x_actor_discord_id: str = Header()) -> str:
    """D90's one identity header, replacing civup's five."""
    return x_actor_discord_id


def actor_is_staff(x_actor_is_staff: bool = Header(default=False)) -> bool:
    """D152: Mite supplies facts about the guild, core-api owns the rules.

    An assertion by the token holder, not a verification -- it cannot catch
    a Mite bug, and Mite's own check remains the enforcement.
    """
    return x_actor_is_staff


def _not_found(message: str = "Match not found") -> Exception:
    return api_error(code="NOT_FOUND", message=message, status_code=404)


def _forbidden(message: str) -> Exception:
    return api_error(code="FORBIDDEN", message=message, status_code=403)


def _invalid(message: str) -> Exception:
    return api_error(code="INVALID_REQUEST", message=message, status_code=400)


async def _load(svc: MatchService, match_id: str) -> Dict[str, Any]:
    try:
        return await svc.get_match(match_id)
    except NotFoundError as exc:
        raise _not_found() from exc
    except InvalidIDError as exc:
        raise _invalid(str(exc)) from exc


def _require_reporter(doc: Dict[str, Any], actor: str, is_staff: bool) -> None:
    """D91: reporter -> core-api, staff -> Mite. Two guard clauses, no policy layer."""
    if is_staff:
        return
    if doc.get("reporter_discord_id") != actor:
        raise _forbidden("Only the reporter or staff may edit this match.")


def _require_player(doc: Dict[str, Any], actor: str, is_staff: bool) -> None:
    """D91: a player in the match, or staff.

    Unassigned seats hold placeholder ids, so those players cannot be matched
    and cannot contest -- which is why "or staff" is in the rule.
    """
    if is_staff:
        return
    if not any(p.get("discord_id") == actor for p in doc.get("players", [])):
        raise _forbidden("Only a player in this match, or staff, may contest it.")


@router.post("/matches", response_model=MatchResponse)
async def upload_match(
    file: UploadFile = File(...),
    is_cloud: str = Form(...),
    discord_message_id: str = Form(...),
    actor: str = Depends(actor_discord_id),
    db=Depends(get_database),
) -> Dict[str, Any]:
    raw = await _read_capped(file)
    svc = MatchService(db)
    try:
        created = await IngestService(svc).create_from_save(
            raw, actor, is_cloud == "1", discord_message_id
        )
    except ParseError as exc:
        raise _invalid(f"Unrecognized save file format: {exc}") from exc
    logger.info("Stored match %s", created["match_id"])
    return created


# Declared before /matches/{id}: a literal path at the same depth as a
# parameterised one is captured by whichever registers first (§6b).
@router.get("/matches/leaderboard", response_model=LeaderboardRankingResponse)
async def get_leaderboard(
    game: str,
    game_type: str,
    game_mode: str,
    is_seasonal: bool = False,
    is_combined: bool = False,
    db=Depends(get_database),
) -> Any:
    svc = MatchService(db)
    try:
        return await svc.get_leaderboard(
            match_type=game_mode,
            is_cloud=game_type,
            is_seasonal=is_seasonal,
            is_combined=is_combined,
            civ_version=game,
        )
    except NotFoundError as exc:
        raise _not_found(str(exc)) from exc
    except MatchServiceError as exc:
        raise _invalid(str(exc)) from exc


@router.get("/matches/{match_id}", response_model=MatchResponse)
async def get_match(match_id: str, db=Depends(get_database)) -> Dict[str, Any]:
    return await _load(MatchService(db), match_id)


@router.patch("/matches/{match_id}/players", response_model=MatchResponse)
async def patch_players(
    match_id: str,
    body: PlayersPatch,
    actor: str = Depends(actor_discord_id),
    is_staff: bool = Depends(actor_is_staff),
    db=Depends(get_database),
) -> Dict[str, Any]:
    svc = MatchService(db)
    _require_reporter(await _load(svc, match_id), actor, is_staff)
    try:
        return await EditingService(svc).patch_players(
            match_id, body.to_seat_patches(), actor_is_staff=is_staff
        )
    except NotFoundError as exc:
        raise _not_found() from exc
    except MatchServiceError as exc:
        raise _invalid(str(exc)) from exc


@router.post("/matches/{match_id}/approve", response_model=MatchResponse)
async def approve_match(
    match_id: str,
    actor: str = Depends(actor_discord_id),
    db=Depends(get_database),
) -> Dict[str, Any]:
    # Staff-only, enforced Mite-side (D91): staff is a guild property
    # core-api cannot see.
    svc = MatchService(db)
    try:
        return await svc.approve_match(match_id, actor)
    except NotFoundError as exc:
        raise _not_found() from exc
    except (InvalidIDError, MatchServiceError) as exc:
        raise _invalid(str(exc)) from exc


@router.post("/matches/{match_id}/contest", response_model=MatchResponse)
async def contest_match(
    match_id: str,
    body: ContestBody,
    actor: str = Depends(actor_discord_id),
    is_staff: bool = Depends(actor_is_staff),
    db=Depends(get_database),
) -> Dict[str, Any]:
    svc = MatchService(db)
    _require_player(await _load(svc, match_id), actor, is_staff)
    try:
        return await EditingService(svc).contest_report(match_id, actor, body.reason)
    except NotFoundError as exc:
        raise _not_found() from exc
    except MatchServiceError as exc:
        raise _invalid(str(exc)) from exc


@router.post("/matches/{match_id}/revert", response_model=MatchResponse)
async def revert_match(match_id: str, db=Depends(get_database)) -> Dict[str, Any]:
    # Staff-only, enforced Mite-side (D91).
    svc = MatchService(db)
    try:
        return await svc.revert_match(match_id)
    except NotFoundError as exc:
        raise _not_found() from exc
    except (InvalidIDError, MatchServiceError) as exc:
        raise _invalid(str(exc)) from exc


@router.delete("/matches/{match_id}", response_model=MatchResponse)
async def delete_match(
    match_id: str,
    actor: str = Depends(actor_discord_id),
    is_staff: bool = Depends(actor_is_staff),
    db=Depends(get_database),
) -> Dict[str, Any]:
    svc = MatchService(db)
    _require_reporter(await _load(svc, match_id), actor, is_staff)
    try:
        return await EditingService(svc).delete_pending_match(match_id)
    except NotFoundError as exc:
        raise _not_found() from exc
    except MatchServiceError as exc:
        raise _invalid(str(exc)) from exc
