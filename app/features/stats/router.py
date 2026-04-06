from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException

from app.core.dependencies import get_database
from app.features.stats.errors import InvalidStatsRequestError, StatsNotFoundError
from app.features.stats.schemas import BatchStatsRequest, BatchStatsResponse, TeamGenRequest, TeamGenResponse, UserStatsResponse
from app.features.stats.service import StatsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/stats", tags=["stats"])
legacy_router = APIRouter(prefix="/api/v1", tags=["stats"])


def _pick_civ_version(*, version: Optional[str], civ_version: Optional[str]) -> str:
    chosen_version = (civ_version or version or "").strip()
    if not chosen_version:
        raise HTTPException(status_code=400, detail="Missing civ_version/version")
    return chosen_version


@router.get("/user", response_model=UserStatsResponse)
async def get_user_stats(civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)):
    service = StatsService(db)
    try:
        return await service.get_user_stats(civ_version=civ_version, game_type=game_type, discord_id=discord_id)
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except StatsNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc


@router.post("/batch", response_model=BatchStatsResponse)
async def get_users_stats_batch(payload: BatchStatsRequest, db=Depends(get_database)):
    service = StatsService(db)
    ids = payload.discord_ids or []
    if len(ids) > 200:
        raise HTTPException(status_code=400, detail="Too many discord ids")

    try:
        results = await service.get_users_stats_batch(
            civ_version=payload.civ_version,
            game_type=payload.game_type,
            discord_ids=ids,
        )
        normalized_civ_version = results[0].civ_version if results else payload.civ_version.strip().lower()
        normalized_game_type = results[0].game_type if results else payload.game_type.strip().lower()
        return BatchStatsResponse(civ_version=normalized_civ_version, game_type=normalized_game_type, results=results)
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Batch stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc

@router.put("/reset/user", response_model=UserStatsResponse)
async def reset_user_stats(civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)):
    service = StatsService(db)
    try:
        return await service.reset_user_stats(civ_version=civ_version, game_type=game_type, discord_id=discord_id)
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except StatsNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Reset Stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc

@router.post("/team-gen", response_model=TeamGenResponse)
async def get_team_gen(payload: TeamGenRequest, db=Depends(get_database)):
    service = StatsService(db)
    ids = payload.discord_ids or []
    if len(ids) > 200:
        raise HTTPException(status_code=400, detail="Too many discord ids")

    try:
        return await service.get_team_gen(
            civ_version=payload.civ_version,
            game_type=payload.game_type,
            discord_ids=ids,
        )
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Team gen failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc


@legacy_router.put("/get-user-stats/", response_model=UserStatsResponse)
async def put_get_user_stats(
    version: Optional[str] = Form(None),
    civ_version: Optional[str] = Form(None),
    game_type: str = Form(...),
    discord_id: str = Form(...),
    db=Depends(get_database),
):
    service = StatsService(db)
    chosen_version = _pick_civ_version(version=version, civ_version=civ_version)

    try:
        return await service.get_user_stats(civ_version=chosen_version, game_type=game_type, discord_id=discord_id)
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except StatsNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Legacy stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc


@legacy_router.put("/get-user-stats-batch/", response_model=BatchStatsResponse)
async def put_get_users_stats_batch(
    version: Optional[str] = Form(None),
    civ_version: Optional[str] = Form(None),
    game_type: str = Form(...),
    discord_id_list: List[str] = Form(...),
    db=Depends(get_database),
):
    service = StatsService(db)
    chosen_version = _pick_civ_version(version=version, civ_version=civ_version)

    if len(discord_id_list) > 200:
        raise HTTPException(status_code=400, detail="Too many discord ids")

    try:
        results = await service.get_users_stats_batch(
            civ_version=chosen_version,
            game_type=game_type,
            discord_ids=discord_id_list,
        )
        normalized_civ_version = results[0].civ_version if results else chosen_version.strip().lower()
        normalized_game_type = results[0].game_type if results else game_type.strip().lower()
        return BatchStatsResponse(civ_version=normalized_civ_version, game_type=normalized_game_type, results=results)
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Legacy batch stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc
