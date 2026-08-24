from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.core.dependencies import get_database, require_mito_token
from app.features.stats.errors import InvalidStatsRequestError, StatsNotFoundError
from app.features.stats.schemas import (
    BatchStatsRequest,
    BatchStatsResponse,
    TeamGenRequest,
    TeamGenResponse,
    UserStatsResponse,
)
from app.features.stats.service import StatsService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/stats",
    tags=["stats"],
    dependencies=[Depends(require_mito_token)],
)


@router.get("/user", response_model=UserStatsResponse)
async def get_user_stats(
    civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)
):
    service = StatsService(db)
    try:
        return await service.get_user_stats(
            civ_version=civ_version, game_type=game_type, discord_id=discord_id
        )
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
        normalized_civ_version = (
            results[0].civ_version if results else payload.civ_version.strip().lower()
        )
        normalized_game_type = (
            results[0].game_type if results else payload.game_type.strip().lower()
        )
        return BatchStatsResponse(
            civ_version=normalized_civ_version,
            game_type=normalized_game_type,
            results=results,
        )
    except InvalidStatsRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("Batch stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable") from exc


@router.put("/reset/user", response_model=UserStatsResponse)
async def reset_user_stats(
    civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)
):
    service = StatsService(db)
    try:
        return await service.reset_user_stats(
            civ_version=civ_version, game_type=game_type, discord_id=discord_id
        )
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
