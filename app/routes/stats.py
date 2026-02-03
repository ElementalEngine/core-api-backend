from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_database
from app.models.schemas import BatchStatsRequest, BatchStatsResponse, UserStatsResponse
from app.services.stats_service import InvalidStatsRequestError, StatsNotFoundError, StatsService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/stats", tags=["stats"])


@router.get("/user", response_model=UserStatsResponse)
async def get_user_stats(civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)):
    svc = StatsService(db)
    try:
        return await svc.get_user_stats(civ_version=civ_version, game_type=game_type, discord_id=discord_id)
    except InvalidStatsRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StatsNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable")


@router.post("/batch", response_model=BatchStatsResponse)
async def get_users_stats_batch(payload: BatchStatsRequest, db=Depends(get_database)):
    svc = StatsService(db)
    ids = payload.discord_ids or []
    if len(ids) > 200:
        raise HTTPException(status_code=400, detail="Too many discord ids")

    try:
        results = await svc.get_users_stats_batch(
            civ_version=payload.civ_version,
            game_type=payload.game_type,
            discord_ids=ids,
        )
        normalized_civ_version = (results[0].civ_version if results else payload.civ_version.strip().lower())
        normalized_game_type = (results[0].game_type if results else payload.game_type.strip().lower())
        return BatchStatsResponse(civ_version=normalized_civ_version, game_type=normalized_game_type, results=results)
    except InvalidStatsRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Batch stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable")
