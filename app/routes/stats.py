from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException

from app.dependencies import get_database
from app.models.schemas import BatchStatsRequest, BatchStatsResponse, UserStatsResponse
from app.services.stats_service import InvalidStatsRequestError, StatsNotFoundError, StatsService

logger = logging.getLogger(__name__)

# Modern endpoints (typed, standard HTTP)
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
    except Exception:
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


# Legacy endpoints (PUT + multipart/form-data). These are thin compatibility wrappers around StatsService.
legacy_router = APIRouter(prefix="/api/v1", tags=["stats"])


def _pick_civ_version(*, version: Optional[str], civ_version: Optional[str]) -> str:
    cv = (civ_version or version or "").strip()
    if not cv:
        raise HTTPException(status_code=400, detail="Missing civ_version/version")
    return cv


@legacy_router.put("/get-user-stats/", response_model=UserStatsResponse)
async def put_get_user_stats(
    version: Optional[str] = Form(None),
    civ_version: Optional[str] = Form(None),
    game_type: str = Form(...),
    discord_id: str = Form(...),
    db=Depends(get_database),
):
    svc = StatsService(db)
    cv = _pick_civ_version(version=version, civ_version=civ_version)

    try:
        return await svc.get_user_stats(civ_version=cv, game_type=game_type, discord_id=discord_id)
    except InvalidStatsRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StatsNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Legacy stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable")


@legacy_router.put("/get-user-stats-batch/", response_model=BatchStatsResponse)
async def put_get_users_stats_batch(
    version: Optional[str] = Form(None),
    civ_version: Optional[str] = Form(None),
    game_type: str = Form(...),
    discord_id_list: List[str] = Form(...),
    db=Depends(get_database),
):
    svc = StatsService(db)
    cv = _pick_civ_version(version=version, civ_version=civ_version)

    if len(discord_id_list) > 200:
        raise HTTPException(status_code=400, detail="Too many discord ids")

    try:
        results = await svc.get_users_stats_batch(civ_version=cv, game_type=game_type, discord_ids=discord_id_list)
        normalized_civ_version = (results[0].civ_version if results else cv.strip().lower())
        normalized_game_type = (results[0].game_type if results else game_type.strip().lower())
        return BatchStatsResponse(civ_version=normalized_civ_version, game_type=normalized_game_type, results=results)
    except InvalidStatsRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Legacy batch stats lookup failed")
        raise HTTPException(status_code=503, detail="Backend unavailable")
