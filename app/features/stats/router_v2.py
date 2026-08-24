"""C2's four stats routes on /api/v2.

Same shape as C1: one router, no assembly file, the Mito gate on the router
rather than per route. The handlers catch only what they can name -- there
is no `except Exception -> 503` here, because a bug is not a transient
outage and D92's catch-all in core/errors answers it as INTERNAL/500.

The v1 routes stay until cutover: Mite still calls them (C2, "Replace").
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.dependencies import get_database, require_mito_token
from app.core.errors import invalid_request, not_found
from app.features.stats.errors import InvalidStatsRequestError, StatsNotFoundError
from app.features.stats.schemas import (
    BatchStatsRequest,
    TeamGenRequest,
    TeamGenResponse,
)
from app.features.stats.schemas_v2 import (
    BatchStatsResponseV2,
    UserStatsResponseV2,
)
from app.features.stats.service import StatsService

MAX_BATCH_IDS = 200

router = APIRouter(
    prefix="/api/v2",
    tags=["stats-v2"],
    dependencies=[Depends(require_mito_token)],
)


@router.get("/stats/user", response_model=UserStatsResponseV2)
async def get_user_stats(
    civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)
) -> UserStatsResponseV2:
    try:
        result = await StatsService(db).get_user_stats(
            civ_version=civ_version, game_type=game_type, discord_id=discord_id
        )
    except InvalidStatsRequestError as exc:
        raise invalid_request(str(exc)) from exc
    except StatsNotFoundError as exc:
        raise not_found(str(exc)) from exc
    return UserStatsResponseV2.from_v1(result)


@router.post("/stats/batch", response_model=BatchStatsResponseV2)
async def get_users_stats_batch(
    payload: BatchStatsRequest, db=Depends(get_database)
) -> BatchStatsResponseV2:
    ids = payload.discord_ids or []
    if len(ids) > MAX_BATCH_IDS:
        raise invalid_request(f"At most {MAX_BATCH_IDS} discord ids per request")

    try:
        results = await StatsService(db).get_users_stats_batch(
            civ_version=payload.civ_version,
            game_type=payload.game_type,
            discord_ids=ids,
        )
    except InvalidStatsRequestError as exc:
        raise invalid_request(str(exc)) from exc

    return BatchStatsResponseV2(
        civ_version=(
            results[0].civ_version if results else payload.civ_version.strip().lower()
        ),
        game_type=(
            results[0].game_type if results else payload.game_type.strip().lower()
        ),
        results=[UserStatsResponseV2.from_v1(r) for r in results],
    )


@router.post("/stats/team-gen", response_model=TeamGenResponse)
async def get_team_gen(
    payload: TeamGenRequest, db=Depends(get_database)
) -> TeamGenResponse:
    ids = payload.discord_ids or []
    if len(ids) > MAX_BATCH_IDS:
        raise invalid_request(f"At most {MAX_BATCH_IDS} discord ids per request")

    try:
        return await StatsService(db).get_team_gen(
            civ_version=payload.civ_version,
            game_type=payload.game_type,
            discord_ids=ids,
        )
    except InvalidStatsRequestError as exc:
        raise invalid_request(str(exc)) from exc


@router.put("/stats/reset/user", response_model=UserStatsResponseV2)
async def reset_user_stats(
    civ_version: str, game_type: str, discord_id: str, db=Depends(get_database)
) -> UserStatsResponseV2:
    try:
        result = await StatsService(db).reset_user_stats(
            civ_version=civ_version, game_type=game_type, discord_id=discord_id
        )
    except InvalidStatsRequestError as exc:
        raise invalid_request(str(exc)) from exc
    except StatsNotFoundError as exc:
        raise not_found(str(exc)) from exc
    return UserStatsResponseV2.from_v1(result)


__all__ = ["router"]
