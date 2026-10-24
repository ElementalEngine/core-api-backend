"""The v2 season routes, behind the Mito token: the open season, and the
change to the next one. Mite lets only developers ask for the change."""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Depends
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

from app.core.config import settings
from app.core.dependencies import actor_discord_id, get_database, require_mito_token
from app.core.errors import api_error, conflict, invalid_request, not_found
from app.features.seasons.repository import SeasonNotSeededError
from app.features.seasons.rollover import (
    SeasonBackupUnsetError,
    SeasonMovedError,
    SeasonRollover,
    SeasonRolloverError,
)
from app.features.seasons.schemas import SeasonPreview, SeasonRolled, StartNextSeason

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v2/seasons",
    tags=["seasons-v2"],
    dependencies=[Depends(require_mito_token)],
)

Edition = Literal["civ6", "civ7"]


@router.get("/{edition}", response_model=SeasonPreview)
async def get_season(
    edition: Edition, db: AsyncMongoClient = Depends(get_database)
) -> SeasonPreview:
    try:
        return await SeasonRollover(db).preview(edition)
    except SeasonNotSeededError as exc:
        raise not_found(str(exc)) from exc
    except SeasonRolloverError as exc:
        raise invalid_request(str(exc)) from exc


@router.post("/{edition}/next", response_model=SeasonRolled)
async def start_next_season(
    edition: Edition,
    body: StartNextSeason,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> SeasonRolled:
    try:
        rolled = await SeasonRollover(db, settings.season_backup_dir).roll(
            edition, expected=body.expected
        )
    except SeasonNotSeededError as exc:
        raise not_found(str(exc)) from exc
    except SeasonRolloverError as exc:
        raise invalid_request(str(exc)) from exc
    except SeasonMovedError as exc:
        raise conflict(str(exc)) from exc
    except SeasonBackupUnsetError as exc:
        raise api_error(
            code="SEASON_BACKUP_MISCONFIGURED", message=str(exc), status_code=503
        ) from exc
    except PyMongoError as exc:
        if exc.has_error_label("TransientTransactionError"):
            raise api_error(
                code="CONFLICT",
                message="The season change clashed with another change. Try again.",
                status_code=409,
                retryable=True,
            ) from exc
        raise
    logger.info(
        "Season change %s: %s -> %s by %s, backup %s",
        edition,
        rolled.closed,
        rolled.opened,
        actor,
        rolled.backup,
    )
    return rolled


__all__ = ["router"]
