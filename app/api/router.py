from fastapi import APIRouter

from app.api.health import router as health_router
from app.features.matches.router import router as matches_router
from app.features.stats.router import legacy_router as legacy_stats_router
from app.features.stats.router import router as stats_router

router = APIRouter()
router.include_router(health_router)
router.include_router(matches_router)
router.include_router(stats_router)
router.include_router(legacy_stats_router)
