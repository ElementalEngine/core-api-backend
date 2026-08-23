from fastapi import APIRouter

from app.api.health import router as health_router
from app.features.auth.router import public_router as auth_public_router
from app.features.auth.router import router as auth_router
from app.features.civdata.router import router as civ_data_router
from app.features.matches.router import router as matches_router
from app.features.matches.router_v2 import router as matches_v2_router
from app.features.stats.router import legacy_router as legacy_stats_router
from app.features.infractions.router import router as infractions_router
from app.features.stats.router import router as stats_router

router = APIRouter()
router.include_router(health_router)
router.include_router(auth_public_router)
router.include_router(auth_router)
router.include_router(matches_router)
router.include_router(matches_v2_router)
router.include_router(stats_router)
router.include_router(legacy_stats_router)
router.include_router(infractions_router)
router.include_router(civ_data_router)
