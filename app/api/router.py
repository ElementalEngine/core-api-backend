from fastapi import APIRouter

from app.api.health import router as health_router
from app.features.auth.router import (
    public_router as auth_public_router,
    router as auth_router,
)
from app.features.civdata.router import router as civ_data_router
from app.features.infractions.router import router as infractions_router
from app.features.match_setups.router import router as match_setups_router
from app.features.matches.router_v2 import router as matches_v2_router
from app.features.seasons.router import router as seasons_router
from app.features.stats.router_v2 import router as stats_v2_router

router = APIRouter()
router.include_router(health_router)
router.include_router(auth_public_router)
router.include_router(auth_router)
router.include_router(matches_v2_router)
router.include_router(stats_v2_router)
router.include_router(infractions_router)
router.include_router(civ_data_router)
router.include_router(match_setups_router)
router.include_router(seasons_router)
