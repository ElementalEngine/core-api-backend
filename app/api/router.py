from fastapi import APIRouter

from app.api.health import router as health_router
from app.features.auth.router import (
    public_router as auth_public_router,
    router as auth_router,
)
from app.features.civdata.router import router as civ_data_router
from app.features.infractions.router import router as infractions_router
from app.features.lobbies.router import (
    activity_router as lobbies_activity_router,
    mite_router as lobbies_mite_router,
)
from app.features.matches.router import router as matches_router
from app.features.matches.router_v2 import router as matches_v2_router
from app.features.stats.router import router as stats_router
from app.features.stats.router_v2 import router as stats_v2_router

router = APIRouter()
router.include_router(health_router)
router.include_router(auth_public_router)
router.include_router(auth_router)
router.include_router(matches_router)
router.include_router(matches_v2_router)
router.include_router(stats_router)
router.include_router(stats_v2_router)
router.include_router(infractions_router)
router.include_router(civ_data_router)
# C5 section 6b: both lobby routers share `/api/v2/lobbies` and resolve as
# ONE ordered table, so whichever registers first claims an ambiguous path.
# Mite's two routes go first -- a Mite path matching an Activity-gated route
# would be checked against the WRONG TOKEN, which no handler test can catch.
router.include_router(lobbies_mite_router)
router.include_router(lobbies_activity_router)
