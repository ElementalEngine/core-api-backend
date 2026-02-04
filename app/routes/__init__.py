from fastapi import APIRouter

from app.routes.matches import router as matches_router
from app.routes.stats import legacy_router as legacy_stats_router
from app.routes.stats import router as stats_router
from app.routes.upload import router as upload_router

router = APIRouter()
router.include_router(upload_router)
router.include_router(matches_router)
router.include_router(stats_router)
router.include_router(legacy_stats_router)