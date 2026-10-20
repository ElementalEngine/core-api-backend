from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import router
from app.core.config import settings
from app.core.db import db_lifespan
from app.core.errors import (
    AppDependencyError,
    app_dependency_exception_handler,
    request_validation_exception_handler,
    unhandled_exception_handler,
)
from app.core.logging import configure_logging
from app.core.middleware import CorrelationIdMiddleware

configure_logging()

app = FastAPI(title="Civ Save Tool", lifespan=db_lifespan)
# Starlette types the handler for bare Exception; registering a narrower one is
# the documented FastAPI pattern.
app.add_exception_handler(RequestValidationError, request_validation_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(AppDependencyError, app_dependency_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, unhandled_exception_handler)  # type: ignore[arg-type]
app.include_router(router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.allowed_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)
# Added last, so it is outermost: the id exists before anything else runs.
app.add_middleware(CorrelationIdMiddleware)
