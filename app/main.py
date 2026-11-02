from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from app.api.router import router
from app.core.db import db_lifespan
from app.core.errors import (
    AppDependencyError,
    app_dependency_exception_handler,
    request_validation_exception_handler,
    unhandled_exception_handler,
)
from app.core.logging import configure_logging
from app.core.middleware import CorrelationIdMiddleware
from app.features.auth.errors import AuthError, auth_error_handler
from app.features.infractions.errors import InfractionError, infraction_error_handler

configure_logging()

app = FastAPI(title="core-api", version="2.0.0", lifespan=db_lifespan)
# Starlette types the handler for bare Exception; registering a narrower one is
# the documented FastAPI pattern.
app.add_exception_handler(RequestValidationError, request_validation_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(AppDependencyError, app_dependency_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(AuthError, auth_error_handler)  # type: ignore[arg-type]
app.add_exception_handler(InfractionError, infraction_error_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, unhandled_exception_handler)  # type: ignore[arg-type]
app.include_router(router)
# Added last, so it is outermost: the id exists before anything else runs.
app.add_middleware(CorrelationIdMiddleware)
