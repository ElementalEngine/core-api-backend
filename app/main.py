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

configure_logging()

app = FastAPI(title="core-api", lifespan=db_lifespan)
# Starlette types the handler for bare Exception; registering a narrower one is
# the documented FastAPI pattern.
app.add_exception_handler(RequestValidationError, request_validation_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(AppDependencyError, app_dependency_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, unhandled_exception_handler)  # type: ignore[arg-type]
app.include_router(router)
# Added last, so it is outermost: the id exists before anything else runs.
app.add_middleware(CorrelationIdMiddleware)
