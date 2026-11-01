import asyncio
import json

from fastapi.exceptions import RequestValidationError

from app.core.errors import (
    AppDependencyError,
    app_dependency_exception_handler,
    request_validation_exception_handler,
)


def test_request_validation_handler_returns_enveloped_error():
    exc = RequestValidationError(
        [
            {
                "type": "extra_forbidden",
                "loc": ("body", "unexpected_field"),
                "msg": "Extra inputs are not permitted",
                "input": "value",
            }
        ]
    )

    response = asyncio.run(request_validation_exception_handler(None, exc))
    body = json.loads(response.body)

    assert response.status_code == 422
    assert body["detail"]["error"]["code"] == "INVALID_REQUEST"
    assert body["detail"]["error"]["retryable"] is False
    assert body["detail"]["error"]["correlation_id"] is None
    errors = body["detail"]["error"]["details"]["errors"]
    assert errors[0]["type"] == "extra_forbidden"
    assert errors[0]["loc"] == ["body", "unexpected_field"]


def test_app_dependency_handler_returns_enveloped_error():
    exc = AppDependencyError("Mongo client not initialized")

    response = asyncio.run(app_dependency_exception_handler(None, exc))
    body = json.loads(response.body)

    assert response.status_code == 503
    assert body["detail"]["error"]["code"] == "UNAVAILABLE"
    assert body["detail"]["error"]["retryable"] is True
    assert body["detail"]["error"]["correlation_id"] is None
