from app.features.auth.errors import AccountLookupNotFoundError, to_http_exception


def test_to_http_exception_serializes_auth_error_payload():
    response = to_http_exception(AccountLookupNotFoundError(field="discord_id", value="111111111111111111"))

    assert response.status_code == 404
    assert response.detail["error"]["code"] == "ACCOUNT_NOT_FOUND"
    assert response.detail["error"]["details"] == {"discord_id": "111111111111111111"}
