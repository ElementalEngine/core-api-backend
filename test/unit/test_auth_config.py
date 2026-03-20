import importlib


def test_auth_config_loads_env(monkeypatch):
    monkeypatch.setenv("AUTH_SERVICE_TOKEN", "secret-token")
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_ID", "12345")
    monkeypatch.setenv("AUTH_DISCORD_CLIENT_SECRET", "super-secret")
    monkeypatch.setenv(
        "AUTH_DISCORD_REDIRECT_URI",
        "https://example.com/oauth/discord/callback",
    )
    monkeypatch.setenv("AUTH_SESSION_TTL_MINUTES", "30")
    monkeypatch.setenv("AUTH_OAUTH_TIMEOUT_SECONDS", "20")

    import app.core.config as cfg

    importlib.reload(cfg)

    s = cfg.settings
    assert s.auth_service_token.get_secret_value() == "secret-token"
    assert s.auth_discord_client_id == "12345"
    assert s.auth_discord_client_secret.get_secret_value() == "super-secret"
    assert s.auth_discord_redirect_uri == "https://example.com/oauth/discord/callback"
    assert s.auth_session_ttl_minutes == 30
    assert s.auth_oauth_timeout_seconds == 20