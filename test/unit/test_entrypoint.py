from __future__ import annotations

import app.__main__ as entrypoint
from app.core.config import Settings


def test_host_default_is_loopback():
    # Fail-closed: unset API_HOST must not expose the API off-host.
    assert Settings().api_host == "127.0.0.1"


def test_entrypoint_binds_from_settings(monkeypatch):
    captured = {}
    monkeypatch.setattr(entrypoint.settings, "api_host", "127.0.0.1")
    monkeypatch.setattr(entrypoint.settings, "api_port", 8001)
    monkeypatch.setattr(entrypoint.uvicorn, "run", lambda app, **kw: captured.update(app=app, **kw))

    entrypoint.main()

    assert captured == {"app": "app.main:app", "host": "127.0.0.1", "port": 8001}
