from __future__ import annotations

from app.api.health import router


def _paths() -> set[str]:
    return {route.path for route in router.routes}


def test_debug_dbstats_route_is_gone():
    assert "/_debug/db-stats" not in _paths()


def test_liveness_and_readiness_routes_remain():
    assert {"/", "/healthz", "/readyz"} <= _paths()
