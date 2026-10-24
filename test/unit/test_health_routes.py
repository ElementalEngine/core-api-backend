from __future__ import annotations

import asyncio

from app.api.health import readyz, router


def _paths() -> set[str]:
    return {route.path for route in router.routes}


def test_debug_dbstats_route_is_gone():
    assert "/_debug/db-stats" not in _paths()


class _Admin:
    async def command(self, name: str) -> dict[str, int]:
        await asyncio.sleep(0.01)
        return {"ok": 1}


class _Client:
    admin = _Admin()


def test_readyz_times_the_database_ping():
    body = asyncio.run(readyz(_Client()))  # type: ignore[arg-type]
    assert body == {"status": "ready", "db_ms": body["db_ms"]}
    assert body["db_ms"] >= 10
