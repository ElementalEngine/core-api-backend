from __future__ import annotations

import asyncio

import pytest

from app.core.config import settings
from app.features.public import cache, router


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(settings, "public_cache_seconds", 600)
    cache._answers.clear()
    yield
    cache._answers.clear()


class _Reads:
    def __init__(self) -> None:
        self.count = 0

    async def __call__(self) -> int:
        self.count += 1
        return self.count


def _get(key: tuple[str, ...], reads: _Reads) -> int:
    return asyncio.run(cache.cached(key, reads))


def test_a_repeat_inside_the_window_reads_nothing():
    reads = _Reads()
    assert [_get(("ladders", "civ6"), reads) for _ in range(3)] == [1, 1, 1]
    assert reads.count == 1


def test_each_key_is_kept_apart():
    reads = _Reads()
    _get(("ladders", "civ6"), reads)
    _get(("ladders", "civ7"), reads)
    assert reads.count == 2


def test_an_answer_older_than_the_window_is_read_again(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: clock[0])
    reads = _Reads()
    _get(("overview",), reads)
    clock[0] += settings.public_cache_seconds
    assert _get(("overview",), reads) == 2


def test_a_failure_is_not_kept():
    async def fails() -> int:
        raise RuntimeError("not seeded")

    with pytest.raises(RuntimeError):
        asyncio.run(cache.cached(("stats", "civ7"), fails))
    reads = _Reads()
    assert _get(("stats", "civ7"), reads) == 1


def test_zero_seconds_keeps_nothing(monkeypatch):
    monkeypatch.setattr(settings, "public_cache_seconds", 0)
    reads = _Reads()
    _get(("overview",), reads)
    _get(("overview",), reads)
    assert reads.count == 2


def test_the_routes_read_through_the_cache(monkeypatch):
    calls: list[str] = []

    async def ladders(db, edition):
        calls.append(edition)
        return edition

    monkeypatch.setattr(router, "ladders", ladders)
    for _ in range(2):
        asyncio.run(router.get_ladders("civ6", db=None))
    asyncio.run(router.get_ladders("civ7", db=None))
    assert calls == ["civ6", "civ7"]
