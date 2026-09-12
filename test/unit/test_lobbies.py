"""Lobby index declarations and the shapes they enforce.

This asserts what the repository declares, not what Mongo does with it.
Building the indexes, and the multikey behaviour the one-seat-per-player rule
rests on, belong to the deployment dry-run.

Governed by D60, D71.
"""

from __future__ import annotations

import asyncio

from app.core.constants import COL_LOBBIES, COL_LOBBY_STATS, GAMES_DB
from app.features.lobbies.repository import LobbyRepository


class RecordingCollection:
    def __init__(self):
        self.indexes = []

    async def create_index(
        self, keys, name=None, unique=False, partialFilterExpression=None
    ):
        self.indexes.append(
            {
                "keys": keys,
                "name": name,
                "unique": unique,
                "partial": partialFilterExpression,
            }
        )


def _declared():
    lobbies, stats = RecordingCollection(), RecordingCollection()
    client = {GAMES_DB: {COL_LOBBIES: lobbies, COL_LOBBY_STATS: stats}}
    asyncio.run(LobbyRepository(client).ensure_indexes())
    return (
        {i["name"]: i for i in lobbies.indexes},
        {i["name"]: i for i in stats.indexes},
    )


def test_lobbies_declares_exactly_the_one_partial_unique():
    lobbies, _ = _declared()
    assert set(lobbies) == {"one_active_seat_per_player"}
    assert all(i["unique"] is True for i in lobbies.values())


def test_the_seat_index_is_on_the_array_path_not_the_array():
    lobbies, _ = _declared()
    assert lobbies["one_active_seat_per_player"]["keys"] == [("seats.discord_id", 1)]


def test_every_filter_keys_off_closed_at_never_phase():
    # `phase` would need two values kept in step -- `complete` and
    # `cancelled` -- and drift the moment one is forgotten.
    lobbies, _ = _declared()
    for index in lobbies.values():
        assert index["partial"]["closed_at"] is None


def test_no_filter_uses_the_uncreatable_exists_false_form():
    lobbies, stats = _declared()
    for index in list(lobbies.values()) + list(stats.values()):
        for clause in (index["partial"] or {}).values():
            assert clause != {"$exists": False}


def test_the_seat_filter_narrows_open_to_seated():
    lobbies, _ = _declared()
    seat = lobbies["one_active_seat_per_player"]["partial"]
    assert seat["seats.discord_id"] == {"$exists": True}
    assert seat["closed_at"] is None


def test_aggregate_key_is_the_full_four_field_key_and_unpartialled():
    _, stats = _declared()
    assert set(stats) == {"aggregate_key"}
    assert stats["aggregate_key"]["keys"] == [
        ("season_id", 1),
        ("edition", 1),
        ("game_type", 1),
        ("token", 1),
    ]
    assert stats["aggregate_key"]["unique"] is True
    # Every aggregate row carries all four fields, so a partial filter here
    # would exclude rows from the uniqueness it exists to enforce.
    assert stats["aggregate_key"]["partial"] is None
