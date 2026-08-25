"""Entry 7 Half A: the three index declarations (D166, D172).

What should break these: dropping `unique` from any of the three, using the
uncreatable `$exists: False` form, giving the seat index the same filter as
the channel index, indexing `seats` instead of the array path
`seats.discord_id`, giving `aggregate_key` a partial filter, losing a
component of the aggregate key, or binding the wrong collection.

⚠ These prove the code still ASKS for the indexes, never that Mongo builds
or enforces them -- D60 keeps the driver out of CI. Building, and the
multikey behaviour D71 rests on, is Entry 7's dev dry-run.
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


def test_lobbies_declares_exactly_the_two_partial_uniques():
    lobbies, _ = _declared()
    assert set(lobbies) == {
        "one_active_lobby_per_channel",
        "one_active_seat_per_player",
    }
    assert all(i["unique"] is True for i in lobbies.values())


def test_one_active_lobby_per_channel_is_keyed_on_guild_and_channel():
    lobbies, _ = _declared()
    assert lobbies["one_active_lobby_per_channel"]["keys"] == [
        ("guild_id", 1),
        ("channel_id", 1),
    ]


def test_the_seat_index_is_on_the_array_path_not_the_array():
    # `seats` would index whole subdocuments and enforce nothing about the
    # player; `seats.discord_id` is what makes the index multikey and is the
    # structural fix for O-18 (D71).
    lobbies, _ = _declared()
    assert lobbies["one_active_seat_per_player"]["keys"] == [("seats.discord_id", 1)]


def test_every_filter_keys_off_closed_at_never_phase():
    # `phase` would need two values kept in step -- `complete` and
    # `cancelled` -- and drift the moment one is forgotten.
    lobbies, _ = _declared()
    for index in lobbies.values():
        assert index["partial"]["closed_at"] is None


def test_no_filter_uses_the_uncreatable_exists_false_form():
    # MongoDB rewrites {$exists: False} as $not, which a partial index
    # refuses outright: CannotCreateIndex 67. Nothing in CI builds an index,
    # so this is the only place that failure can be caught early.
    lobbies, stats = _declared()
    for index in list(lobbies.values()) + list(stats.values()):
        for clause in (index["partial"] or {}).values():
            assert clause != {"$exists": False}


def test_the_two_lobby_filters_are_not_the_same():
    # A copy-paste giving the seat index the channel index's filter passes
    # every other test here, and silently restores the defect where only ONE
    # seat-less lobby could be open fleet-wide (Correction 73b).
    lobbies, _ = _declared()
    channel = lobbies["one_active_lobby_per_channel"]["partial"]
    seat = lobbies["one_active_seat_per_player"]["partial"]
    assert channel != seat
    assert seat["seats.discord_id"] == {"$exists": True}
    assert "seats.discord_id" not in channel


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
