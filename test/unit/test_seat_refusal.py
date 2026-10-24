from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from app.features.lobbies.repository import (
    LobbyInsertRefused,
    LobbyRepository,
    _refusing_index,
)


def test_an_unfamiliar_index_is_not_guessed_at():
    exc = DuplicateKeyError("E11000", 11000, {"keyPattern": {"something_else": 1}})
    assert _refusing_index(exc) == "unknown"


def test_replace_seats_turns_a_duplicate_key_into_a_refusal():
    """The repository names the refusal rather than letting pymongo's escape."""

    class RefusingCollection:
        async def find_one_and_update(self, *_args, **_kwargs):
            raise DuplicateKeyError(
                "E11000 duplicate key error",
                11000,
                {"keyPattern": {"seats.discord_id": 1}},
            )

    repo = LobbyRepository.__new__(LobbyRepository)
    repo._lobbies = RefusingCollection()

    with pytest.raises(LobbyInsertRefused) as caught:
        asyncio.run(repo.replace_seats(ObjectId(), 1, [], datetime.now(UTC)))
    assert caught.value.index == "one_active_seat_per_player"
