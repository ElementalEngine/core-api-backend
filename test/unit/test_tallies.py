"""The tally rules the rebuild and the approve path both use.

They have to be one implementation. If the rebuild counts differently from
the write path, the first approve after a rebuild starts them drifting and
nothing notices.
"""

from __future__ import annotations

import pytest

from app.features.matches.tallies import bump, is_rated, read_entry, stat_legs


def test_realtime_writes_three_documents():
    assert stat_legs(is_cloud=False) == ((False, False), (True, False), (False, True))


def test_cloud_has_no_season_row():
    legs = stat_legs(is_cloud=True)
    assert legs == ((False, False), (False, True))
    assert not any(is_seasonal for is_seasonal, _ in legs)


@pytest.mark.parametrize(
    "stored,expected",
    [
        (None, (0, 0)),
        (3, (3, 0)),
        ({"games": 3, "wins": 1}, (3, 1)),
        ({"games": "3", "wins": "1"}, (3, 1)),
        ({}, (0, 0)),
        ("nonsense", (0, 0)),
    ],
)
def test_read_entry_handles_every_shape_on_disk(stored, expected):
    assert read_entry(stored) == expected


def test_bump_normalises_the_legacy_int_shape():
    assert bump({"CIVILIZATION_ROME": 4}, "CIVILIZATION_ROME", won=True, step=1) == {
        "CIVILIZATION_ROME": {"games": 5, "wins": 1}
    }


def test_bump_counts_a_loss():
    assert bump({}, "CIVILIZATION_ROME", won=False, step=1) == {
        "CIVILIZATION_ROME": {"games": 1, "wins": 0}
    }


def test_revert_clamps_at_zero():
    assert bump({}, "CIVILIZATION_ROME", won=True, step=-1) == {
        "CIVILIZATION_ROME": {"games": 0, "wins": 0}
    }


def test_approve_then_revert_is_a_round_trip():
    tally: dict = {}
    bump(tally, "CIVILIZATION_ROME", won=True, step=1)
    bump(tally, "CIVILIZATION_ROME", won=True, step=-1)
    assert tally == {"CIVILIZATION_ROME": {"games": 0, "wins": 0}}


@pytest.mark.parametrize("discord_id", [None, "", "-1", "-2", "-76561198000000000"])
def test_placeholder_ids_are_not_rated(discord_id):
    assert is_rated(discord_id) is False


def test_a_real_id_is_rated():
    assert is_rated("267035763240075264") is True
