"""A season change saves the season's stats to files, clears them with a
reset event for each player, and opens the next season, or changes nothing."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest
from bson.int64 import Int64

from app.core.config import settings
from app.features.seasons import repository
from app.features.seasons.rollover import (
    SeasonBackupUnsetError,
    SeasonMovedError,
    SeasonRollover,
    SeasonRolloverError,
    next_label,
)

STARTED = datetime(2026, 8, 25, tzinfo=UTC)
NOW = datetime(2026, 10, 24, 12, 0, 0, tzinfo=UTC)


class Cursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration from None


class Collection:
    def __init__(self, docs=(), fail_delete=False):
        self.docs = [dict(d) for d in docs]
        self.fail_delete = fail_delete

    async def find_one(self, flt, sort=None, session=None):
        rows = [d for d in self.docs if all(d.get(k) == v for k, v in flt.items())]
        rows.sort(key=lambda d: d["started_at"], reverse=True)
        return rows[0] if rows else None

    def find(self, flt, session=None):
        return Cursor(self.docs)

    async def count_documents(self, flt):
        return len(self.docs)

    async def delete_many(self, flt, session=None):
        if self.fail_delete:
            raise RuntimeError("write failed")
        self.staged_delete = True

    async def insert_one(self, doc, session=None):
        self.staged = [*getattr(self, "staged", []), doc]

    async def insert_many(self, docs, session=None):
        self.staged = [*getattr(self, "staged", []), *docs]

    async def update_many(self, flt, update, session=None):
        self.touched = [
            d for d in self.docs if all(d.get(k) == v for k, v in flt.items())
        ]


class Transaction:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class Session:
    def __init__(self, client):
        self._client = client
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def start_transaction(self):
        return Transaction(self)

    async def commit_transaction(self):
        self.committed = True
        self._client.commit()


class Database(dict):
    async def list_collection_names(self):
        return list(self.keys())


class Client:
    """Writes are staged and land only on commit, as in a transaction."""

    def __init__(self, season_stats, fail_delete=False):
        self.games = Database(
            seasons=Collection(
                [{"edition": "civ6", "label": "Season 6", "started_at": STARTED}]
            ),
            rating_events=Collection(),
            pending_matches=Collection([{"game": "civ6"}, {"game": "civ7"}]),
        )
        self.stats = Database(
            {
                name: Collection(docs, fail_delete=fail_delete)
                for name, docs in season_stats.items()
            }
        )

    def __getitem__(self, name):
        return self.games if name == "games" else self.stats

    def start_session(self):
        return Session(self)

    def commit(self):
        for col in [*self.games.values(), *self.stats.values()]:
            if getattr(col, "staged_delete", False):
                col.docs = []
            col.docs += getattr(col, "staged", [])
            col.staged, col.staged_delete = [], False


def _stats():
    return {
        "rt_ffa": [
            {"_id": Int64(11), "mu": 1310.0, "sigma": 90.0, "games": 4},
            {"_id": Int64(12), "mu": 1150.0, "sigma": 100.0, "games": 2},
        ],
        "pbc_ffa": [{"_id": Int64(13), "mu": 1250.0, "sigma": 80.0, "games": 1}],
    }


def test_the_next_season_takes_the_next_number():
    assert next_label("Season 6") == "Season 7"
    with pytest.raises(SeasonRolloverError):
        next_label("Winter 2026")


def test_the_preview_counts_what_a_change_would_clear():
    out = asyncio.run(SeasonRollover(Client(_stats())).preview("civ6"))
    assert (out.label, out.next_label) == ("Season 6", "Season 7")
    assert out.records == {"pbc_ffa": 1, "rt_ffa": 2}


def test_a_change_saves_clears_and_opens_the_next_season(tmp_path):
    client = Client(_stats())
    repository._CACHE["civ6"] = {"label": "Season 6"}

    out = asyncio.run(
        SeasonRollover(client, str(tmp_path)).roll("civ6", expected="Season 6", now=NOW)
    )

    assert (out.closed, out.opened, out.records) == (
        "Season 6",
        "Season 7",
        {"pbc_ffa": 1, "rt_ffa": 2},
    )
    folder = tmp_path / out.backup
    assert out.backup.startswith("civ6-season-6-20261024T120000Z-")
    saved = (folder / "civ6_season_stats.rt_ffa.json").read_text().splitlines()
    assert len(saved) == 2 and '"$numberLong": "11"' in saved[0]
    assert json.loads((folder / "manifest.json").read_text())["opened"] == "Season 7"
    assert not list(tmp_path.glob("*.partial"))

    assert client.stats["rt_ffa"].docs == [] and client.stats["pbc_ffa"].docs == []
    events = client.games["rating_events"].docs
    assert {(e["scope"], int(e["player_id"])) for e in events} == {
        ("civ6_season_stats.rt_ffa", 11),
        ("civ6_season_stats.rt_ffa", 12),
        ("civ6_season_stats.pbc_ffa", 13),
    }
    assert all(e["mu_after"] == settings.ts_mu for e in events)
    assert client.games["seasons"].docs[-1] == {
        "edition": "civ6",
        "label": "Season 7",
        "started_at": NOW,
    }
    # Only this edition's pending reports are touched, so an overlapping
    # approve of one clashes with the change.
    assert client.games["pending_matches"].touched == [{"game": "civ6"}]
    assert repository._CACHE == {}


def test_a_second_press_changes_nothing_twice(tmp_path):
    client = Client(_stats())
    with pytest.raises(SeasonMovedError, match="Season 5 is no longer open"):
        asyncio.run(
            SeasonRollover(client, str(tmp_path)).roll("civ6", expected="Season 5")
        )
    assert len(client.stats["rt_ffa"].docs) == 2
    assert list(tmp_path.iterdir()) == []


def test_a_failed_change_clears_nothing_and_keeps_what_it_wrote(tmp_path):
    client = Client(_stats(), fail_delete=True)
    repository._CACHE["civ6"] = {"label": "Season 6"}
    with pytest.raises(RuntimeError):
        asyncio.run(
            SeasonRollover(client, str(tmp_path)).roll("civ6", expected="Season 6")
        )
    assert len(client.stats["rt_ffa"].docs) == 2
    assert len(client.games["seasons"].docs) == 1
    # A backup is never deleted: the outcome of a failed commit can be unknown.
    assert [p.suffix for p in tmp_path.iterdir()] == [".partial"]
    assert repository._CACHE == {}


def test_no_backup_folder_means_no_change():
    client = Client(_stats())
    with pytest.raises(SeasonBackupUnsetError):
        asyncio.run(SeasonRollover(client).roll("civ6", expected="Season 6"))
    assert len(client.stats["rt_ffa"].docs) == 2
