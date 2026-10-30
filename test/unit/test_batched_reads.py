from __future__ import annotations

import asyncio

from app.core.config import settings
from app.features.matches.models import MatchModel
from app.features.matches.repository import MatchRepository
from app.features.matches.service import MatchService


def _match(ids, steam_ids=None):
    steam_ids = steam_ids or [None] * len(ids)
    return MatchModel(
        game="civ6",
        turn=50,
        map_type="Pangaea",
        game_mode="ffa",
        is_cloud=False,
        players=[
            {
                "steam_id": steam,
                "civ": "CIVILIZATION_ROME",
                "team": n,
                "placement": n,
                "discord_id": discord_id,
            }
            for n, (discord_id, steam) in enumerate(zip(ids, steam_ids))
        ],
        parser_version="1",
        discord_messages_id_list=[],
        save_file_hash="h",
        reporter_discord_id="1",
        contest_report_list=[],
    )


class _Ratings:
    def __init__(self, docs):
        self.docs = docs
        self.batches = []

    async def get_player_stat_docs_batch(self, *, discord_ids, session=None, **_):
        self.batches.append((list(discord_ids), session))
        return {d: dict(self.docs[d]) for d in discord_ids if d in self.docs}


def _service(ratings=None, users=None):
    svc = MatchService.__new__(MatchService)
    svc.ratings = ratings
    svc.q = users
    return svc


IDS = ["111", "-765", None, "222", "111", "-1", "333"]
DOCS = {
    "111": {"_id": 111, "mu": 31.5, "sigma": 4.0, "games": 9, "wins": 3, "first": 2},
    "333": {"_id": 333, "mu": 20.0, "sigma": 6.0, "games": 2, "subbedIn": 1},
}


def test_one_query_rates_every_seat_as_reading_each_alone_would():
    ratings = _Ratings(DOCS)
    svc = _service(ratings)
    match = _match(IDS)
    batched = asyncio.run(svc.get_players_ranking(match, is_seasonal=True))
    # Each seat as its own stored row, or the starting rating with none.
    start = settings.ts_mu
    assert [s.mu for s in batched] == [31.5, start, start, start, 31.5, start, 20.0]
    assert [s.games for s in batched] == [9, 0, 0, 0, 9, 0, 2]
    assert [s.index for s in batched] == list(range(len(IDS)))
    # One read, for the real players only, each named once.
    assert ratings.batches == [(["111", "222", "333"], None)]


def test_a_match_with_no_players_on_record_reads_nothing():
    ratings = _Ratings({})
    out = asyncio.run(_service(ratings).get_players_ranking(_match(["-9", None])))
    assert [s.mu for s in out] == [settings.ts_mu] * 2
    assert ratings.batches == []


def test_the_three_scopes_are_read_once_each():
    ratings = _Ratings(DOCS)
    asyncio.run(_service(ratings)._recompute_deltas(_match(["111", "333"])))
    assert len(ratings.batches) == 3


class _Users:
    def __init__(self, users):
        self.users = users
        self.asked = []

    async def get_users_by_steam_ids(self, steam_ids):
        self.asked.append(list(steam_ids))
        return {s: {"discord_id": self.users[s]} for s in steam_ids if s in self.users}


def test_seats_are_named_from_their_steam_ids_in_one_query():
    users = _Users({"7651": 5001, "7652": 5002})
    match = _match(
        [None, None, None, None, "999"],
        ["7651", "7659", None, "-1", "7652"],
    )
    named = asyncio.run(_service(users=users).match_id_to_discord(match))
    assert [p.discord_id for p in named.players] == [
        "5001",
        "-7659",
        None,
        None,
        "5002",
    ]
    assert users.asked == [["7651", "7659", "7652"]]


class _Cursor:
    def __init__(self, docs):
        self.docs = docs

    def __aiter__(self):
        async def gen():
            for d in self.docs:
                yield d

        return gen()


class _Col:
    def __init__(self, docs):
        self.docs = docs
        self.filter = None

    def find(self, flt):
        self.filter = flt
        return _Cursor(self.docs)


def test_a_steam_id_matches_a_steam_id_or_a_linked_steam_account_only():
    repo = MatchRepository.__new__(MatchRepository)
    repo._users = _Col(
        [
            {"discord_id": "1", "steam_id": "A"},
            {"discord_id": "2", "linked_platform": "steam", "linked_account_id": "B"},
            {"discord_id": "3", "linked_platform": "epic", "linked_account_id": "C"},
            {"discord_id": "4", "steam_id": "A"},
        ]
    )
    found = asyncio.run(repo.get_users_by_steam_ids(["A", "B", "C"]))
    assert {k: v["discord_id"] for k, v in found.items()} == {"A": "1", "B": "2"}
