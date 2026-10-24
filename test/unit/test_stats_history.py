"""A player's history and summary read their own seat, count a win as a game
whose rating went up and a first as first place, and start the summary at
their last reset.

What should break this file: a first place counted as the only kind of win,
a win counted where the rating fell, a sub's leaver
seat counted, a civ6 leader read from `civ`, a summary that ignores a reset or
cuts it on report time, or a page that claims more games when there are none.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from bson import ObjectId

from app.features.stats.errors import InvalidStatsRequestError
from app.features.stats.history import PAGE_SIZE, MatchHistory

ME = "100000000000000001"


def _seat(
    did,
    *,
    placement,
    team=None,
    civ="CIVILIZATION_ROME",
    leader="LEADER_TRAJAN",
    **extra,
):
    return {
        "discord_id": did,
        "placement": placement,
        "team": placement if team is None else team,
        "civ": civ,
        "leader": leader,
        "delta": 12.4,
        **extra,
    }


def _match(*seats, map_type="Pangaea", game="civ6", mode="ffa", when=None):
    return {
        "_id": ObjectId.from_datetime(when or datetime(2026, 10, 1, tzinfo=UTC)),
        "game": game,
        "game_mode": mode,
        "map_type": map_type,
        "turn": 120,
        "players": list(seats),
    }


class _Cursor:
    def __init__(self, docs):
        self.docs = docs
        self.calls = []

    def sort(self, *a):
        self.calls.append(("sort", a))
        return self

    def skip(self, n):
        self.calls.append(("skip", n))
        self.docs = self.docs[n:]
        return self

    def limit(self, n):
        self.calls.append(("limit", n))
        self.docs = self.docs[:n]
        return self

    def __aiter__(self):
        async def gen():
            for d in self.docs:
                yield d

        return gen()


class _Col:
    def __init__(self, docs=(), one=None):
        self.docs = list(docs)
        self.one = one
        self.filters = []

    def find(self, flt, projection=None):
        self.filters.append(flt)
        self.cursor = _Cursor(self.docs)
        return self.cursor

    async def find_one(self, flt, sort=None):
        return self.one


def _history(matches=(), reset=None):
    h = MatchHistory.__new__(MatchHistory)
    h._validated = _Col(matches)
    h._resets = _Col(one=reset)
    return h


def test_a_win_is_a_rating_gain_a_first_is_first_place_and_leavers_do_not_count():
    h = _history(
        [
            _match(_seat(ME, placement=0, delta=20), _seat("2", placement=1)),
            # Third of eight, rating up: a win in FFA, not a first.
            _match(_seat(ME, placement=2, delta=4), map_type="Seven Seas"),
            _match(_seat(ME, placement=5, delta=-11), map_type="Seven Seas"),
            _match(
                _seat(ME, placement=0, delta=9, subbed_out=True),
                _seat("3", placement=0, is_sub=True),
            ),
        ]
    )
    out = asyncio.run(
        h.summary(civ_version="civ6", game_type="realtime", discord_id=ME)
    )
    assert [(m.map_type, m.games, m.wins, m.firsts) for m in out.maps] == [
        ("Seven Seas", 2, 1, 0),
        ("Pangaea", 1, 1, 1),
    ]
    assert [(c.token, c.games, c.wins, c.firsts) for c in out.civs] == [
        ("LEADER_TRAJAN", 3, 2, 1)
    ]


def test_civ6_counts_the_leader_and_civ7_the_civ_and_blanks_are_skipped():
    h = _history(
        [
            _match(
                _seat(ME, placement=0, civ="CIVILIZATION_ROME", leader="LEADER_TRAJAN")
            ),
            _match(_seat(ME, placement=1, civ="Unknown", leader=None)),
        ]
    )
    out = asyncio.run(
        h.summary(civ_version="civ6", game_type="realtime", discord_id=ME)
    )
    assert [c.token for c in out.civs] == ["LEADER_TRAJAN"]
    h7 = _history(
        [
            _match(
                _seat(
                    ME, placement=0, civ="CIVILIZATION_ROME", leader="LEADER_AUGUSTUS"
                ),
                game="civ7",
            )
        ]
    )
    out7 = asyncio.run(
        h7.summary(civ_version="civ7", game_type="realtime", discord_id=ME)
    )
    assert [c.token for c in out7.civs] == ["CIVILIZATION_ROME"]


def test_the_summary_starts_at_the_last_reset_and_says_so():
    at = datetime(2026, 9, 1, tzinfo=UTC)
    h = _history([], reset={"occurred_at": at})
    out = asyncio.run(h.summary(civ_version="civ6", game_type="cloud", discord_id=ME))
    assert out.since == at
    flt = h._validated.filters[0]
    assert flt["approved_at"] == {"$gte": at}
    assert flt["is_cloud"] is True
    assert flt["players.discord_id"] == ME


def test_a_realtime_read_takes_documents_with_no_cloud_flag():
    h = _history([])
    asyncio.run(h.summary(civ_version="civ6", game_type="realtime", discord_id=ME))
    assert h._validated.filters[0]["is_cloud"] == {"$ne": True}
    assert "approved_at" not in h._validated.filters[0]


def test_only_five_civs_most_played_first():
    leaders = ["A", "B", "B", "C", "C", "C", "D", "E", "F", "F"]
    h = _history(
        [_match(_seat(ME, placement=1, leader=f"LEADER_{x}")) for x in leaders]
    )
    out = asyncio.run(
        h.summary(civ_version="civ6", game_type="realtime", discord_id=ME)
    )
    assert [c.token for c in out.civs] == [
        "LEADER_C",
        "LEADER_B",
        "LEADER_F",
        "LEADER_A",
        "LEADER_D",
    ]


def test_a_page_is_newest_first_and_knows_whether_more_follow():
    games = [
        _match(_seat(ME, placement=0), _seat("2", placement=1))
        for _ in range(PAGE_SIZE + 1)
    ]
    h = _history(games)
    first = asyncio.run(
        h.page(civ_version="civ6", game_type="realtime", discord_id=ME, page=0)
    )
    assert len(first.games) == PAGE_SIZE and first.has_more
    assert ("limit", PAGE_SIZE + 1) in h._validated.cursor.calls
    last = asyncio.run(
        _history(games[:3]).page(
            civ_version="civ6", game_type="realtime", discord_id=ME, page=0
        )
    )
    assert len(last.games) == 3 and not last.has_more


def test_a_game_row_names_the_players_side_and_result():
    h = _history(
        [
            _match(
                _seat(ME, placement=1, team=1),
                _seat("2", placement=0, team=0),
                _seat("3", placement=1, team=1),
                mode="teamer",
            )
        ]
    )
    game = asyncio.run(
        h.page(civ_version="civ6", game_type="realtime", discord_id=ME, page=0)
    ).games[0]
    assert (game.game_mode, game.placement, game.sides, game.delta) == (
        "teamer",
        1,
        2,
        12,
    )
    assert game.leader == "LEADER_TRAJAN"
    assert game.reported_at == datetime(2026, 10, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"civ_version": "civ5", "game_type": "realtime", "discord_id": ME},
        {"civ_version": "civ6", "game_type": "pbc", "discord_id": ME},
        {"civ_version": "civ6", "game_type": "cloud", "discord_id": "abc"},
    ],
)
def test_bad_input_is_refused(kwargs):
    with pytest.raises(InvalidStatsRequestError):
        asyncio.run(_history().summary(**kwargs))


def test_a_page_past_the_cap_is_refused():
    with pytest.raises(InvalidStatsRequestError):
        asyncio.run(
            _history().page(
                civ_version="civ6", game_type="realtime", discord_id=ME, page=50
            )
        )


@pytest.mark.parametrize(
    ("raw", "name"),
    [
        ("Pangaea", "Pangaea"),
        ("../../Assets/Maps/Pangaea_Ultima", "Pangaea_Ultima"),
        ("..\\Assets\\Maps\\Lakes.lua", "Lakes"),
        ("", "Unknown"),
        (None, "Unknown"),
    ],
)
def test_a_map_saved_as_a_script_path_counts_as_its_file_name(raw, name):
    from app.features.stats.history import map_name

    assert map_name(raw) == name


def test_the_same_map_by_path_and_by_name_is_one_row():
    h = _history(
        [
            _match(_seat(ME, placement=0), map_type="../../Assets/Maps/Lakes"),
            _match(_seat(ME, placement=1), map_type="Lakes"),
        ]
    )
    out = asyncio.run(
        h.summary(civ_version="civ6", game_type="realtime", discord_id=ME)
    )
    assert [(m.map_type, m.games, m.firsts) for m in out.maps] == [("Lakes", 2, 1)]
