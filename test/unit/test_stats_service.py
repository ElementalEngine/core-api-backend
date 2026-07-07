"""Unit tests for the stats service fixes (batch 6.2).

Covers:
- reset_user_stats 404s BEFORE resetting when the user has no stats (no stat_reset
  marker side effect), and resets exactly once when stats exist;
- request validation rejections;
- team generation returns a clean two-team partition of the input ids.
"""

from __future__ import annotations

import asyncio

import pytest

from app.features.stats.errors import InvalidStatsRequestError, StatsNotFoundError
from app.features.stats.service import StatsService


class FakeStatsRepo:
    def __init__(self, docs_by_discord_id=None):
        # Same doc map returned for every (match_type, seasonal) combination.
        self.docs = docs_by_discord_id or {}
        self.reset_calls = []

    async def get_player_stat_docs_batch(self, *, discord_ids, **kwargs):
        return {did: dict(doc) for did, doc in self.docs.items() if did in discord_ids}

    async def reset_player_stat_doc(self, **kwargs):
        self.reset_calls.append(kwargs)


def make_service(repo) -> StatsService:
    svc = StatsService.__new__(StatsService)
    svc.repository = repo
    return svc


STAT_DOC = {"mu": 30.0, "sigma": 5.0, "games": 3, "wins": 1, "first": 1}


# --- reset ordering ---


def test_reset_without_stats_404s_and_never_touches_the_db():
    repo = FakeStatsRepo(docs_by_discord_id={})
    svc = make_service(repo)

    with pytest.raises(StatsNotFoundError):
        asyncio.run(svc.reset_user_stats(civ_version="civ6", game_type="realtime", discord_id="123"))

    assert repo.reset_calls == []  # pre-fix: a stat_reset marker was inserted anyway


def test_reset_with_stats_returns_pre_reset_stats_and_resets_once():
    repo = FakeStatsRepo(docs_by_discord_id={"123": STAT_DOC})
    svc = make_service(repo)

    response = asyncio.run(
        svc.reset_user_stats(civ_version="civ6", game_type="realtime", discord_id="123")
    )

    assert len(repo.reset_calls) == 1
    assert repo.reset_calls[0]["discord_id"] == "123"
    assert response.lifetime.ffa is not None
    assert response.lifetime.ffa.games == 3


# --- validation ---


def test_reset_rejects_bad_inputs():
    svc = make_service(FakeStatsRepo())
    with pytest.raises(InvalidStatsRequestError):
        asyncio.run(svc.reset_user_stats(civ_version="civ9", game_type="realtime", discord_id="123"))
    with pytest.raises(InvalidStatsRequestError):
        asyncio.run(svc.reset_user_stats(civ_version="civ6", game_type="lan-party", discord_id="123"))
    with pytest.raises(InvalidStatsRequestError):
        asyncio.run(svc.reset_user_stats(civ_version="civ6", game_type="realtime", discord_id="abc"))


# --- team generation ---


def test_team_gen_partitions_all_players_into_two_teams():
    ids = ["101", "102", "103", "104"]
    repo = FakeStatsRepo(docs_by_discord_id={did: STAT_DOC for did in ids})
    svc = make_service(repo)

    response = asyncio.run(
        svc.get_team_gen(civ_version="civ6", game_type="realtime", discord_ids=list(ids))
    )

    assert len(response.teams) == 2
    combined = response.teams[0] + response.teams[1]
    assert sorted(combined) == sorted(ids)
    assert abs(len(response.teams[0]) - len(response.teams[1])) <= 1
    assert response.game_quality > 0.0


def test_team_gen_empty_ids_returns_empty_teams():
    svc = make_service(FakeStatsRepo())
    response = asyncio.run(
        svc.get_team_gen(civ_version="civ6", game_type="realtime", discord_ids=["", "  "])
    )
    assert response.teams == [[], []]
    assert response.game_quality == 0.0
