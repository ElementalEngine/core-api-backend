"""Dealing draft pools.

Two properties matter more than the arithmetic: pools must be disjoint, or
two players draft the same leader; and a deal must NOT be reproducible from
the lobby id, or a blind pool can be computed before it is dealt.
"""

from __future__ import annotations

import random

import pytest

from app.features.lobbies.pools import (
    NotEnoughPool,
    assign_one_each,
    civ_target,
    deal,
    deal_civs,
    even_split,
    remaining_after_bans,
)

TOKENS = [f"LEADER_{i:02d}" for i in range(44)]


def test_the_pool_is_truncated_so_every_player_gets_the_same_count():
    assert even_split(44, 5) == (8, 4)
    assert even_split(44, 4) == (11, 0)


def test_a_player_who_would_get_nothing_is_refused():
    # Failing here is far cheaper than failing at the pick.
    with pytest.raises(NotEnoughPool):
        even_split(3, 5)


@pytest.mark.parametrize("players", [0, -1])
def test_a_draft_needs_players(players):
    with pytest.raises(NotEnoughPool):
        even_split(44, players)


def test_pools_are_disjoint_and_equal():
    pools = deal(TOKENS, 5, random.Random(7))
    assert [len(pool) for pool in pools] == [8, 8, 8, 8, 8]
    flat = [token for pool in pools for token in pool]
    assert len(flat) == len(set(flat)), "a token reached two players"
    assert set(flat) <= set(TOKENS)
    assert len(TOKENS) - len(flat) == 4


def test_a_deal_is_not_reproducible_from_the_lobby():
    first = deal(TOKENS, 5)
    second = deal(TOKENS, 5)
    assert first != second


def test_random_mode_assigns_one_distinct_token_each():
    assigned = assign_one_each(TOKENS, 5, random.Random(7))
    assert len(assigned) == 5
    assert len(set(assigned)) == 5
    assert set(assigned) <= set(TOKENS)


def test_a_pool_too_small_to_assign_is_refused_rather_than_duplicated():
    with pytest.raises(NotEnoughPool):
        assign_one_each(["a", "b"], 5)


def test_bans_are_removed_and_the_rest_keeps_its_order():
    remaining = remaining_after_bans(TOKENS, ["LEADER_00", "LEADER_43", "LEADER_00"])
    assert len(remaining) == 42
    assert remaining == [t for t in TOKENS if t not in {"LEADER_00", "LEADER_43"}]


def test_banning_nothing_removes_nothing():
    assert remaining_after_bans(TOKENS, []) == TOKENS


CIVS = [f"CIVILIZATION_{i:02d}" for i in range(44)]


@pytest.mark.parametrize(
    ("game_type", "groups", "expected"),
    [
        ("ffa", 10, 4),
        ("duel", 2, 4),
        ("teamer", 2, 7),
        ("teamer", 3, 5),
        ("teamer", 4, 5),
        ("teamer", 5, 4),
    ],
)
def test_the_civ_target_is_a_target_not_a_division(game_type, groups, expected):
    # Dividing forty-four civs by twelve players would leave three each.
    assert civ_target(game_type, groups) == expected


def test_civ_pools_are_disjoint_while_the_set_allows_it():
    pools = deal_civs(CIVS, 10, 4, random.Random(7))
    flat = [token for pool in pools for token in pool]
    assert len(flat) == len(set(flat))
    assert all(len(pool) == 4 for pool in pools)


def test_a_set_too_small_to_go_round_is_dealt_with_overlap():
    # Twelve players at four civs each needs forty-eight from forty-four, so
    # some civ lands in more than one pool. This is the normal case for a
    # full lobby and for any game with a chosen starting age.
    pools = deal_civs(CIVS, 12, 4, random.Random(7))
    flat = [token for pool in pools for token in pool]
    assert len(flat) > len(set(flat))
    assert all(len(pool) == 4 for pool in pools)


def test_no_pool_ever_repeats_a_civ_within_itself():
    # Overlap is between pools. A pool offering the same civ twice would be
    # offering a smaller choice than it claims.
    for pools in (deal_civs(CIVS, 12, 4), deal_civs(CIVS[:6], 4, 4)):
        assert all(len(pool) == len(set(pool)) for pool in pools)


def test_a_pool_smaller_than_the_target_deals_what_is_left():
    pools = deal_civs(CIVS[:3], 4, 5, random.Random(7))
    assert all(len(pool) == 3 for pool in pools)


def test_no_civs_at_all_is_refused():
    with pytest.raises(NotEnoughPool):
        deal_civs([], 4, 4)
