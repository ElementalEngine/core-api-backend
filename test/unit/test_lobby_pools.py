"""Dealing draft pools.

⚠ Two properties matter more than the arithmetic: pools must be disjoint, or
two players draft the same leader; and a deal must NOT be reproducible from
the lobby id, or a blind pool can be computed before it is dealt.
"""

from __future__ import annotations

import random

import pytest

from app.features.lobbies.pools import (
    NotEnoughPool,
    assign_one_each,
    deal,
    even_split,
    remaining_after_bans,
)

TOKENS = [f"LEADER_{i:02d}" for i in range(44)]


def test_the_pool_is_truncated_so_every_player_gets_the_same_count():
    # ⚠ Forty-four across five deals eight each and DISCARDS four. An uneven
    # split would hand somebody a wider choice, which matters more than four
    # leaders do.
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
    # ⚠ D197, and the opposite of D191's tie-break by design. Seeding a deal
    # on the lobby id would let anyone holding it compute a blind pool before
    # it is dealt, which is the whole of blind.
    first = deal(TOKENS, 5)
    second = deal(TOKENS, 5)
    assert first != second


def test_random_mode_assigns_one_distinct_token_each():
    assigned = assign_one_each(TOKENS, 5, random.Random(7))
    assert len(assigned) == 5
    assert len(set(assigned)) == 5
    assert set(assigned) <= set(TOKENS)


def test_a_pool_too_small_to_assign_is_refused_rather_than_duplicated():
    # ⚠ Mite samples WITH REPLACEMENT here, handing two players the same
    # leader rather than failing. A silent duplicate is worse than a refusal.
    with pytest.raises(NotEnoughPool):
        assign_one_each(["a", "b"], 5)


def test_bans_are_removed_and_the_rest_keeps_its_order():
    remaining = remaining_after_bans(TOKENS, ["LEADER_00", "LEADER_43", "LEADER_00"])
    assert len(remaining) == 42
    assert remaining == [t for t in TOKENS if t not in {"LEADER_00", "LEADER_43"}]


def test_banning_nothing_removes_nothing():
    assert remaining_after_bans(TOKENS, []) == TOKENS
