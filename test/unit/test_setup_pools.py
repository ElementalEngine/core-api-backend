from __future__ import annotations

import random
from collections import Counter

import pytest

from app.features.civdata.seeds import leader_types
from app.features.match_setups.pools import (
    LEADER_TYPES,
    NAVAL,
    NotEnoughPool,
    civ_target,
    deal,
    deal_civs,
    even_split,
    leader_pools,
    remaining_after_bans,
)

TOKENS = [f"LEADER_{i:02d}" for i in range(44)]


def test_the_pool_is_truncated_so_every_player_gets_the_same_count():
    assert even_split(44, 5) == 8
    assert even_split(44, 4) == 11


def test_a_player_who_would_get_nothing_is_refused():
    # Failing here is far cheaper than failing at the pick.
    with pytest.raises(NotEnoughPool):
        even_split(3, 5)


@pytest.mark.parametrize("players", [0, -1])
def test_a_draft_needs_players(players):
    with pytest.raises(NotEnoughPool):
        even_split(44, players)


def test_pools_are_disjoint_and_equal():
    pools = leader_pools(TOKENS, 5, "standard", {}, random.Random(7))
    assert [len(pool) for pool in pools] == [8, 8, 8, 8, 8]
    flat = [token for pool in pools for token in pool]
    assert len(flat) == len(set(flat)), "a token reached two players"
    assert set(flat) <= set(TOKENS)
    assert len(TOKENS) - len(flat) == 4


def test_a_blind_pool_fits_one_menu_and_random_deals_one_each():
    many = [f"L{n}" for n in range(80)]
    assert [len(p) for p in leader_pools(many, 2, "blind", {})] == [25, 25]
    assert [len(p) for p in leader_pools(many, 3, "random", {})] == [1, 1, 1]


def test_a_deal_is_not_reproducible_from_the_lobby():
    first = leader_pools(TOKENS, 5, "standard", {})
    second = leader_pools(TOKENS, 5, "standard", {})
    assert first != second


def test_random_mode_deals_one_distinct_token_each():
    pools = leader_pools(TOKENS, 5, "random", {}, random.Random(7))
    flat = [token for pool in pools for token in pool]
    assert [len(pool) for pool in pools] == [1, 1, 1, 1, 1]
    assert len(set(flat)) == 5
    assert set(flat) <= set(TOKENS)


def test_a_pool_too_small_to_go_round_is_refused_rather_than_duplicated():
    with pytest.raises(NotEnoughPool):
        leader_pools(["a", "b"], 5, "random", {})


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
        ("teamer", 5, 4),
    ],
)
def test_the_civ_target_is_a_target_not_a_division(game_type, groups, expected):
    assert civ_target(game_type, groups) == expected


def test_civ_pools_are_disjoint_while_the_set_allows_it():
    pools = deal_civs(CIVS, 10, 4, random.Random(7))
    flat = [token for pool in pools for token in pool]
    assert len(flat) == len(set(flat))
    assert all(len(pool) == 4 for pool in pools)


def test_a_set_too_small_to_go_round_is_dealt_with_overlap():
    # Twelve players at four civs each needs forty-eight from forty-four, so
    # some civ lands in more than one pool.
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


def test_a_chosen_pool_size_is_dealt_and_the_rest_left():
    pools = deal(TOKENS, 3, 2, {}, random.Random(7))
    assert [len(pool) for pool in pools] == [2, 2, 2]
    with pytest.raises(NotEnoughPool, match="need"):
        deal(TOKENS, 3, len(TOKENS), {}, random.Random(7))


@pytest.mark.parametrize(("players", "per_pool"), [(0, 2), (3, 0)])
def test_a_deal_needs_a_pool_and_a_leader_in_it(players, per_pool):
    with pytest.raises(NotEnoughPool):
        deal(TOKENS, players, per_pool, {})


CIV6 = leader_types("civ6")


def _count(pool, kind, types=CIV6):
    return sum(1 for token in pool if types[token] == kind)


@pytest.mark.parametrize("draft_mode", ["standard", "blind", "random"])
@pytest.mark.parametrize("players", [2, 3, 4, 6, 8, 10, 12])
def test_every_type_is_spread_across_the_pools_to_within_one(draft_mode, players):
    rng = random.Random(players)
    for _ in range(50):
        left = rng.sample(sorted(CIV6), len(CIV6) - rng.randrange(21))
        pools = leader_pools(left, players, draft_mode, CIV6, rng)
        flat = [token for pool in pools for token in pool]
        assert len(flat) == len(set(flat)) and set(flat) <= set(left)
        assert len({len(pool) for pool in pools}) == 1
        for kind in LEADER_TYPES:
            counts = [_count(pool, kind) for pool in pools]
            assert max(counts) - min(counts) <= 1, (kind, counts)


def test_each_type_is_dealt_in_proportion_to_what_is_left():
    pools = deal(sorted(CIV6), 6, 4, CIV6, random.Random(1))
    dealt = Counter(CIV6[token] for pool in pools for token in pool)
    left = Counter(CIV6.values())
    for kind in LEADER_TYPES:
        assert abs(dealt[kind] - 24 * left[kind] / len(CIV6)) < 2, kind


@pytest.mark.parametrize(("players", "per_pool"), [(6, 4), (10, 3), (14, 2)])
def test_every_pool_gets_a_naval_leader_while_enough_are_left(players, per_pool):
    rng = random.Random(per_pool)
    for _ in range(50):
        pools = deal(sorted(CIV6), players, per_pool, CIV6, rng)
        assert all(_count(pool, NAVAL) >= 1 for pool in pools)


def test_too_few_naval_leaders_reach_as_many_pools_as_they_can():
    types = {f"N{i}": NAVAL for i in range(3)} | {f"W{i}": "War" for i in range(30)}
    pools = deal(sorted(types), 5, 3, types, random.Random(2))
    assert sorted(_count(pool, NAVAL, types) for pool in pools) == [0, 0, 1, 1, 1]


def test_the_naval_slots_come_from_war_first():
    # Rounding alone deals one naval, two culture and one war; the second
    # naval leader takes the war slot.
    types = (
        {f"N{i}": NAVAL for i in range(2)}
        | {f"C{i}": "Culture" for i in range(4)}
        | {f"W{i}": "War" for i in range(4)}
    )
    pools = deal(sorted(types), 2, 2, types, random.Random(3))
    assert sorted(types[token] for pool in pools for token in pool) == [
        "Culture",
        "Culture",
        NAVAL,
        NAVAL,
    ]


def test_random_mode_has_no_naval_minimum():
    types = {f"N{i}": NAVAL for i in range(2)} | {f"W{i}": "War" for i in range(30)}
    pools = leader_pools(sorted(types), 4, "random", types, random.Random(4))
    assert sum(_count(pool, NAVAL, types) for pool in pools) == 0


def test_a_leader_without_a_known_type_is_dealt_as_untyped():
    types = {f"X{i}": "Unheard" for i in range(6)}
    pools = deal(sorted(types) + ["Y0", "Y1"], 2, 4, types, random.Random(5))
    assert sorted(token for pool in pools for token in pool) == sorted(types) + [
        "Y0",
        "Y1",
    ]
