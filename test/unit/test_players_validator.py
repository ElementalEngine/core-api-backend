"""Fixture zone for the players-patch validator (D151-D155).

What should break this file: any change to validation.py's rule set, its
deterministic ordering, or its introduced-only semantics. If a change here
passes without touching validation.py, a fixture is asserting nothing.

Fixtures are duck-typed on purpose: the validator is pure and structural,
and these tables stay readable without a MatchModel constructor.
"""

from types import SimpleNamespace

from app.features.matches.validation import (
    Rule,
    SeatPatch,
    is_placeholder,
    validate_players_patch,
)


def P(team=0, placement=None, discord_id="d", is_sub=False, subbed_out=False):
    return SimpleNamespace(
        team=team,
        placement=placement,
        discord_id=discord_id,
        is_sub=is_sub,
        subbed_out=subbed_out,
    )


def M(*players):
    return SimpleNamespace(players=list(players))


def check(match, patch, *, staff=False):
    return validate_players_patch(match, patch, actor_is_staff=staff)


def rules(violations):
    return [v.rule for v in violations]


def ffa():
    return M(P(0, 0, "a"), P(1, 1, "b"), P(2, 2, "c"))


def paired_duel():
    # [A(is_sub), X(subbed_out, synthetic), B] - the shape assign_sub builds.
    return M(
        P(0, 0, "a", is_sub=True),
        P(0, 0, "x", subbed_out=True),
        P(1, 1, "b"),
    )


def teamer():
    return M(P(0, 0, "a"), P(0, 0, "b"), P(1, 1, "c"), P(1, 1, "d"))


def test_legal_sparse_patch_passes():
    assert check(ffa(), [SeatPatch(0, discord_id="z")]) == []


def test_empty_patch_rejected_both_shapes():
    assert rules(check(ffa(), [])) == [Rule.EMPTY_PATCH]
    assert rules(check(ffa(), [SeatPatch(1)])) == [Rule.EMPTY_PATCH]


def test_unknown_seat_above_and_below_range():
    assert rules(check(ffa(), [SeatPatch(3, quit=True)])) == [Rule.UNKNOWN_SEAT]
    assert rules(check(ffa(), [SeatPatch(-1, quit=True)])) == [Rule.UNKNOWN_SEAT]


def test_placement_range_is_team_count_not_player_count():
    # Correction 44: the duel has three rows but T=2, so 0..1 is the range.
    duel = paired_duel()
    assert rules(check(duel, [SeatPatch(0, placement=2)])) == [
        Rule.PLACEMENT_OUT_OF_RANGE
    ]
    assert rules(check(ffa(), [SeatPatch(0, placement=-1)])) == [
        Rule.PLACEMENT_OUT_OF_RANGE
    ]


def test_tie_requires_staff_and_staff_may_set_one():
    patch = [SeatPatch(0, placement=2)]
    assert rules(check(ffa(), patch)) == [Rule.DUPLICATE_PLACEMENT]
    assert check(ffa(), patch, staff=True) == []


def test_tie_against_an_unpatched_seat_is_caught():
    # Merge-before-check: the duplicate is created against seat 1's existing
    # placement, which this patch never mentions.
    assert rules(check(ffa(), [SeatPatch(0, placement=1)])) == [
        Rule.DUPLICATE_PLACEMENT
    ]


def test_pre_existing_tie_is_grandfathered():
    tied = M(P(0, 0, "a"), P(1, 0, "b"), P(2, 2, "c"))
    assert check(tied, [SeatPatch(2, discord_id="z")]) == []


def test_widening_an_existing_tie_requires_staff():
    tied = M(P(0, 0, "a"), P(1, 0, "b"), P(2, 2, "c"))
    patch = [SeatPatch(2, placement=0)]
    assert rules(check(tied, patch)) == [Rule.DUPLICATE_PLACEMENT]
    assert check(tied, patch, staff=True) == []


def test_teammates_sharing_a_placement_is_not_a_tie():
    # The normal teamer shape: one placement per team, shared by its members.
    assert check(teamer(), [SeatPatch(1, placement=0)]) == []


def test_split_team_placement_rejected_with_the_tie_it_causes():
    # Item 78: no v1 route can produce this; the collapse can. Both facts
    # about the resulting state are reported, None-seat first.
    got = check(teamer(), [SeatPatch(1, placement=1)])
    assert [(v.seat, v.rule) for v in got] == [
        (None, Rule.TEAM_PLACEMENT_MISMATCH),
        (1, Rule.DUPLICATE_PLACEMENT),
    ]


def test_subbed_out_row_is_derived_not_editable():
    assert rules(check(paired_duel(), [SeatPatch(1, placement=0)])) == [
        Rule.SUBBED_OUT_PLACEMENT
    ]


def test_sub_out_is_declarative_and_idempotent():
    duel = paired_duel()
    assert check(duel, [SeatPatch(0, sub_out="x")]) == []  # same pairing
    assert check(duel, [SeatPatch(0, sub_out="y")]) == []  # repoint
    assert check(duel, [SeatPatch(0, sub_out=None)]) == []  # clear


def test_patch_created_sub_is_paired_by_construction():
    # D154: one field carries both halves, so UNPAIRED_SUB is unrepresentable
    # in a well-formed body.
    assert check(ffa(), [SeatPatch(1, sub_out="left")]) == []


def test_quit_on_placeholder_rejected_but_assign_and_quit_passes():
    ph = M(P(0, 0, "-7656119"), P(1, 1, "b"))
    assert rules(check(ph, [SeatPatch(0, quit=True)])) == [Rule.QUIT_ON_PLACEHOLDER]
    assert check(ph, [SeatPatch(0, discord_id="real", quit=True)]) == []
    assert check(ph, [SeatPatch(0, quit=False)]) == []


def test_duplicate_seat_entries_rejected():
    got = check(ffa(), [SeatPatch(0, quit=True), SeatPatch(0, placement=0)])
    assert rules(got) == [Rule.DUPLICATE_SEAT]


def test_too_few_teams_surfaces_item_61_shape():
    one = M(P(0, 0, "a"), P(0, 1, "b"))
    assert Rule.TOO_FEW_TEAMS in rules(check(one, [SeatPatch(0, quit=True)]))


def test_legacy_unpaired_sub_stays_editable():
    legacy = M(P(0, 0, "a", is_sub=True), P(1, 1, "b"), P(2, 2, "c"))
    assert check(legacy, [SeatPatch(2, discord_id="z")]) == []


def test_all_violations_returned_in_deterministic_order():
    got = check(
        ffa(),
        [
            SeatPatch(0, placement=9),
            SeatPatch(5, quit=True),
            SeatPatch(1, quit=True),
        ],
    )
    assert [(v.seat, v.rule) for v in got] == [
        (0, Rule.PLACEMENT_OUT_OF_RANGE),
        (5, Rule.UNKNOWN_SEAT),
    ]


def test_is_placeholder_is_the_single_predicate():
    # §4 item 20 / reconciliation-query §4.2 - the guard both writers skip by.
    assert is_placeholder(None)
    assert is_placeholder("")
    assert is_placeholder("-1")
    assert is_placeholder("-2")
    assert is_placeholder("-76561190000000001")
    assert not is_placeholder("123456789")


def test_canary_the_validator_can_actually_fail():
    # D86 Rule 1: neutering validate_players_patch to `return []` must turn
    # this red while the legal-patch assertions above stay green.
    violating = check(ffa(), [SeatPatch(9, quit=True)])
    assert violating, "a violating patch produced no violations"
    assert check(ffa(), [SeatPatch(0, discord_id="ok")]) == []
