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


def M(*players, is_cloud=True):
    # Cloud unless a test says otherwise: there a reporter may assign players.
    return SimpleNamespace(players=list(players), is_cloud=is_cloud)


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
    duel = paired_duel()
    assert rules(check(duel, [SeatPatch(0, placement=2)])) == [
        Rule.PLACEMENT_OUT_OF_RANGE
    ]
    assert rules(check(ffa(), [SeatPatch(0, placement=-1)])) == [
        Rule.PLACEMENT_OUT_OF_RANGE
    ]


def test_a_reporter_may_set_or_widen_a_tie():
    assert check(ffa(), [SeatPatch(2, placement=1)]) == []
    assert check(ffa(), [SeatPatch(2, placement=1)], staff=True) == []
    tied = M(P(0, 0, "a"), P(1, 0, "b"), P(2, 2, "c"))
    assert check(tied, [SeatPatch(2, placement=0)]) == []


def test_places_stay_dense_even_for_staff():
    # 1st, 2nd, 2nd is a tie; 2nd, 2nd, 3rd leaves nobody first.
    assert rules(check(ffa(), [SeatPatch(0, placement=2)], staff=True)) == [
        Rule.PLACE_GAP
    ]


def test_teammates_sharing_a_placement_is_not_a_tie():
    # The normal teamer shape: one placement per team, shared by its members.
    assert check(teamer(), [SeatPatch(1, placement=0)]) == []


def test_split_team_placement_rejected():
    got = check(teamer(), [SeatPatch(1, placement=1)])
    assert [(v.seat, v.rule) for v in got] == [(None, Rule.TEAM_PLACEMENT_MISMATCH)]


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
    assert check(ffa(), [SeatPatch(1, sub_out="left")]) == []


def test_quit_on_placeholder_rejected_but_assign_and_quit_passes():
    ph = M(P(0, 0, "-7656119"), P(1, 1, "b"))
    assert rules(check(ph, [SeatPatch(0, quit=True)])) == [Rule.QUIT_ON_PLACEHOLDER]
    assert check(ph, [SeatPatch(0, discord_id="real", quit=True)]) == []
    assert check(ph, [SeatPatch(0, quit=False)]) == []


def test_duplicate_seat_entries_rejected():
    got = check(ffa(), [SeatPatch(0, quit=True), SeatPatch(0, placement=0)])
    assert rules(got) == [Rule.DUPLICATE_SEAT]


def test_a_quit_that_leaves_one_team_is_refused():
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
    assert is_placeholder(None)
    assert is_placeholder("")
    assert is_placeholder("-1")
    assert is_placeholder("-2")
    assert is_placeholder("-76561190000000001")
    assert not is_placeholder("123456789")


def test_one_player_per_seat():
    assert rules(check(ffa(), [SeatPatch(0, discord_id="b")])) == [
        Rule.DUPLICATE_PLAYER
    ]
    swap = [SeatPatch(0, discord_id="b"), SeatPatch(1, discord_id="a")]
    assert check(ffa(), swap) == []
    assert rules(check(paired_duel(), [SeatPatch(2, discord_id="x")])) == [
        Rule.DUPLICATE_PLAYER
    ]
    assert rules(check(ffa(), [SeatPatch(1, sub_out="c")])) == [Rule.DUPLICATE_PLAYER]
    doubled = M(P(0, 0, "a"), P(1, 1, "a"), P(2, 2, "c"))
    assert check(doubled, [SeatPatch(0, quit=True)]) == []
    assert rules(check(doubled, [SeatPatch(2, discord_id="a")])) == [
        Rule.DUPLICATE_PLAYER
    ]


def test_messages_number_seats_as_players_see_them():
    (dup,) = check(ffa(), [SeatPatch(0, discord_id="b")])
    assert dup.message == "<@b> would be in seats 1 and 2."
    (own,) = check(ffa(), [SeatPatch(2, sub_out="c")])
    assert own.message == "<@c> cannot come in for themselves in seat 3."


def test_a_reporter_may_change_a_leader_or_civ():
    assert check(ffa(), [SeatPatch(1, leader="LEADER_X", civ="CIV_Y")]) == []


def test_on_a_real_time_report_only_staff_change_a_player():
    real_time = M(P(0, 0, "a"), P(1, 1, "b"), P(2, 2, "c"), is_cloud=False)
    (refused,) = check(real_time, [SeatPatch(1, discord_id="z")])
    assert refused.rule == Rule.STAFF_ONLY_PLAYER
    assert refused.message == (
        "Only staff can change seat 2's player in a real-time game."
    )
    assert check(real_time, [SeatPatch(1, discord_id="z")], staff=True) == []
    # Naming the player already there changes nothing, so it is not refused.
    assert check(real_time, [SeatPatch(1, discord_id="b", quit=True)]) == []
    # A cloud report's reporter fills its seats in.
    assert check(ffa(), [SeatPatch(1, discord_id="z")]) == []
