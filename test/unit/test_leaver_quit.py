"""A seat's quit and the quit of the player its sub came in for are two marks,
on two rows. Either, both or neither may be set.

What should break this file: a leaver's quit landing on the sub, a quit
staying with a leaver who was replaced, an earlier seat's new sub shifting a
later seat's mark onto the wrong row, or a quit accepted for nobody.
"""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app.features.matches.editing import apply_players_patch
from app.features.matches.schemas_v2 import PlayersPatch
from app.features.matches.validation import Rule, SeatPatch, validate_players_patch

STEAM = {"y": "s-y", "L": "s-L"}


def P(
    team=0, placement=None, discord_id="d", is_sub=False, subbed_out=False, quit=False
):
    return SimpleNamespace(
        team=team,
        placement=placement,
        discord_id=discord_id,
        is_sub=is_sub,
        subbed_out=subbed_out,
        civ="Rome",
        leader=None,
        player_alive=True,
        steam_id="s",
        user_name=None,
        quit=quit,
        delta=0.0,
    )


def M(*players):
    return SimpleNamespace(players=list(players))


def subbed():
    # Calcifer came in for Busysnail on seat 0; Bob played seat 2.
    return M(
        P(0, 0, "calcifer", is_sub=True),
        P(0, 0, "busysnail", subbed_out=True),
        P(1, 1, "bob"),
    )


def quits(match):
    return [(p.discord_id, p.quit) for p in match.players]


def test_the_player_who_left_quits_and_the_sub_does_not():
    match = subbed()
    apply_players_patch(match, [SeatPatch(0, leaver_quit=True)], STEAM)
    assert quits(match) == [("calcifer", False), ("busysnail", True), ("bob", False)]


def test_both_can_quit_when_the_sub_left_it_to_the_ai():
    match = subbed()
    apply_players_patch(match, [SeatPatch(0, quit=True, leaver_quit=True)], STEAM)
    assert quits(match) == [("calcifer", True), ("busysnail", True), ("bob", False)]


def test_a_quit_without_a_sub_is_the_seat_players_own():
    match = M(P(0, 0, "a"), P(1, 1, "b"))
    apply_players_patch(match, [SeatPatch(0, quit=True)], STEAM)
    assert quits(match) == [("a", True), ("b", False)]


def test_a_sub_added_in_the_same_patch_takes_its_leavers_quit():
    match = M(P(0, 0, "a"), P(1, 1, "b"))
    apply_players_patch(match, [SeatPatch(0, sub_out="L", leaver_quit=True)], STEAM)
    assert quits(match) == [("a", False), ("L", True), ("b", False)]


def test_an_earlier_seats_new_sub_does_not_shift_a_later_seats_quit():
    match = M(P(0, 0, "a"), P(1, 1, "c", is_sub=True), P(1, 1, "x", subbed_out=True))
    apply_players_patch(
        match, [SeatPatch(0, sub_out="L"), SeatPatch(1, leaver_quit=True)], STEAM
    )
    assert quits(match) == [("a", False), ("L", False), ("c", False), ("x", True)]


def test_a_replaced_leaver_does_not_keep_the_old_leavers_quit():
    match = M(P(0, 0, "c", is_sub=True), P(0, 0, "x", subbed_out=True, quit=True))
    apply_players_patch(match, [SeatPatch(0, sub_out="y")], STEAM)
    assert quits(match) == [("c", False), ("y", False)]


def test_unticking_the_leavers_quit_clears_only_theirs():
    match = M(
        P(0, 0, "c", is_sub=True, quit=True),
        P(0, 0, "x", subbed_out=True, quit=True),
        P(1, 1, "b"),
    )
    apply_players_patch(match, [SeatPatch(0, leaver_quit=False)], STEAM)
    assert quits(match) == [("c", True), ("x", False), ("b", False)]


def _rules(match, patch):
    return [v.rule for v in validate_players_patch(match, patch, actor_is_staff=False)]


def test_a_leavers_quit_needs_someone_who_left():
    assert _rules(M(P(0, 0, "a"), P(1, 1, "b")), [SeatPatch(0, leaver_quit=True)]) == [
        Rule.NO_LEAVER
    ]
    assert _rules(subbed(), [SeatPatch(0, sub_out=None, leaver_quit=True)]) == [
        Rule.NO_LEAVER
    ]
    assert (
        _rules(M(P(0, 0, "a"), P(1, 1, "b")), [SeatPatch(0, leaver_quit=False)]) == []
    )


def test_a_leavers_quit_is_fine_when_the_sub_is_set_in_the_same_patch():
    assert (
        _rules(
            M(P(0, 0, "a"), P(1, 1, "b")), [SeatPatch(0, sub_out="L", leaver_quit=True)]
        )
        == []
    )


def test_an_unassigned_leaver_cannot_be_marked():
    match = M(
        P(0, 0, "c", is_sub=True), P(0, 0, "-7656", subbed_out=True), P(1, 1, "b")
    )
    assert _rules(match, [SeatPatch(0, leaver_quit=True)]) == [Rule.QUIT_ON_PLACEHOLDER]


def test_the_wire_carries_the_leavers_quit_and_refuses_null():
    patch = PlayersPatch.model_validate({"players": [{"seat": 0, "leaver_quit": True}]})
    assert patch.to_seat_patches() == [SeatPatch(0, leaver_quit=True)]
    with pytest.raises(ValidationError):
        PlayersPatch.model_validate({"players": [{"seat": 0, "leaver_quit": None}]})
    assert not SeatPatch(0, leaver_quit=False).is_empty()
