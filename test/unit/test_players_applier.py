"""Fixtures for the players-patch applier (D154).

The write-side twin of test_players_validator: the validator decides, this
proves the decision is carried out on the array without disturbing anything
it did not name.

What should break this file: any change to the ordering rule. Flipping the
structural sort to ascending leaves a well-formed document -- pairing
intact, adjacency intact -- with the wrong player substituted, so the
assertions here are on the exact array, never on an invariant.
"""

from types import SimpleNamespace

from app.features.matches.editing import (
    _leaver_index,
    apply_players_patch,
)
from app.features.matches.validation import SeatPatch, validate_players_patch

STEAM = {"L0": "s-L0", "L2": "s-L2", "y": "s-y", "new": "s-new"}


def P(
    team=0,
    placement=None,
    discord_id="d",
    is_sub=False,
    subbed_out=False,
    civ="Rome",
    leader=None,
):
    return SimpleNamespace(
        team=team,
        placement=placement,
        discord_id=discord_id,
        is_sub=is_sub,
        subbed_out=subbed_out,
        civ=civ,
        leader=leader,
        player_alive=True,
        steam_id="s",
        user_name=None,
        quit=False,
        delta=0.0,
    )


def M(*players):
    return SimpleNamespace(players=list(players))


def shape(match):
    return [
        (p.discord_id, p.team, p.placement, p.is_sub, p.subbed_out)
        for p in match.players
    ]


def test_two_subs_in_one_patch_land_on_the_seats_the_client_named():
    # The fixture nobody writes without D154. Ascending order marks B as the
    # sub instead of C -- valid-looking, and the wrong player.
    match = M(P(0, 0, "A"), P(1, 1, "B"), P(2, 2, "C"), P(3, 3, "D"))
    patch = [SeatPatch(0, sub_out="L0"), SeatPatch(2, sub_out="L2")]
    assert validate_players_patch(match, patch, actor_is_staff=False) == []

    apply_players_patch(match, patch, STEAM)

    assert shape(match) == [
        ("A", 0, 0, True, False),
        ("L0", 0, 0, False, True),
        ("B", 1, 1, False, False),
        ("C", 2, 2, True, False),
        ("L2", 2, 2, False, True),
        ("D", 3, 3, False, False),
    ]


def test_every_sub_in_row_is_followed_by_its_leaver():
    match = M(P(0, 0, "A"), P(1, 1, "B"), P(2, 2, "C"), P(3, 3, "D"))
    apply_players_patch(
        match, [SeatPatch(0, sub_out="L0"), SeatPatch(2, sub_out="L2")], STEAM
    )
    for i, player in enumerate(match.players):
        if player.is_sub:
            leaver = match.players[i + 1]
            assert leaver.subbed_out and leaver.team == player.team


def test_field_changes_land_before_the_structural_ones():
    # The synthetic row inherits the placement the same patch just set,
    # not the one the document arrived with.
    match = M(P(0, 0, "A"), P(1, 1, "B"))
    apply_players_patch(
        match,
        [SeatPatch(0, placement=1, sub_out="L0"), SeatPatch(1, placement=0)],
        STEAM,
    )
    assert shape(match) == [
        ("A", 0, 1, True, False),
        ("L0", 0, 1, False, True),
        ("B", 1, 0, False, False),
    ]


def test_placement_propagates_to_the_derived_leaver_row():
    match = M(P(0, 0, "A", is_sub=True), P(0, 0, "X", subbed_out=True), P(1, 1, "B"))
    apply_players_patch(match, [SeatPatch(0, placement=1)], STEAM)
    assert match.players[0].placement == 1
    assert match.players[1].placement == 1


def test_repointing_a_sub_replaces_the_leaver_rather_than_adding_one():
    # Item 79: v1's assign_sub would strand two subbed_out rows on one team
    # and rate a duel as 2v1. Declaratively the seat holds one pairing.
    match = M(P(0, 0, "A", is_sub=True), P(0, 0, "X", subbed_out=True), P(1, 1, "B"))
    apply_players_patch(match, [SeatPatch(0, sub_out="y")], STEAM)
    assert shape(match) == [
        ("A", 0, 0, True, False),
        ("y", 0, 0, False, True),
        ("B", 1, 1, False, False),
    ]
    assert match.players[1].steam_id == "s-y"
    assert sum(1 for p in match.players if p.subbed_out) == 1


def test_clearing_a_sub_removes_the_row_and_unmarks_the_seat():
    match = M(P(0, 0, "A", is_sub=True), P(0, 0, "X", subbed_out=True), P(1, 1, "B"))
    apply_players_patch(match, [SeatPatch(0, sub_out=None)], STEAM)
    assert shape(match) == [
        ("A", 0, 0, False, False),
        ("B", 1, 1, False, False),
    ]


def test_clearing_a_high_seat_while_editing_a_low_one():
    match = M(
        P(0, 0, "A"),
        P(1, 1, "B", is_sub=True),
        P(1, 1, "X", subbed_out=True),
        P(2, 2, "C"),
    )
    apply_players_patch(
        match, [SeatPatch(0, quit=True), SeatPatch(1, sub_out=None)], STEAM
    )
    assert shape(match) == [
        ("A", 0, 0, False, False),
        ("B", 1, 1, False, False),
        ("C", 2, 2, False, False),
    ]
    assert match.players[0].quit is True


def test_assign_quit_and_sub_in_one_patch():
    match = M(P(0, 0, "-765", civ="Rome", leader="LEADER_X"), P(1, 1, "B"))
    apply_players_patch(
        match, [SeatPatch(0, discord_id="new", quit=True, sub_out="L0")], STEAM
    )
    assert match.players[0].discord_id == "new"
    assert match.players[0].steam_id == "s-new"
    assert match.players[0].quit is True
    # The synthetic row inherits the seat's identity, not the leaver's.
    assert match.players[1].civ == "Rome"
    assert match.players[1].leader == "LEADER_X"
    assert match.players[1].team == 0


def test_leaver_index_reads_adjacency_rather_than_assuming_it():
    match = M(P(0, 0, "A", is_sub=True), P(0, 0, "X", subbed_out=True), P(1, 1, "B"))
    assert _leaver_index(match, 0) == 1
    assert _leaver_index(match, 2) is None
