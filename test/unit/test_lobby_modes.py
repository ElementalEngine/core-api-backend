"""Mode shapes: what game_type implies, and what it refuses.

Every rejection asserts the FIELD it names, not merely that something was
raised -- a validator that always blamed `game_type` would pass a suite that
only checked the exception type, and the host would get an unactionable
error.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.modes import (
    MAX_SEATS,
    InvalidLobbyShape,
    legal_teamer_shapes,
    resolve_shape,
)


def test_ffa_fills_to_a_ceiling_with_a_floor():
    shape = resolve_shape("ffa")
    assert (shape.seat_count, shape.min_seats) == (12, 6)
    # No teams at all -- not "one team of twelve".
    assert (shape.number_teams, shape.team_size) == (None, None)


def test_duel_is_two_teams_of_one():
    # Not team: null. `is_captain` is the lowest seat_index WITHIN a team
    # (D75), so two teams of one names both duellists with no special case.
    shape = resolve_shape("duel")
    assert (shape.number_teams, shape.team_size) == (2, 1)
    assert (shape.seat_count, shape.min_seats) == (2, 2)


@pytest.mark.parametrize(
    "teams,size,seats",
    [(2, 3, 6), (3, 3, 9), (4, 3, 12), (2, 6, 12), (2, 2, 4), (4, 2, 8)],
)
def test_teamer_seat_count_is_teams_times_size(teams, size, seats):
    shape = resolve_shape("teamer", teams, size)
    assert shape.seat_count == seats
    # A teamer starts full: an uneven side is not a game.
    assert shape.min_seats == seats


def test_every_legal_teamer_shape_resolves_and_fits_the_cap():
    shapes = legal_teamer_shapes()
    assert len(shapes) == 10
    assert (3, 3) in shapes and (4, 3) in shapes  # 3v3v3 and 4v4v4
    for teams, size in shapes:
        assert resolve_shape("teamer", teams, size).seat_count <= MAX_SEATS


def test_legal_shapes_are_derived_not_listed():
    # Anything the bounds admit must resolve, and anything they exclude must
    # not -- so the tuple cannot drift from the rules that produce it.
    admitted = set(legal_teamer_shapes())
    for teams in range(1, 6):
        for size in range(1, 8):
            ok = (teams, size) in admitted
            if ok:
                resolve_shape("teamer", teams, size)
            else:
                with pytest.raises(InvalidLobbyShape):
                    resolve_shape("teamer", teams, size)


@pytest.mark.parametrize("game_type", ["ffa", "duel"])
@pytest.mark.parametrize(
    "teams,size,field", [(2, None, "number_teams"), (None, 3, "team_size")]
)
def test_team_fields_are_refused_outside_teamer(game_type, teams, size, field):
    with pytest.raises(InvalidLobbyShape) as exc:
        resolve_shape(game_type, teams, size)
    assert exc.value.field == field


@pytest.mark.parametrize(
    "teams,size,field",
    [
        (None, 3, "number_teams"),
        (3, None, "team_size"),
        (1, 3, "number_teams"),
        (5, 2, "number_teams"),
        (3, 1, "team_size"),
        (2, 7, "team_size"),
    ],
)
def test_teamer_bounds_name_the_offending_field(teams, size, field):
    with pytest.raises(InvalidLobbyShape) as exc:
        resolve_shape("teamer", teams, size)
    assert exc.value.field == field


def test_a_shape_over_the_cap_is_refused_even_though_both_bounds_pass():
    # 3 x 5 = 15. Both numbers are individually legal; the product is not.
    with pytest.raises(InvalidLobbyShape) as exc:
        resolve_shape("teamer", 3, 5)
    assert exc.value.field == "team_size"
    assert "15" in str(exc.value) and "12" in str(exc.value)


@pytest.mark.parametrize("game_type", ["", "FFA", "1v1", "solo", None])
def test_an_unknown_game_type_is_refused(game_type):
    with pytest.raises(InvalidLobbyShape) as exc:
        resolve_shape(game_type)
    assert exc.value.field == "game_type"


def test_shapes_are_frozen():
    # The shape is derived once and read many times; a caller mutating it
    # would change what later code believes the lobby is.
    shape = resolve_shape("teamer", 3, 3)
    with pytest.raises(Exception):
        shape.seat_count = 99  # type: ignore[misc]
