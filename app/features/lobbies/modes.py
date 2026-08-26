"""What a lobby's game type implies about its seats.

Mite sends `game_type`, plus two numbers when it is a teamer. Everything
else is derived here, so no client computes a seat count and there is one
place to read when the rules change (spec section 11).

⚠ `duel` derives to two teams of one. The spec already says "Duel = team of
one" (section 4), and it is what makes `is_captain` -- the lowest seat_index
within a team (D75) -- name both duellists with no special case. It stays a
distinct game_type because the rating scopes are per-mode (`rt_duel`).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

# Civ6's player limit, not a mode rule -- named once so no mode carries a
# copy of it.
MAX_SEATS = 12

FFA = "ffa"
TEAMER = "teamer"
DUEL = "duel"
GAME_TYPES: tuple[str, ...] = (FFA, TEAMER, DUEL)

# FFA fills to a ceiling rather than an exact count, so it needs a floor or
# a lobby that stops at nine never advances.
FFA_MIN_SEATS = 6

MIN_TEAMS, MAX_TEAMS = 2, 4
MIN_TEAM_SIZE, MAX_TEAM_SIZE = 2, 6


class InvalidLobbyShape(ValueError):
    """The mode fields do not describe a lobby that can exist.

    Carries the offending field so the route can answer INVALID_REQUEST with
    something a host can act on, rather than a bare 400.
    """

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field


class InvalidSeating(ValueError):
    """The seats do not describe an arrangement that can exist.

    Distinct from InvalidLobbyShape: there the mode fields are wrong, here
    the shape is fine and the seating is not. Carries the offending field for
    the same reason -- a host can act on "seat 3 is taken", not on a 400.
    """

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field


@dataclass(frozen=True)
class LobbyShape:
    game_type: str
    number_teams: int | None
    team_size: int | None
    seat_count: int
    min_seats: int


def _reject_team_fields(game_type: str, number_teams, team_size) -> None:
    for name, value in (("number_teams", number_teams), ("team_size", team_size)):
        if value is not None:
            raise InvalidLobbyShape(name, f"{name} is only valid for a teamer")


def _require(name: str, value, low: int, high: int) -> int:
    if value is None:
        raise InvalidLobbyShape(name, f"{name} is required for a teamer")
    if not low <= value <= high:
        raise InvalidLobbyShape(name, f"{name} must be between {low} and {high}")
    return value


def resolve_shape(
    game_type: str,
    number_teams: int | None = None,
    team_size: int | None = None,
) -> LobbyShape:
    """Validate the mode fields and fill in what follows from them."""
    if game_type not in GAME_TYPES:
        raise InvalidLobbyShape(
            "game_type", f"game_type must be one of {', '.join(GAME_TYPES)}"
        )

    if game_type == FFA:
        _reject_team_fields(game_type, number_teams, team_size)
        return LobbyShape(FFA, None, None, MAX_SEATS, FFA_MIN_SEATS)

    if game_type == DUEL:
        _reject_team_fields(game_type, number_teams, team_size)
        return LobbyShape(DUEL, 2, 1, 2, 2)

    teams = _require("number_teams", number_teams, MIN_TEAMS, MAX_TEAMS)
    size = _require("team_size", team_size, MIN_TEAM_SIZE, MAX_TEAM_SIZE)
    seats = teams * size
    if seats > MAX_SEATS:
        raise InvalidLobbyShape(
            "team_size",
            f"{teams} teams of {size} needs {seats} seats, over {MAX_SEATS}",
        )
    # Every seat must be filled before a teamer can start -- an uneven side
    # is not a game.
    return LobbyShape(TEAMER, teams, size, seats, seats)


def legal_teamer_shapes() -> tuple[tuple[int, int], ...]:
    """Every `(number_teams, team_size)` the rules above admit.

    Derived, never listed: adding a shape means changing a bound, not editing
    an enumeration that some other file also keeps.
    """
    return tuple(
        (teams, size)
        for teams in range(MIN_TEAMS, MAX_TEAMS + 1)
        for size in range(MIN_TEAM_SIZE, MAX_TEAM_SIZE + 1)
        if teams * size <= MAX_SEATS
    )


def validate_seats(seats: Sequence[Mapping[str, Any]], shape: LobbyShape) -> None:
    """The seating rules, including the two no index can enforce.

    ⚠ Neither of the first two has a Mongo leg left, and both were measured:

    **No duplicate player.** MongoDB de-duplicates multikey keys per
    document, so one lobby's seat array cannot collide with itself (D176).
    The `$ne` predicate on the write is the guarantee; this is the second
    line, and the readable refusal before the round trip.

    **No seat without a player.** D175's partial filter excludes a lobby
    whose only seat lacks `discord_id`, so Mongo ACCEPTS the bad document and
    it breaks a later join instead -- symptom far from cause (Correction 74).
    This is now the only thing between a null seat and a lobby that
    mysteriously refuses joins.

    ⚠ `seat_index` is absolute and gaps are legal. civup's
    `arrangeTeamLobbySlots` compacts before chunking, so closing a hole
    mid-lobby silently moves a player across a team boundary (O-19b).

    Raises InvalidSeating naming the offending field; returns None otherwise.
    """
    players: set[str] = set()
    indexes: set[int] = set()
    per_team: dict[int, int] = {}

    for seat in seats:
        player = seat.get("discord_id")
        if not player:
            raise InvalidSeating("discord_id", "a seat with no player cannot exist")
        if player in players:
            raise InvalidSeating("discord_id", f"{player} already holds a seat")
        players.add(player)

        index = seat.get("seat_index")
        if index is None or not 0 <= index < shape.seat_count:
            raise InvalidSeating(
                "seat_index",
                f"seat_index must be between 0 and {shape.seat_count - 1}",
            )
        if index in indexes:
            raise InvalidSeating("seat_index", f"seat {index} is already taken")
        indexes.add(index)

        # A seated player with no side is legal everywhere: seating means
        # "you are in this lobby", never "you are on red" (spec section 4).
        team = seat.get("team")
        if team is None:
            continue
        if shape.number_teams is None or shape.team_size is None:
            raise InvalidSeating("team", f"{shape.game_type} has no teams")
        if not 0 <= team < shape.number_teams:
            raise InvalidSeating(
                "team", f"team must be between 0 and {shape.number_teams - 1}"
            )
        per_team[team] = per_team.get(team, 0) + 1
        if per_team[team] > shape.team_size:
            raise InvalidSeating("team", f"team {team} already holds {shape.team_size}")


__all__ = [
    "DUEL",
    "FFA",
    "GAME_TYPES",
    "MAX_SEATS",
    "TEAMER",
    "InvalidLobbyShape",
    "InvalidSeating",
    "LobbyShape",
    "legal_teamer_shapes",
    "resolve_shape",
    "validate_seats",
]
