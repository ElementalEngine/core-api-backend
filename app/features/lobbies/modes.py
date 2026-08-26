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

from dataclasses import dataclass

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


__all__ = [
    "DUEL",
    "FFA",
    "GAME_TYPES",
    "MAX_SEATS",
    "TEAMER",
    "InvalidLobbyShape",
    "LobbyShape",
    "legal_teamer_shapes",
    "resolve_shape",
]
