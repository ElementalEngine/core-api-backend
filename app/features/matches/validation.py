from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final
from collections.abc import Sequence

from app.features.matches.models import MatchModel


class UnsetType:
    _instance: UnsetType | None = None

    def __new__(cls) -> UnsetType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Final[UnsetType] = UnsetType()


class Rule(StrEnum):
    EMPTY_PATCH = "empty_patch"
    DUPLICATE_SEAT = "duplicate_seat"
    UNKNOWN_SEAT = "unknown_seat"
    PLACEMENT_OUT_OF_RANGE = "placement_out_of_range"
    DUPLICATE_PLACEMENT = "duplicate_placement"
    SUBBED_OUT_PLACEMENT = "subbed_out_placement"
    TEAM_PLACEMENT_MISMATCH = "team_placement_mismatch"
    UNPAIRED_SUB = "unpaired_sub"
    QUIT_ON_PLACEHOLDER = "quit_on_placeholder"
    TOO_FEW_TEAMS = "too_few_teams"


@dataclass(frozen=True, slots=True)
class Violation:
    rule: Rule
    seat: int | None
    message: str


@dataclass(frozen=True, slots=True)
class SeatPatch:
    """One seat's declarative changes. UNSET means unchanged (D89, D154)."""

    seat: int
    placement: int | UnsetType = UNSET
    discord_id: str | UnsetType = UNSET
    quit: bool | UnsetType = UNSET
    # str creates or repoints a pairing; None clears it (D154, Correction 45).
    sub_out: str | None | UnsetType = UNSET

    def is_empty(self) -> bool:
        return all(
            isinstance(v, UnsetType)
            for v in (self.placement, self.discord_id, self.quit, self.sub_out)
        )


def is_placeholder(discord_id: str | None) -> bool:
    """The one placeholder predicate (§4 item 20; reconciliation-query §4.2)."""
    return not discord_id or discord_id.startswith("-")


@dataclass(slots=True)
class _Seat:
    team: int
    placement: int | None
    discord_id: str | None
    is_sub: bool
    leaver: str | None  # discord_id of the paired synthetic row, if any


def _seats(match: MatchModel) -> list[_Seat]:
    """Project the players array onto editable seats.

    Synthetic subbed_out rows are not seats: their fields are derived from the
    sub-in row above them (service.py:672-686), so every check runs over this
    projection and a patch may never address them.
    """
    seats: list[_Seat] = []
    for i, p in enumerate(match.players):
        if p.subbed_out:
            continue
        nxt = match.players[i + 1] if i + 1 < len(match.players) else None
        leaver = nxt.discord_id if p.is_sub and nxt and nxt.subbed_out else None
        seats.append(_Seat(p.team, p.placement, p.discord_id, p.is_sub, leaver))
    return seats


def _pairing_faults(seats: Sequence[_Seat]) -> set[int]:
    """Teams holding an is_sub seat with no paired leaver (D155)."""
    return {s.team for s in seats if s.is_sub and s.leaver is None}


def _teams_at(seats: Sequence[_Seat]) -> dict[int, set[int]]:
    at: defaultdict[int, set[int]] = defaultdict(set)
    for s in seats:
        if s.placement is not None:
            at[s.placement].add(s.team)
    return at


def _tie_growth(pre: Sequence[_Seat], post: Sequence[_Seat]) -> set[int]:
    """Placement values the patch ties across teams, or widens a tie onto.

    A tie is a placement held by two or more distinct teams — teammates
    sharing a placement is the normal teamer shape, never a tie.
    """
    a, b = _teams_at(pre), _teams_at(post)
    return {v for v, teams in b.items() if len(teams) > 1 and len(teams) > len(a[v])}


def _split_teams(seats: Sequence[_Seat]) -> set[int]:
    by_team: defaultdict[int, set[int]] = defaultdict(set)
    for s in seats:
        if s.placement is not None:
            by_team[s.team].add(s.placement)
    return {t for t, ps in by_team.items() if len(ps) > 1}


def validate_players_patch(
    match: MatchModel, patch: Sequence[SeatPatch], *, actor_is_staff: bool
) -> list[Violation]:
    """Judge the whole patch as one atomic decision (D151).

    Returns every violation, deterministically ordered; [] is the only legal
    result. Conditions that can pre-exist on a match — a tie, an unpaired
    sub, a split team — fire only on what the patch introduces, so legacy
    documents stay editable (D91's "introduces", generalised).
    """
    out: list[Violation] = []

    if not patch or all(e.is_empty() for e in patch):
        return [Violation(Rule.EMPTY_PATCH, None, "The patch changes nothing.")]

    seen = Counter(e.seat for e in patch)
    for seat in sorted(s for s, count in seen.items() if count > 1):
        out.append(
            Violation(
                Rule.DUPLICATE_SEAT,
                seat,
                f"Seat {seat} appears more than once in the patch.",
            )
        )

    last: dict[int, SeatPatch] = {}
    for e in patch:  # last entry per seat wins; the duplicate is flagged above
        last[e.seat] = e

    n = len(match.players)
    pre = _seats(match)
    seat_index = {
        seat: k
        for k, seat in enumerate(
            i for i, p in enumerate(match.players) if not p.subbed_out
        )
    }
    for seat in sorted(set(last) - set(seat_index)):
        if 0 <= seat < n:
            out.append(
                Violation(
                    Rule.SUBBED_OUT_PLACEMENT,
                    seat,
                    f"Seat {seat} left the game; its row is derived, not editable.",
                )
            )
        else:
            out.append(
                Violation(
                    Rule.UNKNOWN_SEAT,
                    seat,
                    f"Seat {seat} is not on the match (0-{n - 1}).",
                )
            )

    merged = [replace(s) for s in pre]
    known = {seat: e for seat, e in last.items() if seat in seat_index}
    for seat, e in known.items():
        s = merged[seat_index[seat]]
        if not isinstance(e.discord_id, UnsetType):
            s.discord_id = e.discord_id
        if not isinstance(e.placement, UnsetType):
            s.placement = e.placement
        if not isinstance(e.sub_out, UnsetType):
            if e.sub_out is None:
                s.is_sub, s.leaver = False, None
            else:
                s.is_sub, s.leaver = True, e.sub_out

    t_count = len({s.team for s in merged})
    if t_count < 2:
        out.append(
            Violation(
                Rule.TOO_FEW_TEAMS,
                None,
                f"The match has {t_count} team(s); rating needs at least two.",
            )
        )

    for seat, e in sorted(known.items()):
        s = merged[seat_index[seat]]
        if not isinstance(e.placement, UnsetType) and not (0 <= e.placement < t_count):
            out.append(
                Violation(
                    Rule.PLACEMENT_OUT_OF_RANGE,
                    seat,
                    f"Placement {e.placement} is outside 0-{t_count - 1}.",
                )
            )
        if e.quit is True and is_placeholder(s.discord_id):
            out.append(
                Violation(
                    Rule.QUIT_ON_PLACEHOLDER,
                    seat,
                    f"Seat {seat} is unassigned; quit cannot be flagged for it.",
                )
            )

    if not actor_is_staff:
        for value in sorted(_tie_growth(pre, merged)):
            holders = [
                seat
                for seat, k in sorted(seat_index.items())
                if merged[k].placement == value
            ]
            out.append(
                Violation(
                    Rule.DUPLICATE_PLACEMENT,
                    holders[0],
                    f"Placement {value} would be shared by seats "
                    f"{', '.join(map(str, holders))}; only staff may set a tie.",
                )
            )

    for team in sorted(_split_teams(merged) - _split_teams(pre)):
        out.append(
            Violation(
                Rule.TEAM_PLACEMENT_MISMATCH,
                None,
                f"Team {team}'s members would hold different placements.",
            )
        )

    for team in sorted(_pairing_faults(merged) - _pairing_faults(pre)):
        out.append(
            Violation(
                Rule.UNPAIRED_SUB,
                None,
                f"Team {team}'s substitutions are unpaired.",
            )
        )

    out.sort(key=lambda v: (v.seat if v.seat is not None else -1, v.rule.value))
    return out
