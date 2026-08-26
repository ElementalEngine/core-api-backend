from __future__ import annotations

from typing import Any

from app.core.coerce import as_int

# One approve writes three stat documents. Cloud games have no season row --
# they are lifetime and combined only.
LIFETIME = (False, False)
SEASON = (True, False)
COMBINED = (False, True)


def stat_legs(*, is_cloud: bool) -> tuple[tuple[bool, bool], ...]:
    """(is_seasonal, is_combined) for every stat document a match writes."""
    if is_cloud:
        return (LIFETIME, COMBINED)
    return (LIFETIME, SEASON, COMBINED)


def read_entry(entry: Any) -> tuple[int, int]:
    """games and wins out of either stored shape: 3, or {games: 3, wins: 1}."""
    if isinstance(entry, dict):
        return as_int(entry.get("games", 0), 0), as_int(entry.get("wins", 0), 0)
    if isinstance(entry, int):
        return entry, 0
    return 0, 0


def bump(tally: dict[str, Any], key: str, *, won: bool, step: int) -> dict[str, Any]:
    """Move one key by step, normalising whatever shape was there.

    Reverts clamp at zero so a revert can never write a negative count.
    """
    games, wins = read_entry(tally.get(key))
    games += step
    if won:
        wins += step
    if step < 0:
        games = max(0, games)
        wins = max(0, wins)
    tally[key] = {"games": games, "wins": wins}
    return tally


def is_rated(discord_id: Any) -> bool:
    """Placeholder ids are skipped by the stat write and the ledger both."""
    return bool(discord_id) and not str(discord_id).startswith("-")


__all__ = ["bump", "is_rated", "read_entry", "stat_legs"]
