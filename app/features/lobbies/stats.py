"""What a completed lobby adds to `lobby_stats`: its bans only. Picks come from
the saves, and settings from the lobby documents."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

BANS = "bans"

PHASE_COMPLETE = "complete"


def contributions(lobby: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    """Per-token counters to increment, keyed by token."""
    if lobby.get("phase") != PHASE_COMPLETE:
        return {}
    counts: dict[str, dict[str, int]] = {}
    landed = lobby.get("bans") or {}
    for kind in ("leader", "civ"):
        for token in landed.get(kind) or []:
            counts.setdefault(token, {BANS: 0})[BANS] += 1
    return counts


__all__ = ["BANS", "contributions"]
