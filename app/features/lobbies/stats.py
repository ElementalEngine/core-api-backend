"""What a finished lobby contributes to `lobby_stats`.

⚠ **`pool_appearances` is the denominator and cannot be backfilled** (D10):
pick rate is picks divided by times offered. A token nobody was ever shown
must not count as a refusal, and a token assigned without a choice must not
count as a pick.

Three shapes, because the draft has three:

- **`random`** contributes NEITHER picks nor appearances. Nobody chose and
  nobody was offered; counting an assignment as a pick makes pick rate noise.
  Its bans still count -- those were real votes.
- **`cwc`** reads `teams[]` for picks and the lobby's ONE shared pool for
  appearances (D199, D201).
- everything else reads `seats[].pick` and each seat's own disjoint pool.

⚠ **Only a completed lobby contributes.** A cancelled draft dealt real pools
and landed real bans, but no game happened, and stats describe what people
play.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

PICKS = "picks"
BANS = "bans"
POOL_APPEARANCES = "pool_appearances"

DRAFT_RANDOM = "random"
DRAFT_CWC = "cwc"
PHASE_COMPLETE = "complete"


def contributions(lobby: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    """Per-token counters to increment, keyed by token.

    The caller supplies `(season_id, edition, game_type)` from the same
    lobby; only the token varies within one document.
    """
    counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {PICKS: 0, BANS: 0, POOL_APPEARANCES: 0}
    )
    if lobby.get("phase") != PHASE_COMPLETE:
        return {}

    landed = lobby.get("bans") or {}
    for kind in ("leader", "civ"):
        for token in landed.get(kind) or []:
            counts[token][BANS] += 1

    mode = (lobby.get("settings") or {}).get("draft_mode")
    if mode == DRAFT_RANDOM:
        return dict(counts)

    seats = lobby.get("seats") or []
    if mode == DRAFT_CWC:
        for team in lobby.get("teams") or []:
            for token in [*team.get("leaders", []), *team.get("civs", [])]:
                counts[token][PICKS] += 1
        # ⚠ Once per lobby, not once per captain: the pool is shared, so a
        # token was offered to the draft once however many turns it survived.
        for field in ("pool", "civ_pool"):
            for token in lobby.get(field) or []:
                counts[token][POOL_APPEARANCES] += 1
        return dict(counts)

    for seat in seats:
        if not seat.get("discord_id"):
            continue
        for field in ("pick", "civ_pick"):
            token = seat.get(field)
            if token:
                counts[token][PICKS] += 1
        for field in ("pool", "civ_pool"):
            for token in seat.get(field) or []:
                counts[token][POOL_APPEARANCES] += 1
    return dict(counts)


__all__ = ["BANS", "PICKS", "POOL_APPEARANCES", "contributions"]
