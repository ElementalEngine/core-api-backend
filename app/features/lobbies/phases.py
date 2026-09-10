"""The lobby's phase union and the reasons it can close.

One enum, previously owned by three modules: `projection.py` held
`settings`/`draft`/`complete`, `repository.py` held `cancelled`/`abandoned`,
`service.py` held `lobby`, and `bans` was defined nowhere at all. CP6 adds
`bans` and every transition between the six, so a fourth owner was one commit
away -- the same accretion section 4 item 107 records for the edition enum.

Forward-only (spec section 3), with one exception: `random` skips the draft
phase entirely, so `bans -> complete` is a legal edge (D193).
"""

from __future__ import annotations

LOBBY = "lobby"
SETTINGS = "settings"
BANS = "bans"
DRAFT = "draft"
COMPLETE = "complete"
CANCELLED = "cancelled"

# Ordered as a lobby travels them. `cancelled` is not in the sequence: it is
# reachable from any open phase and is terminal.
SEQUENCE = (LOBBY, SETTINGS, BANS, DRAFT, COMPLETE)
OPEN_PHASES = (LOBBY, SETTINGS, BANS, DRAFT)

# `closed_at` is set on both terminal phases, which is why the partial indexes
# test one field and cannot drift from `phase` (spec section 2).
TERMINAL_PHASES = (COMPLETE, CANCELLED)

CANCEL_BY_HOST = "host"
CANCEL_TIMEOUT = "timeout"
CANCEL_ABANDONED = "abandoned"
# D198. Bans are capped but the pool is not guaranteed: a lobby can ban
# itself past the point where every player gets one leader. The advance
# CANCELS rather than raising -- after D194 an advance can be triggered by a
# POLL, so raising would make every read a 500 with no route out.
CANCEL_NO_POOL = "no_pool"

SOURCE_COMMAND = "command"
SOURCE_ACTIVITY = "activity"

__all__ = [
    "BANS",
    "CANCELLED",
    "CANCEL_ABANDONED",
    "CANCEL_BY_HOST",
    "CANCEL_NO_POOL",
    "CANCEL_TIMEOUT",
    "COMPLETE",
    "DRAFT",
    "LOBBY",
    "OPEN_PHASES",
    "SEQUENCE",
    "SETTINGS",
    "SOURCE_ACTIVITY",
    "SOURCE_COMMAND",
    "TERMINAL_PHASES",
]
