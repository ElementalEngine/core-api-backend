"""Resolving the settings ballot.

⚠ **Carried from Mite's `services/voting/domain/`, not reinvented** (D191).
Changing how settings resolve changes game outcomes, which puts it in D66's
class -- it belongs to the league, not to a migration.

Three rules, each with a way to go wrong that no green test would show:

**Plurality per question**, and **approval** where `max_selections > 1`. A
seat approving two maps casts two votes, not half a vote each -- v1 stores
them `a|b` and counts each.

**A question not answered by EVERY voter locks to its default.** Not to the
plurality of those who did answer. Stragglers decide nothing, and a settings
phase that advances on timer expiry (spec section 7) therefore locks every
unanswered question to its default.

**Ties break deterministically** on `sha256(lobby_id:question_id)`, so a
disputed tie recomputes forever -- better than civup's seeded random. ⚠ The
tied options are SORTED before indexing: without that the "deterministic"
answer depends on dict iteration order, which is stable within a process and
not across a restart, destroying the one property the tie-break exists for.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

BALLOT = "ballot"
SELECTION_SEPARATOR = "|"


def seeded_index(lobby_id: str, question_id: str, count: int) -> int:
    """v1's tie-break, seeded on the lobby rather than the session."""
    digest = hashlib.sha256(f"{lobby_id}:{question_id}".encode()).hexdigest()
    return int(digest[:8], 16) % count


def resolve_settings(
    seats: Sequence[Mapping[str, Any]],
    questions: Sequence[Mapping[str, Any]],
    lobby_id: str,
) -> dict[str, str]:
    """The winning option per question. Every question gets an answer."""
    voters = [seat for seat in seats if seat.get("discord_id")]
    resolved: dict[str, str] = {}

    for question in questions:
        question_id = question["id"]
        # ⚠ Restricted to options the catalogue still offers. The catalogue
        # changes by release (D192) and a lobby can be mid-vote across one, so
        # a retired option must not win on votes cast before it went away.
        offered = {option["id"] for option in question["options"]}
        counts: Counter[str] = Counter()
        answered = 0

        for seat in voters:
            raw = (seat.get(BALLOT) or {}).get(question_id)
            if not raw:
                continue
            answered += 1
            for choice in str(raw).split(SELECTION_SEPARATOR):
                if choice in offered:
                    counts[choice] += 1

        if answered < len(voters) or not counts:
            resolved[question_id] = question["default_option_id"]
            continue

        most = max(counts.values())
        tied = sorted(option for option, votes in counts.items() if votes == most)
        resolved[question_id] = (
            tied[0]
            if len(tied) == 1
            else tied[seeded_index(lobby_id, question_id, len(tied))]
        )

    return resolved


__all__ = ["resolve_settings", "seeded_index"]
