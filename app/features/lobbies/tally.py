"""Resolves the settings vote: plurality, or approval where a question allows
more than one. Unanswered questions take their default; ties break on a hash."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

BALLOT = "ballot"
SELECTION_SEPARATOR = "|"


def seeded_index(lobby_id: str, question_id: str, count: int) -> int:
    """The tie-break index, seeded on the lobby and the question."""
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
