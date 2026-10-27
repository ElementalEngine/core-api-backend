"""The settings vote: each question goes to the option most players chose, a
tie broken on a hash, and an unvoted question to its default."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence, Set
from typing import Any


def seeded_index(seed: str, question_id: str, count: int) -> int:
    """The tie-break index, seeded on the game and the question."""
    digest = hashlib.sha256(f"{seed}:{question_id}".encode()).hexdigest()
    return int(digest[:8], 16) % count


def voted_settings(
    questions: Sequence[Mapping[str, Any]],
    votes: Mapping[str, Mapping[str, Sequence[str]]],
    voters: Set[str],
    seed: str,
) -> dict[str, str]:
    """The winning option per question. A voter may back several options of a
    question, and each counts once; anyone else's votes are ignored."""
    won: dict[str, str] = {}
    for question in questions:
        cast = votes.get(question["id"], {})
        counts = {
            option["id"]: len(voters & set(cast.get(option["id"], ())))
            for option in question["options"]
        }
        most = max(counts.values(), default=0)
        tied = [option for option, n in counts.items() if n == most]
        if most == 0:
            won[question["id"]] = question["default_option_id"]
        elif len(tied) == 1:
            won[question["id"]] = tied[0]
        else:
            won[question["id"]] = tied[seeded_index(seed, question["id"], len(tied))]
    return won


__all__ = ["seeded_index", "voted_settings"]
