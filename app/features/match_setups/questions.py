"""The settings questions, authored here rather than seeded: a stored ballot
names their ids, so they change only by release."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.core.constants import EDITIONS

SEED_DIR = Path(__file__).resolve().parent / "seed"
# A lobby setting its host leaves to the players, settled with the bans vote.
VOTE = "vote"


class UnknownBallot(ValueError):
    """No catalogue for that edition or game type."""


@lru_cache(maxsize=len(EDITIONS))
def _catalogue(edition: str) -> dict[str, Any]:
    if edition not in EDITIONS:
        raise UnknownBallot(f"unknown edition: {edition}")
    with (SEED_DIR / f"{edition}.json").open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
    return data


def settings_for(edition: str, game_type: str) -> list[dict[str, Any]]:
    """The game settings asked for a game type, without the draft mode."""
    catalogue = _catalogue(edition)
    try:
        settings = catalogue["settings"][catalogue["settings_for"][game_type]]
    except KeyError as exc:
        raise UnknownBallot(f"{edition} has no ballot for {game_type}") from exc
    return json.loads(json.dumps(settings))


def questions_for(edition: str, game_type: str) -> list[dict[str, Any]]:
    """Every question a game is set up with: its settings, then `draft_mode`."""
    asked = settings_for(edition, game_type)
    catalogue = _catalogue(edition)
    variant = catalogue["draft_mode_for"][game_type]
    asked.append(json.loads(json.dumps(catalogue["draft_mode"][variant])))
    return asked


def preset(
    questions: Sequence[Mapping[str, Any]], *, host_only: bool
) -> list[dict[str, Any]]:
    """The settings a game starts with: for a lobby, every default, and one with
    none put to the players; for a vote, only the host-only ones it never asks."""
    if host_only:
        return [
            {"id": q["id"], "value": q["default_option_id"], "by": "default"}
            for q in questions
            if q.get("host_only")
        ]
    return [
        {"id": q["id"], "value": q["default_option_id"] or VOTE, "by": "default"}
        for q in questions
    ]


def seeded_index(seed: str, question_id: str, count: int) -> int:
    """The tie-break, fixed by the vote and the question, so it can be checked."""
    digest = hashlib.sha256(f"{seed}:{question_id}".encode()).hexdigest()
    return int(digest[:8], 16) % count


def _backing(
    question: Mapping[str, Any], votes: Mapping[str, Sequence[str]], voters: set[str]
) -> dict[str, int]:
    return {
        o["id"]: len(voters & set(votes.get(o["id"], ()))) for o in question["options"]
    }


def winner(
    question: Mapping[str, Any],
    votes: Mapping[str, Sequence[str]],
    voters: set[str],
    seed: str,
) -> str:
    """The option most players reacted for. A tie, none reacting included, goes
    to one of the tied options by a fixed hash."""
    counts = _backing(question, votes, voters)
    most = max(counts.values(), default=0)
    tied = sorted(option for option, count in counts.items() if count == most)
    return tied[seeded_index(seed, question["id"], len(tied))]


def settled(
    vote: Mapping[str, Any],
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    questions: Sequence[Mapping[str, Any]],
    *,
    final: bool,
) -> list[dict[str, Any]]:
    """The settings won so far: one an option has a majority of the players
    for is won at once and stays won. The final count settles the rest."""
    voters = {p["discord_id"] for p in vote["players"]}
    need = len(voters) // 2 + 1
    chosen = {s["id"]: s for s in vote["settings"]}
    for q in questions:
        votes = setting_votes.get(q["id"], {})
        if q["id"] in chosen:
            continue
        if final or max(_backing(q, votes, voters).values(), default=0) >= need:
            value = winner(q, votes, voters, str(vote["_id"]))
            chosen[q["id"]] = {"id": q["id"], "value": value, "by": "vote"}
    return [chosen[q["id"]] for q in questions if q["id"] in chosen]


__all__ = [
    "VOTE",
    "UnknownBallot",
    "preset",
    "questions_for",
    "seeded_index",
    "settings_for",
    "settled",
    "winner",
]
