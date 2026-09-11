"""The settings ballot's question catalogue.

**Authored data, not a collection.** `civdata` seeds Mongo because its rows
are queried and versioned; these are read whole, never queried by field, and a
stored ballot references their ids by name. A catalogue editable without a
deploy could retire a question a live lobby has already voted on, so this
changes by review and release rather than by a seed run.

**Carried from Mite's `civ6-voting.config.ts` and `civ7-voting.config.ts`,
with one deliberate correction.** Every title, label, emoji, default and cap
was verified against that source. Duel's draft modes were NOT carried: v1
hands duel the FFA list -- snake and blind included -- and `lobbies-domain-spec`
section 5 said `standard` and CWC. Both are wrong; duel offers `standard` and
`random`.

Settings vary by game type in civ6 and not in civ7, while draft modes vary by
game type in both. Two independent maps, because they vary independently.

Governed by D192, D193.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

SEED_DIR = Path(__file__).resolve().parent / "seed"
EDITIONS = ("civ6", "civ7")


class UnknownBallot(ValueError):
    """No catalogue for that edition or game type."""


@lru_cache(maxsize=len(EDITIONS))
def _catalogue(edition: str) -> dict[str, Any]:
    if edition not in EDITIONS:
        raise UnknownBallot(f"unknown edition: {edition}")
    with (SEED_DIR / f"{edition}.json").open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
    return data


def questions_for(edition: str, game_type: str) -> list[dict[str, Any]]:
    """Every question a seat votes on, settings first and `draft_mode` last."""
    catalogue = _catalogue(edition)
    try:
        settings = catalogue["settings"][catalogue["settings_for"][game_type]]
    except KeyError as exc:
        raise UnknownBallot(f"{edition} has no ballot for {game_type}") from exc
    # A teamer's draft mode is chosen when the lobby is created, so it is not
    # on the ballot; every other game type votes on it.
    variant = catalogue["draft_mode_for"].get(game_type)
    asked = [*settings]
    if variant is not None:
        asked.append(catalogue["draft_mode"][variant])
    return json.loads(json.dumps(asked))


def question_ids(edition: str, game_type: str) -> set[str]:
    """The ids a ballot may name. Anything else is a 400, not a silent drop."""
    return {question["id"] for question in questions_for(edition, game_type)}


__all__ = ["EDITIONS", "UnknownBallot", "question_ids", "questions_for"]
