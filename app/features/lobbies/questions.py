"""The settings questions, authored here rather than seeded: a stored ballot
names their ids, so they change only by release."""

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


__all__ = ["EDITIONS", "UnknownBallot", "questions_for"]
