"""The settings questions, authored here rather than seeded: a stored ballot
names their ids, so they change only by release."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.core.constants import EDITIONS

SEED_DIR = Path(__file__).resolve().parent / "seed"


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


__all__ = ["UnknownBallot", "questions_for", "settings_for"]
