from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

SEED_DIR = Path(__file__).resolve().parent / "seed"
EDITIONS = ("civ6", "civ7")


def load_seed(edition: str) -> Dict[str, Any]:
    """Read one edition's authored civ-data file. No I/O beyond the file."""
    if edition not in EDITIONS:
        raise ValueError(f"unknown edition: {edition}")
    with (SEED_DIR / f"{edition}.json").open(encoding="utf-8") as fh:
        data: Dict[str, Any] = json.load(fh)
    return data


__all__ = ["EDITIONS", "SEED_DIR", "load_seed"]
