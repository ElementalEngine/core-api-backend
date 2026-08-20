from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

SEED_DIR = Path(__file__).resolve().parent / "seed"
EDITIONS = ("civ6", "civ7")


def load_seed(edition: str) -> Dict[str, Any]:
    """Read one edition's authored civ-data file. No I/O beyond the file."""
    if edition not in EDITIONS:
        raise ValueError(f"unknown edition: {edition}")
    with (SEED_DIR / f"{edition}.json").open(encoding="utf-8") as fh:
        data: Dict[str, Any] = json.load(fh)
    return data


def to_documents(edition: str) -> List[Dict[str, Any]]:
    """Flatten one edition's seed into civ_data documents.

    Leaders and civs share the collection and the {edition, token} unique
    index; kind is what tells them apart. The version is stamped on every
    document so a served payload carries the version it was seeded from.
    """
    seed = load_seed(edition)
    version = seed["leader_data_version"]
    docs: List[Dict[str, Any]] = []
    for kind, key in (("leader", "leaders"), ("civ", "civs")):
        for row in seed.get(key, []):
            docs.append(
                {
                    **row,
                    "edition": edition,
                    "kind": kind,
                    "leader_data_version": version,
                }
            )
    return docs


__all__ = ["EDITIONS", "SEED_DIR", "load_seed", "to_documents"]
