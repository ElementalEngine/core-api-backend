"""Entry 9's evidence test: the seed must agree with every save on disk.

The civ6 leader->civ mapping cannot be checked against either old source --
neither held a CIVILIZATION_* token. Saves are the only evidence, so every
pair a fixture yields must match the seed and be marked observed. Adding a
fixture whose pair is absent turns new evidence into a red test rather than
a silent gap.
"""

import glob
import os

from app.features.civdata.seeds import load_seed
from app.features.matches.parsers import civ6, civ7

DATA = os.path.join(os.path.dirname(__file__), "..", "data")


def _saves(subdir, suffix):
    return sorted(glob.glob(os.path.join(DATA, subdir, f"*.{suffix}")))


def test_civ6_saves_agree_with_the_seed():
    mapping = {row["token"]: row for row in load_seed("civ6")["leaders"]}
    checked = 0
    for path in _saves("civ6TestSaves", "Civ6Save"):
        with open(path, "rb") as fh:
            root = civ6.parse(fh.read())
        for actor in root["parsed"]["CIVS"]:
            leader = actor["LEADER_NAME"]["data"]
            if leader == "LEADER_SPECTATOR":
                continue
            observed = actor["ACTOR_NAME"]["data"]
            row = mapping.get(leader)
            assert row is not None, f"{os.path.basename(path)}: {leader} not in seed"
            assert row["civ"] == observed, (
                f"{os.path.basename(path)}: {leader} -> {observed}, seed says {row['civ']}"
            )
            assert row["civ_source"] == "observed", (
                f"{leader} is observed in {os.path.basename(path)} "
                f"but marked {row['civ_source']}"
            )
            checked += 1
    assert checked > 0


def test_civ7_saves_agree_with_the_seed():
    seed = load_seed("civ7")
    leaders = {row["token"] for row in seed["leaders"]}
    pools = {row["token"]: row["age_pool"] for row in seed["civs"]}
    checked = 0
    for path in _saves("civ7TestSaves", "Civ7Save"):
        with open(path, "rb") as fh:
            parsed = civ7.parse_civ7_save(fh.read())
        age = parsed["age"]
        for player in parsed["players"]:
            assert player["leader"] in leaders, f"{player['leader']} not in seed"
            assert player["civ"] in pools, f"{player['civ']} not in seed"
            # A civ is only playable in its own age, so the age the save was
            # taken in is the pool that civ belongs to.
            assert pools[player["civ"]] == age, (
                f"{player['civ']} is {pools[player['civ']]} in the seed, "
                f"observed in {age}"
            )
            checked += 1
    assert checked > 0
