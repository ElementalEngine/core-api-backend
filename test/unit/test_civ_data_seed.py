import re

from app.core.constants import EDITIONS
from app.features.civdata.seeds import (
    AUTHORING_ONLY,
    load_seed,
    to_documents,
)

AGE_POOLS = {"AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN"}
CIV_SOURCES = {"observed", "inferred", "unmapped"}
POOL_SOURCES = {"observed", "mite", "inferred"}


def test_both_editions_load_and_share_one_version():
    versions = {e: load_seed(e)["leader_data_version"] for e in EDITIONS}
    assert len(set(versions.values())) == 1, f"version is global (D96): {versions}"


def test_civ6_leaders():
    seed = load_seed("civ6")
    leaders = seed["leaders"]
    assert seed["edition"] == "civ6"
    assert len(leaders) == 89
    tokens = [row["token"] for row in leaders]
    assert len(set(tokens)) == len(tokens)
    for row in leaders:
        assert row["token"].startswith("LEADER_")
        assert row["name"]
        assert row["civ_source"] in CIV_SOURCES
        if row["civ_source"] == "unmapped":
            assert row["civ"] is None
        else:
            assert row["civ"].startswith("CIVILIZATION_")


def test_civ6_has_no_civ_documents():
    assert "civs" not in load_seed("civ6")


def test_civ7_leaders_are_civ_agnostic():
    seed = load_seed("civ7")
    leaders = seed["leaders"]
    assert len(leaders) == 38
    tokens = [row["token"] for row in leaders]
    assert len(set(tokens)) == len(tokens)
    for row in leaders:
        assert row["token"].startswith("LEADER_")
        assert row["name"]
        assert "civ" not in row


def test_civ7_civs():
    civs = load_seed("civ7")["civs"]
    assert len(civs) == 50
    tokens = [row["token"] for row in civs]
    assert len(set(tokens)) == len(tokens)
    for row in civs:
        assert row["token"].startswith("CIVILIZATION_")
        assert row["name"]
        assert row["age_pool"] in AGE_POOLS
        assert row["age_pool_source"] in POOL_SOURCES


def test_every_name_can_be_a_discord_emoji_and_is_unique():
    """A portrait is found by its emoji name, on the bot and on the server."""
    for edition in EDITIONS:
        names = [doc["name"] for doc in to_documents(edition)]
        assert len(set(names)) == len(names), edition
        assert [n for n in names if not re.fullmatch(r"\w{2,32}", n, re.ASCII)] == []


def test_to_documents_shape():
    docs = to_documents("civ6") + to_documents("civ7")
    assert len(docs) == 89 + 38 + 50
    for doc in docs:
        assert doc["edition"] in EDITIONS
        assert doc["kind"] in {"leader", "civ"}
        assert doc["leader_data_version"] == 5
        assert doc["token"]
    # {edition, token} is the collection's unique index.
    keys = [(doc["edition"], doc["token"]) for doc in docs]
    assert len(set(keys)) == len(keys)


def test_documents_carry_no_authoring_fields():
    docs = to_documents("civ6") + to_documents("civ7")
    leaked = {key for doc in docs for key in doc if key in AUTHORING_ONLY}
    assert leaked == set()
    # ...and it is still in the file, which is the record.
    assert all("civ_source" in row for row in load_seed("civ6")["leaders"])


def test_every_row_has_a_display_name():
    """Clients show `display`; `name` is an emoji id name, not for people."""
    missing = [
        doc["token"]
        for edition in EDITIONS
        for doc in to_documents(edition)
        if not (doc.get("display") or "").strip()
    ]
    assert missing == []
    civ6 = load_seed("civ6")["leaders"]
    assert [r["token"] for r in civ6 if not (r.get("civ_display") or "").strip()] == []


def test_display_names_are_unique_where_players_choose_between_them():
    """Two tiles reading the same is a wrong pick waiting to happen."""
    for edition in EDITIONS:
        seed = load_seed(edition)
        for key in ("leaders", "civs"):
            rows = seed.get(key, [])
            labels = [(r["display"], r.get("civ_display")) for r in rows]
            assert len(set(labels)) == len(labels), (edition, key)
