"""Entry 9's integrity test: the authored table validates itself, no database.

D49 puts this test with the data rather than in a bot-side copy -- a test that
passes while the source is broken is worse than no test. Pure, per D59/D60.
"""

from app.features.civdata.seeds import (
    AUTHORING_ONLY,
    EDITIONS,
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
    # D96: civ6 is leaders only. Its civ tokens appear as the target of the
    # mapping, never as rows -- nobody holds civ6 civ display names.
    assert "civs" not in load_seed("civ6")


def test_civ7_leaders_are_civ_agnostic():
    seed = load_seed("civ7")
    leaders = seed["leaders"]
    assert len(leaders) == 33
    tokens = [row["token"] for row in leaders]
    assert len(set(tokens)) == len(tokens)
    for row in leaders:
        assert row["token"].startswith("LEADER_")
        assert row["name"]
        # A civ7 leader's CPL name does not vary by civilization -- the tuple
        # key get_cpl_name used was never built (D138).
        assert "civ" not in row


def test_civ7_civs():
    civs = load_seed("civ7")["civs"]
    assert len(civs) == 44
    tokens = [row["token"] for row in civs]
    assert len(set(tokens)) == len(tokens)
    for row in civs:
        assert row["token"].startswith("CIVILIZATION_")
        assert row["name"]
        assert row["age_pool"] in AGE_POOLS
        assert row["age_pool_source"] in POOL_SOURCES


def test_no_token_collides_within_an_edition():
    # The collection's unique index is {edition, token}, so leader and civ
    # tokens share one namespace per edition.
    for edition in EDITIONS:
        seed = load_seed(edition)
        tokens = [row["token"] for row in seed["leaders"]]
        tokens += [row["token"] for row in seed.get("civs", [])]
        assert len(set(tokens)) == len(tokens)


def test_to_documents_shape():
    docs = to_documents("civ6") + to_documents("civ7")
    assert len(docs) == 89 + 33 + 44
    for doc in docs:
        assert doc["edition"] in EDITIONS
        assert doc["kind"] in {"leader", "civ"}
        assert doc["leader_data_version"] == 1
        assert doc["token"]
    # {edition, token} is the collection's unique index.
    keys = [(doc["edition"], doc["token"]) for doc in docs]
    assert len(set(keys)) == len(keys)


def test_documents_carry_no_authoring_fields():
    # Provenance stays in the file. A served field nobody reads still lands
    # in Mite's generated types.
    docs = to_documents("civ6") + to_documents("civ7")
    leaked = {key for doc in docs for key in doc if key in AUTHORING_ONLY}
    assert leaked == set()
    # ...and it is still in the file, which is the record.
    assert all("civ_source" in row for row in load_seed("civ6")["leaders"])
