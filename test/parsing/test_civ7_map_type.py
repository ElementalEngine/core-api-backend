# D83 Bug 1: extract_map_type left map_type unbound when no en_US locale
# existed, raising UnboundLocalError -> 500 on upload. The inner break also
# exited only the inner loop, so on a multi-entry blob the LAST match won.
#
# D134: the fallback is the entry KEY, never another locale. map_type feeds
# the composition hash (service.py:351), so a fr_FR fallback would hash the
# same game differently from an en_US client and split dedup by language.
import json

import pytest

from app.features.matches.parsers import civ7


def _root(blob):
    return {"map": {"value": json.dumps(blob)}}


def test_english_locale_is_preferred():
    root = _root(
        {
            "LOC_MAP_CONTINENTS_NAME": [
                {"locale": "fr_FR", "text": "Continents FR"},
                {"locale": "en_US", "text": "Continents"},
            ]
        }
    )
    assert civ7.extract_map_type(root) == "Continents"


def test_no_english_falls_back_to_the_key_not_another_locale():
    root = _root(
        {
            "LOC_MAP_ARCHIPELAGO_NAME": [
                {"locale": "fr_FR", "text": "Archipel"},
                {"locale": "de_DE", "text": "Inselgruppe"},
            ]
        }
    )
    assert civ7.extract_map_type(root) == "LOC_MAP_ARCHIPELAGO_NAME"


def test_first_entry_wins_when_several_carry_english():
    root = _root(
        {
            "LOC_MAP_PANGAEA_NAME": [{"locale": "en_US", "text": "Pangaea"}],
            "LOC_MAP_SHUFFLE_NAME": [{"locale": "en_US", "text": "Shuffle"}],
        }
    )
    assert civ7.extract_map_type(root) == "Pangaea"


def test_empty_blob_raises():
    with pytest.raises(ValueError, match="extract_map_type"):
        civ7.extract_map_type(_root({}))
