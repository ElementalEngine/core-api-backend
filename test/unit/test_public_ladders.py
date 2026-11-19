from __future__ import annotations

from datetime import UTC, datetime

from bson.int64 import Int64

from app.features.public.schemas import Ladder, Ladders
from app.features.public.service import UNREGISTERED, ladder_rows

ANN = "100000000000000001"
BOB = "100000000000000002"
CY = "100000000000000003"
SEEN = datetime(2026, 11, 18, 20, 0, tzinfo=UTC)


def _doc(discord_id: str, mu: float) -> dict:
    return {
        "_id": Int64(discord_id),
        "mu": mu,
        "sigma": 90.5,
        "games": 12,
        "wins": 7,
        "first": 3,
        "lastModified": SEEN,
    }


def test_a_ladder_ranks_in_its_order_and_names_each_player():
    docs = [_doc(ANN, 1402.6), _doc(BOB, 1310.0), _doc(CY, 1250.2)]
    users = {
        ANN: {"discord_id": ANN, "display_name": "Ann", "discord_username": "ann_1"},
        BOB: {"discord_id": BOB, "display_name": " ", "discord_username": "bob_2"},
    }
    rows = ladder_rows(docs, users)
    assert [(r.rank, r.display_name, r.rating) for r in rows] == [
        (1, "Ann", 1403),
        (2, "bob_2", 1310),
        (3, UNREGISTERED, 1250),
    ]
    assert rows[0].last_active_at == SEEN


def test_no_discord_id_reaches_the_website():
    docs = [_doc(ANN, 1402.6), _doc(BOB, 1310.0)]
    users = {ANN: {"discord_id": ANN, "display_name": "Ann"}}
    payload = Ladders(
        updated_at=SEEN,
        edition="civ6",
        season_label="Season 7",
        season_started_at=SEEN,
        min_games=3,
        ladders=[Ladder(key="season-ffa", total=2, rows=ladder_rows(docs, users))],
    ).model_dump_json()
    assert ANN not in payload
    assert BOB not in payload
