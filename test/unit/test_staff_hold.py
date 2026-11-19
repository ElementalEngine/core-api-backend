from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.features.matches.models import ContestReport, MatchModel
from app.features.matches.router_v2 import _require_player
from app.features.matches.validation import approval_refusal

ANN = "100000000000000001"
STAFF = "100000000000000009"


def _match(contests: list[ContestReport]) -> MatchModel:
    seat = {"civ": "CIVILIZATION_ROME", "leader": "LEADER_TRAJAN", "team": 0}
    return MatchModel(
        game="civ6",
        turn=100,
        map_type="Pangaea",
        game_mode="duel",
        is_cloud=False,
        players=[
            {**seat, "discord_id": ANN, "placement": 0},
            {**seat, "discord_id": "100000000000000002", "team": 1, "placement": 1},
        ],
        parser_version="1",
        discord_messages_id_list=["m0"],
        save_file_hash="h",
        reporter_discord_id=ANN,
        contest_report_list=contests,
    )


def test_a_staff_hold_stops_an_approve_and_a_player_contest_does_not():
    player = ContestReport(contestor_discord_id=ANN, reason="wrong order")
    hold = ContestReport(contestor_discord_id=STAFF, reason="smurf?", by_staff=True)
    assert approval_refusal(_match([player])) is None
    refusal = approval_refusal(_match([player, hold]))
    assert refusal is not None and refusal.startswith(
        "Held by staff for review: smurf?"
    )


def test_staff_contest_any_report_and_others_only_their_own_game():
    doc = {"players": [{"discord_id": ANN}]}
    _require_player(doc, ANN, is_staff=False)
    _require_player(doc, STAFF, is_staff=True)
    with pytest.raises(HTTPException) as exc_info:
        _require_player(doc, STAFF, is_staff=False)
    assert exc_info.value.status_code == 403


def test_a_report_stored_before_holds_reads_as_not_held():
    assert ContestReport(contestor_discord_id=ANN, reason="x").by_staff is False
