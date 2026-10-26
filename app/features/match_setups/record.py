"""How a game was set up, one `match_setups` document per game, for reporting
on the modes played, the leaders and civs banned and the settings chosen."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.features.match_setups.bans import BAN_KINDS
from app.features.match_setups.schemas import DraftRequest


def draft_setup(
    request: DraftRequest, deal: Mapping[str, Any], season_id: Any, now: datetime
) -> dict[str, Any]:
    """/draft's setup: who drafted what, in which season, banning what."""
    return {
        "command": "draft",
        "edition": request.edition,
        "mode": request.game_type,
        "draft_mode": "standard",
        "season_id": season_id,
        "host_id": request.host_id,
        "bans": [
            {"token": token, "kind": kind, "by": "host"}
            for kind in BAN_KINDS
            for token in deal["bans"][kind]
        ],
        "created_at": now,
    }


__all__ = ["draft_setup"]
