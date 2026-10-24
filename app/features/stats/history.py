"""A player's approved games: a page of the latest, and a summary by map
and by civ since their stats were last reset."""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from typing import Any

from pymongo import DESCENDING, AsyncMongoClient

from app.core.constants import COL_STAT_RESETS, GAMES_DB
from app.features.matches.repository import COL_VALIDATED_MATCHES
from app.features.stats.errors import InvalidStatsRequestError
from app.features.stats.schemas_v2 import (
    CivRecord,
    HistoryGame,
    HistoryPage,
    MapRecord,
    StatsSummary,
)

PAGE_SIZE = 20
MAX_PAGE = 49
TOP_CIVS = 5
# Civ and leader values that name nothing: older saves and unpicked seats.
_NO_PICK = frozenset({"", "Unknown", "None", "LEADER_NONE", "CIVILIZATION_NONE"})


def _validate(
    civ_version: str, game_type: str, discord_id: str
) -> tuple[str, bool, str]:
    version = (civ_version or "").strip().lower()
    kind = (game_type or "").strip().lower()
    did = (discord_id or "").strip()
    if version not in ("civ6", "civ7"):
        raise InvalidStatsRequestError("Invalid civ_version")
    if kind not in ("realtime", "cloud"):
        raise InvalidStatsRequestError("Invalid game_type")
    if not did.isdigit():
        raise InvalidStatsRequestError("Invalid discord_id")
    return version, kind == "cloud", did


def _seat(match: dict[str, Any], discord_id: str) -> dict[str, Any] | None:
    """The player's own seat; when they subbed in, the seat they finished in."""
    seats = [p for p in match.get("players", []) if p.get("discord_id") == discord_id]
    finished = [p for p in seats if not p.get("subbed_out")]
    return (finished or seats or [None])[0]


def _pick(match: dict[str, Any], seat: dict[str, Any]) -> str | None:
    """The token a civ6 player picks is the leader; a civ7 player picks a civ."""
    value = seat.get("civ") if match.get("game") == "civ7" else seat.get("leader")
    value = value or seat.get("civ")
    return None if value in _NO_PICK else value


def map_name(map_type: Any) -> str:
    """A map as the summary groups it: a saved script path counts as its file
    name, so `../../Assets/Maps/Lakes` and `Lakes` are one map."""
    name = re.split(r"[\\/]", str(map_type or "").strip())[-1]
    name = re.sub(r"\.(lua|civ6map|civ7map|xml)$", "", name, flags=re.IGNORECASE)
    return name or "Unknown"


def _won(seat: dict[str, Any]) -> bool:
    """A win is a game whose rating went up, as the stat documents count it."""
    return float(seat.get("delta") or 0.0) > 0


def _first(seat: dict[str, Any]) -> bool:
    return seat.get("placement") == 0


class MatchHistory:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._validated = client[GAMES_DB][COL_VALIDATED_MATCHES]
        self._resets = client[GAMES_DB][COL_STAT_RESETS]

    def _filter(self, version: str, is_cloud: bool, did: str) -> dict[str, Any]:
        # Older documents may carry no is_cloud; they were realtime reports.
        cloud: Any = True if is_cloud else {"$ne": True}
        return {"players.discord_id": did, "game": version, "is_cloud": cloud}

    async def page(
        self, *, civ_version: str, game_type: str, discord_id: str, page: int
    ) -> HistoryPage:
        version, is_cloud, did = _validate(civ_version, game_type, discord_id)
        if not 0 <= page <= MAX_PAGE:
            raise InvalidStatsRequestError(f"page must be 0 to {MAX_PAGE}")
        cursor = (
            self._validated.find(
                self._filter(version, is_cloud, did),
                {
                    "game": 1,
                    "game_mode": 1,
                    "map_type": 1,
                    "turn": 1,
                    "players": 1,
                },
            )
            .sort("_id", DESCENDING)
            .skip(page * PAGE_SIZE)
            .limit(PAGE_SIZE + 1)
        )
        docs = [d async for d in cursor]
        games = [g for d in docs[:PAGE_SIZE] if (g := _game(d, did)) is not None]
        return HistoryPage(
            discord_id=did,
            civ_version=version,
            game_type="cloud" if is_cloud else "realtime",
            page=page,
            page_size=PAGE_SIZE,
            has_more=len(docs) > PAGE_SIZE and page < MAX_PAGE,
            games=games,
        )

    async def summary(
        self, *, civ_version: str, game_type: str, discord_id: str
    ) -> StatsSummary:
        version, is_cloud, did = _validate(civ_version, game_type, discord_id)
        reset = await self._resets.find_one(
            {"civ_version": version, "is_cloud": is_cloud, "discord_id": did},
            sort=[("occurred_at", DESCENDING)],
        )
        since: datetime | None = reset["occurred_at"] if reset else None
        flt = self._filter(version, is_cloud, did)
        if since is not None:
            # Ratings move at approve, so the reset cuts on approved_at too.
            flt["approved_at"] = {"$gte": since}
        # Only the player's own seat comes back: the positional projection.
        cursor = self._validated.find(flt, {"game": 1, "map_type": 1, "players.$": 1})
        maps: Counter[str] = Counter()
        map_wins: Counter[str] = Counter()
        map_firsts: Counter[str] = Counter()
        civs: Counter[str] = Counter()
        civ_wins: Counter[str] = Counter()
        civ_firsts: Counter[str] = Counter()
        async for match in cursor:
            seat = _seat(match, did)
            if seat is None or seat.get("subbed_out"):
                continue
            won, first = _won(seat), _first(seat)
            map_type = map_name(match.get("map_type"))
            maps[map_type] += 1
            map_wins[map_type] += won
            map_firsts[map_type] += first
            if (pick := _pick(match, seat)) is not None:
                civs[pick] += 1
                civ_wins[pick] += won
                civ_firsts[pick] += first
        return StatsSummary(
            discord_id=did,
            civ_version=version,
            game_type="cloud" if is_cloud else "realtime",
            since=since,
            maps=[
                MapRecord(map_type=m, games=n, wins=map_wins[m], firsts=map_firsts[m])
                for m, n in sorted(maps.items(), key=lambda kv: (-kv[1], kv[0]))
            ],
            civs=[
                CivRecord(token=c, games=n, wins=civ_wins[c], firsts=civ_firsts[c])
                for c, n in sorted(civs.items(), key=lambda kv: (-kv[1], kv[0]))[
                    :TOP_CIVS
                ]
            ],
        )


def _game(match: dict[str, Any], did: str) -> HistoryGame | None:
    seat = _seat(match, did)
    if seat is None:
        return None
    players = match.get("players", [])
    finished = [p for p in players if not p.get("subbed_out")]
    teams = {p.get("team") for p in finished}
    return HistoryGame(
        match_id=str(match["_id"]),
        # When it was reported: the order the page is in.
        reported_at=match["_id"].generation_time,
        game_mode=str(match.get("game_mode") or ""),
        map_type=map_name(match.get("map_type")),
        turn=match.get("turn") if isinstance(match.get("turn"), int) else None,
        civ=seat.get("civ") if seat.get("civ") not in _NO_PICK else None,
        leader=seat.get("leader") if seat.get("leader") not in _NO_PICK else None,
        placement=seat.get("placement"),
        sides=len(teams),
        delta=round(float(seat.get("delta") or 0.0)),
        subbed_in=bool(seat.get("is_sub")),
        subbed_out=bool(seat.get("subbed_out")),
    )


__all__ = ["MAX_PAGE", "PAGE_SIZE", "MatchHistory"]
