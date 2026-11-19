"""One season counted for the website, mode by mode: the maps played, the
bans of its drafts, each leader's (and on Civ 7 each civ's) picks, wins and
first places, and on FFA the best picks per map by average place."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from app.features.public.schemas import (
    DraftBan,
    MapGames,
    MapPick,
    MapPicks,
    ModeStats,
    PickKind,
    PickRecord,
    StatsMode,
)
from app.features.stats.history import NO_MAP, NO_PICK, map_name

MODES: tuple[StatsMode, ...] = ("ffa", "teamer", "duel")


@dataclass
class _Picks:
    picks: int = 0
    wins: int = 0
    firsts: int = 0


@dataclass
class _Places:
    games: int = 0
    firsts: int = 0
    # Places counted from 1, summed.
    total: int = 0


def mode_stats(
    *,
    mode: StatsMode,
    edition: str,
    matches: Iterable[Mapping[str, Any]],
    setups: Iterable[Mapping[str, Any]],
    names: Mapping[str, str],
    min_games: int,
    top_n: int,
) -> ModeStats:
    """Civ 6 players pick a leader; Civ 7 players pick a leader and a civ."""
    kinds: tuple[PickKind, ...] = (
        ("leader", "civ") if edition == "civ7" else ("leader",)
    )
    games = 0
    maps: Counter[str] = Counter()
    picks: dict[PickKind, defaultdict[str, _Picks]] = {
        kind: defaultdict(_Picks) for kind in kinds
    }
    places: defaultdict[tuple[str, PickKind], defaultdict[str, _Places]] = defaultdict(
        lambda: defaultdict(_Places)
    )

    for match in matches:
        # Older reports stored the mode capitalised.
        if str(match.get("game_mode") or "").lower() != mode:
            continue
        games += 1
        map_ = map_name(match.get("map_type"))
        if map_ != NO_MAP:
            maps[map_] += 1
        for seat in match.get("players") or []:
            if seat.get("subbed_out"):
                continue
            # A win is a game whose rating went up, as the stat documents count it.
            won = float(seat.get("delta") or 0.0) > 0
            place = seat.get("placement")
            for kind in kinds:
                token = seat.get(kind)
                if not token or token in NO_PICK:
                    continue
                pick = picks[kind][token]
                pick.picks += 1
                pick.wins += won
                pick.firsts += place == 0
                if mode == "ffa" and map_ != NO_MAP and isinstance(place, int):
                    placed = places[(map_, kind)][token]
                    placed.games += 1
                    placed.firsts += place == 0
                    placed.total += place + 1

    bans: Counter[tuple[PickKind, str]] = Counter()
    for setup in setups:
        if setup.get("mode") != mode:
            continue
        for ban in setup.get("bans") or []:
            kind, token = ban.get("kind"), ban.get("token")
            if kind in ("leader", "civ") and token:
                bans[(kind, token)] += 1

    def name(token: str) -> str:
        return names.get(token) or token

    def records(tally: Mapping[str, _Picks]) -> list[PickRecord]:
        return [
            PickRecord(
                token=token,
                name=name(token),
                picks=t.picks,
                wins=t.wins,
                firsts=t.firsts,
                win_rate=round(t.wins / t.picks, 4) if t.picks >= min_games else None,
            )
            for token, t in sorted(tally.items(), key=lambda kv: (-kv[1].picks, kv[0]))
        ]

    def best(tally: Mapping[str, _Places]) -> list[MapPick]:
        ranked = sorted(
            ((token, p) for token, p in tally.items() if p.games >= min_games),
            key=lambda kv: (kv[1].total / kv[1].games, -kv[1].games, kv[0]),
        )
        return [
            MapPick(
                token=token,
                name=name(token),
                games=p.games,
                firsts=p.firsts,
                avg_placement=round(p.total / p.games, 2),
            )
            for token, p in ranked[:top_n]
        ]

    ordered_maps = sorted(maps.items(), key=lambda kv: (-kv[1], kv[0]))
    return ModeStats(
        mode=mode,
        games=games,
        maps=[MapGames(map=m, games=n) for m, n in ordered_maps],
        draft_bans=[
            DraftBan(token=token, name=name(token), kind=kind, bans=n)
            for (kind, token), n in sorted(
                bans.items(), key=lambda kv: (-kv[1], kv[0][0], kv[0][1])
            )
        ],
        leaders=records(picks["leader"]),
        civs=records(picks["civ"]) if "civ" in picks else [],
        map_picks=[
            MapPicks(map=m, kind=kind, rows=rows)
            for m, _ in ordered_maps
            for kind in kinds
            if (rows := best(places[(m, kind)]))
        ],
    )


__all__ = ["MODES", "mode_stats"]
