from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from bson import ObjectId
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from trueskill import Rating

from app.core.coerce import as_float, as_int
from app.core.config import settings
from app.features.matches.approval import ApprovalService
from app.features.matches.errors import (
    ApprovingError,
    InvalidIDError,
    MatchServiceError,
    NotFoundError,
    ParseError,
)
from app.features.matches.models import (
    MatchModel,
    StatModel,
)
from app.features.matches.parsers import parse_civ6_save, parse_civ7_save
from app.features.matches.repository import MatchRepository
from app.features.matches.validation import is_placeholder
from app.features.ratings.repository import RatingsRepository
from app.features.ratings.skill import make_ts_env
from app.features.seasons.repository import SeasonsRepository

logger = logging.getLogger(__name__)


def _unrated(discord_id: str | None, index: int) -> StatModel:
    return StatModel(
        discord_id=discord_id,
        index=index,
        id=0,
        mu=settings.ts_mu,
        sigma=settings.ts_sigma,
        games=0,
        wins=0,
        first=0,
        subbedIn=0,
        subbedOut=0,
        civs={},
        leaders={},
    )


def _stat_of(discord_id: str, index: int, doc: dict[str, Any] | None) -> StatModel:
    """A player's stored rating, or the starting one before their first game."""
    if not doc:
        return StatModel(
            discord_id=discord_id,
            index=index,
            id=discord_id,
            mu=settings.ts_mu,
            sigma=settings.ts_sigma,
            games=0,
            wins=0,
            first=0,
            subbedIn=0,
            subbedOut=0,
            civs={},
            leaders={},
        )
    return StatModel(
        discord_id=discord_id,
        index=index,
        mu=as_float(doc.get("mu"), settings.ts_mu),
        sigma=as_float(doc.get("sigma"), settings.ts_sigma),
        games=as_int(doc.get("games"), 0),
        wins=as_int(doc.get("wins"), 0),
        first=as_int(doc.get("first"), 0),
        subbedIn=as_int(doc.get("subbedIn"), 0),
        subbedOut=as_int(doc.get("subbedOut"), 0),
        civs=dict(doc.get("civs", {})) if isinstance(doc.get("civs", {}), dict) else {},
        leaders=dict(doc.get("leaders", {}))
        if isinstance(doc.get("leaders", {}), dict)
        else {},
    )


class MatchService:
    def __init__(self, client: AsyncMongoClient):
        self.q = MatchRepository(client)
        self.ratings = RatingsRepository(client)
        self.seasons = SeasonsRepository(client)

    def _to_oid(self, match_id: str) -> ObjectId:
        if not ObjectId.is_valid(match_id):
            raise InvalidIDError("Invalid match id")
        return ObjectId(match_id)

    async def refusal(self, oid: ObjectId) -> Exception:
        """Why a guarded write on a pending match changed nothing."""
        if await self.q.find_pending_by_id(oid) is None:
            return NotFoundError("Match not found")
        return ApprovingError("This report is being approved.")

    def _parse_save(self, file_bytes: bytes) -> dict[str, Any]:
        if file_bytes.startswith(b"CIV6"):
            parser = parse_civ6_save
        elif file_bytes.startswith(b"CIV7"):
            parser = parse_civ7_save
        else:
            raise ParseError(
                f"Unrecognized save file format. starts with {file_bytes[:4]!r}"
            )
        try:
            data = parser(file_bytes, settings.civ_save_parser_version)
            logger.info(f"✅ 🔍 Parsed as {data.get('game')}")
            return data
        except Exception as e:
            raise ParseError(f"Parse attempt failed: {e}") from e

    async def seat_steam_id(self, discord_id: str) -> str | None:
        """A registered player's Steam id, or None for one who linked another
        platform. Someone not registered at all cannot take a seat."""
        player = await self.q.get_user_by_discord_id(discord_id)
        if not player:
            raise MatchServiceError(f"User {discord_id} not found")
        if player.get("steam_id"):
            return str(player["steam_id"])
        if player.get("linked_platform") == "steam" and player.get("linked_account_id"):
            return str(player["linked_account_id"])
        return None

    async def match_id_to_discord(self, match: MatchModel) -> MatchModel:
        """Each seat's player from its Steam id, read in one query; a Steam id
        nobody registered marks the seat unassigned as "-<steam id>"."""
        wanted = [
            p.steam_id
            for p in match.players
            if p.steam_id and p.steam_id != "-1" and not p.steam_id.startswith("-")
        ]
        if not wanted:
            return match
        users = await self.q.get_users_by_steam_ids(list(dict.fromkeys(wanted)))
        for player in match.players:
            sid = player.steam_id
            if sid in users:
                player.discord_id = str(users[sid]["discord_id"])
            elif sid and sid in wanted:
                player.discord_id = f"-{sid}"
        return match

    async def get_players_ranking(
        self,
        match: MatchModel,
        is_seasonal: bool = False,
        is_combined: bool = False,
        session: AsyncClientSession | None = None,
    ) -> list[StatModel]:
        """Every seat's rating in one scope, read in one query, in seat order."""
        ids = [p.discord_id for p in match.players]
        real = [i for i in ids if i is not None and not is_placeholder(i)]
        docs = (
            await self.ratings.get_player_stat_docs_batch(
                civ_version=match.game,
                is_seasonal=is_seasonal,
                match_type=match.game_mode,
                is_cloud=match.is_cloud,
                is_combined=is_combined,
                discord_ids=list(dict.fromkeys(real)),
                session=session,
            )
            if real
            else {}
        )
        return [
            _stat_of(i, n, docs.get(i))
            if i is not None and not is_placeholder(i)
            else _unrated(i, n)
            for n, i in enumerate(ids)
        ]

    def update_player_stats(
        self, match: MatchModel, players_ranking: list[StatModel], delta_value_name: str
    ) -> tuple[MatchModel, list[StatModel]]:
        teams_wo_subs = defaultdict(list)
        teams_with_sub_ins = defaultdict(list)
        for i, p in enumerate(match.players):
            if p.is_sub:
                teams_with_sub_ins[p.team].append((i, p))
            elif p.subbed_out:
                teams_wo_subs[p.team].append((i, p))
            else:
                teams_wo_subs[p.team].append((i, p))
                teams_with_sub_ins[p.team].append((i, p))
        team_wo_subs_states: list[list[StatModel]] = [
            [players_ranking[p_index_tuple[0]] for p_index_tuple in teams_wo_subs[team]]
            for team in teams_wo_subs
        ]
        team_with_sub_ins_states: list[list[StatModel]] = [
            [
                players_ranking[p_index_tuple[0]]
                for p_index_tuple in teams_with_sub_ins[team]
            ]
            for team in teams_with_sub_ins
        ]

        ts_teams_wo_subs = [
            [Rating(p.mu, p.sigma) for p in team] for team in team_wo_subs_states
        ]
        ts_teams_with_sub_ins = [
            [Rating(p.mu, p.sigma) for p in team] for team in team_with_sub_ins_states
        ]

        placements_wo_subs = [
            teams_wo_subs[team][0][1].placement for team in teams_wo_subs
        ]
        placements_with_sub_ins = [
            teams_with_sub_ins[team][0][1].placement for team in teams_with_sub_ins
        ]

        ts_wo_subs_env = make_ts_env()
        ts_with_sub_ins_env = make_ts_env()

        new_ts_wo_subs = ts_wo_subs_env.rate(ts_teams_wo_subs, ranks=placements_wo_subs)
        new_ts_with_sub_ins = ts_with_sub_ins_env.rate(
            ts_teams_with_sub_ins, ranks=placements_with_sub_ins
        )

        post: list[StatModel] = list(range(len(match.players)))
        for team_idx, team in enumerate(team_wo_subs_states):
            for player_index, player in enumerate(team):
                if match.players[player.index].is_sub:
                    raise ValueError(
                        "a sub reached the rating of the team without its subs"
                    )
                r = new_ts_wo_subs[team_idx][player_index]
                post[player.index] = StatModel(
                    discord_id=player.id,
                    index=player.index,
                    id=player.id,
                    mu=float(r.mu),
                    sigma=float(r.sigma),
                    games=player.games,
                    wins=player.wins,
                    first=player.first,
                    subbedIn=player.subbedIn,
                    subbedOut=player.subbedOut,
                    civs=player.civs,
                    leaders=player.leaders,
                )
        for team_idx, team in enumerate(team_with_sub_ins_states):
            for player_index, player in enumerate(team):
                if match.players[player.index].is_sub:
                    r = new_ts_with_sub_ins[team_idx][player_index]
                    post[player.index] = StatModel(
                        discord_id=player.id,
                        index=player.index,
                        id=player.id,
                        mu=float(r.mu),
                        sigma=float(r.sigma),
                        games=player.games,
                        wins=player.wins,
                        first=player.first,
                        subbedIn=player.subbedIn,
                        subbedOut=player.subbedOut,
                        civs=player.civs,
                        leaders=player.leaders,
                    )
        for i, p in enumerate(match.players):
            p_current_ranking = players_ranking[i]
            delta = (
                round(post[i].mu - p_current_ranking.mu) if p.discord_id != None else 0
            )
            if p.is_sub:
                # Subbed in player
                p.__setattr__(
                    delta_value_name, max(settings.min_points_for_subs, delta)
                )
            elif p.subbed_out:
                # Subbed out Player
                p.__setattr__(delta_value_name, delta if delta < 0 else 0)
            else:
                # Regular player
                p.__setattr__(delta_value_name, delta)
            post[i].mu = p_current_ranking.mu + getattr(p, delta_value_name)
        return match, post

    async def _recompute_deltas(self, match: MatchModel) -> MatchModel:
        """Recompute the delta/season_delta/combined_delta triple on match.players."""
        # Three reads, one per scope, at once: none depends on another.
        (
            players_ranking,
            players_season_ranking,
            players_combined_ranking,
        ) = await asyncio.gather(
            self.get_players_ranking(match),
            self.get_players_ranking(match, is_seasonal=True),
            self.get_players_ranking(match, is_combined=True),
        )

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(
            match, players_season_ranking, "season_delta"
        )
        match, _ = self.update_player_stats(
            match, players_combined_ranking, "combined_delta"
        )
        return match

    async def get_match(self, match_id: str) -> dict[str, Any]:
        oid = self._to_oid(match_id)
        doc = await self.q.find_pending_by_id(oid)
        if not doc:
            raise NotFoundError("Match not found")
        doc["match_id"] = str(doc.pop("_id"))
        return doc

    async def revert_match(self, match_id: str, actor_id: str) -> dict[str, Any]:
        return await ApprovalService(self).revert_match(match_id, actor_id)

    async def approve_match(
        self,
        match_id: str,
        approver_discord_id: str,
        *,
        expect: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await ApprovalService(self).approve_match(
            match_id, approver_discord_id, expect=expect
        )


__all__ = [
    "InvalidIDError",
    "MatchService",
    "MatchServiceError",
    "NotFoundError",
    "ParseError",
]
