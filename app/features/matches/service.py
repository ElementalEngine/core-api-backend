from __future__ import annotations
import asyncio
import hashlib
import logging
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.int64 import Int64
from motor.motor_asyncio import AsyncIOMotorClient
from trueskill import Rating

from app.core.config import settings
from app.features.matches.models import MatchModel, PlayerModel, StatModel, ContestReport
from app.features.matches.repository import MatchRepository
from app.features.matches.parsers import parse_civ6_save, parse_civ7_save
from app.features.stats.skill import make_ts_env
from app.features.matches.utils import get_cpl_name

logger = logging.getLogger(__name__)

approve_lock = asyncio.Lock()
RANKING_CONCURRENCY_LIMIT = 8  


def _as_float(value: Any, default: float) -> float:
    """Safely coerce Mongo values (None/Decimal128/Int64/str) to float."""
    if value is None:
        return default
    try:
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int) -> int:
    """Safely coerce Mongo values (None/Decimal128/Int64/str) to int."""
    if value is None:
        return default
    try:
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()
        if isinstance(value, str):
            return int(float(value))
        return int(value)
    except (TypeError, ValueError):
        return default
class NotFoundError(Exception):
    pass

class MatchServiceError(Exception):
    pass

class InvalidIDError(MatchServiceError):
    pass

class ParseError(MatchServiceError):
    pass


def _require_int(value: Any, field_name: str) -> int:
    """Parse a client-supplied numeric field; bad input becomes a 400 instead of a bare 500."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        raise MatchServiceError(f"{field_name} must be a whole number, got {value!r}")

class MatchService:
    def __init__(self, client: AsyncIOMotorClient):
        self.q = MatchRepository(client)

    def _to_oid(self, match_id: str) -> ObjectId:
        if not ObjectId.is_valid(match_id):
            raise InvalidIDError("Invalid match id")
        return ObjectId(match_id)

    def _parse_save(self, file_bytes: bytes) -> Dict[str, Any]:
        if file_bytes.startswith(b'CIV6'):
            parser = parse_civ6_save
        elif file_bytes.startswith(b'CIV7'):
            parser = parse_civ7_save
        else:
            raise ParseError(f"Unrecognized save file format. starts with {file_bytes[:4]!r}")
        try:
            data = parser(file_bytes, settings.civ_save_parser_version)
            logger.info(f"✅ 🔍 Parsed as {data.get('game')}")
            return data
        except Exception as e:
            raise ParseError(f"⚠️ Parse attempt failed: {e}")

    async def discord_to_steam_id(self, discord_id: str) -> str:
        player = await self.q.get_user_by_discord_id(discord_id)
        if not player:
            raise MatchServiceError(f"User {discord_id} not found")
        steam_id = player.get("steam_id")
        if steam_id:
            return str(steam_id)
        if player.get("linked_platform") == "steam" and player.get("linked_account_id"):
            return str(player["linked_account_id"])
        raise MatchServiceError(f"User {discord_id} does not have a linked Steam account")

    async def steam_to_discord_id(self, steam_id: str) -> str:
        player = await self.q.get_user_by_steam_id(steam_id)
        if not player:
            return f"-{steam_id}"
        return str(player["discord_id"])

    async def match_id_to_discord(self, match: MatchModel) -> MatchModel:
        for player in match.players:
            if player.steam_id and player.steam_id != '-1' and player.steam_id.startswith("-") == False:
                player.discord_id = await self.steam_to_discord_id(player.steam_id)
        return match

    async def get_player_ranking(
        self,
        match: MatchModel,
        discord_id: Optional[str],
        player_index: int,
        is_seasonal: bool = False,
        is_combined: bool = False,
    ) -> StatModel:
        # Missing / placeholder IDs
        if not discord_id or discord_id in ("-1", "-2") or discord_id.startswith("-"):
            return StatModel(
                discord_id=discord_id,
                index=player_index,
                id=0,
                mu=settings.ts_mu,
                sigma=settings.ts_sigma,
                games=0,
                wins=0,
                first=0,
                subbedIn=0,
                subbedOut=0,
                civs={},
            )

        doc = await self.q.get_player_stat_doc(
            civ_version=match.game,
            is_seasonal=is_seasonal,
            match_type=match.game_mode,
            is_cloud=match.is_cloud,
            is_combined=is_combined,
            discord_id=discord_id,
        )

        if not doc:
            return StatModel(
                discord_id=discord_id,
                index=player_index,
                id=discord_id,
                mu=settings.ts_mu,
                sigma=settings.ts_sigma,
                games=0,
                wins=0,
                first=0,
                subbedIn=0,
                subbedOut=0,
                civs={},
            )

        return StatModel(
            discord_id=discord_id,
            index=player_index,
            mu=_as_float(doc.get("mu"), settings.ts_mu),
            sigma=_as_float(doc.get("sigma"), settings.ts_sigma),
            games=_as_int(doc.get("games"), 0),
            wins=_as_int(doc.get("wins"), 0),
            first=_as_int(doc.get("first"), 0),
            subbedIn=_as_int(doc.get("subbedIn"), 0),
            subbedOut=_as_int(doc.get("subbedOut"), 0),
            civs=dict(doc.get("civs", {})) if isinstance(doc.get("civs", {}), dict) else {},
        )

    async def get_players_ranking(
        self,
        match: MatchModel,
        is_seasonal: bool = False,
        is_combined: bool = False,
    ) -> List[StatModel]:
        if not match.players:
            return []

        # Bounded concurrency, preserve order
        sem = asyncio.Semaphore(min(RANKING_CONCURRENCY_LIMIT, len(match.players)))

        async def _one(i: int, p: PlayerModel) -> StatModel:
            async with sem:
                return await self.get_player_ranking(match, p.discord_id, i, is_seasonal, is_combined)

        tasks = [asyncio.create_task(_one(i, p)) for i, p in enumerate(match.players)]
        return await asyncio.gather(*tasks)

    def update_player_stats(
        self, match: MatchModel, players_ranking: List[StatModel], delta_value_name: str
    ) -> tuple[MatchModel, List[StatModel]]:
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
        team_wo_subs_states: List[List[StatModel]] = [
            [players_ranking[p_index_tuple[0]] for p_index_tuple in teams_wo_subs[team]] for team in teams_wo_subs
        ]
        team_with_sub_ins_states: List[List[StatModel]] = [
            [players_ranking[p_index_tuple[0]] for p_index_tuple in teams_with_sub_ins[team]] for team in teams_with_sub_ins
        ]

        ts_teams_wo_subs = [[Rating(p.mu, p.sigma) for p in team] for team in team_wo_subs_states]
        ts_teams_with_sub_ins = [[Rating(p.mu, p.sigma) for p in team] for team in team_with_sub_ins_states]
        
        placements_wo_subs = [teams_wo_subs[team][0][1].placement for team in teams_wo_subs]
        placements_with_sub_ins = [teams_with_sub_ins[team][0][1].placement for team in teams_with_sub_ins]

        ts_wo_subs_env = make_ts_env()
        ts_with_sub_ins_env = make_ts_env()
        
        new_ts_wo_subs = ts_wo_subs_env.rate(ts_teams_wo_subs, ranks=placements_wo_subs)
        new_ts_with_sub_ins = ts_with_sub_ins_env.rate(ts_teams_with_sub_ins, ranks=placements_with_sub_ins)

        post: List[StatModel] = list(range(len(match.players)))
        for team_idx, team in enumerate(team_wo_subs_states):
            for player_index, player in enumerate(team):
                if match.players[player.index].is_sub:
                    raise ValueError("This should not happen: player is a sub but being processed in wo_subs team.")
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
                    )
        for i, p in enumerate(match.players):
            p_current_ranking = players_ranking[i]
            delta = round(post[i].mu - p_current_ranking.mu) if p.discord_id != None else 0
            if p.is_sub:
                # Subbed in player
                p.__setattr__(delta_value_name, max(settings.min_points_for_subs, delta))
            elif p.subbed_out:
                # Subbed out Player
                p.__setattr__(delta_value_name, delta if delta < 0 else 0)
            else:
                # Regular player
                p.__setattr__(delta_value_name, delta)
            post[i].mu = p_current_ranking.mu + getattr(p, delta_value_name)
        return match, post

    async def _recompute_deltas(self, match: MatchModel) -> MatchModel:
        """Recompute the delta/season_delta/combined_delta triple on match.players.

        This spine (three ranking loads + three rating passes) used to be copy-pasted
        in six methods.
        """
        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")
        return match

    def _player_delta_changes(self, match: MatchModel) -> Dict[str, Any]:
        """$set keys for every player's recomputed deltas."""
        changes: Dict[str, Any] = {}
        for i, player in enumerate(match.players):
            changes[f"players.{i}.delta"] = player.delta
            changes[f"players.{i}.season_delta"] = player.season_delta
            changes[f"players.{i}.combined_delta"] = player.combined_delta
        return changes

    async def _reload_pending(self, oid: ObjectId) -> Dict[str, Any]:
        """Re-fetch a pending match and rename _id -> match_id for the response."""
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    def _shift_civ_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
        step: int,
    ) -> Dict[str, Any]:
        # Normalize civ naming
        civ_name = get_cpl_name(match.game, player.civ, getattr(player, "leader", None))

        civs = dict(existing_civs) if isinstance(existing_civs, dict) else {}

        entry = civs.get(civ_name)

        # Backwards compatibility:
        # - legacy shape: {"DutchWilhelmina": 3}
        # - new shape: {"DutchWilhelmina": {"games": 3, "wins": 1}}
        if isinstance(entry, dict):
            games = _as_int(entry.get("games", 0), 0)
            wins = _as_int(entry.get("wins", 0), 0)
        elif isinstance(entry, int):
            games = entry
            wins = 0
        else:
            games = 0
            wins = 0

        games += step
        if player.delta > 0:
            wins += step
        if step < 0:
            games = max(0, games)
            wins = max(0, wins)

        civs[civ_name] = {"games": games, "wins": wins}
        return civs

    def update_existing_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._shift_civ_stat(match, player, existing_civs, +1)

    def revert_existing_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._shift_civ_stat(match, player, existing_civs, -1)

    def _build_stat_doc(
        self,
        *,
        discord_id: str,
        pre: StatModel,
        player: PlayerModel,
        mu: float,
        sigma: float,
        delta_value: float,
        civs: Dict[str, Any],
        step: int,
    ) -> Dict[str, Any]:
        """One stats document for approve (step=+1) or revert (step=-1).

        Reverted counters clamp at 0 so a revert can never write negative stats.
        """

        def shift(current: int, hit: bool) -> int:
            value = current + (step if hit else 0)
            return max(0, value) if step < 0 else value

        return {
            "_id": Int64(discord_id),
            "mu": float(mu),
            "sigma": float(sigma),
            "games": shift(int(pre.games), True),
            "wins": shift(int(pre.wins), delta_value > 0),
            "first": shift(int(pre.first), player.placement == 0),
            "subbedIn": shift(int(pre.subbedIn), player.is_sub),
            "subbedOut": shift(int(pre.subbedOut), player.subbed_out),
            "civs": civs,
            "lastModified": datetime.now(UTC),
        }

    async def create_from_save(
        self, file_bytes: bytes, reporter_discord_id: str, is_cloud: bool, discord_message_id: str
    ) -> Dict[str, Any]:
        parsed = self._parse_save(file_bytes)

        # Stable hash (preserves current behavior)
        m = hashlib.sha256()
        unique_data = ",".join(
            [parsed["game"], parsed["map_type"]]
            + [p["civ"] + (p.get("leader") or "") for p in parsed["players"]]
        )
        m.update(unique_data.encode("utf-8"))
        save_file_hash = m.hexdigest()

        existing = await self.q.find_pending_by_hash(save_file_hash)
        if existing:
            match_id = str(existing["_id"])
            del existing["_id"]
            existing["match_id"] = match_id
            existing["repeated"] = True
            return existing

        parsed["save_file_hash"] = save_file_hash
        parsed["repeated"] = False
        parsed["reporter_discord_id"] = reporter_discord_id
        parsed["is_cloud"] = is_cloud
        parsed["discord_messages_id_list"] = [discord_message_id]
        parsed["contest_report_list"] = []

        match = MatchModel(**parsed)
        match = await self.match_id_to_discord(match)
        match = await self._recompute_deltas(match)

        inserted_id = await self.q.insert_pending_match(match.dict())
        return {"match_id": str(inserted_id), **match.dict()}

    async def append_discord_message_id_list(
        self, match_id: str, discord_message_id_list: list[str]
    ) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        current_list = res.get("discord_messages_id_list", [])
        updated_list = current_list + discord_message_id_list

        await self.q.update_pending_match_set(oid, {"discord_messages_id_list": updated_list})
        return await self._reload_pending(oid)

    async def get_match(self, match_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        doc = await self.q.find_pending_by_id(oid)
        if not doc:
            raise NotFoundError("Match not found")
        doc["match_id"] = str(doc.pop("_id"))
        return doc

    async def update_match(self, match_id: str, update_data: Dict[str, Any]) -> Dict[str, Any]:
        if not update_data:
            raise MatchServiceError("Empty update payload")
        oid = self._to_oid(match_id)

        existing = await self.q.find_pending_by_id(oid)
        if not existing:
            raise NotFoundError("Match not found")

        await self.q.update_pending_match_set(oid, update_data)
        logger.info("✅ 🔄 Updated match %s", match_id)
        return await self._reload_pending(oid)

    async def set_player_order(self, match_id: str, player_order: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        if 'team' in match.game_mode.lower():
            raise MatchServiceError("Cannot set player order for teamer matches. Use change_order instead.")
        player_order_list = player_order.split(" ")
        curr_placement = 0
        placement = {}
        for i in range(len(player_order_list)):
            if player_order_list[i].upper() == "TIE":
                curr_placement = max(curr_placement - 1, 0)
            else:
                placement[player_order_list[i]] = curr_placement
                curr_placement += 1

        for i, player in enumerate(match.players):
            if player.subbed_out == False and player.discord_id not in placement:
                raise MatchServiceError(f"Discord ID {player.discord_id} not found in player order list")
            if player.subbed_out:
                player.placement = match.players[i - 1].placement
            else:
                player.placement = placement[player.discord_id]

        match = await self._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        changes.update(self._player_delta_changes(match))
        for i, player in enumerate(match.players):
            changes[f"players.{i}.placement"] = player.placement

        await self.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Changed player order for match %s", match_id)
        return await self._reload_pending(oid)

    async def change_order(self, match_id: str, new_order: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        num_teams = len({player.team for player in match.players})
        new_order_list = new_order.split(" ")
        if len(new_order_list) != num_teams:
            raise MatchServiceError(f"New order length does not match number of players/teams ({num_teams})")

        for player in match.players:
            if player.team < 0 or player.team >= len(new_order_list):
                raise MatchServiceError(
                    f"Team {player.team} has no entry in the new order list"
                )
            player.placement = _require_int(new_order_list[player.team], "new_order") - 1

        match = await self._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        changes.update(self._player_delta_changes(match))
        for i, player in enumerate(match.players):
            changes[f"players.{i}.placement"] = player.placement

        await self.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Changed player order for match %s", match_id)
        return await self._reload_pending(oid)

    async def delete_pending_match(self, match_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")
        res["match_id"] = str(res.pop("_id"))
        await self.q.delete_pending_match(oid)
        logger.info("✅ 🔄 Match %s removed", match_id)
        return res

    async def trigger_quit(self, match_id: str, quitter_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        changes: Dict[str, Any] = {}
        quitter_found = False
        for i, player in enumerate(res["players"]):
            if player.get("discord_id") == quitter_discord_id:
                changes[f"players.{i}.quit"] = not bool(res["players"][i]["quit"])
                quitter_found = True
                break
        if not quitter_found:
            raise MatchServiceError("Quitter discord_id not found in match players")

        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        await self.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Match %s, player %s quit toggled", match_id, quitter_discord_id)
        return await self._reload_pending(oid)

    async def assign_discord_id(self, match_id: str, player_id: str, player_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        idx = _require_int(player_id, "player_id") - 1
        if idx < 0 or idx >= len(match.players):
            raise MatchServiceError("Player ID out of range. Must be between 1 and number of players")

        match.players[idx].discord_id = player_discord_id
        match.players[idx].steam_id = await self.discord_to_steam_id(player_discord_id)

        match = await self._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        changes[f"players.{idx}.discord_id"] = player_discord_id
        changes[f"players.{idx}.steam_id"] = match.players[idx].steam_id
        changes.update(self._player_delta_changes(match))

        await self.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Assigned discord_id for match %s (player %s)", match_id, player_id)
        return await self._reload_pending(oid)

    async def assign_discord_id_all(self, match_id: str, player_discord_id: list[str], discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        if len(player_discord_id) != len(match.players):
            raise MatchServiceError("Must provide one discord_id per player")

        # Build mapping (fixed bug: always use the *same index* when writing steam_id)
        for i, did in enumerate(player_discord_id):
            match.players[i].discord_id = did
            match.players[i].steam_id = await self.discord_to_steam_id(did)

        match = await self._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        changes.update(self._player_delta_changes(match))
        for i, p in enumerate(match.players):
            changes[f"players.{i}.discord_id"] = p.discord_id
            changes[f"players.{i}.steam_id"] = p.steam_id

        await self.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Assigned all discord_ids for match %s", match_id)
        return await self._reload_pending(oid)

    async def assign_sub(self, match_id: str, sub_in_id: str, sub_out_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        sub_in_idx = _require_int(sub_in_id, "sub_in_id")
        if sub_in_idx < 0 or sub_in_idx >= len(match.players):
            raise MatchServiceError("Sub-in slot invalid or already claimed")

        match.players[sub_in_idx].is_sub = True
        sub_out_player_steam_id = await self.discord_to_steam_id(sub_out_discord_id)
        match.players.insert(sub_in_idx + 1, PlayerModel(
            steam_id = sub_out_player_steam_id,
            user_name = None,
            civ = match.players[sub_in_idx].civ,
            team = match.players[sub_in_idx].team,
            leader = match.players[sub_in_idx].leader,
            player_alive = match.players[sub_in_idx].player_alive,
            discord_id = sub_out_discord_id,
            placement = match.players[sub_in_idx].placement,
            quit = False,
            delta = 0.0,
            is_sub = False,
            subbed_out = True,
        ))
        match = await self._recompute_deltas(match)

        match.discord_messages_id_list = match.discord_messages_id_list + [discord_message_id]

        await self.q.replace_pending_match(oid, match.dict())
        logger.info("✅ 🔄 Sub assigned for match %s", match_id)
        return await self._reload_pending(oid)

    async def remove_sub(self, match_id: str, sub_out_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        idx = _require_int(sub_out_id, "sub_out_id")
        if idx < 1 or idx >= len(match.players):
            raise MatchServiceError("Player ID out of range. Must be between 2 and number of players")
        if not match.players[idx].subbed_out:
            raise MatchServiceError("That player is not marked as a sub")

        # Unmark the sub-out player
        match.players[idx - 1].is_sub = False

        # Remove the sub slot correctly (fix: pop wrong index)
        match.players.pop(idx)
        match.discord_messages_id_list = match.discord_messages_id_list + [discord_message_id]

        await self.q.replace_pending_match(oid, match.dict())
        logger.info("✅ 🔄 Sub removed for match %s", match_id)
        return await self._reload_pending(oid)
    
    async def contest_report(self, match_id: str, contestor_discord_id: str, reason: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        contest_report_entry = ContestReport(
            contestor_discord_id=contestor_discord_id,
            reason=reason)
        match.contest_report_list.append(contest_report_entry)
        match.discord_messages_id_list = match.discord_messages_id_list + [discord_message_id]

        await self.q.replace_pending_match(oid, match.dict())
        logger.info("✅ 🔄 Match %s contested by %s", match_id, contestor_discord_id)
        return await self._reload_pending(oid)
    
    async def revert_match(self, match_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_validated_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        
        # Pre-states
        pre_lifetime = await self.get_players_ranking(match)
        pre_season = await self.get_players_ranking(match, is_seasonal=True)
        pre_combined = await self.get_players_ranking(match, is_combined=True)

        session = await self.q.start_session()
        async with session:
            async with session.start_transaction():
                try:
                    # Stats writes
                    for i, p in enumerate(match.players):
                        if not p.discord_id or p.discord_id in ("-1", "-2") or p.discord_id.startswith("-"):
                            continue

                        did = str(p.discord_id)

                        # Legacy validated docs may lack the season/combined deltas (None);
                        # coerce once so the arithmetic below cannot TypeError.
                        delta = _as_float(p.delta, 0.0)
                        season_delta = _as_float(p.season_delta, 0.0)
                        combined_delta = _as_float(p.combined_delta, 0.0)

                        for pre, delta_value, is_seasonal, is_combined in (
                            (pre_lifetime[i], delta, False, False),
                            (pre_season[i], season_delta, True, False),
                            (pre_combined[i], combined_delta, False, True),
                        ):
                            doc = self._build_stat_doc(
                                discord_id=did,
                                pre=pre,
                                player=p,
                                mu=pre.mu - delta_value,
                                sigma=pre.sigma + 2,
                                delta_value=delta_value,
                                civs=self.revert_existing_stat(match, p, pre.civs),
                                step=-1,
                            )
                            await self.q.upsert_player_stat_doc(
                                civ_version=match.game,
                                is_seasonal=is_seasonal,
                                match_type=match.game_mode,
                                is_cloud=match.is_cloud,
                                is_combined=is_combined,
                                discord_id=did,
                                doc=doc,
                                session=session,
                            )

                        if p.is_sub:
                            await self.q.dec_subs_in(did, session=session)

                    await self.q.delete_validated_match(oid, session=session)

                    await session.commit_transaction()
                except Exception as e:
                    logger.exception("Transaction failed while writing to DB; aborting")
                    await session.abort_transaction()
                    raise MatchServiceError(f"An error occured during writing to DB: {e}")
        return {"match_id": str(match_id), **match.dict()}

    async def approve_match(self, match_id: str, approver_discord_id: str) -> Dict[str, Any]:
        async with approve_lock:
            oid = self._to_oid(match_id)
            res = await self.q.find_pending_by_id(oid)
            if not res:
                raise NotFoundError("Match not found")

            match = MatchModel(**res)

            # Ensure all players have placements set
            for p in match.players:
                if p.placement is None:
                    raise MatchServiceError("All players must have a placement before approving")

            # Pre-states
            pre_lifetime = await self.get_players_ranking(match)
            pre_season = await self.get_players_ranking(match, is_seasonal=True)
            pre_combined = await self.get_players_ranking(match, is_combined=True)

            # Rating updates (writes deltas into match players + returns post mu/sigma)
            match, post_lifetime = self.update_player_stats(match, pre_lifetime, "delta")
            match, post_season = self.update_player_stats(match, pre_season, "season_delta")
            match, post_combined = self.update_player_stats(match, pre_combined, "combined_delta")

            session = await self.q.start_session()
            async with session:
                async with session.start_transaction():
                    try:
                        # Stats writes
                        for i, p in enumerate(match.players):
                            if not p.discord_id or p.discord_id in ("-1", "-2") or p.discord_id.startswith("-"):
                                continue

                            did = str(p.discord_id)

                            for pre, post, delta_value, is_seasonal, is_combined in (
                                (pre_lifetime[i], post_lifetime[i], p.delta, False, False),
                                (pre_season[i], post_season[i], p.season_delta, True, False),
                                (pre_combined[i], post_combined[i], p.combined_delta, False, True),
                            ):
                                doc = self._build_stat_doc(
                                    discord_id=did,
                                    pre=pre,
                                    player=p,
                                    mu=post.mu,
                                    sigma=post.sigma,
                                    delta_value=delta_value,
                                    civs=self.update_existing_stat(match, p, pre.civs),
                                    step=1,
                                )
                                await self.q.upsert_player_stat_doc(
                                    civ_version=match.game,
                                    is_seasonal=is_seasonal,
                                    match_type=match.game_mode,
                                    is_cloud=match.is_cloud,
                                    is_combined=is_combined,
                                    discord_id=did,
                                    doc=doc,
                                    session=session,
                                )

                            if p.is_sub:
                                await self.q.inc_subs_in(did, session=session)

                        # Move pending -> validated
                        now = datetime.now(UTC)
                        validated_doc = match.dict()
                        validated_doc["created_at"] = res.get("created_at", now)
                        validated_doc["approved_at"] = now
                        validated_doc["reporter_discord_id"] = res.get("reporter_discord_id")
                        validated_doc["approver_discord_id"] = approver_discord_id
                        validated_doc["discord_messages_id_list"] = res.get("discord_messages_id_list", [])
                        validated_doc["save_file_hash"] = res.get("save_file_hash", "")
                        validated_doc["contest_report_list"] = []

                        validated_insert_id = await self.q.insert_validated_match(validated_doc, session=session)
                        await self.q.delete_pending_match(oid, session=session)

                        await session.commit_transaction()
                    except Exception as e:
                        logger.exception("Transaction failed while writing to DB; aborting")
                        await session.abort_transaction()
                        raise MatchServiceError(f"An error occured during writing to DB: {e}")

            logger.info("✅ ✅ Approved match %s", match_id)
            affected_players = []
            if match.game_mode.lower() == "ffa" or match.is_cloud:
                affected_players = [
                    {"discord_id": str(player.discord_id), "rating_mu": float(post_combined[index].mu) if match.is_cloud else post_lifetime[index].mu}
                    for index, player in enumerate(match.players)
                    if player.discord_id and player.discord_id not in ("-1", "-2") and not str(player.discord_id).startswith("-")
                ]
            return {"match_id": str(validated_insert_id), **match.dict(), "affected_players": affected_players}

    async def get_leaderboard(
        self,
        match_type: str,
        is_cloud: str,
        is_seasonal: bool,
        is_combined: bool,
        civ_version: str,
    ) -> Dict[str, Any]:
        is_cloud_game = str(is_cloud).strip().lower() in {"pbc", "cloud", "true", "1"}

        lb = await self.q.get_leaderboard(
            civ_version=civ_version,
            is_seasonal=is_seasonal,
            match_type=match_type,
            is_cloud=is_cloud_game,
            is_combined=is_combined,
            min_games=3,
            limit=100,
        )

        out: List[Dict[str, Any]] = []
        for idx, row in enumerate(lb.rows or [], start=1):
            did = str(row.get("_id"))
            mu = _as_float(row.get("mu"), 0.0)
            games = _as_int(row.get("games"), 0)
            out.append(
                {
                    "rank": idx,
                    "discord_id": did,
                    "mu": mu,
                    "sigma": _as_float(row.get("sigma"), 0.0),
                    "games": games,

                    # Backwards-compatible aliases for older clients.
                    "rating": int(round(mu)),
                    "games_played": games,
                    "wins": _as_int(row.get("wins"), 0),
                    "first": _as_int(row.get("first"), 0),
                }
            )
        last_updated_ts = int(lb.last_updated.timestamp()) if isinstance(lb.last_updated, datetime) else 0
        return {"rankings": out, "last_updated": last_updated_ts}


__all__ = ["InvalidIDError", "MatchService", "MatchServiceError", "NotFoundError", "ParseError"]
