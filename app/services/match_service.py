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

from app.config import settings
from app.models.db_models import MatchModel, PlayerModel, StatModel
from app.models.mongo_queries import MongoQueries
from app.parsers import parse_civ6_save, parse_civ7_save  
from app.services.skill import make_ts_env
from app.utils import get_cpl_name

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

class MatchService:
    def __init__(self, client: AsyncIOMotorClient):
        self.q = MongoQueries(client)

    def _to_oid(self, match_id: str) -> ObjectId:
        if not ObjectId.is_valid(match_id):
            raise InvalidIDError("Invalid match id")
        return ObjectId(match_id)

    def _parse_save(self, file_bytes: bytes) -> Dict[str, Any]:
        try:
            return parse_civ6_save(file_bytes)
        except Exception:
            try:
                return parse_civ7_save(file_bytes)
            except Exception as e7:
                raise ParseError("Unrecognized save file format") from e7


    async def discord_to_steam_id(self, discord_id: str) -> str:
        player = await self.q.get_user_by_discord_id(discord_id)
        if not player:
            raise MatchServiceError("User not found")
        return str(player["steam_id"])

    async def steam_to_discord_id(self, steam_id: str) -> str:
        player = await self.q.get_user_by_steam_id(steam_id)
        if not player:
            return "-1"
        return str(player["discord_id"])

    async def match_id_to_discord(self, match: MatchModel) -> MatchModel:
        players = []
        for player in match.players:
            discord_id = await self.steam_to_discord_id(player.steam_id)
            player.discord_id = discord_id
            players.append(player)
        match.players = players
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
        if not discord_id or discord_id in ("-1", "-2") or match.players[player_index].is_sub:
            return StatModel(
                discord_id=discord_id or "-1",
                index=player_index,
                mu=settings.ts_mu,
                sigma=settings.ts_sigma,
                games=0,
                wins=0,
                first=0,
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
                mu=settings.ts_mu,
                sigma=settings.ts_sigma,
                games=0,
                wins=0,
                first=0,
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
        ts_env = make_ts_env()

        # rating inputs (ordered)
        pre = [Rating(p.mu, p.sigma) for p in players_ranking]

        # build teams by team index
        teams_by_id: dict[int, list[int]] = defaultdict(list)
        for i, player in enumerate(match.players):
            teams_by_id[player.team].append(i)

        teams = [[pre[i] for i in teams_by_id[t]] for t in sorted(teams_by_id)]
        ranks = []
        for t in sorted(teams_by_id):
            # placement is stored per player; for team games expect identical placement per team
            ranks.append(match.players[teams_by_id[t][0]].placement)

        new_ratings = ts_env.rate(teams, ranks=ranks)

        # map back to player index
        post_mu_sigma: dict[int, Rating] = {}
        for t_idx, t in enumerate(sorted(teams_by_id)):
            for local_i, global_i in enumerate(teams_by_id[t]):
                post_mu_sigma[global_i] = new_ratings[t_idx][local_i]

        # deltas and “post” StatModels (only mu/sigma are authoritative here)
        post: List[StatModel] = []
        for i, p in enumerate(match.players):
            before = players_ranking[i]
            after = post_mu_sigma[i]
            delta = float(after.mu) - float(before.mu)

            # quitter logic (preserve existing behavior)
            if p.quit:
                p.__setattr__(delta_value_name, delta if delta < 0 else 0.0)
            else:
                p.__setattr__(delta_value_name, delta)

            post.append(
                StatModel(
                    discord_id=p.discord_id,
                    index=i,
                    mu=float(after.mu),
                    sigma=float(after.sigma),
                    games=before.games,
                    wins=before.wins,
                    first=before.first,
                    civs=before.civs,
                )
            )
        return match, post

    def update_existing_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Normalize civ naming
        civ_name = get_cpl_name(match.game, player.civ, getattr(player, "leader", None))

        civs = dict(existing_civs) if isinstance(existing_civs, dict) else {}

        def _to_int(v: Any, default: int = 0) -> int:
            try:
                return int(v)
            except (TypeError, ValueError):
                return default

        entry = civs.get(civ_name)

        # Backwards compatibility:
        # - legacy shape: {"DutchWilhelmina": 3}
        # - new shape: {"DutchWilhelmina": {"games": 3, "wins": 1}}
        if isinstance(entry, dict):
            games = _to_int(entry.get("games", 0), 0)
            wins = _to_int(entry.get("wins", 0), 0)
        elif isinstance(entry, int):
            games = _to_int(entry, 0)
            wins = 0
        else:
            games = 0
            wins = 0

        games += 1
        if player.placement == 0:
            wins += 1

        civs[civ_name] = {"games": games, "wins": wins}
        return civs

    def build_stats_doc(
        self,
        discord_id: str,
        pre: StatModel,
        post: StatModel,
        civs: Dict[str, Any],
    ) -> Dict[str, Any]:

        # wins/first are driven by placement; caller sets those increments explicitly
        return {
            "_id": Int64(discord_id),
            "mu": float(post.mu),
            "sigma": float(post.sigma),
            "games": int(pre.games) + 1,
            "wins": int(pre.wins),
            "first": int(pre.first),
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

        match = MatchModel(**parsed)
        match = await self.match_id_to_discord(match)

        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")

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
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    async def get(self, match_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        doc = await self.q.find_pending_by_id(oid)
        if not doc:
            raise NotFoundError("Match not found")
        doc["match_id"] = str(doc.pop("_id"))
        return doc

    async def update(self, match_id: str, update_data: Dict[str, Any]) -> Dict[str, Any]:
        if not update_data:
            raise MatchServiceError("Empty update payload")
        oid = self._to_oid(match_id)

        existing = await self.q.find_pending_by_id(oid)
        if not existing:
            raise NotFoundError("Match not found")

        await self.q.update_pending_match_set(oid, update_data)
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Updated match %s", match_id)
        return updated

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
            player.placement = int(new_order_list[player.team]) - 1

        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        for i, player in enumerate(match.players):
            changes[f"players.{i}.placement"] = player.placement
            changes[f"players.{i}.delta"] = match.players[i].delta
            changes[f"players.{i}.season_delta"] = match.players[i].season_delta
            changes[f"players.{i}.combined_delta"] = match.players[i].combined_delta

        await self.q.update_pending_match_set(oid, changes)
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Changed player order for match %s", match_id)
        return updated

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
        for i, player in enumerate(res["players"]):
            if player.get("discord_id") == quitter_discord_id:
                changes[f"players.{i}.quit"] = not bool(res["players"][i]["quit"])
                break

        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        await self.q.update_pending_match_set(oid, changes)
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Match %s, player %s quit toggled", match_id, quitter_discord_id)
        return updated

    async def assign_discord_id(self, match_id: str, player_id: str, player_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        idx = int(player_id) - 1
        if idx < 0 or idx >= len(match.players):
            raise MatchServiceError("Player ID out of range. Must be between 1 and number of players")

        match.players[idx].discord_id = player_discord_id
        match.players[idx].steam_id = await self.discord_to_steam_id(player_discord_id)

        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        changes[f"players.{idx}.discord_id"] = player_discord_id
        changes[f"players.{idx}.steam_id"] = match.players[idx].steam_id
        for i in range(len(match.players)):
            changes[f"players.{i}.delta"] = match.players[i].delta
            changes[f"players.{i}.season_delta"] = match.players[i].season_delta
            changes[f"players.{i}.combined_delta"] = match.players[i].combined_delta

        await self.q.update_pending_match_set(oid, changes)
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Assigned discord_id for match %s (player %s)", match_id, player_id)
        return updated

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

        # small verification guard (mapping accurate)
        for i in range(len(match.players)):
            if match.players[i].discord_id != player_discord_id[i]:
                raise MatchServiceError("Internal mapping error: discord_id order mismatch")

        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [discord_message_id]
        for i, p in enumerate(match.players):
            changes[f"players.{i}.discord_id"] = p.discord_id
            changes[f"players.{i}.steam_id"] = p.steam_id
            changes[f"players.{i}.delta"] = p.delta
            changes[f"players.{i}.season_delta"] = p.season_delta
            changes[f"players.{i}.combined_delta"] = p.combined_delta

        await self.q.update_pending_match_set(oid, changes)
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Assigned all discord_ids for match %s", match_id)
        return updated

    async def assign_sub(self, match_id: str, sub_in_id: str, sub_out_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        sub_in_idx = int(sub_in_id)
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
        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)
        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")

        await self.q.replace_pending_match(oid, match.dict())
        await self.q.update_pending_match_set(oid, {"discord_messages_id_list": res["discord_messages_id_list"] + [discord_message_id]})
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Sub assigned for match %s", match_id)
        return updated

    async def remove_sub(self, match_id: str, sub_out_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        idx = int(sub_out_id)
        if idx < 1 or idx >= len(match.players):
            raise MatchServiceError("Player ID out of range. Must be between 2 and number of players")
        if not match.players[idx].subbed_out:
            raise MatchServiceError("That player is not marked as a sub")

        # Unmark the sub-out player
        match.players[idx - 1].is_sub = False

        # Remove the sub slot correctly (fix: pop wrong index)
        match.players.pop(idx)

        await self.q.replace_pending_match(oid, match.dict())
        await self.q.update_pending_match_set(oid, {"discord_messages_id_list": res["discord_messages_id_list"] + [discord_message_id]})
        updated = await self.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        logger.info("✅ 🔄 Sub removed for match %s", match_id)
        return updated

    async def approve_match(self, match_id: str) -> List[str]:
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
                    # Stats writes
                    for i, p in enumerate(match.players):
                        if not p.discord_id or p.discord_id in ("-1", "-2"):
                            continue

                        did = str(p.discord_id)

                        # lifetime
                        civs_life = self.update_existing_stat(match, p, pre_lifetime[i].civs)
                        doc_life = {
                            "_id": Int64(did),
                            "mu": float(post_lifetime[i].mu),
                            "sigma": float(post_lifetime[i].sigma),
                            "games": int(pre_lifetime[i].games) + 1,
                            "wins": int(pre_lifetime[i].wins) + (1 if p.placement == 0 else 0),
                            "first": int(pre_lifetime[i].first) + (1 if p.placement == 0 else 0),
                            "civs": civs_life,
                            "lastModified": datetime.now(UTC),
                        }
                        await self.q.upsert_player_stat_doc(
                            civ_version=match.game,
                            is_seasonal=False,
                            match_type=match.game_mode,
                            is_cloud=match.is_cloud,
                            is_combined=False,
                            discord_id=did,
                            doc=doc_life,
                            session=session,
                        )

                        # seasonal
                        civs_season = self.update_existing_stat(match, p, pre_season[i].civs)
                        doc_season = {
                            "_id": Int64(did),
                            "mu": float(post_season[i].mu),
                            "sigma": float(post_season[i].sigma),
                            "games": int(pre_season[i].games) + 1,
                            "wins": int(pre_season[i].wins) + (1 if p.placement == 0 else 0),
                            "first": int(pre_season[i].first) + (1 if p.placement == 0 else 0),
                            "civs": civs_season,
                            "lastModified": datetime.now(UTC),
                        }
                        await self.q.upsert_player_stat_doc(
                            civ_version=match.game,
                            is_seasonal=True,
                            match_type=match.game_mode,
                            is_cloud=match.is_cloud,
                            is_combined=False,
                            discord_id=did,
                            doc=doc_season,
                            session=session,
                        )

                        # combined
                        civs_combined = self.update_existing_stat(match, p, pre_combined[i].civs)
                        doc_combined = {
                            "_id": Int64(did),
                            "mu": float(post_combined[i].mu),
                            "sigma": float(post_combined[i].sigma),
                            "games": int(pre_combined[i].games) + 1,
                            "wins": int(pre_combined[i].wins) + (1 if p.placement == 0 else 0),
                            "first": int(pre_combined[i].first) + (1 if p.placement == 0 else 0),
                            "civs": civs_combined,
                            "lastModified": datetime.now(UTC),
                        }
                        await self.q.upsert_player_stat_doc(
                            civ_version=match.game,
                            is_seasonal=False,
                            match_type=match.game_mode,
                            is_cloud=match.is_cloud,
                            is_combined=True,
                            discord_id=did,
                            doc=doc_combined,
                            session=session,
                        )

                        if p.is_sub:
                            await self.q.inc_subs_in(did, session=session)

                    # Move pending -> validated
                    now = datetime.now(UTC)
                    validated_doc = match.dict()
                    validated_doc["created_at"] = res.get("created_at", now)
                    validated_doc["updated_at"] = now
                    validated_doc["status"] = "validated"
                    validated_doc["validated_at"] = now
                    validated_doc["reporter_discord_id"] = res.get("reporter_discord_id")
                    validated_doc["discord_messages_id_list"] = res.get("discord_messages_id_list", [])
                    validated_doc["save_file_hash"] = res.get("save_file_hash", "")

                    await self.q.insert_validated_match(validated_doc, session=session)
                    await self.q.delete_pending_match(oid, session=session)

            logger.info("✅ ✅ Approved match %s", match_id)
            return res.get("discord_messages_id_list", [])

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
