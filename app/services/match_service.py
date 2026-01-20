import logging
from collections import defaultdict
from typing import Any, Dict, List
from bson import ObjectId
from bson.int64 import Int64
from app.parsers import parse_civ7_save, parse_civ6_save  # do not modify parser code
from app.utils import get_cpl_name
from app.config import settings
from app.models.db_models import MatchModel, StatModel, PlayerModel
from trueskill import Rating
from app.services.skill import make_ts_env
import hashlib
import asyncio
from datetime import datetime, UTC
import copy

logger = logging.getLogger(__name__)

class MatchServiceError(Exception): ...
class InvalidIDError(MatchServiceError): ...
class ParseError(MatchServiceError): ...
class NotFoundError(MatchServiceError): ...

approve_lock = asyncio.Lock()
class MatchService:
    def __init__(self, db):
        self.db = db
        self.pending_matches = db["match_reporter"].pending_matches
        self.validated_matches = db["match_reporter"].validated_matches
        self.players = db["server_members"].users
        self.subs_table = db["server_members"].subs
        self.civ6_lifetime_stats = db["civ6_lifetime_stats"]
        self.civ7_lifetime_stats = db["civ7_lifetime_stats"]
        self.civ6_seasonal_stats = db["civ6_season_stats"]
        self.civ7_seasonal_stats = db["civ7_season_stats"]

    @staticmethod
    def _to_oid(match_id: str) -> ObjectId:
        try:
            return ObjectId(match_id)
        except Exception:
            raise InvalidIDError("Invalid match ID")

    @staticmethod
    def _parse_save(file_bytes: bytes) -> Dict[str, Any]:
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
        player = await self.players.find_one({"discord_id": f"{discord_id}"})
        if player:
            return player.get("steam_id")
        return None

    async def steam_to_discord_id(self, steam_id: str) -> str:
        player = await self.players.find_one({"steam_id": f"{steam_id}"})
        if player:
            return player.get("discord_id")
        return None

    async def match_id_to_discord(self, match):
        for player in match.players:
            if player.steam_id and player.steam_id != '-1':
                player.discord_id = await self.steam_to_discord_id(player.steam_id)
        return match

    def get_stat_table(self, is_cloud: bool, match_type: str, civ_version: str, is_seasonal: bool = False, is_combined: bool = False):
        if is_seasonal:
            if civ_version == "civ6":
                db = self.civ6_seasonal_stats
            else:
                db = self.civ7_seasonal_stats
        else:
            if civ_version == "civ6":
                db = self.civ6_lifetime_stats
            else:
                db = self.civ7_lifetime_stats
        match_table = ("pbc_" if is_cloud else "rt_") + ('combined' if is_combined else match_type)
        return getattr(db, match_table)

    def get_player_stats_db(self, match, player, player_new_stats: StatModel, delta_value_name: str) -> Dict[str, Any]:
        player_stats_db = {}
        player_stats_db[f"mu"] = player_new_stats.mu
        player_stats_db[f"sigma"] = player_new_stats.sigma
        player_stats_db[f"games"] = player_new_stats.games + 1
        player_stats_db[f"wins"] = player_new_stats.wins + (1 if getattr(player, delta_value_name) > 0 else 0)
        player_stats_db[f"first"] = player_new_stats.first + (1 if player.placement == 0 else 0)
        player_stats_db[f"subbedIn"] = player_new_stats.subbedIn + (1 if player.is_sub else 0)
        player_stats_db[f"subbedOut"] = player_new_stats.subbedOut + (1 if player.subbed_out else 0)
        player_stats_db[f"lastModified"] = datetime.now(UTC)
        if player.civ:
            civs = player_new_stats.civs
            player_civ_leader = get_cpl_name(match.game, player.civ, player.leader)
            civs[player_civ_leader] = civs.get(player_civ_leader, 0) + 1
            player_stats_db[f"civs"] = civs
        return player_stats_db

    async def get_player_ranking(self, match: MatchModel, discord_id: str, player_index: int, is_seasonal: bool, is_combined: bool) -> StatModel:
        if discord_id == None:
            return StatModel(
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
        stat_table = self.get_stat_table(match.is_cloud, match.game_mode, match.game, is_seasonal, is_combined)
        player = await stat_table.find_one({"_id": Int64(discord_id)})
        if player:
            player['id'] = player.pop('_id')
            player['index'] = player_index
            return StatModel(**player)
        else:
            return StatModel(
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

    async def get_players_ranking(self, match: MatchModel, is_seasonal: bool = False, is_combined: bool = False) -> List[StatModel]:
        players_ranking = []
        for player_index, player in enumerate(match.players):
            ranking = await self.get_player_ranking(match, player.discord_id, player_index, is_seasonal, is_combined)
            players_ranking.append(ranking)
        return players_ranking

    def update_player_stats(self, match: MatchModel, players_ranking: List[StatModel], delta_value_name: str):
        num_teams = len(set([p.team for p in match.players]))
        if num_teams <= 1:
            print(f"Skipping match with less than 2 teams. Validation Msg ID: {match.validation_msg_id}")
            return None, None
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

    async def create_from_save(self, file_bytes: bytes, reporter_discord_id: str, is_cloud: bool, discord_message_id: str) -> Dict[str, Any]:
        parsed = self._parse_save(file_bytes)
        m = hashlib.sha256()
        unique_data = ','.join(
            [parsed['game']] + 
            [parsed['map_type']] +
            [p['civ'] + (p['leader'] if 'leader' in p else '') for p in parsed['players']]
        )
        m.update(unique_data.encode('utf-8'))
        save_file_hash = m.hexdigest()
        res = await self.pending_matches.find_one({"save_file_hash": save_file_hash})
        if res:
            match_id = str(res["_id"])
            del res["_id"]
            res["match_id"] = match_id
            res['repeated'] = True
            return res
        parsed['save_file_hash'] = save_file_hash
        parsed['repeated'] = False
        parsed['reporter_discord_id'] = reporter_discord_id
        parsed['is_cloud'] = is_cloud
        parsed['discord_messages_id_list'] = [discord_message_id]
        match = MatchModel(**parsed)
        match = await self.match_id_to_discord(match)
        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)
        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")
        res = await self.pending_matches.insert_one(match.dict())
        return {"match_id": str(res.inserted_id), **match.dict()}
    
    async def append_discord_message_id_list(self, match_id: str, discord_message_id_list: list[str]) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if not res:
            raise NotFoundError("Match not found")
        current_list = res.get("discord_messages_id_list", [])
        updated_list = current_list + discord_message_id_list
        await self.pending_matches.update_one({"_id": oid}, {"$set": {"discord_messages_id_list": updated_list}})
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    async def get(self, match_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        doc = await self.pending_matches.find_one({"_id": oid})
        if not doc:
            raise NotFoundError("Match not found")
        doc["match_id"] = str(doc.pop("_id"))
        return doc

    async def update(self, match_id: str, update_data: Dict[str, Any]) -> Dict[str, Any]:
        if not update_data:
            raise MatchServiceError("Empty update payload")
        oid = self._to_oid(match_id)
        res = await self.pending_matches.update_one({"_id": oid}, {"$set": update_data})
        if res.matched_count == 0:
            raise NotFoundError("Match not found")
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        logger.info(f"✅ 🔄 Updated match {match_id}")
        return updated

    async def change_order(self, match_id: str, new_order: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if res == None:
            raise NotFoundError("Match not found")
        match = MatchModel(**res)
        num_teams = len({player.team for player in match.players})
        new_order_list = new_order.split(' ')
        if len(new_order_list) != num_teams:
            raise MatchServiceError(f"New order length does not match number of players/teams ({num_teams})")
        for i, player in enumerate(match.players):
            player.placement = int(new_order_list[player.team]) - 1
        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)
        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")
        changes = {}
        changes["discord_messages_id_list"] = res['discord_messages_id_list'] + [discord_message_id]
        for i, player in enumerate(match.players):
            changes[f"players.{i}.placement"] = player.placement
            changes[f"players.{i}.delta"] = match.players[i].delta
            changes[f"players.{i}.season_delta"] = match.players[i].season_delta
            changes[f"players.{i}.combined_delta"] = match.players[i].combined_delta
        await self.pending_matches.update_one({"_id": oid}, {"$set": changes})
        logger.info(f"✅ 🔄 Changed player order for match {match_id}")
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    async def delete_pending_match(self, match_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if res == None:
            raise NotFoundError("Match not found")
        res["match_id"] = str(res.pop("_id"))
        await self.pending_matches.delete_one({"_id": oid})
        logger.info(f"✅ 🔄 Match {match_id} removed")
        return res

    async def trigger_quit(self, match_id: str, quitter_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if res == None:
            raise NotFoundError("Match not found")
        changes = {}
        for i, player in enumerate(res['players']):
            if player.get('discord_id') == quitter_discord_id:
                changes[f"players.{i}.quit"] = False if res['players'][i]['quit'] else True
                break
        changes["discord_messages_id_list"] = res['discord_messages_id_list'] + [discord_message_id]
        await self.pending_matches.update_one({"_id": oid}, {"$set": changes})
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        logger.info(f"✅ 🔄 Match {match_id}, player {quitter_discord_id} quit triggered")
        return updated

    async def assign_discord_id(self, match_id: str, player_id: str, player_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if res == None:
            raise NotFoundError("Match not found")
        match = MatchModel(**res)
        if int(player_id) < 1 or int(player_id) > len(match.players):
            raise MatchServiceError("Player ID out of range. Must be between 1 and number of players")
        match.players[int(player_id)-1].discord_id = player_discord_id
        match.players[int(player_id)-1].steam_id = await self.discord_to_steam_id(player_discord_id)
        players_ranking = await self.get_players_ranking(match)
        print(match.is_cloud, match.game_mode, match.game)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)
        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")
        changes = {}
        changes["discord_messages_id_list"] = res['discord_messages_id_list'] + [discord_message_id]
        changes[f"players.{int(player_id)-1}.discord_id"] = player_discord_id
        changes[f"players.{int(player_id)-1}.steam_id"] = match.players[int(player_id)-1].steam_id
        for i, player in enumerate(res['players']):
            changes[f"players.{i}.delta"] = match.players[i].delta
            changes[f"players.{i}.season_delta"] = match.players[i].season_delta
        await self.pending_matches.update_one({"_id": oid}, {"$set": changes})
        logger.info(f"✅ 🔄 Assigned player id for match {match_id}")
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    async def assign_sub(self, match_id: str, sub_in_id: str, sub_out_discord_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if res == None:
            raise NotFoundError("Match not found")
        match = MatchModel(**res)
        if int(sub_in_id) < 0 or int(sub_in_id) >= len(match.players):
            raise MatchServiceError("Sub in Player ID out of range. Must be between 0 and number of players - 1")
        match.players[int(sub_in_id)].is_sub = True
        sub_out_player_steam_id = await self.discord_to_steam_id(sub_out_discord_id)
        match.players.insert(int(sub_in_id) + 1, PlayerModel(
            steam_id = sub_out_player_steam_id,
            user_name = None,
            civ = match.players[int(sub_in_id)].civ,
            team = match.players[int(sub_in_id)].team,
            leader = match.players[int(sub_in_id)].leader,
            player_alive = match.players[int(sub_in_id)].player_alive,
            discord_id = sub_out_discord_id,
            placement = match.players[int(sub_in_id)].placement,
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
        match.discord_messages_id_list = res['discord_messages_id_list'] + [discord_message_id]
        await self.pending_matches.replace_one({"_id": oid}, match.dict())
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        logger.info(f"✅ 🔄 Match {match_id}, sub_in: {sub_in_id}, sub_out: {sub_out_discord_id}")
        return updated
    
    async def remove_sub(self, match_id: str, sub_out_id: str, discord_message_id: str) -> Dict[str, Any]:
        oid = self._to_oid(match_id)
        res = await self.pending_matches.find_one({"_id": oid})
        if res == None:
            raise NotFoundError("Match not found")
        match = MatchModel(**res)
        if int(sub_out_id) < 1 or int(sub_out_id) >= len(match.players) or not match.players[int(sub_out_id)].subbed_out:
            raise MatchServiceError("Sub in Player ID out of range. Must be between 1 and number of players - 1")
        match.players[int(sub_out_id)-1].is_sub = False
        match.players.pop(int(sub_out_id))
        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(match, is_combined=True)
        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(match, players_season_ranking, "season_delta")
        match, _ = self.update_player_stats(match, players_combined_ranking, "combined_delta")
        match.discord_messages_id_list = res['discord_messages_id_list'] + [discord_message_id]
        await self.pending_matches.replace_one({"_id": oid}, match.dict())
        updated = await self.pending_matches.find_one({"_id": oid})
        updated["match_id"] = str(updated.pop("_id"))
        logger.info(f"✅ 🔄 Match {match_id}, sub_out_id: {sub_out_id}")
        return updated

    async def approve_match(self, match_id: str, approver_discord_id: str) -> Dict[str, Any]:
        # Use a lock to make sure only one approval happens at a time
        async with approve_lock:
            oid = self._to_oid(match_id)
            res = await self.pending_matches.find_one({"_id": oid})
            if res == None:
                raise NotFoundError("Match not found")
            match = MatchModel(**res)
            for i, player in enumerate(match.players):
                if player.discord_id == None:
                    raise MatchServiceError(f"Player {player.user_name} has no linked Discord ID")
            players_ranking = await self.get_players_ranking(match)
            players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
            players_combined_ranking = await self.get_players_ranking(match, is_combined=True)
            match, post = self.update_player_stats(match, players_ranking, "delta")
            match, season_post = self.update_player_stats(match, players_season_ranking, "season_delta")
            match, combined_post = self.update_player_stats(match, players_combined_ranking, "combined_delta")
            match.approved_at = datetime.now(UTC)
            match.approver_discord_id = approver_discord_id
            stats_table = self.get_stat_table(match.is_cloud, match.game_mode, match.game, is_seasonal=False)
            season_stats_table = self.get_stat_table(match.is_cloud, match.game_mode, match.game, is_seasonal=True)
            combined_stats_table = self.get_stat_table(match.is_cloud, match.game_mode, match.game, is_combined=True)
            session = await self.db.start_session()
            async with session:
                async with session.start_transaction():
                    try:
                        for i, player in enumerate(match.players):
                            player_stats_db = self.get_player_stats_db(match, player, post[i], "delta")
                            player_season_stats_db = self.get_player_stats_db(match, player, season_post[i], "season_delta")
                            player_combined_stats_db = self.get_player_stats_db(match, player, combined_post[i], "combined_delta")
                            await stats_table.replace_one({"_id": Int64(player.discord_id)}, player_stats_db, upsert=True, session=session)
                            await season_stats_table.replace_one({"_id": Int64(player.discord_id)}, player_season_stats_db, upsert=True, session=session)
                            await combined_stats_table.replace_one({"_id": Int64(player.discord_id)}, player_combined_stats_db, upsert=True, session=session)
                            if player.is_sub:
                                await self.subs_table.update_one(
                                    {"_id": player.discord_id},
                                    {"$inc": {"subs_in": 1}},
                                    upsert=True,
                                    session=session
                                )
                        validated = await self.validated_matches.insert_one(match.dict(), session=session)
                        await self.pending_matches.delete_one({"_id": oid}, session=session)
                        # Commit the transaction
                        await session.commit_transaction()
                    except Exception as e:
                        # Abort the transaction in case of an error
                        print("An error occurred while writing to DB:", e)
                        await session.abort_transaction()
                        raise MatchServiceError(f"An error occured during writing to DB: {e}")
            logger.info(f"✅ 🔄 Match {match_id} approved")
            return {"match_id": str(validated.inserted_id), **match.dict()}
        
    async def get_leaderboard(self, is_cloud: str, game: str, game_mode: str, is_seasonal: bool, is_combined: bool) -> Dict[str, Any]:
        stats_table = self.get_stat_table(is_cloud == "PBC", game_mode, game, is_seasonal=is_seasonal, is_combined=is_combined)
        cursor = stats_table.find({ "games": { "$gt": 2 } }).sort([("mu", -1), ("sigma", 1)]).limit(100)
        leaderboard = []
        async for doc in cursor:
            leaderboard.append({
                "discord_id": str(doc["_id"]),
                "rating": int(doc["mu"]),
                "games_played": doc["games"],
                "wins": doc["wins"],
                "first": doc["first"],
            })
        return {"rankings": leaderboard}