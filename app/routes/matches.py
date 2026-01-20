import logging
from fastapi import APIRouter, Depends, HTTPException, Form
from app.dependencies import get_database
from app.models.schemas import MatchResponse, MatchUpdate, ChangeOrder, DeletePendingMatch, TriggerQuit, AppendDiscordMessageID, AssignDiscordId, AssignSub, RemoveSub, ApproveMatch, GetLeaderboardRequest, LeaderboardRankingResponse
from app.services.match_service import MatchService, InvalidIDError, NotFoundError, MatchServiceError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["matches"])

@router.put("/get-match/", response_model=MatchResponse)
async def get_match(match_id: str = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.get(match_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid match ID: {match_id}")
        raise HTTPException(status_code=400, detail="Invalid match ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")

@router.put("/append-message-id-list/", response_model=MatchResponse)
async def append_message_id_list(payload: AppendDiscordMessageID = Form(), db = Depends(get_database)):
    match_id = payload.match_id
    discord_message_id = payload.discord_message_id
    svc = MatchService(db)
    try:
        return await svc.append_discord_message_id_list(match_id, discord_message_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid match ID: {match_id}")
        raise HTTPException(status_code=400, detail="Invalid match ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/update-match/", response_model=MatchResponse)
async def update_match(payload: MatchUpdate = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    try:
        return await svc.update(match_id, payload.dict(exclude_unset=True))
    except InvalidIDError:
        logger.error(f"🔴 Invalid match ID: {match_id}")
        raise HTTPException(status_code=400, detail="Invalid match ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/change-order/", response_model=MatchResponse)
async def change_order(payload: ChangeOrder = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    new_order = payload.new_order
    discord_message_id = payload.discord_message_id
    try:
        return await svc.change_order(match_id, new_order, discord_message_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid match ID: {match_id}")
        raise HTTPException(status_code=400, detail="Invalid match ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/delete-pending-match/", response_model=MatchResponse)
async def delete_pending_match(payload: DeletePendingMatch = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    try:
        return await svc.delete_pending_match(match_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid match ID: {match_id}")
        raise HTTPException(status_code=400, detail="Invalid match ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/trigger-quit/", response_model=MatchResponse)
async def trigger_quit(payload: TriggerQuit = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    quitter_discord_id = payload.quitter_discord_id
    discord_message_id = payload.discord_message_id
    try:
        return await svc.trigger_quit(match_id, quitter_discord_id, discord_message_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid quitter discord ID: {match_id}, quitter_discord_id: {quitter_discord_id}")
        raise HTTPException(status_code=400, detail="Invalid match ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found. matchID: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/assign-discord-id/", response_model=MatchResponse)
async def assign_discord_id(payload: AssignDiscordId = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    player_id = payload.player_id
    player_discord_id = payload.player_discord_id
    discord_message_id = payload.discord_message_id
    try:
        return await svc.assign_discord_id(match_id, player_id, player_discord_id, discord_message_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid player ID: {match_id}, player_id: {player_id}, discord_id: {player_discord_id}")
        raise HTTPException(status_code=400, detail="Invalid player ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found. matchID: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/assign-sub/", response_model=MatchResponse)
async def assign_sub(payload: AssignSub = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    sub_in_id = payload.sub_in_id
    sub_out_discord_id = payload.sub_out_discord_id
    discord_message_id = payload.discord_message_id
    try:
        return await svc.assign_sub(match_id, sub_in_id, sub_out_discord_id, discord_message_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid player ID: {match_id}, sub_in_id: {sub_in_id}, sub_out_discord_id: {sub_out_discord_id}")
        raise HTTPException(status_code=400, detail="Invalid player ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found. matchID: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/remove-sub/", response_model=MatchResponse)
async def remove_sub(payload: RemoveSub = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    sub_out_id = payload.sub_out_id
    discord_message_id = payload.discord_message_id
    try:
        return await svc.remove_sub(match_id, sub_out_id, discord_message_id)
    except InvalidIDError:
        logger.error(f"🔴 Invalid player ID: {match_id}, sub_out_id: {sub_out_id}")
        raise HTTPException(status_code=400, detail="Invalid player ID")
    except NotFoundError:
        logger.warning(f"🔴 Match not found. matchID: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/approve-match/", response_model=MatchResponse)
async def approve_match(payload: ApproveMatch = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    match_id = payload.match_id
    approver_discord_id = payload.approver_discord_id
    try:
        return await svc.approve_match(match_id, approver_discord_id)
    except NotFoundError:
        logger.warning(f"🔴 Match not found. matchID: {match_id}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/get-leaderboard-ranking/", response_model=LeaderboardRankingResponse)
async def get_leaderboard_ranking(payload: GetLeaderboardRequest = Form(), db = Depends(get_database)):
    svc = MatchService(db)
    game = payload.game
    game_type = payload.game_type
    game_mode = payload.game_mode
    is_seasonal = payload.is_seasonal
    is_combined = payload.is_combined
    try:
        return await svc.get_leaderboard(game_type, game, game_mode, is_seasonal, is_combined)
    except NotFoundError:
        logger.warning(f"🔴 Invalid game type for leaderboard. game:{game} game_mode:{game_mode}")
        raise HTTPException(status_code=404, detail="Match not found")
    except MatchServiceError as e:
        logger.warning(f"⚠️ Update error: {e}")
        raise HTTPException(status_code=400, detail=str(e))