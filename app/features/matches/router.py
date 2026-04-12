from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from app.core.dependencies import get_database
from app.features.matches.schemas import (
    AppendDiscordMessageID,
    ApproveMatch,
    AssignDiscordId,
    AssignDiscordIdAll,
    AssignSub,
    ChangeOrder,
    ContestReportRequest,
    DeletePendingMatch,
    GetLeaderboardRequest,
    LeaderboardRankingResponse,
    MatchResponse,
    MatchUpdate,
    RemoveSub,
    RevertMatchRequest,
    SetPlayerOrder,
    TriggerQuit,
)
from app.features.matches.service import (
    InvalidIDError,
    MatchService,
    MatchServiceError,
    NotFoundError,
    ParseError,
)

logger = logging.getLogger(__name__)

matches_router = APIRouter(prefix="/api/v1", tags=["matches"])
upload_router = APIRouter(prefix="/api/v1", tags=["upload"])


@upload_router.post("/upload-game-report/")
async def upload_game_report(
    file: UploadFile = File(...),
    reporter_discord_id: str = Form(...),
    is_cloud: str = Form(...),
    discord_message_id: str = Form(...),
    db=Depends(get_database),
):
    raw = await file.read()
    is_cloud_game = is_cloud == "1"
    svc = MatchService(db)
    try:
        created = await svc.create_from_save(raw, reporter_discord_id, is_cloud_game, discord_message_id)
        logger.info("✅ Stored match %s", created["match_id"])
        return created
    except ParseError as exc:
        logger.error("🔴 Unrecognized save file format")
        raise HTTPException(status_code=400, detail=f"Unrecognized save file format {exc}") from exc
    except Exception as exc:  # pragma: no cover - exercised in integration tests
        logger.exception("🔴 Failed to store match: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error") from exc


@matches_router.put("/get-match/", response_model=MatchResponse)
async def get_match(match_id: str = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.get_match(match_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid match ID: %s", match_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found: %s", match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc


@matches_router.put("/append-message-id-list/", response_model=MatchResponse)
async def append_message_id_list(payload: AppendDiscordMessageID = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.append_discord_message_id_list(payload.match_id, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid match ID: %s", payload.match_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/update-match/", response_model=MatchResponse)
async def update_match(payload: MatchUpdate = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.update_match(payload.match_id, payload.dict(exclude_unset=True))
    except InvalidIDError as exc:
        logger.error("🔴 Invalid match ID: %s", payload.match_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/set-player-order/", response_model=MatchResponse)
async def set_player_order(payload: SetPlayerOrder = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.set_player_order(payload.match_id, payload.player_order, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid match ID: %s", payload.match_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/change-order/", response_model=MatchResponse)
async def change_order(payload: ChangeOrder = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.change_order(payload.match_id, payload.new_order, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid match ID: %s", payload.match_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/delete-pending-match/", response_model=MatchResponse)
async def delete_pending_match(payload: DeletePendingMatch = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.delete_pending_match(payload.match_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid match ID: %s", payload.match_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/trigger-quit/", response_model=MatchResponse)
async def trigger_quit(payload: TriggerQuit = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.trigger_quit(payload.match_id, payload.quitter_discord_id, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid quitter discord ID: %s, quitter_discord_id: %s", payload.match_id, payload.quitter_discord_id)
        raise HTTPException(status_code=400, detail="Invalid match ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/assign-discord-id/", response_model=MatchResponse)
async def assign_discord_id(payload: AssignDiscordId = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.assign_discord_id(payload.match_id, payload.player_id, payload.player_discord_id, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error(
            "🔴 Invalid player ID: %s, player_id: %s, discord_id: %s",
            payload.match_id,
            payload.player_id,
            payload.player_discord_id,
        )
        raise HTTPException(status_code=400, detail="Invalid player ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/assign-discord-id-all/", response_model=MatchResponse)
async def assign_discord_id_all(payload: AssignDiscordIdAll = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.assign_discord_id_all(payload.match_id, payload.discord_id_list, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid player ID: %s, discord_id_list: %s", payload.match_id, payload.discord_id_list)
        raise HTTPException(status_code=400, detail="Invalid player ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/assign-sub/", response_model=MatchResponse)
async def assign_sub(payload: AssignSub = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.assign_sub(payload.match_id, payload.sub_in_id, payload.sub_out_discord_id, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error(
            "🔴 Invalid player ID: %s, sub_in_id: %s, sub_out_discord_id: %s",
            payload.match_id,
            payload.sub_in_id,
            payload.sub_out_discord_id,
        )
        raise HTTPException(status_code=400, detail="Invalid player ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/remove-sub/", response_model=MatchResponse)
async def remove_sub(payload: RemoveSub = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.remove_sub(payload.match_id, payload.sub_out_id, payload.discord_message_id)
    except InvalidIDError as exc:
        logger.error("🔴 Invalid player ID: %s, sub_out_id: %s", payload.match_id, payload.sub_out_id)
        raise HTTPException(status_code=400, detail="Invalid player ID") from exc
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/approve-match/", response_model=MatchResponse)
async def approve_match(payload: ApproveMatch = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.approve_match(payload.match_id, payload.approver_discord_id)
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/contest-report/", response_model=MatchResponse)
async def contest_report(payload: ContestReportRequest = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.contest_report(payload.match_id, payload.contestor_discord_id, payload.reason, payload.discord_message_id)
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/revert-match/", response_model=MatchResponse)
async def revert_match(payload: RevertMatchRequest = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        return await svc.revert_match(payload.match_id)
    except NotFoundError as exc:
        logger.warning("🔴 Match not found. matchID: %s", payload.match_id)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@matches_router.put("/get-leaderboard-ranking/", response_model=LeaderboardRankingResponse)
async def get_leaderboard_ranking(payload: GetLeaderboardRequest = Form(), db=Depends(get_database)):
    svc = MatchService(db)
    try:
        # NOTE: parameter order matters here.
        # - game: civ_version (civ6|civ7)
        # - game_type: PBC|realtime (used to infer cloud)
        # - game_mode: ffa|teamer|duel|combined (match_type)
        return await svc.get_leaderboard(
            match_type=payload.game_mode,
            is_cloud=payload.game_type,
            is_seasonal=payload.is_seasonal,
            is_combined=payload.is_combined,
            civ_version=payload.game,
        )
    except NotFoundError as exc:
        logger.warning("🔴 Invalid game type for leaderboard. game:%s game_mode:%s", payload.game, payload.game_mode)
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except MatchServiceError as exc:
        logger.warning("⚠️ Update error: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


router = APIRouter()
router.include_router(upload_router)
router.include_router(matches_router)

__all__ = ["matches_router", "upload_router", "router"]
