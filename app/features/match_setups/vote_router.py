"""Mite's /vote routes: a vote from the host's voice channel to the draft."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from random import SystemRandom
from typing import Any

from fastapi import APIRouter, Depends, status
from pymongo import AsyncMongoClient

from app.core.dependencies import (
    actor_discord_id,
    actor_is_staff,
    get_database,
    require_mito_token,
)
from app.core.errors import api_error, invalid_request, not_found
from app.features.match_setups.bans import ban_caps, bannable_civs
from app.features.match_setups.deal import DraftRefused, checked_bans
from app.features.match_setups.lobby import (
    CANCELLED,
    DONE,
    LobbyRefused,
    ballot_counted,
    cancel_reason,
    finished,
    lobby_code,
    picked,
    revealed,
    vote_placed,
)
from app.features.match_setups.questions import questions_for
from app.features.match_setups.repository import CodeTaken, MatchSetupRepository
from app.features.match_setups.router import (
    WRITE_ATTEMPTS,
    change_setup,
    civ_data_for,
    lobby_view,
    open_season_id,
)
from app.features.match_setups.schemas import (
    LeaderPick,
    LobbyBallot,
    LobbyCancel,
    VoteBallot,
    VoteMessages,
    VoteRequest,
    VoteView,
)
from app.features.match_setups.vote import new_vote, vote_tallied

router = APIRouter(
    prefix="/api/v2/match-setups/votes",
    tags=["match-setups"],
    dependencies=[Depends(require_mito_token)],
)

_RANDOM = SystemRandom()
VOTE = "vote"


def _draftable(
    civ_data: Mapping[str, Any], age: str | None, edition: str
) -> tuple[list[str], list[str]]:
    """The leaders, and civ7's civs of the age, a vote may ban and deal."""
    leaders = [row["token"] for row in civ_data["leaders"]]
    civs = bannable_civs(civ_data["civs"], age) if edition == "civ7" else []
    return leaders, civs


async def _read(
    db: AsyncMongoClient, vote_id: str
) -> tuple[dict[str, Any], list[str], list[str]]:
    """The vote, with the leaders and civs it may ban and deal."""
    vote = await MatchSetupRepository(db).lobby(vote_id, VOTE)
    if vote is None:
        raise not_found("there is no such vote")
    civ_data = await civ_data_for(db, vote["edition"])
    return vote, *_draftable(civ_data, vote["age"], vote["edition"])


@router.post("", status_code=status.HTTP_201_CREATED, response_model=VoteView)
async def start_vote(
    request: VoteRequest,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A vote open at once, under a code no other open vote here has."""
    if request.edition == "civ6" and (request.age or request.civ_bans):
        raise invalid_request("civ6 has no starting age or civ bans")
    civ_data = await civ_data_for(db, request.edition)
    leaders, civs = _draftable(civ_data, request.age, request.edition)
    caps = ban_caps(request.edition, request.age)
    try:
        bans = [
            {"token": token, "kind": kind}
            for kind, asked, allowed in (
                ("leader", request.leader_bans, leaders),
                ("civ", request.civ_bans, civs),
            )
            for token in checked_bans(asked, allowed, caps[kind], kind)
        ]
    except DraftRefused as exc:
        raise invalid_request(str(exc)) from exc
    for _ in range(WRITE_ATTEMPTS):
        try:
            vote = new_vote(
                **request.model_dump(exclude={"leader_bans", "civ_bans"}),
                host_id=actor,
                bans=bans,
                code=lobby_code(),
                now=datetime.now(UTC),
            )
        except LobbyRefused as exc:
            raise invalid_request(str(exc)) from exc
        try:
            vote["_id"] = await MatchSetupRepository(db).insert(vote)
        except CodeTaken:
            continue
        return lobby_view(vote)
    raise api_error(
        code="CONFLICT",
        message="no free vote code was found",
        status_code=status.HTTP_409_CONFLICT,
        retryable=True,
    )


@router.get("/due", response_model=list[VoteView])
async def votes_due(
    db: AsyncMongoClient = Depends(get_database),
) -> list[dict[str, Any]]:
    """Votes and blind picks past their time, for the bot to move on."""
    due = await MatchSetupRepository(db).due(datetime.now(UTC), VOTE)
    return [lobby_view(vote) for vote in due]


@router.post("/cancel", response_model=VoteView)
async def cancel_vote(
    body: LobbyCancel,
    actor: str = Depends(actor_discord_id),
    staff: bool = Depends(actor_is_staff),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The host, or staff, cancel an open vote by its code."""
    code = body.code.strip().upper()
    found = await MatchSetupRepository(db).lobby_by_code(body.guild_id, code, VOTE)
    if found is None:
        raise not_found("no open vote here has that code")

    def cancelled(vote: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": CANCELLED,
            "cancel_reason": cancel_reason(vote, actor, staff),
            "deadline": None,
            "closed_at": datetime.now(UTC),
        }

    return await change_setup(db, str(found["_id"]), cancelled, VOTE)


@router.get("/{vote_id}", response_model=VoteView)
async def read_vote(
    vote_id: str, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    vote = await MatchSetupRepository(db).lobby(vote_id, VOTE)
    if vote is None:
        raise not_found("there is no such vote")
    return lobby_view(vote)


@router.put("/{vote_id}/messages", response_model=VoteView)
async def place_vote_messages(
    vote_id: str,
    body: VoteMessages,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Where the bot posted the vote's messages, then the blind picks', acting
    as the host."""
    return await change_setup(
        db,
        vote_id,
        lambda vote: vote_placed(vote, actor, body.channel_id, body.messages),
        VOTE,
    )


@router.post("/{vote_id}/finished", response_model=VoteView)
async def finish_vote(
    vote_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    return await change_setup(db, vote_id, lambda vote: finished(vote, actor), VOTE)


@router.post("/{vote_id}/ballot", response_model=VoteView)
async def count_vote_bans(
    vote_id: str,
    body: LobbyBallot,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The ban reactions so far, sent by the bot as the host: a leader or civ
    reaching a majority is banned at once and stays banned."""
    _, leaders, civs = await _read(db, vote_id)
    allowed = {"leader": leaders, "civ": civs} if civs else {"leader": leaders}
    return await change_setup(
        db,
        vote_id,
        lambda vote: ballot_counted(vote, actor, ban_votes=body.bans, allowed=allowed),
        VOTE,
    )


@router.post("/{vote_id}/tally", response_model=VoteView)
async def tally_vote(
    vote_id: str,
    body: VoteBallot,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """Count the settings, the draft mode and the bans, and deal the draft. The
    bot counts, acting as the host, once all finish or the time is up."""
    read, leaders, civs = await _read(db, vote_id)
    season_id = await open_season_id(db, read["edition"])

    def counted(vote: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        changes = vote_tallied(
            vote,
            actor,
            setting_votes=body.settings,
            ban_votes=body.bans,
            questions=questions_for(vote["edition"], vote["mode"]),
            leaders=leaders,
            civs=civs,
            now=now,
            rng=_RANDOM,
        )
        closed = {"closed_at": now} if changes.get("status") == DONE else {}
        return {**changes, "season_id": season_id, **closed}

    return await change_setup(db, vote_id, counted, VOTE)


@router.post("/{vote_id}/pick", response_model=VoteView)
async def pick_in_vote(
    vote_id: str,
    body: LeaderPick,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """A player's blind pick: a leader, and a civ on civ7."""
    return await change_setup(
        db, vote_id, lambda vote: picked(vote, actor, body.leader, body.civ), VOTE
    )


@router.post("/{vote_id}/reveal", response_model=VoteView)
async def reveal_vote_picks(
    vote_id: str,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The blind picks revealed; the bot reveals, acting as the host, once all
    have picked or the time is up."""

    def shown(vote: dict[str, Any]) -> dict[str, Any]:
        return {**revealed(vote, actor), "closed_at": datetime.now(UTC)}

    return await change_setup(db, vote_id, shown, VOTE)
