"""Mite's /vote routes: a vote from the host's voice channel to the draft."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from pymongo import AsyncMongoClient

from app.core.dependencies import (
    actor_discord_id,
    actor_is_staff,
    get_database,
    require_mito_token,
)
from app.core.errors import invalid_request
from app.features.civdata.router import seeded_civ_data
from app.features.match_setups.bans import ban_caps, bannable_civs
from app.features.match_setups.deal import DraftRefused, checked_bans
from app.features.match_setups.lobby import finished, picked, revealed, vote_placed
from app.features.match_setups.questions import questions_for
from app.features.match_setups.repository import MatchSetupRepository
from app.features.match_setups.router import (
    cancel_by_code,
    change_setup,
    insert_coded,
    lobby_view,
    open_season_id,
    read_setup,
)
from app.features.match_setups.schemas import (
    LeaderPick,
    LobbyCancel,
    VoteBallot,
    VoteMessages,
    VoteRequest,
    VoteView,
)
from app.features.match_setups.vote import new_vote, vote_counted, vote_tallied

router = APIRouter(
    prefix="/api/v2/match-setups/votes",
    tags=["match-setups"],
    dependencies=[Depends(require_mito_token)],
)

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
    vote = await read_setup(db, vote_id, VOTE)
    civ_data = await seeded_civ_data(db, vote["edition"])
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
    civ_data = await seeded_civ_data(db, request.edition)
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

    def made(code: str) -> dict[str, Any]:
        return new_vote(
            **request.model_dump(exclude={"leader_bans", "civ_bans"}),
            host_id=actor,
            bans=bans,
            code=code,
            now=datetime.now(UTC),
            questions=questions_for(request.edition, request.mode),
        )

    return await insert_coded(db, made, VOTE)


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
    """The starter, or staff, cancel an open vote by its code."""
    return await cancel_by_code(db, body, actor, staff, VOTE)


@router.get("/{vote_id}", response_model=VoteView)
async def read_vote(
    vote_id: str, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    return lobby_view(await read_setup(db, vote_id, VOTE))


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
async def count_vote_so_far(
    vote_id: str,
    body: VoteBallot,
    actor: str = Depends(actor_discord_id),
    db: AsyncMongoClient = Depends(get_database),
) -> dict[str, Any]:
    """The reactions so far, sent by the bot as the host: a setting option or a
    ban reaching a majority is won at once and stays won."""
    _, leaders, civs = await _read(db, vote_id)

    def counted(vote: dict[str, Any]) -> dict[str, Any]:
        return vote_counted(
            vote,
            actor,
            setting_votes=body.settings,
            ban_votes=body.bans,
            questions=questions_for(vote["edition"], vote["mode"]),
            leaders=leaders,
            civs=civs,
        )

    return await change_setup(db, vote_id, counted, VOTE)


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
        changes = vote_tallied(
            vote,
            actor,
            setting_votes=body.settings,
            ban_votes=body.bans,
            questions=questions_for(vote["edition"], vote["mode"]),
            leaders=leaders,
            civs=civs,
            now=datetime.now(UTC),
        )
        return {**changes, "season_id": season_id}

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
    return await change_setup(db, vote_id, lambda vote: revealed(vote, actor), VOTE)


__all__ = ["router"]
