"""Creating and resolving lobbies.

Half B of playbook Entry 7 begins here. Creation is one insert: the shape is
derived, the season stamped, the roster seated when it fits, and the two
unique indexes do the rest.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from app.features.lobbies.modes import LobbyShape, resolve_shape
from app.features.lobbies.schemas import CreateLobbyRequest

PHASE_LOBBY = "lobby"
SOURCE_COMMAND = "command"


def seat_the_roster(roster: Sequence[str], shape: LobbyShape) -> list[dict[str, Any]]:
    """Seats for the roster, or none at all.

    ⚠ Fits-or-empty. Fifteen in voice and a 3v3 opens empty: seating an
    arbitrary first six excludes people by list order, and self-selection is
    the rule that matters here (D75, D181).

    ⚠ Deduplicated first. A repeated id would otherwise become two seats for
    one player -- which neither index catches, because MongoDB de-duplicates
    multikey keys per document (D176), and the `$ne` write guard does not
    apply to an insert.

    `team` is null for everyone, including a teamer: seating means "you are
    in this lobby", never "you are on red". Sides are chosen, not assigned.
    """
    unique = list(dict.fromkeys(player for player in roster if player))
    if not unique or len(unique) > shape.seat_count:
        return []
    return [
        {"seat_index": index, "discord_id": player, "team": None}
        for index, player in enumerate(unique)
    ]


def build_lobby_document(
    request: CreateLobbyRequest,
    shape: LobbyShape,
    season: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """The document as it exists at creation.

    ⚠ Fields the lobby has not decided yet are ABSENT, not null: draft_mode,
    map settings and pool size are the settings phase's; bans, picks and
    pool_appearances belong to phases that have not run. `closed_at` is
    absent too, and the partial filters select that exactly as they select an
    explicit null -- measured (D175, Correction 73a).
    """
    document: dict[str, Any] = {
        "guild_id": request.guild_id,
        "channel_id": request.channel_id,
        "host_discord_id": request.host_discord_id,
        "source": SOURCE_COMMAND,
        "season_id": season["_id"],
        "season_label": season["label"],
        "edition": request.edition,
        "game_type": shape.game_type,
        "number_teams": shape.number_teams,
        "team_size": shape.team_size,
        "seat_count": shape.seat_count,
        "min_seats": shape.min_seats,
        "seats": seat_the_roster(request.roster, shape),
        "phase": PHASE_LOBBY,
        "revision": 1,
        "created_at": now,
        # Bumped by every mutation from CP5 on. D177's staleness reads this:
        # turn_expires_at exists only during bans and draft, so D74's timer
        # cannot see a lobby abandoned in `lobby` phase -- the case eviction
        # exists for.
        "updated_at": now,
    }
    if request.instance_id is not None:
        document["instance_id"] = request.instance_id
    return document


class LobbyService:
    def __init__(self, repository: Any, seasons: Any) -> None:
        self._repository = repository
        self._seasons = seasons

    async def create(self, request: CreateLobbyRequest) -> dict[str, Any]:
        """Raises InvalidLobbyShape for a bad mode, LobbyInsertRefused when a
        unique index says the channel or a player is already taken.

        The shape is resolved BEFORE the season lookup, so a malformed request
        never touches the database.
        """
        shape = resolve_shape(
            request.game_type, request.number_teams, request.team_size
        )
        season = await self._seasons.get_current_season(request.edition)
        document = build_lobby_document(request, shape, season, datetime.now(UTC))
        return await self._repository.insert_lobby(document)

    async def resolve_active(
        self, guild_id: str, channel_id: str
    ) -> dict[str, Any] | None:
        """The open lobby for a channel -- one or none, by the D71 index."""
        found = await self._repository.find_open(guild_id, channel_id=channel_id)
        return found[0] if found else None

    async def browse(
        self,
        guild_id: str,
        edition: str | None = None,
        game_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Open lobbies for a guild, optionally filtered (D180)."""
        return await self._repository.find_open(
            guild_id, edition=edition, game_type=game_type
        )


__all__ = ["LobbyService", "build_lobby_document", "seat_the_roster"]
