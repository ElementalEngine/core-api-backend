"""Creating and resolving lobbies.

Half B of playbook Entry 7 begins here. Creation is one insert: the shape is
derived, the season stamped, the roster seated when it fits, and the two
unique indexes do the rest.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from bson import ObjectId

from app.features.lobbies.modes import LobbyShape, resolve_shape
from app.features.lobbies.projection import project_lobby
from app.features.lobbies.schemas import CreateLobbyRequest

PHASE_LOBBY = "lobby"
SOURCE_COMMAND = "command"

# The ids Mongo owns. Everything else on a lobby document is already a JSON
# primitive, a datetime, or a list of them.
OBJECT_ID_FIELDS = ("_id", "season_id")


class InvalidLobbyId(ValueError):
    """The path id is not a well-formed ObjectId.

    Kept distinct from LobbyNotFound on purpose: a malformed id is a caller
    defect (400) and a well-formed id with no document is an ordinary miss
    (404). One answer for both would return 404 for a typo and hide the bug.
    """


class LobbyNotFound(LookupError):
    """No lobby carries that id."""


def as_lobby_id(lobby_id: str) -> ObjectId:
    """The path string as an ObjectId, or InvalidLobbyId.

    ⚠ `ObjectId("nope")` raises `bson.errors.InvalidId`, which no handler
    names, so it would reach D92's catch-all as a 500 -- C5 section 6b's own
    "malformed-ObjectId 500". Tested rather than caught, following
    `MatchService._to_oid`.
    """
    if not ObjectId.is_valid(lobby_id):
        raise InvalidLobbyId("Invalid lobby id")
    return ObjectId(lobby_id)


def _wire_value(value: Any) -> Any:
    """RFC 3339 for a datetime, unchanged for anything else.

    ⚠ `Z`, not `+00:00`. Both name the same instant, but FastAPI chooses
    between them by accident: a route with a response model serialises
    through Pydantic and emits `Z`, while `response_model=None` falls back to
    `jsonable_encoder`, which calls `.isoformat()` and emits `+00:00`.
    `GET /{id}` needs `response_model=None` for its 204, so it disagreed with
    its three siblings on the wire -- measured, Correction 90.

    Converting here removes the choice rather than settling it: a str reaches
    either encoder unchanged, so every lobby route agrees by construction
    instead of by both encoders happening to match.
    """
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


def for_the_wire(
    document: dict[str, Any], viewer_discord_id: str | None
) -> dict[str, Any]:
    """A stored lobby as `viewer_discord_id` may see it, in JSON's types.

    One edge doing two jobs, censoring first (D184, D186). Two calls would be
    two rules to remember at every route; this is one, and the viewer has no
    default, so no route can put a lobby on the wire without deciding who is
    reading it. CP4b shipped three routes that returned the stored document
    raw -- `project_lobby` was on no application path at all.

    ⚠ `_id` and `season_id` are BSON ObjectIds. Every other v2 feature returns
    a Pydantic response model, which declares its ids as `str` and serialises
    them for free; lobbies returns the raw document because D73's projection
    decides the shape per recipient (D179), so nothing converts them and
    nothing typed the boundary. FastAPI's serializer raises on the first one:
    "'ObjectId' object is not iterable", after the write has already landed.

    Copied, never mutated: the caller may still be holding the stored form.
    """
    projected = project_lobby(document, viewer_discord_id)
    return {
        key: str(value)
        if key in OBJECT_ID_FIELDS and value is not None
        else _wire_value(value)
        for key, value in projected.items()
    }


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
        # Mite holds no seat, so the create response is the observer view.
        # A `lobby`-phase document has neither censored surface, so that is
        # the whole document -- and it still crosses the one boundary.
        return for_the_wire(await self._repository.insert_lobby(document), None)

    async def resolve_active(
        self, guild_id: str, channel_id: str, viewer_discord_id: str
    ) -> dict[str, Any] | None:
        """The open lobby for a channel -- one or none, by the D71 index."""
        found = await self._repository.find_open(guild_id, channel_id=channel_id)
        return for_the_wire(found[0], viewer_discord_id) if found else None

    async def read(
        self, lobby_id: str, viewer_discord_id: str, since: int | None = None
    ) -> dict[str, Any] | None:
        """The censored snapshot, or None when `since` already holds its revision.

        D77's revision gate: a polling caller sends the revision it has, and
        None means nothing has moved -- 204 at the route, never 304, which
        would invite cache and proxy semantics into the polling path.

        Raises InvalidLobbyId for a malformed id, LobbyNotFound for a miss.
        """
        found = await self._repository.find_by_id(as_lobby_id(lobby_id))
        if found is None:
            raise LobbyNotFound(lobby_id)
        # Subscript, not .get(): a lobby with no revision is corrupt, and a
        # None here would compare unequal forever and never answer 204.
        if since is not None and found["revision"] == since:
            return None
        return for_the_wire(found, viewer_discord_id)

    async def browse(
        self,
        guild_id: str,
        viewer_discord_id: str,
        edition: str | None = None,
        game_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Open lobbies for a guild, optionally filtered (D180).

        ⚠ `find_open` selects on `closed_at` alone, so this returns lobbies in
        every open phase -- including `settings` and blind `draft`. It is the
        broadest read in the feature and the one that most needs a viewer.
        """
        found = await self._repository.find_open(
            guild_id, edition=edition, game_type=game_type
        )
        return [for_the_wire(lobby, viewer_discord_id) for lobby in found]


__all__ = [
    "InvalidLobbyId",
    "LobbyNotFound",
    "LobbyService",
    "as_lobby_id",
    "build_lobby_document",
    "for_the_wire",
    "seat_the_roster",
]
