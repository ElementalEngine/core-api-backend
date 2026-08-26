"""Creating and resolving lobbies.

Half B of playbook Entry 7 begins here. Creation is one insert: the shape is
derived, the season stamped, the roster seated when it fits, and the two
unique indexes do the rest.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId

from app.features.lobbies.modes import LobbyShape, resolve_shape, validate_seats
from app.features.lobbies.projection import project_lobby
from app.features.lobbies.schemas import (
    ChangeSeatRequest,
    CreateLobbyRequest,
    SeatAction,
)

PHASE_LOBBY = "lobby"
SOURCE_COMMAND = "command"

# D177's staleness threshold. A whole draft is roughly fifteen minutes of
# timers (spec section 7); an hour untouched is abandoned by any reading,
# and every seat change bumps `updated_at`, so a lobby with people in it
# never reaches this.
STALE_AFTER = timedelta(hours=1)

# The ids Mongo owns. Everything else on a lobby document is already a JSON
# primitive, a datetime, or a list of them.
OBJECT_ID_FIELDS = ("_id", "season_id")

logger = logging.getLogger(__name__)


class InvalidLobbyId(ValueError):
    """The path id is not a well-formed ObjectId.

    Kept distinct from LobbyNotFound on purpose: a malformed id is a caller
    defect (400) and a well-formed id with no document is an ordinary miss
    (404). One answer for both would return 404 for a typo and hide the bug.
    """


class LobbyNotFound(LookupError):
    """No lobby carries that id."""


class NotTheHost(PermissionError):
    """Only the host may move somebody else's seat."""


class SeatChangeRefused(Exception):
    """The lobby would not take the change. Carries both revisions.

    One class for every 409 on this path (C5 invariant 5): a stale revision,
    a race lost to D176's `$ne`, and a lobby past `lobby` phase all answer
    CONFLICT with the same two numbers, and the MESSAGE says which.
    """

    def __init__(self, message: str, expected: int, current: int | None) -> None:
        super().__init__(message)
        self.expected = expected
        self.current = current


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
        now = datetime.now(UTC)

        # ⚠ D177. `one_active_seat_per_player` turns a stuck lobby into a
        # stuck player: D74's timers are lazy, and an ABANDONED lobby gets
        # no read to evaluate them, so the seat is held indefinitely and the
        # only symptom is a bare E11000 with no route out. The read that
        # triggers evaluation has to be the NEW lobby's creation -- the one
        # event guaranteed to happen when the lockout matters to somebody.
        roster = [player for player in dict.fromkeys(request.roster) if player]
        if roster:
            for evicted in await self._repository.evict_stale(
                roster, now - STALE_AFTER, now
            ):
                logger.info(
                    "evicted stale lobby. lobby=%s channel=%s updated_at=%s",
                    evicted["_id"],
                    evicted.get("channel_id"),
                    evicted.get("updated_at"),
                )

        document = build_lobby_document(request, shape, season, now)
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

    async def change_seat(
        self, lobby_id: str, actor_discord_id: str, request: ChangeSeatRequest
    ) -> dict[str, Any]:
        """One seat change, returning the updated censored snapshot (D77).

        ⚠ Seats move only while the lobby is in `lobby` phase. Nothing said
        so before -- inferred and recorded as D189, because from `settings`
        on, ballots are per seat, ban turns are per team and `turn_index`
        points into the seating, so a move corrupts state no validator reads.

        Raises InvalidLobbyId, LobbyNotFound, NotTheHost, InvalidSeating for
        an arrangement that cannot exist, and SeatChangeRefused for any 409.
        """
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)

        current = found["revision"]
        if found["phase"] != PHASE_LOBBY:
            raise SeatChangeRefused(
                f"Seats are settled once the lobby reaches {found['phase']}",
                request.expected_revision,
                current,
            )

        target = request.discord_id or actor_discord_id
        if target != actor_discord_id and actor_discord_id != found["host_discord_id"]:
            raise NotTheHost("Only the host can move another player's seat")

        seated = found.get("seats") or []
        arrangement = rearranged(seated, target, request)
        validate_seats(
            arrangement,
            resolve_shape(
                found["game_type"], found.get("number_teams"), found.get("team_size")
            ),
        )

        written = await self._repository.replace_seats(
            oid,
            request.expected_revision,
            arrangement,
            datetime.now(UTC),
            absent_player=None
            if any(seat["discord_id"] == target for seat in seated)
            else target,
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(*self._why_refused(latest, request))
        return for_the_wire(written, actor_discord_id)

    @staticmethod
    def _why_refused(
        lobby: dict[str, Any] | None, request: ChangeSeatRequest
    ) -> tuple[str, int, int | None]:
        """Spec section 9: matched-count zero is stale revision OR already
        seated, and only a re-read tells them apart."""
        current = lobby["revision"] if lobby else None
        if current != request.expected_revision:
            return ("The lobby has moved on", request.expected_revision, current)
        return ("That player already holds a seat", request.expected_revision, current)

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


def rearranged(
    seats: Sequence[Mapping[str, Any]], target: str, request: ChangeSeatRequest
) -> list[dict[str, Any]]:
    """The seat array the request asks for, sorted by `seat_index`.

    ⚠ A move KEEPS the existing seat document and changes its position, so
    whatever the seat carries -- a ballot, later a pool and a pick -- follows
    the player. Rebuilding the seat would silently drop it.

    ⚠ Gaps are left exactly where they are. civup's `arrangeTeamLobbySlots`
    compacts before chunking, which moves a player across a team boundary
    without anyone asking for it (O-19b, C5 invariant 1).
    """
    others = [dict(seat) for seat in seats if seat.get("discord_id") != target]
    if request.action is SeatAction.LEAVE:
        return sorted(others, key=lambda seat: seat["seat_index"])
    existing = next(
        (dict(seat) for seat in seats if seat.get("discord_id") == target), {}
    )
    moved = {
        **existing,
        "discord_id": target,
        "seat_index": request.seat_index,
        "team": request.team,
    }
    return sorted([*others, moved], key=lambda seat: seat["seat_index"])


__all__ = [
    "STALE_AFTER",
    "InvalidLobbyId",
    "LobbyNotFound",
    "LobbyService",
    "NotTheHost",
    "SeatChangeRefused",
    "as_lobby_id",
    "build_lobby_document",
    "for_the_wire",
    "rearranged",
    "seat_the_roster",
]
