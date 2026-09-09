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

from app.features.lobbies.bans import bannable_civs, resolve_bans
from app.features.lobbies.modes import (
    InvalidSeating,
    LobbyShape,
    resolve_shape,
    validate_seats,
)
from app.features.lobbies.phases import (
    BANS,
    CANCEL_BY_HOST,
    CANCEL_NO_POOL,
    CANCELLED,
    COMPLETE,
    DRAFT,
    LOBBY,
    SETTINGS,
    SOURCE_COMMAND,
)
from app.features.lobbies.pools import (
    NotEnoughPool,
    assign_one_each,
    deal,
    remaining_after_bans,
)
from app.features.lobbies.projection import project_lobby
from app.features.lobbies.questions import questions_for
from app.features.lobbies.schemas import (
    ChangeSeatRequest,
    CreateLobbyRequest,
    SeatAction,
    SubmitBallotRequest,
    SubmitBansRequest,
    SubmitPickRequest,
)
from app.features.lobbies.tally import resolve_settings

# D177's staleness threshold. A whole draft is roughly fifteen minutes of
# timers (spec section 7); an hour untouched is abandoned by any reading,
# and every seat change bumps `updated_at`, so a lobby with people in it
# never reaches this.
STALE_AFTER = timedelta(hours=1)

# Spec section 7. Bans is one window here; D72's two mechanisms -- a majority
# vote in FFA, 1-2-2-1 captain turns in a teamer -- differ per turn and are
# CP6b's, so 6a sets the phase's first deadline and nothing finer.
SETTINGS_WINDOW = timedelta(minutes=5)
DRAFT_RANDOM = "random"
BANS_WINDOW = timedelta(minutes=5)
DRAFT_WINDOW = timedelta(minutes=5)

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


class PickIsFinal(Exception):
    """The seat already holds a pick, and a pick cannot be changed (O-34)."""


class NotSeated(PermissionError):
    """Only a seated player votes. The host is not special here."""


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
        "phase": LOBBY,
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
    if request.starting_age is not None:
        document["starting_age"] = request.starting_age
    return document


class LobbyService:
    def __init__(self, repository: Any, seasons: Any, civ_data: Any = None) -> None:
        self._repository = repository
        self._seasons = seasons
        # ⚠ Only the ban path reads it, so it defaults rather than forcing
        # nineteen test call sites to pass a fake they never use. A ban
        # submission with it absent raises loudly at the first attribute
        # access; nothing reaches Mongo on a half-built service.
        self._civ_data = civ_data

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

    async def _advanced(self, lobby: dict[str, Any]) -> dict[str, Any]:
        """The lobby, with any expired deadline already applied (D74, D194).

        ⚠ Timers are lazy: nothing sweeps them, so the next read or write past
        `turn_expires_at` is what advances the phase. That has to include the
        POLL -- a settings phase in a lobby nobody is writing to would
        otherwise never expire, which is D177's lockout in a different place.

        The write is revision-guarded, so fourteen clients polling the same
        expired lobby produce exactly one advance; the losers get None and
        keep the document they read, which the winner has already superseded.
        """
        expires = lobby.get("turn_expires_at")
        # `core/db.py` opens the client tz_aware, so this compares two aware
        # datetimes and needs no coercion.
        if expires is None or expires > datetime.now(UTC):
            return lobby
        if lobby["phase"] == SETTINGS:
            return await self._resolve_settings(lobby) or lobby
        if lobby["phase"] == BANS:
            return await self._resolve_bans(lobby) or lobby
        return lobby

    async def _resolve_settings(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Tally the ballots and move to `bans`. None if the lobby moved.

        ⚠ Every question gets an answer, including from an empty room: a
        question no seat answered locks to its default (D191), which is what
        makes an expired settings phase safe to advance rather than stall.
        """
        now = datetime.now(UTC)
        return await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {
                "settings": resolve_settings(
                    lobby.get("seats") or [],
                    questions_for(lobby["edition"], lobby["game_type"]),
                    str(lobby["_id"]),
                ),
                "phase": BANS,
                "turn_index": 0,
                "turn_expires_at": now + BANS_WINDOW,
            },
            now,
        )

    async def _deal_the_draft(self, lobby: dict[str, Any]) -> dict[str, Any]:
        """What leaving `bans` writes, by draft mode (D193, D198).

        ⚠ `random` has no draft: leaders are assigned straight from the
        post-ban pool and the lobby is DONE. That is the `bans -> complete`
        edge section 3's forward-only sequence does not otherwise permit, and
        it was deferred until the dealer existed to make `complete` truthful.

        ⚠ A lobby can ban itself past a usable pool. The caller CANCELS
        rather than letting this raise: after D194 an advance can be triggered
        by a POLL, so raising would make every read a 500 with no route out.
        """
        payload = await self._civ_data.fetch(lobby["edition"])
        landed = lobby.get("bans") or {}
        seated = [seat for seat in lobby.get("seats") or [] if seat.get("discord_id")]
        players = len(seated)
        mode = (lobby.get("settings") or {}).get("draft_mode")

        # ⚠ civ7 drafts a leader AND a civ, from two independently dealt
        # pools. civ6 is leaders only, so its civ list is empty and every
        # civ-side call below no-ops rather than needing a branch.
        kinds = {
            "leader": remaining_after_bans(
                [row["token"] for row in payload["leaders"]],
                landed.get("leader") or [],
            ),
            "civ": remaining_after_bans(
                [row["token"] for row in payload["civs"]], landed.get("civ") or []
            ),
        }

        per_seat: dict[str, dict[str, Any]] = {
            seat["discord_id"]: {} for seat in seated
        }
        for kind, pool in kinds.items():
            if not pool:
                continue
            field = "pick" if mode == DRAFT_RANDOM else "pool"
            suffix = "" if kind == "leader" else "civ_"
            if mode == DRAFT_RANDOM:
                allotted: list[Any] = list(assign_one_each(pool, players))
            else:
                allotted = list(deal(pool, players))
            for seat, share in zip(seated, allotted, strict=True):
                per_seat[seat["discord_id"]][f"{suffix}{field}"] = share

        seats = [
            {**seat, **per_seat[seat["discord_id"]]} if seat.get("discord_id") else seat
            for seat in lobby.get("seats") or []
        ]
        if mode == DRAFT_RANDOM:
            return {
                "seats": seats,
                "phase": COMPLETE,
                "closed_at": datetime.now(UTC),
                "turn_expires_at": None,
            }
        return {
            "seats": seats,
            "phase": DRAFT,
            "turn_index": 0,
            "turn_expires_at": datetime.now(UTC) + DRAFT_WINDOW,
        }

    async def _resolve_bans(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Tally the bans and move to `draft`. None if the lobby moved.

        ⚠ No civ-data here, deliberately. Tokens were checked when they were
        submitted, so the advance needs only what the document already holds
        -- which keeps a 503 for unseeded civ-data off the POLL path, where
        "your lobby is unavailable because a seed is missing" is a poor
        answer to "what is the state of my lobby".

        ⚠ `random` advances to `draft` like every other mode for now. D193
        has it skip to `complete`, but that shortcut belongs with the
        assignment that makes `complete` truthful (CP6c): a terminal lobby
        with no picks either lacks `closed_at`, breaking section 3, or sets
        it and frees the channel on a lobby that never resolved.
        """
        now = datetime.now(UTC)
        tallied = {
            **lobby,
            "bans": resolve_bans(
                lobby.get("seats") or [],
                lobby["edition"],
                lobby.get("starting_age"),
            ),
        }
        try:
            changes = await self._deal_the_draft(tallied)
        except NotEnoughPool as exc:
            logger.warning("lobby banned itself out. lobby=%s %s", lobby["_id"], exc)
            changes = {
                "phase": CANCELLED,
                "cancel_reason": CANCEL_NO_POOL,
                "closed_at": now,
                "turn_expires_at": None,
            }
        return await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {"bans": tallied["bans"], **changes},
            now,
        )

    async def cancel(
        self, lobby_id: str, actor_discord_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """The host abandons the lobby. Terminal, and it frees the channel.

        ⚠ Host only. Any seated player leaving is `PATCH /seats`; cancelling
        ends it for everyone, which is the host's call alone.
        """
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        if found.get("closed_at") is not None:
            raise SeatChangeRefused(
                f"This lobby is already {found['phase']}",
                expected_revision,
                found["revision"],
            )
        if actor_discord_id != found["host_discord_id"]:
            raise NotTheHost("Only the host can cancel the lobby")

        now = datetime.now(UTC)
        written = await self._repository.apply_changes(
            oid,
            expected_revision,
            {
                "phase": CANCELLED,
                "cancel_reason": CANCEL_BY_HOST,
                "closed_at": now,
                "turn_expires_at": None,
            },
            now,
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(*self._why_refused(latest, expected_revision))
        return for_the_wire(written, actor_discord_id)

    async def submit_pick(
        self, lobby_id: str, actor_discord_id: str, request: SubmitPickRequest
    ) -> dict[str, Any]:
        """One seat's pick. Completes the lobby once every seat has picked."""
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        found = await self._advanced(found)
        if found["phase"] != DRAFT:
            raise SeatChangeRefused(
                f"The draft is not open at {found['phase']}",
                request.expected_revision,
                found["revision"],
            )

        seats = found.get("seats") or []
        mine = next((s for s in seats if s.get("discord_id") == actor_discord_id), None)
        if mine is None:
            raise NotSeated("Only a seated player picks")
        # ⚠ O-34, and NOT a revision check: read at revision 5, change your
        # mind, write at revision 5, and a revision guard is satisfied.
        if mine.get("pick") is not None:
            raise PickIsFinal("Your pick is already locked in")

        wanted = {"pick": (request.token, mine.get("pool"))}
        if request.civ_token is not None:
            wanted["civ_pick"] = (request.civ_token, mine.get("civ_pool"))
        chosen: dict[str, Any] = {}
        for field, (token, pool) in wanted.items():
            if token not in (pool or []):
                raise InvalidSeating(field, f"{token} is not in your pool")
            chosen[field] = token

        picked = [
            {**seat, **chosen} if seat.get("discord_id") == actor_discord_id else seat
            for seat in seats
        ]
        now = datetime.now(UTC)
        written = await self._repository.replace_seats(
            oid, request.expected_revision, picked, now
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )

        occupied = [seat for seat in picked if seat.get("discord_id")]
        if all(seat.get("pick") is not None for seat in occupied):
            # ⚠ `revealed_at` is what un-censors a blind draft (D73). It is set
            # for every mode: `complete` already reveals, so a stray mode
            # cannot leave a finished lobby hidden.
            written = (
                await self._repository.apply_changes(
                    oid,
                    written["revision"],
                    {
                        "phase": COMPLETE,
                        "revealed_at": now,
                        "closed_at": now,
                        "turn_expires_at": None,
                    },
                    now,
                )
                or written
            )
        return for_the_wire(written, actor_discord_id)

    async def submit_bans(
        self, lobby_id: str, actor_discord_id: str, request: SubmitBansRequest
    ) -> dict[str, Any]:
        """One seat's bans. Resolves the phase once every seat has submitted."""
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        found = await self._advanced(found)
        if found["phase"] != BANS:
            raise SeatChangeRefused(
                f"Bans are not open at {found['phase']}",
                request.expected_revision,
                found["revision"],
            )

        seats = found.get("seats") or []
        if not any(seat.get("discord_id") == actor_discord_id for seat in seats):
            raise NotSeated("Only a seated player bans")

        payload = await self._civ_data.fetch(found["edition"])
        legal = {
            "leader": {row["token"] for row in payload["leaders"]},
            # ⚠ Age-filtered. A civ outside the chosen starting age is a valid
            # token for a civ that is not in the game, and banning it burns
            # one of only three slots -- a client bug, not collusion.
            "civ": set(bannable_civs(payload["civs"], found.get("starting_age"))),
        }
        submitted = {"leader": request.leader_keys, "civ": request.civ_keys}
        for kind, keys in submitted.items():
            unknown = [key for key in keys if key not in legal[kind]]
            if unknown:
                raise InvalidSeating(f"{kind}_keys", f"not bannable: {unknown[0]}")

        banned = [
            {
                **seat,
                "bans": {
                    "leader_keys": list(request.leader_keys),
                    "civ_keys": list(request.civ_keys),
                },
            }
            if seat.get("discord_id") == actor_discord_id
            else seat
            for seat in seats
        ]
        now = datetime.now(UTC)
        written = await self._repository.replace_seats(
            oid, request.expected_revision, banned, now
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )

        occupied = [seat for seat in banned if seat.get("discord_id")]
        if all(seat.get("bans") is not None for seat in occupied):
            written = await self._resolve_bans(written) or written
        return for_the_wire(written, actor_discord_id)

    async def submit_ballot(
        self, lobby_id: str, actor_discord_id: str, request: SubmitBallotRequest
    ) -> dict[str, Any]:
        """One seat's ballot. Resolves the phase once every seat has voted."""
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        found = await self._advanced(found)
        if found["phase"] != SETTINGS:
            raise SeatChangeRefused(
                f"The settings vote is not open at {found['phase']}",
                request.expected_revision,
                found["revision"],
            )

        seats = found.get("seats") or []
        if not any(seat.get("discord_id") == actor_discord_id for seat in seats):
            raise NotSeated("Only a seated player votes")

        catalogue = {
            question["id"]: question
            for question in questions_for(found["edition"], found["game_type"])
        }
        for question_id, raw in request.selections.items():
            question = catalogue.get(question_id)
            if question is None:
                raise InvalidSeating("selections", f"no question {question_id!r}")
            offered = {option["id"] for option in question["options"]}
            chosen = [c for c in str(raw).split("|") if c]
            if not chosen or any(c not in offered for c in chosen):
                raise InvalidSeating("selections", f"bad option for {question_id}")
            if len(chosen) > question.get("max_selections", 1):
                raise InvalidSeating(
                    "selections", f"too many choices for {question_id}"
                )

        # The whole ballot replaces what the seat had, so a dropped answer
        # cannot leave a stale one behind.
        voted = [
            {**seat, "ballot": dict(request.selections)}
            if seat.get("discord_id") == actor_discord_id
            else seat
            for seat in seats
        ]
        now = datetime.now(UTC)
        written = await self._repository.replace_seats(
            oid, request.expected_revision, voted, now
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )

        occupied = [seat for seat in voted if seat.get("discord_id")]
        if all(seat.get("ballot") for seat in occupied):
            written = await self._resolve_settings(written) or written
        return for_the_wire(written, actor_discord_id)

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
        found = await self._advanced(found)
        # Subscript, not .get(): a lobby with no revision is corrupt, and a
        # None here would compare unequal forever and never answer 204.
        if since is not None and found["revision"] == since:
            return None
        return for_the_wire(found, viewer_discord_id)

    async def start(
        self, lobby_id: str, actor_discord_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """Close seating and open the settings vote (D190).

        ⚠ Section 7's advance table has no `lobby -> settings` row and there
        is no "all submitted" condition to hang it on, so this is the one
        transition a timer cannot cover -- which is the restore trigger D94
        recorded when it deleted the general `/advance`. Narrow on purpose:
        one transition, one caller.

        Not automatic at `min_seats`. FFA seats twelve and needs six, so the
        sixth arrival does not mean nobody else is coming.
        """
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        if found["phase"] != LOBBY:
            raise SeatChangeRefused(
                f"This lobby is already at {found['phase']}",
                expected_revision,
                found["revision"],
            )
        if actor_discord_id != found["host_discord_id"]:
            raise NotTheHost("Only the host can start the lobby")

        seats = found.get("seats") or []
        seated = len([seat for seat in seats if seat.get("discord_id")])
        if seated < found["min_seats"]:
            raise SeatChangeRefused(
                f"{found['min_seats']} players are needed to start,"
                f" and {seated} are seated",
                expected_revision,
                found["revision"],
            )

        now = datetime.now(UTC)
        written = await self._repository.apply_changes(
            oid,
            expected_revision,
            {"phase": SETTINGS, "turn_expires_at": now + SETTINGS_WINDOW},
            now,
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(*self._why_refused(latest, expected_revision))
        return for_the_wire(written, actor_discord_id)

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
        if found["phase"] != LOBBY:
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
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )
        return for_the_wire(written, actor_discord_id)

    @staticmethod
    def _why_refused(
        lobby: dict[str, Any] | None, expected: int
    ) -> tuple[str, int, int | None]:
        """Spec section 9: matched-count zero is stale revision OR already
        seated, and only a re-read tells them apart."""
        current = lobby["revision"] if lobby else None
        if current != expected:
            return ("The lobby has moved on", expected, current)
        return ("That player already holds a seat", expected, current)

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
    "NotSeated",
    "NotTheHost",
    "PickIsFinal",
    "SeatChangeRefused",
    "as_lobby_id",
    "build_lobby_document",
    "for_the_wire",
    "rearranged",
    "seat_the_roster",
]
