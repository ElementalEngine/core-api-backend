"""Creating and resolving lobbies.

Creation is one insert: the shape is derived from the game type and the size
the host chose, the season is stamped, the host takes the first seat, and the
two unique indexes stop a second lobby in the channel or a player holding two
seats at once. Everyone else joins through the seat route.
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
    CANCEL_NO_PICK,
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
    civ_target,
    deal,
    deal_civs,
    remaining_after_bans,
)
from app.features.lobbies.projection import project_lobby
from app.features.lobbies.questions import questions_for
from app.features.lobbies.schemas import (
    ChangeSeatRequest,
    CreateLobbyRequest,
    MarkReadyRequest,
    SeatAction,
    SubmitBallotRequest,
    SubmitBansRequest,
    SubmitPickRequest,
)
from app.features.lobbies.stats import contributions
from app.features.lobbies.tally import resolve_settings
from app.features.lobbies.turns import cwc_order, whose_turn

STALE_AFTER = timedelta(hours=1)

SETTINGS_WINDOW = timedelta(minutes=5)
DRAFT_RANDOM = "random"
DRAFT_CWC = "cwc"
DRAFT_STANDARD = "standard"
BANS_WINDOW = timedelta(minutes=5)
# One cwc turn, for a ban or for a pick.
CWC_TURN = timedelta(seconds=30)
DRAFT_WINDOW = timedelta(minutes=5)

# The ids Mongo owns. Everything else on a lobby document is already a JSON
# primitive, a datetime, or a list of them.
OBJECT_ID_FIELDS = ("_id", "season_id")

logger = logging.getLogger(__name__)


class InvalidLobbyId(ValueError):
    """The path id is not a well-formed ObjectId."""


class LobbyNotFound(LookupError):
    """No lobby carries that id."""


class NotYourTurn(PermissionError):
    """Somebody else is owed this pick."""


class PickIsFinal(Exception):
    """The seat already holds a pick, and a pick cannot be changed."""


class NotSeated(PermissionError):
    """Only a seated player votes. The host is not special here."""


class NotTheHost(PermissionError):
    """Only the host may move somebody else's seat."""


class SeatChangeRefused(Exception):
    """The lobby would not take the change. Carries both revisions."""

    def __init__(self, message: str, expected: int, current: int | None) -> None:
        super().__init__(message)
        self.expected = expected
        self.current = current


def as_lobby_id(lobby_id: str) -> ObjectId:
    """The path string as an ObjectId, or InvalidLobbyId."""
    if not ObjectId.is_valid(lobby_id):
        raise InvalidLobbyId("Invalid lobby id")
    return ObjectId(lobby_id)


def _wire_value(value: Any) -> Any:
    """RFC 3339 for a datetime, unchanged for anything else."""
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


def for_the_wire(
    document: dict[str, Any],
    viewer_discord_id: str | None,
    *,
    uncensored: bool = False,
) -> dict[str, Any]:
    """A stored lobby as `viewer_discord_id` may see it, in JSON's types.

    `uncensored` is for staff observers, who need a blind draft's picks to
    adjudicate it. The secrecy is between players.
    """
    projected = (
        dict(document) if uncensored else project_lobby(document, viewer_discord_id)
    )
    return {
        key: str(value)
        if key in OBJECT_ID_FIELDS and value is not None
        else _wire_value(value)
        for key, value in projected.items()
    }


def seat_the_host(host_discord_id: str) -> list[dict[str, Any]]:
    """The one seat a lobby opens with.

    Everyone else joins through the seat route, so the lobby fills by
    self-selection rather than by whoever happened to be in voice when the
    command ran. `team` is null even in a teamer: seating means you are in
    this lobby, never that you are on a particular side.
    """
    return [{"seat_index": 0, "discord_id": host_discord_id, "team": None}]


def shape_of(lobby: Mapping[str, Any]) -> LobbyShape:
    """The shape of a lobby that already exists.

    Only a teamer's team fields came from the caller; a duel's are values
    resolve_shape itself derived and the builder stored, so handing them back
    would look like a caller sending team fields for a duel. An ffa's seat
    count did come from the caller and is read back.
    """
    teamer = lobby["game_type"] == "teamer"
    return resolve_shape(
        lobby["game_type"],
        lobby.get("number_teams") if teamer else None,
        lobby.get("team_size") if teamer else None,
        lobby.get("seat_count"),
    )


def build_lobby_document(
    request: CreateLobbyRequest,
    shape: LobbyShape,
    season: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """The document as it exists at creation."""
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
        "seats": seat_the_host(request.host_discord_id),
        "voice_channel_id": request.voice_channel_id,
        "phase": LOBBY,
        "revision": 1,
        "created_at": now,
        "updated_at": now,
    }
    if request.instance_id is not None:
        document["instance_id"] = request.instance_id
    if request.starting_age is not None:
        document["starting_age"] = request.starting_age
    if request.host_rules:
        document["host_rules"] = request.host_rules
    if request.draft_mode is not None:
        document["settings"] = {"draft_mode": request.draft_mode}
    return document


class LobbyService:
    def __init__(self, repository: Any, seasons: Any, civ_data: Any = None) -> None:
        self._repository = repository
        self._seasons = seasons
        self._civ_data = civ_data

    async def create(self, request: CreateLobbyRequest) -> dict[str, Any]:
        """
        Raises InvalidLobbyShape for a bad mode, LobbyInsertRefused when a unique
        index says the channel or a player is already taken.
        """
        shape = resolve_shape(
            request.game_type,
            request.number_teams,
            request.team_size,
            request.size,
        )
        season = await self._seasons.get_current_season(request.edition)
        now = datetime.now(UTC)

        for evicted in await self._repository.evict_stale(
            [request.host_discord_id], now - STALE_AFTER, now
        ):
            logger.info(
                "evicted stale lobby. lobby=%s channel=%s updated_at=%s",
                evicted["_id"],
                evicted.get("channel_id"),
                evicted.get("updated_at"),
            )

        document = build_lobby_document(request, shape, season, now)
        return for_the_wire(await self._repository.insert_lobby(document), None)

    async def claim_post(self, guild_id: str) -> dict[str, Any] | None:
        """The next finished lobby Mite should post, or None for 204."""
        claimed = await self._repository.claim_for_posting(guild_id, datetime.now(UTC))
        return None if claimed is None else for_the_wire(claimed, None)

    async def _count_the_lobby(self, lobby_id: Any) -> None:
        """Fold a finished lobby into `lobby_stats`, once and only once."""
        try:
            claimed = await self._repository.claim_for_stats(
                lobby_id, datetime.now(UTC)
            )
            if claimed is None:
                return
            counts = contributions(claimed)
            if not counts:
                return
            await self._repository.add_contributions(
                {
                    "season_id": claimed.get("season_id"),
                    "edition": claimed["edition"],
                    "game_type": claimed["game_type"],
                },
                counts,
            )
        except Exception:
            logger.exception("lobby stats not counted. lobby=%s", lobby_id)

    async def _seated_in(
        self, lobby_id: str, phase: str, actor: str, expected_revision: int, verb: str
    ) -> tuple[Any, dict[str, Any], list[dict[str, Any]]]:
        """Load a lobby, apply expired timers, and check the actor may act.

        Every phase submission opens this way. Returns the object id, the
        lobby as it stands after any advance, and its seats.
        """
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        found = await self._advanced(found)
        if found["phase"] != phase:
            raise SeatChangeRefused(
                f"{verb} is not open at {found['phase']}",
                expected_revision,
                found["revision"],
            )
        seats = found.get("seats") or []
        if not any(seat.get("discord_id") == actor for seat in seats):
            raise NotSeated(f"Only a seated player {verb.split()[-1]}s")
        return oid, found, seats

    async def _advanced(self, lobby: dict[str, Any]) -> dict[str, Any]:
        """The lobby, with any expired deadline already applied."""
        expires = lobby.get("turn_expires_at")
        # `core/db.py` opens the client tz_aware, so this compares two aware
        # datetimes and needs no coercion.
        if expires is None or expires > datetime.now(UTC):
            return lobby
        if lobby["phase"] == SETTINGS:
            return await self._resolve_settings(lobby) or lobby
        if lobby["phase"] == BANS:
            if lobby.get("ban_order"):
                return await self._skip_cwc_ban(lobby) or lobby
            return await self._resolve_bans(lobby) or lobby
        if lobby["phase"] == DRAFT:
            return await self._abandon_draft(lobby) or lobby
        return lobby

    async def _abandon_draft(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Cancel a draft nobody finished.

        Unlike settings and bans there is no safe default: assigning a leader
        on somebody's behalf decides their game for them. The reason names
        who was still owed a pick so the embed can say why.
        """
        order = lobby.get("pick_order")
        if order:
            owed = [whose_turn(order, lobby.get("turn_index") or 0)]
        else:
            owed = [
                seat["discord_id"]
                for seat in lobby.get("seats") or []
                if seat.get("discord_id") and seat.get("pick") is None
            ]
        now = datetime.now(UTC)
        logger.info("draft abandoned. lobby=%s waiting on %s", lobby["_id"], owed)
        return await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {
                "phase": CANCELLED,
                "cancel_reason": CANCEL_NO_PICK,
                "no_pick_from": [who for who in owed if who],
                "closed_at": now,
                "turn_expires_at": None,
            },
            now,
        )

    async def _skip_cwc_ban(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """A captain who runs out of time loses the ban, not the lobby."""
        turn_index = (lobby.get("turn_index") or 0) + 1
        now = datetime.now(UTC)
        logger.info(
            "cwc ban turn skipped. lobby=%s turn=%s", lobby["_id"], turn_index - 1
        )
        if whose_turn(lobby["ban_order"], turn_index) is None:
            return await self._deal_after_cwc_bans({**lobby, "turn_index": turn_index})
        return await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {"turn_index": turn_index, "turn_expires_at": now + CWC_TURN},
            now,
        )

    @staticmethod
    def _unready(seats: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Seats with their ready flags cleared.

        Ready means finished with THIS phase. Carrying it into the next one
        opens that phase with everybody already done, so the first write to
        touch the lobby ends it.
        """
        return [{k: v for k, v in seat.items() if k != "ready"} for seat in seats]

    async def _resolve_settings(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Tally the ballots and move to `bans`. None if the lobby moved."""
        now = datetime.now(UTC)
        return await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {
                # Merged, not replaced: a teamer's draft mode was written at
                # creation and is not on the ballot to be re-resolved.
                "settings": {
                    **(lobby.get("settings") or {}),
                    **resolve_settings(
                        lobby.get("seats") or [],
                        questions_for(lobby["edition"], lobby["game_type"]),
                        str(lobby["_id"]),
                    ),
                },
                "phase": BANS,
                "seats": self._unready(lobby.get("seats") or []),
                "turn_index": 0,
                "turn_expires_at": now + BANS_WINDOW,
            },
            now,
        )

    @staticmethod
    def _captains(seats: list[dict[str, Any]]) -> list[str]:
        """Seat one of each team, in team order."""
        lowest: dict[int, dict[str, Any]] = {}
        for seat in seats:
            team = seat.get("team")
            if team is None or not seat.get("discord_id"):
                continue
            if team not in lowest or seat["seat_index"] < lowest[team]["seat_index"]:
                lowest[team] = seat
        return [lowest[key]["discord_id"] for key in sorted(lowest)]

    async def _deal_the_draft(self, lobby: dict[str, Any]) -> dict[str, Any]:
        """What leaving `bans` writes, by draft mode."""
        payload = await self._civ_data.fetch(lobby["edition"])
        landed = lobby.get("bans") or {}
        seated = [seat for seat in lobby.get("seats") or [] if seat.get("discord_id")]
        mode = (lobby.get("settings") or {}).get("draft_mode")

        captains = self._captains(seated)
        mode = self._workable_mode(lobby, mode, captains)

        kinds = {
            "leader": remaining_after_bans(
                [row["token"] for row in payload["leaders"]],
                landed.get("leader") or [],
            ),
            "civ": remaining_after_bans(
                [row["token"] for row in payload["civs"]], landed.get("civ") or []
            ),
        }

        shared, per_seat = self._dealt_pools(lobby, mode, kinds, seated)

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
        ordered: dict[str, Any] = {}
        if mode == DRAFT_CWC:
            ordered["pick_order"] = cwc_order(
                captains, (lobby.get("team_size") or 0) * 2
            )
            ordered["teams"] = [
                {"team_index": index, "leaders": [], "civs": []} for index in range(2)
            ]

        if mode != (lobby.get("settings") or {}).get("draft_mode"):
            ordered["settings"] = {
                **(lobby.get("settings") or {}),
                "draft_mode": mode,
            }
        return {
            "seats": self._unready(seats),
            "phase": DRAFT,
            "turn_index": 0,
            "turn_expires_at": datetime.now(UTC) + DRAFT_WINDOW,
            **shared,
            **ordered,
        }

    def _workable_mode(
        self, lobby: dict[str, Any], mode: str | None, captains: list[str]
    ) -> str | None:
        """The voted mode, or standard when cwc cannot run in this shape.

        Decided before anything is dealt: a fallback needs per-seat pools
        where cwc needs one shared pool.
        """
        if mode != DRAFT_CWC:
            return mode
        try:
            cwc_order(captains, (lobby.get("team_size") or 0) * 2)
        except ValueError as exc:
            logger.warning(
                "cwc unavailable, falling back to standard. lobby=%s %s",
                lobby["_id"],
                exc,
            )
            return DRAFT_STANDARD
        return mode

    def _dealt_pools(
        self,
        lobby: dict[str, Any],
        mode: str | None,
        kinds: dict[str, list[str]],
        seated: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        """What each seat is offered, and what the lobby holds in common.

        cwc draws from one shared pool; every other mode deals per seat.
        """
        players = len(seated)
        if mode == DRAFT_CWC:
            shared: dict[str, Any] = {"pool": list(kinds["leader"])}
            if kinds["civ"]:
                shared["civ_pool"] = list(kinds["civ"])
            return shared, {seat["discord_id"]: {} for seat in seated}

        per_seat: dict[str, dict[str, Any]] = {s["discord_id"]: {} for s in seated}
        for kind, pool in kinds.items():
            if not pool:
                continue
            field = "pick" if mode == DRAFT_RANDOM else "pool"
            suffix = "" if kind == "leader" else "civ_"
            if mode == DRAFT_RANDOM:
                allotted: list[Any] = list(assign_one_each(pool, players))
            elif kind == "civ":
                # Civs are dealt to a target and may repeat across pools;
                # leaders are split and never do.
                groups = lobby.get("number_teams") or players
                allotted = list(
                    deal_civs(pool, players, civ_target(lobby["game_type"], groups))
                )
            else:
                allotted = list(deal(pool, players))
            for seat, share in zip(seated, allotted, strict=True):
                per_seat[seat["discord_id"]][f"{suffix}{field}"] = share
        return {}, per_seat

    async def _ban_in_turn(
        self,
        oid: Any,
        lobby: dict[str, Any],
        actor_discord_id: str,
        request: SubmitBansRequest,
    ) -> dict[str, Any]:
        """One captain's turn in a cwc ban order.

        A turn bans one leader, and one civ as well in civ7. The bans land on
        the lobby rather than on a seat: nobody is banning for themselves.
        """
        order = lobby["ban_order"]
        turn_index = lobby.get("turn_index") or 0
        if whose_turn(order, turn_index) != actor_discord_id:
            raise NotYourTurn("It is not your turn to ban")

        wants_civ = lobby["edition"] == "civ7"
        expected = 1 + int(wants_civ)
        if len(request.leader_keys) != 1 or len(request.civ_keys) != int(wants_civ):
            raise InvalidSeating(
                "leader_keys", f"a turn bans exactly {expected} key(s)"
            )

        landed = lobby.get("bans") or {}
        already = set(landed.get("leader") or []) | set(landed.get("civ") or [])
        for key in [*request.leader_keys, *request.civ_keys]:
            if key in already:
                raise InvalidSeating("leader_keys", f"{key} is already banned")

        now = datetime.now(UTC)
        written = await self._repository.apply_changes(
            oid,
            request.expected_revision,
            {
                "bans": {
                    "leader": [*(landed.get("leader") or []), *request.leader_keys],
                    "civ": [*(landed.get("civ") or []), *request.civ_keys],
                },
                "turn_index": turn_index + 1,
                "turn_expires_at": now + CWC_TURN,
            },
            now,
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )
        if whose_turn(order, turn_index + 1) is None:
            written = await self._deal_after_cwc_bans(written) or written
        return for_the_wire(written, actor_discord_id)

    async def _deal_after_cwc_bans(
        self, lobby: dict[str, Any]
    ) -> dict[str, Any] | None:
        """The ban order is spent, so the draft is dealt from what is left."""
        now = datetime.now(UTC)
        changes = await self._deal_or_cancel(lobby, now)
        return await self._repository.apply_changes(
            lobby["_id"], lobby["revision"], changes, now
        )

    async def _deal_or_cancel(
        self, lobby: dict[str, Any], now: datetime
    ) -> dict[str, Any]:
        """The draft, or a cancellation when the bans left too small a pool."""
        try:
            return await self._deal_the_draft(lobby)
        except NotEnoughPool as exc:
            logger.warning("lobby banned itself out. lobby=%s %s", lobby["_id"], exc)
            return {
                "phase": CANCELLED,
                "cancel_reason": CANCEL_NO_POOL,
                "closed_at": now,
                "turn_expires_at": None,
            }

    async def _resolve_bans(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Tally the bans and move to `draft`. None if the lobby moved."""
        now = datetime.now(UTC)
        tallied = {
            **lobby,
            "bans": resolve_bans(
                lobby.get("seats") or [],
                lobby["edition"],
                lobby.get("starting_age"),
            ),
        }
        changes = await self._deal_or_cancel(tallied, now)
        written = await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {"bans": tallied["bans"], **changes},
            now,
        )
        # `random` reaches `complete` right here, with no pick ever
        # submitted, so this is the only place its stats can be counted.
        if written is not None and written.get("phase") == COMPLETE:
            await self._count_the_lobby(written["_id"])
        return written

    async def mark_ready(
        self, lobby_id: str, actor_discord_id: str, request: MarkReadyRequest
    ) -> dict[str, Any]:
        """A seat declares it has finished with the current phase.

        Before the draft a player may be ready without submitting, and the
        unanswered questions take their defaults. In the draft a pick is
        required, so this refuses a seat that holds none.
        """
        oid = as_lobby_id(lobby_id)
        found = await self._repository.find_by_id(oid)
        if found is None:
            raise LobbyNotFound(lobby_id)
        found = await self._advanced(found)
        phase = found["phase"]
        if phase not in (SETTINGS, BANS, DRAFT):
            raise SeatChangeRefused(
                f"There is nothing to be ready for at {phase}",
                request.expected_revision,
                found["revision"],
            )

        seats = found.get("seats") or []
        mine = next((s for s in seats if s.get("discord_id") == actor_discord_id), None)
        if mine is None:
            raise NotSeated("Only a seated player can be ready")
        if phase == DRAFT and mine.get("pick") is None:
            raise InvalidSeating("pick", "choose a leader before finishing")

        ready = [
            {**seat, "ready": True}
            if seat.get("discord_id") == actor_discord_id
            else seat
            for seat in seats
        ]
        now = datetime.now(UTC)
        written = await self._repository.replace_seats(
            oid, request.expected_revision, ready, now
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )

        occupied = [seat for seat in ready if seat.get("discord_id")]
        if all(seat.get("ready") for seat in occupied):
            written = await self._finish(phase, written) or written
        return for_the_wire(written, actor_discord_id)

    async def _complete(self, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Close a finished draft. `revealed_at` un-censors a blind one."""
        now = datetime.now(UTC)
        return await self._repository.apply_changes(
            lobby["_id"],
            lobby["revision"],
            {
                "phase": COMPLETE,
                "revealed_at": now,
                "closed_at": now,
                "turn_expires_at": None,
            },
            now,
        )

    async def _finish(self, phase: str, lobby: dict[str, Any]) -> dict[str, Any] | None:
        """Resolve whichever phase every seat has just finished."""
        if phase == SETTINGS:
            return await self._resolve_settings(lobby)
        if phase == BANS:
            return await self._resolve_bans(lobby)
        return await self._complete(lobby)

    async def cancel(
        self, lobby_id: str, actor_discord_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """The host abandons the lobby. Terminal, and it frees the channel."""
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
        oid, found, seats = await self._seated_in(
            lobby_id, DRAFT, actor_discord_id, request.expected_revision, "The draft"
        )
        mine = next(s for s in seats if s.get("discord_id") == actor_discord_id)
        mode = (found.get("settings") or {}).get("draft_mode")
        order = found.get("pick_order") or []
        turn_index = found.get("turn_index") or 0
        if order and whose_turn(order, turn_index) != actor_discord_id:
            raise NotYourTurn("It is not your turn to pick")

        if mode == DRAFT_CWC:
            taken = {
                token
                for team in found.get("teams") or []
                for token in [*team.get("leaders", []), *team.get("civs", [])]
            }
            available = {
                "pick": [t for t in found.get("pool") or [] if t not in taken],
                "civ_pick": [t for t in found.get("civ_pool") or [] if t not in taken],
            }
        else:
            available = {
                "pick": list(mine.get("pool") or []),
                "civ_pick": list(mine.get("civ_pool") or []),
            }

        wanted: dict[str, tuple[str, Any]] = {}
        if request.token is not None:
            wanted["pick"] = (request.token, available["pick"])
        if request.civ_token is not None:
            wanted["civ_pick"] = (request.civ_token, available["civ_pick"])
        if mode != DRAFT_CWC and mine.get("pick") is not None:
            raise PickIsFinal("Your pick is already locked in")
        chosen: dict[str, Any] = {}
        for field, (token, pool) in wanted.items():
            if token not in (pool or []):
                raise InvalidSeating(field, f"{token} is not in your pool")
            chosen[field] = token

        chosen["ready"] = True
        picked = [
            {**seat, **chosen} if seat.get("discord_id") == actor_discord_id else seat
            for seat in seats
        ]
        changes: dict[str, Any] = {"seats": picked}
        if mode == DRAFT_CWC:
            teams = [dict(team) for team in found.get("teams") or []]
            side = next(
                (
                    seat.get("team")
                    for seat in seats
                    if seat.get("discord_id") == actor_discord_id
                ),
                None,
            )
            for team in teams:
                if team["team_index"] == side:
                    team["leaders"] = [*team["leaders"], request.token]
                    if request.civ_token is not None:
                        team["civs"] = [*team["civs"], request.civ_token]
            changes = {"teams": teams}
        now = datetime.now(UTC)
        if order:
            changes["turn_index"] = turn_index + 1
            changes["turn_expires_at"] = now + CWC_TURN
        written = await self._repository.apply_changes(
            oid, request.expected_revision, changes, now
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(
                *self._why_refused(latest, request.expected_revision)
            )

        occupied = [seat for seat in picked if seat.get("discord_id")]
        finished = (
            whose_turn(order, turn_index + 1) is None
            if order
            else all(seat.get("ready") for seat in occupied)
        )
        if finished:
            written = await self._complete(written) or written
            await self._count_the_lobby(oid)
        return for_the_wire(written, actor_discord_id)

    async def submit_bans(
        self, lobby_id: str, actor_discord_id: str, request: SubmitBansRequest
    ) -> dict[str, Any]:
        """One seat's bans. Resolves the phase once every seat has submitted."""
        oid, found, seats = await self._seated_in(
            lobby_id, BANS, actor_discord_id, request.expected_revision, "The ban"
        )

        payload = await self._civ_data.fetch(found["edition"])
        legal = {
            "leader": {row["token"] for row in payload["leaders"]},
            "civ": set(bannable_civs(payload["civs"], found.get("starting_age"))),
        }
        submitted = {"leader": request.leader_keys, "civ": request.civ_keys}
        for kind, keys in submitted.items():
            unknown = [key for key in keys if key not in legal[kind]]
            if unknown:
                raise InvalidSeating(f"{kind}_keys", f"not bannable: {unknown[0]}")

        if found.get("ban_order"):
            return await self._ban_in_turn(oid, found, actor_discord_id, request)

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
        if all(seat.get("ready") for seat in occupied):
            written = await self._resolve_bans(written) or written
        return for_the_wire(written, actor_discord_id)

    async def submit_ballot(
        self, lobby_id: str, actor_discord_id: str, request: SubmitBallotRequest
    ) -> dict[str, Any]:
        """One seat's ballot. Resolves the phase once every seat has voted."""
        oid, found, seats = await self._seated_in(
            lobby_id, SETTINGS, actor_discord_id, request.expected_revision, "The vote"
        )

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
        if all(seat.get("ready") for seat in occupied):
            written = await self._resolve_settings(written) or written
        return for_the_wire(written, actor_discord_id)

    async def read(
        self,
        lobby_id: str,
        viewer_discord_id: str,
        since: int | None = None,
        *,
        is_staff: bool = False,
    ) -> dict[str, Any] | None:
        """The censored snapshot, or None when `since` already holds its revision."""
        found = await self._repository.find_by_id(as_lobby_id(lobby_id))
        if found is None:
            raise LobbyNotFound(lobby_id)
        found = await self._advanced(found)
        # Subscript, not .get(): a lobby with no revision is corrupt, and a
        # None here would compare unequal forever and never answer 204.
        if since is not None and found["revision"] == since:
            return None
        observing = await self._observing(found, viewer_discord_id, is_staff)
        return for_the_wire(found, viewer_discord_id, uncensored=observing)

    async def _observing(
        self, lobby: dict[str, Any], viewer_discord_id: str, is_staff: bool
    ) -> bool:
        """Whether this caller watches the lobby rather than plays in it.

        Staff only, and not while they are mid-game themselves: someone in a
        lobby that has started cannot watch another one.
        """
        if not is_staff:
            return False
        if any(
            seat.get("discord_id") == viewer_discord_id
            for seat in lobby.get("seats") or []
        ):
            return False
        return not await self._repository.is_playing_a_started_lobby(viewer_discord_id)

    async def start(
        self, lobby_id: str, actor_discord_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """Close seating and open the settings vote."""
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
            {
                "phase": SETTINGS,
                "turn_expires_at": now + SETTINGS_WINDOW,
                # cwc captains ban in turn, so the order has to exist before
                # the ban phase opens rather than when the draft is dealt.
                **self._cwc_ban_order(found),
            },
            now,
        )
        if written is None:
            latest = await self._repository.find_by_id(oid)
            raise SeatChangeRefused(*self._why_refused(latest, expected_revision))
        return for_the_wire(written, actor_discord_id)

    def _cwc_ban_order(self, lobby: dict[str, Any]) -> dict[str, Any]:
        """The ban turn order, for a cwc lobby only."""
        if (lobby.get("settings") or {}).get("draft_mode") != DRAFT_CWC:
            return {}
        captains = self._captains(lobby.get("seats") or [])
        return {
            "ban_order": cwc_order(captains, (lobby.get("team_size") or 0) * 2),
            "turn_index": 0,
        }

    async def change_seat(
        self, lobby_id: str, actor_discord_id: str, request: ChangeSeatRequest
    ) -> dict[str, Any]:
        """One seat change, returning the updated censored snapshot."""
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
            shape_of(found),
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
        """
        Spec section 9: matched-count zero is stale revision OR already seated, and
        only a re-read tells them apart.
        """
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
        channel_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Open lobbies for a guild, optionally filtered."""
        found = await self._repository.find_open(
            guild_id, channel_id=channel_id, edition=edition, game_type=game_type
        )
        return [for_the_wire(lobby, viewer_discord_id) for lobby in found]


def rearranged(
    seats: Sequence[Mapping[str, Any]], target: str, request: ChangeSeatRequest
) -> list[dict[str, Any]]:
    """The seat array the request asks for, sorted by `seat_index`."""
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
    "NotYourTurn",
    "PickIsFinal",
    "SeatChangeRefused",
    "as_lobby_id",
    "build_lobby_document",
    "for_the_wire",
    "rearranged",
    "seat_the_host",
]
