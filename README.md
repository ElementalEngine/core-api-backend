# core-api

<p>
  <img src="https://img.shields.io/badge/python-3.14-3776AB" alt="Python 3.14" />
  <img src="https://img.shields.io/badge/FastAPI-0.163-009688" alt="FastAPI 0.163" />
  <img src="https://img.shields.io/badge/MongoDB-Atlas-47A248" alt="MongoDB Atlas" />
  <img src="https://img.shields.io/badge/license-MIT-lightgrey" alt="MIT" />
</p>

The backend of the Civilization Players League's Discord bots and website. Mite, Auth and Lady Justice each call it over HTTPS with their own service token, and the CPL website reads its ladders with a read-only one. It parses Civ VI and Civ VII save files, keeps every player's TrueSkill rating, runs drafts, lobbies and votes, and handles registration and infractions. It is the only service that talks to the database.

```
Mite ─────────┐
Auth ─────────┤
Lady Justice ─┼──HTTPS, one token per caller──▶ core-api ──▶ MongoDB Atlas
CPL website ──┘
```

## Contents

- [What it does](#what-it-does)
- [Talking to it](#talking-to-it)
- [Running it](#running-it)
- [Configuration](#configuration)
- [Dependencies](#dependencies)
- [Checks](#checks)
- [Project layout](#project-layout)
- [Operations](#operations)
- [License](#license)

## What it does

| Area | Routes | Used by |
|---|---|---|
| **Matches** | upload a save, edit seats, contest, approve, revert, auto-approve, leaderboards | Mite |
| **Ratings and stats** | TrueSkill updates on approve, lifetime and seasonal stats, history, team generation, resets | Mite |
| **Match setups** | drafts, Civ6 lobbies, voice-channel votes, the settings each game asks | Mite |
| **Seasons** | the open season, and starting the next one | Mite |
| **Civ data** | every leader and civ, with display names and emoji | all three bots |
| **Auth** | Discord OAuth and Steam checks for registration, account lookups with standing, rank-role requests; v2 keeps only the registered player, in `server_members.users` | Auth |
| **Infractions** | tiered and flat suspensions, days added or removed, overdue suspensions | Lady Justice |
| **Public** | league totals, each edition's ladders and current season stats, players named by display name, read only | CPL website |

**How a game is rated.** Mite uploads the save. The parser reads the placements, leaders and civs, and the match waits as a report the players can fix and contest. On approve, ratings move in one transaction, and every change is written to a ledger of rating events beside it. A revert writes the opposite events, so the ledger always sums to the stats. A report that needs no staff (every seat registered, every leader and civ known, no quit, sub or contest) is approved 48 hours after upload.

The full route list is in [`openapi.json`](openapi.json). CI fails if the app and that file disagree.

## Talking to it

Every route under `/api` needs the caller's Bearer token: each bot has its own, and the website's opens only `/api/v2/public`. A route that acts for a member, such as an upload or an approve, also takes their Discord id in `x-actor-discord-id`.

A read:

```bash
curl -s https://core-api.example/api/v2/stats/user \
  -H "Authorization: Bearer $MITO_SERVICE_TOKEN" \
  -G -d civ_version=civ6 -d game_type=realtime -d discord_id=100000000000000001
```

```json
{
  "discord_id": "100000000000000001",
  "civ_version": "civ6",
  "game_type": "realtime",
  "lifetime": {
    "ffa": { "mu": 1342, "sigma": 92.1, "games": 41, "first": 9, "rating_gains": 23, "subbedIn": 2, "subbedOut": 0, "lastModified": "2026-10-29T21:14:03Z" },
    "teamer": null,
    "duel": null
  },
  "season": {
    "ffa": { "mu": 1286, "sigma": 110.4, "games": 7, "first": 2, "rating_gains": 4, "subbedIn": 0, "subbedOut": 0, "lastModified": "2026-10-29T21:14:03Z" },
    "teamer": null,
    "duel": null
  },
  "season_label": "Season 6"
}
```

Every error has the same shape. A bot retries only when `retryable` is true:

```json
{ "detail": { "error": { "code": "NOT_FOUND", "message": "No stats found", "details": null, "retryable": false, "correlation_id": null } } }
```

The codes Mite's routes can answer with are a closed set: `INVALID_REQUEST`, `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `CONFLICT`, `RATE_LIMITED`, `UNAVAILABLE` and `INTERNAL`, plus a named code when the server itself is set up wrong (`MITO_SERVICE_MISCONFIGURED`, `CIV_DATA_NOT_SEEDED`). An `INTERNAL` error carries a correlation id that matches the log line.

Auth's routes, under `/api/v2/auth`, answer with their own codes, such as `STEAM_PROFILE_PRIVATE` or `STEAM_ID_CONFLICT`, and a `message` written for the player. A clash names the other Discord account and its standing with Lady Justice in `details`, for staff. A refusal from the auth service itself carries a correlation id. Registering a player by hand also takes `x-actor-is-staff: true`; without it the answer is `FORBIDDEN`.

`/healthz` answers while the process is up, and `/readyz` once the database answers too.

## Running it

```bash
python3.14 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements-dev.lock
cp .env.example .env        # then fill in the required values
.venv/bin/python -m app     # uvicorn on API_HOST:API_PORT, 127.0.0.1:8000 by default
```

At startup the service builds every index it needs, then serves. Seeding the leaders, civs and seasons is a separate, deliberate step:

```bash
.venv/bin/python scripts/seed_civ_data.py --dry-run
.venv/bin/python scripts/seed_civ_data.py
```

## Configuration

Everything comes from the environment or `.env`. [`.env.example`](.env.example) lists each variable, its default and what it changes. The four groups to know:

- **Database:** `MONGO_URL`.
- **Tokens:** `LJ_SERVICE_TOKEN` (required at startup), `MITO_SERVICE_TOKEN`, `AUTH_SERVICE_TOKEN` and `WEBSITE_SERVICE_TOKEN` (each caller's routes answer 503 until set).
- **Registration:** the Discord OAuth app and the Steam Web API key.
- **Ratings:** the `TS_*` values. They must match production, or ratings stop being comparable.

## Dependencies

`requirements.txt` and `requirements-dev.txt` pin the packages core-api uses directly. `requirements.lock` and `requirements-dev.lock` pin those and everything they pull in, each file with its sha256 hashes. Every install, in CI and on the server, reads a lock with `--require-hashes`, so only the exact files locked will install: production runs what dev tested.

To change a package, change its pin, then rebuild both locks with uv and review what else moved:

```bash
uv pip compile requirements.txt --generate-hashes --python-version 3.14 -o requirements.lock
uv pip compile requirements-dev.txt --generate-hashes --python-version 3.14 -o requirements-dev.lock
```

A test fails when a pin and its lock disagree.

## Checks

```bash
.venv/bin/mypy                         # type-checks app/
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/pytest -q                    # unit tests and real save files
.venv/bin/python scripts/spec.py       # regenerate openapi.json after a route change
```

The save-parsing tests read real `.Civ6Save` and `.Civ7Save` files from [`test/data`](test/data). A new save shape gets a file there and a test.

## Project layout

```
app/
  api/          health routes and the router that mounts every feature
  core/         settings, the database client, errors, logging, auth gates
  features/
    auth/          registration and account lookups
    civdata/       leaders and civs, and their seed files
    infractions/   suspensions
    match_setups/  drafts, lobbies, votes and their settings
    matches/       uploads, reports, approve and revert; parsers/ reads the saves
    public/        the website's read-only totals, ladders and season stats
    ratings/       TrueSkill, the rating ledger and its reconciliation
    seasons/       seasons and the season change
    stats/         stats, history, team generation
scripts/        civ data seed, export and restore, reconcile, repair
test/           unit/, parsing/ and the save files in data/
```

Each feature owns its routes, schemas, service and data access.

## Operations

- `scripts/reconcile.py` checks that the rating ledger sums to the stats. The failure it detects has no other symptom, so it runs after any migration and from time to time in production.
- `scripts/export` writes collections to JSON lines with a manifest. It is the rollback for a cluster without point-in-time restore: run it before anything that rewrites data.
- `scripts/copy_database.py` copies one database into an empty one and checks the copy, byte for byte.
- `scripts/rebuild_civ_tallies.py` rebuilds the civ and leader tallies from the matches. It is a dry run unless given `--apply`.

The privacy policy for the bots and the website is at https://cpl.elementalengine.net/privacy/.

## License

[MIT](LICENSE).
