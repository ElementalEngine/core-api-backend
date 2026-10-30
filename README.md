# core-api

<p>
  <img src="https://img.shields.io/badge/python-3.14-3776AB" alt="Python 3.14" />
  <img src="https://img.shields.io/badge/FastAPI-0.128-009688" alt="FastAPI 0.128" />
  <img src="https://img.shields.io/badge/MongoDB-Atlas-47A248" alt="MongoDB Atlas" />
  <img src="https://img.shields.io/badge/license-MIT-lightgrey" alt="MIT" />
</p>

The backend of the Civilization Players League's Discord bots. Mite, Auth and Lady Justice each call it over HTTPS with their own service token. It parses Civ VI and Civ VII save files, keeps every player's TrueSkill rating, runs drafts, lobbies and votes, and handles registration and infractions. It is the only service that talks to the database.

```
Mite ─────────┐
Auth ─────────┼──HTTPS, one token per bot──▶ core-api ──▶ MongoDB Atlas
Lady Justice ─┘
```

## Contents

- [What it does](#what-it-does)
- [Talking to it](#talking-to-it)
- [Running it](#running-it)
- [Configuration](#configuration)
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
| **Auth** | Discord OAuth and Steam checks for registration, account lookups, rank-role requests | Auth |
| **Infractions** | tiered and flat suspensions, days added or removed, overdue suspensions | Lady Justice |

**How a game is rated.** Mite uploads the save. The parser reads the placements, leaders and civs, and the match waits as a report the players can fix and contest. On approve, ratings move in one transaction, and every change is written to a ledger of rating events beside it. A revert writes the opposite events, so the ledger always sums to the stats. A report that needs no staff (every seat registered, every leader and civ known, no quit, sub or contest) is approved 48 hours after upload.

The full route list is in [`openapi.json`](openapi.json). CI fails if the app and that file disagree.

## Talking to it

Every route under `/api` needs the calling bot's Bearer token. A route that acts for a member, such as an upload or an approve, also takes their Discord id in `x-actor-discord-id`.

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

`/healthz` answers while the process is up, and `/readyz` once the database answers too.

## Running it

```bash
python3.14 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
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
- **Tokens:** `LJ_SERVICE_TOKEN` (required at startup), `MITO_SERVICE_TOKEN` and `AUTH_SERVICE_TOKEN` (each bot's routes answer 503 until set).
- **Registration:** the Discord OAuth app and the Steam Web API key.
- **Ratings:** the `TS_*` values. They must match production, or ratings stop being comparable.

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
    ratings/       TrueSkill, the rating ledger and its reconciliation
    seasons/       seasons and the season change
    stats/         stats, history, team generation
scripts/        seeding, migrations, export, reconcile, the v1 differential
test/           unit/, parsing/ and the save files in data/
```

Each feature owns its routes, schemas, service and data access.

## Operations

- `scripts/reconcile.py` checks that the rating ledger sums to the stats. The failure it detects has no other symptom, so it runs after any migration and from time to time in production.
- `scripts/export` writes collections to JSON lines with a manifest. It is the rollback for a cluster without point-in-time restore: run it before anything that rewrites data.
- `scripts/copy_database.py` copies one database into an empty one and checks the copy, byte for byte.
- The migration and differential scripts are one-off tools for the v2 cutover. The migrations are dry runs unless given `--apply`.

The bots' privacy policy is in [`PRIVACY.md`](PRIVACY.md).

## License

[MIT](LICENSE).
