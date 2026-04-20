# Arrow Setup

How a fresh clone reaches `SELECT 1`.

## Prerequisites

- macOS with Homebrew
- `uv` (install: `brew install uv`)

## 1. Postgres 16 (local, via Homebrew)

Arrow runs on Postgres 16. If another Postgres is already on port 5432, Arrow's goes on **5433** alongside it.

```bash
brew install postgresql@16
# If 5432 is taken by another pg, pin arrow's to 5433:
sed -i '' 's/^#port = 5432/port = 5433/' /opt/homebrew/var/postgresql@16/postgresql.conf
brew services start postgresql@16
```

Create the `arrow` role and database:

```bash
/opt/homebrew/opt/postgresql@16/bin/psql -h localhost -p 5433 -d postgres <<'SQL'
CREATE ROLE arrow WITH LOGIN PASSWORD 'arrow' CREATEDB;
CREATE DATABASE arrow OWNER arrow;
SQL
```

Extensions (e.g. `pg_trgm`) are declared inside numbered schema migrations under `db/schema/`, not as one-shot `psql` commands — that way a fresh rebuild reproduces them.

## 2. Environment

Copy `.env.example` → `.env` and fill in real values:

```bash
cp .env.example .env
# edit .env, set FMP_API_KEY
```

`.env` is gitignored. `DATABASE_URL` defaults to the local pg 16 on 5433.

## 3. Python environment (uv)

```bash
uv sync
```

Creates `.venv/`, installs `psycopg[binary]` and `python-dotenv` from `pyproject.toml` / `uv.lock`.

## 4. Smoke test

```bash
uv run python scripts/db_ping.py
```

Expected output:

```
user=arrow  db=arrow
PostgreSQL 16.13 (Homebrew) on ...
```

If that prints, the whole chain (env → Python → psycopg → pg 16) works.

## 5. Apply the schema

```bash
uv run scripts/apply_schema.py
```

Runs every pending migration under `db/schema/` in numeric order. Idempotent — re-running after no changes prints `No pending migrations.`

## 6. Generate the live schema view

```bash
uv run scripts/gen_schema_viz.py
```

Introspects the live database and writes `arrow_db_schema.html` — a full ER diagram plus per-table cards (columns, FKs, CHECK constraints, indexes). Open it directly in a browser, or serve the repo with any static server and load `/arrow_db_schema.html`.

Re-run after every migration. The HTML is a pure projection of what's actually in Postgres; if it's not on the page, it's not in the database.

## Notes

- **Why port 5433, not 5432?** Many developer Macs already have a Homebrew pg (often `postgresql@14`) running on 5432. Arrow picks 5433 so it can coexist. Cloud deployment uses whatever port the target provides — just update `DATABASE_URL`.
- **Why no Docker?** Single-operator project; Homebrew is simpler and the isolation Docker provides isn't load-bearing here. If a second project ever needs Postgres on 5432, we have options (different port, Docker for the other project, etc.).
- **Why `uv` over `pip`/`poetry`?** Faster resolver, standard `pyproject.toml`, and also manages Python versions. Default for new Python projects as of ~2025.
- **No ORM.** `psycopg` directly. Revisit only if we feel real pain from raw SQL.

## Stopping / removing

```bash
# Stop the service:
brew services stop postgresql@16

# Nuke the data (⚠ destructive, only when you want a clean slate):
brew services stop postgresql@16
rm -rf /opt/homebrew/var/postgresql@16
# then rerun initdb/brew install step
```
