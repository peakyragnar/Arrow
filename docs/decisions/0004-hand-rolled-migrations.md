# ADR-0004: Hand-rolled SQL migrations, not Alembic
Status: accepted
Date: 2026-04-19

## Context

Step 6 of the Build Order delivers the first real schema (`raw_responses` + `ingest_runs`). From here forward, every new table or schema change requires a migration discipline. The default Python choice is Alembic; the minimal alternative is a small hand-rolled runner over numbered SQL files.

Arrow's working rules (from `docs/architecture/system.md`):
- SQL-first — `db/schema/*.sql` holds the contracts
- ORM-less — `psycopg` directly, no SQLAlchemy
- Append-only migrations — never edit a past file; add a new numbered one
- Rollback in production = restore from backup, not a `downgrade()` function

## Decision

**Hand-rolled runner.** ~80 lines of Python at `src/arrow/db/migrations.py`. It:

- Discovers `db/schema/*.sql` in filename-sorted order
- Bootstraps its own `schema_migrations` tracking table on first run (`filename PK, checksum, applied_at`)
- Applies unapplied files one transaction per file
- Stores SHA-256 checksum of every applied file; mismatch at next run raises `MigrationChanged`, forcing additive-only discipline

CLI entrypoint: `uv run python scripts/apply_schema.py`.

## Consequences

**Positive**
- Zero dependencies beyond `psycopg` (already in use)
- SQL-first stays SQL-first; no SQLAlchemy coupling by stealth
- Append-only discipline enforced by the runner — edits to applied files fail loudly with a clear message
- Concepts to learn: one. "Files get applied once, in order, in a transaction"
- Full transparency — the runner is 80 lines and readable in five minutes

**Negative**
- No downgrade tooling. Rollback in production means restore from backup. Matches `system.md`'s append-only posture, but removes a safety net some teams depend on
- No migration autogeneration from ORM models (we don't have ORM models, so this isn't a loss today)
- A small amount of custom code that has to be maintained vs. a battle-tested package

## Alternatives considered

**Alembic** — the standard Python migration tool.

Stripping down what Alembic offers, two big features stand out:
1. `upgrade()` / `downgrade()` functions per migration
2. Autogeneration of migrations from SQLAlchemy model diffs

For Arrow:
- (2) is worth zero — no SQLAlchemy, so no model to diff
- (1) is worth zero — system.md commits to append-only; rollback = restore from backup
- What's left is a Python wrapper that reads SQL files and executes them — which is exactly what the hand-rolled runner is, with 10× less ceremony (no `versions/` directory, no `env.py`, no `alembic.ini`, no `script.py.mako`)

Net: Alembic would add surface area and dependencies in exchange for features we've already decided not to use.

## When to revisit

- We adopt SQLAlchemy for analyst tools or API layers, unlocking Alembic's autogeneration
- We find ourselves needing scripted downgrades in production (multiple occurrences per year)
- The hand-rolled runner develops bugs that Alembic has already solved (and the cost of fixing them hand-rolled exceeds the cost of migrating)
- Team size grows past solo and a more opinionated migration tool reduces coordination cost
