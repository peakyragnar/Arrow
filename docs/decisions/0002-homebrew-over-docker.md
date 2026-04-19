# ADR-0002: Homebrew over Docker for local Postgres
Status: accepted
Date: 2026-04-19

## Context

Step 2 of the Build Order requires a running local Postgres 16 accessible from Python. Two realistic paths existed:

- **Docker** — `docker compose up -d` with a `postgres:16` image and a named volume
- **Homebrew** — `brew install postgresql@16 && brew services start`

The operator's machine already had `postgresql@14` running on port 5432 (from an unrelated or historical install).

## Decision

**Homebrew.** Installed `postgresql@16` alongside the existing `postgresql@14`, configured on port **5433** to avoid the port collision. `arrow` role and database created, accessible via `DATABASE_URL=postgresql://arrow:arrow@localhost:5433/arrow`.

## Consequences

**Positive**
- Native `psql`, `pg_dump`, `pg_restore` on `$PATH` immediately — no `docker compose exec` indirection for routine DB work
- Runs as a launchd service (~50 MB idle) — no 500 MB Docker Desktop tax
- Simpler debugging surface — no container/network layer between Python and pg
- Matches the production model: Hetzner (ADR-0001) will run pg directly on a VM, not in Docker

**Negative**
- No image-tag-level version pinning — `postgresql@16` Homebrew formula pins to major 16, but the exact minor drifts with `brew upgrade`
- If a second local project ever needs pg on 5432, a port conflict or version conflict could arise (mitigations: different port, Docker for that project, or stop/start services)
- Multi-version coexistence via port assignment is slightly awkward to document

## Alternatives considered

**Docker** was the initial recommendation, with the pitch resting on three claims:

1. "Wipe and rebuild in seconds" — but Homebrew's `brew services stop postgresql@16 && rm -rf /opt/homebrew/var/postgresql@16 && initdb ...` is equivalent, and used rarely
2. "Version parity with cloud" — true only if the cloud target pins the same image. Hetzner runs whatever pg major we install; parity comes from choosing pg 16 everywhere, not from the container
3. "Isolation" — real, but not load-bearing for a single-operator project

On honest review, Docker's advantages weren't load-bearing. The overhead of Docker Desktop always running, plus the `docker compose exec` friction for every `psql` invocation, outweighed the isolation benefit.

## When to revisit

- A second local project needs Postgres on the default port and clean coexistence becomes painful
- Team size grows past one and "works on my machine" starts biting
- We add other services (Redis, elasticsearch, etc.) and want a unified compose file for the whole stack
- CI needs to run DB integration tests in a reproducible container

Migration to Docker is `pg_dump | docker compose up postgres | pg_restore` + `DATABASE_URL` swap — same mechanical reversal as the cloud migration.
