# Architecture Decision Records

This directory holds short, numbered records of the non-obvious design choices Arrow has made. When someone (including future-us) asks "why did we pick X?", the answer lives here — not buried in commit messages, PR bodies, or chat logs.

## Format

Each ADR follows Michael Nygard's template (adapted):

```markdown
# ADR-NNNN: Short title in imperative or declarative form
Status: proposed | accepted | superseded by ADR-MMMM
Date: YYYY-MM-DD

## Context
What forces were at play? What did the situation look like?

## Decision
What did we actually choose?

## Consequences
What becomes easier, harder, or different as a result? Both positive and negative.

## Alternatives considered
What else was on the table, and why was it not chosen?

## When to revisit
What signal would tell us this decision should be reopened?
```

## Conventions

- **Numbered sequentially.** `0001`, `0002`, ... — no gaps, no renumbering.
- **Append-only.** Never edit an accepted ADR to change the decision. Write a new ADR that supersedes it. Small fixes (typos, clarifications that don't change meaning) are fine.
- **Short.** 30–80 lines. If an ADR is growing past a page, consider splitting it or promoting content to `docs/architecture/` or `docs/reference/`.
- **Authoritative for their scope.** If system.md says "no pgvector" as a principle, an ADR need not repeat it. ADRs capture the *trade-off* decisions that principles don't settle.

## What goes in an ADR vs. elsewhere

| Content | Goes in |
|---|---|
| A tool choice with real alternatives (uv vs pip, Hetzner vs Render) | ADR |
| A storage-shape choice with trade-offs (JSONB vs bytea) | ADR |
| A universal principle (append-only, two clocks) | `docs/architecture/system.md` |
| A domain rule (Q4 = FY − 9M YTD) | `docs/reference/` |
| A day-to-day code pattern | Code comments, CLAUDE.md |
| A bug fix's rationale | Commit message |
| A feature's rollout reasoning | PR body |

## Status values

- **proposed** — under discussion, not yet committed
- **accepted** — decision in force; the code reflects it
- **superseded by ADR-MMMM** — decision no longer applies; see the named ADR for the replacement

## Index

| # | Title | Status | Date |
|---|---|---|---|
| [0001](0001-hetzner-as-cloud-target.md) | Hetzner Cloud as the cloud-later target | accepted | 2026-04-19 |
| [0002](0002-homebrew-over-docker.md) | Homebrew over Docker for local Postgres | accepted | 2026-04-19 |
| [0003](0003-uv-for-python-toolchain.md) | uv for Python package and environment management | accepted | 2026-04-19 |
| [0004](0004-hand-rolled-migrations.md) | Hand-rolled SQL migrations, not Alembic | accepted | 2026-04-19 |
| [0005](0005-raw-responses-storage-split.md) | Storage split for raw_responses: JSONB + filesystem | accepted | 2026-04-19 |
| [0006](0006-request-identity-vs-row-identity.md) | Request identity separated from row identity in raw_responses | accepted | 2026-04-19 |
| [0007](0007-artifact-hybrid-metadata.md) | Hybrid artifact shape — columns + `artifact_metadata` jsonb | accepted | 2026-04-19 |
| [0008](0008-chunks-tsvector-generated-from-search-text.md) | `artifact_chunks.tsv` generated from `search_text` with `text` fallback | withdrawn | 2026-04-19 |
| [0009](0009-supersedes-restrict-not-set-null.md) | `supersedes` uses `ON DELETE RESTRICT`; `is_current` is derived, not stored | accepted | 2026-04-19 |
