# ADR-0007: Hybrid artifact shape — columns + `artifact_metadata` jsonb
Status: accepted
Date: 2026-04-19

## Context

`artifacts` holds every source document Arrow ingests: SEC filings (10-K, 10-Q, 8-K), earnings transcripts, press releases, news articles, presentation decks, video transcripts, research primers, macro releases. These types don't universalize cleanly — a 10-K has an accession number and CIK; a transcript has speakers and a call type; a news article has a publisher and byline. We need a shape that doesn't either (a) grow sparse columns with every new type or (b) bury everything in opaque JSON that can't be queried.

## Decision

Hybrid shape:

1. **Common columns** for things most or all artifact types carry — `artifact_type`, `source`, `source_document_id`, `ticker`, `title`, `url`, `content_type`, `language`, `published_at`, `effective_at`, `raw_hash`, `canonical_hash`, period fields (fiscal + calendar + `period_end`), lineage (`supersedes`, `superseded_at`), research freshness (`authored_by`, `last_reviewed_at`, `asserted_valid_through`).
2. **Type-specific fields in `artifact_metadata jsonb`** (`NOT NULL DEFAULT '{}'::jsonb`) — e.g. `primary_document` / `form_type` for filings, `speakers[]` for transcripts, `publisher` for news. Filing identity fields later promoted to columns in migration 014.
3. **A metadata key conventions doc** (`docs/reference/artifact_metadata.md`) defines what keys are expected per artifact type. The conventions doc is the guardrail — every key in `artifact_metadata` is either listed there or being added in the same commit that introduces it.
4. **Sidecar tables** (e.g. a dedicated `filings` table) are deferred and considered case-by-case. Introduce one only when a specific artifact type accumulates enough operational weight — indexable fields, frequent joins, specific CHECK constraints — that a sidecar justifies its coordination cost.

## Consequences

**Positive**
- Common analyst queries (`WHERE ticker = X AND artifact_type = '10k' ORDER BY published_at DESC`) hit indexed columns directly — no JSON-path traversal
- New artifact types integrate without schema changes to the base table; only the type-list CHECK extends + the conventions doc updates
- `artifact_metadata` stays queryable via JSONB operators when needed (e.g. `WHERE artifact_metadata->>'primary_document' = X`)
- Clear decision rule for "column vs metadata": ≥ 3 artifact types share it → promote to column; otherwise metadata

**Negative**
- `artifact_metadata` drift is a real risk — inconsistent keys (`accession` vs `accession_number` vs `acc_no`) silently accumulate unless discipline holds
- Some queries that would be single-column lookups in a typed sidecar require JSONB path traversal
- Two places to look when debugging artifact shape (column list + conventions doc)

## Alternatives considered

- **All columns on `artifacts`, nullable per type** — every new type either adds columns (schema bloat, sparse rows at scale) or reuses columns (semantic drift). Rejected.
- **Pure JSON blob (`body jsonb` for everything)** — loses column-level indexability, CHECK-constraintability, and the readability of the schema itself. Rejected.
- **Per-type sidecar tables from day one** (`filings`, `transcripts`, `news_articles`)  — feels right in theory but over-engineered for v1. Most types don't have enough operational weight to justify their own table. A type-specific sidecar remains a future option when a real pain point emerges.

## Guardrails (the main risk)

- `docs/reference/artifact_metadata.md` is authoritative for metadata keys
- Adding a key to `artifact_metadata` without updating the conventions doc is a bug (will be caught in code review; later a lint job)
- If a metadata key starts applying to 3+ types, promote it to a column via a new migration and a PR that references this ADR

## When to revisit

- `artifact_metadata` keys have drifted beyond what code review catches, and queries across types become painful
- A specific artifact type (likely `10k` filings) develops enough operational weight — multi-column CHECKs, indexes on metadata paths, frequent joins — that a sidecar table would clearly pay for itself
- A new artifact type requires structural constraints that can't be expressed in JSON (rare)
