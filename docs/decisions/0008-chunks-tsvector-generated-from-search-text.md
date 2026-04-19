# ADR-0008: `artifact_chunks.tsv` generated from `search_text` with `text` fallback
Status: accepted
Date: 2026-04-19

## Context

`artifact_chunks` is the FTS surface for every retrievable piece of Arrow — filing sections, transcript speaker turns, research-note paragraphs. We need `tsvector` on every chunk so `tsv @@ websearch_to_tsquery(...)` works without per-query computation. The question is *how* the `tsvector` gets populated and kept consistent.

Three options presented at the design stage:
1. **Generated column** (`GENERATED ALWAYS AS (...) STORED`) from `text`
2. **Trigger** on insert/update that populates a regular `tsvector` column
3. **Ingest-layer populated** — ingest code computes and writes the tsvector directly

The refinement that shifted the decision: for some chunk types (transcript speaker turns, noisy tables, boilerplate preambles), the *raw display text* isn't the best FTS input. Ingest wants the option to strip speaker labels, table scaffolding, boilerplate — but *also* wants the raw text preserved for display / citation purposes. Those are two different pieces of text with the same chunk identity.

## Decision

Store both, generate `tsv` from the normalized one when present:

```sql
text              text        NOT NULL,       -- raw display text, preserved as-is
search_text       text,                       -- normalized FTS input (optional)
tsv               tsvector    GENERATED ALWAYS AS (
                     to_tsvector('english', COALESCE(search_text, text))
                  ) STORED
```

Ingest decides whether to populate `search_text`:
- **Leave null** when the raw `text` already makes a good FTS input (most cases — e.g. plain paragraphs from filings)
- **Populate** when the raw text contains structural noise that would bloat the FTS index without aiding retrieval (e.g. transcript chunks where speaker labels dilute term frequency; HTML table chunks where markup is noisy)

## Consequences

**Positive**
- **Postgres-invariant.** `tsv` is always `to_tsvector(COALESCE(search_text, text))` by construction. No trigger can forget to fire; no ingest bug can leave `tsv` stale
- **Ingest flexibility.** Normalization logic lives in Python where it's testable and vendor-specific, without giving up the DB-level invariant
- **Clean separation.** `text` stays as-is for display/citation; `search_text` carries the FTS shape
- **Both columns queryable.** You can still `SELECT text FROM artifact_chunks WHERE tsv @@ ...` — the returned display text is the raw one, which is what the analyst wants

**Negative**
- Modest storage overhead: `search_text` roughly doubles chunk text size when populated. Mitigated because most chunks won't populate it (default is NULL)
- Changing the normalization function requires re-populating `search_text` for affected chunks, which regenerates `tsv` automatically — but it's a re-ingest, not a schema migration
- Changing to a non-English dictionary or custom tokenizer requires an `ALTER TABLE` to update the generated expression — slightly more ceremony than updating a trigger

## Alternatives considered

- **Generated from `text` only** — simpler, but forces every piece of FTS text to be the raw display text. Loses the ability to strip transcript speaker labels / table chrome cleanly
- **Trigger-populated** — flexible, but trigger drift is a class of bug where an insert path bypasses the trigger (e.g. via COPY, or a direct `INSERT ... RETURNING` that fails to fire under some circumstance). One more moving part to debug when FTS returns nothing
- **Ingest-populated `tsvector`** — works, but discipline-dependent. An ingest path that forgets to populate `tsv` produces rows that silently don't match any query. The generated column makes this impossible

## Related

- `docs/architecture/system.md` § Why Search-First — FTS is the retrieval substrate; drift here breaks every analyst query
- ADR-0007 on artifact-level shape; this ADR is its chunks-level sibling

## When to revisit

- We adopt a non-English language — `ALTER TABLE ... DROP COLUMN tsv; ADD COLUMN tsv tsvector GENERATED ALWAYS AS (to_tsvector('spanish', ...)) STORED` is the mechanism, and a new ADR captures the move
- We want to use a non-default dictionary (e.g. a custom synonyms file for finance terminology)
- FTS query performance requires partial / filtered GIN indexes tuned to specific query patterns (a schema evolution, not a retraction of this decision)
