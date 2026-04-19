# ADR-0005: Storage split for raw_responses — JSONB + filesystem
Status: accepted
Date: 2026-04-19

## Context

`raw_responses.body` needs to store vendor payloads (FMP JSON, SEC HTML/PDF, later macro/news). Two tensions:

1. **Queryability** — we want `SELECT body->>'symbol' FROM raw_responses` to work inside Postgres
2. **Byte-fidelity for hashes** — `raw_hash = SHA-256(bytes-as-received)` must round-trip for audit and replay; without byte-fidelity, hash verification is meaningless

The system.md Raw Cache Layout already mandates a filesystem cache under `data/raw/{vendor}/...` as the byte-exact replay source. That predates this ADR and solves half the problem by itself.

## Decision

Three-part storage split:

1. **JSON responses** (FMP, most REST APIs) → `body_jsonb JSONB` — parsed, queryable, TOAST-compressed
2. **Non-JSON responses** (SEC HTML, PDFs, other binary) → `body_raw BYTEA` — exact bytes
3. **Exactly one of the two is populated per row**, enforced by CHECK:
   ```sql
   CONSTRAINT raw_responses_body_xor
       CHECK ((body_jsonb IS NULL) <> (body_raw IS NULL))
   ```
4. **Filesystem cache** under `data/raw/{vendor}/endpoint-path/{TICKER}/{key}.json` holds the byte-exact original for any source. This is the hash-verification substrate.
5. **Both hashes** (`raw_hash`, `canonical_hash`) are computed in the ingest layer at write time, against the original bytes, before any JSONB normalization.

## Consequences

**Positive**
- DB is queryable by content (`body_jsonb`) without sacrificing hash truth
- Filesystem is the authoritative byte-truth replay source for any payload
- Storage isn't duplicated — exactly one of the two body columns is populated per row
- JSONB gets TOAST compression automatically; body_raw bytea is a direct byte store
- Clear mental model: DB = queryable index, filesystem = byte replay

**Negative**
- JSONB's parse/normalize step is lossy relative to original bytes; you cannot recompute `raw_hash` from the JSONB column alone
- Two storage locations must be kept in sync — the ingest layer writes both, and must do so atomically from the caller's POV (a failure after DB write but before FS write produces divergence; ingest should retry FS write idempotently)
- For analyst tools that only have DB access, byte-fidelity audit requires reading from filesystem, not from SQL

## Alternatives considered

- **`body_raw BYTEA` only** — simplest for hash truth, but Postgres sees every response as an opaque blob. Every query that looks inside a payload must deserialize in application code. Kills the analyst's ability to `WHERE body->>'ticker' = 'NVDA'`.
- **`body_jsonb JSONB` + `body_raw BYTEA` both always populated** — best of both on paper, but doubles storage for hundreds of GB of raw cache. Dubious gain.
- **`text` column storing raw JSON string** — worst of both: neither queryable via JSON operators (you'd have to cast) nor compact (no TOAST optimization for structured data). No reason to prefer.

## Related

- `docs/architecture/system.md` § Raw Cache Layout — mandates the filesystem cache and endpoint-mirrored path convention
- `docs/architecture/system.md` § Artifacts vs Facts — mandates the double-hash discipline
- ADR-0006 covers the adjacent decision on request vs row identity

## When to revisit

- JSONB's lossiness bites in production — we discover a payload class that needs its exact whitespace preserved in the DB (unlikely for JSON APIs)
- Filesystem cache management becomes more painful than expected (disk-full incidents, sync drift bugs)
- We move to an object-storage backend for raw cache (Hetzner Storage Box, R2) and the sync discipline shifts
- A specific query pattern emerges that would benefit from both columns populated (revisit the "both always" option for a subset of responses)
