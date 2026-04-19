# ADR-0006: Request identity separated from row identity in raw_responses
Status: accepted
Date: 2026-04-19

## Context

`raw_responses` is append-only per `system.md`. Re-fetching the same endpoint with the same parameters on different days is expected and useful:

- FMP updates fundamentals late in the filing cycle — later pulls return revised data
- SEC 10-K/A amendments produce materially different content for the same accession
- Any vendor fix to a historical data point becomes visible only by re-fetching

We need two distinct identities:

- "Which HTTP fetch event produced this row?" — unique per fetch
- "What logical request does this row correspond to?" — stable across fetches of the same `(vendor, endpoint, params)`

Conflating the two destroys either the revision history or the ability to query by logical request.

## Decision

Two separate identity concepts, both first-class in the schema:

- **Row identity = `(id)`** — `bigserial` primary key. One per HTTP fetch event. Never reused.
- **Request identity = `(vendor, endpoint, params_hash)`** — stable logical key across fetches.

Specifically:

- `params` stored as `JSONB` — queryable (`params->>'ticker' = 'NVDA'`)
- `params_hash` stored as `BYTEA` with `CHECK (octet_length(params_hash) = 32)` — SHA-256 of a canonical serialization of params. Computed by the ingest layer at write time using stable key ordering.
- **No unique constraint on request identity.** Multiple rows per `(vendor, endpoint, params_hash)` are the normal case — that's how revision history is preserved.
- **Primary query index**: `(vendor, endpoint, params_hash, fetched_at DESC)`. Pattern: `... ORDER BY fetched_at DESC LIMIT 1` for "most recent response for this request."

## Consequences

**Positive**
- Re-fetches are natural; no UPDATE or UPSERT logic ever appears in ingest code
- Revision history preserved by construction — no data loss on re-fetch
- Point-in-time queries trivial: `WHERE fetched_at <= $asof ORDER BY fetched_at DESC LIMIT 1`
- Dedup by content (via `raw_hash` / `canonical_hash`) is orthogonal to request identity — we can ask "is this payload byte-identical to anything we've ever fetched?" independently of the request structure
- `params_hash` gives a stable, compact, indexable request key; `params` stays queryable as JSON

**Negative**
- Hash must be computed deterministically at write time. The canonicalization function (stable param ordering, stable number rendering) becomes part of the ingest contract. Bug there means two valid fetches of the same request hash differently.
- Storage grows monotonically — re-fetching the same unchanged payload daily still adds a row, distinguished only by `fetched_at` and identical hashes. Mitigation comes later via content-hash dedup at the analyst layer, not by rejecting at write time.
- "Has this request ever been made?" requires a `LIMIT 1` on the request-identity index, not a unique-constraint lookup.

## Alternatives considered

- **Unique constraint on `(vendor, endpoint, params)` with upsert** — violates append-only. Loses revision history. Turns every re-fetch into a silent overwrite and demands conflict-resolution logic. Rejected.
- **Row identity = request identity** (unique key) — equivalent to the above; same drawbacks.
- **`params_hash` alone, no `params` column** — opaque. Can't ask "what params were these?" without a reverse lookup table. Rejected.
- **`params` alone, no `params_hash`** — JSONB equality queries work for small params but scale poorly, and the index would have to cover the full JSONB. Hash gives us fixed-size, indexable identity almost free.

## Related

- ADR-0005 covers the body storage split
- `docs/architecture/system.md` § Artifacts vs Facts — mandates append-only discipline for source truth
- `docs/architecture/system.md` § Time-Aware Model — the point-in-time query pattern `params_hash` supports

## When to revisit

- A query pattern emerges where request identity really needs to be unique (hard to imagine without breaking append-only)
- Hash computation becomes a bottleneck (unlikely; SHA-256 on small JSON params is microseconds)
- Storage growth from re-fetches outpaces analytical value — introduce a dedup policy at write time (don't write if `raw_hash` matches the most recent row for this request identity)
- We adopt a different canonicalization scheme and need to migrate `params_hash` — this would be a dedicated migration ADR of its own
