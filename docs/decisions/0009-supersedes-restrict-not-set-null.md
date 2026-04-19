# ADR-0009: `supersedes` uses `ON DELETE RESTRICT`, not `SET NULL`; `is_current` is derived, not stored
Status: accepted
Date: 2026-04-19

## Context

`artifacts.supersedes` is a self-referential foreign key. When a corrected document arrives (e.g. a 10-K/A amendment), a new artifact row is inserted with `supersedes = <prior_id>` and the prior row gets `superseded_at` set. This preserves lineage — you can always walk backwards through revisions to the original.

Two related choices arose at design time:
1. What should `ON DELETE` on the self-FK be? `CASCADE`, `SET NULL`, `RESTRICT`, or no action (which behaves like `RESTRICT`)?
2. Should we add an `is_current boolean` column for convenience, or derive "current" from `superseded_at IS NULL`?

Initial lean: `ON DELETE SET NULL` for the FK, store `is_current` for query convenience.

## Decision

**`ON DELETE RESTRICT`** on `supersedes`. Deleting an artifact that is referenced by another artifact's `supersedes` fails loudly at the DB level.

**No `is_current` column.** "Current" is computed on the fly via `WHERE superseded_at IS NULL`, and the supporting partial index (`artifacts_current_idx`) is defined that way.

## Consequences

**Positive**

*On RESTRICT:*
- Lineage integrity is a hard property, not a convention. A future cleanup script that accidentally tries to delete a referenced artifact fails loudly instead of silently orphaning the supersession chain
- Matches the append-only posture of the rest of the schema — deletions of artifacts should be pathological, not routine; RESTRICT reflects that
- Consistent with how we FK'd `raw_responses.ingest_run_id` — uniform posture across the schema

*On derived `is_current`:*
- Zero drift risk. There is no path where `is_current = true` can desync from `superseded_at IS NULL`
- Fewer columns, simpler mental model
- The partial index `WHERE superseded_at IS NULL` is just as fast as indexing a separate boolean

**Negative**

*On RESTRICT:*
- If we genuinely need to delete a referenced artifact (e.g. data-retention compliance), the operator must manually break the chain first — walk descendants, null out their `supersedes` pointers in a dedicated migration, then delete. Deliberate friction, not accidental, which is the point
- Slightly less symmetric if we later add an archival mechanism; revisit then

*On derived `is_current`:*
- Every "current" query writes `WHERE superseded_at IS NULL` (or queries the partial index `artifacts_current_idx`). Slightly more verbose than `WHERE is_current`. Negligible in practice; we'll wrap it in a view if it becomes annoying

## Alternatives considered

*For `ON DELETE`:*
- **`CASCADE`** — deletes the parent's descendants when the parent is deleted. Catastrophically wrong for lineage; a single `DELETE` could wipe an entire revision chain
- **`SET NULL`** — on delete of the parent, children's `supersedes` go to NULL. Silently breaks lineage; a reader can no longer tell that the child is a revision. Rejected
- **No action / default** — functionally equivalent to `RESTRICT` in effect but less explicit in the SQL. Using `RESTRICT` documents intent

*For `is_current`:*
- **`is_current boolean` populated by trigger** — eliminates drift via trigger, at the cost of a trigger. For a boolean that's a pure function of one other column, a derived query is clearer
- **`is_current boolean` populated by ingest** — drift risk; a new code path that inserts or updates `superseded_at` without touching `is_current` produces wrong "current" reads

## Related

- ADR-0007 on the artifact shape; this ADR governs how the supersession fields behave
- `docs/architecture/system.md` § Artifacts vs Facts — mandates append-only + supersedes-pointer for corrections

## When to revisit

- A real data-retention or privacy-deletion requirement forces artifact deletion regularly enough that walking chains becomes tedious — introduce a deletion helper that handles the chain, not a FK change
- Query performance on "give me the current NVDA 10-K" degrades to the point that a denormalized `is_current` column materially helps (highly unlikely — `superseded_at IS NULL` is a partial-index lookup)
