-- Drop artifact_chunks (introduced in 005).
--
-- Rationale: the chunks table was built ahead of the data it serves.
-- Per the regeneratability invariants in docs/architecture/system.md,
-- chunks are derived from artifacts and can be re-built at any time
-- once chunking has actual documents to operate on. Re-add as a new
-- migration the day we ingest the first filing/transcript and need
-- a chunking pass.
--
-- Migration discipline note (ADR-0004): we do not edit prior migrations.
-- 005 stays in place as the historical record of what was once applied;
-- this migration is the explicit retraction.

DROP TABLE IF EXISTS artifact_chunks;
