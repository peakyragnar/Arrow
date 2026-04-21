-- Extend data_quality_flags.resolution to include 'superseded_by_reingest'.
--
-- Semantics: when a re-ingest supersedes the financial_facts rows a flag
-- was raised against, the flag no longer reflects current data. It should
-- be auto-resolved in the same transaction that supersedes the facts, so
-- the `data_quality_flags WHERE resolved_at IS NULL` view always refers
-- to anomalies the most recent ingest actually detected.
--
-- The three pre-existing resolution codes mean "an analyst acted on this
-- flag". The new code means "no analyst acted; the flag is closed because
-- its underlying facts were replaced by a fresh ingest run". Filtering by
-- resolution = 'superseded_by_reingest' lets us separate automated
-- housekeeping from analyst decisions when auditing.

ALTER TABLE data_quality_flags
    DROP CONSTRAINT data_quality_flags_resolution_enum;

ALTER TABLE data_quality_flags
    ADD CONSTRAINT data_quality_flags_resolution_enum
    CHECK (resolution IS NULL OR resolution IN (
        'approve_suggestion',
        'override',
        'accept_as_is',
        'superseded_by_reingest'
    ));

COMMENT ON COLUMN data_quality_flags.resolution IS
  'approve_suggestion | override | accept_as_is → analyst actions. '
  'superseded_by_reingest → auto-closed when the underlying facts were '
  'replaced by a later ingest run.';
