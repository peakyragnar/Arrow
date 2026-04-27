-- Allow generic text units for FMP earnings-call transcripts.
--
-- `artifact_text_units` is the generic text-unit layer for non-10-K/Q
-- artifacts. Migration 015 introduced it for earnings press releases;
-- transcripts use the same substrate, with one unit per parsed speaker turn
-- or one unparsed fallback unit when turn parsing is not reliable.

ALTER TABLE artifact_text_units
    DROP CONSTRAINT artifact_text_units_type_check;

ALTER TABLE artifact_text_units
    ADD CONSTRAINT artifact_text_units_type_check
    CHECK (unit_type IN ('press_release', 'transcript'));
