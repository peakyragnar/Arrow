-- Tighten artifact_text_units.company_id to NOT NULL.
--
-- Migration 015 introduced the column as nullable, but in practice every
-- inserter (src/arrow/ingest/sec/qualitative.py and
-- src/arrow/agents/fmp_transcripts.py) always supplies it, and the sibling
-- table artifact_sections (migration 014) already declares its company_id
-- NOT NULL. The unenforced contract forced downstream joins and steward
-- views to defend against a NULL that never appears. Aligning the
-- constraint matches the actual data and removes the asymmetry.
--
-- Pre-flight: confirmed zero NULL company_id rows at apply time
-- (17,016 rows, 0 NULL).

ALTER TABLE artifact_text_units
    ALTER COLUMN company_id SET NOT NULL;
