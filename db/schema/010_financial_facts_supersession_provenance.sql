-- Add amendment-supersession provenance to financial_facts.
--
-- Both columns are NULL on normal ingest rows (the common case). They're
-- populated only when a row is inserted specifically to supersede an
-- earlier row due to an XBRL-detected restatement (see
-- docs/research/amendment_phase_1_5_design.md).
--
-- `supersedes_fact_id` is a self-FK pointing to the row being superseded.
-- RESTRICT on delete preserves the audit trail — you cannot delete a
-- superseded row while its replacement still references it.
--
-- `supersession_reason` carries human-readable provenance (accession,
-- filing date, filing form, the specific restatement). Queryable for
-- audit. Size is bounded by content, not a hard cap.

ALTER TABLE financial_facts
    ADD COLUMN supersedes_fact_id  BIGINT       REFERENCES financial_facts(id) ON DELETE RESTRICT,
    ADD COLUMN supersession_reason TEXT;

CREATE INDEX financial_facts_supersedes_idx
    ON financial_facts (supersedes_fact_id)
    WHERE supersedes_fact_id IS NOT NULL;

COMMENT ON COLUMN financial_facts.supersedes_fact_id IS
  'Self-FK to the row this one supersedes. NULL for normal ingest rows. See docs/research/amendment_phase_1_5_design.md';
COMMENT ON COLUMN financial_facts.supersession_reason IS
  'Human-readable provenance for amendment supersessions (accession, filing date, restatement context). NULL for normal ingest rows.';
