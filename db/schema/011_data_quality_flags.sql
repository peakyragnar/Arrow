-- Data quality flags: soft-validation anomalies from Layer 3/4.
--
-- These do NOT block ingest. They record discrepancies for analyst
-- visibility while allowing the data to load. Contrast with hard-gate
-- layers (1, 2, 5): those raise on failure and prevent persistence.
--
-- Workflow:
--   1. Ingest → Layer 1/2/5 block on failure. If they pass, data loads.
--   2. Amendment agent resolves what it can via XBRL supersession.
--   3. Remaining Layer 3/4 anomalies → one row per anomaly in this table.
--   4. Backfill emits a review file listing flags with context + SEC links.
--   5. Analyst edits review file: approve suggestion, override, or accept as-is.
--   6. fix-flags script applies approved corrections via supersession (using
--      extraction_version='human-verified-v1') and marks flags resolved.
--
-- Flags are NEVER deleted — resolved flags stay with `resolved_at` set so
-- the audit trail is queryable forever.

CREATE TABLE data_quality_flags (
    id              BIGSERIAL PRIMARY KEY,
    company_id      BIGINT      NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    -- Scope: what the flag is about.
    statement           text,
    concept             text,
    fiscal_year         integer,
    fiscal_quarter      integer,      -- NULL when flag is fiscal-year-scoped (Q-sum vs FY)
    period_end          date,         -- NULL when flag is fiscal-year-scoped
    period_type         text,         -- 'quarter' | 'annual' (NULL when FY-scoped)

    -- The check that fired and the measurements.
    flag_type           text        NOT NULL,
    severity            text        NOT NULL,    -- 'informational' | 'warning' | 'investigate'
    expected_value      numeric,                  -- what the tie formula expected
    computed_value      numeric,                  -- what we observed
    delta               numeric,
    tolerance           numeric,
    suggested_value     numeric,                  -- if derivable by identity (optional)

    -- Human-readable explanation. Shown to analyst in review file.
    reason              text        NOT NULL,
    context             jsonb,                    -- extra structured detail (Q-sum, source accessions, etc.)

    -- Provenance + resolution
    source_run_id       bigint      REFERENCES ingest_runs(id),
    flagged_at          timestamptz NOT NULL DEFAULT now(),
    resolved_at         timestamptz,
    resolution          text,                     -- 'approve_suggestion' | 'override' | 'accept_as_is' | NULL
    resolution_value    numeric,                  -- the value actually written (if corrected)
    resolution_source   text,                     -- analyst's source citation
    resolution_note     text,                     -- analyst's free-form note

    CONSTRAINT data_quality_flags_severity_enum
        CHECK (severity IN ('informational', 'warning', 'investigate')),
    CONSTRAINT data_quality_flags_resolution_enum
        CHECK (resolution IS NULL OR resolution IN ('approve_suggestion', 'override', 'accept_as_is')),
    CONSTRAINT data_quality_flags_flag_type_nonempty
        CHECK (length(flag_type) > 0)
);

-- Primary lookup: "show me all unresolved flags for this company"
CREATE INDEX data_quality_flags_company_unresolved_idx
    ON data_quality_flags (company_id, flagged_at DESC)
    WHERE resolved_at IS NULL;

-- Aggregate view: "anomaly counts by type across universe"
CREATE INDEX data_quality_flags_type_idx
    ON data_quality_flags (flag_type, severity)
    WHERE resolved_at IS NULL;

-- Join-friendly: match to financial_facts
CREATE INDEX data_quality_flags_scope_idx
    ON data_quality_flags (company_id, concept, fiscal_year);

COMMENT ON TABLE data_quality_flags IS
  'Soft-validation anomalies from Layer 3/4. Resolved flags retained as audit trail. See docs/research/amendment_phase_1_5_design.md and Phase 1.5 addendum.';
COMMENT ON COLUMN data_quality_flags.flag_type IS
  'Known types: layer3_q_sum_vs_fy, layer4_eps_reconcile, layer4_tax_rate_sanity, layer4_margin_sanity.';
COMMENT ON COLUMN data_quality_flags.severity IS
  'informational (<1% / noise) | warning (1-10% / worth reviewing) | investigate (>=10% / likely material)';
