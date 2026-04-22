-- Data quality flags: soft-validation anomalies from the audit side rail.
--
-- These do NOT block ingest. They record discrepancies for analyst
-- visibility while allowing the data to load. Only Layer 1 (intra-statement
-- subtotal ties) hard-blocks; it raises and rolls back the transaction
-- before any facts persist for the failing period. All other layers are
-- either side-rail (Layer 3 via amendment_detect, Layer 5 via
-- scripts/reconcile_fmp_vs_xbrl.py) or scaffold/planned (Layer 2, Layer 4).
--
-- Workflow:
--   1. Mainline ingest runs Layer 1 inline. If it raises, the transaction
--      rolls back and no facts persist. If it passes, facts land.
--   2. Audit side rail (Layer 3 and/or Layer 5) may be invoked separately.
--      When it runs:
--      - amendment_detect attempts to resolve Layer 3 period-arithmetic
--        violations via SEC XBRL supersession, atomically, under the
--        savepoint protocol in docs/research/amendment_phase_1_5_design.md.
--      - Anything the agent can't safely resolve, plus Layer 5
--        cross-source divergences, land as rows in this table.
--   3. Operator review: approve suggestion, override, or accept as-is.
--   4. Approved corrections apply via supersession with
--      extraction_version='human-verified-v1', and the flag row gets
--      resolved_at + resolution populated.
--
-- Re-ingest auto-closes dependent flags (migration 012): when
-- backfill_fmp_statements supersedes the facts a flag points at, the flag
-- is resolved with resolution='superseded_by_reingest' before the fresh
-- verification pass runs. This keeps "unresolved flags" pointing at
-- current data.
--
-- Flags are NEVER deleted — resolved flags stay with `resolved_at` set so
-- the audit trail is queryable forever.
--
-- NOTE: This file is an applied migration. Its SQL DDL is append-only and
-- must not be re-edited. The -- header comments above are documentation
-- and were rewritten in-place when the FMP-baseline pivot (ADR-0010)
-- changed which layers are mainline vs side rail. They do not affect DB
-- state.

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
