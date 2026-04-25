-- Steward layer: data-trust runtime substrate.
--
-- Two tables:
--
--   coverage_membership — curated coverage universe with tier
--     ('core' | 'extended'). The steward enforces per-tier quality
--     expectations. Distinct from `companies`, which is "any ticker we
--     have ever touched"; coverage_membership is "tickers we are committed
--     to keeping right". Future `watchlists` (deferred) is a separate,
--     lighter monitoring scope — different consumer, different table.
--
--   data_quality_findings — steward-produced findings with two-state
--     lifecycle (open → closed) and structured `closed_reason`
--     ('resolved' | 'suppressed' | 'dismissed'). Audit captured in the
--     `history` jsonb column; no separate audit table in V1. Distinct
--     from `data_quality_flags` (inline ingest validation; financial-
--     fact-scoped). The dashboard reads both via `v_open_quality_signals`
--     (defined in db/queries/15_v_open_quality_signals.sql).
--
-- Per the steward design (docs/architecture/steward.md):
--   - Every finding has one OPEN row per fingerprint (partial unique idx).
--     Closed findings accumulate freely as historical record — recurrences
--     of the same fingerprint over time are visible.
--   - Suggested action is structured (jsonb: kind, params, command, prose)
--     so the future agent reads the structured fields and the human reads
--     the prose. Both populated; no LLM in V1.
--   - Suppression with optional expiry lives on the closed finding.
--     The runner respects active suppressions when reopening fingerprints.

CREATE TABLE coverage_membership (
    id          BIGSERIAL   PRIMARY KEY,
    company_id  BIGINT      NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    tier        text        NOT NULL,
    added_at    timestamptz NOT NULL DEFAULT now(),
    added_by    text        NOT NULL,
    notes       text,

    CONSTRAINT coverage_membership_tier_enum
        CHECK (tier IN ('core', 'extended')),
    CONSTRAINT coverage_membership_added_by_nonempty
        CHECK (length(added_by) > 0),
    CONSTRAINT coverage_membership_one_per_company
        UNIQUE (company_id)
);

CREATE INDEX coverage_membership_tier_idx
    ON coverage_membership (tier);

CREATE INDEX coverage_membership_added_at_idx
    ON coverage_membership (added_at DESC);

COMMENT ON TABLE coverage_membership IS
  'Curated coverage universe; steward enforces per-tier expectations. See docs/architecture/steward.md.';
COMMENT ON COLUMN coverage_membership.tier IS
  'core: full quality bar (full FMP backfill, SEC filings, segments, employees). extended: lighter expectations (financial baseline only). Edit per docs/architecture/steward.md § ExpectationSet.';
COMMENT ON COLUMN coverage_membership.added_by IS
  'Actor that added the membership: human:michael | agent:steward_v1 | system:bootstrap.';


CREATE TABLE data_quality_findings (
    id                BIGSERIAL   PRIMARY KEY,
    fingerprint       text        NOT NULL,
    finding_type      text        NOT NULL,
    severity          text        NOT NULL,

    -- Scope (any subset may be NULL for cross-cutting findings).
    company_id        BIGINT      REFERENCES companies(id) ON DELETE CASCADE,
    ticker            text,
    vertical          text,
    fiscal_period_key text,

    -- Detection details.
    source_check      text        NOT NULL,
    evidence          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    summary           text        NOT NULL,
    suggested_action  jsonb,

    -- Lifecycle: two-state. closed_reason is structured.
    status            text        NOT NULL DEFAULT 'open',
    closed_reason     text,
    closed_at         timestamptz,
    closed_by         text,
    closed_note       text,
    suppressed_until  timestamptz,

    -- Audit trail (no separate table in V1).
    history           jsonb       NOT NULL DEFAULT '[]'::jsonb,

    -- Provenance.
    created_at        timestamptz NOT NULL DEFAULT now(),
    created_by        text        NOT NULL,
    last_seen_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT data_quality_findings_severity_enum
        CHECK (severity IN ('informational', 'warning', 'investigate')),
    CONSTRAINT data_quality_findings_status_enum
        CHECK (status IN ('open', 'closed')),
    CONSTRAINT data_quality_findings_closed_reason_enum
        CHECK (closed_reason IS NULL
               OR closed_reason IN ('resolved', 'suppressed', 'dismissed')),
    CONSTRAINT data_quality_findings_finding_type_nonempty
        CHECK (length(finding_type) > 0),
    CONSTRAINT data_quality_findings_source_check_nonempty
        CHECK (length(source_check) > 0),
    CONSTRAINT data_quality_findings_summary_nonempty
        CHECK (length(summary) > 0),
    CONSTRAINT data_quality_findings_created_by_nonempty
        CHECK (length(created_by) > 0),
    CONSTRAINT data_quality_findings_fingerprint_nonempty
        CHECK (length(fingerprint) > 0),

    -- Lifecycle integrity: closed rows must have reason + closed_at + closed_by;
    -- open rows must NOT have any of those set.
    CONSTRAINT data_quality_findings_lifecycle_contract
        CHECK (
            (status = 'open'
             AND closed_reason IS NULL
             AND closed_at IS NULL
             AND closed_by IS NULL)
         OR (status = 'closed'
             AND closed_reason IS NOT NULL
             AND closed_at IS NOT NULL
             AND closed_by IS NOT NULL)
        )
);

-- Dedup: only one OPEN finding per fingerprint. Closed findings accumulate
-- freely so historical recurrences stay visible.
CREATE UNIQUE INDEX data_quality_findings_open_fingerprint_uidx
    ON data_quality_findings (fingerprint)
    WHERE status = 'open';

-- Primary lookup: "show me open findings for this company"
CREATE INDEX data_quality_findings_company_status_idx
    ON data_quality_findings (company_id, status, severity);

-- Universe-wide triage view: open findings sorted by severity then age
CREATE INDEX data_quality_findings_open_severity_idx
    ON data_quality_findings (severity, created_at DESC)
    WHERE status = 'open';

-- Recency feed: most-recently-seen open findings
CREATE INDEX data_quality_findings_last_seen_idx
    ON data_quality_findings (last_seen_at DESC)
    WHERE status = 'open';

-- Suppression-respecting reopen guard: find any closed-suppressed-and-active
-- row for a fingerprint quickly.
CREATE INDEX data_quality_findings_suppressed_active_idx
    ON data_quality_findings (fingerprint, suppressed_until)
    WHERE status = 'closed' AND closed_reason = 'suppressed';

COMMENT ON TABLE data_quality_findings IS
  'Steward-produced findings with two-state lifecycle. Audit in `history` jsonb. UNIONed with open data_quality_flags by v_open_quality_signals. See docs/architecture/steward.md.';
COMMENT ON COLUMN data_quality_findings.fingerprint IS
  'sha256(check_name | sorted_scope_keys | rule_params). Stable across runs so the runner can dedup and auto-resolve. See src/arrow/steward/fingerprint.py.';
COMMENT ON COLUMN data_quality_findings.suggested_action IS
  'Structured: {kind, params, command, prose}. The future agent reads kind/params; the human reads prose. V1 prose is templated by check authors; V2 may regenerate prose via LLM.';
COMMENT ON COLUMN data_quality_findings.history IS
  'Append-only jsonb array of state changes: [{at, actor, action, before, after, note}, ...]. The training corpus the V2 suggester reads from.';
COMMENT ON COLUMN data_quality_findings.created_by IS
  'Actor that surfaced the finding: system:check_runner (V1) | agent:steward_v1 (V2+).';
COMMENT ON COLUMN data_quality_findings.suppressed_until IS
  'Optional expiry on a suppression. NULL means suppress indefinitely. The runner refuses to reopen a fingerprint while a suppression is active.';
