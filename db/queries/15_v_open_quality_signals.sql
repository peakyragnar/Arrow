-- v_open_quality_signals: unified read surface for the dashboard /findings
-- pane. UNIONs open data_quality_findings (steward output) with open
-- data_quality_flags (inline ingest validation) so the operator works one
-- inbox regardless of which subsystem produced the signal.
--
-- Normalizes both source schemas into a common shape:
--   source           — 'finding' | 'flag'
--   signal_id        — id of the underlying row (unique within source)
--   severity         — 'informational' | 'warning' | 'investigate'
--   ticker, vertical, fiscal_period_key — scope (any may be NULL)
--   summary          — single-line human-readable description
--   detected_at      — when the signal first appeared
--   last_seen_at     — when the signal was most recently re-confirmed
--   age_days         — convenience: detected_at age in days
--
-- See docs/architecture/steward.md § Core Objects and dashboard.md § Routes.
-- Reapplied idempotently by scripts/apply_views.py and the dashboard's
-- _ensure_views() lifespan hook.

CREATE OR REPLACE VIEW v_open_quality_signals AS
SELECT
    'finding'::text                                  AS source,
    f.id                                             AS signal_id,
    f.severity,
    f.company_id,
    f.ticker,
    f.vertical,
    f.fiscal_period_key,
    f.summary,
    f.created_at                                     AS detected_at,
    f.last_seen_at,
    EXTRACT(EPOCH FROM (now() - f.created_at)) / 86400.0
                                                     AS age_days
FROM data_quality_findings f
WHERE f.status = 'open'

UNION ALL

SELECT
    'flag'::text                                     AS source,
    fl.id                                            AS signal_id,
    fl.severity,
    fl.company_id,
    c.ticker,
    NULL::text                                       AS vertical,
    CASE
        WHEN fl.fiscal_year IS NOT NULL AND fl.fiscal_quarter IS NOT NULL
            THEN 'FY' || fl.fiscal_year || '-Q' || fl.fiscal_quarter
        WHEN fl.fiscal_year IS NOT NULL
            THEN 'FY' || fl.fiscal_year
        ELSE NULL
    END                                              AS fiscal_period_key,
    fl.reason                                        AS summary,
    fl.flagged_at                                    AS detected_at,
    fl.flagged_at                                    AS last_seen_at,
    EXTRACT(EPOCH FROM (now() - fl.flagged_at)) / 86400.0
                                                     AS age_days
FROM data_quality_flags fl
JOIN companies c ON c.id = fl.company_id
WHERE fl.resolved_at IS NULL;
