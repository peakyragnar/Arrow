-- v_ff_current: superseded_at IS NULL filter over financial_facts.
--
-- Every downstream metrics view reads this, so PIT filtering (for future
-- asof queries) is applied here once and inherited everywhere.

CREATE OR REPLACE VIEW v_ff_current AS
SELECT
    id,
    company_id,
    statement,
    concept,
    value,
    unit,
    fiscal_year,
    fiscal_quarter,
    fiscal_period_label,
    period_end,
    period_type,
    calendar_year,
    calendar_quarter,
    calendar_period_label,
    published_at,
    source_raw_response_id,
    extraction_version,
    dimension_type,
    dimension_key,
    dimension_label,
    dimension_source
FROM financial_facts
WHERE superseded_at IS NULL;

COMMENT ON VIEW v_ff_current IS
    'Current (non-superseded) facts from financial_facts, including segment dimension metadata. All metric views build on this.';
